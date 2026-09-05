"""Watch data/geonames/ for GeoNames dump files, parse them, publish to NATS JS.

GeoNames distributes a daily database dump as tab-delimited text files.
The main gazetteer table (``allCountries.txt`` or per-country ``XX.txt``)
has 19 columns:

    geonameid  name  asciiname  alternatenames  latitude  longitude
    feature_class  feature_code  country_code  cc2
    admin1_code  admin2_code  admin3_code  admin4_code
    population  elevation  dem  timezone  modification_date

This service converts each row into the same NATS message format that
``watcher.py`` produces for OSM PBF data, so the downstream consumers
(``es_inserter``, ``postgis_inserter``) can index GeoNames records
without any changes.

Message format (identical to watcher.py):
    {
        "osm_id":      "gn<geonameid>",    # prefixed to avoid collisions
        "osm_type":    "node",              # all GeoNames records are points
        "tags":        {"name": ..., "place": ..., "population": ..., ...},
        "geom":        {"type": "Point", "coordinates": [lon, lat]},
        "admin_level": <mapped from feature_code>,
        "area_km2":    0.0,
    }

Supported file types:
  - ``*.txt``  (main gazetteer TSV — allCountries.txt, CA.txt, etc.)
  - Postal-code TSV files are NOT handled here (different schema).

Usage:
    python run.py gn-watcher
"""

import asyncio
import glob
import json
import os
import zipfile

from shared import nats_client
from shared.config import GN_DATA_DIR, NATS_SUBJECT, WATCH_POLL_INTERVAL
from shared.logging import get_logger
from shared.processed import claim, is_processed, load_processed, record_processed
from shared.progress import ProgressTracker

logger = get_logger("gn-watcher")

BATCH_PUBLISH = 50
MAX_RETRIES = 10
MAX_CONSECUTIVE_FAILURES = 50

# GeoNames TSV column positions (0-indexed)
COL_GEONAMEID = 0
COL_NAME = 1
COL_ASCIINAME = 2
COL_ALTERNATENAMES = 3
COL_LATITUDE = 4
COL_LONGITUDE = 5
COL_FEATURE_CLASS = 6
COL_FEATURE_CODE = 7
COL_COUNTRY_CODE = 8
COL_CC2 = 9
COL_ADMIN1 = 10
COL_ADMIN2 = 11
COL_ADMIN3 = 12
COL_ADMIN4 = 13
COL_POPULATION = 14
COL_ELEVATION = 15
COL_DEM = 16
COL_TIMEZONE = 17
COL_MODIFICATION = 18
NUM_COLUMNS = 19

# ---------------------------------------------------------------------------
# Feature class/code → OSM tag mapping
# ---------------------------------------------------------------------------
# GeoNames feature_class → OSM place type (for ranking)
_FEATURE_CODE_TO_PLACE: dict[str, str] = {
    # P — populated places
    "PPLC": "city",  # capital of a political entity
    "PPLA": "city",  # seat of first-order admin div
    "PPLA2": "city",  # seat of second-order admin div
    "PPLA3": "town",  # seat of third-order admin div
    "PPLA4": "town",  # seat of fourth-order admin div
    "PPLA5": "town",
    "PPL": "village",  # populated place
    "PPLS": "village",  # populated places
    "PPLX": "neighbourhood",  # section of populated place
    "PPLF": "hamlet",  # farm village
    "PPLL": "hamlet",  # populated locality
    "PPLQ": "hamlet",  # abandoned populated place
    "PPLR": "hamlet",  # religious populated place
    "PPLW": "hamlet",  # destroyed populated place
    # A — administrative (place tag for ranking)
    "PCLI": "country",  # independent political entity
    "PCLD": "country",  # dependent political entity
    "PCLF": "country",  # freely associated state
    "PCLS": "country",  # semi-independent political entity
    "ADM1": "state",  # first-order admin division
    "ADM1H": "state",
    "ADM2": "region",  # second-order admin division
    "ADM2H": "region",
}

# Feature_class → broad OSM-compatible tag key
_FEATURE_CLASS_TAGS: dict[str, dict[str, str]] = {
    "A": {"boundary": "administrative"},  # admin boundary
    "H": {"natural": "water"},  # hydrographic
    "L": {"landuse": "area"},  # area / park
    "P": {},  # populated place (handled via place tag)
    "R": {"highway": "road"},  # road / railroad
    "S": {"building": "yes"},  # spot / building / farm
    "T": {"natural": "peak"},  # hypsographic (mountain, hill)
    "U": {"natural": "water"},  # undersea
    "V": {"natural": "wood"},  # vegetation
}

# Feature_code → admin_level mapping for administrative boundaries
_FEATURE_CODE_ADMIN_LEVEL: dict[str, int] = {
    "PCLI": 2,  # independent political entity (country)
    "PCLD": 2,  # dependent political entity
    "PCLF": 2,  # freely associated state
    "PCLS": 2,  # semi-independent political entity
    "PCLH": 2,  # historical political entity
    "ADM1": 4,  # first-order admin division (state/province)
    "ADM1H": 4,
    "ADM2": 6,  # second-order (county/district)
    "ADM2H": 6,
    "ADM3": 7,  # third-order (municipality)
    "ADM3H": 7,
    "ADM4": 8,  # fourth-order
    "ADM4H": 8,
    "ADM5": 9,  # fifth-order
    "ADM5H": 9,
    "ADMD": 6,  # administrative division
    "ADMDH": 6,
}


# ---------------------------------------------------------------------------
# Row parsing
# ---------------------------------------------------------------------------


def _parse_geonames_row(fields: list[str]) -> dict | None:
    """Convert one tab-separated GeoNames row to a NATS message dict.

    Returns None for unusable rows (missing coords, empty name, etc.).
    """
    if len(fields) < NUM_COLUMNS:
        return None

    geonameid = fields[COL_GEONAMEID].strip()
    name = fields[COL_NAME].strip()
    asciiname = fields[COL_ASCIINAME].strip()
    lat_raw = fields[COL_LATITUDE].strip()
    lon_raw = fields[COL_LONGITUDE].strip()
    feature_class = fields[COL_FEATURE_CLASS].strip()
    feature_code = fields[COL_FEATURE_CODE].strip()
    country_code = fields[COL_COUNTRY_CODE].strip()
    population_raw = fields[COL_POPULATION].strip()
    alternatenames = fields[COL_ALTERNATENAMES].strip()
    timezone = fields[COL_TIMEZONE].strip()

    if not name or not geonameid:
        return None

    try:
        lat = float(lat_raw)
        lon = float(lon_raw)
    except (ValueError, TypeError):
        return None

    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
        return None

    # Build OSM-compatible tags
    tags: dict[str, str] = {
        "source": "geonames",
        "name": name,
    }

    # Add ascii name as name:en if it differs
    if asciiname and asciiname != name:
        tags["name:en"] = asciiname

    # Alternate names — limit to first 10 to avoid bloating the index
    # (GeoNames can have 10,000+ characters of comma-separated names)
    if alternatenames:
        names = [n.strip() for n in alternatenames.split(",")[:10] if n.strip()]
        if names:
            tags["alt_name"] = ", ".join(names)

    # Country code
    if country_code:
        tags["addr:country"] = country_code

    # GeoNames feature metadata (useful for search/display)
    if feature_class:
        tags["geonames:feature_class"] = feature_class
    if feature_code:
        tags["geonames:feature_code"] = feature_code

    # Map to OSM place type
    place_type = _FEATURE_CODE_TO_PLACE.get(feature_code, "")
    if place_type:
        tags["place"] = place_type
    elif feature_class == "P":
        # Generic populated place
        tags["place"] = "locality"

    # Add broad feature class tags (natural, highway, building, etc.)
    class_tags = _FEATURE_CLASS_TAGS.get(feature_class, {})
    for k, v in class_tags.items():
        if k not in tags:
            tags[k] = v

    # Population
    if population_raw:
        try:
            pop = int(population_raw)
            if pop > 0:
                tags["population"] = str(pop)
        except ValueError:
            pass

    # Timezone
    if timezone:
        tags["timezone"] = timezone

    # Admin level (for administrative boundaries)
    admin_level = _FEATURE_CODE_ADMIN_LEVEL.get(feature_code, 0)

    return {
        "osm_id": f"gn{geonameid}",
        "osm_type": "node",
        "tags": tags,
        "geom": {"type": "Point", "coordinates": [lon, lat]},
        "admin_level": admin_level,
        "area_km2": 0.0,
    }


# ---------------------------------------------------------------------------
# File-level processing
# ---------------------------------------------------------------------------


def _count_lines(filepath: str) -> int:
    """Quick line count for progress tracking."""
    with open(filepath, encoding="utf-8", errors="replace") as f:
        return sum(1 for _ in f)


def _extract_zips(data_dir: str):
    """Extract any .zip files found in the data directory (non-recursive)."""
    for zpath in glob.glob(os.path.join(data_dir, "*.zip")):
        extract_marker = f"{zpath}.extracted"
        if os.path.exists(extract_marker):
            continue
        logger.info("[gn-watcher] Extracting %s ...", os.path.basename(zpath))
        try:
            with zipfile.ZipFile(zpath, "r") as zf:
                zf.extractall(data_dir)
            with open(extract_marker, "w") as f:
                f.write(f"extracted: {zpath}\n")
            logger.info("[gn-watcher] Extracted %s", os.path.basename(zpath))
        except Exception as e:
            logger.error("[gn-watcher] Error extracting %s: %s", os.path.basename(zpath), e)


async def publish_tsv(filepath: str, js):
    """Read a GeoNames TSV file and publish rows as NATS messages."""
    basename = os.path.basename(filepath)
    logger.info("[gn-watcher] Processing: %s", basename)

    total_lines = _count_lines(filepath)
    progress = ProgressTracker(f"gn-watcher {basename}", total=total_lines)

    published = 0
    consecutive_failures = 0
    batch: list[bytes] = []

    with open(filepath, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                progress.skip()
                continue

            fields = line.split("\t")
            msg = _parse_geonames_row(fields)
            if msg is None:
                progress.skip()
                continue

            batch.append(json.dumps(msg).encode())

            if len(batch) >= BATCH_PUBLISH:
                published, consecutive_failures = await _publish_batch(
                    js, batch, published, consecutive_failures, progress
                )
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    logger.error("[gn-watcher] Too many failures, aborting %s", basename)
                    progress.close()
                    return -1
                batch.clear()

    # Flush remainder
    if batch and consecutive_failures < MAX_CONSECUTIVE_FAILURES:
        published, consecutive_failures = await _publish_batch(
            js, batch, published, consecutive_failures, progress
        )

    progress.close()
    logger.info("[gn-watcher] %s: published %s places", basename, published)
    return published


async def _publish_batch(
    js,
    batch: list[bytes],
    published: int,
    consecutive_failures: int,
    progress: ProgressTracker,
) -> tuple[int, int]:
    """Publish a batch of encoded messages to NATS. Returns updated counters."""
    for payload in batch:
        for attempt in range(MAX_RETRIES):
            try:
                await js.publish(NATS_SUBJECT, payload, timeout=30)
                published += 1
                consecutive_failures = 0
                progress.update()
                break
            except Exception as e:
                err_str = str(e).lower()
                # Permanent error — message too large, skip it
                if any(kw in err_str for kw in ("payload", "large", "exceeded")):
                    progress.skip()
                    break
                # Transient error — retry with exponential backoff
                consecutive_failures += 1
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(min(2**attempt, 30))
                else:
                    logger.error(
                        "[gn-watcher] Failed to publish after %s attempts: %s",
                        MAX_RETRIES,
                        e,
                    )

        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            break

    return published, consecutive_failures


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def _scan_and_process(js) -> int:
    """Extract any new zips and process not-yet-processed GeoNames TSVs once.

    Returns the number of files successfully processed this pass.
    """
    # Auto-extract any new zip files first (idempotent — uses .extracted markers)
    _extract_zips(GN_DATA_DIR)

    # Find GeoNames TSV files (*.txt, excluding readme and metadata)
    txt_files = sorted(glob.glob(os.path.join(GN_DATA_DIR, "*.txt")))
    txt_files = [
        f
        for f in txt_files
        if not os.path.basename(f).lower().startswith("readme")
        and not os.path.basename(f).lower().startswith("license")
        and not os.path.basename(f).lower().endswith("codes.txt")
    ]

    done = load_processed(GN_DATA_DIR)
    pending = [f for f in txt_files if not is_processed(GN_DATA_DIR, f, done)]
    if not pending:
        return 0

    logger.info("[gn-watcher] Found %s new data file(s) to process", len(pending))
    processed = 0
    for filepath in pending:
        if not claim(GN_DATA_DIR, filepath, done):
            continue  # another replica owns this file (pg-ledger mode)
        try:
            count = await publish_tsv(filepath, js)
            # Only mark as processed if we didn't abort early
            if count >= 0:
                record_processed(GN_DATA_DIR, filepath, done)
                logger.info("[gn-watcher] Completed %s", os.path.basename(filepath))
                processed += 1
            else:
                logger.warning(
                    "[gn-watcher] Incomplete processing of %s — will retry next scan",
                    os.path.basename(filepath),
                )
        except Exception as e:
            logger.error("[gn-watcher] Error processing %s: %s", os.path.basename(filepath), e)
            import traceback

            traceback.print_exc()

    return processed


async def run():
    """Continuously watch GN_DATA_DIR, importing new GeoNames files as they appear."""
    logger.info("[gn-watcher] Starting GeoNames watcher service ...")
    os.makedirs(GN_DATA_DIR, exist_ok=True)

    nc, js = await nats_client.connect()
    logger.info("[gn-watcher] Watching %s (re-scan every %ss)", GN_DATA_DIR, WATCH_POLL_INTERVAL)

    try:
        first = True
        while True:
            n = await _scan_and_process(js)
            if first and n == 0:
                logger.info(
                    "[gn-watcher] No new files in %s yet; will keep watching.",
                    GN_DATA_DIR,
                )
            first = False
            await asyncio.sleep(WATCH_POLL_INTERVAL)
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(run())
