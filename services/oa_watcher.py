"""Watch data/openaddresses/ for OpenAddresses CSV files, parse them, publish to NATS JS.

OpenAddresses distributes standardised address data as CSV (or GeoJSON) with
the following columns:

    LON, LAT, NUMBER, STREET, UNIT, CITY, DISTRICT, REGION, POSTCODE, ID, HASH

This service converts each row into the same NATS message format that
``watcher.py`` produces for OSM PBF data, so the downstream consumers
(``es_inserter``, ``postgis_inserter``) can index OpenAddresses records
without any changes.

Message format (identical to watcher.py):
    {
        "osm_id":      "oa<hash>",        # prefixed to avoid collisions with OSM IDs
        "osm_type":    "node",             # all OA records are point features
        "tags":        {"addr:housenumber": ..., "addr:street": ..., ...},
        "geom":        {"type": "Point", "coordinates": [lon, lat]},
        "admin_level": 0,
        "area_km2":    0.0,
    }

Usage:
    python run.py oa-watcher
"""

import asyncio
import csv
import glob
import hashlib
import json
import os

from shared import nats_client
from shared.config import NATS_SUBJECT, OA_DATA_DIR, WATCH_POLL_INTERVAL
from shared.logging import get_logger
from shared.processed import claim, is_processed, load_processed, record_processed
from shared.progress import ProgressTracker

logger = get_logger("oa-watcher")

BATCH_PUBLISH = 50
# Maximum retries per message
MAX_RETRIES = 10
MAX_CONSECUTIVE_FAILURES = 50


# ---------------------------------------------------------------------------
# Row parsing
# ---------------------------------------------------------------------------


def _source_hash(filepath: str) -> str:
    """Short deterministic hash of a file path for use in ID generation."""
    return hashlib.md5(filepath.encode()).hexdigest()[:12]


def _parse_csv_row(row: dict, row_idx: int, src_hash: str = "") -> dict | None:
    """Convert one OpenAddresses CSV row to a NATS message dict.

    Returns None if the row is unusable (missing coordinates or address data).
    *src_hash* is a short hash of the source file path, used to guarantee
    unique IDs across files when the HASH column is missing.
    """
    lon_raw = (row.get("LON") or "").strip()
    lat_raw = (row.get("LAT") or "").strip()
    if not lon_raw or not lat_raw:
        return None

    try:
        lon = float(lon_raw)
        lat = float(lat_raw)
    except ValueError:
        return None

    # Basic coordinate sanity check
    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
        return None

    number = (row.get("NUMBER") or "").strip()
    street = (row.get("STREET") or "").strip()

    # Need at least a street or a house number to be useful
    if not number and not street:
        return None

    tags: dict[str, str] = {"source": "openaddresses"}
    if number:
        tags["addr:housenumber"] = number
    if street:
        tags["addr:street"] = street
    if unit := (row.get("UNIT") or "").strip():
        tags["addr:unit"] = unit
    if city := (row.get("CITY") or "").strip():
        tags["addr:city"] = city
    if district := (row.get("DISTRICT") or "").strip():
        tags["addr:county"] = district
    if region := (row.get("REGION") or "").strip():
        tags["addr:state"] = region
    if postcode := (row.get("POSTCODE") or "").strip():
        tags["addr:postcode"] = postcode

    # Generate a stable ID — prefer the HASH column, fall back to
    # source_hash + row_idx to avoid collisions across files.
    oa_hash = (row.get("HASH") or "").strip()
    if oa_hash:
        oa_id_suffix = oa_hash
    else:
        oa_id_suffix = f"{src_hash}_{row_idx}" if src_hash else str(row_idx)

    return {
        "osm_id": f"oa{oa_id_suffix}",
        "osm_type": "node",
        "tags": tags,
        "geom": {"type": "Point", "coordinates": [lon, lat]},
        "admin_level": 0,
        "area_km2": 0.0,
    }


def _parse_geojson_feature(feature: dict, row_idx: int, src_hash: str = "") -> dict | None:
    """Convert one OpenAddresses GeoJSON Feature to a NATS message dict.

    *src_hash* disambiguates row-index-based IDs across files.
    """
    geom = feature.get("geometry")
    props = feature.get("properties", {})

    if not geom or geom.get("type") != "Point":
        return None

    coords = geom.get("coordinates")
    if not coords or len(coords) < 2:
        return None

    try:
        lon = float(coords[0])
        lat = float(coords[1])
    except (TypeError, ValueError):
        return None

    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
        return None

    number = (str(props.get("NUMBER") or props.get("number") or "")).strip()
    street = (str(props.get("STREET") or props.get("street") or "")).strip()

    if not number and not street:
        return None

    tags: dict[str, str] = {"source": "openaddresses"}
    if number:
        tags["addr:housenumber"] = number
    if street:
        tags["addr:street"] = street

    for oa_key, osm_key in (
        ("UNIT", "addr:unit"),
        ("CITY", "addr:city"),
        ("DISTRICT", "addr:county"),
        ("REGION", "addr:state"),
        ("POSTCODE", "addr:postcode"),
    ):
        val = (str(props.get(oa_key) or props.get(oa_key.lower()) or "")).strip()
        if val:
            tags[osm_key] = val

    oa_hash = (str(props.get("HASH") or props.get("hash") or "")).strip()
    if oa_hash:
        oa_id_suffix = oa_hash
    else:
        oa_id_suffix = f"{src_hash}_{row_idx}" if src_hash else str(row_idx)

    return {
        "osm_id": f"oa{oa_id_suffix}",
        "osm_type": "node",
        "tags": tags,
        "geom": {"type": "Point", "coordinates": [lon, lat]},
        "admin_level": 0,
        "area_km2": 0.0,
    }


# ---------------------------------------------------------------------------
# File-level processing
# ---------------------------------------------------------------------------


def _count_csv_rows(filepath: str) -> int:
    """Quick line count (minus header) for progress tracking."""
    with open(filepath, encoding="utf-8", errors="replace") as f:
        return sum(1 for _ in f) - 1  # subtract header


async def publish_csv(filepath: str, js):
    """Read a CSV file and publish rows as NATS messages."""
    basename = os.path.basename(filepath)
    logger.info("[oa-watcher] Processing CSV: %s", basename)

    total_rows = _count_csv_rows(filepath)
    progress = ProgressTracker(f"oa-watcher {basename}", total=total_rows)

    published = 0
    consecutive_failures = 0
    shash = _source_hash(filepath)

    with open(filepath, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        batch: list[bytes] = []

        for row_idx, row in enumerate(reader):
            msg = _parse_csv_row(row, row_idx, src_hash=shash)
            if msg is None:
                progress.skip()
                continue

            batch.append(json.dumps(msg).encode())

            if len(batch) >= BATCH_PUBLISH:
                published, consecutive_failures = await _publish_batch(
                    js, batch, published, consecutive_failures, progress
                )
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    logger.error("[oa-watcher] Too many failures, aborting %s", basename)
                    progress.close()
                    return -1
                batch.clear()

        # Flush remainder
        if batch and consecutive_failures < MAX_CONSECUTIVE_FAILURES:
            published, consecutive_failures = await _publish_batch(
                js, batch, published, consecutive_failures, progress
            )

    progress.close()
    logger.info("[oa-watcher] %s: published %s addresses", basename, published)
    return published


def _is_ndjson(filepath: str) -> bool:
    """Detect newline-delimited GeoJSON (one Feature per line) vs FeatureCollection."""
    with open(filepath, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                return obj.get("type") == "Feature" and "geometry" in obj and "properties" in obj
            except json.JSONDecodeError:
                return False
    return False


def _count_lines(filepath: str) -> int:
    """Count lines in a file for progress tracking."""
    with open(filepath, encoding="utf-8", errors="replace") as f:
        return sum(1 for _ in f)


async def publish_geojson(filepath: str, js):
    """Read a GeoJSON file and publish features as NATS messages.

    Supports both standard FeatureCollection and newline-delimited GeoJSON
    (NDJSON), which is the format OpenAddresses actually distributes.
    Also supports gzip-compressed files (.geojson.gz).
    """
    import gzip

    basename = os.path.basename(filepath)
    logger.info("[oa-watcher] Processing GeoJSON: %s", basename)

    is_gz = filepath.endswith(".gz")
    _open = gzip.open if is_gz else open

    ndjson = False
    if not is_gz:
        ndjson = _is_ndjson(filepath)
    else:
        # For .gz we always assume NDJSON (OpenAddresses convention)
        ndjson = True

    if ndjson:
        logger.info("[oa-watcher] Detected newline-delimited GeoJSON (NDJSON)")
        total = _count_lines(filepath) if not is_gz else 0
        features = None  # will stream line-by-line
    else:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            data = json.load(f)
        features = data.get("features", [])
        total = len(features)

    progress = ProgressTracker(f"oa-watcher {basename}", total=total)
    shash = _source_hash(filepath)

    published = 0
    consecutive_failures = 0
    batch: list[bytes] = []

    def _process_feature(feature: dict, row_idx: int) -> bool:
        """Process a single feature. Returns False if we should abort."""
        nonlocal published, consecutive_failures
        msg = _parse_geojson_feature(feature, row_idx, src_hash=shash)
        if msg is None:
            progress.skip()
            return True
        batch.append(json.dumps(msg).encode())
        return True

    async def _flush_if_full() -> bool:
        """Flush batch if full. Returns False if we should abort."""
        nonlocal published, consecutive_failures
        if len(batch) >= BATCH_PUBLISH:
            published, consecutive_failures = await _publish_batch(
                js, batch, published, consecutive_failures, progress
            )
            batch.clear()
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error("[oa-watcher] Too many failures, aborting %s", basename)
                return False
        return True

    if ndjson:
        # Stream line-by-line to avoid loading entire file into memory
        with _open(filepath, "rt", encoding="utf-8", errors="replace") as f:
            for row_idx, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    feature = json.loads(line)
                except json.JSONDecodeError:
                    progress.skip()
                    continue
                _process_feature(feature, row_idx)
                if not await _flush_if_full():
                    progress.close()
                    return -1
    else:
        for row_idx, feature in enumerate(features):
            _process_feature(feature, row_idx)
            if not await _flush_if_full():
                progress.close()
                return -1

    if batch and consecutive_failures < MAX_CONSECUTIVE_FAILURES:
        published, consecutive_failures = await _publish_batch(
            js, batch, published, consecutive_failures, progress
        )

    progress.close()
    logger.info("[oa-watcher] %s: published %s addresses", basename, published)
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
                        "[oa-watcher] Failed to publish after %s attempts: %s",
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
    """Process any not-yet-processed CSV/GeoJSON files in OA_DATA_DIR once.

    Returns the number of files successfully processed this pass.
    """
    # Scan recursively — OA archives often unzip into nested dirs
    # (e.g. collection-ca/ca/on/york-addresses-county.geojson)
    csv_files = glob.glob(os.path.join(OA_DATA_DIR, "**", "*.csv"), recursive=True)
    geojson_files = glob.glob(
        os.path.join(OA_DATA_DIR, "**", "*.geojson"), recursive=True
    ) + glob.glob(os.path.join(OA_DATA_DIR, "**", "*.geojson.gz"), recursive=True)
    all_files = sorted(f for f in csv_files + geojson_files if not f.endswith(".meta"))

    # Only act on files we haven't already imported (skip processed quietly).
    done = load_processed(OA_DATA_DIR)
    pending = [f for f in all_files if not is_processed(OA_DATA_DIR, f, done)]
    if not pending:
        return 0

    logger.info("[oa-watcher] Found %s new file(s) to process", len(pending))
    processed = 0
    for filepath in pending:
        rel_path = os.path.relpath(filepath, OA_DATA_DIR)
        if not claim(OA_DATA_DIR, filepath, done):
            continue  # another replica owns this file (pg-ledger mode)
        try:
            if filepath.endswith(".csv"):
                count = await publish_csv(filepath, js)
            elif filepath.endswith((".geojson", ".geojson.gz")):
                count = await publish_geojson(filepath, js)
            else:
                continue

            # Only mark as processed if we didn't abort early
            if count >= 0:
                record_processed(OA_DATA_DIR, filepath, done)
                logger.info("[oa-watcher] Completed %s", rel_path)
                processed += 1
        except Exception as e:
            logger.error("[oa-watcher] Error processing %s: %s", rel_path, e)
            import traceback

            traceback.print_exc()

    return processed


async def run():
    """Continuously watch OA_DATA_DIR, importing new CSV/GeoJSON files as they appear."""
    logger.info("[oa-watcher] Starting OpenAddresses watcher service ...")
    os.makedirs(OA_DATA_DIR, exist_ok=True)

    nc, js = await nats_client.connect()
    logger.info("[oa-watcher] Watching %s (re-scan every %ss)", OA_DATA_DIR, WATCH_POLL_INTERVAL)

    try:
        first = True
        while True:
            n = await _scan_and_process(js)
            if first and n == 0:
                logger.info(
                    "[oa-watcher] No new files in %s yet; will keep watching.",
                    OA_DATA_DIR,
                )
            first = False
            await asyncio.sleep(WATCH_POLL_INTERVAL)
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(run())
