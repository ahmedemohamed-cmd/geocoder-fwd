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
import json
import os
import time

from shared.config import OA_DATA_DIR, NATS_SUBJECT
from shared import nats_client

BATCH_PUBLISH = 50
# Maximum retries per message
MAX_RETRIES = 10
MAX_CONSECUTIVE_FAILURES = 50


class ProgressTracker:
    """Simple progress tracker that logs updates at regular intervals."""

    def __init__(self, description: str, total: int = 0, log_interval: int = 5):
        self.description = description
        self.total = total
        self.log_interval = log_interval
        self.count = 0
        self.skipped = 0
        self.start_time = time.time()
        self.last_log_time = self.start_time

    def update(self, n: int = 1):
        self.count += n
        now = time.time()
        if now - self.last_log_time >= self.log_interval:
            self._log()
            self.last_log_time = now

    def skip(self, n: int = 1):
        self.skipped += n

    def _log(self):
        elapsed = time.time() - self.start_time
        rate = self.count / elapsed if elapsed > 0 else 0
        if self.total > 0:
            pct = self.count / self.total * 100
            print(
                f"[{self.description}] {self.count}/{self.total} ({pct:.1f}%) "
                f"- {rate:.0f} items/sec  (skipped {self.skipped})",
                flush=True,
            )
        else:
            print(
                f"[{self.description}] {self.count} items - {rate:.0f} items/sec"
                f"  (skipped {self.skipped})",
                flush=True,
            )

    def close(self):
        self._log()
        elapsed = time.time() - self.start_time
        print(
            f"[{self.description}] Completed in {elapsed:.1f}s — "
            f"{self.count} published, {self.skipped} skipped",
            flush=True,
        )


# ---------------------------------------------------------------------------
# Row parsing
# ---------------------------------------------------------------------------

def _parse_csv_row(row: dict, row_idx: int) -> dict | None:
    """Convert one OpenAddresses CSV row to a NATS message dict.

    Returns None if the row is unusable (missing coordinates or address data).
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
    if (unit := (row.get("UNIT") or "").strip()):
        tags["addr:unit"] = unit
    if (city := (row.get("CITY") or "").strip()):
        tags["addr:city"] = city
    if (district := (row.get("DISTRICT") or "").strip()):
        tags["addr:county"] = district
    if (region := (row.get("REGION") or "").strip()):
        tags["addr:state"] = region
    if (postcode := (row.get("POSTCODE") or "").strip()):
        tags["addr:postcode"] = postcode

    # Generate a stable ID — prefer the HASH column, fall back to row index
    oa_hash = (row.get("HASH") or "").strip()
    oa_id_suffix = oa_hash if oa_hash else str(row_idx)

    return {
        "osm_id": f"oa{oa_id_suffix}",
        "osm_type": "node",
        "tags": tags,
        "geom": {"type": "Point", "coordinates": [lon, lat]},
        "admin_level": 0,
        "area_km2": 0.0,
    }


def _parse_geojson_feature(feature: dict, row_idx: int) -> dict | None:
    """Convert one OpenAddresses GeoJSON Feature to a NATS message dict."""
    geom = feature.get("geometry")
    props = feature.get("properties", {})

    if not geom or geom.get("type") != "Point":
        return None

    coords = geom.get("coordinates")
    if not coords or len(coords) < 2:
        return None

    lon, lat = coords[0], coords[1]
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
    oa_id_suffix = oa_hash if oa_hash else str(row_idx)

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
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        return sum(1 for _ in f) - 1  # subtract header


async def publish_csv(filepath: str, js):
    """Read a CSV file and publish rows as NATS messages."""
    basename = os.path.basename(filepath)
    print(f"[oa-watcher] Processing CSV: {basename}", flush=True)

    total_rows = _count_csv_rows(filepath)
    progress = ProgressTracker(f"oa-watcher {basename}", total=total_rows)

    published = 0
    consecutive_failures = 0

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        batch: list[bytes] = []

        for row_idx, row in enumerate(reader):
            msg = _parse_csv_row(row, row_idx)
            if msg is None:
                progress.skip()
                continue

            batch.append(json.dumps(msg).encode())

            if len(batch) >= BATCH_PUBLISH:
                published, consecutive_failures = await _publish_batch(
                    js, batch, published, consecutive_failures, progress
                )
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    print(f"[oa-watcher] Too many failures, aborting {basename}", flush=True)
                    break
                batch.clear()

        # Flush remainder
        if batch and consecutive_failures < MAX_CONSECUTIVE_FAILURES:
            published, consecutive_failures = await _publish_batch(
                js, batch, published, consecutive_failures, progress
            )

    progress.close()
    print(f"[oa-watcher] {basename}: published {published} addresses", flush=True)
    return published


def _is_ndjson(filepath: str) -> bool:
    """Detect newline-delimited GeoJSON (one Feature per line) vs FeatureCollection."""
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        first_char = f.read(1).strip()
        if not first_char:
            return False
        f.seek(0)
        first_line = f.readline().strip()
        if not first_line:
            return False
        try:
            obj = json.loads(first_line)
            # If the first line is a Feature (not a FeatureCollection), it's NDJSON
            return obj.get("type") == "Feature"
        except json.JSONDecodeError:
            return False


def _count_lines(filepath: str) -> int:
    """Count lines in a file for progress tracking."""
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        return sum(1 for _ in f)


async def publish_geojson(filepath: str, js):
    """Read a GeoJSON file and publish features as NATS messages.

    Supports both standard FeatureCollection and newline-delimited GeoJSON
    (NDJSON), which is the format OpenAddresses actually distributes.
    """
    basename = os.path.basename(filepath)
    print(f"[oa-watcher] Processing GeoJSON: {basename}", flush=True)

    ndjson = _is_ndjson(filepath)

    if ndjson:
        print(f"[oa-watcher] Detected newline-delimited GeoJSON (NDJSON)", flush=True)
        total = _count_lines(filepath)
        features = None  # will stream line-by-line
    else:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            data = json.load(f)
        features = data.get("features", [])
        total = len(features)

    progress = ProgressTracker(f"oa-watcher {basename}", total=total)

    published = 0
    consecutive_failures = 0
    batch: list[bytes] = []

    def _process_feature(feature: dict, row_idx: int) -> bool:
        """Process a single feature. Returns False if we should abort."""
        nonlocal published, consecutive_failures
        msg = _parse_geojson_feature(feature, row_idx)
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
                print(f"[oa-watcher] Too many failures, aborting {basename}", flush=True)
                return False
        return True

    if ndjson:
        # Stream line-by-line to avoid loading entire file into memory
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
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
                    break
    else:
        for row_idx, feature in enumerate(features):
            _process_feature(feature, row_idx)
            if not await _flush_if_full():
                break

    if batch and consecutive_failures < MAX_CONSECUTIVE_FAILURES:
        published, consecutive_failures = await _publish_batch(
            js, batch, published, consecutive_failures, progress
        )

    progress.close()
    print(f"[oa-watcher] {basename}: published {published} addresses", flush=True)
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
                ack = await js.publish(NATS_SUBJECT, payload, timeout=30)
                if ack:
                    published += 1
                    consecutive_failures = 0
                    progress.update()
                    break
                else:
                    consecutive_failures += 1
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(0.5 * (attempt + 1))
            except Exception as e:
                consecutive_failures += 1
                err_str = str(e).lower()
                if "payload" in err_str and ("large" in err_str or "exceeded" in err_str):
                    # Permanent error — skip this message
                    progress.skip()
                    break
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    print(f"[oa-watcher] Failed to publish after {MAX_RETRIES} attempts: {e}", flush=True)

        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            break

    return published, consecutive_failures


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def run():
    """Scan OA_DATA_DIR for CSV/GeoJSON files and publish them to NATS."""
    print("[oa-watcher] Starting OpenAddresses watcher service ...", flush=True)
    os.makedirs(OA_DATA_DIR, exist_ok=True)

    # Scan recursively — OA archives often unzip into nested dirs
    # (e.g. collection-ca/ca/on/york-addresses-county.geojson)
    csv_files = sorted(glob.glob(os.path.join(OA_DATA_DIR, "**", "*.csv"), recursive=True))
    geojson_files = sorted(
        glob.glob(os.path.join(OA_DATA_DIR, "**", "*.geojson"), recursive=True)
        + glob.glob(os.path.join(OA_DATA_DIR, "**", "*.geojson.gz"), recursive=True)
    )
    # Filter out .meta files that OpenAddresses ships alongside data files
    geojson_files = [f for f in geojson_files if not f.endswith(".meta")]

    all_files = csv_files + geojson_files
    if not all_files:
        print(
            f"[oa-watcher] No CSV or GeoJSON files found in {OA_DATA_DIR} (scanned recursively). "
            f"Place OpenAddresses files there and restart.",
            flush=True,
        )
        return

    print(f"[oa-watcher] Found {len(csv_files)} CSV, {len(geojson_files)} GeoJSON files (recursive scan)", flush=True)

    nc, js = await nats_client.connect()

    total_published = 0
    for filepath in all_files:
        # Show path relative to OA_DATA_DIR for readability
        rel_path = os.path.relpath(filepath, OA_DATA_DIR)
        lock_file = f"{filepath}.processed"
        if os.path.exists(lock_file):
            print(f"[oa-watcher] Skipping {rel_path} (already processed)", flush=True)
            continue

        try:
            if filepath.endswith(".csv"):
                count = await publish_csv(filepath, js)
            elif filepath.endswith(".geojson") or filepath.endswith(".geojson.gz"):
                count = await publish_geojson(filepath, js)
            else:
                continue

            total_published += count

            # Mark as processed
            with open(lock_file, "w") as lf:
                lf.write(f"processed: {filepath}\n")
            print(f"[oa-watcher] Completed {rel_path}", flush=True)

        except Exception as e:
            print(f"[oa-watcher] Error processing {rel_path}: {e}", flush=True)
            import traceback
            traceback.print_exc()

    print(f"[oa-watcher] All files processed. Total published: {total_published}", flush=True)
    await nc.close()


if __name__ == "__main__":
    asyncio.run(run())
