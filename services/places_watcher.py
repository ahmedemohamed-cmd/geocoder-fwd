"""Watch data/places/ for curated place exports, map them, publish to NATS JS.

Imports the curated place datasets that aren't downloadable feeds (OSM/OA/
GeoNames) but JSON-array exports we drop into ``data/places/``. Every file is
expected in the **unified place schema** (``shared.places_mapping``); raw
Pelias/Postgres exports are converted to it once via ``scripts/unify_places.py``.

Each unified record is mapped (``place_to_element``) into the same NATS message
format ``watcher.py`` produces for OSM PBF data, so the downstream consumers
(``es_inserter``, ``postgis_inserter``) index it with no changes. A single
``.processed`` ledger in the data dir records imported files so a restart does
NOT re-import them (re-imports are idempotent anyway — osm_id is stable).

Usage:
    python run.py places-watcher
"""

import asyncio
import glob
import json
import os

from shared import nats_client
from shared.config import NATS_SUBJECT, PLACES_DATA_DIR, WATCH_POLL_INTERVAL
from shared.logging import get_logger
from shared.places_mapping import place_to_element
from shared.processed import claim, is_processed, load_processed, record_processed
from shared.progress import ProgressTracker

logger = get_logger("places-watcher")

BATCH_PUBLISH = 500
MAX_RETRIES = 10
MAX_CONSECUTIVE_FAILURES = 50


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
                if any(kw in err_str for kw in ("payload", "large", "exceeded")):
                    progress.skip()  # permanent — message too large, skip it
                    break
                consecutive_failures += 1
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(min(2**attempt, 30))
                else:
                    logger.error(
                        "[places-watcher] Failed to publish after %s attempts: %s",
                        MAX_RETRIES,
                        e,
                    )
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            break
    return published, consecutive_failures


async def publish_file(filepath: str, js) -> int:
    """Read a JSON-array place export and publish each record as a NATS message.

    Returns the number published, or -1 if aborted early (too many failures).
    """
    basename = os.path.basename(filepath)
    logger.info("[places-watcher] Processing: %s", basename)

    try:
        with open(filepath, encoding="utf-8") as f:
            records = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.error("[places-watcher] Cannot read %s: %s", basename, e)
        return -1
    if not isinstance(records, list):
        logger.error("[places-watcher] %s: expected a JSON array, skipping", basename)
        return -1

    progress = ProgressTracker(f"places-watcher {basename}", total=len(records))
    published = 0
    consecutive_failures = 0
    batch: list[bytes] = []

    for rec in records:
        try:
            msg = place_to_element(rec)
        except Exception as e:
            logger.debug("[places-watcher] %s: bad record skipped: %s", basename, e)
            msg = None
        if msg is None:
            progress.skip()
            continue

        batch.append(json.dumps(msg, ensure_ascii=False).encode())
        if len(batch) >= BATCH_PUBLISH:
            published, consecutive_failures = await _publish_batch(
                js, batch, published, consecutive_failures, progress
            )
            batch.clear()
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error("[places-watcher] Too many failures, aborting %s", basename)
                progress.close()
                return -1

    if batch and consecutive_failures < MAX_CONSECUTIVE_FAILURES:
        published, consecutive_failures = await _publish_batch(
            js, batch, published, consecutive_failures, progress
        )

    progress.close()
    logger.info("[places-watcher] %s: published %s places", basename, published)
    return published


async def _scan_and_process(js) -> int:
    """Process not-yet-imported *.json place exports once. Returns files done."""
    json_files = sorted(glob.glob(os.path.join(PLACES_DATA_DIR, "*.json")))

    done = load_processed(PLACES_DATA_DIR)
    pending = [f for f in json_files if not is_processed(PLACES_DATA_DIR, f, done)]
    if not pending:
        return 0

    logger.info("[places-watcher] Found %s new place file(s) to process", len(pending))
    processed = 0
    for filepath in pending:
        if not claim(PLACES_DATA_DIR, filepath, done):
            continue  # another replica owns this file (pg-ledger mode)
        try:
            count = await publish_file(filepath, js)
            if count >= 0:
                record_processed(PLACES_DATA_DIR, filepath, done)
                logger.info("[places-watcher] Completed %s", os.path.basename(filepath))
                processed += 1
            else:
                logger.warning(
                    "[places-watcher] Incomplete processing of %s — will retry next scan",
                    os.path.basename(filepath),
                )
        except Exception as e:
            logger.error("[places-watcher] Error processing %s: %s", os.path.basename(filepath), e)
            import traceback

            traceback.print_exc()
    return processed


async def run():
    """Continuously watch PLACES_DATA_DIR, importing new place exports as they appear."""
    logger.info("[places-watcher] Starting places watcher service ...")
    os.makedirs(PLACES_DATA_DIR, exist_ok=True)

    nc, js = await nats_client.connect()
    logger.info(
        "[places-watcher] Watching %s (re-scan every %ss)", PLACES_DATA_DIR, WATCH_POLL_INTERVAL
    )

    try:
        first = True
        while True:
            n = await _scan_and_process(js)
            if first and n == 0:
                logger.info(
                    "[places-watcher] No new files in %s yet; will keep watching.",
                    PLACES_DATA_DIR,
                )
            first = False
            await asyncio.sleep(WATCH_POLL_INTERVAL)
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(run())
