"""Shared CLI for the one-off place importers (load → map → publish to NATS).

Both ``import_pelias_google.py`` and ``import_places_pg.py`` are thin wrappers
around :func:`run_cli`, passing the dataset-specific mapper from
``shared.places_mapping``. The startup equivalent is the ``places-watcher``
service; these scripts exist for manual / ad-hoc re-imports.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from collections.abc import Callable

from shared.config import NATS_SUBJECT

Mapper = Callable[[dict], "dict | None"]


def _build(path: str, limit: int | None, mapper: Mapper) -> tuple[list[dict], int]:
    """Load a JSON-array export and map every record. Returns (messages, skipped)."""
    with open(path, encoding="utf-8") as f:
        records = json.load(f)
    if limit:
        records = records[:limit]
    messages, skipped = [], 0
    for rec in records:
        msg = mapper(rec)
        if msg is None:
            skipped += 1
            continue
        messages.append(msg)
    return messages, skipped


async def _publish(messages: list[dict], batch_size: int, logger: logging.Logger) -> int:
    """Publish element messages to the OSM stream, awaiting acks in batches."""
    from shared import nats_client

    nc, js = await nats_client.connect()
    published = 0
    try:
        for start in range(0, len(messages), batch_size):
            batch = messages[start : start + batch_size]
            acks = await asyncio.gather(
                *(
                    js.publish(NATS_SUBJECT, json.dumps(m, ensure_ascii=False).encode(), timeout=30)
                    for m in batch
                ),
                return_exceptions=True,
            )
            for i, ack in enumerate(acks):
                if isinstance(ack, Exception):
                    logger.error("publish failed for %s: %s", batch[i]["osm_id"], ack)
                else:
                    published += 1
            logger.info("published %d/%d", published, len(messages))
    finally:
        await nc.drain()
    return published


async def run_cli(name: str, mapper: Mapper, *, default_file: str) -> None:
    """Argparse + load + map + publish loop shared by the importer scripts."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger(name)

    ap = argparse.ArgumentParser(description=f"{name}: publish a place export to NATS")
    ap.add_argument("--file", required=True, help=f"Path to the export JSON (e.g. {default_file})")
    ap.add_argument("--limit", type=int, default=None, help="Only import the first N records")
    ap.add_argument("--batch", type=int, default=500, help="Publish batch size (acks awaited)")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Map + print samples and stats; do NOT connect/publish to NATS",
    )
    args = ap.parse_args()

    logger.info("loading %s ...", args.file)
    messages, skipped = _build(args.file, args.limit, mapper)
    logger.info("mapped %d messages (%d skipped: unplaceable)", len(messages), skipped)

    no_name = sum(1 for m in messages if not m["tags"].get("name"))
    if no_name:
        logger.warning("%d messages have no name tag", no_name)

    if args.dry_run:
        for m in messages[:5]:
            print(json.dumps(m, ensure_ascii=False, indent=2))
        logger.info("dry-run: nothing published")
        return

    published = await _publish(messages, args.batch, logger)
    logger.info("DONE: published %d/%d messages to %s", published, len(messages), NATS_SUBJECT)
    if published != len(messages):
        sys.exit(1)
