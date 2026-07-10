"""In-place backfill of the category_* fields onto existing ``osm_places`` docs.

Adding ``category_key`` / ``category_value`` / ``category_group`` to the mapping
is additive, but docs indexed before that change have no values — so
``/nearby?category=`` / ``?group=`` would miss them until they are re-ingested.
This script fills them **in place**, reading each doc's ``tags`` (still stored in
``_source`` even though the field is ``enabled: false``) and running the exact
same :func:`shared.categories.classify` used at ingest and at query time — so the
derivation can never drift. No reindex, no multi-hour PBF re-run.

It is idempotent: by default it only touches docs with no ``category_group`` yet
(``--all`` reprocesses every doc). It throttles between bulk batches so it doesn't
starve live query serving. Safe to re-run and safe to interrupt.

Usage (inside a container on the compose network, or with PYTHONPATH=repo root):

    python scripts/backfill_categories.py                 # missing docs only
    python scripts/backfill_categories.py --all           # reprocess everything
    python scripts/backfill_categories.py --batch-size 2000 --sleep 0.25
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk, async_scan

from shared.categories import classify
from shared.config import ELASTICSEARCH_URL

INDEX = "osm_places"


async def backfill(batch_size: int, sleep_s: float, reprocess_all: bool) -> None:
    es = AsyncElasticsearch(ELASTICSEARCH_URL)
    # A doc with any category_group value (including "") has already been done;
    # missing-only selects docs that predate the field entirely.
    query = (
        {"match_all": {}}
        if reprocess_all
        else {"bool": {"must_not": [{"exists": {"field": "category_group"}}]}}
    )

    scanned = 0
    updated = 0
    actions: list[dict] = []

    async def flush() -> None:
        nonlocal actions, updated
        if not actions:
            return
        ok, errors = await async_bulk(es, actions, raise_on_error=False)
        updated += ok
        if errors:
            print(f"  {len(errors)} error(s) in batch; first: {errors[0]}")
        actions = []
        if sleep_s:
            await asyncio.sleep(sleep_s)  # throttle so live queries aren't starved

    try:
        async for doc in async_scan(
            es,
            index=INDEX,
            query={"query": query, "_source": ["tags", "admin_level"]},
            size=batch_size,
        ):
            scanned += 1
            src = doc.get("_source", {})
            cat = classify(src.get("tags", {}), src.get("admin_level"))
            actions.append(
                {
                    "_op_type": "update",
                    "_index": INDEX,
                    "_id": doc["_id"],
                    # retry rather than fail if the doc is written concurrently
                    # (e.g. live ingest re-indexing the same element).
                    "retry_on_conflict": 3,
                    "doc": {
                        "category_key": cat.key or "",
                        "category_value": cat.value or "",
                        "category_group": cat.group or "",
                    },
                }
            )
            if len(actions) >= batch_size:
                await flush()
                print(f"  scanned={scanned} updated={updated}")
        await flush()
    finally:
        await es.close()

    print(f"done: scanned={scanned} updated={updated}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--batch-size", type=int, default=1000, help="docs per scan/bulk batch")
    ap.add_argument(
        "--sleep", type=float, default=0.5, help="seconds to pause between batches (throttle)"
    )
    ap.add_argument(
        "--all",
        action="store_true",
        help="reprocess every doc, not just those missing category_group",
    )
    args = ap.parse_args()
    asyncio.run(backfill(args.batch_size, args.sleep, args.all))


if __name__ == "__main__":
    main()
