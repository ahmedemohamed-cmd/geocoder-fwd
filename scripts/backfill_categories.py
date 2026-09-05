"""In-place backfill of the category_* fields onto existing ``osm_places`` docs.

Adding ``category_key`` / ``category_value`` / ``category_group`` / ``category_text``
to the mapping is additive, but docs indexed before that change have no values — so
``/nearby?category=`` / ``?group=`` would miss them, and ``/autocomplete`` could not
answer a type query like "metro", until they are re-ingested. This script fills them
**in place**, reading each doc's ``tags`` (still stored in ``_source`` even though the
field is ``enabled: false``) and running the exact same
:func:`shared.categories.classify` / :func:`shared.categories.category_text` used at
ingest and at query time — so the derivation can never drift. No reindex, no
multi-hour PBF re-run.

It is idempotent: by default it only touches docs with no ``category_text`` yet
(``--all`` reprocesses every doc). Selecting on ``category_text`` — the newest of the
four fields — means docs left behind by an earlier run of this script, which wrote
only the three keyword fields, are still picked up. It throttles between bulk batches
so it doesn't starve live query serving. Safe to re-run and safe to interrupt.

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

from shared.categories import category_text, classify
from shared.config import ELASTICSEARCH_URL

INDEX = "osm_places"


async def backfill(
    batch_size: int,
    sleep_s: float,
    reprocess_all: bool,
    only_values: list[str] | None = None,
) -> None:
    es = AsyncElasticsearch(ELASTICSEARCH_URL)
    # `--only-values a,b` re-derives the category fields for docs of a given
    # category_value, regardless of whether they already have a category_text.
    # Needed when the *derivation* changes rather than the schema: those docs
    # already have a value, so the missing-only predicate below skips them, and
    # `--all` would rewrite all 20.9M (an hour, plus 9.5M fresh tombstones).
    # This is how `railway=subway_entrance` was retired from category_text —
    # 5,247 docs, seconds.
    if only_values:
        query = {"terms": {"category_value": only_values}}
        scanned = updated = 0
        actions: list[dict] = []
        try:
            async for doc in async_scan(
                es,
                index=INDEX,
                query={"query": query, "_source": ["tags", "admin_level"]},
                size=batch_size,
            ):
                scanned += 1
                src = doc.get("_source", {})
                tags = src.get("tags", {})
                cat = classify(tags, src.get("admin_level"))
                actions.append(
                    {
                        "_op_type": "update",
                        "_index": INDEX,
                        "_id": doc["_id"],
                        "retry_on_conflict": 3,
                        "doc": {
                            "category_key": cat.key or "",
                            "category_value": cat.value or "",
                            "category_group": cat.group or "",
                            "category_text": category_text(tags),
                        },
                    }
                )
                if len(actions) >= batch_size:
                    ok, errors = await async_bulk(es, actions, raise_on_error=False)
                    updated += ok
                    if errors:
                        print(f"  {len(errors)} error(s); first: {errors[0]}")
                    actions = []
                    if sleep_s:
                        await asyncio.sleep(sleep_s)
            if actions:
                ok, errors = await async_bulk(es, actions, raise_on_error=False)
                updated += ok
                if errors:
                    print(f"  {len(errors)} error(s); first: {errors[0]}")
        finally:
            await es.close()
        print(f"done (only-values={only_values}): scanned={scanned} updated={updated}")
        return

    # Missing-only = "has a type, but no category_text yet".
    #
    # Keyed on category_text (the newest field) rather than category_group, so
    # docs written by an earlier run — which wrote only the three keyword fields
    # — are still picked up. The category_key clause matters for both speed and
    # idempotency: only ~400k of 3M docs carry a type tag, and `category_text` is
    # empty for the rest. An empty string produces no tokens in a `text` field, so
    # `exists` stays false however many times we write it — without this clause
    # every run would rescan (and rewrite) all 3M docs forever.
    #
    # Edge case: a doc tagged *only* `healthcare=`/`cuisine=`/`station=` (which are
    # in CATEGORY_TEXT_KEYS but not CATEGORY_KEYS) has an empty category_key yet a
    # non-empty category_text. Rare — a lab or a subway platform almost always
    # carries an `amenity`/`railway` tag too. Use `--all` to sweep those in.
    query = (
        {"match_all": {}}
        if reprocess_all
        else {
            "bool": {
                "must_not": [
                    {"exists": {"field": "category_text"}},
                    {"term": {"category_key": ""}},
                ]
            }
        }
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
            tags = src.get("tags", {})
            cat = classify(tags, src.get("admin_level"))
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
                        "category_text": category_text(tags),
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
    ap.add_argument(
        "--only-values",
        type=str,
        default=None,
        help=(
            "comma-separated category_value list to re-derive even if they already "
            "have a category_text (use when the derivation changed, not the schema), "
            "e.g. --only-values subway_entrance,platform"
        ),
    )
    args = ap.parse_args()
    only = (
        [v.strip() for v in args.only_values.split(",") if v.strip()] if args.only_values else None
    )
    asyncio.run(backfill(args.batch_size, args.sleep, args.all, only))


if __name__ == "__main__":
    main()
