"""Redis-backed autocomplete index.

Stores top-K completions per prefix in Redis sorted sets for sub-3ms
lookups.  Supports optional geo-biased results via geohash-partitioned
keys.

Key layout
----------
``ac:{prefix}``
    Global sorted set keyed by 2-char and 3-char normalised prefixes.
    Members are JSON-encoded suggestion dicts.  Scores are computed as
    ``offline_rank * 10 + log1p(popularity) * 5``.

``ac:g:{geohash4}:{prefix}``
    Geo-partitioned sorted set.  Same structure but scoped to a
    ~20 km² geohash-4 cell.

``ac:meta:{osm_id}``
    Stores the current score and label for an entry so that popularity
    updates can adjust the sorted-set score without re-fetching from ES.
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

# How many top results to keep per prefix bucket
TOP_K = 30

# Minimum prefix length to index (edge_ngram min_gram=2 in ES)
MIN_PREFIX = 2

# Maximum prefix length to index
MAX_PREFIX = 6


# ── geohash helpers ──────────────────────────────────────────────────────

_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def encode_geohash(lat: float, lon: float, precision: int = 4) -> str:
    """Encode lat/lon into a geohash string of given precision."""
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    hash_chars: list[str] = []
    is_lon = True
    bit_count = 0
    current = 0

    total_bits = precision * 5
    for _ in range(total_bits):
        if is_lon:
            mid = (lon_range[0] + lon_range[1]) / 2
            if lon >= mid:
                current = current * 2 + 1
                lon_range[0] = mid
            else:
                current = current * 2
                lon_range[1] = mid
        else:
            mid = (lat_range[0] + lat_range[1]) / 2
            if lat >= mid:
                current = current * 2 + 1
                lat_range[0] = mid
            else:
                current = current * 2
                lat_range[1] = mid
        is_lon = not is_lon
        bit_count += 1

        if bit_count == 5:
            hash_chars.append(_BASE32[current])
            current = 0
            bit_count = 0

    return "".join(hash_chars)


# ── score computation ────────────────────────────────────────────────────

def compute_score(offline_rank: float, popularity: float) -> float:
    """Compute the sorted-set score for a suggestion.

    Higher score = shown first (ZREVRANGE).
    """
    return offline_rank * 10.0 + math.log1p(popularity) * 5.0


# ── prefix extraction ───────────────────────────────────────────────────

def _normalise(text: str) -> str:
    """Lowercase + strip for consistent prefix keys."""
    return text.strip().lower()


def _prefixes(text: str) -> list[str]:
    """Return the set of prefixes to index for a given text."""
    normed = _normalise(text)
    if not normed:
        return []
    result: list[str] = []
    for length in range(MIN_PREFIX, min(MAX_PREFIX + 1, len(normed) + 1)):
        result.append(normed[:length])
    return result


# ── suggestion dict ──────────────────────────────────────────────────────

def _build_suggestion(doc: dict[str, Any]) -> dict[str, Any]:
    """Build the compact JSON blob stored as the sorted-set member."""
    name = doc.get("name_en") or doc.get("name", "")
    addr = doc.get("full_address", "")
    if addr and addr != name:
        label = f"{name}, {addr}" if name else addr
    else:
        label = name

    return {
        "osm_id": doc.get("osm_id", ""),
        "label": label,
        "name": doc.get("name", ""),
        "name_en": doc.get("name_en", ""),
        "name_fr": doc.get("name_fr", ""),
        "centroid": doc.get("centroid"),
        "admin_level": doc.get("admin_level", 0),
    }


# ── index operations ────────────────────────────────────────────────────

async def index_entry(
    r: aioredis.Redis,
    doc: dict[str, Any],
    *,
    pipeline: aioredis.client.Pipeline | None = None,
) -> None:
    """Add or update a single document in the autocomplete index.

    Indexes into both global and geo-partitioned prefix sets.
    """
    osm_id = doc.get("osm_id", "")
    name = doc.get("name", "")
    name_en = doc.get("name_en", "")
    name_fr = doc.get("name_fr", "")
    offline_rank = float(doc.get("offline_rank", 0))
    popularity = float(doc.get("popularity", 0))

    score = compute_score(offline_rank, popularity)
    suggestion = _build_suggestion(doc)
    member = json.dumps(suggestion, ensure_ascii=False, separators=(",", ":"))

    # Compute geohash from centroid
    centroid = doc.get("centroid")
    geohash = None
    if centroid:
        lat = centroid.get("lat")
        lon = centroid.get("lon")
        if lat is not None and lon is not None:
            geohash = encode_geohash(lat, lon, precision=4)

    # Collect all text sources for prefix generation
    texts = set()
    if name:
        texts.add(name)
    if name_en:
        texts.add(name_en)
    if name_fr:
        texts.add(name_fr)
    addr_street = doc.get("addr_street", "")
    if addr_street:
        texts.add(addr_street)
    full_address = doc.get("full_address", "")
    if full_address:
        texts.add(full_address)

    # Also index individual words for multi-word queries
    all_words: set[str] = set()
    for text in texts:
        for word in text.split():
            normed = _normalise(word)
            if len(normed) >= MIN_PREFIX:
                all_words.add(normed)

    all_prefixes: set[str] = set()
    for text in texts:
        all_prefixes.update(_prefixes(text))
    for word in all_words:
        all_prefixes.update(_prefixes(word))

    p = pipeline or r.pipeline(transaction=False)

    # Store metadata for score updates (offline_rank needed to recompute score from new popularity)
    meta = json.dumps({
        "member": member,
        "score": score,
        "offline_rank": offline_rank,
        "prefixes": list(all_prefixes),
        "geohash": geohash,
    }, ensure_ascii=False, separators=(",", ":"))
    p.set(f"ac:meta:{osm_id}", meta)

    for prefix in all_prefixes:
        # Global bucket
        p.zadd(f"ac:{prefix}", {member: score})
        # Trim to top-K
        p.zremrangebyrank(f"ac:{prefix}", 0, -(TOP_K + 1))

        # Geo bucket
        if geohash:
            geo_key = f"ac:g:{geohash}:{prefix}"
            p.zadd(geo_key, {member: score})
            p.zremrangebyrank(geo_key, 0, -(TOP_K + 1))

    if pipeline is None:
        await p.execute()


async def update_score(
    r: aioredis.Redis,
    osm_id: str,
    new_popularity: float | None = None,
    boost: float | None = None,
) -> None:
    """Update the score for an existing entry after a popularity change.

    Either provide ``new_popularity`` (absolute) or ``boost`` (increment).
    """
    raw = await r.get(f"ac:meta:{osm_id}")
    if not raw:
        return

    meta = json.loads(raw)
    member = meta["member"]
    old_score = meta["score"]
    prefixes = meta["prefixes"]
    geohash = meta.get("geohash")

    # Parse current suggestion to get offline_rank
    suggestion = json.loads(member)

    # We don't store offline_rank in the suggestion, so back-derive it
    # from the old score: score = offline_rank * 10 + log1p(pop) * 5
    # For the boost case, we just add the boost * 5 (log1p scaling)
    if boost is not None:
        new_score = old_score + math.log1p(boost) * 5.0
    elif new_popularity is not None:
        # Recompute full score from stored offline_rank and the new absolute popularity.
        offline_rank = meta.get("offline_rank", 0.0)
        new_score = compute_score(offline_rank, new_popularity)
    else:
        return

    pipe = r.pipeline(transaction=False)

    # Update metadata
    meta["score"] = new_score
    pipe.set(
        f"ac:meta:{osm_id}",
        json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    )

    for prefix in prefixes:
        pipe.zadd(f"ac:{prefix}", {member: new_score})
        if geohash:
            pipe.zadd(f"ac:g:{geohash}:{prefix}", {member: new_score})

    await pipe.execute()


async def query(
    r: aioredis.Redis,
    text: str,
    limit: int = 7,
    lat: float | None = None,
    lon: float | None = None,
) -> list[dict[str, Any]]:
    """Look up autocomplete suggestions for a prefix.

    Returns up to ``limit`` suggestions sorted by score (highest first).
    When ``lat``/``lon`` are provided, merges geo-local results with
    global results, preferring local matches.
    """
    normed = _normalise(text)
    if len(normed) < MIN_PREFIX:
        # Too short for prefix lookup — pad or return empty
        if not normed:
            return []
        normed = normed.ljust(MIN_PREFIX)

    # Use the longest available prefix (up to MAX_PREFIX)
    prefix = normed[:MAX_PREFIX]
    # Try progressively shorter prefixes if the long one has no results
    candidates: list[str] = []
    for length in range(len(prefix), MIN_PREFIX - 1, -1):
        candidates.append(normed[:length])

    results: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    # If geo-bias requested, check geo-local bucket first
    if lat is not None and lon is not None:
        geohash = encode_geohash(lat, lon, precision=4)
        for pfx in candidates:
            geo_key = f"ac:g:{geohash}:{pfx}"
            raw_results = await r.zrevrange(geo_key, 0, limit - 1)
            if raw_results:
                for raw in raw_results:
                    entry = json.loads(raw)
                    oid = entry.get("osm_id", "")
                    if oid not in seen_ids:
                        seen_ids.add(oid)
                        entry["geo_local"] = True
                        results.append(entry)
                break

    # Fill remaining slots from global bucket
    remaining = limit - len(results)
    if remaining > 0:
        for pfx in candidates:
            global_key = f"ac:{pfx}"
            raw_results = await r.zrevrange(global_key, 0, remaining + 5 - 1)
            if raw_results:
                for raw in raw_results:
                    entry = json.loads(raw)
                    oid = entry.get("osm_id", "")
                    if oid not in seen_ids:
                        seen_ids.add(oid)
                        results.append(entry)
                        if len(results) >= limit:
                            break
                break

    return results[:limit]


async def warm_from_es(
    r: aioredis.Redis,
    es_client,
    index: str,
    batch_size: int = 500,
    max_docs: int | None = None,
) -> int:
    """Populate the Redis autocomplete index from Elasticsearch.

    Scans the ES index ordered by ``offline_rank`` descending, indexing
    the most important places first.  Call this at startup.

    Returns the number of documents indexed.
    """
    logger.info("[autocomplete] Warming Redis autocomplete from ES...")

    body: dict[str, Any] = {
        "size": batch_size,
        "query": {"match_all": {}},
        # _id tiebreaker ensures every sort position is unique so search_after
        # can paginate past groups of docs that share offline_rank=0/popularity=0.
        "sort": [{"offline_rank": "desc"}, {"popularity": "desc"}, {"_id": "asc"}],
        "_source": [
            "osm_id", "name", "name_en", "name_fr", "centroid",
            "admin_level", "offline_rank", "popularity",
            "full_address", "addr_street", "addr_city", "addr_country",
        ],
    }

    count = 0
    search_after = None

    while True:
        if search_after:
            body["search_after"] = search_after

        try:
            resp = await es_client.search(index=index, **body)
        except Exception as e:
            logger.error("[autocomplete] ES scroll error: %s", e)
            break

        hits = resp["hits"]["hits"]
        if not hits:
            break

        pipe = r.pipeline(transaction=False)

        for hit in hits:
            doc = hit["_source"]
            await index_entry(r, doc, pipeline=pipe)
            count += 1

        await pipe.execute()

        search_after = hits[-1]["sort"]

        if max_docs and count >= max_docs:
            break

        if count % 5000 == 0:
            logger.info("[autocomplete] Indexed %d documents...", count)

    logger.info("[autocomplete] Warm-up complete: %d documents indexed", count)
    return count
