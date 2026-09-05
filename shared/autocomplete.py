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
import re
from typing import Any

import redis.asyncio as aioredis

from shared.address import normalize_address_text
from shared.categories import CATEGORY_QUERY_TERMS
from shared.spec import load as _load_spec

_SPEC = _load_spec("autocomplete.toml")

# Fold punctuation to whitespace so `.split()` matches the ES `standard`
# tokenizer (which breaks "Al-Tahrir" into [al, tahrir]). \w is unicode-aware, so
# Arabic and accented letters survive.
_PUNCT_RE = re.compile(r"[^\w\s]+", re.UNICODE)
_WS_RE = re.compile(r"\s+")

logger = logging.getLogger(__name__)

# How many top results to keep per prefix bucket
TOP_K = _SPEC["top_k"]

# Minimum prefix length to index (edge_ngram min_gram=2 in ES)
MIN_PREFIX = _SPEC["min_prefix"]

# Maximum prefix length to index
MAX_PREFIX = _SPEC["max_prefix"]

# Ceiling on accumulated /feedback popularity. Single source of truth — the
# geocoder's /feedback handler imports this rather than defining its own.
POPULARITY_CAP = _SPEC["popularity_cap"]

# ── ranking weights ──────────────────────────────────────────────────────
# Match quality dominates: an exact name match must never be displaced by a
# merely-prominent partial one.  The tier gap (20 pts) is larger than what
# popularity alone can make up, but prominence + proximity together (max ~59)
# can promote across a single tier — which is what we want for landmarks.
W_MATCH = _SPEC["w_match"]
W_RANK = _SPEC["w_rank"]
W_POP = _SPEC["w_pop"]
W_GEO = _SPEC["w_geo"]

# Match tiers, highest first.
MQ_EXACT = _SPEC["mq_exact"]
MQ_PREFIX = _SPEC["mq_prefix"]
MQ_FIRST_WORD = _SPEC["mq_first_word"]
MQ_ANY_WORD = _SPEC["mq_any_word"]

# A Redis answer is only trusted when it is saturated *and* strongly matched;
# anything less defers to Elasticsearch, which has full corpus coverage.
MQ_STRONG = MQ_PREFIX

# Leading articles are noise for match tiering: "Al Tahrir" should score as a
# first-word match for "tahrir", not as a buried later-word match.
_ARTICLES = {"al", "el", "the", "la", "le", "los", "las", "ال"}

# Gaussian proximity decay — mirrors the ES `gauss` in the /autocomplete ES
# fallback (scale 15km, offset 2km, decay 0.5) so the two paths rank alike.
_GEO_SCALE_M = _SPEC["geo_scale_m"]
_GEO_OFFSET_M = _SPEC["geo_offset_m"]
_GEO_DECAY = _SPEC["geo_decay"]
_GEO_SIGMA_SQ = -(_GEO_SCALE_M**2) / (2.0 * math.log(_GEO_DECAY))


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
    """Normalise text for prefix keys and match comparison.

    Delegates to the same ``normalize_address_text`` used at ES index-time and
    query-time (tatweel/tashkeel stripping, alef+yaa+taa-marbuta folding,
    abbreviation expansion, NFKD ascii-folding), lowercases, then folds
    punctuation to spaces so that ``.split()`` tokenises the way the ES
    ``standard`` tokenizer does — otherwise "Al-Tahrir" stays a single token here
    while ES sees ``[al, tahrir]``, and a search for "tahrir" misses it.

    Using one normaliser for the Redis index, the Redis query and the ES path is
    what keeps them from drifting apart.  This was previously a bare
    ``.strip().lower()``, so Arabic and accented queries hashed to different
    buckets than ES resolved them to.

    NOTE: changing this changes every ``ac:`` bucket key, so the index must be
    dropped and re-warmed.
    """
    s = normalize_address_text(text or "").lower()
    s = _PUNCT_RE.sub(" ", s)
    return _WS_RE.sub(" ", s).strip()


# Category vocabulary run through the *same* normaliser as queries, so "مستشفى"
# (with taa marbuta) and "Café" match however the user types them.
_CATEGORY_TERMS: frozenset[str] = frozenset(
    n for n in (_normalise(t) for t in CATEGORY_QUERY_TERMS) if n
)


def is_category_query(text: str) -> bool:
    """True when the query names a place *type* ("metro", "مستشفى") not a place.

    Such queries must skip the Redis fast path.  Redis indexes names only, so for
    "metro" it finds the handful of shops literally called Metro, declares itself
    confident, and returns them — while all 88 Cairo Metro stations sit in
    Elasticsearch behind the ``category_text`` field, never consulted.

    Deliberately an exact match, not a prefix one: "metro" routes to ES but
    "metropolitan" stays on the fast path, since only a whole type word is
    evidence of a type query.
    """
    return _normalise(text) in _CATEGORY_TERMS


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres."""
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(h))


def _geo_decay(distance_m: float) -> float:
    """ES-equivalent gaussian decay in [0, 1]; 1.0 within the offset radius."""
    d = max(0.0, distance_m - _GEO_OFFSET_M)
    return math.exp(-(d * d) / (2.0 * _GEO_SIGMA_SQ))


def _strip_article(words: list[str]) -> list[str]:
    """Drop a leading article so "al tahrir" tiers as a first-word match."""
    if len(words) > 1 and words[0] in _ARTICLES:
        return words[1:]
    return words


def _match_quality(entry: dict[str, Any], normed_q: str) -> float | None:
    """How well does this suggestion actually match the query?

    Returns a tier in (0, 1], or ``None`` if the entry does not match at all.

    This is the post-filter that was missing.  Without it, ``query`` returned
    whatever sat in a truncated prefix bucket — answering "me" when the user
    typed "metro".
    """
    best: float | None = None

    for key in ("name", "name_en", "name_fr", "label"):
        raw = entry.get(key)
        if not raw:
            continue
        text = _normalise(raw)
        if not text:
            continue

        if text == normed_q:
            return MQ_EXACT
        if text.startswith(normed_q):
            best = max(best or 0.0, MQ_PREFIX)
            continue

        words = text.split()
        head = _strip_article(words)
        if head and head[0].startswith(normed_q):
            best = max(best or 0.0, MQ_FIRST_WORD)
            continue
        if any(w.startswith(normed_q) for w in words):
            best = max(best or 0.0, MQ_ANY_WORD)

    return best


def _rank_score(
    entry: dict[str, Any],
    match_quality: float,
    lat: float | None,
    lon: float | None,
) -> float:
    """Blend match quality, prominence and proximity into one sort key.

    The old index sorted purely on ``offline_rank*10 + log1p(pop)*5`` with no
    query term at all, which is why "Tabali Zamalek Delivery area 3" outranked
    "Zamalek".
    """
    score = W_MATCH * match_quality
    score += W_RANK * float(entry.get("offline_rank") or 0.0)
    score += W_POP * math.log1p(float(entry.get("popularity") or 0.0))

    if lat is not None and lon is not None:
        centroid = entry.get("centroid") or {}
        clat, clon = centroid.get("lat"), centroid.get("lon")
        if clat is not None and clon is not None:
            score += W_GEO * _geo_decay(_haversine_m(lat, lon, clat, clon))

    return score


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
        # Carried on the member so `query` can re-rank against the query text
        # without a second round-trip. Adding these changes the member JSON, so
        # the index must be dropped and re-warmed.
        "offline_rank": float(doc.get("offline_rank", 0) or 0),
        "popularity": float(doc.get("popularity", 0) or 0),
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

    # Also index individual words, so a query can match a place by any word of
    # its name, not just the first. Normalise the whole text *then* split —
    # `_normalise` folds punctuation to spaces, so normalising word-by-word would
    # turn "Al-Tahrir" into the single bogus token "al tahrir".
    all_words: set[str] = set()
    for text in texts:
        for word in _normalise(text).split():
            if len(word) >= MIN_PREFIX:
                all_words.add(word)

    all_prefixes: set[str] = set()
    for text in texts:
        all_prefixes.update(_prefixes(text))
    for word in all_words:
        all_prefixes.update(_prefixes(word))

    p = pipeline or r.pipeline(transaction=False)

    # Store metadata for score updates (offline_rank needed to recompute score from new popularity)
    meta = json.dumps(
        {
            "member": member,
            "score": score,
            "offline_rank": offline_rank,
            "popularity": popularity,
            "prefixes": list(all_prefixes),
            "geohash": geohash,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
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
    old_member = meta["member"]
    prefixes = meta["prefixes"]
    geohash = meta.get("geohash")
    offline_rank = float(meta.get("offline_rank", 0.0) or 0.0)
    old_popularity = float(meta.get("popularity", 0.0) or 0.0)

    # Always derive the new score through `compute_score`, never by patching the
    # old one. The previous `old_score + log1p(boost)*5` was a *different*
    # function from `compute_score`, so repeated /feedback calls drifted away
    # from the warm-time formula and inflated without bound — POPULARITY_CAP
    # was never applied on this path.
    if boost is not None:
        new_popularity = min(old_popularity + boost, POPULARITY_CAP)
    elif new_popularity is not None:
        new_popularity = min(new_popularity, POPULARITY_CAP)
    else:
        return

    new_score = compute_score(offline_rank, new_popularity)

    # The member JSON embeds `popularity` (the re-ranker reads it), so a score
    # change means the member string itself changes. ZADD alone would leave the
    # stale member behind as a duplicate — remove it first.
    suggestion = json.loads(old_member)
    suggestion["popularity"] = new_popularity
    new_member = json.dumps(suggestion, ensure_ascii=False, separators=(",", ":"))

    pipe = r.pipeline(transaction=False)

    meta["score"] = new_score
    meta["popularity"] = new_popularity
    meta["member"] = new_member
    pipe.set(
        f"ac:meta:{osm_id}",
        json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    )

    for prefix in prefixes:
        global_key = f"ac:{prefix}"
        if new_member != old_member:
            pipe.zrem(global_key, old_member)
        pipe.zadd(global_key, {new_member: new_score})
        if geohash:
            geo_key = f"ac:g:{geohash}:{prefix}"
            if new_member != old_member:
                pipe.zrem(geo_key, old_member)
            pipe.zadd(geo_key, {new_member: new_score})

    await pipe.execute()


async def query(
    r: aioredis.Redis,
    text: str,
    limit: int = 7,
    lat: float | None = None,
    lon: float | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    """Look up autocomplete suggestions for a prefix.

    Returns ``(suggestions, confident)``.

    ``confident`` is True only when Redis both **saturated** the request (found
    ``limit`` genuine matches) and matched *strongly* (the best hit is at least a
    whole-name prefix match).  The caller must fall through to Elasticsearch when
    it is False: a thin or weak Redis answer means the prefix bucket has hit its
    ``TOP_K``/100k-doc coverage ceiling, and ES — which indexes the whole corpus —
    will do better.

    Three things changed here versus the original implementation:

    1. **Post-filter.**  Candidates are checked against the *full* query, not the
       (≤6 char) bucket key they were fetched under.  The old code walked to
       progressively shorter prefixes and returned the first non-empty bucket
       verbatim, so ``q=metro`` silently answered the query ``me``.
    2. **No shorten-and-retry walk.**  It is dead weight once post-filtering: a
       shorter bucket is *more* crowded under the same ``TOP_K`` trim, so it can
       never hold an entry passing the full-query filter that the longer bucket
       lacks.  Read the longest bucket, or defer to ES.
    3. **Geo is a ranking signal, not a partition.**  Geo-local hits used to be
       blindly prepended above better global ones.  Now both pools are merged,
       filtered, and ranked together with a real distance decay.
    """
    normed = _normalise(text)

    # Below MIN_PREFIX there is no bucket to read (the old code padded with
    # `ljust`, producing keys like "ac:a " that are never written — so 1-char
    # queries always came back empty). Defer: ES `phrase_prefix` handles 1 char.
    if len(normed) < MIN_PREFIX:
        return [], False

    # The longest bucket that exists for this query. Longer key = less crowded.
    bucket = normed[:MAX_PREFIX]

    keys = [f"ac:{bucket}"]
    if lat is not None and lon is not None:
        geohash = encode_geohash(lat, lon, precision=4)
        # The geo bucket is a genuine recall source: a local place can be trimmed
        # out of the global top-30 yet still be present in its own cell.
        keys.append(f"ac:g:{geohash}:{bucket}")

    pipe = r.pipeline(transaction=False)
    for key in keys:
        # Over-fetch: post-filtering will discard some, and we rank the survivors.
        pipe.zrevrange(key, 0, TOP_K - 1)
    buckets = await pipe.execute()

    geo_ids: set[str] = set()
    candidates: dict[str, dict[str, Any]] = {}

    for key, raw_entries in zip(keys, buckets, strict=True):
        is_geo = key.startswith("ac:g:")
        for raw in raw_entries:
            try:
                entry = json.loads(raw)
            except (ValueError, TypeError):
                continue
            oid = entry.get("osm_id", "")
            if not oid:
                continue
            if is_geo:
                geo_ids.add(oid)
            candidates.setdefault(oid, entry)

    scored: list[tuple[float, dict[str, Any]]] = []
    best_quality = 0.0

    for oid, entry in candidates.items():
        quality = _match_quality(entry, normed)
        if quality is None:
            continue  # ← the post-filter: does not actually match the query
        best_quality = max(best_quality, quality)
        entry["geo_local"] = oid in geo_ids
        scored.append((_rank_score(entry, quality, lat, lon), entry))

    scored.sort(key=lambda pair: pair[0], reverse=True)
    hits = [entry for _, entry in scored[:limit]]

    confident = len(hits) >= limit and best_quality >= MQ_STRONG
    return hits, confident


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
        # osm_id (an indexed keyword) is the search_after tiebreaker so every
        # sort position is unique, letting us paginate past large groups of docs
        # that share offline_rank=0/popularity=0. NOTE: do NOT sort on `_id` here
        # — ES 8 disallows fielddata on the _id field, which aborts the scroll
        # (the warm-up would index 0 docs and autocomplete falls back to ES).
        "sort": [{"offline_rank": "desc"}, {"popularity": "desc"}, {"osm_id": "asc"}],
        "_source": [
            "osm_id",
            "name",
            "name_en",
            "name_fr",
            "centroid",
            "admin_level",
            "offline_rank",
            "popularity",
            "full_address",
            "addr_street",
            "addr_city",
            "addr_country",
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
