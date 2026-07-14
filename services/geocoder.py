"""Geocoding HTTP service (FastAPI).

Endpoints
---------
GET  /geocode       - full search via Elasticsearch (text + vector + geo + ranking)
GET  /address       - structured address search (housenumber, street, city, postcode)
POST /feedback      - popularity feedback loop      (boosts future ranking)
GET  /reverse       - reverse geocoding via PostGIS + Elasticsearch
                      (nearest line + nearest address + enclosing polygons)
POST /insert        - insert OSM element by publishing to NATS stream (matches watcher.py format)
POST /places        - add a new place to the geocoding database
GET  /describe      - on-demand AI title + description for a place (blocks until ready)
GET  /health        - dependency health check (ES, PostGIS, NATS, Redis, Ollama)
GET  /features      - feature flags discovery

Query-string flags shared by search endpoints:
  vector=true    enable semantic / AI vector search  (requires ENABLE_VECTORS=true)
  vector=false   disable vector search (text-only)
  ai=true        enable AI-assisted search (requires ENABLE_AI=true)
  ai=false       disable AI features
  describe=true  include AI-generated title + description per result (cached)
  describe=false (default) omit AI descriptions

Online ranking formula: text_similarity × (offline_rank + geo_decay + popularity) via boost_mode=multiply

Address search
--------------
  /address?q=123+Main+Street,Cairo           structured forward address lookup
  /address?q=Main+Street&city=Cairo          field-level restrictor params
  /address?q=Nile+Corniche&postcode=11511    postcode filter

/reverse now also returns ``nearest_address`` – the closest building-level
address point stored in the osm_addresses PostGIS table.
"""

import asyncio
import json
import logging
import os
import socket
import sys
import time
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from itertools import zip_longest

import asyncpg
import nats
import redis.asyncio as aioredis
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

# ── structured logger ─────────────────────────────────────────────────────
_logger = logging.getLogger("geocoder.access")
_logger.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("%(message)s"))
_logger.addHandler(_handler)
_logger.propagate = False

# General-purpose diagnostics logger (distinct from the access logger above).
from shared.logging import get_logger

logger = get_logger("geocoder")

import httpx

from services import nearby
from services.cache_service import ResultCache
from services.enrichment import (
    address_needs_refresh,
    enrich_address,
    schedule_enrichment,
)
from services.geocoder_helpers import (
    _distance_confidence,
    _element_to_geocode_result,
    _haversine_m,
    _interpolated_to_result,
    _normalize_confidence,
    _street_token_match,
    _text_should_full,
    _text_should_lean,
)
from services.geocoder_models import (
    InsertMessage,
    PlaceCreate,
    PlaceResponse,
    ProbeBatch,
    ProbePing,
)
from shared.address import (
    is_address_query,
    normalize_address_text,
    parse_address_query,
)
from shared.autocomplete import (
    index_entry as ac_index_entry,
)
from shared.autocomplete import (
    query as ac_query,
)
from shared.autocomplete import (
    update_score as ac_update_score,
)
from shared.autocomplete import (
    warm_from_es as ac_warm_from_es,
)
from shared.autocomplete import (
    POPULARITY_CAP as _POPULARITY_CAP,
)
from shared.autocomplete import (
    is_category_query as ac_is_category_query,
)
from shared.categories import classify
from shared.config import (
    ELASTICSEARCH_URL,
    ENABLE_AI,
    ENABLE_DEEP,
    ENABLE_TRAFFIC,
    ENABLE_VECTORS,
    GEOCODE_CACHE_COORD_PRECISION,
    GEOCODE_CACHE_ENABLED,
    GEOCODE_CACHE_TTL,
    GOOGLE_MAPS_API_KEY,
    NATS_SUBJECT,
    NATS_URL,
    OLLAMA_MODEL,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    REDIS_HOST,
    REDIS_PORT,
    TRAFFIC_SUBJECT,
    VALHALLA_URL,
)
from shared.embeddings import embed_texts
from shared.google_maps import (
    GoogleMapsError,
    map_place_to_element,
    map_result_to_element,
)
from shared.google_maps import (
    forward_geocode as gmaps_forward,
)
from shared.google_maps import (
    nearby_search as gmaps_nearby,
)
from shared.google_maps import (
    reverse_geocode as gmaps_reverse,
)
from shared.interpolation import (
    _parse_housenumber,
    interpolate_address,
    reverse_interpolate,
)
from shared.llm import generate_description, is_ollama_available, warm_up_model
from shared.nats_client import TRAFFIC_STREAM_CFG, ensure_stream
from shared.redis_client import make_redis_async

INDEX = "osm_places"

# Optimized-effort tuning. The rescore window bounds how many top hits the
# (expensive) function_score is applied to.
_RESCORE_WINDOW = 200


# Confidence scoring + street-token matching moved to services/geocoder_helpers.py
# (imported above as _normalize_confidence, _distance_confidence,
# _STREET_GENERIC_TOKENS, _street_token_match).

from shared.es_mapping import MAPPING as ES_MAPPING

es: AsyncElasticsearch = None  # type: ignore[assignment]
pg_pool: asyncpg.Pool = None  # type: ignore[assignment]
nc = None  # type: ignore[assignment]
js = None  # type: ignore[assignment]
redis_pool: aioredis.Redis = None  # type: ignore[assignment]
# Cache-aside result cache for /geocode and /reverse. Stays None (disabled) until
# Redis connects at startup; endpoints guard on `result_cache is not None`.
result_cache: ResultCache | None = None
_ac_task: asyncio.Task | None = None  # strong reference to prevent GC


def _schedule_enrichment(
    osm_id: str,
    centroid: dict | None,
    self_area: float = 0.0,
    admin_level: int | None = None,
) -> None:
    """Background address enrichment — thin wrapper over services.enrichment that
    binds this module's live ES/PostGIS clients."""
    schedule_enrichment(pg_pool, es, INDEX, osm_id, centroid, self_area, admin_level)


_AC_MAX_DOCS = 100_000  # top-ranked docs to keep in Redis
_AC_BATCH_SIZE = 2_000  # ES fetch size per round-trip
_AC_REWARM_SECS = 600  # re-warm interval (10 minutes)
# Serve /autocomplete from the Redis prefix index when it is confident. Set false
# to route every query to Elasticsearch — a kill switch, and the control arm when
# A/B-ing the fast path's effect on recall.
_AC_REDIS_FAST_PATH = os.getenv("AC_REDIS_FAST_PATH", "true").lower() != "false"


async def _warm_autocomplete():
    """Populate and periodically refresh the Redis autocomplete index from ES.

    Only indexes the top _AC_MAX_DOCS documents by offline_rank so the
    warm-up completes in seconds rather than minutes on large datasets.
    """
    if redis_pool is None:
        return
    warmer_id = f"{socket.gethostname()}:{os.getpid()}"
    while True:
        sleep_secs = 60
        try:
            # Fleet-wide coordination so N workers/replicas don't each rescan ES:
            #  * cadence  — a shared "last_warm" timestamp gates *when* a warm is due
            #  * exclusion — a short "warming" lease lets exactly ONE instance warm
            # The lease TTL is short (not the whole 10-min cycle), so a crashed
            # holder frees it within ~3 min instead of stalling warm-up post-deploy.
            last = await redis_pool.get("geocoder:ac:last_warm")
            due = last is None or (time.time() - float(last)) >= _AC_REWARM_SECS
            if due:
                got = await redis_pool.set(
                    "geocoder:ac:warming",
                    warmer_id,
                    nx=True,
                    ex=180,
                )
                if got:
                    try:
                        count = await ac_warm_from_es(
                            redis_pool,
                            es,
                            INDEX,
                            batch_size=_AC_BATCH_SIZE,
                            max_docs=_AC_MAX_DOCS,
                        )
                        # Only advance the cadence clock once ES actually returned
                        # docs, so a not-yet-ready ES retries soon instead of in 10m.
                        if count > 0:
                            await redis_pool.set("geocoder:ac:last_warm", repr(time.time()))
                            sleep_secs = _AC_REWARM_SECS
                        else:
                            sleep_secs = 30
                        logger.info(
                            f"[geocoder] Autocomplete warm-up complete: {count} docs indexed"
                        )
                    finally:
                        await redis_pool.delete("geocoder:ac:warming")
        except asyncio.CancelledError:
            logger.info("[geocoder] Autocomplete warm-up task cancelled")
            raise
        except Exception as e:
            logger.error(f"[geocoder] Autocomplete warm-up failed: {e}")
            sleep_secs = 30
        await asyncio.sleep(sleep_secs)


async def _warm_ollama():
    """Background task: pre-load the Ollama model to avoid cold-start delays."""
    ok = await warm_up_model()
    if ok:
        logger.info("[geocoder] Ollama model pre-loaded successfully")
    else:
        logger.error(
            "[geocoder] Ollama model warm-up failed (descriptions may be slow on first call)"
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global es, pg_pool, nc, js, redis_pool, _ac_task, result_cache

    # Retry logic for connecting to dependencies
    max_retries = 10
    retry_delay = 2

    # Connect to Elasticsearch
    for attempt in range(max_retries):
        try:
            es = AsyncElasticsearch(ELASTICSEARCH_URL)
            await es.ping()
            logger.info("[geocoder] Successfully connected to Elasticsearch")
            break
        except Exception as e:
            logger.error(
                f"[geocoder] Failed to connect to Elasticsearch (attempt {attempt + 1}/{max_retries}): {e}"
            )
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                raise

    # Connect to PostGIS
    for attempt in range(max_retries):
        try:
            pg_pool = await asyncpg.create_pool(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                min_size=2,
                max_size=10,
            )
            logger.info("[geocoder] Successfully connected to PostGIS")
            # Hand the shared ES + PostGIS clients to the /nearby router (avoids an
            # import cycle — nearby.py must not import services.geocoder). Done here
            # (not in the ES block) so pg_pool exists before /nearby serves traffic.
            nearby.init(es, pg_pool)
            break
        except Exception as e:
            logger.error(
                f"[geocoder] Failed to connect to PostGIS (attempt {attempt + 1}/{max_retries}): {e}"
            )
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                raise

    # Connect to NATS
    for attempt in range(max_retries):
        try:
            nc = await nats.connect(NATS_URL)
            js = nc.jetstream()
            logger.info("[geocoder] Successfully connected to NATS")
            # Ensure the traffic probe stream exists so /traffic/probe(s) can
            # publish even before the aggregator has started.
            if ENABLE_TRAFFIC:
                try:
                    await ensure_stream(js, TRAFFIC_STREAM_CFG)
                    logger.info("[geocoder] Ensured TRAFFIC probe stream")
                except Exception as te:
                    logger.warning(f"[geocoder] Warning: could not ensure TRAFFIC stream: {te}")
            break
        except Exception as e:
            logger.error(
                f"[geocoder] Failed to connect to NATS (attempt {attempt + 1}/{max_retries}): {e}"
            )
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                raise

    # ensure ES index exists
    try:
        if not await es.indices.exists(index=INDEX):
            await es.indices.create(index=INDEX, **ES_MAPPING)
            logger.info(f"[geocoder] Created ES index {INDEX}")
        else:
            # Additive mapping sync so category_* (and any future scalar field) is
            # present for /nearby even when the index predates it. No-op if there.
            await es.indices.put_mapping(
                index=INDEX, properties=ES_MAPPING["mappings"]["properties"]
            )
    except Exception as e:
        logger.error(f"[geocoder] Error checking/creating ES index: {e}")
        try:
            await es.indices.create(index=INDEX, **ES_MAPPING)
            logger.info(f"[geocoder] Created ES index {INDEX} (fallback)")
        except Exception as e2:
            if "resource_already_exists" not in str(e2).lower():
                raise
            logger.info(f"[geocoder] ES index {INDEX} already exists (concurrent create)")

    # Connect to Redis and warm autocomplete index
    try:
        redis_pool = make_redis_async(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=5,
        )
        await redis_pool.ping()
        logger.info(f"[geocoder] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

        # Result cache (cache-aside) for /geocode + /reverse — offloads ES on
        # repeated queries. Fail-open: a Redis error just becomes a cache miss.
        result_cache = ResultCache(
            redis_pool,
            enabled=GEOCODE_CACHE_ENABLED,
            ttl=GEOCODE_CACHE_TTL,
            coord_precision=GEOCODE_CACHE_COORD_PRECISION,
        )
        logger.info(
            f"[geocoder] Result cache enabled={result_cache.enabled} ttl={GEOCODE_CACHE_TTL}s"
        )

        # Warm autocomplete index from ES in background (store ref to prevent GC)
        _ac_task = asyncio.create_task(_warm_autocomplete())
    except Exception as e:
        logger.error(f"[geocoder] Redis connection failed: {e}")
        logger.info("[geocoder] Autocomplete will fall back to Elasticsearch")
        redis_pool = None  # type: ignore[assignment]

    # Warm up Ollama model so the first /describe request doesn't pay cold-start cost
    asyncio.create_task(_warm_ollama())

    yield
    if _ac_task and not _ac_task.done():
        _ac_task.cancel()
        try:
            await _ac_task
        except asyncio.CancelledError:
            pass
    await es.close()
    await pg_pool.close()
    await nc.close()
    if redis_pool:
        await redis_pool.aclose()


app = FastAPI(title="Geocoding Service", lifespan=lifespan)


@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    """Log every request with method, path, latency, and status code."""
    start = time.monotonic()
    request_id = uuid.uuid4().hex[:12]
    request.state.request_id = request_id

    try:
        response = await call_next(request)
    except Exception:
        latency_ms = round((time.monotonic() - start) * 1000, 1)
        _logger.info(
            json.dumps(
                {
                    "ts": datetime.now(UTC).isoformat(),
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "query": str(request.url.query),
                    "status": 500,
                    "latency_ms": latency_ms,
                }
            )
        )
        raise

    latency_ms = round((time.monotonic() - start) * 1000, 1)
    log_entry = {
        "ts": datetime.now(UTC).isoformat(),
        "request_id": request_id,
        "method": request.method,
        "path": request.url.path,
        "query": str(request.url.query),
        "status": response.status_code,
        "latency_ms": latency_ms,
    }
    # Include result count for search endpoints (stored by handlers)
    result_count = getattr(request.state, "result_count", None)
    if result_count is not None:
        log_entry["result_count"] = result_count

    _logger.info(json.dumps(log_entry))
    return response


# ── address enrichment ────────────────────────────────────────────────────
async def _enrich_address(
    osm_id: str,
    centroid: dict | None,
    self_area: float = 0.0,
    admin_level: int | None = None,
) -> dict | None:
    """Address enrichment — thin wrapper over services.enrichment that binds
    this module's live ES/PostGIS clients (e.g. /deep/forward's sync path)."""
    return await enrich_address(pg_pool, es, INDEX, osm_id, centroid, self_area, admin_level)


# ── feature flags discovery ──────────────────────────────────────────────
@app.get("/features")
async def features():
    """Return which optional features are enabled on this instance."""
    return {
        "vectors": ENABLE_VECTORS,
        "ai": ENABLE_AI,
        "traffic": ENABLE_TRAFFIC,
    }


# ── health check ─────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    """Check connectivity to all backend dependencies.

    Returns HTTP 200 when all critical services (Elasticsearch, PostGIS)
    are reachable, or 503 if any critical dependency is down.
    Non-critical services (Redis, NATS) are reported but don't fail the check.
    """
    checks: dict[str, dict] = {}

    # Elasticsearch (critical)
    try:
        info = await es.info()
        checks["elasticsearch"] = {
            "status": "ok",
            "version": info["version"]["number"],
        }
    except Exception as e:
        checks["elasticsearch"] = {"status": "error", "detail": str(e)}

    # PostGIS (critical)
    try:
        async with pg_pool.acquire() as conn:
            version = await conn.fetchval("SELECT version()")
        checks["postgis"] = {"status": "ok", "version": version}
    except Exception as e:
        checks["postgis"] = {"status": "error", "detail": str(e)}

    # NATS (non-critical — only used for /insert and /places)
    try:
        if nc and nc.is_connected:
            checks["nats"] = {"status": "ok"}
        else:
            checks["nats"] = {"status": "error", "detail": "disconnected"}
    except Exception as e:
        checks["nats"] = {"status": "error", "detail": str(e)}

    # Redis (non-critical — powers autocomplete + watcher node cache)
    try:
        if redis_pool is not None:
            await redis_pool.ping()
            ac_keys = await redis_pool.dbsize()
            if isinstance(ac_keys, dict):  # cluster client: per-node counts
                ac_keys = sum(ac_keys.values())
            checks["redis"] = {"status": "ok", "autocomplete_keys": ac_keys}
        else:
            r = make_redis_async(host=REDIS_HOST, port=REDIS_PORT, socket_connect_timeout=2)
            await r.ping()
            checks["redis"] = {"status": "ok", "autocomplete": "not_initialised"}
            await r.aclose()
    except Exception as e:
        checks["redis"] = {"status": "error", "detail": str(e)}

    # Overall status: fail only if critical services are down
    critical_ok = all(checks[svc]["status"] == "ok" for svc in ("elasticsearch", "postgis"))
    overall = "healthy" if critical_ok else "degraded"
    status_code = 200 if critical_ok else 503

    # Ollama (non-critical — only used for AI descriptions)
    ollama_ok = await is_ollama_available()
    checks["ollama"] = (
        {"status": "ok", "model": OLLAMA_MODEL}
        if ollama_ok
        else {
            "status": "error",
            "detail": "unreachable",
        }
    )

    return JSONResponse(
        status_code=status_code,
        content={
            "status": overall,
            "checks": checks,
        },
    )


# ── address interpolation helper ──────────────────────────────────────────
async def _resolve_street_names(
    street: str,
    lat: float | None,
    lon: float | None,
    limit: int = 15,
) -> list[str]:
    """Resolve a (possibly cross-language) street name to the name string(s) the
    data actually stores for that street, via ES.

    An English "Tahrir Street" resolves to the matching street way's names
    (``شارع التحرير`` / "Al Tahrir Street"), so interpolation can gather that
    street's Arabic-tagged address points by exact ``street`` name — which an
    English string could never match directly.  Restricted to ways/relations
    (streets are lines, not POIs' parent nodes) and, when coordinates are given,
    geo-filtered so we don't pull in same-named streets from other cities.

    The original query string is always included as a fallback so same-language
    queries keep working even if ES resolution finds nothing.
    """
    names: list[str] = [street] if street else []
    if not street:
        return names

    # operator "and": every query token must be present, so we resolve the actual
    # street (cross-language via name/name_en) and don't fuzzy-drift onto an
    # unrelated nearby street ("التسعين الشمالى" must not match "الرمالي").
    must = [
        {
            "multi_match": {
                "query": street,
                "fields": ["name^3", "name_en^3", "name_fr^3"],
                "type": "best_fields",
                "operator": "and",
                "fuzziness": "AUTO",
                "prefix_length": 1,
            }
        }
    ]
    flt: list[dict] = [{"terms": {"osm_type": ["way", "relation"]}}]
    if lat is not None and lon is not None:
        # Wide metro radius: tight enough to drop same-named streets in other
        # governorates, loose enough to still resolve far districts (New Cairo,
        # 6th October) that a downtown-biased query would otherwise miss.
        flt.append({"geo_distance": {"distance": "60km", "centroid": {"lat": lat, "lon": lon}}})
    body = {
        "size": limit,
        "query": {"bool": {"must": must, "filter": flt}},
        "_source": ["name", "name_en", "name_fr"],
    }
    try:
        resp = await es.search(index=INDEX, **body)
    except Exception as e:
        logger.error(f"[geocoder] Street resolution failed for {street!r}: {e}")
        return names

    seen = {n.lower() for n in names}
    for h in resp["hits"]["hits"]:
        src = h["_source"]
        for f in ("name", "name_en", "name_fr"):
            v = (src.get(f) or "").strip()
            if v and v.lower() not in seen:
                seen.add(v.lower())
                names.append(v)
    return names


# ── AI place descriptions ────────────────────────────────────────────────
async def _get_or_generate_description(osm_id: str, *, wait: bool = False) -> dict[str, str] | None:
    """Return cached AI description or generate one.

    If ``wait=True`` (used by /describe), blocks until generation completes.
    If ``wait=False`` (used by ?describe=true), returns the cached value
    immediately and fires a background task for cache misses.

    Returns the description dict or None.
    """
    # 1. Check ES cache
    try:
        doc = await es.get(
            index=INDEX,
            id=osm_id,
            _source_includes=[
                "ai_description",
                "name",
                "name_en",
                "name_fr",
                "tags",
                "centroid",
                "full_address",
                "addr_housenumber",
                "addr_street",
                "addr_city",
                "addr_suburb",
                "addr_state",
                "addr_postcode",
                "addr_country",
                "admin_level",
            ],
        )
        src = doc["_source"]
        cached = src.get("ai_description")
        if cached:
            return cached
    except Exception:
        return None

    if not wait:
        # Fire background generation — caller gets None for now
        asyncio.create_task(_generate_and_cache(osm_id, src))
        return None

    # Synchronous path: generate, cache, return
    return await _generate_and_cache(osm_id, src)


async def _generate_and_cache(osm_id: str, place_data: dict) -> dict[str, str] | None:
    """Call Ollama, cache the result in ES, return the description."""
    desc = await generate_description(place_data)
    if desc is None:
        return None

    # Cache in ES
    try:
        await es.update(index=INDEX, id=osm_id, body={"doc": {"ai_description": desc}})
    except Exception as e:
        logger.error(f"[geocoder] Failed to cache ai_description for {osm_id}: {e}")

    return desc


async def _attach_descriptions(results: list[dict]) -> None:
    """Attach cached AI descriptions to search results, fire background
    generation for misses.  Mutates ``results`` in place."""
    ids = [r.get("osm_id") for r in results if r.get("osm_id")]
    if not ids:
        return

    _source_fields = [
        "ai_description",
        "name",
        "name_en",
        "name_fr",
        "tags",
        "centroid",
        "full_address",
        "addr_housenumber",
        "addr_street",
        "addr_city",
        "addr_suburb",
        "addr_state",
        "addr_postcode",
        "addr_country",
        "admin_level",
    ]

    try:
        resp = await es.mget(
            index=INDEX,
            body={"ids": ids},
            source_includes=_source_fields,
        )
        docs_by_id = {d["_id"]: d["_source"] for d in resp["docs"] if d.get("found")}
    except Exception:
        for result in results:
            result["ai_description"] = None
        return

    for result in results:
        osm_id = result.get("osm_id")
        if not osm_id:
            continue
        src = docs_by_id.get(osm_id)
        if not src:
            result["ai_description"] = None
            continue
        cached = src.get("ai_description")
        if cached:
            result["ai_description"] = cached
        else:
            result["ai_description"] = None
            asyncio.create_task(_generate_and_cache(osm_id, src))


@app.get("/describe")
async def describe(
    osm_id: str = Query(..., description="OSM ID of the place to describe"),
):
    """Generate (or return cached) AI title + description for a place.

    Blocks until the description is ready.  Result is cached in
    Elasticsearch for subsequent requests.
    """
    desc = await _get_or_generate_description(osm_id, wait=True)
    if desc is None:
        raise HTTPException(
            status_code=404, detail="Place not found or description generation failed"
        )
    return {"osm_id": osm_id, **desc}


# ── text recall clauses (effort-dependent) ───────────────────────────────
# ── geocode (Elasticsearch) ──────────────────────────────────────────────
@app.get("/geocode")
async def geocode(
    request: Request,
    q: str = Query(..., min_length=1),
    lat: float | None = Query(None),
    lon: float | None = Query(None),
    limit: int = Query(10, ge=1, le=50),
    offset: int = Query(
        0,
        ge=0,
        le=9950,
        description="Result offset for pagination/scrolling (from + size must stay ≤ 10000)",
    ),
    vector: bool = Query(True, description="Enable semantic vector search"),
    ai: bool = Query(True, description="Enable AI-assisted search"),
    describe: bool = Query(
        False, description="Include AI-generated descriptions (cached where available)"
    ),
    effort: str = Query(
        "optimized",
        pattern="^(high|optimized)$",
        description="Scoring effort: 'high' (full fuzzy recall + per-doc "
        "function_score over all matches) or 'optimized' (lean "
        "fuzzy + function_score applied via rescore over top hits "
        "+ no exact hit counting, far cheaper for ES under load).",
    ),
):
    """Full geocoding search.

    Online ranking = text_similarity × function_score(offline_rank, geo, popularity).
    Uses boost_mode=multiply so text relevance gates ranking — a high-importance
    element with a weak text match cannot outscore a lower-rank exact match.

    Text similarity searches across name, name_en, name_fr, and tags_text (all tags)
    with phrase and exact-match boosting for multi-word queries.
    vector=true adds KNN cosine similarity re-ranking (requires ENABLE_VECTORS).
    ai=true enables AI-assisted query expansion (requires ENABLE_AI).
    """
    use_vectors = vector and ENABLE_VECTORS
    use_ai = ai and ENABLE_AI
    optimized = effort == "optimized"

    # ── result cache (cache-aside): call cache first, fall back to ES ──────
    # Skip the cache for AI paths (describe / ai) — those are heavier and less
    # repeated, and describe mutates ES-side caches. Everything else is cached.
    cache_key = None
    if result_cache is not None and result_cache.enabled and not describe and not use_ai:
        cache_key = result_cache.key(
            "geocode",
            q=ResultCache.norm_q(q),
            lat=result_cache.coord(lat),
            lon=result_cache.coord(lon),
            limit=limit,
            offset=offset,
            vector=int(use_vectors),
            effort=effort,
        )
        cached = await result_cache.get(cache_key)
        if cached is not None:
            request.state.cache_hit = True
            request.state.result_count = -1  # sentinel: served from cache
            return Response(
                content=cached,
                media_type="application/json",
                headers={"X-Cache": "HIT"},
            )

    loop = asyncio.get_running_loop()

    # ---- function_score query ----
    # final_score = text_score × (baseline + offline_rank_boost + geo_decay + popularity)
    functions: list[dict] = []

    # baseline: ensures function_score is at least 1.0 (preserves text score)
    functions.append(
        {
            "weight": 1.0,
        }
    )

    # offline_rank boost — log-compressed so importance acts as a TIE-BREAKER
    # between comparable text matches rather than a dominant term.  With
    # boost_mode=multiply, a linear factor let a high-importance suburb
    # (rank ~2.0 → ×5.0) outscore a better-matching POI (rank ~1.0 → ×3.0),
    # flipping relevance on its head.  log1p keeps the spread tight
    # (rank 2.0 → ×~1.7, rank 1.0 → ×~1.4) so text relevance leads.
    functions.append(
        {
            "field_value_factor": {
                "field": "offline_rank",
                "modifier": "log1p",
                "factor": 1,
                "missing": 0,
            },
            "weight": 1.5,
        }
    )

    # geo-distance decay (closer = higher score)
    if lat is not None and lon is not None:
        functions.append(
            {
                "gauss": {
                    "centroid": {
                        "origin": {"lat": lat, "lon": lon},
                        "scale": "10km",
                        "offset": "1km",
                        "decay": 0.5,
                    }
                },
                "weight": 2,
            }
        )

    # popularity boost (feedback-driven, capped at 1000 via /feedback endpoint)
    functions.append(
        {
            "field_value_factor": {
                "field": "popularity",
                "modifier": "log1p",
                "factor": 1,
                "missing": 0,
            },
            "weight": 1,
        }
    )

    # Normalize query for better Arabic/abbreviation matching
    q_norm = normalize_address_text(q)

    # ── Auto address detection & decomposition ────────────────────────────
    # Always attempt to detect and decompose the query into address components.
    # This drives two things:
    #   1. Additional address-specific should clauses for scoring
    #   2. A response field showing the decomposition to the client
    addr_detected = is_address_query(q)
    parsed_addr: dict = {}
    if addr_detected:
        parsed_addr = parse_address_query(q)

    # Base text recall clauses. 'high' effort casts a wide fuzzy net across all
    # fields with a closeness gradient (exact > near-exact > broad fuzzy);
    # 'optimized' uses a leaner set that keeps the match set — and thus the
    # per-doc scoring cost — small.  Address-specific clauses are appended to
    # either base below.
    should_clauses: list[dict] = (
        _text_should_lean(q_norm) if optimized else _text_should_full(q_norm)
    )

    # ── Address-specific search layers (active when address detected) ─────
    if addr_detected:
        # Layer 1: Cross-field search across ALL address fields simultaneously.
        # This is the key improvement — "Tahrir Cairo" will match documents where
        # addr_street contains "Tahrir" AND addr_city contains "Cairo" even though
        # neither field alone contains both words.
        should_clauses.append(
            {
                "multi_match": {
                    "query": q_norm,
                    "fields": [
                        "addr_street^5",
                        "addr_street.autocomplete^2",
                        "addr_city^3",
                        "addr_city.autocomplete^1.5",
                        "addr_suburb^2",
                        "addr_suburb.autocomplete",
                        "full_address^3",
                        "full_address.autocomplete^1.5",
                    ],
                    "type": "cross_fields",
                    "operator": "or",
                    "minimum_should_match": "75%",
                    "boost": 4,
                }
            }
        )

        # Layer 2: Phrase match on full_address — rewards documents where the
        # query appears as a contiguous phrase in the stored address string
        should_clauses.append(
            {"match_phrase": {"full_address": {"query": q_norm, "boost": 6, "slop": 2}}}
        )

        # Layer 3: Component-specific targeted matching from the decomposition.
        # Each parsed component gets its own clause with high precision.
        if parsed_addr.get("street"):
            street = parsed_addr["street"]
            # Phrase match: "شارع التحرير" or "Main Street" as ordered tokens
            should_clauses.append(
                {"match_phrase": {"addr_street": {"query": street, "boost": 10, "slop": 1}}}
            )
            # Fuzzy token match: tolerates typos in street name
            should_clauses.append(
                {"match": {"addr_street": {"query": street, "fuzziness": "AUTO", "boost": 5}}}
            )
            # Autocomplete: prefix matching for partial street names
            should_clauses.append(
                {"match": {"addr_street.autocomplete": {"query": street, "boost": 3}}}
            )
            # Also match street name against the place name field (many streets
            # are indexed as named ways/linestrings with the street in "name")
            should_clauses.append(
                {"match_phrase": {"name": {"query": street, "boost": 4, "slop": 1}}}
            )

        if parsed_addr.get("housenumber"):
            hn = parsed_addr["housenumber"].lower()
            if parsed_addr.get("street"):
                # When a street is also given, a bare housenumber is just a digit
                # shared by thousands of unrelated addresses — only reward it when
                # the street matches too, so "10 Wrong Street" can't impersonate a
                # confident "10 Tahrir Street". A street-only match still scores via
                # the dedicated street clauses above.
                should_clauses.append(
                    {
                        "bool": {
                            "must": [
                                {"term": {"addr_housenumber": {"value": hn}}},
                                {
                                    "match_phrase": {
                                        "addr_street": {"query": parsed_addr["street"], "slop": 1}
                                    }
                                },
                            ],
                            "boost": 50,
                        }
                    }
                )
            else:
                # No street context: a bare housenumber term match is all we have.
                should_clauses.append({"term": {"addr_housenumber": {"value": hn, "boost": 15}}})

        if parsed_addr.get("city"):
            city_val = parsed_addr["city"]
            # City match boosts results in the right locality
            should_clauses.append({"match": {"addr_city": {"query": city_val, "boost": 3}}})
            should_clauses.append(
                {"match": {"addr_city.autocomplete": {"query": city_val, "boost": 1.5}}}
            )
            # Also check name field — cities themselves are named places
            should_clauses.append({"match": {"name": {"query": city_val, "boost": 2}}})

        if parsed_addr.get("suburb"):
            suburb_val = parsed_addr["suburb"]
            should_clauses.append({"match": {"addr_suburb": {"query": suburb_val, "boost": 2}}})
            should_clauses.append(
                {"match": {"addr_suburb.autocomplete": {"query": suburb_val, "boost": 1}}}
            )

        if parsed_addr.get("postcode"):
            should_clauses.append(
                {"term": {"addr_postcode": {"value": parsed_addr["postcode"], "boost": 6}}}
            )

        if parsed_addr.get("country"):
            should_clauses.append(
                {"term": {"addr_country": {"value": parsed_addr["country"], "boost": 4}}}
            )

        # Layer 4: Boost documents that have address data (they're more likely
        # to be what the user wants when searching for an address)
        should_clauses.append({"term": {"has_address": {"value": True, "boost": 2}}})

    text_query: dict = {
        "bool": {
            "should": should_clauses,
            "minimum_should_match": 1,
        }
    }

    # ── Housenumber proximity boost ───────────────────────────────────────
    # When the user specifies a housenumber, add a script_score function
    # that gives higher scores to documents whose housenumber is numerically
    # closer to the requested one.  This ensures that when an exact match
    # doesn't exist, the nearest house numbers rank first.
    #
    # When a street is also given, scope this to docs on that street via a
    # function filter — "nearest number" is only meaningful on the right
    # street, otherwise #10 on any random street gets a proximity bonus.
    if addr_detected and parsed_addr.get("housenumber"):
        try:
            requested_hn = int(parsed_addr["housenumber"])
            proximity_fn = {
                "script_score": {
                    "script": {
                        "source": (
                            "if (doc['addr_housenumber'].size() == 0) { return 0; } "
                            "try { "
                            "  long hn = Long.parseLong(doc['addr_housenumber'].value); "
                            "  double diff = Math.abs(hn - params.requested_hn); "
                            "  return 1.0 / (1.0 + diff); "
                            "} catch (NumberFormatException e) { return 0; }"
                        ),
                        "params": {"requested_hn": requested_hn},
                    }
                },
                "weight": 5,
            }
            if parsed_addr.get("street"):
                proximity_fn["filter"] = {
                    "match_phrase": {"addr_street": {"query": parsed_addr["street"], "slop": 1}}
                }
            functions.append(proximity_fn)
        except ValueError:
            pass  # non-numeric housenumber, skip proximity scoring

    function_score_query = {
        "function_score": {
            "query": text_query,
            "functions": functions,
            "score_mode": "sum",
            "boost_mode": "multiply",
        }
    }

    body: dict = {"size": limit, "from": offset}
    if optimized:
        # Two-phase scoring: phase 1 retrieves on the cheap text bool; phase 2
        # applies the (expensive) function_score only to the top window rather
        # than to every matching doc — the main lever against search-pool
        # stalls. The window is widened to cover the requested page so deep
        # offsets still get scored.  Exact hit counting is also skipped, since
        # it forces a full match-set traversal we don't need for ranking.
        body["query"] = text_query
        body["rescore"] = {
            "window_size": max(_RESCORE_WINDOW, offset + limit),
            "query": {
                "rescore_query": function_score_query,
                "query_weight": 0,
                "rescore_query_weight": 1,
            },
        }
        body["track_total_hits"] = False
    else:
        body["query"] = function_score_query

    # optional vector KNN (when enabled)
    if use_vectors:
        vec = (await loop.run_in_executor(None, embed_texts, [q]))[0]
        body["knn"] = {
            "field": "name_vector",
            "query_vector": vec,
            "k": limit * 2,
            "num_candidates": 300,
        }

    resp = await es.search(index=INDEX, **body)

    # Build results; schedule background enrichment for any uncached address.
    max_score = resp["hits"].get("max_score") or 0
    # In optimized mode track_total_hits=false, so ES omits hits.total entirely.
    total_obj = resp["hits"].get("total")
    total_hits = total_obj.get("value") if total_obj else None
    results = []

    for h in resp["hits"]["hits"]:
        src = h["_source"]
        result = {
            "osm_id": src["osm_id"],
            "osm_type": src.get("osm_type", ""),
            "name": src.get("name", ""),
            "name_en": src.get("name_en", ""),
            "name_fr": src.get("name_fr", ""),
            "tags": src.get("tags", {}),
            "tags_text": src.get("tags_text", ""),
            "geom": src.get("geom"),
            "centroid": src.get("centroid"),
            "admin_level": src.get("admin_level", 0),
            "area_km2": src.get("area_km2", 0),
            "offline_rank": src.get("offline_rank", 0),
            "popularity": src.get("popularity", 0),
            "confidence": _normalize_confidence(h["_score"], max_score),
            # structured address fields (present when element has addr:* tags)
            "full_address": src.get("full_address", ""),
            "addr_housenumber": src.get("addr_housenumber", ""),
            "addr_street": src.get("addr_street", ""),
            "addr_city": src.get("addr_city", ""),
            "addr_postcode": src.get("addr_postcode", ""),
            "addr_country": src.get("addr_country", ""),
            "addr_suburb": src.get("addr_suburb", ""),
            "addr_state": src.get("addr_state", ""),
            # reverse-geocoded address enrichment (computed on demand)
            "address": src.get("address"),
        }
        results.append(result)

        # Parents/nearest-street are computed once and cached in ES. If this doc
        # isn't cached yet, kick off enrichment in the BACKGROUND (compute-once,
        # write-back) instead of blocking the response on heavy PostGIS spatial
        # joins. The value is served from ES on subsequent searches. This keeps
        # the search hot path free of synchronous spatial-join fan-out.
        self_area = src.get("area_km2", 0) or 0
        self_admin = src.get("admin_level")
        if address_needs_refresh(result["address"], src["osm_id"], self_area, self_admin):
            _schedule_enrichment(src["osm_id"], src.get("centroid"), self_area, self_admin)

    # ── Address interpolation fallback ────────────────────────────────────
    # When the query contained a housenumber but no result matched it
    # exactly, try to interpolate the position from known addresses on
    # the same street.
    # Only attempt interpolation on the first page — it synthesises a single
    # top result, which is meaningless when the client is scrolling deeper.
    interpolated_result = None
    if offset == 0 and addr_detected and parsed_addr.get("housenumber"):
        try:
            req_hn = int(parsed_addr["housenumber"])
            req_street = parsed_addr.get("street", "")
            # An "exact" hit must match the housenumber AND be on the requested
            # street — otherwise "10 Some Other Street" would wrongly suppress
            # interpolation along the street the user actually asked for.
            # Compare housenumbers by parsed integer ("8 أ", "16A" → 8, 16) so a
            # real hit with a suffixed number still counts as exact.
            has_exact = any(
                _parse_housenumber(r.get("addr_housenumber")) == req_hn
                and _street_token_match(req_street, r.get("addr_street", ""))
                for r in results
            )
            if not has_exact:
                # Resolve the (possibly English) street name to the name string(s)
                # the data stores (e.g. Arabic ``شارع التحرير``), so interpolation
                # can gather that street's address points by exact name.  Coupled
                # with the user's lat/lon, this disambiguates between the many
                # same-named streets across the city.
                street_names = await _resolve_street_names(req_street, lat, lon)
                # Cross-lingual exact guard: the real address may already be in the
                # results but tagged in another language (English query vs Arabic
                # addr_street). If so, don't insert a synthesized result above it.
                name_set = {n.lower() for n in street_names}
                has_exact = any(
                    _parse_housenumber(r.get("addr_housenumber")) == req_hn
                    and (r.get("addr_street") or "").lower() in name_set
                    for r in results
                )
            if not has_exact:
                near = (lat, lon) if (lat is not None and lon is not None) else None
                ia = await interpolate_address(
                    pg_pool,
                    req_hn,
                    street=req_street,
                    city=parsed_addr.get("city"),
                    street_names=street_names,
                    near=near,
                )
                if ia:
                    # If the gather found the exact address point, it carries a
                    # real osm_id. Skip insertion when that doc is already in the
                    # results — don't surface a duplicate or shove the real hit
                    # down. Genuinely interpolated points (osm_id None) always go in.
                    existing_ids = {r.get("osm_id") for r in results if r.get("osm_id")}
                    if not (ia.osm_id and ia.osm_id in existing_ids):
                        interpolated_result = _interpolated_to_result(ia)
                        results.insert(0, interpolated_result)
        except (ValueError, TypeError):
            pass

    # Attach AI descriptions when requested
    if describe and ENABLE_AI:
        await _attach_descriptions(results)

    request.state.result_count = len(results)
    response_body = {
        "features": {
            "vectors_enabled": use_vectors,
            "ai_enabled": use_ai,
            "effort": effort,
        },
        # Address decomposition: shows what the system understood from the query
        "address_detected": addr_detected,
        "address_parsed": parsed_addr if addr_detected else None,
        # Pagination/scroll metadata. `total` is ES's match count (may be
        # approximate above 10k); `has_more` tells the client whether another
        # page exists.  Scroll by re-querying with offset += limit.
        "pagination": {
            "limit": limit,
            "offset": offset,
            # total is null in optimized mode (exact counting disabled); fall
            # back to "a full page came back" to decide has_more.
            "total": total_hits,
            "has_more": (offset + len(resp["hits"]["hits"]) < total_hits)
            if total_hits is not None
            else len(resp["hits"]["hits"]) >= limit,
        },
        "results": results,
    }
    # Cache miss just computed the answer — store the serialized body so the next
    # identical request is a single Redis GET (never reaches ES).
    # X-Cache header lets a client see HIT/MISS (BYPASS = cache not applicable,
    # e.g. describe/ai or cache disabled); it propagates out through APISIX/NPM.
    if cache_key is not None:
        jr = JSONResponse(content=response_body, headers={"X-Cache": "MISS"})
        await result_cache.set(cache_key, jr.body)
        return jr
    return JSONResponse(content=response_body, headers={"X-Cache": "BYPASS"})


# ── autocomplete ──────────────────────────────────────────────────────────


@app.get("/autocomplete")
async def autocomplete(
    request: Request,
    q: str = Query(..., min_length=1, description="Partial query text"),
    lat: float | None = Query(None, description="Latitude for geo-bias"),
    lon: float | None = Query(None, description="Longitude for geo-bias"),
    limit: int = Query(7, ge=1, le=20, description="Max suggestions"),
):
    """Fast prefix-based autocomplete, Redis fast path over an Elasticsearch base.

    Designed for keystroke-by-keystroke suggestions.  The Redis prefix index
    (~1-3 ms) answers only when it is *confident*: it found ``limit`` genuine
    matches for the full query and matched at least at whole-name-prefix strength.
    Anything less — a thin bucket, a weak match, a category query like "metro"
    that no name prefix satisfies — defers to the Elasticsearch edge-ngram query
    below, which indexes the whole corpus (Redis holds only the top 100k by
    ``offline_rank``) and can additionally match on category.

    This gate is load-bearing.  Redis used to answer whenever it returned
    *anything*, and because it silently truncated the query to a 2-char prefix it
    always returned something — so ES was never consulted and ``q=metro`` came
    back with MEDITOWN and Mena Garden City.

    Ranking blends match quality, ``offline_rank``, ``popularity`` (updated via
    ``/feedback``) and, when ``lat``/``lon`` are given, a gaussian distance decay
    matching the ES one.
    """
    # ── Redis autocomplete (fast path — only when confident) ────────────
    # AC_REDIS_FAST_PATH=false forces every query to Elasticsearch. Kept as a
    # kill switch (Redis index stale or evicted → turn it off, lose latency not
    # correctness) and as the control arm for A/B-ing the fast path's effect on
    # recall with tests/run_autocomplete_recall.py.
    # Is the user naming a place, or a *kind* of place? Decided once, and it
    # drives two things: whether Redis may answer at all, and how hard the ES
    # query leans on `category_text`.
    is_category = ac_is_category_query(q)

    # A type query ("metro", "مستشفى") must skip Redis: the prefix index holds
    # names only, so it would confidently answer with the Metro supermarket chain
    # and never surface a station. Only ES sees `category_text`.
    if redis_pool is not None and _AC_REDIS_FAST_PATH and not is_category:
        try:
            redis_hits, confident = await ac_query(
                redis_pool, q, limit=limit, lat=lat, lon=lon
            )
            if confident:
                request.state.result_count = len(redis_hits)
                return {"source": "redis", "results": redis_hits}
        except Exception as e:
            logger.error(f"[geocoder] Redis autocomplete error, falling back to ES: {e}")

    # ── Elasticsearch edge-ngram autocomplete (fallback) ────────────────
    q_norm = normalize_address_text(q)

    # Matches the place's NAME. Deliberately free of `category_text` — see below.
    name_query: dict = {
        "bool": {
            "should": [
                {
                    "multi_match": {
                        "query": q_norm,
                        "fields": [
                            "name.autocomplete^5",
                            "name_en.autocomplete^5",
                            "name_fr.autocomplete^5",
                            "addr_street.autocomplete^3",
                            "addr_city.autocomplete^2",
                            "full_address.autocomplete^2",
                        ],
                        "type": "best_fields",
                    }
                },
                {
                    "multi_match": {
                        "query": q_norm,
                        "fields": ["name^8", "name_en^8", "name_fr^8"],
                        "type": "phrase_prefix",
                    }
                },
                # WHOLE-WORD match. The two clauses above are prefix-based, so they
                # score "Metro" and "Metrosport" alike and let `offline_rank` break
                # the tie — which is how a query for "metro" near Toronto returned
                # Metrosport, MetroCentre and Metrogate while the 61 shops named
                # exactly "Metro" sat below them. A `phrase` match hits the *token*
                # "metro" (present in "Metro" and "Metro Market", absent from
                # "Metrosport", which analyses to the single token "metrosport"), so
                # it separates "your query IS this name" from "your query is a
                # prefix of this name". Measured: exact-"Metro" in top-4 near
                # Vaughan goes 0/4 → 4/4, Cairo stays 4/4.
                #
                # Contributes nothing to a mid-typing prefix like "metr" (no such
                # token), so it never hurts keystroke recall — it only breaks ties
                # once a whole word has been typed.
                {
                    "multi_match": {
                        "query": q_norm,
                        "fields": ["name^10", "name_en^10", "name_fr^10"],
                        "type": "phrase",
                    }
                },
            ],
            "minimum_should_match": 1,
        }
    }

    # Matches the place's TYPE ("metro", "مستشفى", "pharmacy").
    type_query: dict = {"match": {"category_text.autocomplete": q_norm}}

    functions: list[dict] = [
        {"weight": 1.0},
        {
            "field_value_factor": {
                "field": "offline_rank",
                "modifier": "none",
                "factor": 1,
                "missing": 0,
            },
            "weight": 2,
        },
        {
            "field_value_factor": {
                "field": "popularity",
                "modifier": "log1p",
                "factor": 1,
                "missing": 0,
            },
            "weight": 1.5,
        },
    ]

    if lat is not None and lon is not None:
        # TWO decays, doing two different jobs. The index is global (43.8M docs),
        # and a single decay cannot serve both:
        #
        #   Regional (300km, weight 25) — kills continental distance. Without it,
        #   `offline_rank` (weight 2, ranging to ~14) let a far prominent place beat
        #   a near relevant one: "metro" biased to Cairo returned Metrotown and
        #   Metro Vancouver Regional District, 9,200 km away; "bank" returned
        #   Bankview and Bankfield. At a 300km scale this term is essentially FLAT
        #   across a single city, so it discriminates between continents without
        #   disturbing ranking within one.
        #
        #   Local (15km, weight 3) — the fine-grained "near me" bias.
        #
        # Why not just raise the local decay's weight? Measured on the production
        # index: 15km@25 fixes category (68%→92%) but craters named recall
        # (strict@1 48%→37%), because it ranks by "closest to the bias point"
        # rather than "best name match" — and a searched-for place is rarely at the
        # exact point you biased from. The split keeps both: strict@1 45%,
        # category 98%.
        #
        # Only added when lat/lon are supplied; coordinate-free global search is
        # unaffected.
        functions.append(
            {
                "gauss": {
                    "centroid": {
                        "origin": {"lat": lat, "lon": lon},
                        "scale": "300km",
                        "offset": "50km",
                        "decay": 0.5,
                    }
                },
                "weight": 25,
            }
        )
        functions.append(
            {
                "gauss": {
                    "centroid": {
                        "origin": {"lat": lat, "lon": lon},
                        "scale": "15km",
                        "offset": "2km",
                        "decay": 0.5,
                    }
                },
                "weight": 3,
            }
        )

    _SOURCE = [
        "osm_id",
        "osm_type",
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
    ]

    def _body(query: dict) -> dict:
        return {
            "size": limit,
            "query": {
                "function_score": {
                    "query": query,
                    "functions": functions,
                    "score_mode": "sum",
                    "boost_mode": "multiply",
                }
            },
            "_source": _SOURCE,
        }

    if not is_category:
        # A name query. One list, ranked as before.
        resp = await es.search(index=INDEX, **_body(name_query))
        hits = resp["hits"]["hits"]
        max_score = resp["hits"].get("max_score") or 0
    else:
        # A TYPE query, and the word is usually ambiguous *by locale*: "metro" is
        # the subway in Cairo and a grocery chain in Canada (61 shops named exactly
        # "Metro" within 50km of Toronto; 5 near Cairo). Both readings are correct
        # and the server cannot know which the user meant.
        #
        # A single ranked list cannot express "some of each". Scoring both
        # populations together and tuning one `category_text` boost produces a
        # CLIFF, not a gradient — measured on the production index, q=metro, top-7:
        #
        #     boost 1-8 :  Cairo 0 stations/7 named   Toronto 0 stations/7 named
        #     boost 20  :  Cairo 4 stations/2 named   Toronto 7 stations/1 named
        #
        # Nothing in between blends them, because the two populations' score
        # distributions don't interleave — a multiplier just shifts one wholesale
        # past the other. So rank them SEPARATELY and merge with a quota.
        m_resp = await es.msearch(
            index=INDEX,
            searches=[{}, _body(name_query), {}, _body(type_query)],
        )
        name_hits, type_hits = (r["hits"]["hits"] for r in m_resp["responses"])
        max_score = max(
            (r["hits"].get("max_score") or 0) for r in m_resp["responses"]
        )

        # Interleave, starting with the name list (the literal reading of what was
        # typed). Dedupe by osm_id — a place can be in both lists (Vaughan
        # Metropolitan Centre is a station *named* "Metro…"), and it keeps its
        # better slot. If one list runs short, the other fills the page.
        hits = []
        seen: set[str] = set()
        for pair in zip_longest(name_hits, type_hits):
            for h in pair:
                if h is None:
                    continue
                oid = h["_source"].get("osm_id")
                if oid in seen:
                    continue
                seen.add(oid)
                hits.append(h)
        hits = hits[:limit]

    results = []
    for h in hits:
        src = h["_source"]
        name = src.get("name_en") or src.get("name", "")
        addr = src.get("full_address", "")
        if addr and addr != name:
            label = f"{name}, {addr}" if name else addr
        else:
            label = name

        results.append(
            {
                "osm_id": src.get("osm_id"),
                "label": label,
                "name": src.get("name", ""),
                "name_en": src.get("name_en", ""),
                "name_fr": src.get("name_fr", ""),
                "centroid": src.get("centroid"),
                "admin_level": src.get("admin_level", 0),
                "confidence": _normalize_confidence(h["_score"], max_score),
            }
        )

    request.state.result_count = len(results)
    return {"source": "elasticsearch", "results": results}


# ── feedback loop ─────────────────────────────────────────────────────────


@app.post("/feedback")
async def feedback(
    osm_id: str = Query(...),
    boost: float = Query(1.0, ge=0.1, le=10.0),
):
    """Increment popularity for an element in Elasticsearch and Redis.

    The boost value is clamped to [0.1, 10.0] and popularity is capped
    at 1000 to prevent unbounded growth or abuse.

    Also updates the Redis autocomplete index so that popular results
    rank higher in real-time without waiting for a full re-warm.
    """

    try:
        await es.update(
            index=INDEX,
            id=osm_id,
            body={
                "script": {
                    "source": "ctx._source.popularity = Math.min(ctx._source.popularity + params.boost, params.max_pop)",
                    "params": {"boost": boost, "max_pop": _POPULARITY_CAP},
                }
            },
        )
    except Exception as e:
        logger.error(f"[geocoder] Failed to update popularity for {osm_id}: {e}")

    # Update Redis autocomplete score in background
    if redis_pool is not None:
        try:
            await ac_update_score(redis_pool, osm_id, boost=boost)
        except Exception:
            logger.debug("Failed to update autocomplete score for %s", osm_id, exc_info=True)

    return {"status": "ok", "osm_id": osm_id}


# ── address search (Elasticsearch) ───────────────────────────────────────────
@app.get("/address")
async def address_search(
    request: Request,
    q: str = Query(..., min_length=1, description="Address query string"),
    lat: float | None = Query(None, description="Latitude for proximity boost"),
    lon: float | None = Query(None, description="Longitude for proximity boost"),
    limit: int = Query(10, ge=1, le=50),
    postcode: str | None = Query(None, description="Restrict results to postal code"),
    city: str | None = Query(None, description="Restrict results to city/town"),
    country: str | None = Query(None, description="Restrict to ISO country code (e.g. EG)"),
    describe: bool = Query(
        False, description="Include AI-generated descriptions (cached where available)"
    ),
):
    """Structured address search powered by Elasticsearch.

    Supports free-form queries like:
    - ``"123 Tahrir Street, Cairo"``  → parsed into housenumber + street + city
    - ``"Tahrir Street, Zamalek"``    → street + suburb/city
    - ``"شارع التحرير, القاهرة"``     → Arabic address
    - ``"ش التحرير"``                 → abbreviated Arabic
    - ``"11511"``                     → postcode lookup
    - ``"Cairo Tower"``               → name fallback (no addr:* required)

    Use the ``postcode``, ``city``, ``country`` query params to add hard filters
    on top of the free-text query.

    Results are ordered by: text relevance × (offline_rank + geo proximity).
    Searches addr:* fields first, then falls back to name/full-text if no
    address-specific results are found.
    """
    q_norm = normalize_address_text(q)
    parsed = parse_address_query(q)

    # Explicit query params override parsed components
    if postcode:
        parsed["postcode"] = postcode
    if city:
        parsed["city"] = city
    if country:
        parsed["country"] = country.upper()

    must_clauses: list[dict] = []
    should_clauses: list[dict] = []
    filter_clauses: list[dict] = []

    # ── hard filters ─────────────────────────────────────────────────────
    if parsed.get("postcode"):
        must_clauses.append({"term": {"addr_postcode": parsed["postcode"]}})

    if parsed.get("country"):
        filter_clauses.append({"term": {"addr_country": parsed["country"]}})

    # City as hard filter only when supplied as explicit query param
    if city and parsed.get("city"):
        filter_clauses.append(
            {"match": {"addr_city": {"query": parsed["city"], "operator": "and"}}}
        )

    # ── scored should clauses ────────────────────────────────────────────

    # 1. Cross-field search: the full normalized query across all address fields
    #    This lets "Tahrir Cairo" match street=Tahrir + city=Cairo
    should_clauses.append(
        {
            "multi_match": {
                "query": q_norm,
                "fields": [
                    "addr_street^5",
                    "addr_street.autocomplete^2",
                    "addr_city^3",
                    "addr_city.autocomplete",
                    "addr_suburb^2",
                    "addr_suburb.autocomplete",
                    "full_address^2",
                    "full_address.autocomplete",
                ],
                "type": "cross_fields",
                "operator": "or",
                "minimum_should_match": "75%",
            }
        }
    )

    # 2. Phrase match on full_address for exact ordering boost
    should_clauses.append(
        {"match_phrase": {"full_address": {"query": q_norm, "boost": 8, "slop": 1}}}
    )

    # 3. Parsed component-specific boosts (higher precision)
    if parsed.get("street"):
        # Exact phrase match on street
        should_clauses.append(
            {"match_phrase": {"addr_street": {"query": parsed["street"], "boost": 10, "slop": 1}}}
        )
        # Fuzzy token match on street
        should_clauses.append(
            {
                "match": {
                    "addr_street": {
                        "query": parsed["street"],
                        "fuzziness": "AUTO",
                        "boost": 4,
                    }
                }
            }
        )
        # Autocomplete match on street
        should_clauses.append(
            {
                "match": {
                    "addr_street.autocomplete": {
                        "query": parsed["street"],
                        "boost": 2,
                    }
                }
            }
        )

    if parsed.get("housenumber"):
        hn_val = parsed["housenumber"].lower()
        if parsed.get("street"):
            # With a street in play, only reward the housenumber when the street
            # matches too — a bare "10" on the wrong street must not look like an
            # exact address match.
            should_clauses.append(
                {
                    "bool": {
                        "must": [
                            {"term": {"addr_housenumber": {"value": hn_val}}},
                            {
                                "match_phrase": {
                                    "addr_street": {"query": parsed["street"], "slop": 1}
                                }
                            },
                        ],
                        "boost": 50,
                    }
                }
            )
        else:
            # No street context: a bare housenumber term match is all we have.
            should_clauses.append({"term": {"addr_housenumber": {"value": hn_val, "boost": 15}}})

    # City as boost (when not already a hard filter)
    if parsed.get("city") and not city:
        should_clauses.append({"match": {"addr_city": {"query": parsed["city"], "boost": 3}}})
        should_clauses.append(
            {"match": {"addr_city.autocomplete": {"query": parsed["city"], "boost": 1}}}
        )

    if parsed.get("suburb"):
        should_clauses.append({"match": {"addr_suburb": {"query": parsed["suburb"], "boost": 2}}})

    # 4. Name fallback — search POI/place names so "Cairo Tower" still works
    should_clauses.append(
        {
            "multi_match": {
                "query": q_norm,
                "fields": [
                    "name^3",
                    "name.autocomplete^1",
                    "name_en^3",
                    "name_en.autocomplete^1",
                    "name_fr^3",
                    "name_fr.autocomplete^1",
                ],
                "type": "best_fields",
                "fuzziness": "AUTO",
                "boost": 2,
            }
        }
    )

    # 5. Tags-text fallback for broad matching
    should_clauses.append({"match": {"tags_text": {"query": q_norm, "boost": 0.5}}})

    # ── Prefer results with addr:* data but don't exclude others ─────────
    # Instead of filtering has_address=true, we boost it
    should_clauses.append({"term": {"has_address": {"value": True, "boost": 3}}})

    text_query: dict = {
        "bool": {
            "must": must_clauses,
            "should": should_clauses,
            "filter": filter_clauses,
            "minimum_should_match": 1,
        }
    }

    # ── scoring functions ────────────────────────────────────────────────
    functions: list[dict] = [{"weight": 1.0}]

    # offline_rank: use linear (no log compression) so main streets clearly
    # outrank minor POIs in address context
    functions.append(
        {
            "field_value_factor": {
                "field": "offline_rank",
                "modifier": "none",
                "factor": 1,
                "missing": 0,
            },
            "weight": 1.5,
        }
    )

    # popularity boost
    functions.append(
        {
            "field_value_factor": {
                "field": "popularity",
                "modifier": "log1p",
                "factor": 1,
                "missing": 0,
            },
            "weight": 0.5,
        }
    )

    if lat is not None and lon is not None:
        # Tight geo decay for address search (1 km scale)
        functions.append(
            {
                "gauss": {
                    "centroid": {
                        "origin": {"lat": lat, "lon": lon},
                        "scale": "1km",
                        "offset": "100m",
                        "decay": 0.5,
                    }
                },
                "weight": 5,
            }
        )

    # ── Housenumber proximity boost ─────────────────────────────────────
    # Scoped to the requested street (when given) so "nearest number" only
    # applies on the right street, not to #10 on any random street.
    if parsed.get("housenumber"):
        try:
            requested_hn = int(parsed["housenumber"])
            proximity_fn = {
                "script_score": {
                    "script": {
                        "source": (
                            "if (doc['addr_housenumber'].size() == 0) { return 0; } "
                            "try { "
                            "  long hn = Long.parseLong(doc['addr_housenumber'].value); "
                            "  double diff = Math.abs(hn - params.requested_hn); "
                            "  return 1.0 / (1.0 + diff); "
                            "} catch (NumberFormatException e) { return 0; }"
                        ),
                        "params": {"requested_hn": requested_hn},
                    }
                },
                "weight": 5,
            }
            if parsed.get("street"):
                proximity_fn["filter"] = {
                    "match_phrase": {"addr_street": {"query": parsed["street"], "slop": 1}}
                }
            functions.append(proximity_fn)
        except ValueError:
            pass

    body: dict = {
        "size": limit,
        "query": {
            "function_score": {
                "query": text_query,
                "functions": functions,
                "score_mode": "sum",
                "boost_mode": "multiply",
            }
        },
    }

    resp = await es.search(index=INDEX, **body)

    max_score = resp["hits"].get("max_score") or 0
    addr_results = resp["hits"]["hits"]
    results = [
        {
            "osm_id": h["_source"]["osm_id"],
            "osm_type": h["_source"].get("osm_type", ""),
            "name": h["_source"].get("name", ""),
            "name_en": h["_source"].get("name_en", ""),
            "name_fr": h["_source"].get("name_fr", ""),
            "full_address": h["_source"].get("full_address", ""),
            "addr_housenumber": h["_source"].get("addr_housenumber", ""),
            "addr_street": h["_source"].get("addr_street", ""),
            "addr_city": h["_source"].get("addr_city", ""),
            "addr_postcode": h["_source"].get("addr_postcode", ""),
            "addr_country": h["_source"].get("addr_country", ""),
            "addr_suburb": h["_source"].get("addr_suburb", ""),
            "addr_state": h["_source"].get("addr_state", ""),
            "centroid": h["_source"].get("centroid"),
            "geom": h["_source"].get("geom"),
            "offline_rank": h["_source"].get("offline_rank", 0),
            "confidence": _normalize_confidence(h["_score"], max_score),
        }
        for h in addr_results
    ]

    # ── Address interpolation fallback ────────────────────────────────────
    if parsed.get("housenumber"):
        try:
            req_hn = int(parsed["housenumber"])
            req_street = parsed.get("street", "")
            # Exact match requires the housenumber AND the requested street, so a
            # wrong-street number match doesn't suppress interpolation.
            has_exact = any(
                r.get("addr_housenumber") == str(req_hn)
                and _street_token_match(req_street, r.get("addr_street", ""))
                for r in results
            )
            if not has_exact:
                ia = await interpolate_address(
                    pg_pool,
                    req_hn,
                    street=req_street,
                    city=parsed.get("city") or city,
                )
                if ia:
                    results.insert(0, _interpolated_to_result(ia))
        except (ValueError, TypeError):
            pass

    if describe and ENABLE_AI:
        await _attach_descriptions(results)

    request.state.result_count = len(results)
    return {
        "query": q,
        "normalized": q_norm,
        "parsed": parsed,
        "results": results,
    }


# ── Pydantic models for place management ───────────────────────────────────────
# PlaceCreate / InsertMessage / PlaceResponse moved to services/geocoder_models.py


# ── insert endpoint (matches watcher.py message format) ───────────────────────
@app.post("/insert")
async def insert(message: InsertMessage):
    """Insert an OSM element by publishing it to NATS stream.

    This endpoint accepts the exact same message format that watcher.py publishes,
    allowing you to manually insert OSM elements that will be processed by the
    es-inserter and postgis-inserter services.

    Message format matches watcher.py:
    {
        "osm_id": "n123" | "w456" | "r789",
        "osm_type": "node" | "way" | "relation",
        "tags": {"name": "...", "addr:street": "...", ...},
        "geom": {"type": "Point|LineString|Polygon|MultiPolygon", "coordinates": [...]},
        "admin_level": 0-10,
        "area_km2": 0.0
    }
    """
    try:
        # Convert to dict and publish to NATS stream
        message_dict = message.model_dump()
        msg_json = json.dumps(message_dict).encode()
        ack = await js.publish(NATS_SUBJECT, msg_json, timeout=10)

        if not ack:
            raise HTTPException(status_code=503, detail="Failed to publish to NATS stream")

        logger.info(f"[geocoder] Published element {message.osm_id} to NATS stream")

        return {
            "status": "ok",
            "osm_id": message.osm_id,
            "message": "Element published to NATS stream for processing",
        }

    except Exception as e:
        logger.error(f"[geocoder] Error inserting element: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to insert element: {str(e)}") from e


# ── add place endpoint ─────────────────────────────────────────────────────────
@app.post("/places", response_model=PlaceResponse)
async def add_place(place: PlaceCreate):
    """Add a new place to the geocoding database.

    Publishes the place to NATS stream for processing by the inserters.
    Returns the created place with its generated ID immediately.
    """
    # Generate a unique ID for the custom place
    custom_id = f"custom_{uuid.uuid4().hex[:16]}"

    # Create geometry from lat/lon (GeoJSON format: [lon, lat])
    geom_point = {"type": "Point", "coordinates": [place.lon, place.lat]}

    # Prepare tags for publishing
    tags = place.tags or {}
    # Add name to tags for consistency with OSM data
    tags["name"] = place.name
    if place.name_en:
        tags["name:en"] = place.name_en
    if place.name_fr:
        tags["name:fr"] = place.name_fr
    # Map address fields → OSM addr:* tags
    _addr_map = {
        "addr:housenumber": place.addr_housenumber,
        "addr:street": place.addr_street,
        "addr:city": place.addr_city,
        "addr:postcode": place.addr_postcode,
        "addr:country": place.addr_country,
        "addr:suburb": place.addr_suburb,
        "addr:state": place.addr_state,
    }
    for tag_key, tag_val in _addr_map.items():
        if tag_val:
            tags[tag_key] = tag_val

    # Get current timestamp
    created_at = datetime.now(UTC).isoformat()

    try:
        # Create message in the same format as watcher publishes
        message = {
            "osm_id": custom_id,
            "osm_type": place.osm_type,
            "tags": tags,
            "geom": geom_point,
            "admin_level": place.admin_level,
            "area_km2": 0.0,  # Points have no area
        }

        # Publish to NATS stream
        msg_json = json.dumps(message).encode()
        ack = await js.publish(NATS_SUBJECT, msg_json, timeout=10)

        if not ack:
            raise HTTPException(status_code=503, detail="Failed to publish to NATS stream")

        logger.info(f"[geocoder] Published place {custom_id} to NATS stream")

        # Index into Redis autocomplete immediately (don't wait for ES round-trip)
        if redis_pool is not None:
            try:
                ac_doc = {
                    "osm_id": custom_id,
                    "name": place.name,
                    "name_en": place.name_en or "",
                    "name_fr": place.name_fr or "",
                    "centroid": {"lat": place.lat, "lon": place.lon},
                    "admin_level": place.admin_level,
                    "offline_rank": 0,
                    "popularity": 0,
                    "full_address": "",
                    "addr_street": place.addr_street or "",
                    "addr_city": place.addr_city or "",
                }
                await ac_index_entry(redis_pool, ac_doc)
            except Exception:
                pass  # non-critical

        return PlaceResponse(
            osm_id=custom_id,
            osm_type=place.osm_type,
            name=place.name,
            name_en=place.name_en,
            name_fr=place.name_fr,
            tags=tags,
            lat=place.lat,
            lon=place.lon,
            admin_level=place.admin_level,
            created_at=created_at,
        )

    except Exception as e:
        logger.error(f"[geocoder] Error adding place: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to add place: {str(e)}") from e


# ── live-traffic probe ingestion ───────────────────────────────────────────
# App users stream GPS pings here. We publish each batch to the TRAFFIC NATS
# stream; the traffic-aggregator map-matches them to Valhalla edges, smooths a
# per-edge speed into Redis, and the traffic-writer pushes those speeds into
# Valhalla's traffic.tar. See services/traffic_aggregator.py + traffic_writer.py.
# ProbePing / ProbeBatch moved to services/geocoder_models.py


async def _publish_probes(batch: ProbeBatch) -> int:
    """Publish a probe batch to the TRAFFIC stream. Returns the point count."""
    if not ENABLE_TRAFFIC:
        raise HTTPException(
            status_code=503, detail="Live traffic is disabled (ENABLE_TRAFFIC=false)"
        )
    payload = json.dumps(batch.model_dump()).encode()
    ack = await js.publish(TRAFFIC_SUBJECT, payload, timeout=10)
    if not ack:
        raise HTTPException(status_code=503, detail="Failed to publish probes to NATS")
    return len(batch.points)


@app.post("/traffic/probes", status_code=202)
async def submit_probes(batch: ProbeBatch):
    """Submit an ordered GPS trace (multiple pings) for live-traffic aggregation."""
    try:
        n = await _publish_probes(batch)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[geocoder] Error publishing probes: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to submit probes: {str(e)}") from e
    return {"status": "accepted", "device_id": batch.device_id, "points": n}


@app.post("/traffic/probe", status_code=202)
async def submit_probe(ping: ProbePing, device_id: str = Query(..., min_length=1, max_length=128)):
    """Submit a single GPS ping (convenience wrapper around /traffic/probes)."""
    batch = ProbeBatch(device_id=device_id, points=[ping])
    try:
        await _publish_probes(batch)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[geocoder] Error publishing probe: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to submit probe: {str(e)}") from e
    return {"status": "accepted", "device_id": device_id, "points": 1}


@app.get("/traffic/edge")
async def traffic_edge(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
):
    """Debug/ops: snap a point to its Valhalla edge and report the current live speed.

    Snaps via Valhalla /locate to get the edge GraphId, then reads the smoothed
    speed the aggregator keeps in Redis (null if no live data for that edge yet).
    """
    if not ENABLE_TRAFFIC:
        raise HTTPException(
            status_code=503, detail="Live traffic is disabled (ENABLE_TRAFFIC=false)"
        )
    # Snap to the nearest edge via Valhalla.
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{VALHALLA_URL}/locate",
                json={"locations": [{"lat": lat, "lon": lon}], "costing": "auto", "verbose": True},
            )
            resp.raise_for_status()
            located = resp.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Valhalla /locate failed: {e}") from e

    edges = (located[0].get("edges") if located else None) or []
    if not edges:
        return {
            "lat": lat,
            "lon": lon,
            "edge_id": None,
            "live_speed_kph": None,
            "detail": "no road edge near this point",
        }
    edge = edges[0]
    graphid = edge.get("edge_id", {}).get("value")
    way_id = edge.get("way_id")

    live = None
    if redis_pool is not None and graphid is not None:
        h = await redis_pool.hgetall(f"tf:e:{graphid}")
        if h and "kph" in h:
            live = {
                "kph": float(h["kph"]),
                "samples": int(float(h.get("n", 0))),
                "updated": float(h["ts"]) if "ts" in h else None,
            }
    return {
        "lat": lat,
        "lon": lon,
        "edge_id": graphid,
        "way_id": way_id,
        "live_speed_kph": live["kph"] if live else None,
        "live": live,
    }


# ── reverse geocoding (PostGIS + Elasticsearch) ───────────────────────────
@app.get("/reverse")
async def reverse(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    describe: bool = Query(
        False, description="Include AI-generated descriptions (cached where available)"
    ),
):
    """Reverse geocoding using PostGIS + Elasticsearch.

    Returns:
    - nearest_address: Closest building-level address point from osm_addresses
                       (housenumber, street, city, postcode, distance_m)
    - nearest_line: The closest LineString geometry (road/street) with ES metadata
    - enclosing_polygons: Admin boundaries / areas containing the point with ES metadata
    """
    # NOTE: /reverse is intentionally NOT cached. Here the coordinate *is* the
    # query and the answer is the specific nearest building/street, so the
    # coarse coordinate rounding used for /geocode's soft geo-bias would return
    # a wrong address. At a safe (building-level) precision, continuous GPS
    # points almost never repeat, so caching would add risk for ~no hit rate.
    point_wkt = f"POINT({lon} {lat})"

    async with pg_pool.acquire() as conn:
        # Find nearest building address (uses GiST index via <-> operator)
        nearest_addr_query = """
            SELECT osm_id, osm_type, housenumber, street, city,
                   postcode, country, full_address,
                   ST_AsGeoJSON(geom) as geom,
                   ST_Distance(geom::geography,
                               ST_GeomFromText($1, 4326)::geography) AS distance_m
            FROM osm_addresses
            ORDER BY geom <-> ST_GeomFromText($1, 4326)
            LIMIT 1
        """
        nearest_addr_row = await conn.fetchrow(nearest_addr_query, point_wkt)

        # Find nearest line (LineString) — uses GiST index via <-> operator
        nearest_line_query = """
            SELECT osm_id, osm_type, ST_AsGeoJSON(geom) as geom,
                   ST_Distance(geom::geography,
                               ST_GeomFromText($1, 4326)::geography) AS distance_m
            FROM osm_geometries
            WHERE ST_GeometryType(geom) = 'ST_LineString'
            ORDER BY geom <-> ST_GeomFromText($1, 4326)
            LIMIT 1
        """
        nearest_line = await conn.fetchrow(nearest_line_query, point_wkt)

        # Find enclosing polygons/multipolygons/boundaries
        enclosing_query = """
            SELECT osm_id, osm_type, ST_AsGeoJSON(geom) as geom
            FROM osm_geometries
            WHERE ST_GeometryType(geom) IN ('ST_Polygon', 'ST_MultiPolygon')
            AND ST_Contains(geom, ST_GeomFromText($1, 4326))
        """
        enclosing_polygons = await conn.fetch(enclosing_query, point_wkt)

    # Collect all osm_ids to fetch from Elasticsearch
    osm_ids = []
    if nearest_line:
        osm_ids.append(nearest_line["osm_id"])
    osm_ids.extend(row["osm_id"] for row in enclosing_polygons)

    # Fetch data from Elasticsearch for all found osm_ids
    es_data = {}
    if osm_ids:
        try:
            resp = await es.mget(index=INDEX, ids=osm_ids)
            for doc in resp["docs"]:
                if doc.get("found"):
                    es_data[doc["_id"]] = doc["_source"]
        except Exception as e:
            logger.error(f"[geocoder] Error fetching from Elasticsearch: {e}")

    # Helper to merge PostGIS and ES data
    def merge_result(pg_row, es_source):
        raw_geom = pg_row["geom"]
        result = {
            "osm_id": pg_row["osm_id"],
            "osm_type": pg_row["osm_type"],
            "geom": json.loads(raw_geom) if isinstance(raw_geom, str) else raw_geom,
        }
        if es_source:
            result["name"] = es_source.get("name", "")
            result["name_en"] = es_source.get("name_en", "")
            result["name_fr"] = es_source.get("name_fr", "")
            result["tags"] = es_source.get("tags", {})
            result["admin_level"] = es_source.get("admin_level", 0)
            result["area_km2"] = es_source.get("area_km2", 0)
            result["offline_rank"] = es_source.get("offline_rank", 0)
            result["popularity"] = es_source.get("popularity", 0)
        return result

    result: dict = {
        "nearest_address": None,
        "interpolated_address": None,
        "nearest_line": None,
        "enclosing_polygons": [],
    }

    # nearest_address comes directly from osm_addresses (no ES lookup needed)
    if nearest_addr_row:
        raw_addr_geom = nearest_addr_row["geom"]
        dist = round(nearest_addr_row["distance_m"], 1)
        result["nearest_address"] = {
            "osm_id": nearest_addr_row["osm_id"],
            "osm_type": nearest_addr_row["osm_type"],
            "housenumber": nearest_addr_row["housenumber"],
            "street": nearest_addr_row["street"],
            "city": nearest_addr_row["city"],
            "postcode": nearest_addr_row["postcode"],
            "country": nearest_addr_row["country"],
            "full_address": nearest_addr_row["full_address"],
            "geom": json.loads(raw_addr_geom) if isinstance(raw_addr_geom, str) else raw_addr_geom,
            "distance_m": dist,
            "confidence": _distance_confidence(dist),
        }

    # ── Reverse address interpolation ─────────────────────────────────
    # Estimate the housenumber at the query point from nearby addresses.
    try:
        ia = await reverse_interpolate(pg_pool, lat, lon)
        if ia:
            result["interpolated_address"] = {
                "housenumber": ia.housenumber,
                "street": ia.street,
                "city": ia.city,
                "postcode": ia.postcode,
                "country": ia.country,
                "lat": ia.lat,
                "lon": ia.lon,
                "match_type": ia.match_type,
                "confidence": ia.confidence,
                "side": ia.side,
                "bracket_low": ia.bracket_low,
                "bracket_high": ia.bracket_high,
            }
    except Exception as e:
        _logger.debug("Reverse interpolation failed: %s", e)

    if nearest_line:
        es_source = es_data.get(nearest_line["osm_id"])
        merged = merge_result(nearest_line, es_source)
        line_dist = round(nearest_line["distance_m"], 1)
        merged["distance_m"] = line_dist
        merged["confidence"] = _distance_confidence(line_dist)
        result["nearest_line"] = merged

    polys = [merge_result(row, es_data.get(row["osm_id"])) for row in enclosing_polygons]
    polys.sort(
        key=lambda p: (
            p.get("area_km2", 0),
            -p["admin_level"] if p.get("admin_level") is not None else float("inf"),
        )
    )
    result["enclosing_polygons"] = polys

    # Attach AI descriptions when requested
    if describe and ENABLE_AI:
        desc_targets = []
        if result.get("nearest_line"):
            desc_targets.append(result["nearest_line"])
        desc_targets.extend(result.get("enclosing_polygons", []))
        if desc_targets:
            await _attach_descriptions(desc_targets)

    return result


# ── deep geocoding via Google Maps (external provider) ─────────────────────
# /deep/forward and /deep/reverse query the Google Maps Geocoding API, map each
# result into the OSM-element NATS format (so the inserters index it just like
# native OSM data), publish it for indexing, then return the data to the caller
# enriched and shaped exactly like /geocode and /reverse respectively.


def _deep_guard() -> None:
    if not ENABLE_DEEP:
        raise HTTPException(
            status_code=503, detail="Deep endpoints are disabled (ENABLE_DEEP=false)"
        )
    if not GOOGLE_MAPS_API_KEY:
        raise HTTPException(status_code=503, detail="GOOGLE_MAPS_API_KEY is not configured")


async def _publish_element(message: dict) -> bool:
    """Publish a mapped OSM-element message to NATS for the inserters. Best-effort."""
    if js is None:
        return False
    try:
        await js.publish(NATS_SUBJECT, json.dumps(message).encode(), timeout=10)
        return True
    except Exception as e:
        logger.error(f"[geocoder] deep: failed to publish {message.get('osm_id')} to NATS: {e}")
        return False


@app.get("/deep/forward")
async def deep_forward(
    request: Request,
    q: str = Query(..., min_length=1, description="Free-text place/address query"),
    language: str = Query(
        ..., min_length=2, max_length=10, description="Result language (mandatory), e.g. en, ar, fr"
    ),
    limit: int = Query(10, ge=1, le=25),
    region: str | None = Query(None, description="ccTLD region bias, e.g. 'eg'"),
    publish: bool = Query(True, description="Publish mapped results to NATS for indexing"),
    enrich: bool = Query(
        True, description="Attach parent/nearest-street enrichment from local data"
    ),
):
    """Deep forward geocoding via Google Maps.

    Calls Google in the requested ``language``, maps each result to OSM tags
    (with a ``name:<language>`` tag), publishes it to NATS for the inserters to
    index/merge, then returns results in the same shape as /geocode (with address
    enrichment from local PostGIS where available).
    """
    _deep_guard()
    try:
        raw = await gmaps_forward(q, language, region=region)
    except GoogleMapsError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

    mapped = []
    for r in raw[:limit]:
        try:
            mapped.append(map_result_to_element(r, language))
        except Exception as e:
            logger.warning(f"[geocoder] deep/forward: skipped a result ({e})")

    published = 0
    if publish:
        for extra in mapped:
            published += int(await _publish_element(extra["message"]))

    results = [_element_to_geocode_result(extra) for extra in mapped]

    # Enrich parents/nearest-street from local geometry (synchronous: deep calls
    # are low-volume and the caller explicitly asked for enriched data).
    if enrich and results:
        enrichments = await asyncio.gather(
            *[
                _enrich_address(
                    r["osm_id"], r["centroid"], r.get("area_km2", 0) or 0, r.get("admin_level")
                )
                for r in results
            ],
            return_exceptions=True,
        )
        for r, addr in zip(results, enrichments, strict=False):
            if isinstance(addr, dict):
                r["address"] = addr

    addr_detected = is_address_query(q)
    parsed_addr = parse_address_query(q) if addr_detected else {}
    request.state.result_count = len(results)
    return {
        "features": {"vectors_enabled": False, "ai_enabled": False},
        "source": "google",
        "published": published,
        "address_detected": addr_detected,
        "address_parsed": parsed_addr if addr_detected else None,
        "pagination": {"limit": limit, "offset": 0, "total": len(results), "has_more": False},
        "results": results,
    }


@app.get("/deep/reverse")
async def deep_reverse(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    language: str = Query(
        ..., min_length=2, max_length=10, description="Result language (mandatory), e.g. en, ar, fr"
    ),
    publish: bool = Query(True, description="Publish mapped results to NATS for indexing"),
    describe: bool = Query(False, description="Include AI descriptions on local geometry context"),
):
    """Deep reverse geocoding via Google Maps.

    Resolves the point's address with Google in the requested ``language``, maps +
    publishes it to NATS, and returns the same structure as /reverse:
    ``nearest_address`` comes from Google (the freshly resolved address) while
    ``nearest_line``, ``enclosing_polygons`` and ``interpolated_address`` are
    enriched from local PostGIS data.
    """
    _deep_guard()
    try:
        raw = await gmaps_reverse(lat, lon, language)
    except GoogleMapsError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

    mapped = []
    for r in raw:
        try:
            mapped.append(map_result_to_element(r, language))
        except Exception as e:
            logger.warning(f"[geocoder] deep/reverse: skipped a result ({e})")

    published = 0
    if publish:
        for extra in mapped:
            published += int(await _publish_element(extra["message"]))

    # Local geometry context + interpolation (best-effort; empty if no local data)
    try:
        result = await reverse(lat=lat, lon=lon, describe=describe)
    except Exception as e:
        logger.error(f"[geocoder] deep/reverse: local reverse failed ({e})")
        result = {
            "nearest_address": None,
            "interpolated_address": None,
            "nearest_line": None,
            "enclosing_polygons": [],
        }

    # Prefer a Google result that carries a full street address as nearest_address.
    best = None
    for extra in mapped:
        t = extra["message"]["tags"]
        if t.get("addr:housenumber") and t.get("addr:street"):
            best = extra
            break
    if best is None and mapped:
        best = mapped[0]

    if best:
        t = best["message"]["tags"]
        c = best["centroid"]
        dist = round(_haversine_m(lat, lon, c["lat"], c["lon"]), 1)
        bcat = classify(t, best["message"].get("admin_level", 0))
        result["nearest_address"] = {
            "osm_id": best["message"]["osm_id"],
            "osm_type": "node",
            "housenumber": t.get("addr:housenumber", ""),
            "street": t.get("addr:street", ""),
            "city": t.get("addr:city", ""),
            "postcode": t.get("addr:postcode", ""),
            "country": t.get("addr:country", ""),
            "full_address": best.get("formatted_address", ""),
            "category_key": bcat.key or "",
            "category_value": bcat.value or "",
            "category_group": bcat.group or "",
            "geom": best["message"]["geom"],
            "distance_m": dist,
            "confidence": best.get("confidence", _distance_confidence(dist)),
            "source": "google",
        }

    result["source"] = "google"
    result["published"] = published
    return result


@app.get("/deep/nearby")
async def deep_nearby(
    request: Request,
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    language: str = Query(
        ..., min_length=2, max_length=10, description="Result language (mandatory), e.g. en, ar, fr"
    ),
    radius: int = Query(1500, ge=1, le=50000, description="Location-bias radius in metres"),
    type: str | None = Query(
        None, description="Google place type, e.g. restaurant, cafe, pharmacy"
    ),
    keyword: str | None = Query(None, description="Free-text query (one of type/keyword required)"),
    rankby: str = Query(
        "prominence",
        pattern="^(prominence|distance)$",
        description="prominence = ranked by relevance; distance = nearest first",
    ),
    limit: int = Query(20, ge=1, le=20, description="Results per page (Google max 20)"),
    cursor: str | None = Query(
        None,
        description="pagination.next_cursor from a previous response (keep other params identical)",
    ),
    publish: bool = Query(True, description="Publish mapped results to NATS for indexing"),
):
    """Deep nearby search via Google Places API (New) Text Search.

    Queries Google around ``(lat, lon)`` (radius applied as a location bias), maps
    each place into the OSM-element NATS format (so the inserters index/merge it
    like native data), publishes it, then returns the places nearby-shaped (with
    ``distance_m`` + category). Freshly-discovered places have ``address: null``;
    the /nearby PostGIS enrichment fills nearest-street/parents once indexed.

    Scroll with ``pagination.next_cursor`` → pass it back as ``cursor`` for the
    next page (it's Google's page token; keep every other param identical).
    ``type`` or ``keyword`` is required (it is the text query).
    """
    _deep_guard()
    if not (type or keyword):
        raise HTTPException(status_code=422, detail="a type or keyword is required")
    try:
        places, next_token = await gmaps_nearby(
            lat,
            lon,
            radius,
            language=language,
            place_type=type,
            keyword=keyword,
            rankby=rankby,
            page_size=limit,
            page_token=cursor,
        )
    except GoogleMapsError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

    mapped = []
    for p in places:
        try:
            mapped.append(map_place_to_element(p, language))
        except Exception as e:
            logger.warning(f"[geocoder] deep/nearby: skipped a result ({e})")

    published = 0
    if publish:
        for extra in mapped:
            published += int(await _publish_element(extra["message"]))

    results = []
    for extra in mapped:
        row = _element_to_geocode_result(extra)
        c = extra.get("centroid") or {}
        if c.get("lat") is not None and c.get("lon") is not None:
            row["distance_m"] = round(_haversine_m(lat, lon, c["lat"], c["lon"]), 1)
        else:
            row["distance_m"] = None
        results.append(row)

    if rankby == "distance":
        results.sort(key=lambda r: (r["distance_m"] is None, r["distance_m"] or 0))

    request.state.result_count = len(results)
    return {
        "features": {"vectors_enabled": False, "ai_enabled": False},
        "source": "google",
        "published": published,
        "query": {
            "lat": lat,
            "lon": lon,
            "radius": radius,
            "type": type,
            "keyword": keyword,
            "rankby": rankby,
        },
        "pagination": {
            "limit": limit,
            "next_cursor": next_token,
            "has_more": next_token is not None,
        },
        "results": results,
    }


# ── Valhalla routing proxy (with Arabic narration) ────────────────────────────
# The /status, /route, /optimized_route, /sources_to_targets, /isochrone and
# /locate endpoints live in services.routing (they're a stateless pass-through to
# Valhalla) and are mounted here.
from services.routing import router as routing_router

app.include_router(routing_router)

# /nearby + /nearby/categories — explore nearby places, filterable by category.
# The router module is imported at the top (no import cycle: nearby.py does not
# import services.geocoder); its ES client is injected in the lifespan.
app.include_router(nearby.router)


if __name__ == "__main__":
    import uvicorn

    # The geocoder is stateless (all state in ES/PostGIS/Redis/NATS), so it scales
    # vertically via uvicorn workers and horizontally via replicas. With >1 worker
    # uvicorn needs an import string to fork; the per-worker warm-up is guarded by
    # a fleet-wide Redis lock so workers/replicas don't all rescan ES.
    workers = int(os.getenv("GEOCODER_WORKERS", "1"))
    # Hold idle keep-alive connections longer than uvicorn's 5s default so the
    # APISIX upstream pool (and any keep-alive client) reuses warm connections
    # instead of re-handshaking between bursts. Tunable via env.
    keep_alive = int(os.getenv("GEOCODER_KEEPALIVE_TIMEOUT", "30"))
    if workers > 1:
        uvicorn.run(
            "services.geocoder:app",
            host="0.0.0.0",
            port=8000,
            workers=workers,
            timeout_keep_alive=keep_alive,
        )
    else:
        uvicorn.run(app, host="0.0.0.0", port=8000, timeout_keep_alive=keep_alive)
