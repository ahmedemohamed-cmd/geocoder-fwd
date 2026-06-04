"""Geocoding HTTP service (FastAPI) — PostgreSQL backend.

All search functionality is powered by PostgreSQL (pg_trgm + tsvector + PostGIS),
replacing the previous Elasticsearch backend.  See shared/pg_search.py for queries.

Endpoints
---------
GET  /geocode       - full-text geocoding via PostgreSQL (trigram + tsvector + geo)
GET  /address       - structured address search (housenumber, street, city, postcode)
GET  /autocomplete  - fast prefix/fuzzy autocomplete via pg_trgm
POST /feedback      - popularity feedback loop (boosts future ranking)
GET  /reverse       - reverse geocoding via PostGIS (nearest line + address + polygons)
POST /insert        - insert OSM element by publishing to NATS stream
POST /places        - add a new place to the geocoding database
GET  /describe      - on-demand AI title + description for a place
GET  /health        - dependency health check (PostGIS, NATS, Redis, Ollama)
GET  /features      - feature flags discovery

Online ranking formula: text_similarity × (1 + offline_rank×2 + log(1+popularity))
"""

import asyncio
from contextlib import asynccontextmanager
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
import json

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import asyncpg
import nats
import redis.asyncio as aioredis

# ── structured logger ─────────────────────────────────────────────────────
_logger = logging.getLogger("geocoder.access")
_logger.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("%(message)s"))
_logger.addHandler(_handler)
_logger.propagate = False

from shared.config import (
    ENABLE_VECTORS,
    ENABLE_AI,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    NATS_URL,
    NATS_SUBJECT,
    REDIS_HOST,
    REDIS_PORT,
    OLLAMA_URL,
    OLLAMA_MODEL,
)
from shared.llm import generate_description, is_ollama_available, warm_up_model
from shared.interpolation import interpolate_address, reverse_interpolate, InterpolatedAddress
from shared.address import (
    extract_address_components,
    build_full_address,
    is_address_query,
    parse_address_query,
    normalize_address_text,
)
import shared.pg_search as pgsearch


def _normalize_confidence(score: float, max_score: float) -> float:
    """Normalize an Elasticsearch score to a 0.0-1.0 confidence value.

    Uses the max score in the result set as the reference point so the
    top result always gets 1.0 and others are proportional.
    """
    if max_score <= 0:
        return 0.0
    return round(min(score / max_score, 1.0), 4)


def _distance_confidence(distance_m: float) -> float:
    """Convert a distance in metres to a 0.0-1.0 confidence score.

    Follows the Pelias convention:
      <1 m   → 1.0
      1-10   → 0.9
      10-100 → 0.8
      100-250→ 0.7
      250-1k → 0.6
      1k-5k  → 0.4
      5k+    → 0.2
    """
    if distance_m < 1:
        return 1.0
    if distance_m < 10:
        return 0.9
    if distance_m < 100:
        return 0.8
    if distance_m < 250:
        return 0.7
    if distance_m < 1000:
        return 0.6
    if distance_m < 5000:
        return 0.4
    return 0.2

pg_pool: asyncpg.Pool = None  # type: ignore[assignment]
nc = None  # type: ignore[assignment]
js = None  # type: ignore[assignment]
redis_pool: aioredis.Redis = None  # type: ignore[assignment]


async def _warm_ollama():
    """Background task: pre-load the Ollama model to avoid cold-start delays."""
    ok = await warm_up_model()
    if ok:
        print("[geocoder] Ollama model pre-loaded successfully")
    else:
        print("[geocoder] Ollama model warm-up failed (descriptions may be slow on first call)")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pg_pool, nc, js, redis_pool

    max_retries = 10
    retry_delay = 2

    # Connect to PostGIS (primary data store — replaces Elasticsearch)
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
            print(f"[geocoder] Successfully connected to PostGIS")
            break
        except Exception as e:
            print(f"[geocoder] Failed to connect to PostGIS (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                raise

    # Connect to NATS
    for attempt in range(max_retries):
        try:
            nc = await nats.connect(NATS_URL)
            js = nc.jetstream()
            print(f"[geocoder] Successfully connected to NATS")
            break
        except Exception as e:
            print(f"[geocoder] Failed to connect to NATS (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                raise

    # Connect to Redis (optional — used for caching, not autocomplete)
    try:
        redis_pool = aioredis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=5,
        )
        await redis_pool.ping()
        print(f"[geocoder] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        print(f"[geocoder] Redis connection failed: {e}")
        redis_pool = None  # type: ignore[assignment]

    # Warm up Ollama model
    asyncio.create_task(_warm_ollama())

    yield
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
            json.dumps({
                "ts": datetime.now(timezone.utc).isoformat(),
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "query": str(request.url.query),
                "status": 500,
                "latency_ms": latency_ms,
            })
        )
        raise

    latency_ms = round((time.monotonic() - start) * 1000, 1)
    log_entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
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
async def _enrich_address(osm_id: str, centroid: dict | None) -> dict | None:
    """Look up address/parent data from PostGIS and cache it in ES.

    Returns the address dict or None if the centroid is missing.
    The address structure:
        {
            "nearest_street": {"osm_id": ..., "name": ..., "name_en": ...} | None,
            "parents": [
                {"osm_id": ..., "name": ..., "name_en": ..., "admin_level": ...},
                ...
            ]
        }
    """
    if not centroid:
        return None

    # centroid is stored as {"lat": ..., "lon": ...} in ES
    lat = centroid.get("lat")
    lon = centroid.get("lon")
    if lat is None or lon is None:
        return None

    point_wkt = f"POINT({lon} {lat})"

    try:
        async with pg_pool.acquire() as conn:
            # Set a query timeout to avoid slow enrichment blocking the API
            await conn.execute("SET LOCAL statement_timeout = '3000'")  # 3s

            # Find nearest lines (fetch several to find one with a name)
            nearest_lines_query = """
                SELECT osm_id, osm_type
                FROM osm_geometries
                WHERE ST_GeometryType(geom) = 'ST_LineString'
                  AND ST_DWithin(geom, ST_GeomFromText($1, 4326), 0.005)
                ORDER BY ST_Distance(geom, ST_GeomFromText($1, 4326))
                LIMIT 10
            """
            nearest_lines = await conn.fetch(nearest_lines_query, point_wkt)

            # Find enclosing polygons/multipolygons
            enclosing_query = """
                SELECT osm_id, osm_type
                FROM osm_geometries
                WHERE ST_GeometryType(geom) IN ('ST_Polygon', 'ST_MultiPolygon')
                AND ST_Contains(geom, ST_GeomFromText($1, 4326))
            """
            enclosing_polygons = await conn.fetch(enclosing_query, point_wkt)

            # Also check closed LineStrings that form boundaries around the point
            closed_lines_query = """
                SELECT osm_id, osm_type
                FROM osm_geometries
                WHERE ST_GeometryType(geom) = 'ST_LineString'
                AND ST_IsClosed(geom)
                AND ST_NPoints(geom) >= 4
                AND ST_Contains(ST_MakePolygon(geom), ST_GeomFromText($1, 4326))
            """
            closed_lines = await conn.fetch(closed_lines_query, point_wkt)
    except Exception as e:
        print(f"[geocoder] Enrichment PostGIS query failed for {osm_id}: {e}")
        return None

    # Collect all osm_ids to fetch from ES
    line_ids = [row["osm_id"] for row in nearest_lines]
    parent_ids = [row["osm_id"] for row in enclosing_polygons]
    parent_ids.extend(row["osm_id"] for row in closed_lines)

    all_ids = list(set(line_ids + parent_ids))
    if not all_ids:
        address = {"nearest_street": None, "parents": []}
        try:
            await pgsearch.cache_address(pg_pool, osm_id, address)
        except Exception:
            pass
        return address

    # Batch-fetch metadata from PostgreSQL
    pg_data: dict[str, dict] = {}
    try:
        pg_data = await pgsearch.mget_places(pg_pool, all_ids)
    except Exception as e:
        print(f"[geocoder] Error fetching address data from PG: {e}")

    # Find nearest street: first line in distance order that has a name
    nearest_street = None
    for row in nearest_lines:
        src = pg_data.get(row["osm_id"])
        if src and (src.get("name") or src.get("name_en") or src.get("name_fr")):
            nearest_street = {
                "osm_id": row["osm_id"],
                "name": src.get("name", ""),
                "name_en": src.get("name_en", ""),
                "name_fr": src.get("name_fr", ""),
            }
            break

    # Build parents list from enclosing polygons + closed lines, sorted by admin_level
    parents = []
    seen = set()
    for row_id in parent_ids:
        if row_id in seen:
            continue
        seen.add(row_id)
        src = pg_data.get(row_id)
        if src and (src.get("name") or src.get("name_en") or src.get("name_fr")):
            parents.append({
                "osm_id": row_id,
                "name": src.get("name", ""),
                "name_en": src.get("name_en", ""),
                "name_fr": src.get("name_fr", ""),
                "admin_level": src.get("admin_level", 0),
            })
    parents.sort(key=lambda p: p["admin_level"], reverse=True)

    address = {"nearest_street": nearest_street, "parents": parents}

    # Cache the address data in PostgreSQL
    try:
        await pgsearch.cache_address(pg_pool, osm_id, address)
    except Exception as e:
        print(f"[geocoder] Error caching address for {osm_id}: {e}")

    return address


# ── feature flags discovery ──────────────────────────────────────────────
@app.get("/features")
async def features():
    """Return which optional features are enabled on this instance."""
    return {
        "vectors": ENABLE_VECTORS,
        "ai": ENABLE_AI,
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

    # PostGIS (critical — primary data store)
    try:
        async with pg_pool.acquire() as conn:
            version = await conn.fetchval("SELECT version()")
            place_count = await conn.fetchval("SELECT count(*) FROM osm_places")
        checks["postgis"] = {"status": "ok", "version": version, "places": place_count}
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

    # Redis (non-critical)
    try:
        if redis_pool is not None:
            await redis_pool.ping()
            checks["redis"] = {"status": "ok"}
        else:
            checks["redis"] = {"status": "error", "detail": "not connected"}
    except Exception as e:
        checks["redis"] = {"status": "error", "detail": str(e)}

    # Overall status: fail only if critical services are down
    critical_ok = checks.get("postgis", {}).get("status") == "ok"
    overall = "healthy" if critical_ok else "degraded"
    status_code = 200 if critical_ok else 503

    # Ollama (non-critical — only used for AI descriptions)
    ollama_ok = await is_ollama_available()
    checks["ollama"] = {"status": "ok", "model": OLLAMA_MODEL} if ollama_ok else {
        "status": "error", "detail": "unreachable",
    }

    return JSONResponse(
        status_code=status_code,
        content={
            "status": overall,
            "checks": checks,
        },
    )


# ── address interpolation helper ──────────────────────────────────────────
def _interpolated_to_result(ia: InterpolatedAddress) -> dict:
    """Convert an InterpolatedAddress to a search-result dict."""
    return {
        "osm_id": None,
        "osm_type": "",
        "name": f"{ia.housenumber} {ia.street}",
        "name_en": "",
        "name_fr": "",
        "tags": {},
        "tags_text": "",
        "geom": {"type": "Point", "coordinates": [ia.lon, ia.lat]},
        "centroid": {"lat": ia.lat, "lon": ia.lon},
        "admin_level": 0,
        "area_km2": 0,
        "offline_rank": 0,
        "popularity": 0,
        "confidence": ia.confidence,
        "match_type": ia.match_type,
        "interpolation": {
            "side": ia.side,
            "bracket_low": ia.bracket_low,
            "bracket_high": ia.bracket_high,
        },
        "full_address": f"{ia.housenumber} {ia.street}, {ia.city}".strip(", "),
        "addr_housenumber": ia.housenumber,
        "addr_street": ia.street,
        "addr_city": ia.city,
        "addr_postcode": ia.postcode,
        "addr_country": ia.country,
        "addr_suburb": "",
        "addr_state": "",
        "address": None,
    }


# ── AI place descriptions ────────────────────────────────────────────────
async def _get_or_generate_description(
    osm_id: str, *, wait: bool = False
) -> dict[str, str] | None:
    """Return cached AI description or generate one.

    If ``wait=True`` (used by /describe), blocks until generation completes.
    If ``wait=False`` (used by ?describe=true), returns the cached value
    immediately and fires a background task for cache misses.

    Returns the description dict or None.
    """
    # 1. Check PG cache
    try:
        cached = await pgsearch.get_description(pg_pool, osm_id)
        if cached:
            return cached
    except Exception:
        pass

    # 2. Fetch place data for generation
    try:
        src = await pgsearch.get_place(pg_pool, osm_id)
        if not src:
            return None
    except Exception:
        return None

    if not wait:
        asyncio.create_task(_generate_and_cache(osm_id, src))
        return None

    return await _generate_and_cache(osm_id, src)


async def _generate_and_cache(osm_id: str, place_data: dict) -> dict[str, str] | None:
    """Call Ollama, cache the result in PG, return the description."""
    desc = await generate_description(place_data)
    if desc is None:
        return None

    try:
        await pgsearch.cache_description(pg_pool, osm_id, desc)
    except Exception as e:
        print(f"[geocoder] Failed to cache ai_description for {osm_id}: {e}")

    return desc


async def _attach_descriptions(results: list[dict]) -> None:
    """Attach cached AI descriptions to search results, fire background
    generation for misses.  Mutates ``results`` in place."""
    for result in results:
        osm_id = result.get("osm_id")
        if not osm_id:
            continue
        try:
            cached = await pgsearch.get_description(pg_pool, osm_id)
            if cached:
                result["ai_description"] = cached
            else:
                result["ai_description"] = None
                src = await pgsearch.get_place(pg_pool, osm_id)
                if src:
                    asyncio.create_task(_generate_and_cache(osm_id, src))
        except Exception:
            result["ai_description"] = None


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
        raise HTTPException(status_code=404, detail="Place not found or description generation failed")
    return {"osm_id": osm_id, **desc}


# ── geocode (PostgreSQL) ──────────────────────────────────────────────────
@app.get("/geocode")
async def geocode_endpoint(
    request: Request,
    q: str = Query(..., min_length=1),
    lat: float | None = Query(None),
    lon: float | None = Query(None),
    limit: int = Query(10, ge=1, le=50),
    vector: bool = Query(True, description="Enable semantic vector search"),
    ai: bool = Query(True, description="Enable AI-assisted search"),
    describe: bool = Query(False, description="Include AI-generated descriptions"),
):
    """Full geocoding search backed by PostgreSQL.

    Uses pg_trgm for fuzzy/prefix matching, tsvector for full-text search,
    and PostGIS for geo-distance scoring.  Ranking mirrors the ES
    function_score formula: text_similarity × (offline_rank + geo + popularity).
    """
    use_vectors = vector and ENABLE_VECTORS
    use_ai = ai and ENABLE_AI

    q_norm = normalize_address_text(q)

    # Address detection & decomposition
    addr_detected = is_address_query(q)
    parsed_addr: dict = {}
    if addr_detected:
        parsed_addr = parse_address_query(q)

    results = await pgsearch.geocode(
        pg_pool, q_norm,
        limit=limit,
        lat=lat,
        lon=lon,
        housenumber=parsed_addr.get("housenumber"),
        street_query=parsed_addr.get("street"),
        is_address=addr_detected,
    )

    # Enrich results missing address data
    enrich_tasks = []
    for idx, result in enumerate(results):
        if result.get("address") is None:
            enrich_tasks.append((idx, result["osm_id"], result.get("centroid")))

    if enrich_tasks:
        enrichments = await asyncio.gather(
            *[_enrich_address(osm_id, centroid) for _, osm_id, centroid in enrich_tasks],
            return_exceptions=True,
        )
        for (idx, _, _), addr in zip(enrich_tasks, enrichments):
            if isinstance(addr, dict):
                results[idx]["address"] = addr

    # Address interpolation fallback
    interpolated_result = None
    if addr_detected and parsed_addr.get("housenumber"):
        try:
            req_hn = int(parsed_addr["housenumber"])
            has_exact = any(
                r.get("addr_housenumber") == str(req_hn)
                for r in results
            )
            if not has_exact:
                ia = await interpolate_address(
                    pg_pool,
                    req_hn,
                    street=parsed_addr.get("street", ""),
                    city=parsed_addr.get("city"),
                )
                if ia:
                    interpolated_result = _interpolated_to_result(ia)
                    results.insert(0, interpolated_result)
        except (ValueError, TypeError):
            pass

    if describe and ENABLE_AI:
        await _attach_descriptions(results)

    request.state.result_count = len(results)
    return {
        "features": {
            "vectors_enabled": use_vectors,
            "ai_enabled": use_ai,
        },
        "address_detected": addr_detected,
        "address_parsed": parsed_addr if addr_detected else None,
        "results": results,
    }


# ── autocomplete ──────────────────────────────────────────────────────────

@app.get("/autocomplete")
async def autocomplete(
    request: Request,
    q: str = Query(..., min_length=1, description="Partial query text"),
    lat: float | None = Query(None, description="Latitude for geo-bias"),
    lon: float | None = Query(None, description="Longitude for geo-bias"),
    limit: int = Query(7, ge=1, le=20, description="Max suggestions"),
):
    """Fast prefix-based autocomplete backed by PostgreSQL pg_trgm.

    Uses trigram similarity + ILIKE prefix matching on name fields,
    ranked by text similarity × offline_rank × popularity.
    """
    results = await pgsearch.autocomplete(
        pg_pool, q, limit=limit, lat=lat, lon=lon,
    )
    request.state.result_count = len(results)
    return {"source": "postgresql", "results": results}


# ── feedback loop ─────────────────────────────────────────────────────────
_POPULARITY_CAP = 1000.0


@app.post("/feedback")
async def feedback(
    osm_id: str = Query(...),
    boost: float = Query(1.0, ge=0.1, le=10.0),
):
    """Increment popularity for an element in PostgreSQL.

    The boost value is clamped to [0.1, 10.0] and popularity is capped
    at 1000 to prevent unbounded growth or abuse.
    """
    try:
        await pgsearch.update_popularity(pg_pool, osm_id, boost, _POPULARITY_CAP)
    except Exception:
        pass

    return {"status": "ok", "osm_id": osm_id}


# ── address search (PostgreSQL) ───────────────────────────────────────────
@app.get("/address")
async def address_search_endpoint(
    request: Request,
    q: str = Query(..., min_length=1, description="Address query string"),
    lat: float | None = Query(None, description="Latitude for proximity boost"),
    lon: float | None = Query(None, description="Longitude for proximity boost"),
    limit: int = Query(10, ge=1, le=50),
    postcode: str | None = Query(None, description="Restrict results to postal code"),
    city: str | None = Query(None, description="Restrict results to city/town"),
    country: str | None = Query(None, description="Restrict to ISO country code (e.g. EG)"),
    describe: bool = Query(False, description="Include AI-generated descriptions"),
):
    """Structured address search powered by PostgreSQL.

    Supports free-form queries, field-level filters (postcode, city, country),
    and address interpolation fallback.
    """
    q_norm = normalize_address_text(q)
    parsed = parse_address_query(q)

    if postcode:
        parsed["postcode"] = postcode
    if city:
        parsed["city"] = city
    if country:
        parsed["country"] = country.upper()

    results = await pgsearch.address_search(
        pg_pool, q_norm,
        limit=limit,
        lat=lat,
        lon=lon,
        housenumber=parsed.get("housenumber"),
        street=parsed.get("street"),
        city=parsed.get("city"),
        postcode=parsed.get("postcode"),
        country=parsed.get("country"),
    )

    # Address interpolation fallback
    if parsed.get("housenumber"):
        try:
            req_hn = int(parsed["housenumber"])
            has_exact = any(
                r.get("addr_housenumber") == str(req_hn)
                for r in results
            )
            if not has_exact:
                ia = await interpolate_address(
                    pg_pool,
                    req_hn,
                    street=parsed.get("street", ""),
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
class PlaceCreate(BaseModel):
    """Model for creating a new place."""
    name: str = Field(..., min_length=1, max_length=255)
    name_en: str | None = Field(None, max_length=255)
    name_fr: str | None = Field(None, max_length=255)
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    tags: dict[str, str] | None = Field(default_factory=dict)
    osm_type: str = Field(default="node", description="OSM type: node, way, or relation")
    admin_level: int = Field(default=0, ge=0, le=10)
    # Optional structured address fields
    addr_housenumber: str | None = Field(None, max_length=50,  description="House/building number")
    addr_street:      str | None = Field(None, max_length=255, description="Street name")
    addr_city:        str | None = Field(None, max_length=255, description="City or town")
    addr_postcode:    str | None = Field(None, max_length=20,  description="Postal / ZIP code")
    addr_country:     str | None = Field(None, max_length=10,  description="ISO 3166-1 country code")
    addr_suburb:      str | None = Field(None, max_length=255, description="Suburb / neighbourhood")
    addr_state:       str | None = Field(None, max_length=255, description="State / governorate")


class InsertMessage(BaseModel):
    """Model for insert endpoint - matches watcher.py message format exactly."""
    osm_id: str = Field(..., description="OSM ID (e.g., 'n123', 'w456', 'r789')")
    osm_type: str = Field(..., description="OSM type: node, way, or relation")
    tags: dict[str, str] = Field(..., description="OSM tags")
    geom: dict = Field(..., description="GeoJSON geometry")
    admin_level: int = Field(default=0, ge=0, le=10, description="Administrative level")
    area_km2: float = Field(default=0.0, ge=0, description="Area in square kilometers")


class PlaceResponse(BaseModel):
    """Model for place response."""
    osm_id: str
    osm_type: str
    name: str
    name_en: str | None
    name_fr: str | None
    tags: dict[str, str]
    lat: float
    lon: float
    admin_level: int
    created_at: str


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
        
        print(f"[geocoder] Published element {message.osm_id} to NATS stream")
        
        return {
            "status": "ok",
            "osm_id": message.osm_id,
            "message": "Element published to NATS stream for processing"
        }
        
    except Exception as e:
        print(f"[geocoder] Error inserting element: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to insert element: {str(e)}")


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
        "addr:street":      place.addr_street,
        "addr:city":        place.addr_city,
        "addr:postcode":    place.addr_postcode,
        "addr:country":     place.addr_country,
        "addr:suburb":      place.addr_suburb,
        "addr:state":       place.addr_state,
    }
    for tag_key, tag_val in _addr_map.items():
        if tag_val:
            tags[tag_key] = tag_val
    
    # Get current timestamp
    created_at = datetime.now(timezone.utc).isoformat()
    
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
        
        print(f"[geocoder] Published place {custom_id} to NATS stream")

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
            created_at=created_at
        )
        
    except Exception as e:
        print(f"[geocoder] Error adding place: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to add place: {str(e)}")



# ── reverse geocoding (PostGIS + Elasticsearch) ───────────────────────────
@app.get("/reverse")
async def reverse(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    describe: bool = Query(False, description="Include AI-generated descriptions (cached where available)"),
):
    """Reverse geocoding using PostGIS + Elasticsearch.

    Returns:
    - nearest_address: Closest building-level address point from osm_addresses
                       (housenumber, street, city, postcode, distance_m)
    - nearest_line: The closest LineString geometry (road/street) with ES metadata
    - enclosing_polygons: Admin boundaries / areas containing the point with ES metadata
    """
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

    # Collect all osm_ids to fetch from PostgreSQL
    osm_ids = []
    if nearest_line:
        osm_ids.append(nearest_line["osm_id"])
    osm_ids.extend(row["osm_id"] for row in enclosing_polygons)

    # Fetch data from PostgreSQL for all found osm_ids
    pg_data = {}
    if osm_ids:
        try:
            pg_data = await pgsearch.mget_places(pg_pool, osm_ids)
        except Exception as e:
            print(f"[geocoder] Error fetching from PostgreSQL: {e}")

    # Helper to merge PostGIS geometry and osm_places metadata
    def merge_result(pg_row, place_data):
        raw_geom = pg_row["geom"]
        result = {
            "osm_id": pg_row["osm_id"],
            "osm_type": pg_row["osm_type"],
            "geom": json.loads(raw_geom) if isinstance(raw_geom, str) else raw_geom,
        }
        if place_data:
            result["name"] = place_data.get("name", "")
            result["name_en"] = place_data.get("name_en", "")
            result["name_fr"] = place_data.get("name_fr", "")
            result["tags"] = place_data.get("tags", {})
            result["admin_level"] = place_data.get("admin_level", 0)
            result["area_km2"] = place_data.get("area_km2", 0)
            result["offline_rank"] = place_data.get("offline_rank", 0)
            result["popularity"] = place_data.get("popularity", 0)
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
            "osm_id":       nearest_addr_row["osm_id"],
            "osm_type":     nearest_addr_row["osm_type"],
            "housenumber":  nearest_addr_row["housenumber"],
            "street":       nearest_addr_row["street"],
            "city":         nearest_addr_row["city"],
            "postcode":     nearest_addr_row["postcode"],
            "country":      nearest_addr_row["country"],
            "full_address": nearest_addr_row["full_address"],
            "geom":         json.loads(raw_addr_geom) if isinstance(raw_addr_geom, str) else raw_addr_geom,
            "distance_m":   dist,
            "confidence":   _distance_confidence(dist),
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
        place_source = pg_data.get(nearest_line["osm_id"])
        merged = merge_result(nearest_line, place_source)
        line_dist = round(nearest_line["distance_m"], 1)
        merged["distance_m"] = line_dist
        merged["confidence"] = _distance_confidence(line_dist)
        result["nearest_line"] = merged

    result["enclosing_polygons"] = [
        merge_result(row, pg_data.get(row["osm_id"]))
        for row in enclosing_polygons
    ]

    # Attach AI descriptions when requested
    if describe and ENABLE_AI:
        desc_targets = []
        if result.get("nearest_line"):
            desc_targets.append(result["nearest_line"])
        desc_targets.extend(result.get("enclosing_polygons", []))
        if desc_targets:
            await _attach_descriptions(desc_targets)

    return result


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
