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

Query-string flags shared by search endpoints:
  vector=true   enable semantic / AI vector search  (requires ENABLE_VECTORS=true)
  vector=false  disable vector search (text-only)
  ai=true       enable AI-assisted search (requires ENABLE_AI=true)
  ai=false      disable AI features

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
from contextlib import asynccontextmanager
import uuid
from datetime import datetime, timezone
import json

from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel, Field
from elasticsearch import AsyncElasticsearch
import asyncpg
import nats

from shared.config import (
    ELASTICSEARCH_URL,
    EMBEDDING_DIM,
    ENABLE_VECTORS,
    ENABLE_AI,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    NATS_URL,
    NATS_SUBJECT,
)
from shared.embeddings import embed_texts
from shared.address import (
    extract_address_components,
    build_full_address,
    is_address_query,
    parse_address_query,
)

INDEX = "osm_places"

# ── Elasticsearch index mapping ───────────────────────────────────────────
ES_MAPPING = {
    "settings": {
        "index": {"number_of_replicas": 0},
    },
    "mappings": {
        "properties": {
            "osm_id": {"type": "keyword"},
            "osm_type": {"type": "keyword"},
            "name": {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "name_en": {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "tags_text": {
                "type": "text",
                "analyzer": "standard",
            },
            "tags": {"type": "object", "enabled": False},
            "geom": {"type": "geo_shape"},
            "centroid": {"type": "geo_point"},
            "admin_level": {"type": "integer"},
            "area_km2": {"type": "float"},
            "offline_rank": {"type": "float"},
            "popularity": {"type": "float"},
            "name_vector": {
                "type": "dense_vector",
                "dims": EMBEDDING_DIM,
                "index": True,
                "similarity": "cosine",
            },
            # ── address fields ────────────────────────────────────────────
            "addr_housenumber": {"type": "keyword"},
            "addr_street": {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "addr_city": {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "addr_postcode": {"type": "keyword"},
            "addr_country":  {"type": "keyword"},
            "addr_suburb":   {"type": "text"},
            "addr_state":    {"type": "text"},
            "full_address":  {"type": "text", "analyzer": "standard"},
            "has_address":   {"type": "boolean"},
        }
    },
}

es: AsyncElasticsearch = None  # type: ignore[assignment]
pg_pool: asyncpg.Pool = None  # type: ignore[assignment]
nc = None  # type: ignore[assignment]
js = None  # type: ignore[assignment]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global es, pg_pool, nc, js

    # Retry logic for connecting to dependencies
    max_retries = 10
    retry_delay = 2
    
    # Connect to Elasticsearch
    for attempt in range(max_retries):
        try:
            es = AsyncElasticsearch(ELASTICSEARCH_URL)
            await es.ping()
            print(f"[geocoder] Successfully connected to Elasticsearch")
            break
        except Exception as e:
            print(f"[geocoder] Failed to connect to Elasticsearch (attempt {attempt + 1}/{max_retries}): {e}")
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

    # ensure ES index exists
    try:
        if not await es.indices.exists(index=INDEX):
            await es.indices.create(index=INDEX, **ES_MAPPING)
            print(f"[geocoder] Created ES index {INDEX}")
    except Exception as e:
        print(f"[geocoder] Error checking/creating ES index: {e}")
        try:
            await es.indices.create(index=INDEX, **ES_MAPPING)
            print(f"[geocoder] Created ES index {INDEX} (fallback)")
        except Exception as e2:
            print(f"[geocoder] Failed to create ES index: {e2}")

    yield
    await es.close()
    await pg_pool.close()
    await nc.close()


app = FastAPI(title="Geocoding Service", lifespan=lifespan)


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

    async with pg_pool.acquire() as conn:
        # Find nearest lines (fetch several to find one with a name)
        nearest_lines_query = """
            SELECT osm_id, osm_type
            FROM osm_geometries
            WHERE ST_GeometryType(geom) = 'ST_LineString'
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

    # Collect all osm_ids to fetch from ES
    line_ids = [row["osm_id"] for row in nearest_lines]
    parent_ids = [row["osm_id"] for row in enclosing_polygons]
    parent_ids.extend(row["osm_id"] for row in closed_lines)

    all_ids = list(set(line_ids + parent_ids))
    if not all_ids:
        address = {"nearest_street": None, "parents": []}
        # Cache even empty results to avoid repeated lookups
        try:
            await es.update(index=INDEX, id=osm_id, body={"doc": {"address": address}})
        except Exception:
            pass
        return address

    # Batch-fetch metadata from ES
    es_data: dict[str, dict] = {}
    try:
        resp = await es.mget(index=INDEX, ids=all_ids)
        for doc in resp["docs"]:
            if doc.get("found"):
                es_data[doc["_id"]] = doc["_source"]
    except Exception as e:
        print(f"[geocoder] Error fetching address data from ES: {e}")

    # Find nearest street: first line in distance order that has a name
    nearest_street = None
    for row in nearest_lines:
        src = es_data.get(row["osm_id"])
        if src and (src.get("name") or src.get("name_en")):
            nearest_street = {
                "osm_id": row["osm_id"],
                "name": src.get("name", ""),
                "name_en": src.get("name_en", ""),
            }
            break

    # Build parents list from enclosing polygons + closed lines, sorted by admin_level
    parents = []
    seen = set()
    for row_id in parent_ids:
        if row_id in seen:
            continue
        seen.add(row_id)
        src = es_data.get(row_id)
        if src and (src.get("name") or src.get("name_en")):
            parents.append({
                "osm_id": row_id,
                "name": src.get("name", ""),
                "name_en": src.get("name_en", ""),
                "admin_level": src.get("admin_level", 0),
            })
    # Sort parents by admin_level descending (most specific first: suburb→city→state→country)
    parents.sort(key=lambda p: p["admin_level"], reverse=True)

    address = {"nearest_street": nearest_street, "parents": parents}

    # Cache the address data in ES
    try:
        await es.update(index=INDEX, id=osm_id, body={"doc": {"address": address}})
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


# ── geocode (Elasticsearch) ──────────────────────────────────────────────
@app.get("/geocode")
async def geocode(
    q: str = Query(..., min_length=1),
    lat: float | None = Query(None),
    lon: float | None = Query(None),
    limit: int = Query(10, ge=1, le=50),
    vector: bool = Query(True, description="Enable semantic vector search"),
    ai: bool = Query(True, description="Enable AI-assisted search"),
):
    """Full geocoding search.

    Online ranking = text_similarity × function_score(offline_rank, geo, popularity).
    Uses boost_mode=multiply so text relevance gates ranking — a high-importance
    element with a weak text match cannot outscore a lower-rank exact match.

    Text similarity searches across name, name_en, and tags_text (all tags)
    with phrase and exact-match boosting for multi-word queries.
    vector=true adds KNN cosine similarity re-ranking (requires ENABLE_VECTORS).
    ai=true enables AI-assisted query expansion (requires ENABLE_AI).
    """
    use_vectors = vector and ENABLE_VECTORS
    use_ai = ai and ENABLE_AI

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

    # offline_rank boost (tiebreaker, not dominant — text relevance gates via multiply)
    functions.append(
        {
            "field_value_factor": {
                "field": "offline_rank",
                "modifier": "log1p",
                "factor": 1,
                "missing": 0,
            },
            "weight": 3,
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

    # text query – single multi_match + phrase/exact boosts
    should_clauses: list[dict] = [
        # fuzzy token matching across all searchable fields
        {
            "multi_match": {
                "query": q,
                "fields": ["name^5", "name_en^5", "tags_text"],
                "type": "best_fields",
                "fuzziness": "AUTO",
            }
        },
        # phrase boost: "New York" must appear as a contiguous phrase
        {"match_phrase": {"name":    {"query": q, "boost": 10}}},
        {"match_phrase": {"name_en": {"query": q, "boost": 10}}},
        # exact keyword match: strongest signal when query is an exact name
        {"term": {"name.keyword":    {"value": q, "boost": 15}}},
        {"term": {"name_en.keyword": {"value": q, "boost": 15}}},
    ]

    # When the query looks like a structured address, also search address fields
    if is_address_query(q):
        parsed_addr = parse_address_query(q)
        should_clauses.append(
            {"match": {"full_address": {"query": q, "boost": 3, "fuzziness": "AUTO"}}}
        )
        if parsed_addr.get("street"):
            should_clauses.append(
                {"match": {"addr_street": {"query": parsed_addr["street"], "boost": 4}}}
            )
        if parsed_addr.get("city"):
            should_clauses.append(
                {"match": {"addr_city": {"query": parsed_addr["city"], "boost": 2}}}
            )
        if parsed_addr.get("housenumber"):
            should_clauses.append(
                {"term": {"addr_housenumber": parsed_addr["housenumber"]}}
            )

    text_query: dict = {
        "bool": {
            "should": should_clauses,
            "minimum_should_match": 1,
        }
    }

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

    # Build results and enrich address data where missing
    results = []
    enrich_tasks = []

    for h in resp["hits"]["hits"]:
        src = h["_source"]
        result = {
            "osm_id": src["osm_id"],
            "osm_type": src.get("osm_type", ""),
            "name": src.get("name", ""),
            "name_en": src.get("name_en", ""),
            "tags": src.get("tags", {}),
            "tags_text": src.get("tags_text", ""),
            "geom": src.get("geom"),
            "centroid": src.get("centroid"),
            "admin_level": src.get("admin_level", 0),
            "area_km2": src.get("area_km2", 0),
            "offline_rank": src.get("offline_rank", 0),
            "popularity": src.get("popularity", 0),
            "score": h["_score"],
            # structured address fields (present when element has addr:* tags)
            "full_address":      src.get("full_address", ""),
            "addr_housenumber":  src.get("addr_housenumber", ""),
            "addr_street":       src.get("addr_street", ""),
            "addr_city":         src.get("addr_city", ""),
            "addr_postcode":     src.get("addr_postcode", ""),
            "addr_country":      src.get("addr_country", ""),
            "addr_suburb":       src.get("addr_suburb", ""),
            "addr_state":        src.get("addr_state", ""),
            # reverse-geocoded address enrichment (computed on demand)
            "address": src.get("address"),
        }
        results.append(result)

        # If address is not cached, schedule enrichment
        if result["address"] is None:
            enrich_tasks.append((len(results) - 1, src["osm_id"], src.get("centroid")))

    # Enrich all results that are missing address data concurrently
    if enrich_tasks:
        enrichments = await asyncio.gather(
            *[_enrich_address(osm_id, centroid) for _, osm_id, centroid in enrich_tasks],
            return_exceptions=True,
        )
        for (idx, _, _), addr in zip(enrich_tasks, enrichments):
            if isinstance(addr, dict):
                results[idx]["address"] = addr

    return {
        "features": {
            "vectors_enabled": use_vectors,
            "ai_enabled": use_ai,
        },
        "results": results,
    }


# ── feedback loop ─────────────────────────────────────────────────────────
_POPULARITY_CAP = 1000.0


@app.post("/feedback")
async def feedback(
    osm_id: str = Query(...),
    boost: float = Query(1.0, ge=0.1, le=10.0),
):
    """Increment popularity for an element in Elasticsearch.

    The boost value is clamped to [0.1, 10.0] and popularity is capped
    at 1000 to prevent unbounded growth or abuse.
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
    except Exception:
        pass

    return {"status": "ok", "osm_id": osm_id}


# ── address search (Elasticsearch) ───────────────────────────────────────────
@app.get("/address")
async def address_search(
    q: str = Query(..., min_length=1, description="Address query string"),
    lat: float | None = Query(None, description="Latitude for proximity boost"),
    lon: float | None = Query(None, description="Longitude for proximity boost"),
    limit: int = Query(10, ge=1, le=50),
    postcode: str | None = Query(None, description="Restrict results to postal code"),
    city: str | None = Query(None, description="Restrict results to city/town"),
    country: str | None = Query(None, description="Restrict to ISO country code (e.g. EG)"),
):
    """Structured address search powered by Elasticsearch.

    Supports free-form queries like:
    - ``"123 Tahrir Street, Cairo"``  → parsed into housenumber + street + city
    - ``"Tahrir Street, Zamalek"``    → street + suburb/city
    - ``"شارع التحرير, القاهرة"``     → Arabic address
    - ``"11511"``                     → postcode lookup

    Use the ``postcode``, ``city``, ``country`` query params to add hard filters
    on top of the free-text query.

    Results are ordered by: text relevance × (offline_rank + geo proximity).
    Only elements that carry at least one ``addr:*`` tag are returned.
    """
    parsed = parse_address_query(q)

    # Explicit query params override parsed components
    if postcode:
        parsed["postcode"] = postcode
    if city:
        parsed["city"] = city
    if country:
        parsed["country"] = country.upper()

    must_clauses:   list[dict] = []
    should_clauses: list[dict] = []
    filter_clauses: list[dict] = [{"term": {"has_address": True}}]

    # ── hard filters ─────────────────────────────────────────────────────
    if parsed.get("housenumber"):
        must_clauses.append({"term": {"addr_housenumber": parsed["housenumber"]}})

    if parsed.get("postcode"):
        must_clauses.append({"term": {"addr_postcode": parsed["postcode"]}})

    if parsed.get("country"):
        filter_clauses.append({"term": {"addr_country": parsed["country"]}})

    # City as hard filter only when supplied as explicit query param
    if city and parsed.get("city"):
        filter_clauses.append(
            {"match": {"addr_city": {"query": parsed["city"], "operator": "or"}}}
        )

    # ── scored should clauses ────────────────────────────────────────────
    if parsed.get("street"):
        should_clauses.append(
            {
                "multi_match": {
                    "query": parsed["street"],
                    "fields": ["addr_street^4", "full_address^2"],
                    "fuzziness": "AUTO",
                }
            }
        )
    # City as boost (when not already a hard filter)
    if parsed.get("city") and not city:
        should_clauses.append(
            {"match": {"addr_city": {"query": parsed["city"], "boost": 2}}}
        )

    # Full-address fallback: always present so we can still find partial matches
    should_clauses.append(
        {"match": {"full_address": {"query": q, "fuzziness": "AUTO", "boost": 1}}}
    )

    text_query: dict = {
        "bool": {
            "must":   must_clauses,
            "should": should_clauses,
            "filter": filter_clauses,
            "minimum_should_match": 1 if should_clauses else 0,
        }
    }

    # ── scoring functions ────────────────────────────────────────────────
    functions: list[dict] = [{"weight": 1.0}]
    functions.append(
        {
            "field_value_factor": {
                "field": "offline_rank",
                "modifier": "log1p",
                "factor": 1,
                "missing": 0,
            },
            "weight": 3,
        }
    )
    if lat is not None and lon is not None:
        # Tight geo decay for address search (500 m scale)
        functions.append(
            {
                "gauss": {
                    "centroid": {
                        "origin": {"lat": lat, "lon": lon},
                        "scale": "500m",
                        "decay": 0.5,
                    }
                },
                "weight": 4,
            }
        )

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

    return {
        "query": q,
        "parsed": parsed,
        "results": [
            {
                "osm_id":          h["_source"]["osm_id"],
                "osm_type":        h["_source"].get("osm_type", ""),
                "name":            h["_source"].get("name", ""),
                "full_address":    h["_source"].get("full_address", ""),
                "addr_housenumber": h["_source"].get("addr_housenumber", ""),
                "addr_street":     h["_source"].get("addr_street", ""),
                "addr_city":       h["_source"].get("addr_city", ""),
                "addr_postcode":   h["_source"].get("addr_postcode", ""),
                "addr_country":    h["_source"].get("addr_country", ""),
                "addr_suburb":     h["_source"].get("addr_suburb", ""),
                "addr_state":      h["_source"].get("addr_state", ""),
                "centroid":        h["_source"].get("centroid"),
                "geom":            h["_source"].get("geom"),
                "offline_rank":    h["_source"].get("offline_rank", 0),
                "score":           h["_score"],
            }
            for h in resp["hits"]["hits"]
        ],
    }


# ── Pydantic models for place management ───────────────────────────────────────
class PlaceCreate(BaseModel):
    """Model for creating a new place."""
    name: str = Field(..., min_length=1, max_length=255)
    name_en: str | None = Field(None, max_length=255)
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

        # Find nearest line (LineString)
        nearest_line_query = """
            SELECT osm_id, osm_type, ST_AsGeoJSON(geom) as geom
            FROM osm_geometries
            WHERE ST_GeometryType(geom) = 'ST_LineString'
            ORDER BY ST_Distance(geom, ST_GeomFromText($1, 4326))
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
            print(f"[geocoder] Error fetching from Elasticsearch: {e}")

    # Helper to merge PostGIS and ES data
    def merge_result(pg_row, es_source):
        result = {
            "osm_id": pg_row["osm_id"],
            "osm_type": pg_row["osm_type"],
            "geom": pg_row["geom"],
        }
        if es_source:
            result["name"] = es_source.get("name", "")
            result["name_en"] = es_source.get("name_en", "")
            result["tags"] = es_source.get("tags", {})
            result["admin_level"] = es_source.get("admin_level", 0)
            result["area_km2"] = es_source.get("area_km2", 0)
            result["offline_rank"] = es_source.get("offline_rank", 0)
            result["popularity"] = es_source.get("popularity", 0)
        return result

    result: dict = {
        "nearest_address": None,
        "nearest_line": None,
        "enclosing_polygons": [],
    }

    # nearest_address comes directly from osm_addresses (no ES lookup needed)
    if nearest_addr_row:
        result["nearest_address"] = {
            "osm_id":       nearest_addr_row["osm_id"],
            "osm_type":     nearest_addr_row["osm_type"],
            "housenumber":  nearest_addr_row["housenumber"],
            "street":       nearest_addr_row["street"],
            "city":         nearest_addr_row["city"],
            "postcode":     nearest_addr_row["postcode"],
            "country":      nearest_addr_row["country"],
            "full_address": nearest_addr_row["full_address"],
            "geom":         nearest_addr_row["geom"],
            "distance_m":   round(nearest_addr_row["distance_m"], 1),
        }

    if nearest_line:
        es_source = es_data.get(nearest_line["osm_id"])
        result["nearest_line"] = merge_result(nearest_line, es_source)

    result["enclosing_polygons"] = [
        merge_result(row, es_data.get(row["osm_id"]))
        for row in enclosing_polygons
    ]

    return result


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
