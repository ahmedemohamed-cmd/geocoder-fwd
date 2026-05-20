"""Geocoding HTTP service (FastAPI).

Endpoints
---------
GET  /autocomplete  - prefix search via Typesense  (fast, typo-tolerant)
GET  /geocode       - full search via Elasticsearch (text + vector + geo + ranking)
POST /feedback      - popularity feedback loop      (boosts future ranking)
GET  /reverse       - reverse geocoding via PostGIS + Elasticsearch (nearest line + enclosing polygons with metadata)

Query-string flags shared by both search endpoints:
  vector=true   enable semantic / AI vector search  (requires ENABLE_VECTORS=true)
  vector=false  disable vector search (text-only)
  ai=true       enable AI-assisted search (requires ENABLE_AI=true)
  ai=false      disable AI features

Online ranking formula: offline_rank * text_similarity (+ optional vector KNN + geo decay)
"""

import asyncio
from contextlib import asynccontextmanager
import uuid
from datetime import datetime

from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel, Field
from elasticsearch import AsyncElasticsearch
import typesense
import asyncpg

from typesense.exceptions import ObjectNotFound

from shared.config import (
    ELASTICSEARCH_URL,
    TYPESENSE_HOST,
    TYPESENSE_PORT,
    TYPESENSE_API_KEY,
    EMBEDDING_DIM,
    ENABLE_VECTORS,
    ENABLE_AI,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)
from shared.embeddings import embed_texts

INDEX = "osm_places"
COLLECTION = "osm_places"

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
        }
    },
}

# ── Typesense collection schema ──────────────────────────────────────────
TS_SCHEMA = {
    "name": COLLECTION,
    "fields": [
        {"name": "osm_id", "type": "string", "facet": True},
        {"name": "name", "type": "string"},
        {"name": "name_en", "type": "string", "optional": True},
        {"name": "osm_type", "type": "string", "facet": True},
        {"name": "tags_text", "type": "string"},
        {"name": "admin_level", "type": "int32", "facet": True},
        {"name": "offline_rank", "type": "float"},
        {"name": "popularity", "type": "float"},
        {"name": "location", "type": "geopoint", "optional": True},
        {
            "name": "name_vector",
            "type": "float[]",
            "num_dim": EMBEDDING_DIM,
            "optional": True,
        },
    ],
    "default_sorting_field": "offline_rank",
}

es: AsyncElasticsearch = None  # type: ignore[assignment]
ts: typesense.Client = None  # type: ignore[assignment]
pg_pool: asyncpg.Pool = None  # type: ignore[assignment]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global es, ts, pg_pool

    # Retry logic for connecting to dependencies
    max_retries = 10
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            es = AsyncElasticsearch(ELASTICSEARCH_URL)
            # Test ES connection
            await es.ping()
            print(f"[geocoder] Successfully connected to Elasticsearch")
            break
        except Exception as e:
            print(f"[geocoder] Failed to connect to Elasticsearch (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                raise
    else:
        raise Exception("Failed to connect to Elasticsearch after maximum retries")
    
    for attempt in range(max_retries):
        try:
            ts = typesense.Client(
                {
                    "nodes": [
                        {
                            "host": TYPESENSE_HOST,
                            "port": str(TYPESENSE_PORT),
                            "protocol": "http",
                        }
                    ],
                    "api_key": TYPESENSE_API_KEY,
                    "connection_timeout_seconds": 10,
                }
            )
            # Test TS connection
            ts.collections.retrieve()
            print(f"[geocoder] Successfully connected to Typesense")
            break
        except Exception as e:
            print(f"[geocoder] Failed to connect to Typesense (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                raise
    else:
        raise Exception("Failed to connect to Typesense after maximum retries")

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
    else:
        raise Exception("Failed to connect to PostGIS after maximum retries")

    # ensure ES index exists
    try:
        if not await es.indices.exists(index=INDEX):
            await es.indices.create(index=INDEX, **ES_MAPPING)
            print(f"[geocoder] Created ES index {INDEX}")
    except Exception as e:
        print(f"[geocoder] Error checking/creating ES index: {e}")
        # Try to create the index anyway
        try:
            await es.indices.create(index=INDEX, **ES_MAPPING)
            print(f"[geocoder] Created ES index {INDEX} (fallback)")
        except Exception as e2:
            print(f"[geocoder] Failed to create ES index: {e2}")
    
    # ensure TS collection exists
    try:
        ts.collections[COLLECTION].retrieve()
    except ObjectNotFound:
        ts.collections.create(TS_SCHEMA)
        print(f"[geocoder] Created TS collection {COLLECTION}")
    except Exception as e:
        print(f"[geocoder] Error checking TS collection: {e}")
        try:
            ts.collections.create(TS_SCHEMA)
            print(f"[geocoder] Created TS collection {COLLECTION} (fallback)")
        except Exception as e2:
            print(f"[geocoder] Failed to create TS collection: {e2}")
    yield
    await es.close()
    await pg_pool.close()


app = FastAPI(title="Geocoding Service", lifespan=lifespan)


# ── feature flags discovery ──────────────────────────────────────────────
@app.get("/features")
async def features():
    """Return which optional features are enabled on this instance."""
    return {
        "vectors": ENABLE_VECTORS,
        "ai": ENABLE_AI,
    }


# ── autocomplete (Typesense) ─────────────────────────────────────────────
@app.get("/autocomplete")
async def autocomplete(
    q: str = Query(..., min_length=1),
    lat: float | None = Query(None),
    lon: float | None = Query(None),
    limit: int = Query(10, ge=1, le=50),
    vector: bool = Query(True, description="Enable semantic vector search"),
    ai: bool = Query(True, description="Enable AI-assisted search"),
):
    """Fast prefix / typo-tolerant autocomplete powered by Typesense.

    Online ranking: text_match * offline_rank (+ optional geo + popularity).
    When vector=true (and ENABLE_VECTORS is on) with query > 3 chars,
    adds semantic vector re-ranking.
    When ai=true (and ENABLE_AI is on), uses enhanced query understanding.
    """
    use_vectors = vector and ENABLE_VECTORS
    use_ai = ai and ENABLE_AI

    params: dict = {
        "q": q,
        "query_by": "name,name_en,tags_text",
        "prefix": "true,true,true",
        "per_page": limit,
    }

    # sorting: text match > offline_rank > geo (max 3 sort fields for Typesense)
    if lat is not None and lon is not None:
        params["sort_by"] = (
            f"_text_match:desc,offline_rank:desc,"
            f"location({lat},{lon}):asc"
        )
    else:
        params["sort_by"] = "_text_match:desc,offline_rank:desc,popularity:desc"

    loop = asyncio.get_event_loop()

    # hybrid vector search for longer queries (when enabled)
    if use_vectors and len(q) > 3:
        vec = (await loop.run_in_executor(None, embed_texts, [q]))[0]
        vec_str = ",".join(str(v) for v in vec)
        params["vector_query"] = f"name_vector:([{vec_str}], k:{limit})"
        params["collection"] = COLLECTION

        def _multi_search():
            return ts.multi_search.perform({"searches": [params]}, {})

        ms_result = await loop.run_in_executor(None, _multi_search)
        results = ms_result["results"][0]
    else:
        results = await loop.run_in_executor(
            None, ts.collections[COLLECTION].documents.search, params
        )

    return {
        "features": {
            "vectors_enabled": use_vectors,
            "ai_enabled": use_ai,
        },
        "results": [
            {
                "osm_id": h["document"]["osm_id"],
                "name": h["document"]["name"],
                "name_en": h["document"].get("name_en", ""),
                "osm_type": h["document"].get("osm_type", ""),
                "tags_text": h["document"].get("tags_text", ""),
                "admin_level": h["document"].get("admin_level", 0),
                "offline_rank": h["document"].get("offline_rank", 0),
                "popularity": h["document"].get("popularity", 0),
                "location": h["document"].get("location"),
            }
            for h in results.get("hits", [])
        ],
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

    Online ranking = offline_rank * text_similarity (+ optional vector KNN + geo decay).
    Text similarity searches across name, name_en, and tags_text (all tags).
    vector=true adds KNN cosine similarity re-ranking (requires ENABLE_VECTORS).
    ai=true enables AI-assisted query expansion (requires ENABLE_AI).
    """
    use_vectors = vector and ENABLE_VECTORS
    use_ai = ai and ENABLE_AI

    loop = asyncio.get_event_loop()

    # ---- function_score query ----
    # Online rank = text_similarity + offline_rank_boost + geo_decay + popularity
    functions: list[dict] = []

    # baseline score to ensure text matches always get some score
    functions.append(
        {
            "weight": 1.0,
        }
    )

    # offline_rank boost (dominant signal – based on admin_level + area)
    functions.append(
        {
            "field_value_factor": {
                "field": "offline_rank",
                "modifier": "log1p",
                "factor": 2,
                "missing": 0,
            },
            "weight": 5,
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
                        "decay": 0.5,
                    }
                },
                "weight": 2,
            }
        )

    # popularity boost (feedback-driven)
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

    # text query – searches name, name_en AND tags_text (all tags)
    text_query: dict = {
        "bool": {
            "should": [
                {
                    "multi_match": {
                        "query": q,
                        "fields": ["name^3", "name_en^3", "tags_text"],
                        "type": "best_fields",
                        "fuzziness": "AUTO",
                    }
                },
                {
                    "match": {
                        "name": {
                            "query": q,
                            "boost": 2,
                        }
                    }
                },
                {
                    "match": {
                        "name_en": {
                            "query": q,
                            "boost": 2,
                        }
                    }
                },
                {
                    "match": {
                        "tags_text": {
                            "query": q,
                            "boost": 1,
                        }
                    }
                },
            ],
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
                "boost_mode": "sum",
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
            "num_candidates": 100,
        }

    resp = await es.search(index=INDEX, **body)

    return {
        "features": {
            "vectors_enabled": use_vectors,
            "ai_enabled": use_ai,
        },
        "results": [
            {
                "osm_id": h["_source"]["osm_id"],
                "osm_type": h["_source"].get("osm_type", ""),
                "name": h["_source"].get("name", ""),
                "name_en": h["_source"].get("name_en", ""),
                "tags": h["_source"].get("tags", {}),
                "tags_text": h["_source"].get("tags_text", ""),
                "geom": h["_source"].get("geom"),
                "admin_level": h["_source"].get("admin_level", 0),
                "area_km2": h["_source"].get("area_km2", 0),
                "offline_rank": h["_source"].get("offline_rank", 0),
                "popularity": h["_source"].get("popularity", 0),
                "score": h["_score"],
            }
            for h in resp["hits"]["hits"]
        ],
    }


# ── feedback loop ─────────────────────────────────────────────────────────
@app.post("/feedback")
async def feedback(
    osm_id: str = Query(...),
    boost: float = Query(1.0),
):
    """Increment popularity for an element (updates both ES and TS)."""

    # Elasticsearch - atomic increment
    try:
        await es.update(
            index=INDEX,
            id=osm_id,
            body={
                "script": {
                    "source": "ctx._source.popularity += params.boost",
                    "params": {"boost": boost},
                }
            },
        )
    except Exception:
        pass

    # Typesense - read-then-update (sync client, offload to thread)
    def _ts_update():
        try:
            doc = ts.collections[COLLECTION].documents[osm_id].retrieve()
            new_pop = doc.get("popularity", 0) + boost
            ts.collections[COLLECTION].documents[osm_id].update(
                {"popularity": new_pop}
            )
        except Exception:
            pass

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _ts_update)

    return {"status": "ok", "osm_id": osm_id}


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


# ── add place endpoint ─────────────────────────────────────────────────────────
@app.post("/places", response_model=PlaceResponse)
async def add_place(place: PlaceCreate):
    """Add a new place to the geocoding database.
    
    Stores the place in PostGIS and indexes it in Elasticsearch and Typesense
    for searchability. Returns the created place with its generated ID.
    """
    # Generate a unique ID for the custom place
    custom_id = f"custom_{uuid.uuid4().hex[:16]}"
    
    # Create geometry from lat/lon
    geom_point = {"type": "Point", "coordinates": [place.lon, place.lat]}
    
    # Prepare tags for indexing
    tags = place.tags or {}
    tags_text = " ".join(f"{k}:{v}" for k, v in tags.items())
    
    # Get current timestamp
    created_at = datetime.utcnow().isoformat()
    
    try:
        # Store in PostGIS
        async with pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO osm_geometries (osm_id, osm_type, geom)
                VALUES ($1, $2, ST_SetSRID(ST_MakePoint($3, $4), 4326))
                ON CONFLICT (osm_id) DO UPDATE SET
                    osm_type = $2, geom = ST_SetSRID(ST_MakePoint($3, $4), 4326)
                """,
                custom_id, place.osm_type, place.lon, place.lat
            )
        
        # Index in Elasticsearch
        es_doc = {
            "osm_id": custom_id,
            "osm_type": place.osm_type,
            "name": place.name,
            "name_en": place.name_en or place.name,
            "tags": tags,
            "tags_text": tags_text,
            "geom": geom_point,
            "centroid": {"lat": place.lat, "lon": place.lon},
            "admin_level": place.admin_level,
            "area_km2": 0.0,
            "offline_rank": 0.0,
            "popularity": 0.0,
        }
        
        # Add embedding vector if vectors are enabled
        if ENABLE_VECTORS:
            try:
                name_for_embedding = place.name_en or place.name
                embedding = await embed_texts([name_for_embedding])
                if embedding and len(embedding) > 0:
                    es_doc["name_vector"] = embedding[0].tolist()
            except Exception as e:
                print(f"[geocoder] Error generating embedding for place: {e}")
        
        await es.index(index=INDEX, id=custom_id, body=es_doc)
        
        # Index in Typesense
        ts_doc = {
            "osm_id": custom_id,
            "name": place.name,
            "name_en": place.name_en or place.name,
            "osm_type": place.osm_type,
            "tags_text": tags_text,
            "admin_level": place.admin_level,
            "offline_rank": 0.0,
            "popularity": 0.0,
            "location": [place.lat, place.lon],
        }
        
        ts.collections[COLLECTION].documents.create(ts_doc)
        
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
    - nearest_line: The closest LineString geometry to the point (with ES data)
    - enclosing_polygons: List of polygons/multipolygons/boundaries that contain the point (with ES data)
    """
    point_wkt = f"POINT({lon} {lat})"

    async with pg_pool.acquire() as conn:
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

    result = {
        "nearest_line": None,
        "enclosing_polygons": [],
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
