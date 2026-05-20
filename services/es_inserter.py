"""Consume OSM elements from NATS JS and index into Elasticsearch.

Stored per element:
  - osm_id, osm_type (keyword)
  - name, name_en     (text – for matching)
  - tags_text         (text – all tag values concatenated, for full-tag search)
  - tags              (object – raw tags stored for retrieval)
  - geom              (geo_shape)
  - centroid          (geo_point – for function_score geo decay)
  - name_vector       (dense_vector – cosine similarity)
  - admin_level       (integer)
  - area_km2          (float – polygon area)
  - offline_rank      (float – pre-computed importance)
  - popularity        (float – feedback-driven, starts at 0)
"""

import asyncio
import json

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from shared.config import ELASTICSEARCH_URL, EMBEDDING_DIM, ENABLE_VECTORS, BATCH_SIZE, MAX_CONCURRENT_BATCHES
from shared.nats_client import connect, subscribe, is_transient_error
from shared.ranking import compute_offline_rank

INDEX = "osm_places"

MAPPING = {
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


def _centroid(geom: dict) -> dict | None:
    """Return {lat, lon} centroid from a GeoJSON geometry, or None."""
    if not geom:
        return None
    gtype = geom["type"]
    coords = geom["coordinates"]
    if gtype == "Point":
        return {"lat": coords[1], "lon": coords[0]}
    if gtype == "LineString":
        if not coords:
            return None
        avg_lat = sum(c[1] for c in coords) / len(coords)
        avg_lon = sum(c[0] for c in coords) / len(coords)
        return {"lat": avg_lat, "lon": avg_lon}
    if gtype == "Polygon":
        if not coords or not coords[0]:
            return None
        pts = coords[0]  # exterior ring
        avg_lat = sum(c[1] for c in pts) / len(pts)
        avg_lon = sum(c[0] for c in pts) / len(pts)
        return {"lat": avg_lat, "lon": avg_lon}
    if gtype == "MultiPolygon":
        if not coords:
            return None
        # Collect all points from all polygons
        all_pts = []
        for polygon in coords:
            if polygon and polygon[0]:
                all_pts.extend(polygon[0])
        if not all_pts:
            return None
        avg_lat = sum(c[1] for c in all_pts) / len(all_pts)
        avg_lon = sum(c[0] for c in all_pts) / len(all_pts)
        return {"lat": avg_lat, "lon": avg_lon}
    return None


async def ensure_index(es: AsyncElasticsearch):
    max_retries = 5
    retry_delay = 3
    
    for attempt in range(max_retries):
        try:
            if not await es.indices.exists(index=INDEX):
                await es.indices.create(index=INDEX, **MAPPING)
                print(f"[es-inserter] Created index {INDEX}", flush=True)
            else:
                print(f"[es-inserter] Index {INDEX} already exists", flush=True)
            return
        except Exception as e:
            print(f"[es-inserter] Error checking/creating ES index (attempt {attempt + 1}/{max_retries}): {e}", flush=True)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                # Final attempt - try to create anyway
                try:
                    await es.indices.create(index=INDEX, **MAPPING)
                    print(f"[es-inserter] Created ES index {INDEX} (fallback)", flush=True)
                except Exception as e2:
                    print(f"[es-inserter] Failed to create ES index: {e2}", flush=True)
                    # If index already exists, that's fine
                    if "resource_already_exists" not in str(e2).lower():
                        raise


async def run():
    # Retry logic for connecting to Elasticsearch
    max_retries = 10
    retry_delay = 2
    
    es = None
    for attempt in range(max_retries):
        try:
            es = AsyncElasticsearch(ELASTICSEARCH_URL)
            # Test ES connection
            await es.ping()
            print(f"[es-inserter] Successfully connected to Elasticsearch", flush=True)
            break
        except Exception as e:
            print(f"[es-inserter] Failed to connect to Elasticsearch (attempt {attempt + 1}/{max_retries}): {e}", flush=True)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                raise
    else:
        raise Exception("Failed to connect to Elasticsearch after maximum retries")
    
    await ensure_index(es)

    # Pre-load embedding model before starting workers to avoid concurrent download conflicts
    if ENABLE_VECTORS:
        print("[es-inserter] Pre-loading embedding model...", flush=True)
        from shared.embeddings import get_model
        get_model()  # Force model download/load
        print("[es-inserter] Embedding model loaded", flush=True)

    nc, js = await connect()
    sub = await subscribe(js, "es-consumer")
    print("[es-inserter] Subscription created, listening for messages ...", flush=True)

    # Create worker pool for concurrent batch processing
    async def worker(worker_id: int):
        """Worker that fetches and processes batches concurrently."""
        while True:
            max_fetch_retries = 5
            for fetch_attempt in range(max_fetch_retries):
                try:
                    msgs = await asyncio.wait_for(sub.fetch(batch=BATCH_SIZE, timeout=5), timeout=10)
                    print(f"[es-inserter] Worker {worker_id}: Fetched {len(msgs)} messages", flush=True)
                    break
                except asyncio.TimeoutError:
                    print(f"[es-inserter] Worker {worker_id}: Fetch timeout", flush=True)
                    await asyncio.sleep(1)
                    if fetch_attempt < max_fetch_retries - 1:
                        continue
                    else:
                        # Timeout on final attempt, just continue to next iteration
                        break
                except Exception as e:
                    is_transient = is_transient_error(e)
                    print(f"[es-inserter] Worker {worker_id}: Fetch error (attempt {fetch_attempt + 1}/{max_fetch_retries}): {e} (transient: {is_transient})", flush=True)
                    
                    if is_transient and fetch_attempt < max_fetch_retries - 1:
                        # Exponential backoff for transient errors
                        delay = min(1 * (2 ** fetch_attempt), 10)  # Cap at 10 seconds
                        await asyncio.sleep(delay)
                        continue
                    else:
                        # Non-transient error or final attempt failed
                        await asyncio.sleep(1)
                        if fetch_attempt < max_fetch_retries - 1:
                            continue
                        else:
                            # Break outer loop on final attempt
                            break
            
            # If we didn't get messages, continue to next iteration
            if 'msgs' not in locals() or not msgs:
                continue

            elements = []
            for msg in msgs:
                elements.append(json.loads(msg.data))
                await msg.ack()

            print(f"[es-inserter] Worker {worker_id}: Parsed {len(elements)} elements", flush=True)
            
            if not elements:
                continue

            # Import embedding functions here to avoid slow startup
            from shared.embeddings import embed_texts, build_text

            # build searchable text from ALL tags for each element
            texts = [build_text(e["tags"]) for e in elements]
            non_empty = [(i, t) for i, t in enumerate(texts) if t]

            # compute vectors only when ENABLE_VECTORS is on
            vectors: list[list[float] | None] = [None] * len(elements)
            if ENABLE_VECTORS and non_empty:
                indices, batch_texts = zip(*non_empty)
                batch_vecs = embed_texts(list(batch_texts))
                for idx, vec in zip(indices, batch_vecs):
                    vectors[idx] = vec

            # prepare bulk actions
            actions = []
            for elem, txt, vec in zip(elements, texts, vectors):
                tags = elem["tags"]
                admin_level = elem.get("admin_level", 0)
                area_km2 = elem.get("area_km2", 0.0)
                rank = compute_offline_rank(tags, admin_level, area_km2)

                doc = {
                    "_index": INDEX,
                    "_id": elem["osm_id"],
                    "osm_id": elem["osm_id"],
                    "osm_type": elem.get("osm_type", ""),
                    "name": tags.get("name", ""),
                    "name_en": tags.get("name:en", ""),
                    "tags_text": txt,
                    "tags": tags,
                    "admin_level": admin_level,
                    "area_km2": area_km2,
                    "offline_rank": rank,
                    "popularity": 0.0,
                }
                if elem.get("geom"):
                    doc["geom"] = elem["geom"]
                    c = _centroid(elem["geom"])
                    if c:
                        doc["centroid"] = c
                if vec is not None:
                    doc["name_vector"] = vec
                actions.append(doc)

            await async_bulk(es, actions, raise_on_error=False)
            print(f"[es-inserter] Worker {worker_id}: Indexed {len(actions)} docs", flush=True)

    # Spawn multiple workers
    workers = [asyncio.create_task(worker(i)) for i in range(MAX_CONCURRENT_BATCHES)]
    print(f"[es-inserter] Started {MAX_CONCURRENT_BATCHES} concurrent workers", flush=True)
    
    # Wait for all workers (they run indefinitely)
    await asyncio.gather(*workers)

    await es.close()
    await nc.close()


if __name__ == "__main__":
    asyncio.run(run())
