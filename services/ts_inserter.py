"""Consume OSM elements from NATS JS and upsert into Typesense.

Stored per element:
  - osm_id, name, name_en, osm_type, tags_text (strings)
  - location      (geopoint – [lat, lon])
  - name_vector   (float[] – for semantic search)
  - admin_level   (int32, facet)
  - offline_rank  (float – pre-computed importance, default sort)
  - popularity    (float – feedback-driven)
"""

import asyncio
import json

import typesense
from typesense.exceptions import ObjectNotFound

from shared.config import TYPESENSE_HOST, TYPESENSE_PORT, TYPESENSE_API_KEY, EMBEDDING_DIM, ENABLE_VECTORS, BATCH_SIZE, MAX_CONCURRENT_BATCHES
from shared.nats_client import connect, subscribe
from shared.ranking import compute_offline_rank

COLLECTION = "osm_places"


def ts_client() -> typesense.Client:
    return typesense.Client(
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


def ensure_collection(client: typesense.Client):
    try:
        client.collections[COLLECTION].retrieve()
    except ObjectNotFound:
        try:
            client.collections.create(
                {
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
            )
            print(f"[ts-inserter] Created collection {COLLECTION}", flush=True)
        except Exception as e:
            print(f"[ts-inserter] Failed to create collection: {e}", flush=True)
            if "already exists" not in str(e).lower():
                raise
    except Exception as e:
        print(f"[ts-inserter] Error checking collection: {e}", flush=True)
        # Try to create the collection anyway
        try:
            client.collections.create(
                {
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
            )
            print(f"[ts-inserter] Created collection {COLLECTION} (fallback)", flush=True)
        except Exception as e2:
            print(f"[ts-inserter] Failed to create collection (fallback): {e2}", flush=True)
            if "already exists" not in str(e2).lower():
                raise


def _centroid(geom: dict) -> list[float] | None:
    """Return [lat, lon] centroid from a GeoJSON geometry, or None."""
    if not geom:
        return None
    gtype = geom["type"]
    coords = geom["coordinates"]
    if gtype == "Point":
        return [coords[1], coords[0]]
    if gtype == "LineString":
        if not coords:
            return None
        avg_lat = sum(c[1] for c in coords) / len(coords)
        avg_lon = sum(c[0] for c in coords) / len(coords)
        return [avg_lat, avg_lon]
    if gtype == "Polygon":
        if not coords or not coords[0]:
            return None
        pts = coords[0]  # exterior ring
        avg_lat = sum(c[1] for c in pts) / len(pts)
        avg_lon = sum(c[0] for c in pts) / len(pts)
        return [avg_lat, avg_lon]
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
        return [avg_lat, avg_lon]
    return None


async def run():
    # Retry logic for connecting to Typesense
    max_retries = 10
    retry_delay = 2
    
    client = None
    for attempt in range(max_retries):
        try:
            client = ts_client()
            # Test TS connection
            client.collections.retrieve()
            print(f"[ts-inserter] Successfully connected to Typesense", flush=True)
            break
        except Exception as e:
            print(f"[ts-inserter] Failed to connect to Typesense (attempt {attempt + 1}/{max_retries}): {e}", flush=True)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                raise
    else:
        raise Exception("Failed to connect to Typesense after maximum retries")
    
    ensure_collection(client)

    # Pre-load embedding model before starting workers to avoid concurrent download conflicts
    if ENABLE_VECTORS:
        print("[ts-inserter] Pre-loading embedding model...", flush=True)
        from shared.embeddings import get_model
        get_model()  # Force model download/load
        print("[ts-inserter] Embedding model loaded", flush=True)

    nc, js = await connect()
    sub = await subscribe(js, "ts-consumer")
    loop = asyncio.get_event_loop()
    print("[ts-inserter] Subscription created, listening for messages ...", flush=True)

    # Create worker pool for concurrent batch processing
    async def worker(worker_id: int):
        """Worker that fetches and processes batches concurrently."""
        while True:
            try:
                msgs = await asyncio.wait_for(sub.fetch(batch=BATCH_SIZE, timeout=5), timeout=10)
                print(f"[ts-inserter] Worker {worker_id}: Fetched {len(msgs)} messages", flush=True)
            except asyncio.TimeoutError:
                print(f"[ts-inserter] Worker {worker_id}: Fetch timeout", flush=True)
                await asyncio.sleep(1)
                continue
            except Exception as e:
                print(f"[ts-inserter] Worker {worker_id}: Fetch error: {e}", flush=True)
                await asyncio.sleep(1)
                continue

            elements = []
            for msg in msgs:
                elements.append(json.loads(msg.data))
                await msg.ack()

            print(f"[ts-inserter] Worker {worker_id}: Parsed {len(elements)} elements", flush=True)
            
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

            docs = []
            for elem, vec in zip(elements, vectors):
                tags = elem["tags"]
                admin_level = elem.get("admin_level", 0)
                area_km2 = elem.get("area_km2", 0.0)
                rank = compute_offline_rank(tags, admin_level, area_km2)

                doc: dict = {
                    "id": elem["osm_id"],
                    "osm_id": elem["osm_id"],
                    "name": tags.get("name", ""),
                    "name_en": tags.get("name:en", ""),
                    "osm_type": elem.get("osm_type", ""),
                    "tags_text": build_text(tags),
                    "admin_level": admin_level,
                    "offline_rank": rank,
                    "popularity": 0.0,
                }
                loc = _centroid(elem.get("geom"))
                if loc is not None:
                    doc["location"] = loc
                if vec is not None:
                    doc["name_vector"] = vec
                docs.append(doc)

            def _import():
                try:
                    client.collections[COLLECTION].documents.import_(
                        docs, {"action": "upsert"}
                    )
                except Exception as exc:
                    print(f"[ts-inserter] Worker {worker_id}: import error: {exc}")

            await loop.run_in_executor(None, _import)
            print(f"[ts-inserter] Worker {worker_id}: Imported {len(docs)} docs", flush=True)

    # Spawn multiple workers
    workers = [asyncio.create_task(worker(i)) for i in range(MAX_CONCURRENT_BATCHES)]
    print(f"[ts-inserter] Started {MAX_CONCURRENT_BATCHES} concurrent workers", flush=True)
    
    # Wait for all workers (they run indefinitely)
    await asyncio.gather(*workers)

    await nc.close()


if __name__ == "__main__":
    asyncio.run(run())
