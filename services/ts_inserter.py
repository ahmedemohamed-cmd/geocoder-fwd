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

from shared.config import TYPESENSE_HOST, TYPESENSE_PORT, TYPESENSE_API_KEY, EMBEDDING_DIM, ENABLE_VECTORS
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
    if gtype in ("LineString", "Polygon"):
        pts = coords[0] if gtype == "Polygon" else coords
        if not pts:
            return None
        avg_lat = sum(c[1] for c in pts) / len(pts)
        avg_lon = sum(c[0] for c in pts) / len(pts)
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

    nc, js = await connect()
    sub = await subscribe(js, "ts-consumer")
    loop = asyncio.get_event_loop()
    print("[ts-inserter] Subscription created, listening for messages ...", flush=True)

    iteration = 0
    while True:
        iteration += 1
        if iteration % 10 == 0:
            print(f"[ts-inserter] Loop iteration {iteration}", flush=True)
        try:
            msgs = await asyncio.wait_for(sub.fetch(batch=100, timeout=5), timeout=10)
            print(f"[ts-inserter] Fetched {len(msgs)} messages", flush=True)
        except asyncio.TimeoutError:
            print(f"[ts-inserter] Fetch timeout", flush=True)
            await asyncio.sleep(1)
            continue
        except Exception as e:
            print(f"[ts-inserter] Fetch error: {e}", flush=True)
            await asyncio.sleep(1)
            continue

        elements = []
        for msg in msgs:
            elements.append(json.loads(msg.data))
            await msg.ack()

        print(f"[ts-inserter] Parsed {len(elements)} elements", flush=True)
        
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
                print(f"[ts-inserter] import error: {exc}")

        await loop.run_in_executor(None, _import)
        print(f"[ts-inserter] Imported {len(docs)} docs", flush=True)

    await nc.close()


if __name__ == "__main__":
    asyncio.run(run())
