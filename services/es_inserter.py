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
  Address fields (all optional):
  - addr_housenumber  (keyword – exact house/building number)
  - addr_street       (text + keyword – street name)
  - addr_city         (text + keyword – city/town/village)
  - addr_postcode     (keyword – postal code)
  - addr_country      (keyword – ISO country code)
  - addr_suburb       (text – suburb or neighbourhood)
  - addr_state        (text – state / governorate)
  - full_address      (text – normalised human-readable address string)
  - has_address       (boolean – quick filter for elements with addr: data)
"""

import asyncio
import json

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from shared.config import ELASTICSEARCH_URL, EMBEDDING_DIM, ENABLE_VECTORS, BATCH_SIZE, MAX_CONCURRENT_BATCHES
from shared.nats_client import connect, subscribe, is_transient_error, is_connection_error, reconnect
from shared.ranking import compute_offline_rank
from shared.centroid import centroid_latlon
from shared.address import extract_address_components, build_full_address, has_address

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
    
    if es is None:
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

    # Use mutable containers for connection objects so workers can update them
    conn_state = {"nc": nc, "js": js, "sub": sub}

    # Create worker pool for concurrent batch processing
    async def worker(worker_id: int):
        """Worker that fetches and processes batches concurrently."""
        while True:
            msgs = None
            max_fetch_retries = 5
            for fetch_attempt in range(max_fetch_retries):
                try:
                    msgs = await conn_state["sub"].fetch(batch=BATCH_SIZE, timeout=30)
                    print(f"[es-inserter] Worker {worker_id}: Fetched {len(msgs)} messages", flush=True)
                    break
                except asyncio.TimeoutError:
                    print(f"[es-inserter] Worker {worker_id}: Fetch timeout", flush=True)
                    await asyncio.sleep(1)
                    continue
                except Exception as e:
                    is_conn_err = is_connection_error(e)
                    is_transient = is_transient_error(e)
                    print(f"[es-inserter] Worker {worker_id}: Fetch error (attempt {fetch_attempt + 1}/{max_fetch_retries}): {e} (transient: {is_transient}, connection_error: {is_conn_err})", flush=True)
                    
                    if is_conn_err:
                        # Connection is broken, need to reconnect
                        print(f"[es-inserter] Worker {worker_id}: Connection error detected, reconnecting...", flush=True)
                        try:
                            conn_state["nc"], conn_state["js"] = await reconnect(conn_state["nc"], conn_state["js"])
                            conn_state["sub"] = await subscribe(conn_state["js"], "es-consumer")
                            print(f"[es-inserter] Worker {worker_id}: Reconnected and resubscribed", flush=True)
                            break  # Retry fetch with new connection
                        except Exception as reconnect_err:
                            print(f"[es-inserter] Worker {worker_id}: Reconnection failed: {reconnect_err}", flush=True)
                            await asyncio.sleep(5)
                            continue
                    elif is_transient and fetch_attempt < max_fetch_retries - 1:
                        # Exponential backoff for transient errors
                        delay = min(1 * (2 ** fetch_attempt), 10)  # Cap at 10 seconds
                        await asyncio.sleep(delay)
                    else:
                        await asyncio.sleep(1)
            
            if not msgs:
                continue

            # Parse messages (don't ack yet)
            elements = []
            for msg in msgs:
                elements.append(json.loads(msg.data))

            print(f"[es-inserter] Worker {worker_id}: Parsed {len(elements)} elements", flush=True)
            
            if not elements:
                # Ack empty/unparseable messages
                for msg in msgs:
                    await msg.ack()
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

                # address fields
                addr = extract_address_components(tags)
                full_addr = build_full_address(tags)

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
                    # address
                    "has_address": bool(full_addr),
                    "full_address": full_addr,
                    "addr_housenumber": addr.get("housenumber", ""),
                    "addr_street":      addr.get("street", ""),
                    "addr_city":        addr.get("city", ""),
                    "addr_postcode":    addr.get("postcode", ""),
                    "addr_country":     addr.get("country", ""),
                    "addr_suburb":      addr.get("suburb", ""),
                    "addr_state":       addr.get("state", ""),
                }
                if elem.get("geom"):
                    doc["geom"] = elem["geom"]
                    c = centroid_latlon(elem["geom"])
                    if c:
                        doc["centroid"] = c
                if vec is not None:
                    doc["name_vector"] = vec
                actions.append(doc)

            await async_bulk(es, actions, raise_on_error=False)
            print(f"[es-inserter] Worker {worker_id}: Indexed {len(actions)} docs", flush=True)

            # Ack messages only after successful indexing
            for msg in msgs:
                await msg.ack()

    # Spawn multiple workers
    workers = [asyncio.create_task(worker(i)) for i in range(MAX_CONCURRENT_BATCHES)]
    print(f"[es-inserter] Started {MAX_CONCURRENT_BATCHES} concurrent workers", flush=True)
    
    # Wait for all workers (they run indefinitely)
    await asyncio.gather(*workers)

    await es.close()
    await conn_state["nc"].close()


if __name__ == "__main__":
    asyncio.run(run())
