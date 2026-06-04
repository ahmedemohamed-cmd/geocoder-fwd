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
import random
import time as _time

import nats.errors
from elasticsearch import AsyncElasticsearch, ApiError
from elasticsearch.helpers import async_bulk
from elastic_transport import ConnectionTimeout

from shared.config import ELASTICSEARCH_URL, EMBEDDING_DIM, ENABLE_VECTORS, BATCH_SIZE, MAX_CONCURRENT_BATCHES
from shared.nats_client import connect, subscribe, is_transient_error, is_connection_error, reconnect
from shared.ranking import compute_offline_rank
from shared.centroid import centroid_latlon
from shared.address import extract_address_components, build_full_address, has_address, normalize_address_text

INDEX = "osm_places"

MAPPING = {
    "settings": {
        "index": {"number_of_replicas": 0},
        "analysis": {
            "char_filter": {
                # Normalize Arabic characters at char level before tokenization
                "arabic_normalize_char": {
                    "type": "pattern_replace",
                    "pattern": "[\u0640]",  # tatweel
                    "replacement": "",
                },
            },
            "filter": {
                # English street-type synonyms (bidirectional)
                "street_synonyms_en": {
                    "type": "synonym",
                    "synonyms": [
                        "st, street",
                        "rd, road",
                        "ave, av, avenue",
                        "blvd, bvd, boulevard",
                        "ln, lane",
                        "dr, drive",
                        "pl, place",
                        "ct, court",
                        "sq, square",
                        "hwy, highway",
                        "cres, crescent",
                        "terr, terrace",
                        "pkwy, parkway",
                    ],
                },
                # Arabic street-type synonyms
                "street_synonyms_ar": {
                    "type": "synonym",
                    "synonyms": [
                        "ش, شارع",
                        "ط, طريق",
                        "م, ميدان",
                    ],
                },
                # French street-type synonyms
                "street_synonyms_fr": {
                    "type": "synonym",
                    "synonyms": [
                        "r, rue",
                        "av, ave, avenue",
                        "bd, blvd, boulevard",
                        "pl, place",
                        "ch, chemin",
                        "imp, impasse",
                        "all, allée",
                        "crs, cours",
                        "rte, route",
                        "pass, passage",
                    ],
                },
                # Edge n-gram for autocomplete / prefix matching
                "edge_ngram_filter": {
                    "type": "edge_ngram",
                    "min_gram": 2,
                    "max_gram": 15,
                },
                # Arabic normalization (alef, taa marbuta, etc.)
                "arabic_normalization": {
                    "type": "arabic_normalization",
                },
            },
            "normalizer": {
                "lowercase": {
                    "type": "custom",
                    "filter": ["lowercase"],
                },
            },
            "analyzer": {
                # Primary address analyzer: synonyms + lowercase
                "address_standard": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "char_filter": ["arabic_normalize_char"],
                    "filter": [
                        "lowercase",
                        "arabic_normalization",
                        "street_synonyms_en",
                        "street_synonyms_ar",
                        "street_synonyms_fr",
                    ],
                },
                # Edge n-gram analyzer for autocomplete (index-time only)
                "address_autocomplete": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "char_filter": ["arabic_normalize_char"],
                    "filter": [
                        "lowercase",
                        "arabic_normalization",
                        "street_synonyms_en",
                        "street_synonyms_ar",
                        "street_synonyms_fr",
                        "edge_ngram_filter",
                    ],
                },
                # Search analyzer: same as address_standard but NO edge n-gram
                "address_search": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "char_filter": ["arabic_normalize_char"],
                    "filter": [
                        "lowercase",
                        "arabic_normalization",
                        "street_synonyms_en",
                        "street_synonyms_ar",
                        "street_synonyms_fr",
                    ],
                },
                # Arabic-optimized analyzer for name fields
                "arabic_name": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "char_filter": ["arabic_normalize_char"],
                    "filter": [
                        "lowercase",
                        "arabic_normalization",
                    ],
                },
            },
        },
    },
    "mappings": {
        "properties": {
            "osm_id": {"type": "keyword"},
            "osm_type": {"type": "keyword"},
            "name": {
                "type": "text",
                "analyzer": "arabic_name",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "address_autocomplete",
                        "search_analyzer": "address_search",
                    },
                },
            },
            "name_en": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "address_autocomplete",
                        "search_analyzer": "address_search",
                    },
                },
            },
            "name_fr": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "address_autocomplete",
                        "search_analyzer": "address_search",
                    },
                },
            },
            "tags_text": {
                "type": "text",
                "analyzer": "arabic_name",
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
            "addr_housenumber": {
                "type": "keyword",
                "normalizer": "lowercase",
            },
            "addr_street": {
                "type": "text",
                "analyzer": "address_standard",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "address_autocomplete",
                        "search_analyzer": "address_search",
                    },
                },
            },
            "addr_city": {
                "type": "text",
                "analyzer": "address_standard",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "address_autocomplete",
                        "search_analyzer": "address_search",
                    },
                },
            },
            "addr_postcode": {"type": "keyword"},
            "addr_country":  {"type": "keyword"},
            "addr_suburb": {
                "type": "text",
                "analyzer": "address_standard",
                "fields": {
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "address_autocomplete",
                        "search_analyzer": "address_search",
                    },
                },
            },
            "addr_state": {
                "type": "text",
                "analyzer": "address_standard",
            },
            "full_address": {
                "type": "text",
                "analyzer": "address_standard",
                "fields": {
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "address_autocomplete",
                        "search_analyzer": "address_search",
                    },
                },
            },
            "has_address": {"type": "boolean"},
            # AI-generated place description (cached, not indexed)
            "ai_description": {"type": "object", "enabled": False},
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


async def set_bulk_mode(es: AsyncElasticsearch, enabled: bool):
    """Toggle index settings optimised for bulk ingest vs. normal query serving."""
    if enabled:
        settings = {
            "refresh_interval": "60s",
            "translog.durability": "async",
            "translog.flush_threshold_size": "1gb",
            "translog.sync_interval": "30s",
            # Suppress merges during ingest — tolerate many segments
            "merge.scheduler.max_thread_count": 1,
            "merge.scheduler.auto_throttle": False,
            "merge.policy.segments_per_tier": 50,
            "merge.policy.max_merged_segment": "50gb",
            "merge.policy.max_merge_at_once": 2,
        }
        label = "BULK"
    else:
        settings = {
            "refresh_interval": "5s",
            "translog.durability": "request",
            "translog.flush_threshold_size": "512mb",
            "translog.sync_interval": "5s",
            # Restore normal merge behaviour
            "merge.scheduler.max_thread_count": 1,
            "merge.scheduler.auto_throttle": True,
            "merge.policy.segments_per_tier": 10,
            "merge.policy.max_merged_segment": "5gb",
            "merge.policy.max_merge_at_once": 10,
        }
        label = "NORMAL"
    try:
        await es.indices.put_settings(index=INDEX, settings={"index": settings})
        print(f"[es-inserter] Index mode set to {label}", flush=True)
    except Exception as e:
        print(f"[es-inserter] Warning: could not set {label} mode: {e}", flush=True)


async def run():
    import time as _time

    # Retry logic for connecting to Elasticsearch
    max_retries = 10
    retry_delay = 2
    
    es = None
    for attempt in range(max_retries):
        try:
            es = AsyncElasticsearch(ELASTICSEARCH_URL, request_timeout=60)
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
    await set_bulk_mode(es, enabled=True)

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
    reconnect_lock = asyncio.Lock()

    # ── Pipeline parallelism ──────────────────────────────────────────────
    # A single fetcher goroutine pulls batches from NATS and places them on
    # an asyncio.Queue.  Multiple worker coroutines pop from the queue,
    # process (embed + rank), and bulk-insert into Elasticsearch.
    # This avoids concurrent .fetch() calls on the same pull subscription
    # and cleanly separates I/O (fetch/ack) from CPU-bound work (embed).
    work_queue: asyncio.Queue[list] = asyncio.Queue(maxsize=MAX_CONCURRENT_BATCHES * 2)

    in_bulk_mode = True       # tracks whether we've applied bulk settings
    consecutive_empty = 0     # number of consecutive empty fetches

    async def fetcher():
        """Single fetcher that pulls batches from NATS into the work queue."""
        nonlocal in_bulk_mode, consecutive_empty
        while True:
            max_fetch_retries = 5
            msgs = None
            for fetch_attempt in range(max_fetch_retries):
                try:
                    msgs = await conn_state["sub"].fetch(batch=BATCH_SIZE, timeout=30)
                    break
                except nats.errors.TimeoutError:
                    msgs = []
                    break
                except Exception as e:
                    is_conn_err = is_connection_error(e)
                    is_transient = is_transient_error(e)
                    print(f"[es-inserter] Fetcher: error (attempt {fetch_attempt + 1}/{max_fetch_retries}): {e} (transient: {is_transient}, conn: {is_conn_err})", flush=True)

                    if is_conn_err:
                        async with reconnect_lock:
                            try:
                                if conn_state["nc"].is_closed:
                                    print("[es-inserter] Fetcher: Reconnecting...", flush=True)
                                    conn_state["nc"], conn_state["js"] = await reconnect(conn_state["nc"], conn_state["js"])
                                conn_state["sub"] = await subscribe(conn_state["js"], "es-consumer")
                                print("[es-inserter] Fetcher: Reconnected / resubscribed", flush=True)
                            except Exception as reconnect_err:
                                print(f"[es-inserter] Fetcher: Reconnection failed: {reconnect_err}", flush=True)
                                await asyncio.sleep(5)
                                continue
                        break
                    elif is_transient and fetch_attempt < max_fetch_retries - 1:
                        delay = min(1 * (2 ** fetch_attempt), 10)
                        await asyncio.sleep(delay)
                    else:
                        await asyncio.sleep(1)

            if not msgs:
                consecutive_empty += 1
                # Switch back to normal mode after 3 consecutive empty fetches (~90s idle)
                if in_bulk_mode and consecutive_empty >= 3:
                    await set_bulk_mode(es, enabled=False)
                    in_bulk_mode = False
                continue

            # Switch to bulk mode when work arrives
            if not in_bulk_mode:
                await set_bulk_mode(es, enabled=True)
                in_bulk_mode = True
            consecutive_empty = 0

            # Place the raw messages on the queue (blocks if queue is full,
            # providing natural back-pressure to the fetcher).
            await work_queue.put(msgs)

    async def worker(worker_id: int):
        """Worker that processes batches from the queue and indexes into ES."""
        from shared.embeddings import embed_texts, build_text

        while True:
            msgs = await work_queue.get()
            batch_start = _time.monotonic()

            # Parse messages (don't ack yet)
            elements = []
            for msg in msgs:
                elements.append(json.loads(msg.data))

            if not elements:
                for msg in msgs:
                    await msg.ack()
                work_queue.task_done()
                continue

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
                full_addr = normalize_address_text(build_full_address(tags))

                doc = {
                    "_index": INDEX,
                    "_id": elem["osm_id"],
                    "osm_id": elem["osm_id"],
                    "osm_type": elem.get("osm_type", ""),
                    "name": tags.get("name", ""),
                    "name_en": tags.get("name:en", ""),
                    "name_fr": tags.get("name:fr", ""),
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

            # Bulk index with retry on transient ES errors (timeout, circuit breaker, etc.)
            # Uses exponential backoff capped at 15s with jitter to avoid thundering herd.
            max_bulk_retries = 20
            for bulk_attempt in range(max_bulk_retries):
                try:
                    await async_bulk(es, actions, raise_on_error=False)
                    break
                except ApiError as e:
                    if e.status_code == 429 and bulk_attempt < max_bulk_retries - 1:
                        delay = min(2 ** (bulk_attempt + 1), 15) + random.uniform(0, 3)
                        print(f"[es-inserter] Worker {worker_id}: ES overloaded (429), backing off {delay:.1f}s (attempt {bulk_attempt + 1}/{max_bulk_retries})", flush=True)
                        await asyncio.sleep(delay)
                    else:
                        raise
                except (ConnectionTimeout, ConnectionError, OSError) as e:
                    if bulk_attempt < max_bulk_retries - 1:
                        delay = min(2 ** bulk_attempt, 15) + random.uniform(0, 3)
                        print(f"[es-inserter] Worker {worker_id}: Bulk index failed ({type(e).__name__}), retrying in {delay:.1f}s (attempt {bulk_attempt + 1}/{max_bulk_retries})", flush=True)
                        await asyncio.sleep(delay)
                    else:
                        print(f"[es-inserter] Worker {worker_id}: Bulk index failed after {max_bulk_retries} attempts: {e}", flush=True)
                        raise

            # Ack messages only after successful indexing
            for msg in msgs:
                await msg.ack()

            elapsed = _time.monotonic() - batch_start
            throughput = len(actions) / elapsed if elapsed > 0 else 0
            print(f"[es-inserter] Worker {worker_id}: Indexed {len(actions)} docs in {elapsed:.2f}s ({throughput:.0f} docs/s)", flush=True)
            work_queue.task_done()

    # Spawn one fetcher + multiple processing workers
    tasks = [asyncio.create_task(fetcher())]
    tasks += [asyncio.create_task(worker(i)) for i in range(MAX_CONCURRENT_BATCHES)]
    print(f"[es-inserter] Started 1 fetcher + {MAX_CONCURRENT_BATCHES} processing workers (pipeline parallel)", flush=True)
    
    # Wait for all tasks (they run indefinitely)
    await asyncio.gather(*tasks)

    await es.close()
    await conn_state["nc"].close()


if __name__ == "__main__":
    asyncio.run(run())
