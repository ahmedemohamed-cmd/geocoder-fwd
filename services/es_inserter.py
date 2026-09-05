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
from elastic_transport import ConnectionTimeout
from elasticsearch import ApiError, AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from shared.centroid import centroid_latlon
from shared.config import (
    BATCH_SIZE,
    ELASTICSEARCH_URL,
    ENABLE_VECTORS,
    MAX_CONCURRENT_BATCHES,
)
from shared.nats_client import (
    connect,
    is_connection_error,
    is_transient_error,
    reconnect,
    subscribe,
)
from shared.ranking import compute_offline_rank

# ---------------------------------------------------------------------------
# Geometry simplification — prevent Lucene Tessellator from choking on
# complex polygons with thousands of vertices / holes.
# ---------------------------------------------------------------------------
MAX_GEOM_VERTICES = 2000  # keep geometries ≤ this many total vertices


def _count_ring(ring):
    return len(ring) if ring else 0


def _count_vertices(geom):
    """Count total coordinate vertices in a GeoJSON geometry."""
    gtype = geom.get("type", "")
    coords = geom.get("coordinates")
    if not coords:
        # GeometryCollection
        return sum(_count_vertices(g) for g in geom.get("geometries", []))
    if gtype == "Point":
        return 1
    if gtype in ("LineString", "MultiPoint"):
        return len(coords)
    if gtype == "Polygon":
        return sum(_count_ring(r) for r in coords)
    if gtype == "MultiLineString":
        return sum(len(ls) for ls in coords)
    if gtype == "MultiPolygon":
        return sum(sum(_count_ring(r) for r in poly) for poly in coords)
    return 0


def _thin_ring(ring, keep_every):
    """Keep every Nth coordinate, always preserving first and last."""
    if len(ring) <= 4:
        return ring
    thinned = [ring[i] for i in range(0, len(ring) - 1, keep_every)]
    # Close the ring
    if thinned[-1] != ring[-1]:
        thinned.append(ring[-1])
    # Polygons need at least 4 coordinates
    if len(thinned) < 4:
        return ring[:3] + [ring[-1]]
    return thinned


def simplify_geometry(geom):
    """Simplify a GeoJSON geometry if it has too many vertices.

    Uses uniform vertex thinning (keep every Nth point) rather than a
    full Visvalingam / Douglas-Peucker algorithm to avoid adding a
    dependency.  Good enough for search/display purposes.
    """
    if not geom:
        return geom

    total = _count_vertices(geom)
    if total <= MAX_GEOM_VERTICES:
        return geom

    keep_every = max(2, total // MAX_GEOM_VERTICES + 1)
    gtype = geom.get("type", "")
    coords = geom.get("coordinates")

    if gtype == "Polygon" and coords:
        new_coords = [_thin_ring(r, keep_every) for r in coords]
        # Drop holes that became degenerate (< 4 pts)
        outer = new_coords[0]
        holes = [h for h in new_coords[1:] if len(h) >= 4]
        return {"type": "Polygon", "coordinates": [outer] + holes}

    if gtype == "MultiPolygon" and coords:
        new_polys = []
        for poly in coords:
            new_rings = [_thin_ring(r, keep_every) for r in poly]
            outer = new_rings[0]
            holes = [h for h in new_rings[1:] if len(h) >= 4]
            new_polys.append([outer] + holes)
        return {"type": "MultiPolygon", "coordinates": new_polys}

    if gtype in ("LineString",) and coords and len(coords) > MAX_GEOM_VERTICES:
        thinned = [coords[i] for i in range(0, len(coords), keep_every)]
        if thinned[-1] != coords[-1]:
            thinned.append(coords[-1])
        return {"type": "LineString", "coordinates": thinned}

    if gtype == "MultiLineString" and coords:
        new_lines = []
        for ls in coords:
            if len(ls) > MAX_GEOM_VERTICES:
                thinned = [ls[i] for i in range(0, len(ls), keep_every)]
                if thinned[-1] != ls[-1]:
                    thinned.append(ls[-1])
                new_lines.append(thinned)
            else:
                new_lines.append(ls)
        return {"type": "MultiLineString", "coordinates": new_lines}

    # GeometryCollection — simplify each sub-geometry
    if gtype == "GeometryCollection":
        return {
            "type": "GeometryCollection",
            "geometries": [simplify_geometry(g) for g in geom.get("geometries", [])],
        }

    return geom


from shared.address import (
    build_full_address,
    extract_address_components,
    normalize_address_text,
)
from shared.categories import category_text, classify
from shared.es_mapping import MAPPING
from shared.logging import get_logger

logger = get_logger("es-inserter")

INDEX = "osm_places"

# Painless merge script for provider docs (e.g. Google deep search) that may be
# ingested repeatedly in different languages. Instead of overwriting the doc, it
# MERGES the incoming tags into the existing ones so multilingual name tags
# (name:en, name:ar, …) accumulate on a single document — exactly as if the place
# had been imported from OSM with multiple name:* tags. name/name_en/name_fr and
# tags_text are recomputed from the merged tags; remaining scalar/derived fields
# (geom, centroid, offline_rank, addr_*, …) are taken from the latest request.
_MERGE_TAGS_SCRIPT = """
if (ctx._source.tags == null) { ctx._source.tags = [:]; }
for (def e : params.tags.entrySet()) { ctx._source.tags[e.getKey()] = e.getValue(); }
def t = ctx._source.tags;
if (t.containsKey('name'))    { ctx._source.name = t['name']; }
if (t.containsKey('name:en')) { ctx._source.name_en = t['name:en']; }
if (t.containsKey('name:fr')) { ctx._source.name_fr = t['name:fr']; }
def parts = new ArrayList();
if (t.containsKey('name') && t['name'] instanceof String && t['name'].length() > 0) { parts.add(t['name']); }
for (def e : t.entrySet()) {
  def k = e.getKey();
  if (k == 'name' || k == 'source' || k.startsWith('ref:')) { continue; }
  def v = e.getValue();
  if (v instanceof String && v.length() > 0) { parts.add(v); }
}
ctx._source.tags_text = String.join(' ', parts);
for (def e : params.fields.entrySet()) { ctx._source[e.getKey()] = e.getValue(); }
if (ctx._source.popularity == null) { ctx._source.popularity = 0.0; }
"""


async def ensure_index(es: AsyncElasticsearch):
    max_retries = 5
    retry_delay = 3

    for attempt in range(max_retries):
        try:
            if not await es.indices.exists(index=INDEX):
                await es.indices.create(index=INDEX, **MAPPING)
                logger.info(f"[es-inserter] Created index {INDEX}")
            else:
                # Additive, non-breaking on a dynamic:false mapping — brings the
                # category_* fields (and any future scalar keyword) onto an index
                # created before they existed. No-op once already present.
                await es.indices.put_mapping(
                    index=INDEX, properties=MAPPING["mappings"]["properties"]
                )
                logger.info(f"[es-inserter] Index {INDEX} already exists (mapping ensured)")
            return
        except Exception as e:
            logger.error(
                f"[es-inserter] Error checking/creating ES index (attempt {attempt + 1}/{max_retries}): {e}",
            )
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                # Final attempt - try to create anyway
                try:
                    await es.indices.create(index=INDEX, **MAPPING)
                    logger.info(f"[es-inserter] Created ES index {INDEX} (fallback)")
                except Exception as e2:
                    logger.error(f"[es-inserter] Failed to create ES index: {e2}")
                    # If index already exists, that's fine
                    if "resource_already_exists" not in str(e2).lower():
                        raise


async def set_bulk_mode(es: AsyncElasticsearch, enabled: bool):
    """Toggle index settings optimised for bulk ingest vs. normal query serving.

    BULK mode: 60s refresh, async translog, but merges still run (1 thread)
    so segments don't pile up unbounded.
    NORMAL mode: 5s refresh, sync translog, normal merge policy.  When
    switching to NORMAL we also trigger a background force-merge if the
    segment count is too high.
    """
    if enabled:
        settings = {
            "refresh_interval": "60s",
            "translog.durability": "async",
            "translog.flush_threshold_size": "1gb",
            "translog.sync_interval": "30s",
            # Allow merges but limit to 1 thread so they don't starve ingest
            "merge.scheduler.max_thread_count": 1,
            "merge.scheduler.auto_throttle": True,
            "merge.policy.segments_per_tier": 10,
            "merge.policy.max_merged_segment": "5gb",
            "merge.policy.max_merge_at_once": 10,
        }
        label = "BULK"
    else:
        settings = {
            "refresh_interval": "5s",
            "translog.durability": "request",
            "translog.flush_threshold_size": "512mb",
            "translog.sync_interval": "5s",
            "merge.scheduler.max_thread_count": 1,
            "merge.scheduler.auto_throttle": True,
            "merge.policy.segments_per_tier": 10,
            "merge.policy.max_merged_segment": "5gb",
            "merge.policy.max_merge_at_once": 10,
        }
        label = "NORMAL"
    try:
        await es.indices.put_settings(index=INDEX, settings={"index": settings})
        logger.info(f"[es-inserter] Index mode set to {label}")
    except Exception as e:
        logger.warning(f"[es-inserter] Warning: could not set {label} mode: {e}")

    # When leaving bulk mode, kick off a background force-merge if needed
    if not enabled:
        try:
            stats = await es.indices.stats(index=INDEX, metric="segments")
            seg_count = stats["indices"][INDEX]["primaries"]["segments"]["count"]
            if seg_count > 10:
                logger.info(
                    f"[es-inserter] {seg_count} segments detected, starting background force-merge to 1",
                )
                # Read-heavy index: merge each shard to a single segment for the
                # lowest per-query overhead. The >10 guard means this only runs
                # when trickle writes have re-fragmented it, so a settled index
                # isn't rewritten needlessly.
                await es.indices.forcemerge(
                    index=INDEX,
                    max_num_segments=1,
                    wait_for_completion=False,
                    flush=True,
                )
        except Exception as e:
            logger.error(f"[es-inserter] Warning: force-merge request failed: {e}")


async def run():

    # Retry logic for connecting to Elasticsearch
    max_retries = 10
    retry_delay = 2

    es = None
    for attempt in range(max_retries):
        try:
            es = AsyncElasticsearch(ELASTICSEARCH_URL, request_timeout=60)
            # Test ES connection
            await es.ping()
            logger.info("[es-inserter] Successfully connected to Elasticsearch")
            break
        except Exception as e:
            logger.error(
                f"[es-inserter] Failed to connect to Elasticsearch (attempt {attempt + 1}/{max_retries}): {e}",
            )
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
        logger.info("[es-inserter] Pre-loading embedding model...")
        from shared.embeddings import get_model

        get_model()  # Force model download/load
        logger.info("[es-inserter] Embedding model loaded")

    nc, js = await connect()
    sub = await subscribe(js, "es-consumer")
    logger.info("[es-inserter] Subscription created, listening for messages ...")

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

    in_bulk_mode = True  # tracks whether we've applied bulk settings
    consecutive_empty = 0  # number of consecutive empty fetches

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
                except (TimeoutError, nats.errors.TimeoutError):
                    # Bare asyncio.TimeoutError (str == '') is raised by nats-py's
                    # fetch() internals on some no-message paths and slips past a
                    # nats.errors.TimeoutError-only catch — treat both as empty.
                    msgs = []
                    break
                except Exception as e:
                    is_conn_err = is_connection_error(e)
                    is_transient = is_transient_error(e)
                    logger.error(
                        f"[es-inserter] Fetcher: error (attempt {fetch_attempt + 1}/{max_fetch_retries}): {type(e).__name__}: {e} (transient: {is_transient}, conn: {is_conn_err})",
                    )

                    if is_conn_err:
                        async with reconnect_lock:
                            try:
                                if conn_state["nc"].is_closed:
                                    logger.warning("[es-inserter] Fetcher: Reconnecting...")
                                    conn_state["nc"], conn_state["js"] = await reconnect(
                                        conn_state["nc"], conn_state["js"]
                                    )
                                conn_state["sub"] = await subscribe(conn_state["js"], "es-consumer")
                                logger.info("[es-inserter] Fetcher: Reconnected / resubscribed")
                            except Exception as reconnect_err:
                                logger.error(
                                    f"[es-inserter] Fetcher: Reconnection failed: {reconnect_err}",
                                )
                                await asyncio.sleep(5)
                                continue
                        break
                    elif is_transient and fetch_attempt < max_fetch_retries - 1:
                        delay = min(1 * (2**fetch_attempt), 10)
                        await asyncio.sleep(delay)
                    else:
                        await asyncio.sleep(1)

            if not msgs:
                consecutive_empty += 1
                # Switch back to normal mode after 10 consecutive empty fetches (~5 min idle)
                if in_bulk_mode and consecutive_empty >= 10:
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
        from shared.embeddings import build_text, embed_texts

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
                indices, batch_texts = zip(*non_empty, strict=False)
                batch_vecs = embed_texts(list(batch_texts))
                for idx, vec in zip(indices, batch_vecs, strict=False):
                    vectors[idx] = vec

            # prepare bulk actions
            actions = []
            for elem, txt, vec in zip(elements, texts, vectors, strict=False):
                tags = elem["tags"]
                admin_level = elem.get("admin_level")  # None when tag is absent
                area_km2 = elem.get("area_km2", 0.0)
                rank = compute_offline_rank(tags, admin_level, area_km2)

                # filterable category (drives /nearby) — derived from the tags by
                # the same classifier used at query time and in the backfill
                cat = classify(tags, admin_level)

                # address fields
                addr = extract_address_components(tags)
                full_addr = normalize_address_text(build_full_address(tags))

                # Fields updated on every ingest (excludes popularity — preserved across re-ingests)
                fields = {
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
                    "category_key": cat.key or "",
                    "category_value": cat.value or "",
                    "category_group": cat.group or "",
                    # the type rendered as searchable EN+AR text, so /autocomplete
                    # can answer "metro" / "مستشفى" — tags_text holds tag values
                    # only, which never contain the word users actually type
                    "category_text": category_text(tags),
                    # address
                    "has_address": bool(full_addr),
                    "full_address": full_addr,
                    "addr_housenumber": addr.get("housenumber", ""),
                    "addr_street": addr.get("street", ""),
                    "addr_city": addr.get("city", ""),
                    "addr_postcode": addr.get("postcode", ""),
                    "addr_country": addr.get("country", ""),
                    "addr_suburb": addr.get("suburb", ""),
                    "addr_state": addr.get("state", ""),
                }
                if elem.get("geom"):
                    simplified_geom = simplify_geometry(elem["geom"])
                    fields["geom"] = simplified_geom
                    # Use the simplified geometry for the centroid so the geo_point
                    # stays consistent with the stored geo_shape.
                    c = centroid_latlon(simplified_geom)
                    if c:
                        fields["centroid"] = c
                if vec is not None:
                    fields["name_vector"] = vec

                # Provider docs (e.g. Google deep search) may be ingested
                # repeatedly in different languages. MERGE their tags so name:*
                # variants accumulate on one document (recomputing name/name_en/
                # name_fr/tags_text from the merged tags). Native OSM docs are
                # self-contained, so they keep the faster doc-overwrite path.
                if tags.get("source") == "google":
                    scalar_fields = {
                        k: v
                        for k, v in fields.items()
                        if k not in ("tags", "tags_text", "name", "name_en", "name_fr")
                    }
                    doc = {
                        "_index": INDEX,
                        "_id": elem["osm_id"],
                        "_op_type": "update",
                        "scripted_upsert": True,
                        "script": {
                            "lang": "painless",
                            "source": _MERGE_TAGS_SCRIPT,
                            "params": {"tags": tags, "fields": scalar_fields},
                        },
                        "upsert": {},
                    }
                else:
                    # update+upsert: re-ingesting an existing doc does NOT reset the
                    # popularity accumulated via /feedback.  First insert initialises it to 0.
                    doc = {
                        "_index": INDEX,
                        "_id": elem["osm_id"],
                        "_op_type": "update",
                        "doc": fields,
                        "upsert": {**fields, "popularity": 0.0},
                    }
                actions.append(doc)

            # Bulk index with retry on transient ES errors (timeout, circuit breaker, etc.)
            # Uses exponential backoff capped at 15s with jitter to avoid thundering herd.
            max_bulk_retries = 20
            for bulk_attempt in range(max_bulk_retries):
                try:
                    _, errors = await async_bulk(es, actions, raise_on_error=False)
                    if errors:
                        logger.error(
                            f"[es-inserter] Worker {worker_id}: {len(errors)} docs rejected by ES (non-transient)",
                        )
                        for err in errors[:3]:
                            logger.error(f"[es-inserter] Worker {worker_id}: Doc error: {err}")
                    break
                except ApiError as e:
                    if e.status_code == 429 and bulk_attempt < max_bulk_retries - 1:
                        delay = min(2 ** (bulk_attempt + 1), 15) + random.uniform(0, 3)
                        logger.info(
                            f"[es-inserter] Worker {worker_id}: ES overloaded (429), backing off {delay:.1f}s (attempt {bulk_attempt + 1}/{max_bulk_retries})",
                        )
                        await asyncio.sleep(delay)
                    else:
                        raise
                except (ConnectionTimeout, ConnectionError, OSError) as e:
                    if bulk_attempt < max_bulk_retries - 1:
                        delay = min(2**bulk_attempt, 15) + random.uniform(0, 3)
                        logger.error(
                            f"[es-inserter] Worker {worker_id}: Bulk index failed ({type(e).__name__}), retrying in {delay:.1f}s (attempt {bulk_attempt + 1}/{max_bulk_retries})",
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(
                            f"[es-inserter] Worker {worker_id}: Bulk index failed after {max_bulk_retries} attempts: {e}",
                        )
                        raise

            # Ack messages only after successful indexing
            for msg in msgs:
                await msg.ack()

            elapsed = _time.monotonic() - batch_start
            throughput = len(actions) / elapsed if elapsed > 0 else 0
            logger.info(
                f"[es-inserter] Worker {worker_id}: Indexed {len(actions)} docs in {elapsed:.2f}s ({throughput:.0f} docs/s)",
            )
            work_queue.task_done()

    # Spawn one fetcher + multiple processing workers
    tasks = [asyncio.create_task(fetcher())]
    tasks += [asyncio.create_task(worker(i)) for i in range(MAX_CONCURRENT_BATCHES)]
    logger.info(
        f"[es-inserter] Started 1 fetcher + {MAX_CONCURRENT_BATCHES} processing workers (pipeline parallel)",
    )

    # Wait for all tasks (they run indefinitely)
    await asyncio.gather(*tasks)

    await es.close()
    await conn_state["nc"].close()


if __name__ == "__main__":
    asyncio.run(run())
