"""Consume OSM elements from NATS JS and store geometry in PostGIS.

Tables
------
osm_geometries
  osm_id   TEXT PRIMARY KEY
  osm_type TEXT
  geom     geometry(Geometry, 4326)

osm_addresses
  osm_id       TEXT PRIMARY KEY
  osm_type     TEXT
  housenumber  TEXT
  street       TEXT
  city         TEXT
  postcode     TEXT
  country      TEXT
  full_address TEXT
  geom         geometry(Geometry, 4326)   -- centroid/point for proximity queries

  Indexes:
    GIST on geom  – nearest-address spatial queries
    btree on lower(street) – street-name lookups
    btree on postcode      – postcode-boundary lookups
"""

import asyncio
import json
import random

import asyncpg
import nats.errors

from shared.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    BATCH_SIZE,
    MAX_CONCURRENT_BATCHES,
)
from shared.nats_client import connect, subscribe, is_transient_error, is_connection_error, reconnect
from shared.address import extract_address_components, build_full_address, has_address
from shared.centroid import centroid_latlon

TABLE         = "osm_geometries"
ADDRESS_TABLE = "osm_addresses"


def _geojson_to_wkt(geom: dict, osm_id: str = "") -> str | None:
    """Convert a GeoJSON geometry dict to WKT."""
    try:
        gtype = geom.get("type")
        coords = geom.get("coordinates")
        
        if not gtype or coords is None:
            print(f"[postgis-inserter] Invalid geometry for {osm_id}: missing type or coordinates")
            return None
            
        if gtype == "Point":
            return f"POINT({coords[0]} {coords[1]})"
        if gtype == "LineString":
            if not coords:
                print(f"[postgis-inserter] Empty LineString for {osm_id}")
                return None
            pts = ", ".join(f"{c[0]} {c[1]}" for c in coords)
            return f"LINESTRING({pts})"
        if gtype == "Polygon":
            if not coords:
                print(f"[postgis-inserter] Empty Polygon for {osm_id}")
                return None
            rings = []
            for ring in coords:
                pts = ", ".join(f"{c[0]} {c[1]}" for c in ring)
                rings.append(f"({pts})")
            return f"POLYGON({', '.join(rings)})"
        if gtype == "MultiPolygon":
            if not coords:
                print(f"[postgis-inserter] Empty MultiPolygon for {osm_id}")
                return None
            polygons = []
            for polygon in coords:
                rings = []
                for ring in polygon:
                    pts = ", ".join(f"{c[0]} {c[1]}" for c in ring)
                    rings.append(f"({pts})")
                polygons.append(f"({', '.join(rings)})")
            return f"MULTIPOLYGON({', '.join(polygons)})"
        
        print(f"[postgis-inserter] Unsupported geometry type '{gtype}' for {osm_id}")
        return None
    except Exception as e:
        print(f"[postgis-inserter] Error converting geometry for {osm_id}: {e}")
        return None


async def _executemany_with_retry(pool, sql, data, label, worker_id):
    """Run an upsert executemany, retrying on deadlock/serialization aborts.

    Concurrent workers upserting overlapping osm_ids can still race even with
    sorted lock ordering (e.g. a row touched by the address table vs. the geom
    table, or transient lock-manager ordering). Postgres resolves a deadlock by
    aborting ONE transaction with DeadlockDetectedError; the correct response is
    simply to retry it. Backoff is short with jitter to de-synchronise workers.
    """
    for attempt in range(5):
        try:
            async with pool.acquire() as conn:
                await conn.executemany(sql, data)
            return
        except (asyncpg.exceptions.DeadlockDetectedError,
                asyncpg.exceptions.SerializationError) as e:
            if attempt == 4:
                print(f"[postgis-inserter] Worker {worker_id}: {label} failed after retries: {e}", flush=True)
                raise
            delay = 0.1 * (2 ** attempt) + random.uniform(0, 0.1)
            print(f"[postgis-inserter] Worker {worker_id}: {label} deadlock, retrying in {delay:.2f}s (attempt {attempt + 1}/5)", flush=True)
            await asyncio.sleep(delay)


async def ensure_table(pool: asyncpg.Pool):
    try:
        async with pool.acquire() as conn:
            print(f"[postgis-inserter] Creating PostGIS extension...")
            await conn.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            print(f"[postgis-inserter] PostGIS extension created")
            
            print(f"[postgis-inserter] Creating table {TABLE}...")
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    osm_id   TEXT PRIMARY KEY,
                    osm_type TEXT,
                    geom     geometry(Geometry, 4326)
                )
                """
            )
            print(f"[postgis-inserter] Table {TABLE} created")
            
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom ON {TABLE} USING GIST (geom)"
            )
            print(f"[postgis-inserter] Index idx_{TABLE}_geom created")

            # Partial functional GiST index over the polygon built from closed
            # ways. The enrichment "enclosing closed-line" query filters on
            # ST_Contains(ST_MakePolygon(geom), pt); without this index the
            # planner must compute ST_MakePolygon per row and seq-scans the whole
            # table (~350 ms, parallel workers). Indexing the derived polygon for
            # just the closed-linestring subset turns it into a ~2 ms index scan.
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_closedpoly"
                f" ON {TABLE} USING GIST (ST_MakePolygon(geom))"
                f" WHERE ST_GeometryType(geom) = 'ST_LineString'"
                f"   AND ST_IsClosed(geom) AND ST_NPoints(geom) >= 4"
            )
            print(f"[postgis-inserter] Index idx_{TABLE}_closedpoly created")

            # ── address table ─────────────────────────────────────────────────
            print(f"[postgis-inserter] Creating table {ADDRESS_TABLE}...")
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {ADDRESS_TABLE} (
                    osm_id       TEXT PRIMARY KEY,
                    osm_type     TEXT,
                    housenumber  TEXT,
                    street       TEXT,
                    city         TEXT,
                    postcode     TEXT,
                    country      TEXT,
                    full_address TEXT,
                    geom         geometry(Geometry, 4326)
                )
                """
            )
            print(f"[postgis-inserter] Table {ADDRESS_TABLE} created")
            
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{ADDRESS_TABLE}_geom"
                f" ON {ADDRESS_TABLE} USING GIST (geom)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{ADDRESS_TABLE}_street"
                f" ON {ADDRESS_TABLE} (lower(street))"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{ADDRESS_TABLE}_postcode"
                f" ON {ADDRESS_TABLE} (postcode)"
            )
            print(f"[postgis-inserter] Address table indexes created")
            
            # Verify tables exist
            result = await conn.fetch(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename IN ($1, $2)",
                TABLE, ADDRESS_TABLE
            )
            existing_tables = [row['tablename'] for row in result]
            print(f"[postgis-inserter] Verified tables exist: {existing_tables}")
            
    except Exception as e:
        print(f"[postgis-inserter] ERROR ensuring tables: {e}", flush=True)
        raise


async def run():
    import time as _time

    pool = await asyncpg.create_pool(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        min_size=2,
        max_size=10,
    )
    await ensure_table(pool)

    nc, js = await connect()
    try:
        sub = await subscribe(js, "postgis-consumer")
        print("[postgis-inserter] Listening for messages ...")
    except Exception as e:
        print(f"[postgis-inserter] Failed to create subscription: {e}", flush=True)
        raise

    # Use mutable containers for connection objects so workers can update them
    conn_state = {"nc": nc, "js": js, "sub": sub}
    reconnect_lock = asyncio.Lock()

    # ── Pipeline parallelism ──────────────────────────────────────────────
    # A single fetcher pulls batches from NATS and places them on a queue.
    # Multiple workers pop from the queue, parse, and insert into PostGIS.
    work_queue: asyncio.Queue[list] = asyncio.Queue(maxsize=MAX_CONCURRENT_BATCHES * 2)

    async def fetcher():
        """Single fetcher that pulls batches from NATS into the work queue."""
        while True:
            max_fetch_retries = 5
            msgs = None
            for fetch_attempt in range(max_fetch_retries):
                try:
                    msgs = await conn_state["sub"].fetch(batch=BATCH_SIZE, timeout=30)
                    break
                except (nats.errors.TimeoutError, asyncio.TimeoutError):
                    # Bare asyncio.TimeoutError (str == '') is raised by nats-py's
                    # fetch() internals on some no-message paths and is NOT a
                    # subclass-catch of nats.errors.TimeoutError's parent — handle
                    # both as an empty fetch rather than an unclassified error.
                    msgs = []
                    break
                except Exception as e:
                    is_conn_err = is_connection_error(e)
                    is_transient = is_transient_error(e)
                    print(f"[postgis-inserter] Fetcher: error (attempt {fetch_attempt + 1}/{max_fetch_retries}): {type(e).__name__}: {e} (transient: {is_transient}, conn: {is_conn_err})", flush=True)

                    if is_conn_err:
                        async with reconnect_lock:
                            try:
                                if conn_state["nc"].is_closed:
                                    print("[postgis-inserter] Fetcher: Reconnecting...", flush=True)
                                    conn_state["nc"], conn_state["js"] = await reconnect(conn_state["nc"], conn_state["js"])
                                conn_state["sub"] = await subscribe(conn_state["js"], "postgis-consumer")
                                print("[postgis-inserter] Fetcher: Reconnected / resubscribed", flush=True)
                            except Exception as reconnect_err:
                                print(f"[postgis-inserter] Fetcher: Reconnection failed: {reconnect_err}", flush=True)
                                await asyncio.sleep(5)
                                continue
                        break
                    elif is_transient and fetch_attempt < max_fetch_retries - 1:
                        delay = min(2 * (2 ** fetch_attempt), 15)
                        await asyncio.sleep(delay)
                    else:
                        await asyncio.sleep(10)

            if not msgs:
                continue

            await work_queue.put(msgs)

    async def worker(worker_id: int):
        """Worker that processes batches from the queue and inserts into PostGIS."""
        while True:
            msgs = await work_queue.get()
            batch_start = _time.monotonic()

            # Parse messages (don't ack yet)
            rows: list[tuple[str, str, str]] = []
            addr_rows: list[tuple] = []

            for msg in msgs:
                elem = json.loads(msg.data)
                osm_id   = elem.get("osm_id", "unknown")
                osm_type = elem.get("osm_type", "")
                geom     = elem.get("geom")

                if not geom:
                    continue

                wkt = _geojson_to_wkt(geom, osm_id)
                if not wkt:
                    continue

                rows.append((osm_id, osm_type, wkt))

                # Build address row when addr:* tags are present
                tags = elem.get("tags", {})
                if has_address(tags):
                    addr  = extract_address_components(tags)
                    faddr = build_full_address(tags)
                    # Use centroid as the spatial point for addresses
                    c   = centroid_latlon(geom)
                    cwkt = f"POINT({c['lon']} {c['lat']})" if c else wkt
                    addr_rows.append((
                        osm_id,
                        osm_type,
                        addr.get("housenumber", ""),
                        addr.get("street", ""),
                        addr.get("city", ""),
                        addr.get("postcode", ""),
                        addr.get("country", ""),
                        faddr,
                        cwkt,
                    ))

            # Deduplicate (keep last) and sort by osm_id so EVERY worker acquires
            # row locks in the same order. A consistent global lock ordering is
            # what prevents the cross-batch upsert deadlocks (worker A locks X→Y
            # while worker B locks Y→X). Dedup also avoids self-contention from a
            # batch upserting the same osm_id twice.
            if rows:
                rows = [r for _, r in sorted({r[0]: r for r in rows}.items())]
                await _executemany_with_retry(
                    pool,
                    f"""
                    INSERT INTO {TABLE} (osm_id, osm_type, geom)
                    VALUES ($1, $2, ST_GeomFromText($3, 4326))
                    ON CONFLICT (osm_id) DO UPDATE SET
                        osm_type = EXCLUDED.osm_type,
                        geom     = EXCLUDED.geom
                    """,
                    rows, "geom upsert", worker_id,
                )

            if addr_rows:
                addr_rows = [r for _, r in sorted({r[0]: r for r in addr_rows}.items())]
                await _executemany_with_retry(
                    pool,
                    f"""
                    INSERT INTO {ADDRESS_TABLE}
                        (osm_id, osm_type, housenumber, street, city,
                         postcode, country, full_address, geom)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                            ST_GeomFromText($9, 4326))
                    ON CONFLICT (osm_id) DO UPDATE SET
                        osm_type     = EXCLUDED.osm_type,
                        housenumber  = EXCLUDED.housenumber,
                        street       = EXCLUDED.street,
                        city         = EXCLUDED.city,
                        postcode     = EXCLUDED.postcode,
                        country      = EXCLUDED.country,
                        full_address = EXCLUDED.full_address,
                        geom         = EXCLUDED.geom
                    """,
                    addr_rows, "address upsert", worker_id,
                )

            # Ack messages only after successful processing
            for msg in msgs:
                await msg.ack()

            elapsed = _time.monotonic() - batch_start
            throughput = len(rows) / elapsed if elapsed > 0 else 0
            print(f"[postgis-inserter] Worker {worker_id}: Inserted {len(rows)} geoms + {len(addr_rows)} addrs in {elapsed:.2f}s ({throughput:.0f} rows/s)", flush=True)
            work_queue.task_done()

    # Spawn one fetcher + multiple processing workers
    tasks = [asyncio.create_task(fetcher())]
    tasks += [asyncio.create_task(worker(i)) for i in range(MAX_CONCURRENT_BATCHES)]
    print(f"[postgis-inserter] Started 1 fetcher + {MAX_CONCURRENT_BATCHES} processing workers (pipeline parallel)", flush=True)
    
    # Wait for all tasks (they run indefinitely)
    await asyncio.gather(*tasks)

    await pool.close()
    await conn_state["nc"].close()


if __name__ == "__main__":
    asyncio.run(run())
