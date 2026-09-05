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

from shared.address import build_full_address, extract_address_components, has_address
from shared.centroid import centroid_latlon
from shared.config import (
    BATCH_SIZE,
    MAX_CONCURRENT_BATCHES,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from shared.interpolation import _STREET_NORM_SQL
from shared.logging import get_logger
from shared.nats_client import (
    connect,
    is_connection_error,
    is_transient_error,
    reconnect,
    subscribe,
)

logger = get_logger("postgis-inserter")

TABLE = "osm_geometries"
ADDRESS_TABLE = "osm_addresses"


def _geojson_to_wkt(geom: dict, osm_id: str = "") -> str | None:
    """Convert a GeoJSON geometry dict to WKT."""
    try:
        gtype = geom.get("type")
        coords = geom.get("coordinates")

        if not gtype or coords is None:
            logger.warning(
                "[postgis-inserter] Invalid geometry for %s: missing type or coordinates", osm_id
            )
            return None

        if gtype == "Point":
            return f"POINT({coords[0]} {coords[1]})"
        if gtype == "LineString":
            if not coords:
                logger.info("[postgis-inserter] Empty LineString for %s", osm_id)
                return None
            pts = ", ".join(f"{c[0]} {c[1]}" for c in coords)
            return f"LINESTRING({pts})"
        if gtype == "Polygon":
            if not coords:
                logger.info("[postgis-inserter] Empty Polygon for %s", osm_id)
                return None
            rings = []
            for ring in coords:
                pts = ", ".join(f"{c[0]} {c[1]}" for c in ring)
                rings.append(f"({pts})")
            return f"POLYGON({', '.join(rings)})"
        if gtype == "MultiPolygon":
            if not coords:
                logger.info("[postgis-inserter] Empty MultiPolygon for %s", osm_id)
                return None
            polygons = []
            for polygon in coords:
                rings = []
                for ring in polygon:
                    pts = ", ".join(f"{c[0]} {c[1]}" for c in ring)
                    rings.append(f"({pts})")
                polygons.append(f"({', '.join(rings)})")
            return f"MULTIPOLYGON({', '.join(polygons)})"

        logger.info("[postgis-inserter] Unsupported geometry type '%s' for %s", gtype, osm_id)
        return None
    except Exception as e:
        logger.error("[postgis-inserter] Error converting geometry for %s: %s", osm_id, e)
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
        except (
            asyncpg.exceptions.DeadlockDetectedError,
            asyncpg.exceptions.SerializationError,
        ) as e:
            if attempt == 4:
                logger.error(
                    "[postgis-inserter] Worker %s: %s failed after retries: %s",
                    worker_id,
                    label,
                    e,
                )
                raise
            delay = 0.1 * (2**attempt) + random.uniform(0, 0.1)
            logger.warning(
                "[postgis-inserter] Worker %s: %s deadlock, retrying in %ss (attempt %s/5)",
                worker_id,
                label,
                format(delay, ".2f"),
                attempt + 1,
            )
            await asyncio.sleep(delay)


async def ensure_table(pool: asyncpg.Pool):
    try:
        async with pool.acquire() as conn:
            logger.info("[postgis-inserter] Creating PostGIS extension...")
            await conn.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            logger.info("[postgis-inserter] PostGIS extension created")

            logger.info("[postgis-inserter] Creating table %s...", TABLE)
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    osm_id   TEXT PRIMARY KEY,
                    osm_type TEXT,
                    geom     geometry(Geometry, 4326)
                )
                """
            )
            logger.info("[postgis-inserter] Table %s created", TABLE)

            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom ON {TABLE} USING GIST (geom)"
            )
            logger.info("[postgis-inserter] Index idx_%s_geom created", TABLE)

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
            logger.info("[postgis-inserter] Index idx_%s_closedpoly created", TABLE)

            # ── address table ─────────────────────────────────────────────────
            logger.info("[postgis-inserter] Creating table %s...", ADDRESS_TABLE)
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
            logger.info("[postgis-inserter] Table %s created", ADDRESS_TABLE)

            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{ADDRESS_TABLE}_geom"
                f" ON {ADDRESS_TABLE} USING GIST (geom)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{ADDRESS_TABLE}_street"
                f" ON {ADDRESS_TABLE} (lower(street))"
            )
            # Normalised-name index: address interpolation matches street names
            # diacritic/alef/prefix-insensitively (see shared.interpolation), which
            # can't ride the lower(street) btree. Without this functional index the
            # gather degrades to a full seq scan over every address.
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{ADDRESS_TABLE}_street_norm"
                f" ON {ADDRESS_TABLE} (({_STREET_NORM_SQL.format(col='street')}))"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{ADDRESS_TABLE}_postcode"
                f" ON {ADDRESS_TABLE} (postcode)"
            )
            logger.info("[postgis-inserter] Address table indexes created")

            # Verify tables exist
            result = await conn.fetch(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename IN ($1, $2)",
                TABLE,
                ADDRESS_TABLE,
            )
            existing_tables = [row["tablename"] for row in result]
            logger.info("[postgis-inserter] Verified tables exist: %s", existing_tables)

    except Exception as e:
        logger.error("[postgis-inserter] ERROR ensuring tables: %s", e)
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
        logger.info("[postgis-inserter] Listening for messages ...")
    except Exception as e:
        logger.error("[postgis-inserter] Failed to create subscription: %s", e)
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
                except (TimeoutError, nats.errors.TimeoutError):
                    # Bare asyncio.TimeoutError (str == '') is raised by nats-py's
                    # fetch() internals on some no-message paths and is NOT a
                    # subclass-catch of nats.errors.TimeoutError's parent — handle
                    # both as an empty fetch rather than an unclassified error.
                    msgs = []
                    break
                except Exception as e:
                    is_conn_err = is_connection_error(e)
                    is_transient = is_transient_error(e)
                    logger.error(
                        "[postgis-inserter] Fetcher: error (attempt %s/%s): %s: %s (transient: %s, conn: %s)",
                        fetch_attempt + 1,
                        max_fetch_retries,
                        type(e).__name__,
                        e,
                        is_transient,
                        is_conn_err,
                    )

                    if is_conn_err:
                        async with reconnect_lock:
                            try:
                                if conn_state["nc"].is_closed:
                                    logger.warning("[postgis-inserter] Fetcher: Reconnecting...")
                                    conn_state["nc"], conn_state["js"] = await reconnect(
                                        conn_state["nc"], conn_state["js"]
                                    )
                                conn_state["sub"] = await subscribe(
                                    conn_state["js"], "postgis-consumer"
                                )
                                logger.warning(
                                    "[postgis-inserter] Fetcher: Reconnected / resubscribed",
                                )
                            except Exception as reconnect_err:
                                logger.error(
                                    "[postgis-inserter] Fetcher: Reconnection failed: %s",
                                    reconnect_err,
                                )
                                await asyncio.sleep(5)
                                continue
                        break
                    elif is_transient and fetch_attempt < max_fetch_retries - 1:
                        delay = min(2 * (2**fetch_attempt), 15)
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
                osm_id = elem.get("osm_id", "unknown")
                osm_type = elem.get("osm_type", "")
                geom = elem.get("geom")

                if not geom:
                    continue

                wkt = _geojson_to_wkt(geom, osm_id)
                if not wkt:
                    continue

                rows.append((osm_id, osm_type, wkt))

                # Build address row when addr:* tags are present
                tags = elem.get("tags", {})
                if has_address(tags):
                    addr = extract_address_components(tags)
                    faddr = build_full_address(tags)
                    # Use centroid as the spatial point for addresses
                    c = centroid_latlon(geom)
                    cwkt = f"POINT({c['lon']} {c['lat']})" if c else wkt
                    addr_rows.append(
                        (
                            osm_id,
                            osm_type,
                            addr.get("housenumber", ""),
                            addr.get("street", ""),
                            addr.get("city", ""),
                            addr.get("postcode", ""),
                            addr.get("country", ""),
                            faddr,
                            cwkt,
                        )
                    )

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
                    rows,
                    "geom upsert",
                    worker_id,
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
                    addr_rows,
                    "address upsert",
                    worker_id,
                )

            # Ack messages only after successful processing
            for msg in msgs:
                await msg.ack()

            elapsed = _time.monotonic() - batch_start
            throughput = len(rows) / elapsed if elapsed > 0 else 0
            logger.info(
                "[postgis-inserter] Worker %s: Inserted %s geoms + %s addrs in %ss (%s rows/s)",
                worker_id,
                len(rows),
                len(addr_rows),
                format(elapsed, ".2f"),
                format(throughput, ".0f"),
            )
            work_queue.task_done()

    # Spawn one fetcher + multiple processing workers
    tasks = [asyncio.create_task(fetcher())]
    tasks += [asyncio.create_task(worker(i)) for i in range(MAX_CONCURRENT_BATCHES)]
    logger.info(
        "[postgis-inserter] Started 1 fetcher + %s processing workers (pipeline parallel)",
        MAX_CONCURRENT_BATCHES,
    )

    # Wait for all tasks (they run indefinitely)
    await asyncio.gather(*tasks)

    await pool.close()
    await conn_state["nc"].close()


if __name__ == "__main__":
    asyncio.run(run())
