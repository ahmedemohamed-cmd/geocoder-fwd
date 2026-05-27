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


async def ensure_table(pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                osm_id   TEXT PRIMARY KEY,
                osm_type TEXT,
                geom     geometry(Geometry, 4326)
            )
            """
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom ON {TABLE} USING GIST (geom)"
        )
        print(f"[postgis-inserter] Ensured table {TABLE}")

        # ── address table ─────────────────────────────────────────────────
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
        print(f"[postgis-inserter] Ensured table {ADDRESS_TABLE}")


async def run():
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

    # Create worker pool for concurrent batch processing
    async def worker(worker_id: int):
        """Worker that fetches and processes batches concurrently."""
        while True:
            msgs = None
            max_fetch_retries = 5
            for fetch_attempt in range(max_fetch_retries):
                try:
                    msgs = await conn_state["sub"].fetch(batch=BATCH_SIZE, timeout=30)
                    break
                except nats.errors.TimeoutError:
                    # Stream is empty — normal idle state, no messages to process.
                    msgs = []
                    break
                except Exception as e:
                    is_conn_err = is_connection_error(e)
                    is_transient = is_transient_error(e)
                    print(f"[postgis-inserter] Worker {worker_id}: Fetch error (attempt {fetch_attempt + 1}/{max_fetch_retries}): {e} (transient: {is_transient}, connection_error: {is_conn_err})", flush=True)

                    if is_conn_err:
                        # Only one worker should reconnect at a time
                        async with reconnect_lock:
                            try:
                                if conn_state["nc"].is_closed:
                                    print(f"[postgis-inserter] Worker {worker_id}: Reconnecting...", flush=True)
                                    conn_state["nc"], conn_state["js"] = await reconnect(conn_state["nc"], conn_state["js"])
                                # Always recreate subscription — the consumer may
                                # be missing even when the TCP connection is alive
                                # (e.g. ServiceUnavailableError).
                                conn_state["sub"] = await subscribe(conn_state["js"], "postgis-consumer")
                                print(f"[postgis-inserter] Worker {worker_id}: Reconnected / resubscribed", flush=True)
                            except Exception as reconnect_err:
                                print(f"[postgis-inserter] Worker {worker_id}: Reconnection failed: {reconnect_err}", flush=True)
                                await asyncio.sleep(5)
                                continue
                        break  # Retry fetch with new connection/subscription
                    elif is_transient and fetch_attempt < max_fetch_retries - 1:
                        delay = min(2 * (2 ** fetch_attempt), 15)
                        print(f"[postgis-inserter] Worker {worker_id}: Backing off for {delay}s", flush=True)
                        await asyncio.sleep(delay)
                    else:
                        print(f"[postgis-inserter] Worker {worker_id}: Max retries reached, waiting 10s", flush=True)
                        await asyncio.sleep(10)
            
            if not msgs:
                continue

            # Parse messages (don't ack yet)
            rows: list[tuple[str, str, str]] = []
            addr_rows: list[tuple] = []

            for msg in msgs:
                elem = json.loads(msg.data)
                osm_id   = elem.get("osm_id", "unknown")
                osm_type = elem.get("osm_type", "")
                geom     = elem.get("geom")

                if not geom:
                    print(f"[postgis-inserter] Skipping {osm_id}: no geometry")
                    continue

                wkt = _geojson_to_wkt(geom, osm_id)
                if wkt:
                    rows.append((osm_id, osm_type, wkt))
                else:
                    print(f"[postgis-inserter] Failed to convert geometry for {osm_id}")
                    continue

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

            try:
                if rows:
                    async with pool.acquire() as conn:
                        await conn.executemany(
                            f"""
                            INSERT INTO {TABLE} (osm_id, osm_type, geom)
                            VALUES ($1, $2, ST_GeomFromText($3, 4326))
                            ON CONFLICT (osm_id) DO UPDATE SET
                                osm_type = EXCLUDED.osm_type,
                                geom     = EXCLUDED.geom
                            """,
                            rows,
                        )
                    print(f"[postgis-inserter] Worker {worker_id}: Inserted {len(rows)} geometries")

                if addr_rows:
                    async with pool.acquire() as conn:
                        await conn.executemany(
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
                        )
                    print(f"[postgis-inserter] Worker {worker_id}: Inserted {len(addr_rows)} addresses")

                # Ack messages only after successful processing
                for msg in msgs:
                    await msg.ack()
            except Exception as db_err:
                print(f"[postgis-inserter] Worker {worker_id}: DB error, batch will be redelivered: {db_err}", flush=True)
                await asyncio.sleep(2)

    # Spawn multiple workers
    workers = [asyncio.create_task(worker(i)) for i in range(MAX_CONCURRENT_BATCHES)]
    print(f"[postgis-inserter] Started {MAX_CONCURRENT_BATCHES} concurrent workers", flush=True)
    
    # Wait for all workers (they run indefinitely)
    await asyncio.gather(*workers)

    await pool.close()
    await conn_state["nc"].close()


if __name__ == "__main__":
    asyncio.run(run())
