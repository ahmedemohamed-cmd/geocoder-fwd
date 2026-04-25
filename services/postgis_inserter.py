"""Consume OSM elements from NATS JS and store geometry in PostGIS.

Table: osm_geometries
  osm_id   TEXT PRIMARY KEY
  osm_type TEXT
  geom     geometry(Geometry, 4326)
"""

import asyncio
import json

import asyncpg

from shared.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    BATCH_SIZE,
    MAX_CONCURRENT_BATCHES,
)
from shared.nats_client import connect, subscribe

TABLE = "osm_geometries"


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
    sub = await subscribe(js, "postgis-consumer")
    print("[postgis-inserter] Listening for messages ...")

    # Create worker pool for concurrent batch processing
    async def worker(worker_id: int):
        """Worker that fetches and processes batches concurrently."""
        while True:
            try:
                msgs = await sub.fetch(batch=BATCH_SIZE, timeout=5)
            except Exception:
                await asyncio.sleep(1)
                continue

            rows: list[tuple[str, str, str]] = []
            for msg in msgs:
                elem = json.loads(msg.data)
                await msg.ack()
                geom = elem.get("geom")
                if not geom:
                    print(f"[postgis-inserter] Skipping {elem.get('osm_id', 'unknown')}: no geometry")
                    continue
                wkt = _geojson_to_wkt(geom, elem.get("osm_id", ""))
                if wkt:
                    rows.append((elem["osm_id"], elem.get("osm_type", ""), wkt))
                else:
                    print(f"[postgis-inserter] Failed to convert geometry for {elem.get('osm_id', 'unknown')}")

            if not rows:
                continue

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

    # Spawn multiple workers
    workers = [asyncio.create_task(worker(i)) for i in range(MAX_CONCURRENT_BATCHES)]
    print(f"[postgis-inserter] Started {MAX_CONCURRENT_BATCHES} concurrent workers", flush=True)
    
    # Wait for all workers (they run indefinitely)
    await asyncio.gather(*workers)

    await pool.close()
    await nc.close()


if __name__ == "__main__":
    asyncio.run(run())
