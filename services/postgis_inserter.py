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
)
from shared.nats_client import connect, subscribe

TABLE = "osm_geometries"


def _geojson_to_wkt(geom: dict) -> str | None:
    """Convert a GeoJSON geometry dict to WKT."""
    gtype = geom["type"]
    coords = geom["coordinates"]
    if gtype == "Point":
        return f"POINT({coords[0]} {coords[1]})"
    if gtype == "LineString":
        pts = ", ".join(f"{c[0]} {c[1]}" for c in coords)
        return f"LINESTRING({pts})"
    if gtype == "Polygon":
        rings = []
        for ring in coords:
            pts = ", ".join(f"{c[0]} {c[1]}" for c in ring)
            rings.append(f"({pts})")
        return f"POLYGON({', '.join(rings)})"
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

    while True:
        try:
            msgs = await sub.fetch(batch=100, timeout=5)
        except Exception:
            await asyncio.sleep(1)
            continue

        rows: list[tuple[str, str, str]] = []
        for msg in msgs:
            elem = json.loads(msg.data)
            await msg.ack()
            geom = elem.get("geom")
            if not geom:
                continue
            wkt = _geojson_to_wkt(geom)
            if wkt:
                rows.append((elem["osm_id"], elem.get("osm_type", ""), wkt))

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
        print(f"[postgis-inserter] Inserted {len(rows)} geometries")

    await pool.close()
    await nc.close()


if __name__ == "__main__":
    asyncio.run(run())
