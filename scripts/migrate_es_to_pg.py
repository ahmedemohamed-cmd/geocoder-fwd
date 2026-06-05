#!/usr/bin/env python3
"""Migrate data from Elasticsearch osm_places index to PostgreSQL osm_places table.

Usage:
    python3 scripts/migrate_es_to_pg.py

Connects to ES on localhost:9200 and PG on localhost:5432 by default.
Override with env vars: ELASTICSEARCH_URL, POSTGRES_HOST, POSTGRES_PORT, etc.
"""

import asyncio
import json
import os
import time

import asyncpg
from elasticsearch import AsyncElasticsearch

ES_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "postgres")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")

INDEX = "osm_places"
BATCH_SIZE = 2000
SCROLL_TIME = "5m"


def centroid_to_wkt(centroid: dict | None) -> str | None:
    if not centroid:
        return None
    lat = centroid.get("lat")
    lon = centroid.get("lon")
    if lat is None or lon is None:
        return None
    return f"POINT({lon} {lat})"


def geom_to_wkt(geom: dict | None) -> str | None:
    """Convert GeoJSON geometry to WKT. Returns None for unsupported types."""
    if not geom:
        return None
    try:
        gtype = geom.get("type")
        coords = geom.get("coordinates")
        if not gtype or coords is None:
            return None

        if gtype == "Point":
            return f"POINT({coords[0]} {coords[1]})"
        if gtype == "LineString":
            if not coords:
                return None
            pts = ", ".join(f"{c[0]} {c[1]}" for c in coords)
            return f"LINESTRING({pts})"
        if gtype == "Polygon":
            if not coords:
                return None
            rings = []
            for ring in coords:
                pts = ", ".join(f"{c[0]} {c[1]}" for c in ring)
                rings.append(f"({pts})")
            return f"POLYGON({', '.join(rings)})"
        if gtype == "MultiPolygon":
            if not coords:
                return None
            polygons = []
            for polygon in coords:
                rings = []
                for ring in polygon:
                    pts = ", ".join(f"{c[0]} {c[1]}" for c in ring)
                    rings.append(f"({pts})")
                polygons.append(f"({', '.join(rings)})")
            return f"MULTIPOLYGON({', '.join(polygons)})"
        if gtype == "MultiLineString":
            if not coords:
                return None
            lines = []
            for ls in coords:
                pts = ", ".join(f"{c[0]} {c[1]}" for c in ls)
                lines.append(f"({pts})")
            return f"MULTILINESTRING({', '.join(lines)})"
        return None
    except Exception as e:
        return None


async def run():
    print(f"[migrate] Connecting to ES at {ES_URL}")
    es = AsyncElasticsearch(ES_URL)

    # Check ES index
    info = await es.count(index=INDEX)
    total = info["count"]
    print(f"[migrate] ES index '{INDEX}' has {total:,} documents")

    print(f"[migrate] Connecting to PG at {PG_HOST}:{PG_PORT}")
    pool = await asyncpg.create_pool(
        host=PG_HOST, port=PG_PORT, database=PG_DB,
        user=PG_USER, password=PG_PASS,
        min_size=2, max_size=8,
    )

    # Run schema.sql to ensure table exists
    import pathlib
    schema_path = pathlib.Path(__file__).resolve().parent.parent / "sql" / "schema.sql"
    if schema_path.exists():
        async with pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            await conn.execute("CREATE EXTENSION IF NOT EXISTS unaccent")
            schema_sql = schema_path.read_text()
            await conn.execute(schema_sql)
        print(f"[migrate] Executed {schema_path}")
    else:
        print(f"[migrate] WARNING: {schema_path} not found!")

    # Check current PG count
    async with pool.acquire() as conn:
        pg_count = await conn.fetchval("SELECT count(*) FROM osm_places")
    print(f"[migrate] PG osm_places currently has {pg_count:,} rows")

    if pg_count >= total * 0.95:
        print(f"[migrate] PG already has ≥95% of ES data. Skipping migration.")
        await es.close()
        await pool.close()
        return

    # Scroll through ES and bulk insert into PG
    migrated = 0
    errors = 0
    start_time = time.monotonic()

    # Initial scroll
    resp = await es.search(
        index=INDEX,
        scroll=SCROLL_TIME,
        size=BATCH_SIZE,
        body={"query": {"match_all": {}}},
        _source=True,
    )
    scroll_id = resp["_scroll_id"]
    hits = resp["hits"]["hits"]

    while hits:
        rows = []
        for h in hits:
            src = h["_source"]
            osm_id = src.get("osm_id", h["_id"])
            tags = src.get("tags", {})

            geom_wkt = geom_to_wkt(src.get("geom"))
            centroid_wkt = centroid_to_wkt(src.get("centroid"))

            rows.append((
                osm_id,
                src.get("osm_type", ""),
                src.get("name", ""),
                src.get("name_en", ""),
                src.get("name_fr", ""),
                src.get("tags_text", ""),
                json.dumps(tags, ensure_ascii=False) if isinstance(tags, dict) else "{}",
                geom_wkt,
                centroid_wkt,
                src.get("admin_level", 0),
                src.get("area_km2", 0.0),
                src.get("offline_rank", 0.0),
                src.get("popularity", 0.0),
                src.get("addr_housenumber", ""),
                src.get("addr_street", ""),
                src.get("addr_city", ""),
                src.get("addr_postcode", ""),
                src.get("addr_country", ""),
                src.get("addr_suburb", ""),
                src.get("addr_state", ""),
                src.get("full_address", ""),
                bool(src.get("has_address", False)),
            ))

        if rows:
            try:
                async with pool.acquire() as conn:
                    await conn.executemany(
                        """
                        INSERT INTO osm_places
                            (osm_id, osm_type, name, name_en, name_fr,
                             tags_text, tags, geom, centroid,
                             admin_level, area_km2, offline_rank, popularity,
                             addr_housenumber, addr_street, addr_city,
                             addr_postcode, addr_country, addr_suburb,
                             addr_state, full_address, has_address)
                        VALUES (
                            $1, $2, $3, $4, $5,
                            $6, $7::jsonb,
                            CASE WHEN $8 IS NOT NULL THEN ST_GeomFromText($8, 4326) ELSE NULL END,
                            CASE WHEN $9 IS NOT NULL THEN ST_GeomFromText($9, 4326) ELSE NULL END,
                            $10, $11, $12, $13,
                            $14, $15, $16, $17, $18, $19, $20, $21, $22
                        )
                        ON CONFLICT (osm_id) DO UPDATE SET
                            osm_type         = EXCLUDED.osm_type,
                            name             = EXCLUDED.name,
                            name_en          = EXCLUDED.name_en,
                            name_fr          = EXCLUDED.name_fr,
                            tags_text        = EXCLUDED.tags_text,
                            tags             = EXCLUDED.tags,
                            geom             = EXCLUDED.geom,
                            centroid         = EXCLUDED.centroid,
                            admin_level      = EXCLUDED.admin_level,
                            area_km2         = EXCLUDED.area_km2,
                            offline_rank     = EXCLUDED.offline_rank,
                            addr_housenumber = EXCLUDED.addr_housenumber,
                            addr_street      = EXCLUDED.addr_street,
                            addr_city        = EXCLUDED.addr_city,
                            addr_postcode    = EXCLUDED.addr_postcode,
                            addr_country     = EXCLUDED.addr_country,
                            addr_suburb      = EXCLUDED.addr_suburb,
                            addr_state       = EXCLUDED.addr_state,
                            full_address     = EXCLUDED.full_address,
                            has_address      = EXCLUDED.has_address
                        """,
                        rows,
                    )
                migrated += len(rows)
            except Exception as e:
                errors += len(rows)
                print(f"[migrate] ERROR batch insert: {e}")

        elapsed = time.monotonic() - start_time
        rate = migrated / elapsed if elapsed > 0 else 0
        pct = (migrated / total * 100) if total > 0 else 0
        print(
            f"[migrate] {migrated:,}/{total:,} ({pct:.1f}%) "
            f"| {rate:.0f} rows/s | errors: {errors}",
            flush=True,
        )

        # Next scroll page
        resp = await es.scroll(scroll_id=scroll_id, scroll=SCROLL_TIME)
        scroll_id = resp["_scroll_id"]
        hits = resp["hits"]["hits"]

    # Cleanup scroll
    try:
        await es.clear_scroll(scroll_id=scroll_id)
    except Exception:
        pass

    elapsed = time.monotonic() - start_time
    print(f"\n[migrate] DONE: {migrated:,} rows migrated in {elapsed:.1f}s ({errors} errors)")

    # Verify
    async with pool.acquire() as conn:
        final_count = await conn.fetchval("SELECT count(*) FROM osm_places")
    print(f"[migrate] PG osm_places final count: {final_count:,}")

    await es.close()
    await pool.close()


if __name__ == "__main__":
    asyncio.run(run())
