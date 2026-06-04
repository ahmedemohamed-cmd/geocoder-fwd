"""Consume OSM elements from NATS JS and store into PostgreSQL.

Tables populated
----------------
osm_geometries   – raw geometry for reverse-geocoding spatial queries
osm_addresses    – structured address rows for nearest-address / interpolation
osm_places       – full-text search table (replaces Elasticsearch)
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
from shared.address import extract_address_components, build_full_address, has_address, normalize_address_text
from shared.centroid import centroid_latlon
from shared.ranking import compute_offline_rank
from shared.embeddings import build_text

TABLE         = "osm_geometries"
ADDRESS_TABLE = "osm_addresses"
PLACES_TABLE  = "osm_places"


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


# ---------------------------------------------------------------------------
# Geometry simplification — same as es_inserter to prevent huge polygons
# ---------------------------------------------------------------------------
MAX_GEOM_VERTICES = 2000

def _count_ring(ring):
    return len(ring) if ring else 0

def _count_vertices(geom):
    gtype = geom.get("type", "")
    coords = geom.get("coordinates")
    if not coords:
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
    if len(ring) <= 4:
        return ring
    thinned = [ring[i] for i in range(0, len(ring) - 1, keep_every)]
    if thinned[-1] != ring[-1]:
        thinned.append(ring[-1])
    if len(thinned) < 4:
        return ring[:3] + [ring[-1]]
    return thinned

def simplify_geometry(geom):
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
    if gtype == "LineString" and coords and len(coords) > MAX_GEOM_VERTICES:
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
    return geom


async def ensure_table(pool: asyncpg.Pool):
    try:
        async with pool.acquire() as conn:
            print(f"[postgis-inserter] Creating extensions...")
            await conn.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            await conn.execute("CREATE EXTENSION IF NOT EXISTS unaccent")
            print(f"[postgis-inserter] Extensions created")
            
            # ── osm_geometries (unchanged) ────────────────────────────────
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
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom ON {TABLE} USING GIST (geom)"
            )
            print(f"[postgis-inserter] Table {TABLE} ready")

            # ── osm_addresses (unchanged) ─────────────────────────────────
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
            print(f"[postgis-inserter] Table {ADDRESS_TABLE} ready")

            # ── osm_places (NEW — replaces Elasticsearch) ─────────────────
            print(f"[postgis-inserter] Creating table {PLACES_TABLE}...")

            # Run the full schema SQL
            import pathlib
            schema_path = pathlib.Path(__file__).resolve().parent.parent / "sql" / "schema.sql"
            if schema_path.exists():
                schema_sql = schema_path.read_text()
                await conn.execute(schema_sql)
                print(f"[postgis-inserter] Executed {schema_path}")
            else:
                print(f"[postgis-inserter] WARNING: {schema_path} not found, creating minimal table")
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {PLACES_TABLE} (
                        osm_id TEXT PRIMARY KEY,
                        osm_type TEXT NOT NULL DEFAULT '',
                        name TEXT NOT NULL DEFAULT '',
                        name_en TEXT NOT NULL DEFAULT '',
                        name_fr TEXT NOT NULL DEFAULT '',
                        tags_text TEXT NOT NULL DEFAULT '',
                        tags JSONB NOT NULL DEFAULT '{{}}',
                        geom geometry(Geometry, 4326),
                        centroid geometry(Point, 4326),
                        admin_level INT NOT NULL DEFAULT 0,
                        area_km2 FLOAT NOT NULL DEFAULT 0,
                        offline_rank FLOAT NOT NULL DEFAULT 0,
                        popularity FLOAT NOT NULL DEFAULT 0,
                        addr_housenumber TEXT NOT NULL DEFAULT '',
                        addr_street TEXT NOT NULL DEFAULT '',
                        addr_city TEXT NOT NULL DEFAULT '',
                        addr_postcode TEXT NOT NULL DEFAULT '',
                        addr_country TEXT NOT NULL DEFAULT '',
                        addr_suburb TEXT NOT NULL DEFAULT '',
                        addr_state TEXT NOT NULL DEFAULT '',
                        full_address TEXT NOT NULL DEFAULT '',
                        has_address BOOLEAN NOT NULL DEFAULT FALSE,
                        search_vector tsvector,
                        ai_description JSONB,
                        address JSONB
                    )
                """)

            # Verify
            result = await conn.fetch(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename IN ($1, $2, $3)",
                TABLE, ADDRESS_TABLE, PLACES_TABLE
            )
            existing_tables = [row['tablename'] for row in result]
            print(f"[postgis-inserter] Verified tables: {existing_tables}")
            
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

    conn_state = {"nc": nc, "js": js, "sub": sub}
    reconnect_lock = asyncio.Lock()

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
                except nats.errors.TimeoutError:
                    msgs = []
                    break
                except Exception as e:
                    is_conn_err = is_connection_error(e)
                    is_transient = is_transient_error(e)
                    print(f"[postgis-inserter] Fetcher: error (attempt {fetch_attempt + 1}/{max_fetch_retries}): {e} (transient: {is_transient}, conn: {is_conn_err})", flush=True)

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
        """Worker that processes batches and inserts into all three tables."""
        while True:
            msgs = await work_queue.get()
            batch_start = _time.monotonic()

            geom_rows: list[tuple] = []
            addr_rows: list[tuple] = []
            place_rows: list[tuple] = []

            for msg in msgs:
                elem = json.loads(msg.data)
                osm_id   = elem.get("osm_id", "unknown")
                osm_type = elem.get("osm_type", "")
                tags     = elem.get("tags", {})
                geom     = elem.get("geom")

                # ── osm_geometries row ────────────────────────────────────
                wkt = None
                if geom:
                    wkt = _geojson_to_wkt(geom, osm_id)
                    if wkt:
                        geom_rows.append((osm_id, osm_type, wkt))

                # ── osm_addresses row ─────────────────────────────────────
                if geom and has_address(tags):
                    addr = extract_address_components(tags)
                    faddr = build_full_address(tags)
                    c = centroid_latlon(geom)
                    cwkt = f"POINT({c['lon']} {c['lat']})" if c else wkt
                    addr_rows.append((
                        osm_id, osm_type,
                        addr.get("housenumber", ""),
                        addr.get("street", ""),
                        addr.get("city", ""),
                        addr.get("postcode", ""),
                        addr.get("country", ""),
                        faddr,
                        cwkt,
                    ))

                # ── osm_places row (replaces ES) ─────────────────────────
                name    = tags.get("name", "")
                name_en = tags.get("name:en", "")
                name_fr = tags.get("name:fr", "")

                # Skip elements with no name and no address — not searchable
                txt = build_text(tags)
                if not name and not name_en and not txt:
                    continue

                admin_level = elem.get("admin_level", 0)
                area_km2    = elem.get("area_km2", 0.0)
                rank        = compute_offline_rank(tags, admin_level, area_km2)

                addr_comp  = extract_address_components(tags)
                full_addr  = normalize_address_text(build_full_address(tags))

                # Geometry for osm_places — simplified to avoid index bloat
                place_wkt = None
                centroid_wkt = None
                if geom:
                    simplified = simplify_geometry(geom)
                    place_wkt = _geojson_to_wkt(simplified, osm_id)
                    c = centroid_latlon(geom)
                    if c:
                        centroid_wkt = f"POINT({c['lon']} {c['lat']})"

                place_rows.append((
                    osm_id,
                    osm_type,
                    name,
                    name_en,
                    name_fr,
                    txt,                                  # tags_text
                    json.dumps(tags, ensure_ascii=False),  # tags JSONB
                    place_wkt,                             # geom
                    centroid_wkt,                          # centroid
                    admin_level,
                    area_km2,
                    rank,                                  # offline_rank
                    0.0,                                   # popularity (starts at 0)
                    addr_comp.get("housenumber", ""),
                    addr_comp.get("street", ""),
                    addr_comp.get("city", ""),
                    addr_comp.get("postcode", ""),
                    addr_comp.get("country", ""),
                    addr_comp.get("suburb", ""),
                    addr_comp.get("state", ""),
                    full_addr,                             # full_address
                    bool(full_addr),                       # has_address
                ))

            # ── Batch insert: osm_geometries ──────────────────────────────
            if geom_rows:
                async with pool.acquire() as conn:
                    await conn.executemany(
                        f"""
                        INSERT INTO {TABLE} (osm_id, osm_type, geom)
                        VALUES ($1, $2, ST_GeomFromText($3, 4326))
                        ON CONFLICT (osm_id) DO UPDATE SET
                            osm_type = EXCLUDED.osm_type,
                            geom     = EXCLUDED.geom
                        """,
                        geom_rows,
                    )

            # ── Batch insert: osm_addresses ───────────────────────────────
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

            # ── Batch insert: osm_places ──────────────────────────────────
            if place_rows:
                async with pool.acquire() as conn:
                    await conn.executemany(
                        f"""
                        INSERT INTO {PLACES_TABLE}
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
                        place_rows,
                    )

            # Ack messages only after successful processing
            for msg in msgs:
                await msg.ack()

            elapsed = _time.monotonic() - batch_start
            throughput = len(geom_rows) / elapsed if elapsed > 0 else 0
            print(
                f"[postgis-inserter] Worker {worker_id}: "
                f"{len(geom_rows)} geoms + {len(addr_rows)} addrs + {len(place_rows)} places "
                f"in {elapsed:.2f}s ({throughput:.0f} rows/s)",
                flush=True,
            )
            work_queue.task_done()

    # Spawn one fetcher + multiple processing workers
    tasks = [asyncio.create_task(fetcher())]
    tasks += [asyncio.create_task(worker(i)) for i in range(MAX_CONCURRENT_BATCHES)]
    print(f"[postgis-inserter] Started 1 fetcher + {MAX_CONCURRENT_BATCHES} processing workers (pipeline parallel)", flush=True)
    
    await asyncio.gather(*tasks)

    await pool.close()
    await conn_state["nc"].close()


if __name__ == "__main__":
    asyncio.run(run())
