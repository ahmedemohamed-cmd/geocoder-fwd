"""PostgreSQL-based search queries — replaces Elasticsearch.

Provides autocomplete, geocode, and structured address search using
pg_trgm (fuzzy/prefix matching), tsvector (full-text search), and
PostGIS (geo-distance scoring).

All functions accept an asyncpg.Pool and return plain dicts matching
the same shape the geocoder expects from Elasticsearch.
"""

import json
import math
from typing import Any

import asyncpg

# ── Trigram similarity threshold ──────────────────────────────────────────
# Lower = more fuzzy.  The default pg_trgm threshold is 0.3; we use 0.15
# to catch short prefix matches ("cai" → "Cairo") that have low trigram
# overlap but are valid autocomplete hits.
TRGM_THRESHOLD = 0.15


async def set_trgm_threshold(conn: asyncpg.Connection):
    """Set the trigram similarity threshold for this connection."""
    await conn.execute(f"SET pg_trgm.similarity_threshold = {TRGM_THRESHOLD}")


def _row_to_dict(row: asyncpg.Record) -> dict[str, Any]:
    """Convert an asyncpg Record to a geocoder result dict."""
    centroid = None
    if row.get("clat") is not None and row.get("clon") is not None:
        centroid = {"lat": row["clat"], "lon": row["clon"]}

    geom = None
    if row.get("geom_json"):
        try:
            geom = json.loads(row["geom_json"])
        except (json.JSONDecodeError, TypeError):
            pass

    return {
        "osm_id": row["osm_id"],
        "osm_type": row.get("osm_type", ""),
        "name": row.get("name", ""),
        "name_en": row.get("name_en", ""),
        "name_fr": row.get("name_fr", ""),
        "tags": json.loads(row["tags"]) if isinstance(row.get("tags"), str) else (row.get("tags") or {}),
        "tags_text": row.get("tags_text", ""),
        "geom": geom,
        "centroid": centroid,
        "admin_level": row.get("admin_level", 0),
        "area_km2": row.get("area_km2", 0),
        "offline_rank": row.get("offline_rank", 0),
        "popularity": row.get("popularity", 0),
        "confidence": row.get("score", 0),
        "full_address": row.get("full_address", ""),
        "addr_housenumber": row.get("addr_housenumber", ""),
        "addr_street": row.get("addr_street", ""),
        "addr_city": row.get("addr_city", ""),
        "addr_postcode": row.get("addr_postcode", ""),
        "addr_country": row.get("addr_country", ""),
        "addr_suburb": row.get("addr_suburb", ""),
        "addr_state": row.get("addr_state", ""),
        "address": json.loads(row["address"]) if isinstance(row.get("address"), str) else row.get("address"),
    }


# ══════════════════════════════════════════════════════════════════════════
# AUTOCOMPLETE
# ══════════════════════════════════════════════════════════════════════════

async def autocomplete(
    pool: asyncpg.Pool,
    q: str,
    limit: int = 7,
    lat: float | None = None,
    lon: float | None = None,
) -> list[dict[str, Any]]:
    """Fast prefix / fuzzy autocomplete query.

    Searches name, name_en, name_fr with both trigram similarity and
    ILIKE prefix, ranked by text similarity × offline_rank × popularity,
    with optional geo-bias.
    """
    q_lower = q.strip().lower()
    if not q_lower:
        return []

    # Synonym-expanded query for tsvector matching
    # Prefix pattern for ILIKE
    prefix = q_lower + "%"

    geo_score = ""
    geo_params = ""
    params: list[Any] = [q_lower, prefix]
    param_idx = 3  # next $N

    if lat is not None and lon is not None:
        geo_score = f"""
            * (1.0 / (1.0 + ST_Distance(
                p.centroid::geography,
                ST_MakePoint(${param_idx + 1}, ${param_idx})::geography
            ) / 15000.0))
        """
        params.extend([lat, lon])
        param_idx += 2

    query = f"""
        SELECT
            p.osm_id, p.name, p.name_en, p.name_fr,
            ST_Y(p.centroid) AS clat, ST_X(p.centroid) AS clon,
            p.admin_level, p.offline_rank, p.popularity,
            p.full_address, p.addr_street, p.addr_city, p.addr_country,
            -- scoring: best name similarity × ranking factors
            GREATEST(
                similarity(lower(p.name),    $1),
                similarity(lower(p.name_en), $1),
                similarity(lower(p.name_fr), $1),
                -- bonus for exact prefix match
                CASE WHEN lower(p.name)    LIKE $2 THEN 0.8
                     WHEN lower(p.name_en) LIKE $2 THEN 0.8
                     WHEN lower(p.name_fr) LIKE $2 THEN 0.8
                     ELSE 0 END
            )
            * (1.0 + p.offline_rank * 2.0 + ln(1.0 + p.popularity) * 1.5)
            {geo_score}
            AS score
        FROM osm_places p
        WHERE lower(p.name)    % $1
           OR lower(p.name_en) % $1
           OR lower(p.name_fr) % $1
           OR lower(p.name)    LIKE $2
           OR lower(p.name_en) LIKE $2
           OR lower(p.name_fr) LIKE $2
        ORDER BY score DESC
        LIMIT ${param_idx}
    """
    params.append(limit)

    async with pool.acquire() as conn:
        await set_trgm_threshold(conn)
        rows = await conn.fetch(query, *params)

    results = []
    max_score = rows[0]["score"] if rows else 1.0
    for row in rows:
        name = row["name_en"] or row["name"]
        addr = row.get("full_address", "")
        if addr and addr != name:
            label = f"{name}, {addr}" if name else addr
        else:
            label = name

        centroid = None
        if row["clat"] is not None and row["clon"] is not None:
            centroid = {"lat": row["clat"], "lon": row["clon"]}

        results.append({
            "osm_id": row["osm_id"],
            "label": label,
            "name": row["name"],
            "name_en": row["name_en"] or "",
            "name_fr": row["name_fr"] or "",
            "centroid": centroid,
            "admin_level": row["admin_level"],
            "confidence": round(row["score"] / max_score, 4) if max_score else 0,
        })

    return results


# ══════════════════════════════════════════════════════════════════════════
# GEOCODE  (full-text search)
# ══════════════════════════════════════════════════════════════════════════

async def geocode(
    pool: asyncpg.Pool,
    q: str,
    limit: int = 10,
    lat: float | None = None,
    lon: float | None = None,
    housenumber: str | None = None,
    street_query: str | None = None,
    is_address: bool = False,
) -> list[dict[str, Any]]:
    """Full-text geocoding query — replaces ES function_score.

    Combines trigram similarity on names/streets/address, tsvector full-text
    search on tags_text, exact housenumber matching, and ranking factors.
    """
    q_norm = q.strip().lower()
    if not q_norm:
        return []

    # Build parameters list
    params: list[Any] = [q_norm]  # $1 = query
    param_idx = 2

    # $2 = expanded query for tsvector (synonym-expanded)
    params.append(q_norm)  # expanded by DB function
    param_idx += 1

    # Street-specific query
    street_q = (street_query or q_norm).strip().lower()
    params.append(street_q)  # $3
    param_idx += 1

    # Housenumber
    hn_val = housenumber or ""
    params.append(hn_val)  # $4
    param_idx += 1

    # Geo params
    geo_score = "1.0"
    if lat is not None and lon is not None:
        geo_score = f"""
            (1.0 / (1.0 + ST_Distance(
                p.centroid::geography,
                ST_MakePoint(${param_idx + 1}, ${param_idx})::geography
            ) / 10000.0))
        """
        params.extend([lat, lon])
        param_idx += 2

    # Limit
    params.append(limit)
    limit_idx = param_idx
    param_idx += 1

    query = f"""
        WITH q AS (
            SELECT
                expand_synonyms($1) AS expanded_query,
                $3 AS street_q,
                $4 AS hn
        )
        SELECT
            p.osm_id, p.osm_type, p.name, p.name_en, p.name_fr,
            p.tags::text AS tags, p.tags_text,
            ST_AsGeoJSON(p.geom) AS geom_json,
            ST_Y(p.centroid) AS clat, ST_X(p.centroid) AS clon,
            p.admin_level, p.area_km2, p.offline_rank, p.popularity,
            p.full_address, p.addr_housenumber, p.addr_street,
            p.addr_city, p.addr_postcode, p.addr_country,
            p.addr_suburb, p.addr_state, p.has_address,
            p.address::text AS address,
            (
                -- ── Name similarity (replaces ES multi_match + phrase + and-operator) ──
                GREATEST(
                    similarity(lower(p.name), $1) * 5.0,
                    similarity(lower(p.name_en), $1) * 5.0,
                    similarity(lower(p.name_fr), $1) * 5.0
                )
                -- exact name match bonus (replaces ES operator=and boost=15)
                + CASE WHEN lower(p.name) = $1 OR lower(p.name_en) = $1 THEN 15.0
                       WHEN lower(p.name) LIKE $1 || '%' OR lower(p.name_en) LIKE $1 || '%' THEN 10.0
                       ELSE 0 END
                -- ── Full-text search on tags_text (replaces ES match on tags_text) ──
                + COALESCE(
                    ts_rank_cd(p.search_vector,
                        plainto_tsquery('simple', arabic_normalize(q.expanded_query)),
                        32  -- rank normalization: divide by 1 + document length
                    ) * 3.0,
                    0
                )
                -- ── Street matching (replaces ES addr_street phrase + fuzzy) ──
                + CASE WHEN p.addr_street != '' THEN
                    GREATEST(
                        similarity(lower(p.addr_street), q.street_q) * 10.0,
                        CASE WHEN lower(p.addr_street) LIKE q.street_q || '%' THEN 8.0 ELSE 0 END
                    )
                  ELSE 0 END
                -- ── Full address match (replaces ES full_address phrase slop=2) ──
                + CASE WHEN p.full_address != '' THEN
                    similarity(lower(p.full_address), $1) * 6.0
                  ELSE 0 END
                -- ── Exact housenumber (replaces ES term + combined bool boost=50) ──
                + CASE
                    WHEN q.hn != '' AND p.addr_housenumber = q.hn THEN 50.0
                    WHEN q.hn != '' AND p.addr_housenumber ~ '^\\d+$' AND q.hn ~ '^\\d+$' THEN
                        5.0 / (1.0 + abs(p.addr_housenumber::int - q.hn::int))
                    ELSE 0 END
                -- ── has_address bonus ──
                + CASE WHEN p.has_address THEN 2.0 ELSE 0 END
            )
            -- ── Multiply by ranking factors (replaces ES function_score boost_mode=multiply) ──
            * (1.0 + p.offline_rank * 2.0 + ln(1.0 + p.popularity) * 1.0)
            -- ── Geo-distance decay ──
            * {geo_score}
            AS score
        FROM osm_places p, q
        WHERE
            -- candidate filtering: must match at least one criterion
            p.search_vector @@ plainto_tsquery('simple', arabic_normalize(q.expanded_query))
            OR p.name    % $1
            OR p.name_en % $1
            OR p.name_fr % $1
            OR p.addr_street % q.street_q
            OR p.full_address % $1
            {"OR p.addr_housenumber = $4" if housenumber else ""}
        ORDER BY score DESC
        LIMIT ${limit_idx}
    """

    async with pool.acquire() as conn:
        await set_trgm_threshold(conn)
        rows = await conn.fetch(query, *params)

    if not rows:
        return []

    max_score = rows[0]["score"] if rows[0]["score"] else 1.0
    results = []
    for row in rows:
        d = _row_to_dict(row)
        d["confidence"] = round(row["score"] / max_score, 4) if max_score else 0
        results.append(d)

    return results


# ══════════════════════════════════════════════════════════════════════════
# STRUCTURED ADDRESS SEARCH
# ══════════════════════════════════════════════════════════════════════════

async def address_search(
    pool: asyncpg.Pool,
    q: str,
    limit: int = 10,
    lat: float | None = None,
    lon: float | None = None,
    housenumber: str | None = None,
    street: str | None = None,
    city: str | None = None,
    postcode: str | None = None,
    country: str | None = None,
) -> list[dict[str, Any]]:
    """Structured address search — replaces ES /address endpoint.

    Supports hard filters (postcode, country, city) plus scored matching
    on street, housenumber, and full address.
    """
    q_norm = q.strip().lower()
    if not q_norm:
        return []

    params: list[Any] = [q_norm]  # $1
    param_idx = 2

    street_q = (street or q_norm).strip().lower()
    params.append(street_q)  # $2
    param_idx += 1

    hn_val = housenumber or ""
    params.append(hn_val)  # $3
    param_idx += 1

    # Build hard filter clauses
    filters = []
    if postcode:
        params.append(postcode)
        filters.append(f"p.addr_postcode = ${param_idx}")
        param_idx += 1
    if country:
        params.append(country.upper())
        filters.append(f"p.addr_country = ${param_idx}")
        param_idx += 1
    if city:
        city_lower = city.strip().lower()
        params.append(city_lower)
        filters.append(f"lower(p.addr_city) = ${param_idx}")
        param_idx += 1

    filter_clause = " AND ".join(filters) if filters else "TRUE"

    # Geo scoring
    geo_score = "1.0"
    if lat is not None and lon is not None:
        geo_score = f"""
            (1.0 / (1.0 + ST_Distance(
                p.centroid::geography,
                ST_MakePoint(${param_idx + 1}, ${param_idx})::geography
            ) / 1000.0))
        """
        params.extend([lat, lon])
        param_idx += 2

    params.append(limit)
    limit_idx = param_idx
    param_idx += 1

    query = f"""
        WITH q AS (
            SELECT
                expand_synonyms($1) AS expanded_query,
                $2 AS street_q,
                $3 AS hn
        )
        SELECT
            p.osm_id, p.osm_type, p.name, p.name_en, p.name_fr,
            p.tags::text AS tags, p.tags_text,
            ST_AsGeoJSON(p.geom) AS geom_json,
            ST_Y(p.centroid) AS clat, ST_X(p.centroid) AS clon,
            p.admin_level, p.area_km2, p.offline_rank, p.popularity,
            p.full_address, p.addr_housenumber, p.addr_street,
            p.addr_city, p.addr_postcode, p.addr_country,
            p.addr_suburb, p.addr_state, p.has_address,
            p.address::text AS address,
            (
                -- street match (dominant signal for address search)
                CASE WHEN p.addr_street != '' THEN
                    GREATEST(
                        similarity(lower(p.addr_street), q.street_q) * 10.0,
                        CASE WHEN lower(p.addr_street) LIKE q.street_q || '%' THEN 8.0 ELSE 0 END
                    )
                ELSE 0 END
                -- full address phrase match
                + CASE WHEN p.full_address != '' THEN
                    similarity(lower(p.full_address), $1) * 8.0
                  ELSE 0 END
                -- exact housenumber
                + CASE
                    WHEN q.hn != '' AND p.addr_housenumber = q.hn THEN 50.0
                    WHEN q.hn != '' AND p.addr_housenumber ~ '^\\d+$' AND q.hn ~ '^\\d+$' THEN
                        5.0 / (1.0 + abs(p.addr_housenumber::int - q.hn::int))
                    ELSE 0 END
                -- name fallback
                + GREATEST(
                    similarity(lower(p.name), $1) * 3.0,
                    similarity(lower(p.name_en), $1) * 3.0,
                    similarity(lower(p.name_fr), $1) * 3.0
                )
                -- full-text on tags
                + COALESCE(
                    ts_rank_cd(p.search_vector,
                        plainto_tsquery('simple', arabic_normalize(q.expanded_query)),
                        32
                    ) * 0.5,
                    0
                )
                -- has address bonus (strong for /address)
                + CASE WHEN p.has_address THEN 3.0 ELSE 0 END
            )
            * (1.0 + p.offline_rank * 1.5 + ln(1.0 + p.popularity) * 0.5)
            * {geo_score}
            AS score
        FROM osm_places p, q
        WHERE ({filter_clause})
          AND (
            p.search_vector @@ plainto_tsquery('simple', arabic_normalize(q.expanded_query))
            OR p.addr_street % q.street_q
            OR p.full_address % $1
            OR p.name % $1
            OR p.name_en % $1
            {"OR p.addr_housenumber = $3" if housenumber else ""}
          )
        ORDER BY score DESC
        LIMIT ${limit_idx}
    """

    async with pool.acquire() as conn:
        await set_trgm_threshold(conn)
        rows = await conn.fetch(query, *params)

    if not rows:
        return []

    max_score = rows[0]["score"] if rows[0]["score"] else 1.0
    results = []
    for row in rows:
        d = _row_to_dict(row)
        d["confidence"] = round(row["score"] / max_score, 4) if max_score else 0
        results.append(d)

    return results


# ══════════════════════════════════════════════════════════════════════════
# SINGLE-DOC LOOKUPS (replace es.get / es.mget)
# ══════════════════════════════════════════════════════════════════════════

async def get_place(pool: asyncpg.Pool, osm_id: str) -> dict[str, Any] | None:
    """Fetch a single place by osm_id — replaces es.get()."""
    query = """
        SELECT
            osm_id, osm_type, name, name_en, name_fr,
            tags::text AS tags, tags_text,
            ST_AsGeoJSON(geom) AS geom_json,
            ST_Y(centroid) AS clat, ST_X(centroid) AS clon,
            admin_level, area_km2, offline_rank, popularity,
            full_address, addr_housenumber, addr_street,
            addr_city, addr_postcode, addr_country,
            addr_suburb, addr_state, has_address,
            address::text AS address,
            ai_description::text AS ai_description_json
        FROM osm_places
        WHERE osm_id = $1
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, osm_id)
    if not row:
        return None
    return _row_to_dict(row)


async def mget_places(pool: asyncpg.Pool, osm_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Fetch multiple places by osm_id — replaces es.mget()."""
    if not osm_ids:
        return {}
    query = """
        SELECT
            osm_id, osm_type, name, name_en, name_fr,
            tags::text AS tags, tags_text,
            ST_AsGeoJSON(geom) AS geom_json,
            ST_Y(centroid) AS clat, ST_X(centroid) AS clon,
            admin_level, area_km2, offline_rank, popularity,
            full_address, addr_housenumber, addr_street,
            addr_city, addr_postcode, addr_country,
            addr_suburb, addr_state, has_address,
            address::text AS address
        FROM osm_places
        WHERE osm_id = ANY($1)
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, osm_ids)
    return {row["osm_id"]: _row_to_dict(row) for row in rows}


# ══════════════════════════════════════════════════════════════════════════
# UPDATE helpers (replace es.update)
# ══════════════════════════════════════════════════════════════════════════

async def update_popularity(pool: asyncpg.Pool, osm_id: str, boost: float, max_pop: float = 1000.0):
    """Increment popularity — replaces ES scripted update."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE osm_places
            SET popularity = LEAST(popularity + $2, $3)
            WHERE osm_id = $1
            """,
            osm_id, boost, max_pop,
        )


async def cache_address(pool: asyncpg.Pool, osm_id: str, address: dict):
    """Cache enriched address — replaces ES doc update."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE osm_places SET address = $2::jsonb WHERE osm_id = $1",
            osm_id, json.dumps(address, ensure_ascii=False),
        )


async def cache_description(pool: asyncpg.Pool, osm_id: str, description: dict):
    """Cache AI description — replaces ES doc update."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE osm_places SET ai_description = $2::jsonb WHERE osm_id = $1",
            osm_id, json.dumps(description, ensure_ascii=False),
        )


async def get_description(pool: asyncpg.Pool, osm_id: str) -> dict | None:
    """Get cached AI description — replaces ES get for descriptions."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT ai_description::text AS desc FROM osm_places WHERE osm_id = $1",
            osm_id,
        )
    if not row or not row["desc"]:
        return None
    try:
        return json.loads(row["desc"])
    except (json.JSONDecodeError, TypeError):
        return None
