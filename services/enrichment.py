"""Reverse-geocode address enrichment — the shared ``address`` object.

A POI rarely tags its own ``addr:*``, so its flat address fields are empty. This
module derives the missing context by a PostGIS spatial join around the feature's
centroid — the nearest named street plus the enclosing admin parents — and caches
the result per-doc in the ES ``_source.address`` field (compute-once). Shape::

    {
        "nearest_street": {"osm_id", "name", "name_en", "name_fr"} | None,
        "parents": [{"osm_id", "name", "name_en", "name_fr", "admin_level", "area_km2"}, ...],
    }

This is the single implementation used by both ``/geocode`` (via thin wrappers in
``services/geocoder.py``) and ``/nearby`` (``services/nearby.py``). It takes the
``pg_pool`` / ``es`` clients as explicit parameters (mirroring the
``shared.interpolation.reverse_interpolate(pg_pool, ...)`` precedent) so it stays
client-free and importable from either side without an import cycle — neither
``services.geocoder`` nor ``services.nearby`` is imported here.
"""

from __future__ import annotations

import asyncio

from shared.logging import get_logger

logger = get_logger("enrichment")

# Features larger than this (km²) are treated as areas, not point-like places:
# a "nearest street to the centroid" is meaningless for them, so it's skipped.
# 0.1 km² = 10 hectares (~316 m square): comfortably above any single building
# or POI polygon (malls, hospitals, campuses all keep their nearest street) but
# well below neighbourhood/district scale (Fifth Settlement is 10 km²), where
# the centroid-nearest line is just noise. Admin-level features are always
# skipped regardless of size. Tunable.
_NEAREST_STREET_MAX_AREA_KM2 = 0.1

# Fire-and-forget background enrichment bookkeeping. `_enrich_inflight` dedupes
# concurrent requests for the same doc (shared across every endpoint in the
# process); `_enrich_bg_tasks` holds strong refs so tasks aren't GC'd mid-flight.
_enrich_inflight: set[str] = set()
_enrich_bg_tasks: set[asyncio.Task] = set()


async def enrich_address(
    pg_pool,
    es,
    index: str,
    osm_id: str,
    centroid: dict | None,
    self_area: float = 0.0,
    admin_level: int | None = None,
) -> dict | None:
    """Look up address/parent data from PostGIS and cache it in ES.

    ``self_area`` is the feature's own footprint in km². Parents are discovered
    by point-in-polygon on the centroid, which — for an *area* feature — also
    returns sub-zones that merely contain the centroid (e.g. a small block
    *inside* a district). A genuine parent ENCLOSES the feature and is therefore
    larger than it, so any candidate not strictly larger than ``self_area`` is a
    child/sibling and is dropped. Point features (``self_area == 0``) keep every
    enclosing polygon.

    ``nearest_street`` is skipped entirely for features that are administrative
    boundaries (``admin_level`` not null) or larger than
    ``_NEAREST_STREET_MAX_AREA_KM2``: for such areas the centroid sits arbitrarily
    inside the region, so the closest line to it is not a meaningful address and
    the (heavier) nearest-line query is pure cost. Parents are still computed.

    Returns the address dict or None if the centroid is missing.
    """
    if not centroid:
        return None

    # centroid is stored as {"lat": ..., "lon": ...} in ES
    lat = centroid.get("lat")
    lon = centroid.get("lon")
    if lat is None or lon is None:
        return None

    point_wkt = f"POINT({lon} {lat})"

    # A nearest street only makes sense for point-like features. For admin
    # boundaries or large areas the centroid is arbitrary, so skip the (heavier)
    # nearest-line lookup and report no street. admin_level 0/None both mean
    # "not an administrative boundary" (0 is the Google/deep-path sentinel).
    skip_nearest = bool(admin_level) or self_area > _NEAREST_STREET_MAX_AREA_KM2

    try:
        async with pg_pool.acquire() as conn:
            # Set a query timeout to avoid slow enrichment blocking the API
            await conn.execute("SET LOCAL statement_timeout = '3000'")  # 3s

            # Find nearest lines (fetch several to find one with a name)
            nearest_lines = []
            if not skip_nearest:
                nearest_lines_query = """
                    SELECT osm_id, osm_type
                    FROM osm_geometries
                    WHERE ST_GeometryType(geom) = 'ST_LineString'
                      AND ST_DWithin(geom, ST_GeomFromText($1, 4326), 0.005)
                    ORDER BY ST_Distance(geom, ST_GeomFromText($1, 4326))
                    LIMIT 10
                """
                nearest_lines = await conn.fetch(nearest_lines_query, point_wkt)

            # Find enclosing polygons/multipolygons
            enclosing_query = """
                SELECT osm_id, osm_type
                FROM osm_geometries
                WHERE ST_GeometryType(geom) IN ('ST_Polygon', 'ST_MultiPolygon')
                AND ST_Contains(geom, ST_GeomFromText($1, 4326))
            """
            enclosing_polygons = await conn.fetch(enclosing_query, point_wkt)

            # Also check closed LineStrings that form boundaries around the point
            closed_lines_query = """
                SELECT osm_id, osm_type
                FROM osm_geometries
                WHERE ST_GeometryType(geom) = 'ST_LineString'
                AND ST_IsClosed(geom)
                AND ST_NPoints(geom) >= 4
                AND ST_Contains(ST_MakePolygon(geom), ST_GeomFromText($1, 4326))
            """
            closed_lines = await conn.fetch(closed_lines_query, point_wkt)
    except Exception as e:
        logger.error(f"[enrichment] PostGIS query failed for {osm_id}: {e}")
        return None

    # Collect all osm_ids to fetch from ES.
    # Exclude the element's own osm_id: a polygon's centroid falls inside itself,
    # so without this filter the element would list itself as its own parent.
    line_ids = [row["osm_id"] for row in nearest_lines if row["osm_id"] != osm_id]
    parent_ids = [row["osm_id"] for row in enclosing_polygons if row["osm_id"] != osm_id]
    parent_ids.extend(row["osm_id"] for row in closed_lines if row["osm_id"] != osm_id)

    all_ids = list(set(line_ids + parent_ids))
    if not all_ids:
        address = {"nearest_street": None, "parents": []}
        # Cache even empty results to avoid repeated lookups
        try:
            await es.update(index=index, id=osm_id, body={"doc": {"address": address}})
        except Exception:
            logger.debug("Failed to cache empty address for %s", osm_id, exc_info=True)
        return address

    # Batch-fetch metadata from ES
    es_data: dict[str, dict] = {}
    try:
        resp = await es.mget(index=index, ids=all_ids, request_timeout=5)
        for doc in resp["docs"]:
            if doc.get("found"):
                es_data[doc["_id"]] = doc["_source"]
    except Exception as e:
        logger.error(f"[enrichment] Error fetching address data from ES: {e}")

    # Find nearest street: first line in distance order that has a name
    nearest_street = None
    for row in nearest_lines:
        src = es_data.get(row["osm_id"])
        if src and (src.get("name") or src.get("name_en") or src.get("name_fr")):
            nearest_street = {
                "osm_id": row["osm_id"],
                "name": src.get("name", ""),
                "name_en": src.get("name_en", ""),
                "name_fr": src.get("name_fr", ""),
            }
            break

    # Build parents list from enclosing polygons + closed lines
    parents = []
    seen = set()
    for row_id in parent_ids:
        if row_id in seen:
            continue
        seen.add(row_id)
        src = es_data.get(row_id)
        if src and (src.get("name") or src.get("name_en") or src.get("name_fr")):
            cand_area = src.get("area_km2", 0) or 0
            # Enclosing parent must be larger than the feature; a smaller polygon
            # that only contains the centroid is a child/sub-zone, not a parent.
            if self_area > 0 and cand_area <= self_area:
                continue
            parents.append(
                {
                    "osm_id": row_id,
                    "name": src.get("name", ""),
                    "name_en": src.get("name_en", ""),
                    "name_fr": src.get("name_fr", ""),
                    "admin_level": src.get("admin_level"),
                    "area_km2": cand_area,
                }
            )
    # Smallest area first (most specific enclosing polygon), then by admin_level
    # descending as a tiebreaker. None admin_level sorts last within same area.
    parents.sort(
        key=lambda p: (
            p.get("area_km2", 0),
            -p["admin_level"] if p["admin_level"] is not None else float("inf"),
        )
    )

    address = {"nearest_street": nearest_street, "parents": parents}

    # Cache the address data in ES
    try:
        await es.update(index=index, id=osm_id, body={"doc": {"address": address}})
    except Exception as e:
        logger.error(f"[enrichment] Error caching address for {osm_id}: {e}")

    return address


def schedule_enrichment(
    pg_pool,
    es,
    index: str,
    osm_id: str,
    centroid: dict | None,
    self_area: float = 0.0,
    admin_level: int | None = None,
) -> None:
    """Enrich + cache a doc's address once, in the background (non-blocking).

    No-op if the doc is already being enriched. The result is persisted to ES by
    :func:`enrich_address` so subsequent searches read it straight from the index.
    """
    if not osm_id or osm_id in _enrich_inflight:
        return
    _enrich_inflight.add(osm_id)

    async def _run():
        try:
            await enrich_address(pg_pool, es, index, osm_id, centroid, self_area, admin_level)
        except Exception:
            logger.warning("Background enrichment failed for %s", osm_id, exc_info=True)
        finally:
            _enrich_inflight.discard(osm_id)

    task = asyncio.create_task(_run())
    _enrich_bg_tasks.add(task)
    task.add_done_callback(_enrich_bg_tasks.discard)


def address_needs_refresh(
    address: dict | None,
    osm_id: str,
    self_area: float,
    admin_level: int | None,
) -> bool:
    """Whether a doc's cached ``address`` should be (re)computed.

    True when it's absent, or stale in any of the ways the write format has since
    tightened: a parent missing ``area_km2`` (pre-``area_km2`` cache), a
    self-referential parent, a parent not strictly larger than the feature, or a
    ``nearest_street`` cached on an admin/large-area doc that should have none.
    """
    if address is None:
        return True
    cached_parents = address.get("parents") or []
    stale = any("area_km2" not in p for p in cached_parents)
    self_ref = any(p.get("osm_id") == osm_id for p in cached_parents)
    smaller_parent = self_area > 0 and any(
        (p.get("area_km2", 0) or 0) <= self_area for p in cached_parents
    )
    skip_nearest = bool(admin_level) or self_area > _NEAREST_STREET_MAX_AREA_KM2
    street_on_area = skip_nearest and address.get("nearest_street") is not None
    return stale or self_ref or smaller_parent or street_on_area
