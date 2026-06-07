"""Address interpolation module.

Estimates the geographic position of addresses that don't exist in the
database by linearly interpolating between known addresses on the same
street, respecting odd/even numbering conventions.

Algorithm
---------
1. Query ``osm_addresses`` for all known housenumbers on the target street.
2. Separate addresses into odd and even sets.
3. Find the two *bracketing* addresses on the **same parity side** as the
   requested number (e.g. for #15, find #11 and #19 from the odd set).
4. Optionally snap the interpolated point onto the nearest street
   LineString geometry from ``osm_geometries``.
5. Return estimated coordinates with ``match_type: "interpolated"`` and
   a confidence score based on the gap size.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class InterpolatedAddress:
    """Result of an address interpolation."""
    housenumber: str
    street: str
    city: str
    postcode: str
    country: str
    lat: float
    lon: float
    match_type: str          # "exact" | "interpolated"
    confidence: float        # 0.0 – 1.0
    side: str                # "odd" | "even" | "unknown"
    bracket_low: str | None  # e.g. "10"
    bracket_high: str | None # e.g. "20"


def _parse_housenumber(hn: str) -> int | None:
    """Parse a housenumber string to int, handling common formats."""
    if not hn:
        return None
    # Strip letters/suffixes like "12A", "14bis"
    digits = ""
    for ch in hn:
        if ch.isdigit():
            digits += ch
        elif digits:
            break
    try:
        return int(digits) if digits else None
    except ValueError:
        return None


def _parity(n: int) -> str:
    """Return 'odd' or 'even'."""
    return "odd" if n % 2 else "even"


def _interpolation_confidence(gap: int) -> float:
    """Confidence score based on how far apart the bracketing addresses are.

    Smaller gaps → higher confidence (the estimate is more precise).
    """
    if gap <= 2:
        return 0.95
    if gap <= 6:
        return 0.85
    if gap <= 10:
        return 0.75
    if gap <= 20:
        return 0.65
    if gap <= 50:
        return 0.5
    return 0.3


def _lerp(lat1: float, lon1: float, lat2: float, lon2: float, t: float) -> tuple[float, float]:
    """Linear interpolation between two points.  ``t`` is in [0, 1]."""
    return (
        lat1 + t * (lat2 - lat1),
        lon1 + t * (lon2 - lon1),
    )


def _project_onto_line(
    lat: float, lon: float, line_coords: list[list[float]]
) -> tuple[float, float, float]:
    """Project a point onto a polyline.

    Returns ``(proj_lat, proj_lon, fraction)`` where fraction is the
    normalised distance [0, 1] along the polyline.
    """
    best_dist_sq = float("inf")
    best_proj = (lat, lon)
    best_frac = 0.0
    total_len = 0.0
    seg_starts: list[float] = [0.0]

    # Pre-compute cumulative segment lengths
    for i in range(len(line_coords) - 1):
        x1, y1 = line_coords[i]      # lon, lat (GeoJSON order)
        x2, y2 = line_coords[i + 1]
        seg_len = ((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5
        total_len += seg_len
        seg_starts.append(total_len)

    if total_len == 0:
        return lat, lon, 0.0

    for i in range(len(line_coords) - 1):
        x1, y1 = line_coords[i]
        x2, y2 = line_coords[i + 1]
        # Project (lon, lat) onto segment (x1,y1)-(x2,y2)
        dx, dy = x2 - x1, y2 - y1
        seg_len_sq = dx * dx + dy * dy
        if seg_len_sq == 0:
            t = 0.0
        else:
            t = max(0.0, min(1.0, ((lon - x1) * dx + (lat - y1) * dy) / seg_len_sq))
        px, py = x1 + t * dx, y1 + t * dy
        dist_sq = (lon - px) ** 2 + (lat - py) ** 2
        if dist_sq < best_dist_sq:
            best_dist_sq = dist_sq
            best_proj = (py, px)  # (lat, lon)
            seg_len = ((dx ** 2 + dy ** 2) ** 0.5)
            best_frac = (seg_starts[i] + t * seg_len) / total_len

    return best_proj[0], best_proj[1], best_frac


async def interpolate_address(
    pg_pool,
    requested_hn: int,
    street: str,
    city: str | None = None,
    street_names: list[str] | None = None,
    near: tuple[float, float] | None = None,
    radius_m: float = 3000.0,
) -> InterpolatedAddress | None:
    """Interpolate an address position from known addresses on the same street.

    Parameters
    ----------
    pg_pool : asyncpg.Pool
    requested_hn : int
        The housenumber to estimate.
    street : str
        The street name from the query (used as a fallback name).
    city : str or None
        Optional city filter for disambiguation (only used in the name-only path).
    street_names : list[str] or None
        Candidate street-name strings the data may store for this street, as
        resolved cross-lingually from ES (e.g. English "Tahrir Street" →
        ``["Tahrir Street", "شارع التحرير", "Al Tahrir Street"]``).  Addresses are
        matched by exact (case-insensitive) ``street`` against this list, so an
        English query reaches Arabic-tagged address points.
    near : (lat, lon) or None
        The query's reference point.  When given, addresses are restricted to
        ``radius_m`` around it, disambiguating between same-named streets in
        different parts of the city (there are many ``شارع التحرير`` in Cairo).
    radius_m : float
        Proximity radius in metres for ``near`` (default 3 km).

    Returns
    -------
    InterpolatedAddress or None
        The interpolated result, or None if interpolation is not possible.
    """
    names = [n for n in (street_names or ([street] if street else [])) if n]
    if not names:
        return None

    # ── Step 1: Fetch known addresses on this street ──────────────────────
    # Match by exact street name (cross-lingual via the resolved name list) and,
    # when a reference point is given, restrict to a radius around it so a common
    # street name doesn't pull addresses from a same-named street across the city.
    rows = await _gather_addresses(pg_pool, names, near, radius_m, city)

    if not rows:
        return None

    # ── Step 2: Parse housenumbers, separate by parity ────────────────────
    parsed: list[tuple[int, dict]] = []
    for row in rows:
        hn_int = _parse_housenumber(row["housenumber"])
        if hn_int is not None:
            parsed.append((hn_int, dict(row)))

    if not parsed:
        return None

    # Check for exact match first
    for hn_int, row in parsed:
        if hn_int == requested_hn:
            return InterpolatedAddress(
                housenumber=str(requested_hn),
                street=row["street"],
                city=row.get("city", ""),
                postcode=row.get("postcode", ""),
                country=row.get("country", ""),
                lat=row["lat"],
                lon=row["lon"],
                match_type="exact",
                confidence=1.0,
                side=_parity(requested_hn),
                bracket_low=None,
                bracket_high=None,
            )

    # Separate by parity
    requested_side = _parity(requested_hn)
    same_side = sorted(
        [(hn, row) for hn, row in parsed if _parity(hn) == requested_side],
        key=lambda x: x[0],
    )

    # ── Step 3: Find bracketing addresses ─────────────────────────────────
    lower_addr = None  # closest address below requested_hn
    upper_addr = None  # closest address above requested_hn

    for hn_int, row in same_side:
        if hn_int < requested_hn:
            lower_addr = (hn_int, row)
        elif hn_int > requested_hn:
            upper_addr = (hn_int, row)
            break

    # If we can't bracket on the same side, try using all addresses
    if lower_addr is None or upper_addr is None:
        all_sorted = sorted(parsed, key=lambda x: x[0])
        if lower_addr is None:
            for hn_int, row in all_sorted:
                if hn_int < requested_hn:
                    lower_addr = (hn_int, row)
                else:
                    break
        if upper_addr is None:
            for hn_int, row in all_sorted:
                if hn_int > requested_hn:
                    upper_addr = (hn_int, row)
                    break

    if lower_addr is None and upper_addr is None:
        return None

    # ── Step 4: Interpolate ───────────────────────────────────────────────
    if lower_addr and upper_addr:
        # Both brackets available — linear interpolation
        low_hn, low_row = lower_addr
        high_hn, high_row = upper_addr
        gap = high_hn - low_hn
        if gap == 0:
            t = 0.5
        else:
            t = (requested_hn - low_hn) / gap
        est_lat, est_lon = _lerp(
            low_row["lat"], low_row["lon"],
            high_row["lat"], high_row["lon"],
            t,
        )
        confidence = _interpolation_confidence(gap)
    elif lower_addr:
        # Only lower bracket — extrapolate slightly
        low_hn, low_row = lower_addr
        est_lat, est_lon = low_row["lat"], low_row["lon"]
        confidence = 0.3
    else:
        # Only upper bracket — extrapolate slightly
        high_hn, high_row = upper_addr  # type: ignore[misc]
        est_lat, est_lon = high_row["lat"], high_row["lon"]
        confidence = 0.3

    # ── Step 5: Try to snap to street geometry ────────────────────────────
    street_geom = await _find_street_geometry(pg_pool, street, est_lat, est_lon)
    if street_geom and lower_addr and upper_addr:
        try:
            coords = street_geom["coordinates"]
            # Project both bracket addresses onto the street line
            _, _, frac_low = _project_onto_line(
                low_row["lat"], low_row["lon"], coords
            )
            _, _, frac_high = _project_onto_line(
                high_row["lat"], high_row["lon"], coords
            )
            # Interpolate the fraction along the street
            if gap > 0:
                frac_est = frac_low + (requested_hn - low_hn) / gap * (frac_high - frac_low)
            else:
                frac_est = (frac_low + frac_high) / 2

            # Walk the street line to find the point at frac_est
            snapped_lat, snapped_lon = _point_at_fraction(coords, frac_est)
            est_lat, est_lon = snapped_lat, snapped_lon
        except Exception as e:
            logger.debug("Street snap failed, using linear interpolation: %s", e)

    return InterpolatedAddress(
        housenumber=str(requested_hn),
        street=low_row["street"] if lower_addr else high_row["street"],
        city=(low_row if lower_addr else high_row).get("city", ""),
        postcode=(low_row if lower_addr else high_row).get("postcode", ""),
        country=(low_row if lower_addr else high_row).get("country", ""),
        lat=round(est_lat, 7),
        lon=round(est_lon, 7),
        match_type="interpolated",
        confidence=round(confidence, 2),
        side=requested_side,
        bracket_low=str(lower_addr[0]) if lower_addr else None,
        bracket_high=str(upper_addr[0]) if upper_addr else None,
    )


def _point_at_fraction(
    coords: list[list[float]], frac: float
) -> tuple[float, float]:
    """Return the (lat, lon) at a given normalised fraction along a polyline."""
    frac = max(0.0, min(1.0, frac))

    # Compute total length
    seg_lengths: list[float] = []
    for i in range(len(coords) - 1):
        x1, y1 = coords[i]
        x2, y2 = coords[i + 1]
        seg_lengths.append(((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5)

    total = sum(seg_lengths)
    if total == 0:
        return coords[0][1], coords[0][0]  # lat, lon

    target = frac * total
    cumulative = 0.0
    for i, seg_len in enumerate(seg_lengths):
        if cumulative + seg_len >= target:
            # Point is on this segment
            if seg_len == 0:
                t = 0.0
            else:
                t = (target - cumulative) / seg_len
            x1, y1 = coords[i]
            x2, y2 = coords[i + 1]
            return (
                y1 + t * (y2 - y1),  # lat
                x1 + t * (x2 - x1),  # lon
            )
        cumulative += seg_len

    # Fallback to last point
    return coords[-1][1], coords[-1][0]


async def reverse_interpolate(
    pg_pool,
    lat: float,
    lon: float,
    street_name: str | None = None,
    radius_deg: float = 0.003,
) -> InterpolatedAddress | None:
    """Estimate the housenumber at a given (lat, lon) on the nearest street.

    Used by ``/reverse`` to provide an estimated address when the query
    point doesn't exactly match any known address.

    Algorithm:
    1. Find the two nearest addresses on the closest street.
    2. Project all three points (query + two addresses) onto the street line.
    3. Linearly interpolate the housenumber based on relative position.
    4. Round to the correct parity (odd/even side).
    """
    point_wkt = f"POINT({lon} {lat})"

    async with pg_pool.acquire() as conn:
        # Find nearest addresses on the same street (up to 20 for analysis)
        query = """
            SELECT housenumber, street, city, postcode, country,
                   ST_Y(geom) AS lat, ST_X(geom) AS lon,
                   ST_Distance(geom::geography,
                               ST_GeomFromText($1, 4326)::geography) AS distance_m
            FROM osm_addresses
            WHERE ST_DWithin(geom, ST_GeomFromText($1, 4326), $2)
        """
        params: list = [point_wkt, radius_deg]

        if street_name:
            query += " AND lower(street) = lower($3)"
            params.append(street_name)

        query += " ORDER BY geom <-> ST_GeomFromText($1, 4326) LIMIT 20"

        rows = await conn.fetch(query, *params)

    if len(rows) < 2:
        return None

    # Parse housenumbers
    addr_list: list[tuple[int, dict]] = []
    for row in rows:
        hn_int = _parse_housenumber(row["housenumber"])
        if hn_int is not None:
            addr_list.append((hn_int, dict(row)))

    if len(addr_list) < 2:
        return None

    # Use the two closest addresses
    a_hn, a_row = addr_list[0]
    b_hn, b_row = addr_list[1]

    if a_hn == b_hn:
        return None

    # Project all three points onto the street
    street_geom = await _find_street_geometry(
        pg_pool, a_row["street"], lat, lon
    )

    if street_geom:
        coords = street_geom["coordinates"]
        _, _, frac_a = _project_onto_line(a_row["lat"], a_row["lon"], coords)
        _, _, frac_b = _project_onto_line(b_row["lat"], b_row["lon"], coords)
        _, _, frac_q = _project_onto_line(lat, lon, coords)

        frac_range = frac_b - frac_a
        if abs(frac_range) > 1e-9:
            t = (frac_q - frac_a) / frac_range
            est_hn_raw = a_hn + t * (b_hn - a_hn)
        else:
            # Fractions too close; fall back to distance-based interpolation
            est_hn_raw = (a_hn + b_hn) / 2
    else:
        # No street geometry — interpolate based on distance ratio
        dist_a = ((lat - a_row["lat"]) ** 2 + (lon - a_row["lon"]) ** 2) ** 0.5
        dist_b = ((lat - b_row["lat"]) ** 2 + (lon - b_row["lon"]) ** 2) ** 0.5
        total = dist_a + dist_b
        if total < 1e-12:
            est_hn_raw = (a_hn + b_hn) / 2
        else:
            t = dist_a / total
            est_hn_raw = a_hn + t * (b_hn - a_hn)

    # Round to nearest integer with correct parity (odd/even side detection)
    est_hn = max(1, round(est_hn_raw))
    # Determine dominant parity on this side
    parities = [_parity(hn) for hn, _ in addr_list[:6]]
    dominant = max(set(parities), key=parities.count)
    if _parity(est_hn) != dominant:
        # Nudge to correct parity
        if est_hn_raw > est_hn:
            est_hn += 1
        else:
            est_hn = max(1, est_hn - 1)
        # Final check
        if _parity(est_hn) != dominant:
            est_hn += 1

    gap = abs(b_hn - a_hn)
    street = a_row.get("street", "")

    return InterpolatedAddress(
        housenumber=str(est_hn),
        street=street,
        city=a_row.get("city", ""),
        postcode=a_row.get("postcode", ""),
        country=a_row.get("country", ""),
        lat=round(lat, 7),
        lon=round(lon, 7),
        match_type="interpolated",
        confidence=round(_interpolation_confidence(gap) * 0.8, 2),  # slightly lower for reverse
        side=_parity(est_hn),
        bracket_low=str(min(a_hn, b_hn)),
        bracket_high=str(max(a_hn, b_hn)),
    )


async def _gather_addresses(
    pg_pool,
    street_names: list[str],
    near: tuple[float, float] | None,
    radius_m: float,
    city: str | None = None,
) -> list:
    """Fetch address points on a street, matched by exact (case-insensitive) name.

    ``street_names`` is the cross-lingual candidate list (query + ES-resolved
    names), so an English query reaches Arabic-tagged addresses.  When ``near``
    is given the result is restricted to ``radius_m`` around that point so a
    common street name (e.g. ``شارع التحرير``, which recurs across Cairo) only
    yields the cluster the user is actually near.  Without ``near`` it falls back
    to a name-only match, optionally narrowed by ``city``.
    """
    names = [n.lower() for n in street_names if n]
    if not names:
        return []

    params: list = [names]
    query = """
        SELECT housenumber, street, city, postcode, country,
               ST_Y(geom) AS lat, ST_X(geom) AS lon
        FROM osm_addresses
        WHERE lower(street) = ANY($1::text[])
    """
    if near is not None:
        lat, lon = near
        params.append(f"POINT({lon} {lat})")
        params.append(radius_m)
        query += (
            " AND ST_DWithin(geom::geography,"
            " ST_GeomFromText($2, 4326)::geography, $3)"
        )
    elif city:
        params.append(city)
        query += " AND lower(city) = lower($2)"

    try:
        async with pg_pool.acquire() as conn:
            await conn.execute("SET LOCAL statement_timeout = '3000'")
            return await conn.fetch(query, *params)
    except Exception as e:
        logger.debug("Address gather failed: %s", e)
        return []


async def _find_street_geometry(
    pg_pool, street: str, lat: float, lon: float
) -> dict | None:
    """Find the closest street LineString geometry matching the name.

    Searches ``osm_geometries`` joined with Elasticsearch-indexed name data.
    Falls back to a proximity-only search on LineStrings near the point.
    """
    point_wkt = f"POINT({lon} {lat})"

    async with pg_pool.acquire() as conn:
        # Find nearest LineString within ~500m of the estimated point
        row = await conn.fetchrow(
            """
            SELECT ST_AsGeoJSON(geom) as geom
            FROM osm_geometries
            WHERE ST_GeometryType(geom) = 'ST_LineString'
              AND ST_DWithin(geom, ST_GeomFromText($1, 4326), 0.005)
            ORDER BY geom <-> ST_GeomFromText($1, 4326)
            LIMIT 1
            """,
            point_wkt,
        )

    if row:
        return json.loads(row["geom"])
    return None
