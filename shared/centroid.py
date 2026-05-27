"""Centroid computation for GeoJSON geometries."""

import logging

logger = logging.getLogger(__name__)


def _avg_coords(pts: list) -> dict | None:
    """Average a list of [lon, lat] coordinate pairs. Returns None if empty."""
    if not pts:
        return None
    avg_lat = sum(c[1] for c in pts) / len(pts)
    avg_lon = sum(c[0] for c in pts) / len(pts)
    return {"lat": avg_lat, "lon": avg_lon}


def centroid_latlon(geom: dict) -> dict | None:
    """Return {"lat": ..., "lon": ...} centroid from a GeoJSON geometry, or None.

    Used by ES inserter (expects dict with lat/lon keys).
    Gracefully handles malformed GeoJSON by returning None.
    """
    if not geom or not isinstance(geom, dict):
        return None

    try:
        gtype = geom.get("type")
        coords = geom.get("coordinates")
        if not gtype or coords is None:
            return None

        if gtype == "Point":
            if not coords or len(coords) < 2:
                return None
            return {"lat": coords[1], "lon": coords[0]}

        if gtype == "LineString":
            if not coords:
                return None
            return _avg_coords(coords)

        if gtype == "Polygon":
            if not coords or not coords[0]:
                return None
            return _avg_coords(coords[0])

        if gtype == "MultiPolygon":
            if not coords:
                return None
            all_pts = []
            for polygon in coords:
                if polygon and polygon[0]:
                    all_pts.extend(polygon[0])
            return _avg_coords(all_pts)

        return None
    except (KeyError, IndexError, TypeError) as e:
        logger.debug("centroid_latlon failed for geom type=%s: %s", geom.get("type"), e)
        return None


def centroid_list(geom: dict) -> list[float] | None:
    """Return [lat, lon] centroid from a GeoJSON geometry, or None.

    Convenience wrapper around centroid_latlon for consumers that need a list.
    """
    result = centroid_latlon(geom)
    if result is None:
        return None
    return [result["lat"], result["lon"]]
