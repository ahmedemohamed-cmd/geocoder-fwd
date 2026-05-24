"""Centroid computation for GeoJSON geometries."""


def centroid_latlon(geom: dict) -> dict | None:
    """Return {"lat": ..., "lon": ...} centroid from a GeoJSON geometry, or None.

    Used by ES inserter (expects dict with lat/lon keys).
    """
    if not geom:
        return None
    gtype = geom["type"]
    coords = geom["coordinates"]
    if gtype == "Point":
        return {"lat": coords[1], "lon": coords[0]}
    if gtype == "LineString":
        if not coords:
            return None
        avg_lat = sum(c[1] for c in coords) / len(coords)
        avg_lon = sum(c[0] for c in coords) / len(coords)
        return {"lat": avg_lat, "lon": avg_lon}
    if gtype == "Polygon":
        if not coords or not coords[0]:
            return None
        pts = coords[0]  # exterior ring
        avg_lat = sum(c[1] for c in pts) / len(pts)
        avg_lon = sum(c[0] for c in pts) / len(pts)
        return {"lat": avg_lat, "lon": avg_lon}
    if gtype == "MultiPolygon":
        if not coords:
            return None
        # Collect all points from all polygons
        all_pts = []
        for polygon in coords:
            if polygon and polygon[0]:
                all_pts.extend(polygon[0])
        if not all_pts:
            return None
        avg_lat = sum(c[1] for c in all_pts) / len(all_pts)
        avg_lon = sum(c[0] for c in all_pts) / len(all_pts)
        return {"lat": avg_lat, "lon": avg_lon}
    return None


def centroid_list(geom: dict) -> list[float] | None:
    """Return [lat, lon] centroid from a GeoJSON geometry, or None.

    Convenience wrapper around centroid_latlon for consumers that need a list.
    """
    result = centroid_latlon(geom)
    if result is None:
        return None
    return [result["lat"], result["lon"]]
