"""Pluggable external live-traffic providers.

The crowdsourced probe API is the primary source of live speeds; an external
provider is an optional gap-filler / cold-start booster (roads with no probe
coverage). A provider just returns speed observations as ``{lat, lon, kph}``
points; the aggregator snaps each to a Valhalla edge and folds it into the same
Redis per-edge average (at a lower weight than fresh probes).

Selected via TRAFFIC_PROVIDER. Currently:
  - "none"   (default) — disabled
  - "tomtom"            — TomTom Flow Segment Data (free tier; needs TOMTOM_API_KEY)
"""

import httpx

from shared.config import (
    TOMTOM_API_KEY,
    TOMTOM_FLOW_URL,
    TRAFFIC_PROVIDER,
    TRAFFIC_PROVIDER_BBOX,
    TRAFFIC_PROVIDER_GRID,
)
from shared.logging import get_logger

logger = get_logger("traffic-providers")


def _parse_bbox(s: str) -> tuple[float, float, float, float]:
    parts = [float(x) for x in s.split(",")]
    if len(parts) != 4:
        raise ValueError(
            f"TRAFFIC_PROVIDER_BBOX must be 'min_lat,min_lon,max_lat,max_lon', got {s!r}"
        )
    return parts[0], parts[1], parts[2], parts[3]


def _grid_points(bbox: str, n: int) -> list[tuple[float, float]]:
    """N×N evenly-spaced (lat, lon) sample points across the bbox."""
    min_lat, min_lon, max_lat, max_lon = _parse_bbox(bbox)
    n = max(1, n)
    pts: list[tuple[float, float]] = []
    for i in range(n):
        # +0.5 keeps samples off the exact edges of the bbox.
        flat = (i + 0.5) / n
        lat = min_lat + (max_lat - min_lat) * flat
        for j in range(n):
            flon = (j + 0.5) / n
            lon = min_lon + (max_lon - min_lon) * flon
            pts.append((lat, lon))
    return pts


class TrafficProvider:
    """Base interface: return a list of {lat, lon, kph} observations."""

    name = "none"

    async def fetch(self, client: httpx.AsyncClient) -> list[dict]:
        return []


class TomTomFlowProvider(TrafficProvider):
    """TomTom Flow Segment Data — one call per grid point returns the current
    speed of the road segment nearest that point.

    Free tier is rate-limited, so the grid is deliberately coarse
    (TRAFFIC_PROVIDER_GRID). We report the *query* point as the observation
    location and let the aggregator snap it to the matching Valhalla edge.
    """

    name = "tomtom"

    def __init__(self, bbox: str, grid: int, api_key: str, url: str):
        self.points = _grid_points(bbox, grid)
        self.api_key = api_key
        self.url = url

    async def fetch(self, client: httpx.AsyncClient) -> list[dict]:
        out: list[dict] = []
        for lat, lon in self.points:
            try:
                resp = await client.get(
                    self.url,
                    params={"point": f"{lat},{lon}", "unit": "KMPH", "key": self.api_key},
                )
                if resp.status_code != 200:
                    continue
                seg = resp.json().get("flowSegmentData")
                if not seg:
                    continue
                kph = seg.get("currentSpeed")
                if kph is None:
                    continue
                out.append({"lat": lat, "lon": lon, "kph": float(kph)})
            except Exception:
                # One bad sample shouldn't abort the whole poll.
                continue
        return out


def get_provider() -> TrafficProvider | None:
    """Build the configured provider, or None when disabled / misconfigured."""
    if TRAFFIC_PROVIDER == "tomtom":
        if not TOMTOM_API_KEY:
            logger.info(
                "[traffic-aggregator] TRAFFIC_PROVIDER=tomtom but TOMTOM_API_KEY is unset — provider disabled",
            )
            return None
        return TomTomFlowProvider(
            TRAFFIC_PROVIDER_BBOX, TRAFFIC_PROVIDER_GRID, TOMTOM_API_KEY, TOMTOM_FLOW_URL
        )
    return None
