"""Valhalla routing proxy with Arabic narration support.

Valhalla 3.5.1 has no ar.json locale — ``language: ar`` silently falls back to
en-US.  This module proxies requests to Valhalla and, when the caller requests
Arabic (any ``ar*`` BCP-47 tag), rewrites every maneuver's instruction fields
using structured Arabic templates driven by the maneuver ``type`` and the
``street_names`` list (which the OSM data already provides in Arabic script).
"""

from __future__ import annotations

import asyncio
import re
import time
from datetime import UTC, datetime
from typing import Any

import httpx
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from shared.config import (
    REDIS_HOST,
    REDIS_PORT,
    TOMTOM_API_KEY,
    TRAFFIC_FETCH_DAILY_BUDGET,
    TRAFFIC_FETCH_MAX_CALLS,
    TRAFFIC_FETCH_NEGATIVE_TTL,
    TRAFFIC_FETCH_ON_DEMAND,
    TRAFFIC_FETCH_TTL,
    TRAFFIC_FETCH_WINDOW_KM,
    VALHALLA_URL,
)
from shared.logging import get_logger
from shared.redis_client import make_redis_async
from shared.traffic_providers import tomtom_point_speed

logger = get_logger("routing")

# ── Arabic direction words ────────────────────────────────────────────────────
_DIRECTIONS: dict[str, str] = {
    "north": "شمالاً",
    "south": "جنوباً",
    "east": "شرقاً",
    "west": "غرباً",
    "northeast": "شمال شرقاً",
    "northwest": "شمال غرباً",
    "southeast": "جنوب شرقاً",
    "southwest": "جنوب غرباً",
}

# Arabic ordinals for roundabout exit counts 1-10
_ORDINALS: dict[int, str] = {
    1: "الأول",
    2: "الثاني",
    3: "الثالث",
    4: "الرابع",
    5: "الخامس",
    6: "السادس",
    7: "السابع",
    8: "الثامن",
    9: "التاسع",
    10: "العاشر",
}

_ONTO_RE = re.compile(r"\bonto (.+?)\.$", re.IGNORECASE)
_TOWARD_RE = re.compile(r"\btoward (.+?)\.$", re.IGNORECASE)


def _street(names: list[str]) -> str:
    return names[0] if names else ""


def _dist(length_km: float, units: str) -> str:
    if units == "miles":
        yards = round(length_km * 1760)
        return f"{yards} ياردة" if yards < 300 else f"{length_km * 0.621:.1f} ميل"
    m = round(length_km * 1000)
    return f"{m} متراً" if m < 1000 else f"{length_km:.1f} كيلومتراً"


def _direction_from(instruction: str) -> str:
    lower = instruction.lower()
    for eng, ar in _DIRECTIONS.items():
        if f" {eng} " in lower or lower.endswith(f" {eng}.") or f" {eng}\n" in lower:
            return ar
    return ""


def _onto(instruction: str) -> str:
    m = _ONTO_RE.search(instruction)
    return m.group(1) if m else ""


def _toward(instruction: str) -> str:
    m = _TOWARD_RE.search(instruction)
    return m.group(1) if m else ""


def _translate_maneuver(maneuver: dict, units: str) -> dict:
    typ = maneuver.get("type", 0)
    streets = maneuver.get("street_names", [])
    s = _street(streets)
    raw = maneuver.get("instruction", "")
    length = maneuver.get("length", 0.0)
    n = maneuver.get("roundabout_exit_count", 1)
    ordinal = _ORDINALS.get(n, str(n))
    dist_str = _dist(length, units)
    direction = _direction_from(raw)

    def _onto_s() -> str:
        return _onto(raw) or s

    def _toward_s() -> str:
        return _toward(raw) or s

    ar: str
    if typ in (1, 2, 3):  # Start (various directions)
        ar = f"اتجه {direction} على {s}." if s else f"اتجه {direction}."
    elif typ == 4:  # Destination (arrived)
        ar = "لقد وصلت إلى وجهتك."
    elif typ == 5:  # Destination right
        ar = "وجهتك على اليمين."
    elif typ == 6:  # Destination left
        ar = "وجهتك على اليسار."
    elif typ in (7, 8):  # Becomes / Continue
        ar = f"استمر على {s}." if s else "استمر مباشرة."
    elif typ == 9:  # Slight right
        ar = f"انحرف يميناً نحو {_onto_s()}." if _onto_s() else "انحرف قليلاً يميناً."
    elif typ == 10:  # Right
        ar = f"انعطف يميناً نحو {_onto_s()}." if _onto_s() else "انعطف يميناً."
    elif typ == 11:  # Sharp right
        ar = f"انعطف يميناً حاداً نحو {_onto_s()}." if _onto_s() else "انعطف يميناً حاداً."
    elif typ in (12, 13):  # U-turn
        ar = "استدر."
    elif typ == 14:  # Sharp left
        ar = f"انعطف يساراً حاداً نحو {_onto_s()}." if _onto_s() else "انعطف يساراً حاداً."
    elif typ == 15:  # Left
        ar = f"انعطف يساراً نحو {_onto_s()}." if _onto_s() else "انعطف يساراً."
    elif typ == 16:  # Slight left
        ar = f"انحرف يساراً نحو {_onto_s()}." if _onto_s() else "انحرف قليلاً يساراً."
    elif typ == 17:  # Ramp straight
        ar = f"خذ المنحدر نحو {_onto_s()}." if _onto_s() else "خذ المنحدر."
    elif typ == 18:  # Ramp right
        ar = f"خذ المنحدر على اليمين نحو {_onto_s()}." if _onto_s() else "خذ المنحدر على اليمين."
    elif typ == 19:  # Ramp left
        ar = f"خذ المنحدر على اليسار نحو {_onto_s()}." if _onto_s() else "خذ المنحدر على اليسار."
    elif typ == 20:  # Exit right
        ar = f"اخرج من اليمين نحو {_onto_s()}." if _onto_s() else "اخرج من اليمين."
    elif typ == 21:  # Exit left
        ar = f"اخرج من اليسار نحو {_onto_s()}." if _onto_s() else "اخرج من اليسار."
    elif typ == 22:  # Stay straight
        ar = f"استمر مباشرة على {s}." if s else "استمر مباشرة."
    elif typ == 23:  # Stay right
        ar = (
            f"ابقَ على اليمين نحو {_onto_s() or _toward_s()}."
            if (_onto_s() or _toward_s())
            else "ابقَ على اليمين."
        )
    elif typ == 24:  # Stay left
        ar = (
            f"ابقَ على اليسار نحو {_onto_s() or _toward_s()}."
            if (_onto_s() or _toward_s())
            else "ابقَ على اليسار."
        )
    elif typ == 25:  # Merge
        ar = f"اندمج على {_onto_s()}." if _onto_s() else "اندمج."
    elif typ == 26:  # Roundabout enter
        exit_street = _onto(raw)
        roundabout_name = s
        if roundabout_name and exit_street:
            ar = f"ادخل {roundabout_name} وخذ المخرج {ordinal} نحو {exit_street}."
        elif exit_street:
            ar = f"ادخل الدوار وخذ المخرج {ordinal} نحو {exit_street}."
        else:
            ar = f"ادخل الدوار وخذ المخرج {ordinal}."
    elif typ == 27:  # Roundabout exit
        ar = f"اخرج من الدوار نحو {_onto_s()}." if _onto_s() else "اخرج من الدوار."
    elif typ == 28:  # Ferry enter
        ar = f"استقل العبارة نحو {_onto_s()}." if _onto_s() else "استقل العبارة."
    elif typ == 29:  # Ferry exit
        ar = "غادر العبارة."
    elif typ in (37, 38):  # Merge right/left
        side = "يميناً" if typ == 37 else "يساراً"
        ar = f"اندمج {side} على {_onto_s()}." if _onto_s() else f"اندمج {side}."
    else:
        return maneuver  # transit / unknown — leave as-is

    m = dict(maneuver)
    m["instruction"] = ar
    if "verbal_pre_transition_instruction" in m:
        m["verbal_pre_transition_instruction"] = ar
    if "verbal_transition_alert_instruction" in m:
        m["verbal_transition_alert_instruction"] = ar
    if "verbal_succinct_transition_instruction" in m:
        m["verbal_succinct_transition_instruction"] = ar.rstrip(".")
    if "verbal_post_transition_instruction" in m and length > 0:
        m["verbal_post_transition_instruction"] = f"استمر لمسافة {dist_str}."
    return m


def _translate_response(body: dict) -> dict:
    """Rewrite all maneuver instructions in a Valhalla trip response to Arabic."""
    trip = body.get("trip")
    if not trip:
        return body
    units = trip.get("units", "kilometers")
    for leg in trip.get("legs", []):
        leg["maneuvers"] = [_translate_maneuver(m, units) for m in leg.get("maneuvers", [])]
    trip["language"] = "ar"
    return body


def _wants_arabic(payload: dict) -> bool:
    lang = (payload.get("directions_options") or {}).get("language", "")
    return isinstance(lang, str) and lang.lower().startswith("ar")


def _strip_arabic_language(payload: dict) -> dict:
    """Remove the ar language tag so Valhalla doesn't fall back silently."""
    opts = payload.get("directions_options")
    if opts and opts.get("language", "").lower().startswith("ar"):
        opts = dict(opts)
        del opts["language"]
        payload = dict(payload, directions_options=opts)
    return payload


async def proxy(path: str, method: str, body: dict | None, timeout: float = 30.0) -> Any:
    """Forward a routing request to Valhalla, applying Arabic translation if needed."""
    translate = body is not None and _wants_arabic(body)
    if translate:
        body = _strip_arabic_language(body)

    url = VALHALLA_URL.rstrip("/") + "/" + path.lstrip("/")
    async with httpx.AsyncClient(timeout=timeout) as client:
        if method == "GET":
            resp = await client.get(url)
        else:
            resp = await client.post(url, json=body)

    result = resp.json()
    if translate and resp.status_code == 200:
        result = _translate_response(result)
    return resp.status_code, result


# ── HTTP router ───────────────────────────────────────────────────────────────
# Mounted by the geocoder app via ``app.include_router(router)``. These endpoints
# are a thin pass-through to Valhalla (with the Arabic narration rewrite above)
# and hold no geocoder state, so they live alongside the proxy they delegate to.
router = APIRouter(tags=["routing"])


@router.get("/status")
async def routing_status():
    """Valhalla engine status (version, tileset bbox, available actions)."""
    status_code, body = await proxy("/status", "GET", None)
    return JSONResponse(content=body, status_code=status_code)


@router.post("/route")
async def routing_route(request: Request, traffic: bool = Query(False)):
    """Turn-by-turn directions. Supports language=ar for Arabic narration.

    ``?traffic=true`` routes *using* live traffic, not just painting it: it
    injects ``date_time: {type: 0}`` so Valhalla costs each edge at its live
    speed from traffic.tar (fed by probes + the on-demand fetch), making the ETA
    and route choice traffic-aware wherever we have data — then annotates each
    leg with coloured ``traffic`` runs + a trip-level ``traffic_coverage``. A
    caller-supplied ``date_time`` is respected; plain ``/route`` is unchanged.
    """
    body = await request.json()
    if traffic and "date_time" not in body:
        # Valhalla only reads live speeds on its time-dependent cost path, which
        # date_time selects; type 0 = depart now. Edges with no live data fall
        # back to free-flow, so this is upside-only where we have coverage.
        body = {**body, "date_time": {"type": 0}}
    status_code, result = await proxy("/route", "POST", body)
    if traffic and status_code == 200 and isinstance(result, dict) and result.get("trip"):
        try:
            await _annotate_traffic(result["trip"], body.get("costing", "auto"))
        except Exception as e:  # annotation is best-effort; never break routing
            logger.warning("[routing] traffic annotation failed: %s", e)
    return JSONResponse(content=result, status_code=status_code)


@router.post("/optimized_route")
async def routing_optimized_route(request: Request):
    """Optimized route (TSP) — reorders waypoints for shortest tour."""
    body = await request.json()
    status_code, result = await proxy("/optimized_route", "POST", body)
    return JSONResponse(content=result, status_code=status_code)


@router.post("/sources_to_targets")
async def routing_sources_to_targets(request: Request):
    """Time/distance matrix (many sources to many targets)."""
    body = await request.json()
    status_code, result = await proxy("/sources_to_targets", "POST", body)
    return JSONResponse(content=result, status_code=status_code)


@router.post("/isochrone")
async def routing_isochrone(request: Request):
    """Reachability polygons at given time/distance contours."""
    body = await request.json()
    status_code, result = await proxy("/isochrone", "POST", body)
    return JSONResponse(content=result, status_code=status_code)


@router.post("/locate")
async def routing_locate(request: Request):
    """Snap coordinates to the routing graph (nearest edges/nodes)."""
    body = await request.json()
    status_code, result = await proxy("/locate", "POST", body)
    return JSONResponse(content=result, status_code=status_code)


# ── live-traffic annotation ───────────────────────────────────────────────────
# _annotate_traffic (used by /route?traffic=true) decomposes each leg into its
# Valhalla edges (trace_attributes edge_walk) and colours them by the live speed
# we keep per edge in Redis (tf:e:{graphid}, fed by probes + the on-demand
# provider fetch). Contiguous same-level edges merge into runs that index into
# the leg's own shape. Reads our Redis directly, so it works whether or not the
# caller passed date_time.
#
# Cache-first, demand-driven coverage: when a route crosses edges with no recent
# speed in Redis, we fetch them from TomTom on the spot (capped + budgeted),
# cache the results in the same tf:e:* keys, and colour immediately. Repeat
# routes over a warm corridor cost nothing. See _fill_missing_from_provider.

_traffic_redis = None


def _get_traffic_redis():
    global _traffic_redis
    if _traffic_redis is None:
        _traffic_redis = make_redis_async(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_connect_timeout=5
        )
    return _traffic_redis


def _decode_polyline6(encoded: str) -> list[list[float]]:
    """Decode a Valhalla polyline6 string to ``[[lon, lat], ...]``."""
    coords: list[list[float]] = []
    lat = lon = index = 0
    n = len(encoded)
    while index < n:
        for is_lon in (False, True):
            shift = result = 0
            while True:
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            delta = ~(result >> 1) if (result & 1) else (result >> 1)
            if is_lon:
                lon += delta
            else:
                lat += delta
        coords.append([lon / 1e6, lat / 1e6])
    return coords


# Congestion colouring by live speed as a fraction of free-flow speed.
_TRAFFIC_BANDS = ((0.75, "green"), (0.5, "yellow"), (0.25, "orange"))


def _classify(live_kph: float | None, freeflow_kph: float | None) -> tuple[str, float | None]:
    if live_kph is None or not freeflow_kph or freeflow_kph <= 0:
        return "unknown", None
    ratio = live_kph / freeflow_kph
    for lo, level in _TRAFFIC_BANDS:
        if ratio >= lo:
            return level, round(ratio, 3)
    return "red", round(ratio, 3)


async def _trace_edges(encoded_polyline: str, costing: str) -> list[dict]:
    """edge_walk a route leg's shape back to its Valhalla edges.

    edge_walk walks the exact input shape, so the edges' begin/end_shape_index
    index into that shape — i.e. into the leg's own ``shape``. (map_snap is NOT
    used as a fallback here: it can move/insert vertices, which would misalign
    the indices against the leg shape.) Returns [] if the walk yields nothing.
    """
    url = VALHALLA_URL.rstrip("/") + "/trace_attributes"
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                url,
                json={
                    "encoded_polyline": encoded_polyline,
                    "costing": costing or "auto",
                    "shape_match": "edge_walk",
                },
            )
        if resp.status_code == 200:
            return resp.json().get("edges") or []
    except Exception:
        pass
    return []


async def _fill_missing_from_provider(
    r, edges: list[dict], live: list[float | None], leg_shape: str, max_calls: int
) -> int:
    """Fetch live speeds from TomTom for a leg's uncovered edges, in place.

    Groups contiguous missing edges (that aren't negatively-marked) into windows
    of <= TRAFFIC_FETCH_WINDOW_KM, makes one TomTom call per window at its
    midpoint (capped by ``max_calls`` and the daily budget), writes hits into the
    shared tf:e:* / tf:idx schema (so the traffic-writer bakes them into
    traffic.tar too) and marks no-data windows. Updates ``live`` for hit edges
    and returns the number of provider calls made. Fully fail-open.
    """
    missing = [i for i, v in enumerate(live) if v is None]
    if not missing or max_calls <= 0:
        return 0

    # Skip edges the provider already told us it has no data for (negative cache).
    try:
        pipe = r.pipeline(transaction=False)
        for i in missing:
            pipe.exists(f"tf:nd:{edges[i].get('id')}")
        marked = await pipe.execute()
    except Exception:
        marked = [0] * len(missing)
    fetchable = {i for i, m in zip(missing, marked, strict=True) if not m}
    if not fetchable:
        return 0

    # Group contiguous fetchable edges into <= WINDOW_KM windows.
    windows: list[list[int]] = []
    cur: list[int] = []
    cur_km = 0.0
    for i in range(len(edges)):
        if i in fetchable:
            length = edges[i].get("length") or 0.0
            if cur and cur_km + length > TRAFFIC_FETCH_WINDOW_KM:
                windows.append(cur)
                cur, cur_km = [], 0.0
            cur.append(i)
            cur_km += length
        elif cur:
            windows.append(cur)
            cur, cur_km = [], 0.0
    if cur:
        windows.append(cur)
    if not windows:
        return 0

    # Daily budget guard (Redis counter, resets on the calendar day, UTC).
    bkey = f"tf:budget:{datetime.now(UTC).strftime('%Y-%m-%d')}"
    try:
        used = int(await r.get(bkey) or 0)
    except Exception:
        used = 0
    remaining = min(max_calls, TRAFFIC_FETCH_DAILY_BUDGET - used)
    if remaining <= 0:
        return 0
    windows = windows[:remaining]

    coords = _decode_polyline6(leg_shape)  # [[lon, lat], ...]

    def _sample(win: list[int]) -> tuple[float, float] | None:
        b = edges[win[0]].get("begin_shape_index")
        en = edges[win[-1]].get("end_shape_index")
        if b is None or en is None:
            return None
        mid = (b + en) // 2
        if 0 <= mid < len(coords):
            lon, lat = coords[mid]
            return lat, lon
        return None

    samples = [(w, c) for w in windows if (c := _sample(w)) is not None]
    if not samples:
        return 0

    async with httpx.AsyncClient(timeout=2.5) as client:
        results = await asyncio.gather(
            *[tomtom_point_speed(client, lat, lon) for _, (lat, lon) in samples],
            return_exceptions=True,
        )

    now = int(time.time())
    wpipe = r.pipeline(transaction=False)
    for (win, _c), speed in zip(samples, results, strict=True):
        if isinstance(speed, BaseException) or speed is None or speed <= 0:
            for i in win:  # negative marker: don't re-query this road for a while
                wpipe.setex(f"tf:nd:{edges[i].get('id')}", TRAFFIC_FETCH_NEGATIVE_TTL, 1)
            continue
        for i in win:  # apply the window's speed to every edge in it
            gid = edges[i].get("id")
            live[i] = speed
            key = f"tf:e:{gid}"
            wpipe.hset(key, mapping={"kph": f"{speed:.2f}", "n": 1, "ts": now})
            wpipe.expire(key, TRAFFIC_FETCH_TTL)
            wpipe.zadd("tf:idx", {str(gid): now})
    wpipe.incrby(bkey, len(samples))
    wpipe.expire(bkey, 172800)
    try:
        await wpipe.execute()
    except Exception:
        pass
    return len(samples)


async def _annotate_traffic(trip: dict, costing: str) -> None:
    """Attach live-traffic annotation to each leg of *trip*, in place.

    Sets ``leg["traffic"]`` to a list of coloured runs that index into
    ``leg["shape"]``::

        {"begin_shape_index", "end_shape_index", "level",
         "live_kph", "freeflow_kph", "ratio"}

    ``level`` is green|yellow|orange|red|unknown; ``live_kph``/``ratio`` are the
    run's bottleneck (slowest) edge. Also sets ``trip["traffic_coverage"]``
    (incl. ``provider_calls`` — TomTom calls made this request).
    """
    r = _get_traffic_redis()
    n_edges = n_live = provider_calls = 0
    fetch_budget = TRAFFIC_FETCH_MAX_CALLS  # per-request cap, shared across legs

    for leg in trip.get("legs", []):
        leg["traffic"] = []
        leg_shape = leg.get("shape")
        if not leg_shape:
            continue
        edges = await _trace_edges(leg_shape, costing)
        if not edges:
            continue

        # 1) Cache-first: one pipelined read of the live speed for every edge.
        pipe = r.pipeline(transaction=False)
        for e in edges:
            pipe.hget(f"tf:e:{e.get('id')}", "kph")
        try:
            live_raw = await pipe.execute()
        except Exception:
            live_raw = [None] * len(edges)
        live: list[float | None] = [float(x) if x is not None else None for x in live_raw]

        # 2) On miss, fetch the uncovered stretches from TomTom (capped/budgeted).
        if (
            TRAFFIC_FETCH_ON_DEMAND
            and TOMTOM_API_KEY
            and fetch_budget > 0
            and any(v is None for v in live)
        ):
            made = await _fill_missing_from_provider(r, edges, live, leg_shape, fetch_budget)
            provider_calls += made
            fetch_budget -= made

        # 3) Classify + merge contiguous same-level edges into runs.
        runs: list[dict] = []
        run: dict | None = None
        for e, live_v in zip(edges, live, strict=True):
            b, en = e.get("begin_shape_index"), e.get("end_shape_index")
            if b is None or en is None or en <= b:
                continue
            freeflow = e.get("speed")
            n_edges += 1
            if live_v is not None:
                n_live += 1
            level, ratio = _classify(live_v, freeflow)
            if run is not None and run["level"] == level:
                run["end_shape_index"] = en  # extend the contiguous run
                if live_v is not None and (run["live_kph"] is None or live_v < run["live_kph"]):
                    run["live_kph"], run["ratio"], run["freeflow_kph"] = live_v, ratio, freeflow
            else:
                run = {
                    "begin_shape_index": b,
                    "end_shape_index": en,
                    "level": level,
                    "live_kph": live_v,
                    "freeflow_kph": freeflow,
                    "ratio": ratio,
                }
                runs.append(run)
        leg["traffic"] = runs

    trip["traffic_coverage"] = {
        "edges": n_edges,
        "edges_with_live": n_live,
        "coverage_pct": round(100 * n_live / n_edges, 1) if n_edges else 0.0,
        "provider_calls": provider_calls,
    }
