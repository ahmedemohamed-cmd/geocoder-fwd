"""traffic-aggregator — turn GPS probes (and optional provider data) into per-edge speeds.

Pipeline position: it sits between the probe firehose and the traffic-writer.

  NATS traffic.probes ──▶ map-match via Valhalla /trace_attributes ──▶ edge GraphIds
  external provider   ──▶ snap via Valhalla /locate                ──▶ edge GraphIds
                                          │
                                          ▼
                         EWMA per-edge speed in Redis  (tf:e:{graphid} + tf:idx zset)
                                          │
                                          ▼
                                   traffic_writer  ──▶ Valhalla traffic.tar

Redis schema (consumed by services/traffic_writer.py):
    tf:e:{graphid}  hash {kph, n, ts}     – smoothed speed + sample count + last update
    tf:idx          zset member=graphid score=last_update_epoch  (only once n >= MIN_SAMPLES)
"""

import asyncio
import json
import math
import time

import nats.errors
import redis.asyncio as aioredis
import httpx

from shared.config import (
    REDIS_HOST,
    REDIS_PORT,
    VALHALLA_URL,
    TRAFFIC_EWMA_ALPHA,
    TRAFFIC_EDGE_TTL,
    TRAFFIC_MIN_SAMPLES,
    TRAFFIC_MAX_TRACE,
    TRAFFIC_PROVIDER,
    TRAFFIC_PROVIDER_INTERVAL,
    TRAFFIC_PROVIDER_WEIGHT,
)
from shared.nats_client import (
    connect_traffic,
    subscribe_traffic,
    reconnect,
    is_connection_error,
    is_transient_error,
    TRAFFIC_STREAM_CFG,
)
from shared.traffic_providers import get_provider

_INDEX_KEY = "tf:idx"


def _log(msg: str) -> None:
    print(f"[traffic-aggregator] {msg}", flush=True)


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres."""
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


# ── Redis update ─────────────────────────────────────────────────────────────
async def _update_edge(r: aioredis.Redis, gid: int, kph: float, weight: float, now: float) -> None:
    """Fold one observation into an edge's smoothed speed (EWMA).

    ``weight`` scales the smoothing factor so low-confidence sources (external
    provider) move the estimate less than fresh probes. The edge only becomes
    visible to the writer (added to tf:idx) once it has >= MIN_SAMPLES.
    """
    if kph <= 0 or kph > 300:
        return  # implausible — drop
    key = f"tf:e:{gid}"
    h = await r.hgetall(key)
    if h and "kph" in h:
        prev = float(h["kph"])
        n = int(float(h.get("n", 0)))
        alpha = min(1.0, TRAFFIC_EWMA_ALPHA * weight)
        new_kph = (1 - alpha) * prev + alpha * kph
    else:
        new_kph = kph
        n = 0
    n += 1

    pipe = r.pipeline()
    pipe.hset(key, mapping={"kph": f"{new_kph:.2f}", "n": n, "ts": f"{now:.0f}"})
    pipe.expire(key, TRAFFIC_EDGE_TTL * 2)  # GC safety net; writer drives real expiry via zset
    if n >= TRAFFIC_MIN_SAMPLES:
        pipe.zadd(_INDEX_KEY, {str(gid): now})
    await pipe.execute()


# ── Valhalla map-matching ─────────────────────────────────────────────────────
async def _trace_attributes(client: httpx.AsyncClient, points: list[dict]) -> dict | None:
    shape = []
    for p in points:
        item = {"lat": p["lat"], "lon": p["lon"]}
        if p.get("ts") is not None:
            item["time"] = p["ts"]
        shape.append(item)
    body = {"shape": shape, "costing": "auto", "shape_match": "map_snap"}
    try:
        resp = await client.post(f"{VALHALLA_URL}/trace_attributes", json=body)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception as e:
        _log(f"trace_attributes failed: {e}")
        return None


async def _locate(client: httpx.AsyncClient, locations: list[dict]) -> list | None:
    body = {"locations": locations, "costing": "auto", "verbose": True}
    try:
        resp = await client.post(f"{VALHALLA_URL}/locate", json=body)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception as e:
        _log(f"locate failed: {e}")
        return None


def _edge_speeds_from_match(points: list[dict], match: dict) -> dict[int, float]:
    """Compute one observed speed (kph) per matched Valhalla edge.

    Primary: average the per-point GPS speeds grouped by their matched edge.
    Fallback (no per-point speed): derive a single trace speed from distance/time
    and assign it to every matched edge.
    """
    edges = match.get("edges", []) or []
    mpts = match.get("matched_points", []) or []
    sums: dict[int, list[float]] = {}  # gid -> [sum_kph, count]

    have_point_speed = any(p.get("speed") is not None for p in points)
    if have_point_speed:
        for i, mp in enumerate(mpts):
            if i >= len(points):
                break
            ei = mp.get("edge_index")
            if ei is None or ei < 0 or ei >= len(edges):
                continue
            gid = edges[ei].get("id")
            spd = points[i].get("speed")
            if gid is None or spd is None:
                continue
            acc = sums.setdefault(int(gid), [0.0, 0])
            acc[0] += spd * 3.6
            acc[1] += 1
        if sums:
            return {gid: s / c for gid, (s, c) in sums.items() if c}

    # Fallback: distance/time over the whole trace, assigned to all matched edges.
    dist = 0.0
    dt = 0.0
    for a, b in zip(points, points[1:]):
        dist += _haversine_m(a["lat"], a["lon"], b["lat"], b["lon"])
        if a.get("ts") is not None and b.get("ts") is not None:
            dt += max(0.0, float(b["ts"]) - float(a["ts"]))
    if dt <= 0 or dist <= 0:
        return {}
    trace_kph = (dist / dt) * 3.6
    gids = set()
    for ei_edge in edges:
        gid = ei_edge.get("id")
        if gid is not None:
            gids.add(int(gid))
    return {gid: trace_kph for gid in gids}


# ── Probe consumer ─────────────────────────────────────────────────────────────
async def _process_batch(r: aioredis.Redis, client: httpx.AsyncClient, batch: dict) -> int:
    """Map-match one probe batch and update Redis. Returns edges updated."""
    points = batch.get("points") or []
    if not points:
        return 0
    if len(points) > TRAFFIC_MAX_TRACE:
        points = points[-TRAFFIC_MAX_TRACE:]  # cap map-match cost; keep the most recent

    now = time.time()
    edge_kph: dict[int, float] = {}

    if len(points) >= 2:
        match = await _trace_attributes(client, points)
        if match:
            edge_kph = _edge_speeds_from_match(points, match)

    # Single ping, or map-match produced nothing usable: snap the last point.
    if not edge_kph:
        p = points[-1]
        if p.get("speed") is not None:
            located = await _locate(client, [{"lat": p["lat"], "lon": p["lon"]}])
            edges = (located[0].get("edges") if located else None) or []
            if edges:
                gid = edges[0].get("edge_id", {}).get("value")
                if gid is not None:
                    edge_kph = {int(gid): p["speed"] * 3.6}

    for gid, kph in edge_kph.items():
        await _update_edge(r, gid, kph, weight=1.0, now=now)
    return len(edge_kph)


async def _probe_consumer(r: aioredis.Redis, client: httpx.AsyncClient):
    nc, js = await connect_traffic()
    sub = await subscribe_traffic(js, "traffic-aggregator")
    _log("Subscribed to traffic.probes, consuming probes ...")
    state = {"nc": nc, "js": js, "sub": sub}

    while True:
        try:
            msgs = await state["sub"].fetch(batch=100, timeout=5)
        except nats.errors.TimeoutError:
            continue
        except Exception as e:
            _log(f"fetch error: {e}")
            if is_connection_error(e):
                try:
                    state["nc"], state["js"] = await reconnect(state["nc"], state["js"], TRAFFIC_STREAM_CFG)
                    state["sub"] = await subscribe_traffic(state["js"], "traffic-aggregator")
                    _log("reconnected / resubscribed")
                except Exception as re:
                    _log(f"reconnect failed: {re}")
                    await asyncio.sleep(5)
            elif is_transient_error(e):
                await asyncio.sleep(2)
            else:
                await asyncio.sleep(1)
            continue

        total_edges = 0
        for msg in msgs:
            try:
                batch = json.loads(msg.data)
                total_edges += await _process_batch(r, client, batch)
            except Exception as e:
                _log(f"batch error (acking to avoid redelivery storm): {e}")
            finally:
                await msg.ack()
        if total_edges:
            _log(f"updated {total_edges} edges from {len(msgs)} probe batch(es)")


# ── External provider poller ───────────────────────────────────────────────────
async def _provider_poller(r: aioredis.Redis, client: httpx.AsyncClient):
    provider = get_provider()
    if provider is None:
        _log(f"No external provider active (TRAFFIC_PROVIDER={TRAFFIC_PROVIDER}).")
        return
    _log(f"External provider '{provider.name}' active; polling every {TRAFFIC_PROVIDER_INTERVAL}s")

    while True:
        try:
            obs = await provider.fetch(client)
            if obs:
                located = await _locate(client, [{"lat": o["lat"], "lon": o["lon"]} for o in obs])
                now = time.time()
                updated = 0
                if located:
                    for o, res in zip(obs, located):
                        edges = (res.get("edges") if isinstance(res, dict) else None) or []
                        if not edges:
                            continue
                        gid = edges[0].get("edge_id", {}).get("value")
                        if gid is None:
                            continue
                        await _update_edge(r, int(gid), o["kph"], weight=TRAFFIC_PROVIDER_WEIGHT, now=now)
                        updated += 1
                _log(f"provider '{provider.name}': {len(obs)} samples -> {updated} edges")
        except Exception as e:
            _log(f"provider poll error: {e}")
        await asyncio.sleep(TRAFFIC_PROVIDER_INTERVAL)


async def run():
    _log(f"Starting. valhalla={VALHALLA_URL} min_samples={TRAFFIC_MIN_SAMPLES} ewma_alpha={TRAFFIC_EWMA_ALPHA}")
    r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    # One shared HTTP client; generous timeout because map-matching long traces
    # is the slow path.
    async with httpx.AsyncClient(timeout=30) as client:
        await asyncio.gather(
            _probe_consumer(r, client),
            _provider_poller(r, client),
        )


if __name__ == "__main__":
    asyncio.run(run())
