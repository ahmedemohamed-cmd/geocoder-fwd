"""traffic-aggregator — turn GPS probes (and optional provider data) into per-edge speeds.

Pipeline position: it sits between the probe firehose and the traffic-writer.

  NATS traffic.probes ──▶ map-match via Valhalla /trace_attributes ──▶ edge GraphIds
  NATS traffic.cells  ──▶ provider fetch + snap via /locate        ──▶ edge GraphIds
  (cells enqueued by a leaderless scheduler: every replica tries a Redis
   SET NX per cell per poll window; exactly one wins — no duplicate polling)
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

import httpx
import nats.errors
import redis.asyncio as aioredis

from shared.config import (
    REDIS_HOST,
    REDIS_PORT,
    TRAFFIC_CELL_WORKERS,
    TRAFFIC_CELLS_SUBJECT,
    TRAFFIC_EDGE_TTL,
    TRAFFIC_EWMA_ALPHA,
    TRAFFIC_MAX_TRACE,
    TRAFFIC_MIN_SAMPLES,
    TRAFFIC_PROVIDER,
    TRAFFIC_PROVIDER_INTERVAL,
    TRAFFIC_PROVIDER_WEIGHT,
    VALHALLA_URL,
)
from shared.logging import get_logger
from shared.nats_client import (
    TRAFFIC_CELLS_STREAM_CFG,
    TRAFFIC_STREAM_CFG,
    connect_cells,
    connect_traffic,
    is_connection_error,
    is_transient_error,
    reconnect,
    subscribe_cells,
    subscribe_traffic,
)
from shared.redis_client import make_redis_async
from shared.traffic_providers import get_provider

logger = get_logger("traffic-aggregator")

_INDEX_KEY = "tf:idx"

# Atomic EWMA fold. The read-modify-write (read kph/n, blend, increment, write)
# MUST run server-side under a single EVAL so that concurrent writers — multiple
# aggregator replicas, or the probe consumer racing the provider poller — cannot
# lose updates on a hot edge. A client-side hgetall→compute→hset would race.
#
# The script deliberately touches ONLY the edge hash (one key, one hash slot, so
# it also runs on Redis Cluster). The tf:idx ZADD happens as a separate call in
# _update_edge — it's an idempotent index insert, not part of the racy fold: if
# it is lost to a crash the next probe on that edge simply re-adds it.
#
# KEYS[1] = tf:e:{gid}
# ARGV: alpha, kph, now, ttl
_EWMA_LUA = """
local prev = redis.call('HGET', KEYS[1], 'kph')
local n = tonumber(redis.call('HGET', KEYS[1], 'n')) or 0
local alpha = tonumber(ARGV[1])
local kph = tonumber(ARGV[2])
local new_kph
if prev then
  new_kph = (1 - alpha) * tonumber(prev) + alpha * kph
else
  new_kph = kph
end
n = n + 1
redis.call('HSET', KEYS[1], 'kph', string.format('%.2f', new_kph), 'n', n, 'ts', ARGV[3])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
return n
"""


def _log(msg: str) -> None:
    logger.info(f"[traffic-aggregator] {msg}")


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
    alpha = min(1.0, TRAFFIC_EWMA_ALPHA * weight)
    # Atomic server-side fold — see _EWMA_LUA. Safe under concurrent writers.
    n = await r.eval(
        _EWMA_LUA,
        1,  # numkeys — single key so the script is cluster-slot-safe
        key,
        f"{alpha}",
        f"{kph}",
        f"{now:.0f}",
        f"{TRAFFIC_EDGE_TTL * 2}",  # GC safety net; writer drives real expiry via zset
    )
    if int(n) >= TRAFFIC_MIN_SAMPLES:
        # Publish the edge to the writer's index (idempotent; separate from the
        # atomic fold so the script stays single-key for Redis Cluster).
        await r.zadd(_INDEX_KEY, {str(gid): now})


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
    for a, b in zip(points, points[1:], strict=False):
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
        except (TimeoutError, nats.errors.TimeoutError):
            # nats-py's fetch() raises a BARE asyncio.TimeoutError (str == '')
            # from its internal wait_for on some no-message paths, not only the
            # nats-specific TimeoutError. Both just mean "no probes this window".
            continue
        except Exception as e:
            _log(f"fetch error: {type(e).__name__}: {e}")
            if is_connection_error(e):
                try:
                    state["nc"], state["js"] = await reconnect(
                        state["nc"], state["js"], TRAFFIC_STREAM_CFG
                    )
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


# ── External provider polling, distributed across replicas ────────────────────
#
# The old sequential grid poller was a singleton (replicas would duplicate every
# provider call). It is split into two leaderless halves:
#
#   scheduler (every replica) ──SET NX──▶ Redis dedupe ──▶ publish traffic.cells
#   workers   (every replica) ◀──────── WORKQUEUE stream (each cell to ONE worker)
#
# Every replica runs the scheduler each poll window and attempts a Redis
# `SET tfc:sched:{cell}:{window} NX` per cell — exactly one replica wins each
# cell and enqueues it, so there is no leader and no duplicate polling. Workers
# on all replicas share one durable consumer on the WORKQUEUE stream, giving
# linear scaling of the actual provider fetch + map-snap work.

_SCHED_PREFIX = "tfc:sched:"


def _cell_msg(cell: int, lat: float, lon: float, window: int) -> bytes:
    return json.dumps({"cell": cell, "lat": lat, "lon": lon, "window": window}).encode()


async def _schedule_window(r: aioredis.Redis, js, points, window: int, interval: int) -> int:
    """Enqueue this window's cells, deduped across replicas via Redis SET NX.

    One winner per cell per window; the NX key's TTL covers the window so it
    garbage-collects itself. Returns the number of cells this caller enqueued."""
    enqueued = 0
    for cell, (lat, lon) in enumerate(points):
        won = await r.set(f"{_SCHED_PREFIX}{cell}:{window}", "1", nx=True, ex=interval)
        if not won:
            continue
        await js.publish(TRAFFIC_CELLS_SUBJECT, _cell_msg(cell, lat, lon, window))
        enqueued += 1
    return enqueued


async def _cell_scheduler(r: aioredis.Redis):
    provider = get_provider()
    if provider is None:
        return
    interval = max(1, TRAFFIC_PROVIDER_INTERVAL)
    _log(
        f"cell scheduler: {len(provider.points)} cells every {interval}s "
        f"(leaderless, Redis NX dedupe)"
    )
    nc, js = await connect_cells()
    state = {"nc": nc, "js": js}
    while True:
        window = int(time.time() // interval)
        try:
            enqueued = await _schedule_window(r, state["js"], provider.points, window, interval)
            if enqueued:
                _log(f"cell scheduler: enqueued {enqueued}/{len(provider.points)} cells")
        except Exception as e:
            _log(f"cell scheduler error: {e}")
            if is_connection_error(e):
                try:
                    state["nc"], state["js"] = await reconnect(
                        state["nc"], state["js"], TRAFFIC_CELLS_STREAM_CFG
                    )
                except Exception as re:
                    _log(f"cell scheduler reconnect failed: {re}")
        # Sleep to the next window boundary so all replicas agree on windows.
        await asyncio.sleep(max(1.0, (window + 1) * interval - time.time()))


async def _cell_worker(r: aioredis.Redis, client: httpx.AsyncClient, worker_id: int):
    provider = get_provider()
    if provider is None:
        return
    interval = max(1, TRAFFIC_PROVIDER_INTERVAL)
    nc, js = await connect_cells()
    sub = await subscribe_cells(js, "traffic-cell-worker")
    state = {"nc": nc, "js": js, "sub": sub}
    _log(f"cell worker {worker_id}: consuming {TRAFFIC_CELLS_SUBJECT}")

    while True:
        try:
            msgs = await state["sub"].fetch(batch=10, timeout=5)
        except (TimeoutError, nats.errors.TimeoutError):
            continue
        except Exception as e:
            _log(f"cell worker {worker_id} fetch error: {type(e).__name__}: {e}")
            if is_connection_error(e):
                try:
                    state["nc"], state["js"] = await reconnect(
                        state["nc"], state["js"], TRAFFIC_CELLS_STREAM_CFG
                    )
                    state["sub"] = await subscribe_cells(state["js"], "traffic-cell-worker")
                except Exception as re:
                    _log(f"cell worker {worker_id} reconnect failed: {re}")
                    await asyncio.sleep(5)
            else:
                await asyncio.sleep(2)
            continue

        for msg in msgs:
            try:
                cell = json.loads(msg.data)
                # Stale cell (worker backlog / redelivery after an outage):
                # the speed is superseded by a newer window — drop it.
                if time.time() - cell["window"] * interval > 2 * interval:
                    continue
                obs = await provider.fetch_cell(client, cell["lat"], cell["lon"])
                if obs is not None:
                    located = await _locate(client, [{"lat": obs["lat"], "lon": obs["lon"]}])
                    edges = (located[0].get("edges") if located else None) or []
                    gid = edges[0].get("edge_id", {}).get("value") if edges else None
                    if gid is not None:
                        await _update_edge(
                            r, int(gid), obs["kph"], weight=TRAFFIC_PROVIDER_WEIGHT, now=time.time()
                        )
            except Exception as e:
                _log(f"cell worker {worker_id} error (acking): {e}")
            finally:
                await msg.ack()


async def run():
    _log(
        f"Starting. valhalla={VALHALLA_URL} min_samples={TRAFFIC_MIN_SAMPLES} ewma_alpha={TRAFFIC_EWMA_ALPHA}"
    )
    r = make_redis_async(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    # One shared HTTP client; generous timeout because map-matching long traces
    # is the slow path.
    async with httpx.AsyncClient(timeout=30) as client:
        tasks = [_probe_consumer(r, client)]
        if get_provider() is None:
            _log(f"No external provider active (TRAFFIC_PROVIDER={TRAFFIC_PROVIDER}).")
        else:
            tasks.append(_cell_scheduler(r))
            tasks.extend(_cell_worker(r, client, i) for i in range(max(1, TRAFFIC_CELL_WORKERS)))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(run())
