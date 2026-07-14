"""Redis-backed live metering + durable event buffer + aggregator.

Two planes of usage state:

* **Live counters** (Redis INCR) — authoritative for real-time display and for
  cluster-wide quota enforcement. Every gateway replica increments the *same*
  Redis keys, so the count is global regardless of which replica served the
  request. This is the analogue of APISIX's ``limit-count`` Redis policy.
* **Durable rollups** (Postgres) — billing source of truth. The gateway pushes a
  lightweight event onto a Redis list per request; the aggregator drains it in
  batches into ``usage_rollups``. In production this buffer is NATS/JetStream
  for at-least-once delivery; the list keeps the build self-contained.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, date, datetime
from typing import Any

from redis.exceptions import WatchError

from . import config


def now_parts(ts: datetime | None = None) -> tuple[str, str]:
    """Return (period 'YYYY-MM', day 'YYYY-MM-DD') in UTC."""
    ts = ts or datetime.now(UTC)
    return ts.strftime("%Y-%m"), ts.strftime("%Y-%m-%d")


def is_free_path(path: str) -> bool:
    """True for endpoints that are never billed and never counted against quota
    (see config.FREE_ENDPOINTS): liveness (/health, /status), capability discovery
    (/features), and contributory writes (/feedback, /insert, /places,
    /traffic/probe[s]). Everything that answers a user query bills. Matched on the
    FULL request path (query string ignored), so /traffic/probes is free while
    /traffic/edge bills."""
    return config.norm_path(path) in config.FREE_ENDPOINTS


# ── key cache (shared across replicas) ───────────────────────────────────────
async def cache_key(redis, key_hash: str, payload: dict[str, Any]) -> None:
    await redis.set(config.KEYCACHE_PREFIX + key_hash, json.dumps(payload), ex=config.KEYCACHE_TTL)


async def get_cached_key(redis, key_hash: str) -> dict | None:
    raw = await redis.get(config.KEYCACHE_PREFIX + key_hash)
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode()
    return json.loads(raw)


async def invalidate_key(redis, key_hash: str) -> None:
    await redis.delete(config.KEYCACHE_PREFIX + key_hash)


# ── live counters ────────────────────────────────────────────────────────────
def _tkey(tenant_id: str, period: str) -> str:
    return f"{config.LIVE_TENANT_PREFIX}{tenant_id}:{period}"


def _kkey(key_id: str, period: str) -> str:
    return f"{config.LIVE_KEY_PREFIX}{key_id}:{period}"


async def incr_tenant(redis, tenant_id: str, period: str, milli: int = 1000) -> int:
    return int(await redis.incrby(_tkey(tenant_id, period), milli))


async def incr_tenant_if_allowed(
    redis, tenant_id: str, period: str, cap_milli: int, milli: int = 1000
) -> int | None:
    """Atomically add ``milli`` (the request's weight in milli-credits) to the
    tenant counter unless the result would exceed ``cap_milli``.

    Returns the new count if allowed, or ``None`` when the increment would break
    the cap (``cap_milli <= 0`` means unlimited). Note the check is per-request:
    an expensive (high-weight) request can be rejected while a cheaper one would
    still fit in the remaining allowance. A rejected request never increments,
    so — unlike a plain INCR-then-DECR-on-reject — a crash between the two ops
    can't leak a permanently-consumed quota slot, and concurrent requests never
    observe a transiently inflated counter. Implemented with a WATCH/MULTI
    optimistic transaction (no Lua, so it works against the in-test fakeredis).

    Standalone-Redis only: WATCH/MULTI is unsupported on cluster clients. That is
    fine — this path belongs to the legacy reference gateway; the deployed data
    plane (APISIX) enforces quotas via its limit-count plugin instead."""
    if cap_milli <= 0:
        return await incr_tenant(redis, tenant_id, period, milli)
    key = _tkey(tenant_id, period)
    async with redis.pipeline() as pipe:
        while True:
            try:
                await pipe.watch(key)
                raw = await pipe.get(key)
                cur = int(raw) if raw is not None else 0
                if cur + milli > cap_milli:
                    await pipe.unwatch()
                    return None
                pipe.multi()
                pipe.incrby(key, milli)
                res = await pipe.execute()
                return int(res[0])
            except WatchError:  # concurrent writer touched the key; retry
                continue


async def decr_tenant(redis, tenant_id: str, period: str, milli: int = 1000) -> int:
    return int(await redis.decrby(_tkey(tenant_id, period), milli))


async def incr_key(redis, key_id: str, period: str, milli: int = 1000) -> int:
    return int(await redis.incrby(_kkey(key_id, period), milli))


async def get_tenant_live(redis, tenant_id: str, period: str) -> int:
    val = await redis.get(_tkey(tenant_id, period))
    return int(val) if val is not None else 0


async def get_key_live(redis, key_id: str, period: str) -> int:
    val = await redis.get(_kkey(key_id, period))
    return int(val) if val is not None else 0


# ── durable event buffer ─────────────────────────────────────────────────────
async def push_event(
    redis, *, tenant_id: str, key_id: str, endpoint: str, period: str, day: str, milli: int = 1000
) -> None:
    await redis.rpush(
        config.USAGE_EVENTS_LIST,
        json.dumps(
            {"t": tenant_id, "k": key_id, "e": endpoint, "p": period, "d": day, "m": milli}
        ),
    )


async def record(redis, *, tenant_id: str, key_id: str, endpoint: str, milli: int = 1000) -> None:
    """Record one served request at its credit weight (``milli`` milli-credits):
    bump live counters + enqueue a durable event. Used by the APISIX usage sink
    and the legacy gateway. The live counters are the reference gateway's
    quota-enforcement state and a real-time observability read (e.g.
    ``get_tenant_live``); durable billing/display reads the Postgres rollups the
    durable event feeds."""
    period, day = now_parts()
    await incr_tenant(redis, tenant_id, period, milli)
    await incr_key(redis, key_id, period, milli)
    await push_event(
        redis,
        tenant_id=tenant_id,
        key_id=key_id,
        endpoint=endpoint,
        period=period,
        day=day,
        milli=milli,
    )


async def flush_events(pool, redis, *, batch: int = 1000) -> int:
    """Drain up to ``batch`` buffered events into Postgres rollups (idempotent
    increments). Returns the number of events processed."""
    events: list[dict] = []
    for _ in range(batch):
        raw = await redis.lpop(config.USAGE_EVENTS_LIST)
        if raw is None:
            break
        if isinstance(raw, bytes):
            raw = raw.decode()
        events.append(json.loads(raw))
    if not events:
        return 0

    # count = milli-credits, requests = raw event count. Events written before
    # the credit-units migration lack "m" and default to 1000 (1 credit).
    agg: dict[tuple, list[int]] = defaultdict(lambda: [0, 0])
    for e in events:
        entry = agg[(e["t"], e["k"], e["p"], e["d"], e.get("e", ""))]
        entry[0] += int(e.get("m", 1000))
        entry[1] += 1

    async with pool.acquire() as conn:
        async with conn.transaction():
            for (tenant_id, key_id, period, day, endpoint), (milli, n) in agg.items():
                await conn.execute(
                    """INSERT INTO usage_rollups
                           (tenant_id, key_id, period, day, endpoint, count, requests)
                       VALUES ($1,$2,$3,$4,$5,$6,$7)
                       ON CONFLICT (tenant_id, key_id, day, endpoint)
                       DO UPDATE SET count = usage_rollups.count + EXCLUDED.count,
                                     requests = usage_rollups.requests + EXCLUDED.requests""",
                    tenant_id,
                    key_id,
                    period,
                    date.fromisoformat(day),
                    endpoint,
                    milli,
                    n,
                )
    return len(events)
