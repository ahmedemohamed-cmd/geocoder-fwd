"""Per-endpoint credit weights.

A request's billable cost depends on which endpoint served it: an autocomplete
keystroke is a cheap filtered ES query while a deep/vector search or a Valhalla
matrix call burns an order of magnitude more CPU. Weights are stored in the
``endpoint_weights`` table as integer **milli-credits** (1 credit = 1000) so
counters stay integral end-to-end; endpoints without a row cost the default
1 credit. The endpoint key is the first path segment — the same attribution the
usage sink and gateway already use — so ``deep/forward`` and ``deep/nearby``
both resolve as ``deep``.

Reads go through a small per-process TTL cache: the usage sink resolves a
weight per logged request, and one Postgres query per process per TTL is
plenty fresh for pricing (admin edits call :func:`invalidate` locally and other
processes converge within the TTL).
"""

from __future__ import annotations

import asyncio
import logging
import time

_log = logging.getLogger("billing.weights")

MILLI_PER_CREDIT = 1000
DEFAULT_WEIGHT_MILLI = 1000  # unlisted endpoints cost 1 credit

DEFAULT_WEIGHTS: dict[str, int] = {
    "autocomplete": 250,
    "geocode": 1000,
    "reverse": 1000,
    "nearby": 1000,
    "deep": 3000,
    "route": 5000,
    "optimized_route": 5000,
    "sources_to_targets": 5000,
    "isochrone": 5000,
    "locate": 5000,
}

_TTL = 15.0  # seconds
_cache: dict[str, int] | None = None
_loaded_at: float = 0.0
_lock = asyncio.Lock()


async def get_weights(pool) -> dict[str, int]:
    """Return {endpoint: milli_credits}, cached per-process for ``_TTL`` seconds.
    On a DB error the stale copy (or the seed defaults) is served so metering
    never stalls the hot path."""
    global _cache, _loaded_at
    if _cache is not None and time.monotonic() - _loaded_at < _TTL:
        return _cache
    async with _lock:
        if _cache is not None and time.monotonic() - _loaded_at < _TTL:
            return _cache
        try:
            rows = await pool.fetch("SELECT endpoint, milli_credits FROM endpoint_weights")
            _cache = {r["endpoint"]: int(r["milli_credits"]) for r in rows}
        except Exception as e:  # noqa: BLE001 - serve stale rather than fail metering
            _log.warning("could not load endpoint weights, serving cached/defaults: %s", e)
            if _cache is None:
                _cache = dict(DEFAULT_WEIGHTS)
        _loaded_at = time.monotonic()
        return _cache


def weight_for(weights: dict[str, int], endpoint: str) -> int:
    return weights.get(endpoint, DEFAULT_WEIGHT_MILLI)


def min_weight(weights: dict[str, int]) -> int:
    return min(weights.values(), default=DEFAULT_WEIGHT_MILLI)


def projected_request_cap(weights: dict[str, int], quota_credits: int) -> int:
    """Raw-request backstop for APISIX limit-count: the most requests a tenant
    could make within its credit quota if every call hit the cheapest endpoint.
    The credit quota in Postgres stays authoritative for billing."""
    return quota_credits * MILLI_PER_CREDIT // max(1, min_weight(weights))


def invalidate() -> None:
    """Drop this process's cache (called after an admin weight edit)."""
    global _cache, _loaded_at
    _cache = None
    _loaded_at = 0.0
