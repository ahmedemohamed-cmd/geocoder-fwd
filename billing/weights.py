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

# Matrix calls are billed per source×target element (market prices matrices per
# element; a flat per-call weight lets a 100×100 request buy 10k routings for 5
# credits). The element rate lives in the weights table under this pseudo-key so
# it stays admin-tunable; the flat sources_to_targets weight acts as the floor
# for requests whose size can't be determined.
MATRIX_ENDPOINT = "sources_to_targets"
MATRIX_ELEMENT_KEY = "sources_to_targets_element"
DEFAULT_MATRIX_ELEMENT_MILLI = 100  # 0.1 credit per element

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
    # LLM inference (Ollama) on cache miss — heaviest single op in the stack,
    # amortized by the permanent per-place ES cache. Priced above worst-case
    # GPU cost per generation so cache misses are never underwater.
    "describe": 25000,
    MATRIX_ELEMENT_KEY: DEFAULT_MATRIX_ELEMENT_MILLI,
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
    # The matrix element rate is a multiplier, not an endpoint cost — including
    # it would inflate the raw-request backstop projected into APISIX.
    vals = [v for k, v in weights.items() if k != MATRIX_ELEMENT_KEY]
    return min(vals, default=DEFAULT_WEIGHT_MILLI)


def matrix_milli(weights: dict[str, int], n_sources: int, n_targets: int) -> int:
    """Billable milli-credits for one matrix call: per-element rate × size,
    floored at the flat sources_to_targets weight (which also covers requests
    whose size is unknown)."""
    floor = weight_for(weights, MATRIX_ENDPOINT)
    if n_sources <= 0 or n_targets <= 0:
        return floor
    per_element = weights.get(MATRIX_ELEMENT_KEY, DEFAULT_MATRIX_ELEMENT_MILLI)
    return max(floor, n_sources * n_targets * per_element)


def matrix_size(query_string: str | None, body: bytes | None) -> tuple[int, int]:
    """(n_sources, n_targets) of a Valhalla matrix request — the JSON body when
    present (POST), else the ``json`` query parameter (GET). (0, 0) when the
    request can't be parsed, which bills the flat floor."""
    import json
    from urllib.parse import parse_qs

    raw: bytes | str | None = body or None
    if raw is None and query_string:
        raw = (parse_qs(query_string).get("json") or [None])[0]
    if not raw:
        return (0, 0)
    try:
        req = json.loads(raw)
        return (len(req["sources"]), len(req["targets"]))
    except (ValueError, KeyError, TypeError):
        return (0, 0)


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
