"""Redis result cache for read endpoints (cache-aside).

A small, self-contained service that wraps an aioredis client so the read
endpoints can follow one pattern:

    1. call the cache  (``get(key)``)  → return the stored JSON on a hit
    2. fall back to ES/PostGIS on a miss, then ``set(key, body)``

Design notes
------------
* **Values are the already-serialized response bytes.** A hit is a single Redis
  ``GET`` returned verbatim — no dict rebuild, no re-serialization — so it costs
  almost nothing and never touches Elasticsearch.
* **Keys are versioned** (``_KEY_VERSION``). Bumping the version rolls out a
  query/ranking change cleanly: new keys miss, old entries just age out via TTL.
* **Keys are built only from response-affecting params.** ``lat``/``lon`` are
  rounded (see ``coord``) so thousands of near-identical coordinates collapse to
  one entry — the main hit-rate lever — while staying well inside the geo-decay
  scale, so ranking is unchanged.
* **Fail-open.** Any Redis error is swallowed and treated as a miss, so the cache
  can never take the endpoint down — it only ever removes load.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("geocoder")

# Bump to invalidate every cached entry at once (e.g. after a query-logic change).
_KEY_VERSION = "v1"


class ResultCache:
    """Cache-aside wrapper around an aioredis client (``decode_responses=True``)."""

    def __init__(self, redis, *, enabled: bool, ttl: int, coord_precision: int, prefix: str = "cache"):
        self._redis = redis
        # enabled only when explicitly on AND a client is actually present
        self.enabled = bool(enabled) and redis is not None
        self.ttl = ttl
        self._cp = coord_precision
        self._prefix = prefix

    # ── key construction ──────────────────────────────────────────────────
    @staticmethod
    def norm_q(q: str | None) -> str:
        """Normalize a free-text query for the key: lowercase + collapse spaces."""
        return " ".join((q or "").lower().split())

    def coord(self, v: float | None) -> str:
        """Round a coordinate to the configured precision (or '-' when absent)."""
        if v is None:
            return "-"
        return format(round(float(v), self._cp), f".{self._cp}f")

    def key(self, endpoint: str, **params) -> str:
        """Build a deterministic key from an endpoint name + scalar params."""
        items = "|".join(f"{k}={params[k]}" for k in sorted(params))
        return f"{self._prefix}:{_KEY_VERSION}:{endpoint}:{items}"

    # ── get / set (fail-open) ─────────────────────────────────────────────
    async def get(self, key: str) -> str | None:
        if not self.enabled:
            return None
        try:
            return await self._redis.get(key)
        except Exception:
            logger.debug("[cache] get failed for %s", key, exc_info=True)
            return None

    async def set(self, key: str, body) -> None:
        if not self.enabled:
            return
        try:
            await self._redis.set(key, body, ex=self.ttl)
        except Exception:
            logger.debug("[cache] set failed for %s", key, exc_info=True)
