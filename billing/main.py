"""Runnable ASGI entrypoints.

    uvicorn billing.main:control_plane_app --port 8100   # management API
    uvicorn billing.main:gateway_app      --port 8080   # metered data plane

Both create their own Postgres pool + Redis client on startup. The control plane
bootstraps the schema/seed data; the gateway runs the background usage aggregator
that drains the Redis event buffer into Postgres rollups.
"""

from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager

import redis.asyncio as aioredis

from . import apisix_admin, config, control_plane, db, gateway, usage


def _make_redis() -> aioredis.Redis:
    return aioredis.Redis(
        host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB, decode_responses=True
    )


async def _aggregator_loop(pool, redis, interval: float = 2.0):
    while True:
        try:
            await usage.flush_events(pool, redis)
        except Exception:  # noqa: BLE001 - keep the loop alive
            pass
        await asyncio.sleep(interval)


# ── control plane ────────────────────────────────────────────────────────────
@asynccontextmanager
async def _cp_lifespan(app):
    await db.ensure_database()
    app.state.pool = await db.create_pool()
    app.state.redis = _make_redis()
    await db.bootstrap(app.state.pool)
    if apisix_admin.enabled():  # provision the geocoder + valhalla routes
        try:
            await apisix_admin.ensure_route()
            await apisix_admin.ensure_valhalla_route()
        except Exception:  # noqa: BLE001 - APISIX may still be starting
            pass
    yield
    await app.state.pool.close()
    await app.state.redis.aclose()


control_plane_app = control_plane.build_app()
control_plane_app.router.lifespan_context = _cp_lifespan


# ── gateway (data plane) ─────────────────────────────────────────────────────
@asynccontextmanager
async def _gw_lifespan(app):
    app.state.pool = await db.create_pool()
    app.state.redis = _make_redis()
    if app.state.http_client is None:
        import httpx

        app.state.http_client = httpx.AsyncClient(base_url=config.PROXY_TARGET, timeout=30)
    agg = asyncio.create_task(_aggregator_loop(app.state.pool, app.state.redis))
    yield
    agg.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await agg
    await app.state.http_client.aclose()
    await app.state.pool.close()
    await app.state.redis.aclose()


gateway_app = gateway.build_app()
gateway_app.router.lifespan_context = _gw_lifespan
