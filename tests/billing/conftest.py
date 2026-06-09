"""Test fixtures for the billing subsystem.

Postgres: the real docker-compose instance, isolated ``billing_test`` database,
tables reset per test. Redis: in-process fakeredis (async). Both apps are driven
over httpx ASGITransport; the gateway proxies to an in-process echo upstream.
"""
import os
import sys

import asyncpg
import fakeredis.aioredis
import httpx
import pytest
import pytest_asyncio
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# Point the subsystem at the isolated test database before importing it.
os.environ.setdefault("BILLING_PG_DB", "billing_test")

from billing import config, control_plane, db, gateway  # noqa: E402

TEST_DSN = config.pg_dsn("billing_test")


@pytest_asyncio.fixture
async def pool():
    pg = await asyncpg.create_pool(TEST_DSN, min_size=1, max_size=5)
    await db.drop_all(pg)
    await db.bootstrap(pg)
    try:
        yield pg
    finally:
        await pg.close()


@pytest_asyncio.fixture
async def redis():
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        yield r
    finally:
        await r.flushall()
        await r.aclose()


# ── upstream echo (what the gateway proxies to) ──────────────────────────────
async def _echo(request):
    return JSONResponse({"upstream": True, "path": request.url.path,
                         "method": request.method})


def _upstream_app() -> Starlette:
    return Starlette(routes=[Route("/{path:path}", _echo,
                     methods=["GET", "POST", "PUT", "PATCH", "DELETE"])])


@pytest_asyncio.fixture
async def cp_client(pool, redis):
    app = control_plane.build_app(pool=pool, redis=redis)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://cp") as c:
        yield c


@pytest_asyncio.fixture
async def gw_client(pool, redis):
    upstream = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_upstream_app()), base_url="http://upstream")
    app = gateway.build_app(pool=pool, redis=redis, http_client=upstream)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as c:
        yield c
    await upstream.aclose()


# ── helpers ──────────────────────────────────────────────────────────────────
async def login(client, email, password):
    r = await client.post("/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


def bearer(token):
    return {"Authorization": f"Bearer {token}"}


async def admin_token(client):
    return await login(client, config.BOOTSTRAP_ADMIN_EMAIL, config.BOOTSTRAP_ADMIN_PASSWORD)


async def make_tenant(cp_client, *, name="Acme", plan_id="starter",
                      admin_email="owner@acme.test", admin_password="password123"):
    """Create a tenant as admin; return (tenant_dict, tenant_user_token)."""
    atok = await admin_token(cp_client)
    r = await cp_client.post("/admin/tenants", headers=bearer(atok), json={
        "name": name, "plan_id": plan_id,
        "admin_email": admin_email, "admin_password": admin_password})
    assert r.status_code == 201, r.text
    tenant = r.json()
    ttok = await login(cp_client, admin_email, admin_password)
    return tenant, ttok


async def insert_plan(pool, *, plan_id, quota, base_cents=0, overage=0.0, hard_cap=False):
    await pool.execute(
        """INSERT INTO plans (id, name, monthly_quota, base_price_cents,
                              overage_cents_per_unit, hard_cap)
           VALUES ($1,$2,$3,$4,$5,$6)
           ON CONFLICT (id) DO UPDATE SET monthly_quota=EXCLUDED.monthly_quota,
               base_price_cents=EXCLUDED.base_price_cents,
               overage_cents_per_unit=EXCLUDED.overage_cents_per_unit,
               hard_cap=EXCLUDED.hard_cap""",
        plan_id, plan_id.title(), quota, base_cents, overage, hard_cap)


async def create_key(cp_client, ttok, *, name="k1", scopes=None):
    r = await cp_client.post("/keys", headers=bearer(ttok),
                             json={"name": name, "scopes": scopes or []})
    assert r.status_code == 201, r.text
    return r.json()
