"""Metered data plane — the distributed enforcement tier.

Every replica runs this identically and is stateless: all decisions read/write
*shared* Redis (key cache + cluster-wide live counters + event buffer) and fall
back to shared Postgres on a cache miss. This is the build's stand-in for an
Apache APISIX deployment (key-auth + limit-count[redis] + usage-logger); APISIX
can replace it without changing the control plane or billing.

Per request:
  1. resolve X-API-Key  → shared cache, else Postgres (then warm cache)
  2. authorize          → key active, tenant active, scope allows the path
  3. enforce quota      → atomic INCR of the shared tenant counter; hard-cap → 429
  4. meter              → INCR per-key counter + push durable usage event
  5. proxy              → forward to the upstream geocoder, return its response
"""
from __future__ import annotations

import httpx
from fastapi import FastAPI, Request, Response

from . import config, repo, security, usage

_HOP_BY_HOP = {"content-length", "transfer-encoding", "content-encoding",
               "connection", "keep-alive", "host"}


def build_app(pool=None, redis=None, *, http_client: httpx.AsyncClient | None = None) -> FastAPI:
    app = FastAPI(title="API Key Management — Gateway (data plane)")
    app.state.pool = pool
    app.state.redis = redis
    app.state.http_client = http_client

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.api_route("/{path:path}",
                   methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])
    async def proxy(path: str, request: Request):
        pool, redis = app.state.pool, app.state.redis
        if app.state.http_client is None:  # lazy init when run without a lifespan
            app.state.http_client = httpx.AsyncClient(base_url=config.PROXY_TARGET, timeout=30)

        # 1) resolve key ------------------------------------------------------
        api_key = request.headers.get(config.GATEWAY_API_KEY_HEADER)
        if not api_key:
            return _err(401, "missing API key")
        key_hash = security.hash_api_key(api_key)
        info = await _resolve_key(pool, redis, key_hash)
        if info is None:
            return _err(401, "invalid API key")

        # 2) authorize --------------------------------------------------------
        if info["key_status"] == "disabled":
            return _err(403, "API key is disabled")
        if info["key_status"] != "active":
            return _err(401, "invalid API key")
        if info["tenant_status"] != "active":
            return _err(403, "tenant is not active")

        endpoint = path.split("/", 1)[0] if path else ""
        scopes = info.get("scopes") or []
        if scopes and endpoint not in scopes:
            return _err(403, f"key not scoped for '{endpoint}'")

        # 3) enforce quota (cluster-wide via shared Redis) --------------------
        period, day = usage.now_parts()
        count = await usage.incr_tenant(redis, info["tenant_id"], period)
        quota = int(info.get("quota") or 0)
        if info.get("hard_cap") and quota > 0 and count > quota:
            await usage.decr_tenant(redis, info["tenant_id"], period)
            return _err(429, "monthly quota exceeded")

        # 4) meter ------------------------------------------------------------
        await usage.incr_key(redis, info["key_id"], period)
        await usage.push_event(redis, tenant_id=info["tenant_id"], key_id=info["key_id"],
                               endpoint=endpoint, period=period, day=day)

        # 5) proxy ------------------------------------------------------------
        return await _forward(app.state.http_client, request, path)

    return app


async def _resolve_key(pool, redis, key_hash: str) -> dict | None:
    cached = await usage.get_cached_key(redis, key_hash)
    if cached is not None:
        return cached
    info = await repo.get_key_by_hash(pool, key_hash)
    if info is None:
        return None
    plan = await repo.tenant_with_plan(pool, info["tenant_id"]) if \
        info["tenant_status"] != "deleted" else {}
    payload = {
        "key_id": info["key_id"], "tenant_id": info["tenant_id"],
        "key_status": info["key_status"], "tenant_status": info["tenant_status"],
        "scopes": info["scopes"], "quota": int(plan.get("monthly_quota") or 0),
        "hard_cap": bool(plan.get("hard_cap")), "plan_id": plan.get("plan_id"),
    }
    await usage.cache_key(redis, key_hash, payload)
    return payload


async def _forward(client: httpx.AsyncClient, request: Request, path: str) -> Response:
    body = await request.body()
    fwd_headers = {k: v for k, v in request.headers.items()
                   if k.lower() not in _HOP_BY_HOP
                   and k.lower() != config.GATEWAY_API_KEY_HEADER.lower()}
    try:
        upstream = await client.request(
            request.method, "/" + path, params=request.query_params,
            content=body, headers=fwd_headers)
    except httpx.HTTPError as exc:
        return _err(502, f"upstream error: {exc}")
    resp_headers = {k: v for k, v in upstream.headers.items()
                    if k.lower() not in _HOP_BY_HOP}
    return Response(content=upstream.content, status_code=upstream.status_code,
                    headers=resp_headers)


def _err(status_code: int, detail: str) -> Response:
    import json
    return Response(content=json.dumps({"error": detail}), status_code=status_code,
                    media_type="application/json")
