"""Apache APISIX Admin-API client used by the control plane.

The control plane is the source of truth for keys/plans; this module projects
that state into APISIX (the data plane):

* one **route** → the geocoder, with key-auth + an http-logger to the usage sink
* one **consumer-group per tenant** carrying limit-count(redis) for a hard quota
  (only for hard-cap plans; soft plans are unlimited at the gateway, billed as
  overage)
* one **consumer per API key** (key-auth), placed in its tenant's group

All functions are no-ops unless APISIX is configured, so dev/tests are unaffected.
"""
from __future__ import annotations

import httpx

from . import config


def enabled() -> bool:
    return bool(config.APISIX_ADMIN_URL and config.APISIX_ADMIN_KEY)


def group_id(tenant_id: str) -> str:
    return "tenant_" + tenant_id.replace("-", "_")


def consumer_name(key_id: str) -> str:
    return "k_" + key_id.replace("-", "_")


def key_id_from_consumer(name: str) -> str | None:
    if not name or not name.startswith("k_"):
        return None
    return name[2:].replace("_", "-")


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=config.APISIX_ADMIN_URL.rstrip("/") + "/apisix/admin",
        headers={"X-API-KEY": config.APISIX_ADMIN_KEY}, timeout=15)


async def ensure_route() -> None:
    """Create/refresh the geocoder route + http-logger log format."""
    if not enabled():
        return
    plugins: dict = {"key-auth": {"header": config.APISIX_KEY_HEADER}}
    if config.USAGE_SINK_URL:
        plugins["http-logger"] = {
            "uri": config.USAGE_SINK_URL,
            "batch_max_size": 50, "inactive_timeout": 2, "buffer_duration": 2,
            "include_req_body": False,
        }
    async with _client() as c:
        await c.put("/plugin_metadata/http-logger", json={
            "log_format": {"consumer": "$consumer_name", "uri": "$uri", "status": "$status"}})
        r = await c.put(f"/routes/{config.APISIX_ROUTE_ID}", json={
            "uri": config.APISIX_ROUTE_URI,
            "upstream": {"type": "roundrobin", "nodes": {config.APISIX_UPSTREAM: 1}},
            "plugins": plugins})
        r.raise_for_status()


async def ensure_consumer_group(tenant_id: str, *, quota: int, hard_cap: bool) -> bool:
    """Create/update (hard-cap) or delete (soft) a tenant's limit-count group.
    Returns True if the group exists (so consumers should join it)."""
    if not enabled():
        return False
    gid = group_id(tenant_id)
    async with _client() as c:
        if hard_cap and quota > 0:
            await c.put(f"/consumer_groups/{gid}", json={"plugins": {"limit-count": {
                "count": quota, "time_window": config.APISIX_LIMIT_WINDOW,
                "rejected_code": 429, "policy": "redis",
                "redis_host": config.REDIS_HOST, "redis_port": config.REDIS_PORT,
                "redis_database": config.REDIS_DB,
                "key_type": "constant", "key": gid}}})
            return True
        await c.delete(f"/consumer_groups/{gid}")
        return False


async def upsert_consumer(key_id: str, *, api_key: str, in_group: bool, tenant_id: str) -> None:
    if not enabled():
        return
    body: dict = {"username": consumer_name(key_id),
                  "plugins": {"key-auth": {"key": api_key}}}
    if in_group:
        body["group_id"] = group_id(tenant_id)
    async with _client() as c:
        r = await c.put("/consumers", json=body)
        r.raise_for_status()


async def delete_consumer(key_id: str) -> None:
    if not enabled():
        return
    async with _client() as c:
        await c.delete(f"/consumers/{consumer_name(key_id)}")


async def delete_consumer_group(tenant_id: str) -> None:
    if not enabled():
        return
    async with _client() as c:
        await c.delete(f"/consumer_groups/{group_id(tenant_id)}")
