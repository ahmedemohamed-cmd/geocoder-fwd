"""Gateway (data plane): validation, real-time metering, quota, scopes."""
from conftest import (bearer, create_key, insert_plan, login, make_tenant)


async def test_valid_key_proxies_and_meters_realtime(cp_client, gw_client, pool, redis):
    tenant, ttok = await make_tenant(cp_client, admin_email="g1@acme.test", plan_id="starter")
    key = await create_key(cp_client, ttok)
    apikey = key["api_key"]

    # before any traffic
    cur0 = await cp_client.get("/usage/current", headers=bearer(ttok))
    assert cur0.json()["requests"] == 0

    # three calls through the gateway, proxied to the echo upstream
    for _ in range(3):
        r = await gw_client.get("/geocode", headers={"X-API-Key": apikey})
        assert r.status_code == 200
        assert r.json()["upstream"] is True

    # real-time usage reflects immediately (read straight from Redis live counter)
    cur = await cp_client.get("/usage/current", headers=bearer(ttok))
    body = cur.json()
    assert body["requests"] == 3
    assert body["per_key"][0]["requests"] == 3
    assert body["remaining"] == body["quota"] - 3
    assert body["over_quota"] is False


async def test_missing_invalid_disabled_deleted_keys(cp_client, gw_client):
    _, ttok = await make_tenant(cp_client, admin_email="g2@acme.test")
    key = await create_key(cp_client, ttok)
    apikey, kid = key["api_key"], key["id"]

    assert (await gw_client.get("/geocode")).status_code == 401                       # missing
    assert (await gw_client.get("/geocode",
            headers={"X-API-Key": "gk_bad_nope"})).status_code == 401                  # invalid

    await cp_client.patch(f"/keys/{kid}", headers=bearer(ttok), json={"status": "disabled"})
    assert (await gw_client.get("/geocode",
            headers={"X-API-Key": apikey})).status_code == 403                         # disabled

    await cp_client.patch(f"/keys/{kid}", headers=bearer(ttok), json={"status": "active"})
    assert (await gw_client.get("/geocode",
            headers={"X-API-Key": apikey})).status_code == 200                         # re-enabled

    await cp_client.delete(f"/keys/{kid}", headers=bearer(ttok))
    assert (await gw_client.get("/geocode",
            headers={"X-API-Key": apikey})).status_code == 401                         # soft-deleted


async def test_suspended_tenant_blocked(cp_client, gw_client):
    tenant, ttok = await make_tenant(cp_client, admin_email="g3@acme.test")
    key = await create_key(cp_client, ttok)
    atok = await login(cp_client, "admin@example.com", "admin12345")
    await cp_client.patch(f"/admin/tenants/{tenant['id']}", headers=bearer(atok),
                          json={"status": "suspended"})
    r = await gw_client.get("/geocode", headers={"X-API-Key": key["api_key"]})
    assert r.status_code == 403


async def test_cache_miss_falls_back_to_db(cp_client, gw_client, redis):
    _, ttok = await make_tenant(cp_client, admin_email="g4@acme.test")
    key = await create_key(cp_client, ttok)
    # wipe the shared cache so the gateway must hit Postgres, then warm it again
    await redis.flushall()
    r = await gw_client.get("/geocode", headers={"X-API-Key": key["api_key"]})
    assert r.status_code == 200


async def test_hard_cap_returns_429(cp_client, gw_client, pool):
    await insert_plan(pool, plan_id="tiny", quota=2, hard_cap=True)
    _, ttok = await make_tenant(cp_client, admin_email="g5@acme.test", plan_id="tiny")
    key = await create_key(cp_client, ttok)
    h = {"X-API-Key": key["api_key"]}

    assert (await gw_client.get("/geocode", headers=h)).status_code == 200
    assert (await gw_client.get("/geocode", headers=h)).status_code == 200
    assert (await gw_client.get("/geocode", headers=h)).status_code == 429  # over hard cap

    # counter must not have advanced past the cap on the rejected call
    cur = await cp_client.get("/usage/current", headers=bearer(ttok))
    assert cur.json()["requests"] == 2


async def test_soft_cap_allows_overage(cp_client, gw_client, pool):
    await insert_plan(pool, plan_id="soft", quota=1, hard_cap=False, overage=1.0)
    _, ttok = await make_tenant(cp_client, admin_email="g6@acme.test", plan_id="soft")
    key = await create_key(cp_client, ttok)
    h = {"X-API-Key": key["api_key"]}
    for _ in range(3):
        assert (await gw_client.get("/geocode", headers=h)).status_code == 200
    cur = await cp_client.get("/usage/current", headers=bearer(ttok))
    assert cur.json()["requests"] == 3
    assert cur.json()["over_quota"] is True


async def test_scope_enforcement(cp_client, gw_client):
    _, ttok = await make_tenant(cp_client, admin_email="g7@acme.test")
    key = await create_key(cp_client, ttok, name="scoped", scopes=["geocode"])
    h = {"X-API-Key": key["api_key"]}
    assert (await gw_client.get("/geocode", headers=h)).status_code == 200
    assert (await gw_client.get("/reverse", headers=h)).status_code == 403
