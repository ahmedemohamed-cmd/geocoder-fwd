"""Usage display reads durable Postgres rollups, so it survives a Redis cache
loss (the shared geocoder Redis is non-persistent + LRU-evicting)."""

from conftest import bearer, create_key, make_tenant

from billing import apisix_admin


async def test_usage_survives_redis_reset(cp_client, pool, redis):
    _, ttok = await make_tenant(cp_client, admin_email="dur@u.io", plan_id="starter")
    key = await create_key(cp_client, ttok)
    consumer = apisix_admin.consumer_name(key["id"])

    # three served requests recorded via the usage sink
    await cp_client.post(
        "/internal/usage",
        json=[{"consumer": consumer, "uri": "/geocode", "status": 200} for _ in range(3)],
    )

    # first read flushes the event buffer into Postgres rollups
    cur = await cp_client.get("/usage/current", headers=bearer(ttok))
    assert cur.json()["requests"] == 3
    assert cur.json()["per_key"][0]["requests"] == 3

    # simulate Redis eviction / restart: wipe ALL Redis state
    await redis.flushall()

    # durable display is unchanged (served from Postgres rollups, not Redis)
    cur2 = await cp_client.get("/usage/current", headers=bearer(ttok))
    assert cur2.json()["requests"] == 3
    assert cur2.json()["per_key"][0]["requests"] == 3
