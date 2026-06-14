"""Unit tests for the APISIX integration pieces that don't need a live APISIX:
key encryption round-trip, consumer-name mapping, and the usage sink endpoint."""

from conftest import bearer, create_key, make_tenant

from billing import apisix_admin, security, usage


def test_key_encryption_roundtrip():
    full = "gk_abcd1234_SuperSecretValue"
    enc = security.encrypt_key(full)
    assert enc != full
    assert security.decrypt_key(enc) == full


def test_consumer_name_roundtrip():
    kid = "cc500066-8e1d-48ff-9299-c7c703a5f470"
    name = apisix_admin.consumer_name(kid)
    assert name == "k_cc500066_8e1d_48ff_9299_c7c703a5f470"
    assert apisix_admin.key_id_from_consumer(name) == kid
    assert apisix_admin.key_id_from_consumer("not-a-consumer") is None


def test_group_id():
    assert (
        apisix_admin.group_id("11111111-2222-3333-4444-555555555555")
        == "tenant_11111111_2222_3333_4444_555555555555"
    )


async def test_usage_sink_records_served_only(cp_client, pool, redis):
    tenant, ttok = await make_tenant(cp_client, admin_email="sink@acme.io")
    key = await create_key(cp_client, ttok)
    consumer = apisix_admin.consumer_name(key["id"])

    # APISIX http-logger style batch: one served, one rejected (429)
    r = await cp_client.post(
        "/internal/usage",
        json=[
            {"consumer": consumer, "uri": "/geocode", "status": 200},
            {"consumer": consumer, "uri": "/geocode", "status": 429},
            {"consumer": "k_unknown", "uri": "/geocode", "status": 200},
        ],
    )
    assert r.status_code == 200
    assert r.json()["recorded"] == 1  # only the served, known-key request

    period, _ = usage.now_parts()
    assert await usage.get_tenant_live(redis, tenant["id"], period) == 1
    assert await usage.get_key_live(redis, key["id"], period) == 1

    # and it surfaces through the tenant's real-time usage endpoint
    cur = await cp_client.get("/usage/current", headers=bearer(ttok))
    assert cur.json()["requests"] == 1


async def test_usage_sink_rejects_bad_token(cp_client, monkeypatch):
    from billing import config

    monkeypatch.setattr(config, "USAGE_SINK_SECRET", "s3cret")
    r = await cp_client.post("/internal/usage?token=wrong", json=[])
    assert r.status_code == 403
