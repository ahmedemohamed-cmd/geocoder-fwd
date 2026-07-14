"""Per-endpoint credit weights: seeding, admin CRUD, weighted metering, and the
one-time credit-units migration."""

from conftest import admin_token, bearer, create_key, make_tenant

from billing import apisix_admin, db, weights


# ── seeding ───────────────────────────────────────────────────────────────────
async def test_default_weights_seeded(cp_client):
    atok = await admin_token(cp_client)
    rows = (await cp_client.get("/admin/weights", headers=bearer(atok))).json()
    by_ep = {w["endpoint"]: w["milli_credits"] for w in rows}
    assert by_ep == weights.DEFAULT_WEIGHTS
    assert by_ep["autocomplete"] == 250
    assert by_ep["deep"] == 3000
    assert by_ep["route"] == 5000


async def test_seed_never_clobbers_admin_edit(cp_client, pool):
    atok = await admin_token(cp_client)
    r = await cp_client.put(
        "/admin/weights/autocomplete", headers=bearer(atok), json={"milli_credits": 500}
    )
    assert r.status_code == 200 and r.json()["milli_credits"] == 500
    await db.seed_weights(pool)  # restart-time reseed
    got = await cp_client.get("/admin/weights", headers=bearer(atok))
    assert next(w for w in got.json() if w["endpoint"] == "autocomplete")["milli_credits"] == 500


# ── admin CRUD ────────────────────────────────────────────────────────────────
async def test_weight_crud(cp_client):
    atok = await admin_token(cp_client)
    h = bearer(atok)

    created = await cp_client.put("/admin/weights/describe", headers=h, json={"milli_credits": 2000})
    assert created.status_code == 200 and created.json()["endpoint"] == "describe"

    updated = await cp_client.put("/admin/weights/describe", headers=h, json={"milli_credits": 750})
    assert updated.json()["milli_credits"] == 750

    d = await cp_client.delete("/admin/weights/describe", headers=h)
    assert d.status_code == 204
    assert (await cp_client.delete("/admin/weights/describe", headers=h)).status_code == 404

    # invalid slugs / negative values rejected
    assert (
        await cp_client.put("/admin/weights/Bad%20Slug!", headers=h, json={"milli_credits": 1})
    ).status_code == 422
    assert (
        await cp_client.put("/admin/weights/x", headers=h, json={"milli_credits": -1})
    ).status_code == 422


async def test_tenant_cannot_manage_weights(cp_client):
    _, ttok = await make_tenant(cp_client, admin_email="noweights@acme.io")
    h = bearer(ttok)
    assert (await cp_client.get("/admin/weights", headers=h)).status_code == 403
    assert (
        await cp_client.put("/admin/weights/geocode", headers=h, json={"milli_credits": 1})
    ).status_code == 403
    assert (await cp_client.delete("/admin/weights/geocode", headers=h)).status_code == 403


# ── weighted metering through the usage sink ──────────────────────────────────
async def test_sink_applies_endpoint_weights(cp_client, pool, redis):
    _, ttok = await make_tenant(cp_client, admin_email="wsink@u.io", plan_id="starter")
    key = await create_key(cp_client, ttok)
    consumer = apisix_admin.consumer_name(key["id"])

    entries = (
        [{"consumer": consumer, "uri": "/autocomplete?q=c", "status": 200}] * 4
        + [{"consumer": consumer, "uri": "/geocode?q=cairo", "status": 200}]
        + [{"consumer": consumer, "uri": "/deep/forward?q=x", "status": 200}]
    )
    r = await cp_client.post("/internal/usage", json=entries)
    assert r.json()["recorded"] == 6

    cur = (await cp_client.get("/usage/current", headers=bearer(ttok))).json()
    assert cur["requests"] == 6
    assert cur["credits_used"] == 5.0  # 4×0.25 + 1 + 3
    assert cur["remaining"] == cur["quota"] - 5.0
    assert cur["per_key"][0]["credits"] == 5.0


async def test_weight_edit_changes_metering(cp_client, pool, redis):
    _, ttok = await make_tenant(cp_client, admin_email="wedit@u.io", plan_id="starter")
    key = await create_key(cp_client, ttok)
    consumer = apisix_admin.consumer_name(key["id"])
    atok = await admin_token(cp_client)

    await cp_client.put(
        "/admin/weights/autocomplete", headers=bearer(atok), json={"milli_credits": 500}
    )  # 0.25 → 0.5 credits; PUT invalidates the in-process cache
    await cp_client.post(
        "/internal/usage",
        json=[{"consumer": consumer, "uri": "/autocomplete?q=c", "status": 200}] * 2,
    )
    cur = (await cp_client.get("/usage/current", headers=bearer(ttok))).json()
    assert cur["credits_used"] == 1.0


async def test_unknown_endpoint_costs_one_credit(cp_client, pool, redis):
    _, ttok = await make_tenant(cp_client, admin_email="wdef@u.io", plan_id="starter")
    key = await create_key(cp_client, ttok)
    consumer = apisix_admin.consumer_name(key["id"])
    await cp_client.post(
        "/internal/usage", json=[{"consumer": consumer, "uri": "/describe?id=1", "status": 200}]
    )
    cur = (await cp_client.get("/usage/current", headers=bearer(ttok))).json()
    assert cur["credits_used"] == 1.0


# ── projected raw-request backstop ────────────────────────────────────────────
def test_projected_request_cap():
    w = dict(weights.DEFAULT_WEIGHTS)
    assert weights.projected_request_cap(w, 10_000) == 40_000  # min weight 0.25
    assert weights.projected_request_cap({}, 10_000) == 10_000  # default weight 1


# ── one-time migration idempotency ────────────────────────────────────────────
async def test_credit_units_migration_runs_once(cp_client, pool):
    # bootstrap already ran in the fixture; the migration must be recorded once
    n = await pool.fetchval("SELECT count(*) FROM schema_migrations WHERE id=$1",
                            "2026-07-credit-units")
    assert n == 1
    # rollups written after the migration must never be re-multiplied by a re-run
    _, ttok = await make_tenant(cp_client, admin_email="mig@u.io", plan_id="starter")
    key = await create_key(cp_client, ttok)
    await pool.execute(
        """INSERT INTO usage_rollups (tenant_id, key_id, period, day, endpoint, count, requests)
           VALUES ($1,$2,'2026-07','2026-07-01','geocode',5000,5)""",
        key["tenant_id"],
        key["id"],
    )
    await db.bootstrap(pool)  # e.g. control-plane restart
    row = await pool.fetchrow(
        "SELECT count, requests FROM usage_rollups WHERE key_id=$1", key["id"]
    )
    assert row["count"] == 5000 and row["requests"] == 5
    # plans keep the repriced values and scale still exists exactly once
    plans = await pool.fetch("SELECT id FROM plans WHERE id='scale'")
    assert len(plans) == 1
