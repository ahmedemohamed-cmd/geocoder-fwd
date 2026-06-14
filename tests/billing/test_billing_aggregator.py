"""Usage aggregation (Redis buffer → Postgres rollups) and billing/invoicing."""

from conftest import bearer, create_key, insert_plan, login, make_tenant

from billing import usage
from billing.billing_engine import compute_charge


# ── aggregator ───────────────────────────────────────────────────────────────
async def test_flush_events_aggregates_into_rollups(cp_client, gw_client, pool, redis):
    _, ttok = await make_tenant(cp_client, admin_email="agg@acme.test", plan_id="starter")
    key = await create_key(cp_client, ttok)
    h = {"X-API-Key": key["api_key"]}
    for _ in range(5):
        await gw_client.get("/geocode", headers=h)

    processed = await usage.flush_events(pool, redis)
    assert processed == 5
    total = await pool.fetchval(
        "SELECT SUM(count)::bigint FROM usage_rollups WHERE key_id=$1", key["id"]
    )
    assert total == 5
    # idempotent drain — nothing left to process
    assert await usage.flush_events(pool, redis) == 0


# ── pricing maths ────────────────────────────────────────────────────────────
def test_compute_charge_within_quota():
    amount, items = compute_charge(
        total_requests=100, base_price_cents=2900, overage_cents_per_unit=0.05, monthly_quota=50000
    )
    assert amount == 2900
    assert items[0]["amount_cents"] == 2900


def test_compute_charge_with_overage():
    # 60000 requests, 50000 included, 0.05c each over → 10000 * 0.05 = 500c
    amount, items = compute_charge(
        total_requests=60000,
        base_price_cents=2900,
        overage_cents_per_unit=0.05,
        monthly_quota=50000,
    )
    assert amount == 2900 + 500
    assert items[-1]["quantity"] == 10000


# ── end-to-end billing ───────────────────────────────────────────────────────
async def test_billing_run_marks_and_pays(cp_client, gw_client, pool, redis):
    await insert_plan(pool, plan_id="bill", quota=2, base_cents=1000, overage=10.0)
    tenant, ttok = await make_tenant(cp_client, admin_email="bill@acme.test", plan_id="bill")
    key = await create_key(cp_client, ttok)
    h = {"X-API-Key": key["api_key"]}
    for _ in range(5):  # 2 included + 3 overage @10c = 1000 + 30 = 1030
        await gw_client.get("/geocode", headers=h)

    period, _ = usage.now_parts()
    atok = await login(cp_client, "admin@example.com", "admin12345")

    run = await cp_client.post(f"/admin/billing/run?period={period}", headers=bearer(atok))
    assert run.status_code == 200
    inv = next(i for i in run.json() if i["tenant_id"] == tenant["id"])
    assert inv["total_requests"] == 5
    assert inv["amount_cents"] == 1030
    assert inv["status"] == "pending"

    # tenant sees its own invoice
    mine = await cp_client.get("/invoices", headers=bearer(ttok))
    assert any(i["id"] == inv["id"] for i in mine.json())

    # admin sees the tenant's bills and marks paid
    admin_view = await cp_client.get(
        f"/admin/tenants/{tenant['id']}/invoices", headers=bearer(atok)
    )
    assert admin_view.json()[0]["id"] == inv["id"]

    pay = await cp_client.post(f"/admin/invoices/{inv['id']}/pay", headers=bearer(atok))
    assert pay.status_code == 200 and pay.json()["status"] == "paid"
    assert pay.json()["paid_at"] is not None

    # paying again conflicts; re-running billing must not overwrite a paid invoice
    again = await cp_client.post(f"/admin/invoices/{inv['id']}/pay", headers=bearer(atok))
    assert again.status_code == 409
    rerun = await cp_client.post(f"/admin/billing/run?period={period}", headers=bearer(atok))
    inv2 = next(i for i in rerun.json() if i["tenant_id"] == tenant["id"])
    assert inv2["status"] == "paid"


async def test_usage_history_report(cp_client, gw_client, pool, redis):
    _, ttok = await make_tenant(cp_client, admin_email="hist@acme.test", plan_id="starter")
    key = await create_key(cp_client, ttok)
    h = {"X-API-Key": key["api_key"]}
    for _ in range(4):
        await gw_client.get("/geocode", headers=h)

    period, day = usage.now_parts()
    rep = await cp_client.get(f"/usage/history?from={period}&to={period}", headers=bearer(ttok))
    assert rep.status_code == 200
    rows = rep.json()
    assert sum(r["requests"] for r in rows) == 4
    assert all(r["period"] == period for r in rows)
    assert rows[0]["endpoint"] == "geocode"


async def test_tenant_cannot_reach_admin_billing(cp_client):
    _, ttok = await make_tenant(cp_client, admin_email="noadmin@acme.test")
    assert (await cp_client.get("/admin/invoices", headers=bearer(ttok))).status_code == 403
    assert (
        await cp_client.post("/admin/billing/run?period=2026-06", headers=bearer(ttok))
    ).status_code == 403
