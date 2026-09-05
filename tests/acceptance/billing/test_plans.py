"""Admin CRUD on billing plans."""

from conftest import admin_token, bearer, make_tenant


async def test_list_seeded_plans(cp_client):
    atok = await admin_token(cp_client)
    plans = (await cp_client.get("/admin/plans", headers=bearer(atok))).json()
    by_id = {p["id"]: p for p in plans}
    assert {"free", "starter", "pro", "scale"} <= set(by_id)
    # 2026-07 OSM-band repricing: quotas are credits, overage is ¢/credit
    assert by_id["free"]["monthly_quota"] == 25_000 and by_id["free"]["hard_cap"]
    assert by_id["starter"]["monthly_quota"] == 250_000
    assert by_id["starter"]["overage_cents_per_unit"] == 0.03
    assert by_id["pro"]["monthly_quota"] == 3_000_000
    assert by_id["scale"]["monthly_quota"] == 12_000_000
    assert by_id["scale"]["base_price_cents"] == 99900
    # every seeded plan carries an rps burst cap
    assert by_id["free"]["rps"] == 2 and by_id["scale"]["rps"] == 50


async def test_create_get_update_delete_plan(cp_client):
    atok = await admin_token(cp_client)
    h = bearer(atok)
    body = {
        "id": "enterprise",
        "name": "Enterprise",
        "monthly_quota": 5000000,
        "base_price_cents": 99900,
        "overage_cents_per_unit": 0.01,
        "hard_cap": False,
    }

    created = await cp_client.post("/admin/plans", headers=h, json=body)
    assert created.status_code == 201, created.text
    assert created.json()["base_price_cents"] == 99900

    got = await cp_client.get("/admin/plans/enterprise", headers=h)
    assert got.status_code == 200 and got.json()["name"] == "Enterprise"

    upd = await cp_client.patch(
        "/admin/plans/enterprise", headers=h, json={"base_price_cents": 89900, "hard_cap": True}
    )
    assert upd.status_code == 200
    assert upd.json()["base_price_cents"] == 89900 and upd.json()["hard_cap"] is True

    d = await cp_client.delete("/admin/plans/enterprise", headers=h)
    assert d.status_code == 204
    assert (await cp_client.get("/admin/plans/enterprise", headers=h)).status_code == 404


async def test_duplicate_plan_id_conflicts(cp_client):
    atok = await admin_token(cp_client)
    h = bearer(atok)
    body = {
        "id": "starter",
        "name": "Dup",
        "monthly_quota": 1,
        "base_price_cents": 0,
        "overage_cents_per_unit": 0,
        "hard_cap": True,
    }
    r = await cp_client.post("/admin/plans", headers=h, json=body)
    assert r.status_code == 409


async def test_delete_plan_in_use_conflicts(cp_client):
    # create a plan, assign a tenant to it, then deletion must be blocked
    atok = await admin_token(cp_client)
    h = bearer(atok)
    await cp_client.post(
        "/admin/plans",
        headers=h,
        json={
            "id": "team",
            "name": "Team",
            "monthly_quota": 100,
            "base_price_cents": 500,
            "overage_cents_per_unit": 0.1,
            "hard_cap": False,
        },
    )
    await make_tenant(cp_client, name="OnTeam", plan_id="team", admin_email="team@acme.io")
    r = await cp_client.delete("/admin/plans/team", headers=h)
    assert r.status_code == 409
    assert "in use" in r.json()["detail"]


async def test_update_missing_plan_404(cp_client):
    atok = await admin_token(cp_client)
    r = await cp_client.patch("/admin/plans/ghost", headers=bearer(atok), json={"name": "X"})
    assert r.status_code == 404


async def test_tenant_user_cannot_manage_plans(cp_client):
    _, ttok = await make_tenant(cp_client, admin_email="noplan@acme.io")
    h = bearer(ttok)
    assert (await cp_client.get("/admin/plans", headers=h)).status_code == 403
    assert (
        await cp_client.post(
            "/admin/plans",
            headers=h,
            json={
                "id": "x",
                "name": "X",
                "monthly_quota": 1,
                "base_price_cents": 0,
                "overage_cents_per_unit": 0,
                "hard_cap": True,
            },
        )
    ).status_code == 403
