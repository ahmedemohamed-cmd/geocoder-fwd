"""Admin CRUD on other platform admins (with last-admin lockout protection)."""

from conftest import admin_token, bearer, make_tenant


async def test_list_includes_bootstrap_admin(cp_client):
    atok = await admin_token(cp_client)
    admins = (await cp_client.get("/admin/admins", headers=bearer(atok))).json()
    assert any(a["email"] == "admin@example.com" and a["status"] == "active" for a in admins)


async def test_admin_crud_cycle(cp_client):
    atok = await admin_token(cp_client)
    h = bearer(atok)

    # add a second admin → can log in
    r = await cp_client.post(
        "/admin/admins", headers=h, json={"email": "a2@x.io", "password": "password123"}
    )
    assert r.status_code == 201 and r.json()["status"] == "active"
    assert (
        await cp_client.post("/auth/login", json={"email": "a2@x.io", "password": "password123"})
    ).status_code == 200

    # duplicate → 409
    assert (
        await cp_client.post(
            "/admin/admins", headers=h, json={"email": "a2@x.io", "password": "password123"}
        )
    ).status_code == 409

    # disable → login blocked; enable → works
    assert (
        await cp_client.patch("/admin/admins/a2@x.io", headers=h, json={"status": "disabled"})
    ).json()["status"] == "disabled"
    assert (
        await cp_client.post("/auth/login", json={"email": "a2@x.io", "password": "password123"})
    ).status_code == 403
    await cp_client.patch("/admin/admins/a2@x.io", headers=h, json={"status": "active"})

    # reset password
    assert (
        await cp_client.post(
            "/admin/admins/a2@x.io/reset-password",
            headers=h,
            json={"email": "a2@x.io", "new_password": "NewPass_99"},
        )
    ).status_code == 204
    assert (
        await cp_client.post("/auth/login", json={"email": "a2@x.io", "password": "NewPass_99"})
    ).status_code == 200

    # delete → gone
    assert (await cp_client.delete("/admin/admins/a2@x.io", headers=h)).status_code == 204
    assert (
        await cp_client.post("/auth/login", json={"email": "a2@x.io", "password": "NewPass_99"})
    ).status_code == 401


async def test_cannot_remove_or_disable_last_admin(cp_client):
    atok = await admin_token(cp_client)
    h = bearer(atok)
    # only the bootstrap admin exists → it's the last one
    assert (await cp_client.delete("/admin/admins/admin@example.com", headers=h)).status_code == 409
    assert (
        await cp_client.patch(
            "/admin/admins/admin@example.com", headers=h, json={"status": "disabled"}
        )
    ).status_code == 409


async def test_unknown_admin_404(cp_client):
    atok = await admin_token(cp_client)
    r = await cp_client.delete("/admin/admins/ghost@x.io", headers=bearer(atok))
    assert r.status_code == 404


async def test_tenant_user_cannot_manage_admins(cp_client):
    _, ttok = await make_tenant(cp_client, admin_email="t@adm.io")
    h = bearer(ttok)
    assert (await cp_client.get("/admin/admins", headers=h)).status_code == 403
    assert (
        await cp_client.post(
            "/admin/admins", headers=h, json={"email": "x@x.io", "password": "password123"}
        )
    ).status_code == 403
