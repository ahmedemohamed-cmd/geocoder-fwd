"""Admin CRUD on tenant users (add/modify/disable/delete)."""
from conftest import admin_token, bearer, login, make_tenant


async def test_user_crud_and_disable(cp_client):
    tenant, _ = await make_tenant(cp_client, admin_email="owner@u.io",
                                  admin_password="password123")
    atok = await admin_token(cp_client)
    h = bearer(atok)
    tid = tenant["id"]

    # add a second user
    r = await cp_client.post(f"/admin/tenants/{tid}/users", headers=h,
                             json={"email": "u2@u.io", "password": "password123"})
    assert r.status_code == 201, r.text
    assert r.json()["status"] == "active"
    assert (await cp_client.post("/auth/login",
            json={"email": "u2@u.io", "password": "password123"})).status_code == 200

    # duplicate add → 409
    assert (await cp_client.post(f"/admin/tenants/{tid}/users", headers=h,
            json={"email": "u2@u.io", "password": "password123"})).status_code == 409

    # disable → login blocked
    dis = await cp_client.patch(f"/admin/tenants/{tid}/users/u2@u.io", headers=h,
                                json={"status": "disabled"})
    assert dis.status_code == 200 and dis.json()["status"] == "disabled"
    assert (await cp_client.post("/auth/login",
            json={"email": "u2@u.io", "password": "password123"})).status_code == 403

    # re-enable → login works again
    await cp_client.patch(f"/admin/tenants/{tid}/users/u2@u.io", headers=h,
                          json={"status": "active"})
    assert (await cp_client.post("/auth/login",
            json={"email": "u2@u.io", "password": "password123"})).status_code == 200

    # list shows both with status
    users = (await cp_client.get(f"/admin/tenants/{tid}/users", headers=h)).json()
    emails = {u["email"]: u["status"] for u in users}
    assert emails.get("u2@u.io") == "active" and "owner@u.io" in emails

    # delete → user gone (login fails)
    d = await cp_client.delete(f"/admin/tenants/{tid}/users/u2@u.io", headers=h)
    assert d.status_code == 204
    assert (await cp_client.post("/auth/login",
            json={"email": "u2@u.io", "password": "password123"})).status_code == 401


async def test_user_mgmt_requires_admin(cp_client):
    tenant, ttok = await make_tenant(cp_client, admin_email="owner5@u.io")
    h = bearer(ttok)
    tid = tenant["id"]
    assert (await cp_client.post(f"/admin/tenants/{tid}/users", headers=h,
            json={"email": "x@u.io", "password": "password123"})).status_code == 403
    assert (await cp_client.patch(f"/admin/tenants/{tid}/users/owner5@u.io", headers=h,
            json={"status": "disabled"})).status_code == 403
    assert (await cp_client.delete(f"/admin/tenants/{tid}/users/owner5@u.io",
            headers=h)).status_code == 403


async def test_add_user_unknown_tenant_404(cp_client):
    atok = await admin_token(cp_client)
    r = await cp_client.post("/admin/tenants/00000000-0000-0000-0000-000000000000/users",
                             headers=bearer(atok),
                             json={"email": "x@u.io", "password": "password123"})
    assert r.status_code == 404
