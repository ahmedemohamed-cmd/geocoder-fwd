"""Auth + admin tenant CRUD."""
from conftest import admin_token, bearer, login, make_tenant


async def test_admin_login_ok_and_me(cp_client):
    tok = await admin_token(cp_client)
    r = await cp_client.get("/auth/me", headers=bearer(tok))
    assert r.status_code == 200
    body = r.json()
    assert body["role"] == "admin"
    assert body["tenant_id"] is None


async def test_login_wrong_password(cp_client):
    r = await cp_client.post("/auth/login",
                             json={"email": "admin@example.com", "password": "nope"})
    assert r.status_code == 401


async def test_unauthenticated_rejected(cp_client):
    assert (await cp_client.get("/admin/tenants")).status_code == 401  # missing bearer


async def test_admin_creates_tenant_and_user_can_login(cp_client):
    tenant, ttok = await make_tenant(cp_client, name="Acme", admin_email="a@acme.test")
    assert tenant["name"] == "Acme"
    assert tenant["status"] == "active"
    me = await cp_client.get("/auth/me", headers=bearer(ttok))
    assert me.json()["role"] == "tenant_user"
    assert me.json()["tenant_id"] == tenant["id"]


async def test_tenant_user_cannot_manage_tenants(cp_client):
    _, ttok = await make_tenant(cp_client, admin_email="b@acme.test")
    r = await cp_client.post("/admin/tenants", headers=bearer(ttok), json={
        "name": "X", "admin_email": "x@x.test", "admin_password": "password123"})
    assert r.status_code == 403


async def test_create_tenant_unknown_plan_404(cp_client):
    atok = await admin_token(cp_client)
    r = await cp_client.post("/admin/tenants", headers=bearer(atok), json={
        "name": "Z", "plan_id": "ghost",
        "admin_email": "z@z.test", "admin_password": "password123"})
    assert r.status_code == 404


async def test_create_tenant_duplicate_user_409(cp_client):
    await make_tenant(cp_client, name="One", admin_email="dup@acme.test")
    atok = await admin_token(cp_client)
    r = await cp_client.post("/admin/tenants", headers=bearer(atok), json={
        "name": "Two", "plan_id": "free",
        "admin_email": "dup@acme.test", "admin_password": "password123"})
    assert r.status_code == 409


async def test_list_get_update_delete_tenant(cp_client):
    tenant, _ = await make_tenant(cp_client, name="Beta", admin_email="beta@acme.test")
    atok = await admin_token(cp_client)
    h = bearer(atok)

    lst = await cp_client.get("/admin/tenants", headers=h)
    assert any(t["id"] == tenant["id"] for t in lst.json())

    upd = await cp_client.patch(f"/admin/tenants/{tenant['id']}", headers=h,
                                json={"name": "Beta2", "plan_id": "pro"})
    assert upd.status_code == 200 and upd.json()["name"] == "Beta2"
    assert upd.json()["plan_id"] == "pro"

    # invalid plan on update
    bad = await cp_client.patch(f"/admin/tenants/{tenant['id']}", headers=h,
                                json={"plan_id": "ghost"})
    assert bad.status_code == 400

    # soft delete → excluded from default list, visible with include_deleted
    d = await cp_client.delete(f"/admin/tenants/{tenant['id']}", headers=h)
    assert d.status_code == 204
    lst2 = await cp_client.get("/admin/tenants", headers=h)
    assert all(t["id"] != tenant["id"] for t in lst2.json())
    lst3 = await cp_client.get("/admin/tenants?include_deleted=true", headers=h)
    assert any(t["id"] == tenant["id"] and t["status"] == "deleted" for t in lst3.json())


async def test_delete_missing_tenant_404(cp_client):
    atok = await admin_token(cp_client)
    r = await cp_client.delete("/admin/tenants/00000000-0000-0000-0000-000000000000",
                               headers=bearer(atok))
    assert r.status_code == 404
