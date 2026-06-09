"""Admin password reset for tenant users (dev-auth path: updates the local
users table; in Zitadel mode it also resets the IdP password)."""
from conftest import admin_token, bearer, login, make_tenant


async def test_admin_lists_tenant_users(cp_client):
    tenant, _ = await make_tenant(cp_client, admin_email="owner@reset.io")
    atok = await admin_token(cp_client)
    r = await cp_client.get(f"/admin/tenants/{tenant['id']}/users", headers=bearer(atok))
    assert r.status_code == 200
    assert any(u["email"] == "owner@reset.io" and u["role"] == "tenant_user"
               for u in r.json())


async def test_admin_resets_password(cp_client):
    tenant, _ = await make_tenant(cp_client, admin_email="owner2@reset.io",
                                  admin_password="oldpassword1")
    atok = await admin_token(cp_client)
    # old password works
    assert (await cp_client.post("/auth/login",
            json={"email": "owner2@reset.io", "password": "oldpassword1"})).status_code == 200

    r = await cp_client.post(f"/admin/tenants/{tenant['id']}/reset-password",
                             headers=bearer(atok),
                             json={"email": "owner2@reset.io", "new_password": "BrandNew_99"})
    assert r.status_code == 204

    # new works, old no longer does
    assert (await cp_client.post("/auth/login",
            json={"email": "owner2@reset.io", "password": "BrandNew_99"})).status_code == 200
    assert (await cp_client.post("/auth/login",
            json={"email": "owner2@reset.io", "password": "oldpassword1"})).status_code == 401


async def test_reset_unknown_user_404(cp_client):
    tenant, _ = await make_tenant(cp_client, admin_email="owner3@reset.io")
    atok = await admin_token(cp_client)
    r = await cp_client.post(f"/admin/tenants/{tenant['id']}/reset-password",
                             headers=bearer(atok),
                             json={"email": "nobody@reset.io", "new_password": "whatever123"})
    assert r.status_code == 404


async def test_tenant_user_cannot_reset(cp_client):
    tenant, ttok = await make_tenant(cp_client, admin_email="owner4@reset.io")
    r = await cp_client.post(f"/admin/tenants/{tenant['id']}/reset-password",
                             headers=bearer(ttok),
                             json={"email": "owner4@reset.io", "new_password": "whatever123"})
    assert r.status_code == 403
