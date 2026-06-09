"""Tenant API-key CRUD: create (plaintext once), enable/disable, soft delete,
tenant isolation."""
from conftest import bearer, create_key, make_tenant


async def test_create_key_returns_plaintext_once(cp_client):
    _, ttok = await make_tenant(cp_client, admin_email="k1@acme.test")
    created = await create_key(cp_client, ttok, name="prod")
    assert created["api_key"].startswith("gk_")
    assert created["key_prefix"] in created["api_key"]
    assert created["status"] == "active"

    # listing exposes the full key to the owner (view/copy any time)
    lst = await cp_client.get("/keys", headers=bearer(ttok))
    assert lst.status_code == 200
    rows = lst.json()
    assert len(rows) == 1
    assert rows[0]["id"] == created["id"]
    assert rows[0]["api_key"] == created["api_key"]  # revealable from key_enc


async def test_enable_disable_key(cp_client):
    _, ttok = await make_tenant(cp_client, admin_email="k2@acme.test")
    created = await create_key(cp_client, ttok)
    kid = created["id"]

    dis = await cp_client.patch(f"/keys/{kid}", headers=bearer(ttok),
                                json={"status": "disabled"})
    assert dis.status_code == 200 and dis.json()["status"] == "disabled"

    en = await cp_client.patch(f"/keys/{kid}", headers=bearer(ttok),
                               json={"status": "active"})
    assert en.json()["status"] == "active"


async def test_soft_delete_key(cp_client):
    _, ttok = await make_tenant(cp_client, admin_email="k3@acme.test")
    created = await create_key(cp_client, ttok)
    kid = created["id"]

    d = await cp_client.delete(f"/keys/{kid}", headers=bearer(ttok))
    assert d.status_code == 204

    # gone from listing and from GET, but row still exists in DB (soft)
    assert (await cp_client.get(f"/keys/{kid}", headers=bearer(ttok))).status_code == 404
    assert (await cp_client.get("/keys", headers=bearer(ttok))).json() == []


async def test_soft_delete_keeps_row_in_db(cp_client, pool):
    _, ttok = await make_tenant(cp_client, admin_email="k3b@acme.test")
    created = await create_key(cp_client, ttok)
    await cp_client.delete(f"/keys/{created['id']}", headers=bearer(ttok))
    row = await pool.fetchrow("SELECT status, deleted_at FROM api_keys WHERE id=$1",
                              created["id"])
    assert row["status"] == "deleted"
    assert row["deleted_at"] is not None


async def test_tenant_isolation(cp_client):
    _, tokA = await make_tenant(cp_client, name="A", admin_email="iso-a@acme.test")
    _, tokB = await make_tenant(cp_client, name="B", admin_email="iso-b@acme.test")
    keyA = await create_key(cp_client, tokA, name="a-key")

    # B cannot see or mutate A's key
    assert (await cp_client.get(f"/keys/{keyA['id']}", headers=bearer(tokB))).status_code == 404
    assert (await cp_client.patch(f"/keys/{keyA['id']}", headers=bearer(tokB),
            json={"status": "disabled"})).status_code == 404
    assert (await cp_client.delete(f"/keys/{keyA['id']}", headers=bearer(tokB))).status_code == 404
    assert (await cp_client.get("/keys", headers=bearer(tokB))).json() == []
