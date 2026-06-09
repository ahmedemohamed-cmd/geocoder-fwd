"""Thin Zitadel management-API client used by the control plane.

When AUTH_MODE=zitadel, creating a tenant also provisions the tenant's initial
human user in Zitadel: a user is created, granted the `tenant_user` project
role, and tagged with `metadata[tenant_id] = <our tenant UUID>`. That metadata
is what comes back in the user's OIDC token and is mapped to Identity.tenant_id
by auth.identity_from_claims — closing the loop between Zitadel identities and
our tenant model.

If no service token is configured the client is a no-op, so dev mode and tests
are unaffected.
"""
from __future__ import annotations

import base64

import httpx

from . import config


class ZitadelError(Exception):
    pass


def enabled() -> bool:
    return config.AUTH_MODE == "zitadel" and bool(config.ZITADEL_SERVICE_TOKEN)


def _client() -> httpx.AsyncClient:
    headers = {"Authorization": f"Bearer {config.ZITADEL_SERVICE_TOKEN}",
               "Content-Type": "application/json"}
    if config.ZITADEL_HOST_HEADER:
        headers["Host"] = config.ZITADEL_HOST_HEADER
    return httpx.AsyncClient(base_url=config.ZITADEL_API_URL, headers=headers, timeout=30)


async def provision_tenant_user(*, email: str, password: str, tenant_id: str,
                                display_name: str) -> str | None:
    """Create the tenant's user in Zitadel, grant the role, set tenant metadata.
    Returns the Zitadel user id (or None if the client is disabled)."""
    if not enabled():
        return None
    first, _, last = display_name.partition(" ")
    async with _client() as c:
        # _import creates the user ACTIVE with a usable password and NO email
        # step. NOTE: Zitadel ties USER_STATE_INITIAL (and the first-login email
        # verification step) to an unverified email, so "active + no email step"
        # requires isEmailVerified=true — the two cannot both hold. We honour the
        # primary requirement (no email step) here.
        r = await c.post("/management/v1/users/human/_import", json={
            "userName": email,
            "profile": {"firstName": first or email, "lastName": last or "user"},
            "email": {"email": email, "isEmailVerified": True},
            "password": password,
            "passwordChangeRequired": False,
        })
        if r.status_code >= 300:
            raise ZitadelError(f"create user failed: {r.status_code} {r.text[:300]}")
        user_id = r.json().get("userId") or r.json().get("id")

        meta_val = base64.b64encode(tenant_id.encode()).decode()
        await c.post(f"/management/v1/users/{user_id}/metadata/"
                     f"{config.ZITADEL_TENANT_METADATA_KEY}", json={"value": meta_val})

        if config.ZITADEL_PROJECT_ID:
            await c.post(f"/management/v1/users/{user_id}/grants", json={
                "projectId": config.ZITADEL_PROJECT_ID, "roleKeys": [config.ROLE_TENANT]})
        return user_id
