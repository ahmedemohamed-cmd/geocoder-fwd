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


class InvalidCurrentPassword(ZitadelError):
    """The current password supplied for a self-service change was wrong."""


def enabled() -> bool:
    return config.AUTH_MODE == "zitadel" and bool(config.ZITADEL_SERVICE_TOKEN)


def _client() -> httpx.AsyncClient:
    headers = {
        "Authorization": f"Bearer {config.ZITADEL_SERVICE_TOKEN}",
        "Content-Type": "application/json",
    }
    if config.ZITADEL_HOST_HEADER:
        headers["Host"] = config.ZITADEL_HOST_HEADER
    return httpx.AsyncClient(base_url=config.ZITADEL_API_URL, headers=headers, timeout=30)


async def provision_tenant_user(
    *, email: str, password: str, tenant_id: str, display_name: str
) -> str | None:
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
        r = await c.post(
            "/management/v1/users/human/_import",
            json={
                "userName": email,
                "profile": {"firstName": first or email, "lastName": last or "user"},
                "email": {"email": email, "isEmailVerified": True},
                "password": password,
                "passwordChangeRequired": False,
            },
        )
        if r.status_code < 300:
            user_id = r.json().get("userId") or r.json().get("id")
        elif r.status_code == 409 or "already exist" in r.text.lower():
            # An orphaned identity can survive a tenant deletion that failed
            # IdP cleanup. The local DB is the source of truth for email
            # ownership, so adopt the orphan: overwrite its password and fall
            # through to (re)set the tenant metadata and role grant.
            user_id = await _find_user_id(c, email)
            if not user_id:
                raise ZitadelError(f"create user failed: {r.status_code} {r.text[:300]}")
            pr = await c.post(
                f"/v2/users/{user_id}/password",
                json={"newPassword": {"password": password, "changeRequired": False}},
            )
            if pr.status_code >= 300:
                raise ZitadelError(
                    f"adopting existing user failed: {pr.status_code} {pr.text[:200]}"
                )
        else:
            raise ZitadelError(f"create user failed: {r.status_code} {r.text[:300]}")

        meta_val = base64.b64encode(tenant_id.encode()).decode()
        await c.post(
            f"/management/v1/users/{user_id}/metadata/{config.ZITADEL_TENANT_METADATA_KEY}",
            json={"value": meta_val},
        )

        if config.ZITADEL_PROJECT_ID:
            await c.post(
                f"/management/v1/users/{user_id}/grants",
                json={"projectId": config.ZITADEL_PROJECT_ID, "roleKeys": [config.ROLE_TENANT]},
            )
        return user_id


async def _find_user_id(c: httpx.AsyncClient, email: str) -> str | None:
    found = await c.post(
        "/management/v1/users/_search",
        json={
            "queries": [
                {"userNameQuery": {"userName": email, "method": "TEXT_QUERY_METHOD_EQUALS"}}
            ]
        },
    )
    result = (found.json().get("result") or []) if found.status_code < 300 else []
    return result[0]["id"] if result else None


async def set_user_active(*, email: str, active: bool) -> bool:
    """Deactivate/reactivate a user in Zitadel (no-op if disabled)."""
    if not enabled():
        return False
    async with _client() as c:
        uid = await _find_user_id(c, email)
        if not uid:
            raise ZitadelError(f"user {email!r} not found in the IdP")
        action = "_reactivate" if active else "_deactivate"
        r = await c.post(f"/management/v1/users/{uid}/{action}")
        if r.status_code >= 300 and "already" not in r.text.lower():
            raise ZitadelError(f"{action} failed: {r.status_code} {r.text[:200]}")
        return True


async def delete_user(*, email: str) -> bool:
    """Delete a user from Zitadel (no-op if disabled / already gone)."""
    if not enabled():
        return False
    async with _client() as c:
        uid = await _find_user_id(c, email)
        if not uid:
            return False
        await c.delete(f"/management/v1/users/{uid}")
        return True


async def provision_admin_user(*, email: str, password: str, display_name: str) -> str | None:
    """Create a platform-admin login in Zitadel: active user granted the `admin`
    project role (no tenant metadata). No-op if disabled."""
    if not enabled():
        return None
    first, _, last = display_name.partition(" ")
    async with _client() as c:
        r = await c.post(
            "/management/v1/users/human/_import",
            json={
                "userName": email,
                "profile": {"firstName": first or email, "lastName": last or "admin"},
                "email": {"email": email, "isEmailVerified": True},
                "password": password,
                "passwordChangeRequired": False,
            },
        )
        if r.status_code >= 300:
            raise ZitadelError(f"create admin failed: {r.status_code} {r.text[:300]}")
        user_id = r.json().get("userId") or r.json().get("id")
        if config.ZITADEL_PROJECT_ID:
            await c.post(
                f"/management/v1/users/{user_id}/grants",
                json={"projectId": config.ZITADEL_PROJECT_ID, "roleKeys": [config.ROLE_ADMIN]},
            )
        return user_id


async def set_user_password(*, email: str, password: str) -> bool:
    """Admin reset: set a user's password in Zitadel (no-op if disabled)."""
    if not enabled():
        return False
    async with _client() as c:
        found = await c.post(
            "/management/v1/users/_search",
            json={
                "queries": [
                    {"userNameQuery": {"userName": email, "method": "TEXT_QUERY_METHOD_EQUALS"}}
                ]
            },
        )
        result = (found.json().get("result") or []) if found.status_code < 300 else []
        if not result:
            raise ZitadelError(f"user {email!r} not found in the IdP")
        uid = result[0]["id"]
        r = await c.post(
            f"/v2/users/{uid}/password",
            json={"newPassword": {"password": password, "changeRequired": False}},
        )
        if r.status_code >= 300:
            raise ZitadelError(f"set password failed: {r.status_code} {r.text[:200]}")
        return True


async def change_own_password(*, user_id: str, old_password: str, new_password: str) -> bool:
    """Self-service password change: set the password, verifying the caller's
    current password (Zitadel rejects the call if it doesn't match). Raises
    InvalidCurrentPassword on a bad current password, ZitadelError otherwise.
    No-op (returns False) if the IdP client is disabled (dev mode)."""
    if not enabled():
        return False
    async with _client() as c:
        r = await c.post(
            f"/v2/users/{user_id}/password",
            json={
                "newPassword": {"password": new_password, "changeRequired": False},
                "currentPassword": old_password,
            },
        )
        if r.status_code < 300:
            return True
        if r.status_code in (400, 403):
            msg = r.text.lower()
            # Distinguish a new password rejected by the complexity policy from a
            # wrong current password (both surface as 400).
            if any(w in msg for w in ("complex", "policy", "minimum", "length")):
                raise ZitadelError(f"new password rejected by policy: {r.text[:200]}")
            raise InvalidCurrentPassword("current password is incorrect")
        raise ZitadelError(f"password change failed: {r.status_code} {r.text[:200]}")


async def _remove_totp(c: httpx.AsyncClient, user_id: str) -> bool:
    """Remove the user's TOTP factor. Treats 'not enrolled' as success."""
    r = await c.delete(f"/v2/users/{user_id}/totp")
    if r.status_code < 300:
        return True
    if r.status_code == 404 and "doesn't exist" in r.text.lower():
        return True
    raise ZitadelError(f"MFA reset failed: {r.status_code} {r.text[:200]}")


async def reset_user_mfa(*, user_id: str) -> bool:
    """Self-service: remove the caller's own TOTP so they re-enroll at next login.
    No-op (returns False) if the IdP client is disabled."""
    if not enabled():
        return False
    async with _client() as c:
        return await _remove_totp(c, user_id)


async def reset_user_mfa_by_email(*, email: str) -> bool:
    """Admin: remove a user's TOTP (looked up by email). No-op if disabled."""
    if not enabled():
        return False
    async with _client() as c:
        uid = await _find_user_id(c, email)
        if not uid:
            raise ZitadelError(f"user {email!r} not found in the IdP")
        return await _remove_totp(c, uid)
