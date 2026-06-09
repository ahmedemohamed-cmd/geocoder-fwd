"""Provision Zitadel for the billing console (idempotent).

Creates the project, the two roles (admin / tenant_user), and a public SPA
OIDC client (PKCE, no secret) for the React frontend, then writes the SPA's
runtime config (issuer + clientId + projectId) so the frontend can be built
once and configured at deploy time.

Run as a one-shot init container after Zitadel is up. Auth uses a service-account
PAT (created by Zitadel FirstInstance) supplied via ZITADEL_SERVICE_TOKEN or a
file at ZITADEL_PAT_FILE.

Env:
  ZITADEL_API_URL        base URL (default http://localhost:8085)
  ZITADEL_SERVICE_TOKEN  PAT, or
  ZITADEL_PAT_FILE       path to a file containing the PAT
  SPA_REDIRECT_URIS      comma-separated redirect URIs
  SPA_LOGOUT_URIS        comma-separated post-logout redirect URIs
  PROVISION_OUT          where to write config.json (default ./billing/frontend/public/runtime/config.json)
  ADMIN_LOGIN_NAME       bootstrap human admin to grant the `admin` role
"""
from __future__ import annotations

import json
import os
import sys
import time

import httpx

API = os.getenv("ZITADEL_API_URL", "http://localhost:8085").rstrip("/")
OUT = os.getenv("PROVISION_OUT", "billing/frontend/public/runtime/config.json")
REDIRECTS = os.getenv("SPA_REDIRECT_URIS",
                      "http://localhost:8088/callback,http://localhost:8088/").split(",")
LOGOUTS = os.getenv("SPA_LOGOUT_URIS", "http://localhost:8088/").split(",")
ADMIN_LOGIN = os.getenv("ADMIN_LOGIN_NAME", "zitadel-admin@zitadel.localhost")


def _token() -> str:
    tok = os.getenv("ZITADEL_SERVICE_TOKEN", "")
    if not tok and os.getenv("ZITADEL_PAT_FILE"):
        with open(os.environ["ZITADEL_PAT_FILE"]) as f:
            tok = f.read().strip()
    if not tok:
        sys.exit("no Zitadel PAT (set ZITADEL_SERVICE_TOKEN or ZITADEL_PAT_FILE)")
    return tok


def _wait_ready(c: httpx.Client, tries: int = 60) -> None:
    for _ in range(tries):
        try:
            if c.get(f"{API}/.well-known/openid-configuration").status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(2)
    sys.exit("Zitadel did not become ready")


def _post(c, path, body):
    r = c.post(f"{API}{path}", json=body)
    if r.status_code >= 300 and "already" not in r.text.lower():
        # 409/AlreadyExists is fine (idempotent); anything else is fatal context
        if r.status_code not in (409,):
            print(f"WARN {path} -> {r.status_code} {r.text[:200]}")
    return r


def main() -> None:
    pat = _token()
    headers = {"Authorization": f"Bearer {pat}", "Content-Type": "application/json"}
    host_override = os.getenv("ZITADEL_HOST_HEADER", "")
    if host_override:
        headers["Host"] = host_override  # resolve to the instance via service name
    with httpx.Client(headers=headers, timeout=30) as c:
        _wait_ready(c)
        issuer = c.get(f"{API}/.well-known/openid-configuration").json()["issuer"]

        # 0) Zitadel v4 defaults to "Login UI v2", which is a separate service we
        # don't run; disable it so OIDC uses the built-in login UI (/ui/login).
        r = c.put(f"{API}/v2/features/instance", json={"loginV2": {"required": False}})
        print("loginV2 disabled:", r.status_code)

        # 1) project (assert project roles into tokens)
        r = _post(c, "/management/v1/projects",
                  {"name": "geocoder-billing", "projectRoleAssertion": True,
                   "projectRoleCheck": False, "hasProjectCheck": False})
        project_id = _find_project_id(c, r)
        print("project:", project_id)

        # 2) roles
        for key, name in (("admin", "Platform Admin"), ("tenant_user", "Tenant User")):
            _post(c, f"/management/v1/projects/{project_id}/roles",
                  {"roleKey": key, "displayName": name, "group": "billing"})

        # 3) public SPA OIDC client (PKCE, no secret)
        client_id = _ensure_spa(c, project_id)
        print("clientId:", client_id)

        # 4) best-effort: grant the bootstrap human admin the `admin` role
        _grant_admin(c, project_id)

        # 4b) login policy: authenticator-app TOTP is the only MFA (drop U2F +
        # passwordless); email/SMS OTP are already off by default.
        _configure_login_policy(c)

        # 5) write SPA runtime config (incl. the API/gateway base URLs the SPA calls)
        cfg = {"issuer": issuer, "clientId": client_id, "projectId": project_id,
               "scope": f"openid profile email urn:zitadel:iam:org:project:id:{project_id}:aud "
                        f"urn:zitadel:iam:user:metadata",
               "apiBase": os.getenv("API_BASE", "http://localhost:8100"),
               "gatewayBase": os.getenv("GATEWAY_BASE", "http://localhost:8080")}
        os.makedirs(os.path.dirname(OUT), exist_ok=True)
        with open(OUT, "w") as f:
            json.dump(cfg, f, indent=2)
        print("wrote", OUT, "->", cfg)


def _find_project_id(c, create_resp) -> str:
    if create_resp.status_code < 300:
        return create_resp.json()["id"]
    # already exists → look it up by name
    r = c.post("{}/management/v1/projects/_search".format(API),
               json={"queries": [{"nameQuery": {"name": "geocoder-billing",
                     "method": "TEXT_QUERY_METHOD_EQUALS"}}]})
    return r.json()["result"][0]["id"]


def _ensure_spa(c, project_id) -> str:
    body = {
        "name": "billing-console",
        "redirectUris": [u.strip() for u in REDIRECTS if u.strip()],
        "postLogoutRedirectUris": [u.strip() for u in LOGOUTS if u.strip()],
        "responseTypes": ["OIDC_RESPONSE_TYPE_CODE"],
        "grantTypes": ["OIDC_GRANT_TYPE_AUTHORIZATION_CODE"],
        "appType": "OIDC_APP_TYPE_USER_AGENT",
        "authMethodType": "OIDC_AUTH_METHOD_TYPE_NONE",
        "version": "OIDC_VERSION_1_0",
        "devMode": True,
        "accessTokenType": "OIDC_TOKEN_TYPE_JWT",
        "accessTokenRoleAssertion": True,
        "idTokenRoleAssertion": True,
        "idTokenUserinfoAssertion": True,
    }
    r = c.post(f"{API}/management/v1/projects/{project_id}/apps/oidc", json=body)
    if r.status_code < 300:
        return r.json()["clientId"]
    # already exists → find it
    apps = c.post(f"{API}/management/v1/projects/{project_id}/apps/_search",
                  json={"queries": [{"nameQuery": {"name": "billing-console",
                        "method": "TEXT_QUERY_METHOD_EQUALS"}}]}).json()
    app_id = apps["result"][0]["id"]
    detail = c.get(f"{API}/management/v1/projects/{project_id}/apps/{app_id}").json()
    return detail["app"]["oidcConfig"]["clientId"]


def _configure_login_policy(c) -> None:
    """Authenticator-app TOTP as the only second factor; no U2F/passkey/passwordless."""
    try:
        # remove U2F second + multi factor (idempotent; 404/Method-Not-Allowed ok)
        c.delete(f"{API}/admin/v1/policies/login/second_factors/SECOND_FACTOR_TYPE_U2F")
        c.delete(f"{API}/admin/v1/policies/login/multi_factors/"
                 f"MULTI_FACTOR_TYPE_U2F_WITH_VERIFICATION")
        # ensure TOTP present
        c.post(f"{API}/admin/v1/policies/login/second_factors",
               json={"type": "SECOND_FACTOR_TYPE_OTP"})
        # disable passwordless/passkey prompts
        c.put(f"{API}/admin/v1/policies/login", json={
            # registration disabled: only admin-provisioned users can log in
            "allowUsernamePassword": True, "allowRegister": False, "allowExternalIdp": True,
            "forceMfa": False, "forceMfaLocalOnly": False,
            "passwordlessType": "PASSWORDLESS_TYPE_NOT_ALLOWED",
            "hidePasswordReset": False, "ignoreUnknownUsernames": False,
            "allowDomainDiscovery": True, "passwordCheckLifetime": "864000s",
            "externalLoginCheckLifetime": "864000s", "mfaInitSkipLifetime": "2592000s",
            "secondFactorCheckLifetime": "64800s", "multiFactorCheckLifetime": "43200s",
            "defaultRedirectUri": ""})
        print("login policy: TOTP-only, passwordless disabled")
    except Exception as e:  # noqa: BLE001 - best effort
        print("WARN login policy config failed:", e)


def _grant_admin(c, project_id) -> None:
    try:
        users = c.post(f"{API}/management/v1/users/_search",
                       json={"queries": [{"userNameQuery": {"userName": ADMIN_LOGIN,
                             "method": "TEXT_QUERY_METHOD_EQUALS"}}]}).json()
        result = users.get("result") or []
        if not result:
            print("WARN admin user not found for grant:", ADMIN_LOGIN)
            return
        uid = result[0]["id"]
        c.post(f"{API}/management/v1/users/{uid}/grants",
               json={"projectId": project_id, "roleKeys": ["admin"]})
        print("granted admin role to", ADMIN_LOGIN)
    except Exception as e:  # noqa: BLE001 - best effort
        print("WARN admin grant failed:", e)


if __name__ == "__main__":
    main()
