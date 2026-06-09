"""FastAPI auth dependencies.

The bearer token is a JWT with the Zitadel-compatible claim shape
``{sub, role, tenant_id}``. In dev/test our own issuer mints it (see
security.issue_token); in production Zitadel issues it and only decode_token's
verification config changes.
"""
import base64

import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from . import config, security
from .models import Identity

_bearer = HTTPBearer(auto_error=True)


def identity_from_claims(claims: dict) -> Identity:
    """Normalise dev and Zitadel token claims to {sub, role, tenant_id}.

    Dev tokens carry `role` / `tenant_id` directly. Zitadel tokens carry roles
    under the project-roles grant claim and the tenant UUID in user metadata.
    """
    if "role" in claims:  # dev issuer
        return Identity(sub=claims["sub"], role=claims["role"],
                        tenant_id=claims.get("tenant_id"))

    # Zitadel: role from the project-roles grant; admin wins if both present.
    roles = claims.get(config.ZITADEL_ROLES_CLAIM) or {}
    if config.ROLE_ADMIN in roles:
        role = config.ROLE_ADMIN
    elif config.ROLE_TENANT in roles:
        role = config.ROLE_TENANT
    else:
        raise HTTPException(status.HTTP_403_FORBIDDEN,
                            "token has no recognised billing role")

    tenant_id = None
    meta = claims.get(config.ZITADEL_METADATA_CLAIM) or {}
    raw = meta.get(config.ZITADEL_TENANT_METADATA_KEY)
    if raw:  # Zitadel base64-encodes metadata values
        try:
            tenant_id = base64.b64decode(raw).decode()
        except (ValueError, UnicodeDecodeError):
            tenant_id = raw
    return Identity(sub=claims["sub"], role=role, tenant_id=tenant_id)


def current_identity(creds: HTTPAuthorizationCredentials = Depends(_bearer)) -> Identity:
    try:
        claims = security.decode_token(creds.credentials)
    except jwt.PyJWTError as exc:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, f"invalid token: {exc}")
    return identity_from_claims(claims)


def require_admin(ident: Identity = Depends(current_identity)) -> Identity:
    if ident.role != config.ROLE_ADMIN:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "admin role required")
    return ident


def require_tenant(ident: Identity = Depends(current_identity)) -> Identity:
    if ident.role != config.ROLE_TENANT or not ident.tenant_id:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "tenant role required")
    return ident


def get_pool(request: Request):
    return request.app.state.pool


def get_redis(request: Request):
    return request.app.state.redis
