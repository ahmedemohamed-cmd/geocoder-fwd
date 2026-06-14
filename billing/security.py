"""Password hashing, API-key generation and JWT issue/verify.

API keys are shown to the customer exactly once at creation; only a SHA-256
hash is persisted, so the plaintext is never recoverable from the database.
"""

import base64
import hashlib
import hmac
import secrets
import time
from typing import Any

import jwt

from . import config

# ── passwords (dev-mode auth; Zitadel owns this in production) ──────────────
# stdlib PBKDF2-HMAC-SHA256; format: pbkdf2_sha256$<iters>$<salt_hex>$<hash_hex>
_PBKDF2_ITERS = 240_000


def hash_password(plain: str) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", plain.encode(), salt, _PBKDF2_ITERS)
    return f"pbkdf2_sha256${_PBKDF2_ITERS}${salt.hex()}${dk.hex()}"


def verify_password(plain: str, hashed: str) -> bool:
    try:
        algo, iters, salt_hex, hash_hex = hashed.split("$")
        if algo != "pbkdf2_sha256":
            return False
        dk = hashlib.pbkdf2_hmac("sha256", plain.encode(), bytes.fromhex(salt_hex), int(iters))
        return hmac.compare_digest(dk.hex(), hash_hex)
    except (ValueError, AttributeError):
        return False


# ── API keys ────────────────────────────────────────────────────────────────
_KEY_BYTES = 24


def generate_api_key() -> tuple[str, str, str]:
    """Return (full_key, prefix, sha256_hash).

    full_key  - shown to the customer once, e.g. ``gk_a1b2c3d4_<secret>``
    prefix    - stored & displayed for identification (never the secret)
    hash      - what we persist and look up by
    """
    secret = secrets.token_urlsafe(_KEY_BYTES)
    prefix = secrets.token_hex(4)
    full = f"gk_{prefix}_{secret}"
    return full, f"gk_{prefix}", hash_api_key(full)


def hash_api_key(full_key: str) -> str:
    return hashlib.sha256(full_key.encode("utf-8")).hexdigest()


# Reversible encryption of the key material (needed to (re)push to APISIX).
def _fernet():
    from cryptography.fernet import Fernet

    key = base64.urlsafe_b64encode(hashlib.sha256(config.KEY_ENC_SECRET.encode()).digest())
    return Fernet(key)


def encrypt_key(full_key: str) -> str:
    return _fernet().encrypt(full_key.encode()).decode()


def decrypt_key(token: str) -> str:
    return _fernet().decrypt(token.encode()).decode()


# ── JWT (dev issuer; verify is the only thing that changes for Zitadel) ─────
def issue_token(*, subject: str, role: str, tenant_id: str | None) -> str:
    now = int(time.time())
    payload: dict[str, Any] = {
        "sub": subject,
        "role": role,
        "tenant_id": tenant_id,
        "iss": config.JWT_ISSUER,
        "iat": now,
        "exp": now + config.JWT_TTL_SECONDS,
    }
    return jwt.encode(payload, config.JWT_SECRET, algorithm=config.JWT_ALG)


_jwks_by_kid: dict | None = None


def _fetch_jwks() -> dict:
    """Fetch + index the Zitadel JWKS by kid, sending the Host override so the
    request resolves to the right instance when called via the service name."""
    import httpx

    headers = {"Host": config.ZITADEL_HOST_HEADER} if config.ZITADEL_HOST_HEADER else {}
    r = httpx.get(config.ZITADEL_JWKS_URL, headers=headers, timeout=10)
    r.raise_for_status()
    return {jwk.key_id: jwk for jwk in jwt.PyJWKSet.from_dict(r.json()).keys}


def _zitadel_signing_key(token: str):
    """Resolve the RS256 signing key for a Zitadel token from its JWKS.

    Tests override this to return a local public key (no network)."""
    global _jwks_by_kid
    kid = jwt.get_unverified_header(token).get("kid")
    if _jwks_by_kid is None or kid not in _jwks_by_kid:
        _jwks_by_kid = _fetch_jwks()  # (re)fetch on cold start or key rotation
    if kid not in _jwks_by_kid:
        raise jwt.PyJWKClientError(f"no signing key for kid={kid}")
    return _jwks_by_kid[kid].key


def decode_token(token: str) -> dict[str, Any]:
    """Verify a bearer token according to AUTH_MODE and return its claims.

    Raises jwt.PyJWTError subclasses on invalid/expired tokens. The returned
    claims are normalised by auth.identity_from_claims, so dev and Zitadel
    tokens converge on the same {sub, role, tenant_id} identity.
    """
    if config.AUTH_MODE == "zitadel":
        key = _zitadel_signing_key(token)
        opts = {"require": ["exp", "sub"], "verify_aud": bool(config.ZITADEL_AUDIENCE)}
        return jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            issuer=config.ZITADEL_ISSUER,
            audience=config.ZITADEL_AUDIENCE or None,
            options=opts,
        )
    return jwt.decode(
        token,
        config.JWT_SECRET,
        algorithms=[config.JWT_ALG],
        issuer=config.JWT_ISSUER,
        options={"require": ["exp", "sub", "role"]},
    )
