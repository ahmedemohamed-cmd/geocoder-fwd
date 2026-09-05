"""Zitadel (RS256/JWKS) token verification + claim → identity mapping.

Uses a local RSA keypair to simulate Zitadel-signed tokens, so the whole
verification + mapping path is exercised offline (no running Zitadel needed).
"""

import base64
import time

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from billing import auth, config, security


@pytest.fixture
def rsa_keys():
    priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    priv_pem = priv.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    pub = priv.public_key()
    return priv_pem, pub


@pytest.fixture
def zitadel_mode(monkeypatch, rsa_keys):
    _, pub = rsa_keys
    monkeypatch.setattr(config, "AUTH_MODE", "zitadel")
    monkeypatch.setattr(config, "ZITADEL_AUDIENCE", "")  # skip aud for the test
    monkeypatch.setattr(security, "_zitadel_signing_key", lambda token: pub)
    yield


def _zitadel_token(priv_pem, *, roles, tenant_uuid=None, sub="user-1"):
    claims = {
        "sub": sub,
        "iss": config.ZITADEL_ISSUER,
        "iat": int(time.time()),
        "exp": int(time.time()) + 300,
        config.ZITADEL_ROLES_CLAIM: {r: {"orgid": "acme.localhost"} for r in roles},
    }
    if tenant_uuid is not None:
        enc = base64.b64encode(tenant_uuid.encode()).decode()
        claims[config.ZITADEL_METADATA_CLAIM] = {config.ZITADEL_TENANT_METADATA_KEY: enc}
    return jwt.encode(claims, priv_pem, algorithm="RS256")


def test_zitadel_admin_token_maps_to_admin(zitadel_mode, rsa_keys):
    priv, _ = rsa_keys
    tok = _zitadel_token(priv, roles=["admin"])
    ident = auth.identity_from_claims(security.decode_token(tok))
    assert ident.role == "admin"
    assert ident.tenant_id is None


def test_zitadel_tenant_token_maps_role_and_tenant(zitadel_mode, rsa_keys):
    priv, _ = rsa_keys
    tok = _zitadel_token(
        priv, roles=["tenant_user"], tenant_uuid="11111111-2222-3333-4444-555555555555"
    )
    ident = auth.identity_from_claims(security.decode_token(tok))
    assert ident.role == "tenant_user"
    assert ident.tenant_id == "11111111-2222-3333-4444-555555555555"


def test_zitadel_admin_wins_when_both_roles(zitadel_mode, rsa_keys):
    priv, _ = rsa_keys
    tok = _zitadel_token(priv, roles=["tenant_user", "admin"])
    ident = auth.identity_from_claims(security.decode_token(tok))
    assert ident.role == "admin"


def test_zitadel_no_billing_role_rejected(zitadel_mode, rsa_keys):
    priv, _ = rsa_keys
    tok = _zitadel_token(priv, roles=["some_other_role"])
    with pytest.raises(Exception):  # HTTPException 403 from identity mapping
        auth.identity_from_claims(security.decode_token(tok))


def test_zitadel_bad_signature_rejected(zitadel_mode):
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    other_pem = other.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    tok = _zitadel_token(other_pem, roles=["admin"])  # signed by the wrong key
    with pytest.raises(jwt.PyJWTError):
        security.decode_token(tok)


def test_dev_token_still_works_in_dev_mode():
    # default AUTH_MODE=dev: the local issuer path is unaffected
    tok = security.issue_token(subject="admin@x", role="admin", tenant_id=None)
    ident = auth.identity_from_claims(security.decode_token(tok))
    assert ident.role == "admin"
