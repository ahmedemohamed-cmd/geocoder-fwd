"""Configuration for the billing/API-key subsystem.

All values are environment-driven with localhost defaults so the system runs
against the existing docker-compose Postgres/Redis during development. The
subsystem uses its own Postgres database (``billing``) to keep its tables
isolated from the geocoding data.
"""
import os


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


# ── Postgres (control-plane source of truth) ───────────────────────────────
PG_HOST = os.getenv("BILLING_PG_HOST", os.getenv("POSTGRES_HOST", "localhost"))
PG_PORT = _int("BILLING_PG_PORT", _int("POSTGRES_PORT", 5432))
PG_DB = os.getenv("BILLING_PG_DB", "billing")
PG_USER = os.getenv("BILLING_PG_USER", os.getenv("POSTGRES_USER", "postgres"))
PG_PASSWORD = os.getenv("BILLING_PG_PASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres"))


def pg_dsn(database: str | None = None) -> str:
    db = database or PG_DB
    return f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{db}"


# ── Redis (live counters + key cache + event buffer) ───────────────────────
REDIS_HOST = os.getenv("BILLING_REDIS_HOST", os.getenv("REDIS_HOST", "localhost"))
REDIS_PORT = _int("BILLING_REDIS_PORT", _int("REDIS_PORT", 6379))
REDIS_DB = _int("BILLING_REDIS_DB", 1)  # separate logical db from the geocoder cache

# Redis keys (shared across all gateway replicas → cluster-wide state)
KEYCACHE_PREFIX = "apikey:"          # apikey:<hash> -> json{key_id,tenant_id,status}
LIVE_TENANT_PREFIX = "live:tenant:"  # live:tenant:<tenant>:<period> -> int
LIVE_KEY_PREFIX = "live:key:"        # live:key:<key_id>:<period>    -> int
USAGE_EVENTS_LIST = "usage:events"   # durable buffer drained into Postgres rollups
KEYCACHE_TTL = _int("BILLING_KEYCACHE_TTL", 300)  # seconds; cache miss falls back to PG

# ── Auth ───────────────────────────────────────────────────────────────────
# AUTH_MODE selects the token verifier:
#   "dev"     - local HS256 issuer (security.issue_token) + the `users` table.
#   "zitadel" - verify Zitadel OIDC tokens (RS256) against the Zitadel JWKS.
# Identity (role + tenant_id) is mapped to the same shape in both modes, so the
# control plane / gateway are auth-mode agnostic.
AUTH_MODE = os.getenv("BILLING_AUTH_MODE", "dev").strip().lower()

# Dev issuer.
JWT_SECRET = os.getenv("BILLING_JWT_SECRET", "dev-secret-change-me-please-32bytes-min!!")
JWT_ALG = os.getenv("BILLING_JWT_ALG", "HS256")
JWT_TTL_SECONDS = _int("BILLING_JWT_TTL", 3600)
JWT_ISSUER = os.getenv("BILLING_JWT_ISSUER", "billing-dev")

# Zitadel (production IdP).
ZITADEL_ISSUER = os.getenv("ZITADEL_ISSUER", "http://localhost:8085").rstrip("/")
ZITADEL_JWKS_URL = os.getenv("ZITADEL_JWKS_URL", f"{ZITADEL_ISSUER}/oauth/v2/keys")
# Zitadel routes by Host header (the instance external domain). When containers
# reach Zitadel via its service name (e.g. http://zitadel:8080) the Host must be
# overridden to the external domain so the request resolves to the instance.
ZITADEL_HOST_HEADER = os.getenv("ZITADEL_HOST_HEADER", "")
# Audience to require in the token (the SPA client/project id). Empty = skip aud.
ZITADEL_AUDIENCE = os.getenv("ZITADEL_AUDIENCE", "")
ZITADEL_PROJECT_ID = os.getenv("ZITADEL_PROJECT_ID", "")
# Zitadel role-grant claim and our two role keys within the project.
ZITADEL_ROLES_CLAIM = "urn:zitadel:iam:org:project:roles"
ZITADEL_METADATA_CLAIM = "urn:zitadel:iam:user:metadata"
# Metadata key (set per tenant user in Zitadel) that carries our tenant UUID.
ZITADEL_TENANT_METADATA_KEY = os.getenv("ZITADEL_TENANT_METADATA_KEY", "tenant_id")

# Service-account PAT used by the control plane to provision Zitadel users/metadata
# when AUTH_MODE=zitadel (optional; tenant provisioning falls back to no-op if unset).
ZITADEL_SERVICE_TOKEN = os.getenv("ZITADEL_SERVICE_TOKEN", "")
ZITADEL_API_URL = os.getenv("ZITADEL_API_URL", ZITADEL_ISSUER)

# Bootstrap platform admin (seeded on first start if no admin exists).
BOOTSTRAP_ADMIN_EMAIL = os.getenv("BILLING_ADMIN_EMAIL", "admin@example.com")
BOOTSTRAP_ADMIN_PASSWORD = os.getenv("BILLING_ADMIN_PASSWORD", "admin12345")

# ── Gateway (data plane) ────────────────────────────────────────────────────
# Upstream the (legacy custom) gateway proxies metered traffic to. The deployed
# data plane is Apache APISIX (see below); gateway.py is kept as a tested reference.
PROXY_TARGET = os.getenv("BILLING_PROXY_TARGET", "http://localhost:8000")
GATEWAY_API_KEY_HEADER = os.getenv("BILLING_API_KEY_HEADER", "X-API-Key")

# ── APISIX (deployed data plane) ─────────────────────────────────────────────
# When configured, the control plane manages APISIX consumers (key-auth) and
# per-tenant consumer-groups (limit-count/redis) via the Admin API. No-op when
# unset, so dev/tests are unaffected.
APISIX_ADMIN_URL = os.getenv("APISIX_ADMIN_URL", "")        # e.g. http://apisix:9180
APISIX_ADMIN_KEY = os.getenv("APISIX_ADMIN_KEY", "")
APISIX_KEY_HEADER = os.getenv("APISIX_KEY_HEADER", "X-API-Key")
APISIX_UPSTREAM = os.getenv("APISIX_UPSTREAM", "geocoder:8000")
APISIX_ROUTE_URI = os.getenv("APISIX_ROUTE_URI", "/*")
APISIX_ROUTE_ID = os.getenv("APISIX_ROUTE_ID", "geocoder")
# TTL (seconds) on the per-month limit-count key. The quota itself resets when
# the period-scoped key rolls over each calendar month (re-projected by the
# aggregator), so this only needs to outlast the longest month — 32 days of
# slack keeps a 31-day month's counter from expiring early.
APISIX_LIMIT_WINDOW = _int("APISIX_LIMIT_WINDOW", 2764800)

# Usage sink: APISIX http-logger posts request logs here; the control plane
# turns them into Redis live counters + durable events.
USAGE_SINK_URL = os.getenv("USAGE_SINK_URL", "")           # http://billing-control-plane:8100/internal/usage
USAGE_SINK_SECRET = os.getenv("USAGE_SINK_SECRET", "")

# Encryption-at-rest for API keys (Fernet). The gateway (APISIX) needs the key
# material to authenticate requests, so we keep a reversible encrypted copy to
# re-push on re-enable; the DB still never stores plaintext.
KEY_ENC_SECRET = os.getenv("BILLING_KEY_ENC_SECRET", "dev-key-encryption-secret-change-me")

ROLE_ADMIN = "admin"
ROLE_TENANT = "tenant_user"

# CORS: the SPA origin(s) allowed to call the control plane (comma-separated).
CORS_ORIGINS = [o.strip() for o in os.getenv(
    "BILLING_CORS_ORIGINS", "http://localhost:8088,http://127.0.0.1:8088").split(",") if o.strip()]
