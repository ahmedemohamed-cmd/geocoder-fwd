"""Configuration for the billing/API-key subsystem.

All values are environment-driven with localhost defaults so the system runs
against the existing docker-compose Postgres/Redis during development. The
subsystem uses its own Postgres database (``billing``) to keep its tables
isolated from the geocoding data.
"""

import os
import re


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

# Topology: "standalone" (default) or "cluster" (Redis Cluster). Cluster mode
# has no logical DBs (no SELECT), so REDIS_DB is ignored there and key isolation
# comes from REDIS_PREFIX instead — set BILLING_REDIS_PREFIX="billing:" when
# running against a cluster. Standalone keeps db=REDIS_DB and an empty prefix,
# so existing deployments see identical keys.
REDIS_MODE = os.getenv("BILLING_REDIS_MODE", os.getenv("REDIS_MODE", "standalone")).strip().lower()
REDIS_NODES = os.getenv("BILLING_REDIS_NODES", os.getenv("REDIS_NODES", ""))
REDIS_PREFIX = os.getenv("BILLING_REDIS_PREFIX", "")

# Redis keys (shared across all gateway replicas → cluster-wide state)
KEYCACHE_PREFIX = REDIS_PREFIX + "apikey:"  # apikey:<hash> -> json{key_id,tenant_id,status}
LIVE_TENANT_PREFIX = REDIS_PREFIX + "live:tenant:"  # live:tenant:<tenant>:<period> -> int
LIVE_KEY_PREFIX = REDIS_PREFIX + "live:key:"  # live:key:<key_id>:<period>    -> int
USAGE_EVENTS_LIST = REDIS_PREFIX + "usage:events"  # durable buffer drained into PG rollups
KEYCACHE_TTL = _int("BILLING_KEYCACHE_TTL", 300)  # seconds; cache miss falls back to PG


def redis_cluster_nodes() -> list[str]:
    """REDIS_NODES as a ['host:port', ...] list (APISIX limit-count needs this)."""
    out = []
    for item in REDIS_NODES.split(","):
        item = item.strip()
        if item:
            out.append(item if ":" in item else f"{item}:6379")
    return out or [f"{REDIS_HOST}:{REDIS_PORT}"]


# Cluster name required by APISIX's redis-cluster limit-count policy.
REDIS_CLUSTER_NAME = os.getenv(
    "BILLING_REDIS_CLUSTER_NAME", os.getenv("REDIS_CLUSTER_NAME", "geocoder-redis")
)

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


def _provision_project_id() -> str:
    """Read projectId from the config.json written by billing-zitadel-init.

    Falls back gracefully so dev/test runs without the volume still work.
    """
    if os.getenv("ZITADEL_PROJECT_ID"):
        return os.environ["ZITADEL_PROJECT_ID"]
    cfg_path = os.getenv("PROVISION_CONFIG_PATH", "/runtime/config.json")
    try:
        import json as _json

        with open(cfg_path) as _f:
            return _json.load(_f).get("projectId", "")
    except (OSError, ValueError):
        return ""


ZITADEL_PROJECT_ID = _provision_project_id()
# Zitadel role-grant claim and our two role keys within the project.
ZITADEL_ROLES_CLAIM = "urn:zitadel:iam:org:project:roles"
ZITADEL_METADATA_CLAIM = "urn:zitadel:iam:user:metadata"
# Metadata key (set per tenant user in Zitadel) that carries our tenant UUID.
ZITADEL_TENANT_METADATA_KEY = os.getenv("ZITADEL_TENANT_METADATA_KEY", "tenant_id")


# Service-account PAT used by the control plane to provision Zitadel users/metadata
# when AUTH_MODE=zitadel (optional; tenant provisioning falls back to no-op if unset).
def _service_token() -> str:
    if os.getenv("ZITADEL_SERVICE_TOKEN"):
        return os.environ["ZITADEL_SERVICE_TOKEN"]
    pat_file = os.getenv("ZITADEL_PAT_FILE", "")
    if pat_file:
        try:
            with open(pat_file) as _f:
                return _f.read().strip()
        except OSError:
            pass
    return ""


ZITADEL_SERVICE_TOKEN = _service_token()
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
APISIX_ADMIN_URL = os.getenv("APISIX_ADMIN_URL", "")  # e.g. http://apisix:9180
APISIX_ADMIN_KEY = os.getenv("APISIX_ADMIN_KEY", "")
APISIX_KEY_HEADER = os.getenv("APISIX_KEY_HEADER", "X-API-Key")
APISIX_UPSTREAM = os.getenv("APISIX_UPSTREAM", "geocoder:8000")
APISIX_ROUTE_URI = os.getenv("APISIX_ROUTE_URI", "/*")
APISIX_ROUTE_ID = os.getenv("APISIX_ROUTE_ID", "geocoder")
# Routing paths are handled by the geocoder (which proxies to Valhalla internally
# and adds Arabic narration support). Keep the env-var override for flexibility.
APISIX_VALHALLA_UPSTREAM = os.getenv("APISIX_VALHALLA_UPSTREAM", "geocoder:8000")
APISIX_VALHALLA_ROUTE_ID = os.getenv("APISIX_VALHALLA_ROUTE_ID", "valhalla")
# TTL (seconds) on the per-month limit-count key. The quota itself resets when
# the period-scoped key rolls over each calendar month (re-projected by the
# aggregator), so this only needs to outlast the longest month — 32 days of
# slack keeps a 31-day month's counter from expiring early.
APISIX_LIMIT_WINDOW = _int("APISIX_LIMIT_WINDOW", 2764800)

# Usage sink: APISIX http-logger posts request logs here; the control plane
# turns them into Redis live counters + durable events.
USAGE_SINK_URL = os.getenv("USAGE_SINK_URL", "")  # http://billing-control-plane:8100/internal/usage
USAGE_SINK_SECRET = os.getenv("USAGE_SINK_SECRET", "")

# ── free endpoints ────────────────────────────────────────────────────────────
# Billing rule: if an endpoint *answers a user query* we count it; otherwise it's
# free (never billed, never counted against quota). Free = liveness (/health,
# /status), capability discovery (/features), and contributory writes where the
# client hands data *to* us (/feedback, /insert, /places, /traffic/probe[s]).
#
# Matched on the FULL request path (query string ignored), NOT the first segment,
# so sibling paths can differ: /traffic/probe[s] (upload) is free while
# /traffic/edge (a live-speed query) bills. Override with a comma-separated
# BILLING_FREE_ENDPOINTS of full paths.


def norm_path(p: str) -> str:
    """Normalize a request path for free-endpoint matching: drop the query
    string, lowercase, collapse to a single leading slash, strip the trailing
    slash. '' and '/' both normalize to '/'."""
    return "/" + (p or "").split("?", 1)[0].strip().lower().strip("/")


FREE_ENDPOINTS = frozenset(
    norm_path(s)
    for s in os.getenv(
        "BILLING_FREE_ENDPOINTS",
        "/health,/status,/features,/feedback,/insert,/places,/traffic/probe,/traffic/probes,/nearby/categories",
    ).split(",")
    if s.strip()
)


def free_endpoints_regex() -> str:
    """Anchored exact-match regex over the request path, for APISIX
    ``_meta.filter``. Matches a request only when its path *is* a free endpoint
    (trailing slash tolerated), so /traffic/probes matches but /traffic/edge does
    not. Empty set → a regex that can never match (filter degrades to "count
    everything")."""
    if not FREE_ENDPOINTS:
        return r"(?!)"  # matches nothing
    alt = "|".join(re.escape(p) for p in sorted(FREE_ENDPOINTS))
    return rf"^({alt})/?$"


# Encryption-at-rest for API keys (Fernet). The gateway (APISIX) needs the key
# material to authenticate requests, so we keep a reversible encrypted copy to
# re-push on re-enable; the DB still never stores plaintext.
KEY_ENC_SECRET = os.getenv("BILLING_KEY_ENC_SECRET", "dev-key-encryption-secret-change-me")

ROLE_ADMIN = "admin"
ROLE_TENANT = "tenant_user"

# CORS: the SPA origin(s) allowed to call the control plane (comma-separated).
CORS_ORIGINS = [
    o.strip()
    for o in os.getenv("BILLING_CORS_ORIGINS", "http://localhost:8088,http://127.0.0.1:8088").split(
        ","
    )
    if o.strip()
]
