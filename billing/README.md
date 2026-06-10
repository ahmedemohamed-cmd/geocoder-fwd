# API-key management, metering & billing

A self-contained subsystem that issues per-tenant API keys, meters request usage
in real time, generates monthly bills, and exposes admin + tenant reporting.

## Components (two planes)

| Component | Plane | Responsibility |
|---|---|---|
| `control_plane.py` | management | Admin: tenant + **plan** CRUD, view/mark-paid bills, run billing. Tenant: key CRUD (soft delete), enable/disable, **real-time usage**, history, invoices. Projects key/plan state into APISIX (`apisix_admin.py`) and hosts the `/internal/usage` sink. |
| **Apache APISIX** (+ etcd) | data | The deployed gateway: `key-auth` validates `X-API-Key` against per-key consumers; per-tenant **consumer-groups** carry `limit-count`(redis) for cluster-wide monthly hard quotas; `http-logger` streams usage to the sink. Config lives in etcd. |
| `apisix_admin.py` | management | Admin-API client: route + per-tenant consumer-groups + per-key consumers. |
| `aggregator.py` | batch | Drains the Redis usage-event list → Postgres rollups (its own service). |
| `usage.py` | shared | Redis live counters (real-time display) + durable event buffer + flush. |
| `billing_engine.py` | batch | Monthly invoice generation from rollups × plan pricing (idempotent per tenant/period). |
| `gateway.py` | data (reference) | The earlier self-contained FastAPI gateway, kept as a tested reference; **APISIX is the deployed data plane**. |
| `repo.py` / `db.py` | data | asyncpg data-access + schema/seed. `security.py` hashing/key-enc/JWT. `auth.py` role guards. |

**Quotas & metering.** A plan has a single monthly quota + overage price + hard-cap
flag. APISIX `limit-count`(redis) enforces the per-tenant hard quota in-line and
cluster-wide; its Redis key is scoped to the calendar period (`<group>:<YYYY-MM>`)
so the quota resets on the 1st in step with the Postgres rollups — the aggregator
re-projects each tenant's group when the month rolls over. The usage sink pushes
events to a Redis list that the aggregator
drains into **Postgres rollups** — the durable source of truth for both billing
**and** the usage display (so counts never reset when the cache is evicted).
Counting stays on Redis (not NATS). Enable/disable re-pushes the key to APISIX,
so keys are stored encrypted-at-rest (`key_enc`, Fernet) in addition to the
SHA-256 hash; the owning tenant can reveal/copy them.

**Admin** can CRUD plans, **add/disable/delete tenant users**, and **reset tenant
passwords** (Zitadel). Zitadel is configured for **TOTP-only MFA** with
**registration disabled**.

## Real-time usage & key control (the requirements)

- Tenant usage: `GET /usage/current` reads **durable Postgres rollups** (total +
  per key); refresh to update. (The shared geocoder Redis is non-persistent/LRU,
  so its live counters aren't used for display — they'd reset on eviction/restart.)
- Tenant enable/disable key: `PATCH /keys/{id} {status}` → cache refreshed so the
  gateway honors it within ms.
- Tenant key CRUD, delete is **soft** (`status=deleted`, row retained for audit).
- Admin tenant CRUD, sees tenant bills, marks paid: `/admin/tenants/*`,
  `/admin/tenants/{id}/invoices`, `/admin/invoices/{id}/pay`.

## Run (dev)

```bash
python -m venv .venv-billing && . .venv-billing/bin/activate
pip install -r billing/requirements.txt

# needs the docker-compose Postgres + Redis up
uvicorn billing.main:control_plane_app --port 8100   # management API + docs at /docs
# the deployed data plane is APISIX (see Full stack); gateway.py is a reference:
uvicorn billing.main:gateway_app       --port 8080   # optional reference gateway
```

Bootstrap seeds plans (`free`/`starter`/`pro`) and a platform admin
(`BILLING_ADMIN_EMAIL` / `BILLING_ADMIN_PASSWORD`). Config: see `config.py`
(`BILLING_*` env vars). Uses its own `billing` Postgres database.

## Tests

```bash
pip install -r billing/requirements.txt
pytest tests/billing          # 58 tests: auth/Zitadel, tenant/key/plan CRUD,
                              # gateway enforcement, quotas, scopes, aggregation,
                              # billing, APISIX consumer-mapping + usage sink
```

Tests run against the real Postgres (`billing_test` db) and in-process fakeredis.

## Full stack (docker-compose + Zitadel + React console)

`docker-compose.yaml` wires the whole subsystem:

| Service | Port | Role |
|---|---|---|
| `zitadel` + `zitadel-db` | 8085 | IdP (OIDC). FirstInstance seeds a human admin + a service-account PAT. |
| `billing-zitadel-init` | — | One-shot: provisions the project, the `admin`/`tenant_user` roles, the SPA OIDC client, the login policy (TOTP-only), then writes the SPA runtime config. |
| `etcd` + `apisix` | 8080 (proxy), 9180 (admin) | The metered data plane. APISIX proxies `:8080` → `geocoder:8000`; etcd holds its config. |
| `billing-control-plane` | 8100 | Management API (verifies Zitadel tokens); provisions APISIX consumers/groups; hosts the usage sink. |
| `billing-aggregator` | — | Drains the Redis usage-event list → Postgres rollups. |
| `billing-frontend` | 8088 | React/Vite console (nginx). |

```bash
docker compose up -d --build \
  zitadel-db zitadel etcd apisix billing-control-plane billing-aggregator billing-frontend
docker compose run --rm billing-zitadel-init   # provision Zitadel + write SPA config
# then open http://localhost:8088 and sign in via Zitadel
# bootstrap admin: zitadel-admin@zitadel.localhost / Password1!
# call the metered API:  curl http://localhost:8080/geocode?... -H "X-API-Key: <key>"
```

**Data plane (APISIX):** the control plane drives APISIX's Admin API — each API
key becomes a `key-auth` consumer; each tenant a consumer-group whose
`limit-count`(redis) is the cluster-wide monthly hard quota (hard-cap plans only;
soft plans are unlimited at the gateway and billed as overage). `http-logger`
posts served requests to `/internal/usage`, which writes Redis live counters +
the durable event list. Enable/disable add/remove the consumer (the key is
re-pushed from its encrypted-at-rest copy). The APISIX admin key + the sink token
are dev defaults in compose — override for real deployments.

The provisioner writes `clientId`/`projectId`/`issuer`/`scope` into the shared
`billing-frontend-config` volume, served at `/runtime/config.json`, so the SPA is
**built once and configured at deploy time** (no rebuild to change clients).

To let the control plane provision tenant *users* in Zitadel, pass the service
PAT + project id (the init writes the PAT to `billing/.zitadel-machinekey/pat.txt`):

```bash
ZITADEL_PROJECT_ID=<id> ZITADEL_SERVICE_TOKEN=<pat>   # e.g. via .env
```

**Networking note:** Zitadel routes by `Host` header (instance domain
`localhost:8085`). Browser traffic hits `localhost:8085` directly; container→Zitadel
calls (JWKS fetch, management API) target `http://zitadel:8080` with a
`Host: localhost:8085` override (`ZITADEL_HOST_HEADER`) so they resolve to the
instance. The token `iss` stays `http://localhost:8085` for both.

The console's auth view (login → role-based dashboard) requires the interactive
OIDC redirect, so it's exercised in a browser; the backend verification it relies
on is covered by `tests/billing/test_zitadel_auth.py` (RS256 + claim→identity
mapping against a local keypair) and a live JWKS-fetch check against the running
Zitadel.

## Stack notes

- **IAM = Zitadel.** `AUTH_MODE=zitadel` verifies RS256 tokens against the
  Zitadel JWKS; `AUTH_MODE=dev` uses a local HS256 issuer + the `users` table
  (tests only). Claim shape `{sub, role, tenant_id}` is identical in both.
- **Gateway = Apache APISIX + etcd** (the deployed data plane). `gateway.py`
  remains as a tested reference of the same enforcement/metering semantics.
- **Event buffer = Redis list** (the chosen design — counting stays on Redis,
  not NATS). The aggregator can be pointed at NATS later without touching the
  control plane.
- Tests: `pytest tests/billing` — 58 tests (auth/Zitadel-RS256, tenant/key/plan
  CRUD, gateway enforcement, quotas, scopes, aggregation, billing, APISIX
  consumer-mapping + usage sink). Real Postgres `billing_test` + fakeredis.
