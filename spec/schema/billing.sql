-- Billing schema specification.
--
-- THIS FILE IS SPECIFICATION, NOT A MIGRATION. It is the shape a fresh
-- deployment bootstraps to, applied idempotently by billing/db.py.
--
-- A regeneration must reproduce these tables exactly: column names and types
-- are the contract between the control plane, the aggregator, the billing
-- engine and every invoice already stored. Renaming a column here silently
-- orphans data that already exists.
CREATE TABLE IF NOT EXISTS plans (
    id                     TEXT PRIMARY KEY,
    name                   TEXT NOT NULL,
    monthly_quota          BIGINT NOT NULL DEFAULT 0,   -- included credits / month
    base_price_cents       BIGINT NOT NULL DEFAULT 0,   -- flat monthly fee
    overage_cents_per_unit NUMERIC NOT NULL DEFAULT 0,  -- cents per credit over quota
    hard_cap               BOOLEAN NOT NULL DEFAULT FALSE,
    rps                    INTEGER NOT NULL DEFAULT 0,  -- requests/second cap (0 = uncapped)
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tenants (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name          TEXT NOT NULL,
    contact_email TEXT,
    plan_id       TEXT REFERENCES plans(id),
    status        TEXT NOT NULL DEFAULT 'active',   -- active | suspended | deleted
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dev-mode auth. In production Zitadel owns identities; this table is replaced
-- and JWT verification switches to the Zitadel JWKS. The (role, tenant_id)
-- claim shape is identical, so nothing downstream changes.
CREATE TABLE IF NOT EXISTS users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email         TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role          TEXT NOT NULL,                    -- admin | tenant_user
    tenant_id     UUID REFERENCES tenants(id),
    status        TEXT NOT NULL DEFAULT 'active',   -- active | disabled
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_keys (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id  UUID NOT NULL REFERENCES tenants(id),
    name       TEXT NOT NULL,
    key_prefix TEXT NOT NULL,
    key_hash   TEXT NOT NULL UNIQUE,
    key_enc    TEXT,                                -- Fernet(full key) for APISIX re-push
    scopes     TEXT[] NOT NULL DEFAULT '{}',
    status     TEXT NOT NULL DEFAULT 'active',      -- active | disabled | deleted
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys(tenant_id);

-- Durable usage source of truth for billing (Redis holds the live counter).
CREATE TABLE IF NOT EXISTS usage_rollups (
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    key_id    UUID NOT NULL REFERENCES api_keys(id),
    period    TEXT NOT NULL,                        -- YYYY-MM
    day       DATE NOT NULL,
    endpoint  TEXT NOT NULL DEFAULT '',
    count     BIGINT NOT NULL DEFAULT 0,               -- milli-credits (1 credit = 1000)
    requests  BIGINT NOT NULL DEFAULT 0,               -- raw served requests
    PRIMARY KEY (tenant_id, key_id, day, endpoint)
);
CREATE INDEX IF NOT EXISTS idx_rollups_period ON usage_rollups(tenant_id, period);

CREATE TABLE IF NOT EXISTS invoices (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES tenants(id),
    period              TEXT NOT NULL,                   -- YYYY-MM
    total_requests      BIGINT NOT NULL DEFAULT 0,
    total_milli_credits BIGINT NOT NULL DEFAULT 0,
    amount_cents        BIGINT NOT NULL DEFAULT 0,
    line_items          JSONB NOT NULL DEFAULT '[]',
    status              TEXT NOT NULL DEFAULT 'pending', -- pending | paid
    generated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    paid_at             TIMESTAMPTZ,
    UNIQUE (tenant_id, period)
);

-- Billable cost of one request per endpoint (first path segment), in
-- milli-credits. Endpoints without a row cost weights.DEFAULT_WEIGHT_MILLI.
CREATE TABLE IF NOT EXISTS endpoint_weights (
    endpoint      TEXT PRIMARY KEY,
    milli_credits INTEGER NOT NULL CHECK (milli_credits >= 0),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One-time data migrations already applied (see _run_once).
CREATE TABLE IF NOT EXISTS schema_migrations (
    id         TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
