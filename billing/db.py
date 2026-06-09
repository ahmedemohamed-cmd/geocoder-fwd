"""asyncpg pool, schema DDL and idempotent seeding."""
import asyncpg

from . import config, security

SCHEMA = """
CREATE TABLE IF NOT EXISTS plans (
    id                     TEXT PRIMARY KEY,
    name                   TEXT NOT NULL,
    monthly_quota          BIGINT NOT NULL DEFAULT 0,   -- included requests / month
    base_price_cents       BIGINT NOT NULL DEFAULT 0,   -- flat monthly fee
    overage_cents_per_unit NUMERIC NOT NULL DEFAULT 0,  -- price per request over quota
    hard_cap               BOOLEAN NOT NULL DEFAULT FALSE,
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
    count     BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, key_id, day, endpoint)
);
CREATE INDEX IF NOT EXISTS idx_rollups_period ON usage_rollups(tenant_id, period);

CREATE TABLE IF NOT EXISTS invoices (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      UUID NOT NULL REFERENCES tenants(id),
    period         TEXT NOT NULL,                   -- YYYY-MM
    total_requests BIGINT NOT NULL DEFAULT 0,
    amount_cents   BIGINT NOT NULL DEFAULT 0,
    line_items     JSONB NOT NULL DEFAULT '[]',
    status         TEXT NOT NULL DEFAULT 'pending', -- pending | paid
    generated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    paid_at        TIMESTAMPTZ,
    UNIQUE (tenant_id, period)
);
"""

DEFAULT_PLANS = [
    # id,       name,       quota,   base_cents, overage_per_unit, hard_cap
    ("free",    "Free",        1000,        0,   0.0,   True),
    ("starter", "Starter",    50000,     2900,   0.05,  False),
    ("pro",     "Pro",      1000000,    29900,   0.02,  False),
]


async def ensure_database() -> None:
    """Create the billing database if it does not exist (connects to the
    maintenance `postgres` db). Safe to call repeatedly."""
    if config.PG_DB == "postgres":
        return
    conn = await asyncpg.connect(config.pg_dsn("postgres"))
    try:
        exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", config.PG_DB)
        if not exists:
            await conn.execute(f'CREATE DATABASE "{config.PG_DB}"')
    finally:
        await conn.close()


async def create_pool(dsn: str | None = None) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn or config.pg_dsn(), min_size=1, max_size=10)


async def init_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA)
        # idempotent migrations for pre-existing databases
        await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS key_enc TEXT")
        await conn.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'")


async def seed_plans(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        for pid, name, quota, base, overage, hard in DEFAULT_PLANS:
            await conn.execute(
                """INSERT INTO plans
                       (id, name, monthly_quota, base_price_cents, overage_cents_per_unit, hard_cap)
                   VALUES ($1,$2,$3,$4,$5,$6)
                   ON CONFLICT (id) DO NOTHING""",
                pid, name, quota, base, overage, hard,
            )


async def seed_admin(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM users WHERE role='admin' LIMIT 1")
        if not exists:
            await conn.execute(
                "INSERT INTO users (email, password_hash, role) VALUES ($1,$2,'admin')",
                config.BOOTSTRAP_ADMIN_EMAIL,
                security.hash_password(config.BOOTSTRAP_ADMIN_PASSWORD),
            )


async def bootstrap(pool: asyncpg.Pool) -> None:
    await init_schema(pool)
    await seed_plans(pool)
    await seed_admin(pool)


async def drop_all(pool: asyncpg.Pool) -> None:
    """Test helper: wipe all subsystem tables."""
    async with pool.acquire() as conn:
        await conn.execute(
            "DROP TABLE IF EXISTS invoices, usage_rollups, api_keys, users, tenants, plans CASCADE"
        )
