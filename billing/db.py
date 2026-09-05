"""asyncpg pool, schema DDL and idempotent seeding."""

import logging
from decimal import Decimal

import asyncpg

from . import config, security, weights
from .spec import SPEC_DIR as _SPEC_DIR
from .spec import load as _load_spec

_log = logging.getLogger("billing.db")

SCHEMA = (_SPEC_DIR / "schema" / "billing.sql").read_text(encoding="utf-8")

DEFAULT_PLANS = [
    (
        p["id"],
        p["name"],
        p["quota_credits"],
        p["base_price_cents"],
        p["overage_cents_per_credit"],
        p["hard_cap"],
        p["rps"],
    )
    for p in _load_spec()["plans"]
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
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'"
        )
        await conn.execute(
            "ALTER TABLE usage_rollups ADD COLUMN IF NOT EXISTS requests BIGINT NOT NULL DEFAULT 0"
        )
        await conn.execute(
            "ALTER TABLE invoices "
            "ADD COLUMN IF NOT EXISTS total_milli_credits BIGINT NOT NULL DEFAULT 0"
        )
        # Key names must be unique per tenant (soft-deleted keys excluded, so a
        # name can be reused after deletion). Best-effort: a pre-existing database
        # with duplicate names would make the index build fail — log and continue
        # so startup isn't blocked; the application-level check still applies.
        try:
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_api_keys_tenant_name "
                "ON api_keys (tenant_id, name) WHERE status <> 'deleted'"
            )
        except asyncpg.PostgresError as e:
            _log.warning(
                "could not create unique index on (tenant_id, name) — "
                "deduplicate existing api_keys names to enable it: %s",
                e,
            )
        await conn.execute(
            "ALTER TABLE plans ADD COLUMN IF NOT EXISTS rps INTEGER NOT NULL DEFAULT 0"
        )
        await _run_once(conn, "2026-07-credit-units", _migrate_credit_units)
        await _run_once(conn, "2026-07-leak-fixes", _migrate_leak_fixes)


async def _run_once(conn, mig_id: str, fn) -> None:
    """Run a one-time data migration exactly once across all replicas: the
    schema_migrations insert claims the id under an advisory lock, and the
    body runs in the same transaction only when the claim succeeds."""
    async with conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(hashtext('billing_schema_migrations'))")
        claimed = await conn.fetchval(
            "INSERT INTO schema_migrations (id) VALUES ($1) ON CONFLICT DO NOTHING RETURNING id",
            mig_id,
        )
        if claimed:
            await fn(conn)
            _log.info("applied one-time migration %s", mig_id)


async def _migrate_credit_units(conn) -> None:
    """Switch metering units from raw requests to credits (2026-07 repricing).

    Existing rollup counts are raw requests; convert 1 request = 1 credit
    (= 1000 milli). Per-endpoint retro-weighting from the endpoint column would
    be possible but isn't worth it for one partial month. Pre-existing invoices
    keep their request-based semantics (total_milli_credits stays 0)."""
    for pid, name, quota, base, overage, hard, _rps in DEFAULT_PLANS:
        if pid == "scale":
            continue  # new tier — seed_plans inserts it
        await conn.execute(
            """UPDATE plans SET name=$2, monthly_quota=$3, base_price_cents=$4,
                                overage_cents_per_unit=$5, hard_cap=$6 WHERE id=$1""",
            pid,
            name,
            quota,
            base,
            Decimal(str(overage)),  # exact decimal, not the float's binary expansion
            hard,
        )
    await conn.execute(
        "UPDATE usage_rollups SET requests = count, count = count * 1000 WHERE requests = 0"
    )


async def _migrate_leak_fixes(conn) -> None:
    """2026-07 repricing to the OSM-provider band + pricing-leak fixes: bigger
    quotas at the same base prices, cheaper overage, per-plan rps caps, LLM
    `describe` repriced above its GPU cost, and the per-element matrix rate.
    Overwrites the live rows deliberately — this ships a pricing decision, not
    a seed."""
    for pid, name, quota, base, overage, hard, rps in DEFAULT_PLANS:
        await conn.execute(
            """UPDATE plans SET name=$2, monthly_quota=$3, base_price_cents=$4,
                                overage_cents_per_unit=$5, hard_cap=$6, rps=$7 WHERE id=$1""",
            pid,
            name,
            quota,
            base,
            Decimal(str(overage)),
            hard,
            rps,
        )
    await conn.execute(
        """INSERT INTO endpoint_weights (endpoint, milli_credits) VALUES ('describe', 25000)
           ON CONFLICT (endpoint)
           DO UPDATE SET milli_credits = EXCLUDED.milli_credits, updated_at = now()"""
    )
    await conn.execute(
        """INSERT INTO endpoint_weights (endpoint, milli_credits) VALUES ($1, $2)
           ON CONFLICT (endpoint) DO NOTHING""",
        weights.MATRIX_ELEMENT_KEY,
        weights.DEFAULT_MATRIX_ELEMENT_MILLI,
    )


async def seed_plans(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        for pid, name, quota, base, overage, hard, rps in DEFAULT_PLANS:
            await conn.execute(
                """INSERT INTO plans
                       (id, name, monthly_quota, base_price_cents, overage_cents_per_unit,
                        hard_cap, rps)
                   VALUES ($1,$2,$3,$4,$5,$6,$7)
                   ON CONFLICT (id) DO NOTHING""",
                pid,
                name,
                quota,
                base,
                Decimal(str(overage)),  # exact decimal, not the float's binary expansion
                hard,
                rps,
            )


async def seed_weights(pool: asyncpg.Pool) -> None:
    """Insert the default per-endpoint credit weights; never overwrites an
    admin's edits (ON CONFLICT DO NOTHING)."""
    async with pool.acquire() as conn:
        for endpoint, milli in weights.DEFAULT_WEIGHTS.items():
            await conn.execute(
                """INSERT INTO endpoint_weights (endpoint, milli_credits)
                   VALUES ($1,$2) ON CONFLICT (endpoint) DO NOTHING""",
                endpoint,
                milli,
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
    await seed_weights(pool)
    await seed_admin(pool)


async def drop_all(pool: asyncpg.Pool) -> None:
    """Test helper: wipe all subsystem tables."""
    async with pool.acquire() as conn:
        await conn.execute(
            "DROP TABLE IF EXISTS invoices, usage_rollups, api_keys, users, tenants, plans, "
            "endpoint_weights, schema_migrations CASCADE"
        )
