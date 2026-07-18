"""Data-access layer (Postgres via asyncpg).

All functions take a connection or pool. UUID/date values are stringified at the
boundary so the API layer can hand records straight to pydantic.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import asyncpg

from . import security


def _row(record: asyncpg.Record | None) -> dict[str, Any] | None:
    if record is None:
        return None
    out = dict(record)
    for k, v in list(out.items()):
        if hasattr(v, "hex") and k.endswith("id"):  # UUID columns
            out[k] = str(v)
    return out


class NotFound(Exception):
    pass


class Conflict(Exception):
    pass


# ── users ────────────────────────────────────────────────────────────────────
async def get_user_by_email(pool, email: str) -> dict | None:
    rec = await pool.fetchrow("SELECT * FROM users WHERE email=$1", email)
    return _row(rec)


async def list_tenant_users(pool, tenant_id: str) -> list[dict]:
    rows = await pool.fetch(
        "SELECT email, role, status FROM users WHERE tenant_id=$1 ORDER BY email", tenant_id
    )
    return [dict(r) for r in rows]


async def create_tenant_user(
    pool, *, tenant_id: str, email: str, password_hash: str, role: str = "tenant_user"
) -> dict:
    try:
        rec = await pool.fetchrow(
            """INSERT INTO users (email, password_hash, role, tenant_id)
               VALUES ($1,$2,$3,$4) RETURNING email, role, status""",
            email,
            password_hash,
            role,
            tenant_id,
        )
    except asyncpg.UniqueViolationError:
        raise Conflict(f"user {email!r} already exists") from None
    return dict(rec)


async def set_user_status(pool, *, tenant_id: str, email: str, status: str) -> dict:
    rec = await pool.fetchrow(
        "UPDATE users SET status=$1 WHERE email=$2 AND tenant_id=$3 RETURNING email, role, status",
        status,
        email,
        tenant_id,
    )
    if rec is None:
        raise NotFound("user not found for this tenant")
    return dict(rec)


async def delete_tenant_user(pool, *, tenant_id: str, email: str) -> None:
    res = await pool.execute(
        "DELETE FROM users WHERE email=$1 AND tenant_id=$2 AND role <> 'admin'", email, tenant_id
    )
    if res.endswith("0"):
        raise NotFound("user not found for this tenant")


async def set_user_password(pool, *, email: str, tenant_id: str, password_hash: str) -> None:
    res = await pool.execute(
        "UPDATE users SET password_hash=$1 WHERE email=$2 AND tenant_id=$3",
        password_hash,
        email,
        tenant_id,
    )
    if res.endswith("0"):
        raise NotFound("user not found for this tenant")


# ── platform admins (role='admin', no tenant) ────────────────────────────────
async def list_admins(pool) -> list[dict]:
    rows = await pool.fetch("SELECT email, status FROM users WHERE role='admin' ORDER BY email")
    return [dict(r) for r in rows]


async def create_admin(pool, *, email: str, password_hash: str) -> dict:
    try:
        rec = await pool.fetchrow(
            """INSERT INTO users (email, password_hash, role, tenant_id)
               VALUES ($1,$2,'admin',NULL) RETURNING email, status""",
            email,
            password_hash,
        )
    except asyncpg.UniqueViolationError:
        raise Conflict(f"user {email!r} already exists") from None
    return dict(rec)


async def _other_active_admins(pool, email: str) -> int:
    return int(
        await pool.fetchval(
            "SELECT count(*) FROM users WHERE role='admin' AND status='active' AND email <> $1",
            email,
        )
    )


async def set_admin_status(pool, *, email: str, status: str) -> dict:
    if status == "disabled" and await _other_active_admins(pool, email) == 0:
        raise Conflict("cannot disable the last active admin")
    rec = await pool.fetchrow(
        "UPDATE users SET status=$1 WHERE email=$2 AND role='admin' RETURNING email, status",
        status,
        email,
    )
    if rec is None:
        raise NotFound("admin not found")
    return dict(rec)


async def delete_admin(pool, *, email: str) -> None:
    if await _other_active_admins(pool, email) == 0:
        raise Conflict("cannot remove the last active admin")
    res = await pool.execute("DELETE FROM users WHERE email=$1 AND role='admin'", email)
    if res.endswith("0"):
        raise NotFound("admin not found")


async def set_admin_password(pool, *, email: str, password_hash: str) -> None:
    res = await pool.execute(
        "UPDATE users SET password_hash=$1 WHERE email=$2 AND role='admin'", password_hash, email
    )
    if res.endswith("0"):
        raise NotFound("admin not found")


# ── tenants ──────────────────────────────────────────────────────────────────
async def create_tenant(
    pool,
    *,
    name: str,
    contact_email: str | None,
    plan_id: str,
    admin_email: str,
    admin_password_hash: str,
) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            plan_ok = await conn.fetchval("SELECT 1 FROM plans WHERE id=$1", plan_id)
            if not plan_ok:
                raise NotFound(f"plan {plan_id!r} does not exist")
            if await conn.fetchval("SELECT 1 FROM users WHERE email=$1", admin_email):
                raise Conflict(f"user {admin_email!r} already exists")
            tenant = await conn.fetchrow(
                """INSERT INTO tenants (name, contact_email, plan_id)
                   VALUES ($1,$2,$3) RETURNING *""",
                name,
                contact_email,
                plan_id,
            )
            await conn.execute(
                """INSERT INTO users (email, password_hash, role, tenant_id)
                   VALUES ($1,$2,'tenant_user',$3)""",
                admin_email,
                admin_password_hash,
                tenant["id"],
            )
            return _row(tenant)


async def list_tenants(pool, *, include_deleted: bool = False) -> list[dict]:
    q = "SELECT * FROM tenants"
    if not include_deleted:
        q += " WHERE status <> 'deleted'"
    q += " ORDER BY created_at DESC"
    return [_row(r) for r in await pool.fetch(q)]


async def get_tenant(pool, tenant_id: str, *, include_deleted: bool = False) -> dict:
    rec = await pool.fetchrow("SELECT * FROM tenants WHERE id=$1", tenant_id)
    if rec is None or (not include_deleted and rec["status"] == "deleted"):
        raise NotFound("tenant not found")
    return _row(rec)


async def update_tenant(pool, tenant_id: str, fields: dict) -> dict:
    sets, vals = [], []
    for i, (k, v) in enumerate(fields.items(), start=1):
        sets.append(f"{k}=${i}")
        vals.append(v)
    if not sets:
        return await get_tenant(pool, tenant_id)
    vals.append(tenant_id)
    rec = await pool.fetchrow(
        f"UPDATE tenants SET {', '.join(sets)}, updated_at=now() "
        f"WHERE id=${len(vals)} AND status <> 'deleted' RETURNING *",
        *vals,
    )
    if rec is None:
        raise NotFound("tenant not found")
    return _row(rec)


async def soft_delete_tenant(pool, tenant_id: str) -> list[str]:
    """Deactivate a tenant: mark deleted and disable all its keys (preserves
    invoices/usage history for audit). Its login users are hard-deleted —
    create_tenant enforces email uniqueness across ALL users, so leaving them
    would block re-creating a tenant with the same admin email. Returns the
    removed emails so the caller can clean up the IdP."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            res = await conn.execute(
                "UPDATE tenants SET status='deleted', updated_at=now() "
                "WHERE id=$1 AND status <> 'deleted'",
                tenant_id,
            )
            if res.endswith("0"):
                raise NotFound("tenant not found")
            await conn.execute(
                "UPDATE api_keys SET status='deleted', deleted_at=now(), updated_at=now() "
                "WHERE tenant_id=$1 AND status <> 'deleted'",
                tenant_id,
            )
            rows = await conn.fetch(
                "DELETE FROM users WHERE tenant_id=$1 RETURNING email", tenant_id
            )
            return [r["email"] for r in rows]


# ── api keys ─────────────────────────────────────────────────────────────────
async def _key_name_taken(
    pool, tenant_id: str, name: str, *, exclude_id: str | None = None
) -> bool:
    """True if a non-deleted key with this name already exists for the tenant."""
    if exclude_id is None:
        return bool(
            await pool.fetchval(
                "SELECT 1 FROM api_keys WHERE tenant_id=$1 AND name=$2 AND status <> 'deleted'",
                tenant_id,
                name,
            )
        )
    return bool(
        await pool.fetchval(
            "SELECT 1 FROM api_keys "
            "WHERE tenant_id=$1 AND name=$2 AND id <> $3 AND status <> 'deleted'",
            tenant_id,
            name,
            exclude_id,
        )
    )


async def create_key(pool, *, tenant_id: str, name: str, scopes: list[str]) -> tuple[dict, str]:
    if await _key_name_taken(pool, tenant_id, name):
        raise Conflict(f"a key named {name!r} already exists")
    full, prefix, key_hash = security.generate_api_key()
    try:
        rec = await pool.fetchrow(
            """INSERT INTO api_keys (tenant_id, name, key_prefix, key_hash, key_enc, scopes)
               VALUES ($1,$2,$3,$4,$5,$6) RETURNING *""",
            tenant_id,
            name,
            prefix,
            key_hash,
            security.encrypt_key(full),
            scopes,
        )
    except asyncpg.UniqueViolationError:  # lost a create race on the unique index
        raise Conflict(f"a key named {name!r} already exists") from None
    return _row(rec), full


async def list_keys(pool, tenant_id: str) -> list[dict]:
    rows = await pool.fetch(
        "SELECT * FROM api_keys WHERE tenant_id=$1 AND status <> 'deleted' "
        "ORDER BY created_at DESC",
        tenant_id,
    )
    return [_row(r) for r in rows]


async def get_key(pool, key_id: str, *, tenant_id: str | None = None) -> dict:
    if tenant_id is not None:
        rec = await pool.fetchrow(
            "SELECT * FROM api_keys WHERE id=$1 AND tenant_id=$2", key_id, tenant_id
        )
    else:
        rec = await pool.fetchrow("SELECT * FROM api_keys WHERE id=$1", key_id)
    if rec is None or rec["status"] == "deleted":
        raise NotFound("key not found")
    return _row(rec)


async def update_key(pool, key_id: str, tenant_id: str, fields: dict) -> dict:
    if "name" in fields and await _key_name_taken(
        pool, tenant_id, fields["name"], exclude_id=key_id
    ):
        raise Conflict(f"a key named {fields['name']!r} already exists")
    sets, vals = [], []
    for i, (k, v) in enumerate(fields.items(), start=1):
        sets.append(f"{k}=${i}")
        vals.append(v)
    if not sets:
        return await get_key(pool, key_id, tenant_id=tenant_id)
    vals.extend([key_id, tenant_id])
    try:
        rec = await pool.fetchrow(
            f"UPDATE api_keys SET {', '.join(sets)}, updated_at=now() "
            f"WHERE id=${len(vals) - 1} AND tenant_id=${len(vals)} AND status <> 'deleted' "
            f"RETURNING *",
            *vals,
        )
    except asyncpg.UniqueViolationError:
        raise Conflict(f"a key named {fields['name']!r} already exists") from None
    if rec is None:
        raise NotFound("key not found")
    return _row(rec)


async def soft_delete_key(pool, key_id: str, tenant_id: str) -> dict:
    rec = await pool.fetchrow(
        "UPDATE api_keys SET status='deleted', deleted_at=now(), updated_at=now() "
        "WHERE id=$1 AND tenant_id=$2 AND status <> 'deleted' RETURNING *",
        key_id,
        tenant_id,
    )
    if rec is None:
        raise NotFound("key not found")
    return _row(rec)


async def tenant_id_for_key(pool, key_id: str) -> str | None:
    try:
        uuid.UUID(key_id)
    except (ValueError, TypeError, AttributeError):
        return None
    val = await pool.fetchval("SELECT tenant_id FROM api_keys WHERE id=$1", key_id)
    return str(val) if val else None


async def get_key_by_hash(pool, key_hash: str) -> dict | None:
    """Gateway fallback on cache miss. Returns the key incl. tenant status."""
    rec = await pool.fetchrow(
        """SELECT k.id AS key_id, k.tenant_id, k.status AS key_status, k.scopes,
                  t.status AS tenant_status
             FROM api_keys k JOIN tenants t ON t.id = k.tenant_id
            WHERE k.key_hash=$1""",
        key_hash,
    )
    return _row(rec)


# ── usage history (durable rollups; count = milli-credits) ───────────────────
async def usage_history(pool, tenant_id: str, *, period_from: str, period_to: str) -> list[dict]:
    rows = await pool.fetch(
        """SELECT period, day::text AS day, endpoint,
                  SUM(requests)::bigint AS requests, SUM(count)::bigint AS milli
             FROM usage_rollups
            WHERE tenant_id=$1 AND period BETWEEN $2 AND $3
         GROUP BY period, day, endpoint
         ORDER BY day, endpoint""",
        tenant_id,
        period_from,
        period_to,
    )
    return [dict(r) for r in rows]


async def usage_totals_for_period(pool, tenant_id: str, period: str) -> dict[str, int]:
    """{"milli": milli-credits, "requests": raw requests} for the period."""
    rec = await pool.fetchrow(
        """SELECT COALESCE(SUM(count),0)::bigint AS milli,
                  COALESCE(SUM(requests),0)::bigint AS requests
             FROM usage_rollups WHERE tenant_id=$1 AND period=$2""",
        tenant_id,
        period,
    )
    return {"milli": int(rec["milli"]), "requests": int(rec["requests"])}


async def usage_by_key_for_period(pool, tenant_id: str, period: str) -> dict[str, dict[str, int]]:
    """Durable per-key usage for the period: {key_id: {milli, requests}}."""
    rows = await pool.fetch(
        """SELECT key_id, SUM(count)::bigint AS milli, SUM(requests)::bigint AS requests
             FROM usage_rollups WHERE tenant_id=$1 AND period=$2 GROUP BY key_id""",
        tenant_id,
        period,
    )
    return {
        str(r["key_id"]): {"milli": int(r["milli"]), "requests": int(r["requests"])} for r in rows
    }


# ── invoices ─────────────────────────────────────────────────────────────────
async def upsert_invoice(
    pool,
    *,
    tenant_id: str,
    period: str,
    total_requests: int,
    total_milli_credits: int,
    amount_cents: int,
    line_items: list[dict],
) -> dict:
    rec = await pool.fetchrow(
        """INSERT INTO invoices (tenant_id, period, total_requests, total_milli_credits,
                                 amount_cents, line_items)
           VALUES ($1,$2,$3,$4,$5,$6)
           ON CONFLICT (tenant_id, period) DO UPDATE
             SET total_requests      = EXCLUDED.total_requests,
                 total_milli_credits = EXCLUDED.total_milli_credits,
                 amount_cents        = EXCLUDED.amount_cents,
                 line_items          = EXCLUDED.line_items,
                 generated_at        = now()
           WHERE invoices.status = 'pending'
           RETURNING *""",
        tenant_id,
        period,
        total_requests,
        total_milli_credits,
        amount_cents,
        json.dumps(line_items),
    )
    if rec is None:  # already paid → return existing untouched
        rec = await pool.fetchrow(
            "SELECT * FROM invoices WHERE tenant_id=$1 AND period=$2", tenant_id, period
        )
    return _invoice_row(rec)


async def list_invoices(
    pool, *, tenant_id: str | None = None, status: str | None = None, period: str | None = None
) -> list[dict]:
    conds, vals = [], []
    if tenant_id:
        vals.append(tenant_id)
        conds.append(f"tenant_id=${len(vals)}")
    if status:
        vals.append(status)
        conds.append(f"status=${len(vals)}")
    if period:
        vals.append(period)
        conds.append(f"period=${len(vals)}")
    where = (" WHERE " + " AND ".join(conds)) if conds else ""
    rows = await pool.fetch(f"SELECT * FROM invoices{where} ORDER BY generated_at DESC", *vals)
    return [_invoice_row(r) for r in rows]


async def get_invoice(pool, invoice_id: str, *, tenant_id: str | None = None) -> dict:
    if tenant_id is not None:
        rec = await pool.fetchrow(
            "SELECT * FROM invoices WHERE id=$1 AND tenant_id=$2", invoice_id, tenant_id
        )
    else:
        rec = await pool.fetchrow("SELECT * FROM invoices WHERE id=$1", invoice_id)
    if rec is None:
        raise NotFound("invoice not found")
    return _invoice_row(rec)


async def mark_invoice_paid(pool, invoice_id: str) -> dict:
    rec = await pool.fetchrow(
        "UPDATE invoices SET status='paid', paid_at=now() "
        "WHERE id=$1 AND status='pending' RETURNING *",
        invoice_id,
    )
    if rec is None:
        # distinguish not-found from already-paid
        existing = await pool.fetchrow("SELECT * FROM invoices WHERE id=$1", invoice_id)
        if existing is None:
            raise NotFound("invoice not found")
        raise Conflict("invoice already paid")
    return _invoice_row(rec)


def _invoice_row(rec: asyncpg.Record) -> dict:
    out = _row(rec)
    li = out.get("line_items")
    if isinstance(li, str):
        out["line_items"] = json.loads(li)
    out["total_credits"] = int(out.get("total_milli_credits") or 0) / 1000
    return out


async def tenant_with_plan(pool, tenant_id: str) -> dict:
    rec = await pool.fetchrow(
        """SELECT t.id AS tenant_id, t.plan_id,
                  p.base_price_cents, p.overage_cents_per_unit, p.monthly_quota, p.hard_cap,
                  p.rps
             FROM tenants t LEFT JOIN plans p ON p.id = t.plan_id
            WHERE t.id=$1 AND t.status <> 'deleted'""",
        tenant_id,
    )
    if rec is None:
        raise NotFound("tenant not found")
    return _row(rec)


async def list_key_hashes(pool, tenant_id: str) -> list[str]:
    rows = await pool.fetch("SELECT key_hash FROM api_keys WHERE tenant_id=$1", tenant_id)
    return [r["key_hash"] for r in rows]


async def tenant_ids_on_plan(pool, plan_id: str) -> list[str]:
    rows = await pool.fetch(
        "SELECT id FROM tenants WHERE plan_id=$1 AND status <> 'deleted'", plan_id
    )
    return [str(r["id"]) for r in rows]


async def list_active_tenant_quota_specs(pool) -> list[dict]:
    """(tenant_id, monthly_quota, hard_cap) for every active tenant — used to
    re-project per-tenant APISIX limit-count groups at month rollover."""
    rows = await pool.fetch(
        """SELECT t.id AS tenant_id, p.monthly_quota, p.hard_cap, p.rps
             FROM tenants t JOIN plans p ON p.id = t.plan_id
            WHERE t.status = 'active'"""
    )
    return [dict(r) for r in rows]


async def all_active_tenant_ids(pool) -> list[str]:
    """Tenant ids for every active tenant — used to re-project APISIX consumers
    on startup so the data plane self-heals after an etcd reset."""
    rows = await pool.fetch("SELECT id FROM tenants WHERE status = 'active'")
    return [str(r["id"]) for r in rows]


async def list_plans(pool) -> list[dict]:
    return [dict(r) for r in await pool.fetch("SELECT * FROM plans ORDER BY base_price_cents")]


async def get_plan(pool, plan_id: str) -> dict:
    rec = await pool.fetchrow("SELECT * FROM plans WHERE id=$1", plan_id)
    if rec is None:
        raise NotFound("plan not found")
    return dict(rec)


async def create_plan(
    pool,
    *,
    id: str,
    name: str,
    monthly_quota: int,
    base_price_cents: int,
    overage_cents_per_unit: float,
    hard_cap: bool,
    rps: int = 0,
) -> dict:
    try:
        rec = await pool.fetchrow(
            """INSERT INTO plans (id, name, monthly_quota, base_price_cents,
                                  overage_cents_per_unit, hard_cap, rps)
               VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *""",
            id,
            name,
            monthly_quota,
            base_price_cents,
            overage_cents_per_unit,
            hard_cap,
            rps,
        )
    except asyncpg.UniqueViolationError:
        raise Conflict(f"plan {id!r} already exists") from None
    return dict(rec)


async def update_plan(pool, plan_id: str, fields: dict) -> dict:
    sets, vals = [], []
    for i, (k, v) in enumerate(fields.items(), start=1):
        sets.append(f"{k}=${i}")
        vals.append(v)
    if not sets:
        return await get_plan(pool, plan_id)
    vals.append(plan_id)
    rec = await pool.fetchrow(
        f"UPDATE plans SET {', '.join(sets)} WHERE id=${len(vals)} RETURNING *", *vals
    )
    if rec is None:
        raise NotFound("plan not found")
    return dict(rec)


async def delete_plan(pool, plan_id: str) -> None:
    try:
        res = await pool.execute("DELETE FROM plans WHERE id=$1", plan_id)
    except asyncpg.ForeignKeyViolationError:
        n = await pool.fetchval(
            "SELECT count(*) FROM tenants WHERE plan_id=$1 AND status <> 'deleted'", plan_id
        )
        raise Conflict(f"plan is in use by {n} tenant(s); reassign them first") from None
    if res.endswith("0"):
        raise NotFound("plan not found")


# ── endpoint credit weights ──────────────────────────────────────────────────
async def list_weights(pool) -> list[dict]:
    rows = await pool.fetch("SELECT * FROM endpoint_weights ORDER BY endpoint")
    return [dict(r) for r in rows]


async def upsert_weight(pool, endpoint: str, milli_credits: int) -> dict:
    rec = await pool.fetchrow(
        """INSERT INTO endpoint_weights (endpoint, milli_credits)
           VALUES ($1,$2)
           ON CONFLICT (endpoint)
           DO UPDATE SET milli_credits = EXCLUDED.milli_credits, updated_at = now()
           RETURNING *""",
        endpoint,
        milli_credits,
    )
    return dict(rec)


async def delete_weight(pool, endpoint: str) -> None:
    """Remove an endpoint's weight so it reverts to the default (1 credit)."""
    res = await pool.execute("DELETE FROM endpoint_weights WHERE endpoint=$1", endpoint)
    if res.endswith("0"):
        raise NotFound("weight not found")


async def active_tenants_with_plan(pool) -> list[dict]:
    rows = await pool.fetch(
        """SELECT t.id AS tenant_id, t.plan_id,
                  p.base_price_cents, p.overage_cents_per_unit, p.monthly_quota
             FROM tenants t LEFT JOIN plans p ON p.id = t.plan_id
            WHERE t.status <> 'deleted'"""
    )
    return [_row(r) for r in rows]
