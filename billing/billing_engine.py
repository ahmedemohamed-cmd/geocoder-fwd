"""Monthly invoice generation from usage rollups + plan pricing.

Run for a closed period (e.g. via a CronJob on the 1st of the month) or on
demand by an admin. Idempotent per (tenant, period): re-running updates a still
``pending`` invoice and leaves a ``paid`` one untouched.
"""

from __future__ import annotations

import math

from . import repo
from .weights import MILLI_PER_CREDIT as _MILLI


def compute_charge(
    *,
    total_milli_credits: int,
    base_price_cents: int,
    overage_cents_per_credit: float,
    monthly_quota_credits: int,
) -> tuple[int, list[dict]]:
    """Return (amount_cents, line_items). Usage is metered in milli-credits;
    overage is billed in whole credits (rounded up)."""
    line_items: list[dict] = [
        {"description": "Base subscription", "quantity": 1, "amount_cents": int(base_price_cents)}
    ]
    amount = int(base_price_cents)

    quota_milli = int(monthly_quota_credits) * _MILLI
    overage_milli = max(0, total_milli_credits - quota_milli)
    overage_credits = math.ceil(overage_milli / _MILLI)
    if overage_credits > 0 and overage_cents_per_credit > 0:
        overage_cents = math.ceil(overage_credits * float(overage_cents_per_credit))
        amount += overage_cents
        line_items.append(
            {
                "description": (
                    f"Overage ({overage_credits} credits over {monthly_quota_credits} included)"
                ),
                "quantity": overage_credits,
                "unit_cents": float(overage_cents_per_credit),
                "amount_cents": overage_cents,
            }
        )
    else:
        used_credits = total_milli_credits / _MILLI
        line_items.append(
            {
                "description": f"Included credits ({used_credits:g}/{monthly_quota_credits})",
                "quantity": used_credits,
                "amount_cents": 0,
            }
        )
    return amount, line_items


async def generate_invoice_for_tenant(pool, tenant: dict, period: str) -> dict:
    totals = await repo.usage_totals_for_period(pool, tenant["tenant_id"], period)
    amount, line_items = compute_charge(
        total_milli_credits=totals["milli"],
        base_price_cents=tenant.get("base_price_cents") or 0,
        overage_cents_per_credit=tenant.get("overage_cents_per_unit") or 0,
        monthly_quota_credits=tenant.get("monthly_quota") or 0,
    )
    return await repo.upsert_invoice(
        pool,
        tenant_id=tenant["tenant_id"],
        period=period,
        total_requests=totals["requests"],
        total_milli_credits=totals["milli"],
        amount_cents=amount,
        line_items=line_items,
    )


async def run_billing(pool, period: str) -> list[dict]:
    """Generate/refresh invoices for every active tenant for ``period``."""
    tenants = await repo.active_tenants_with_plan(pool)
    return [await generate_invoice_for_tenant(pool, t, period) for t in tenants]
