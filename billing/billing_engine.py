"""Monthly invoice generation from usage rollups + plan pricing.

Run for a closed period (e.g. via a CronJob on the 1st of the month) or on
demand by an admin. Idempotent per (tenant, period): re-running updates a still
``pending`` invoice and leaves a ``paid`` one untouched.
"""

from __future__ import annotations

import math

from . import repo


def compute_charge(
    *, total_requests: int, base_price_cents: int, overage_cents_per_unit: float, monthly_quota: int
) -> tuple[int, list[dict]]:
    """Return (amount_cents, line_items)."""
    line_items: list[dict] = [
        {"description": "Base subscription", "quantity": 1, "amount_cents": int(base_price_cents)}
    ]
    amount = int(base_price_cents)

    overage_units = max(0, total_requests - int(monthly_quota))
    if overage_units > 0 and overage_cents_per_unit > 0:
        overage_cents = math.ceil(overage_units * float(overage_cents_per_unit))
        amount += overage_cents
        line_items.append(
            {
                "description": f"Overage ({overage_units} requests over {monthly_quota} included)",
                "quantity": overage_units,
                "unit_cents": float(overage_cents_per_unit),
                "amount_cents": overage_cents,
            }
        )
    else:
        line_items.append(
            {
                "description": f"Included requests ({total_requests}/{monthly_quota})",
                "quantity": total_requests,
                "amount_cents": 0,
            }
        )
    return amount, line_items


async def generate_invoice_for_tenant(pool, tenant: dict, period: str) -> dict:
    total = await repo.usage_total_for_period(pool, tenant["tenant_id"], period)
    amount, line_items = compute_charge(
        total_requests=total,
        base_price_cents=tenant.get("base_price_cents") or 0,
        overage_cents_per_unit=tenant.get("overage_cents_per_unit") or 0,
        monthly_quota=tenant.get("monthly_quota") or 0,
    )
    return await repo.upsert_invoice(
        pool,
        tenant_id=tenant["tenant_id"],
        period=period,
        total_requests=total,
        amount_cents=amount,
        line_items=line_items,
    )


async def run_billing(pool, period: str) -> list[dict]:
    """Generate/refresh invoices for every active tenant for ``period``."""
    tenants = await repo.active_tenants_with_plan(pool)
    return [await generate_invoice_for_tenant(pool, t, period) for t in tenants]
