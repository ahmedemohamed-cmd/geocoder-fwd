"""Standalone usage aggregator: drains the Redis usage-event list into Postgres
rollups on a loop. Runs as its own service so it's independent of the data plane
(APISIX) and the control plane.

    python -m billing.aggregator
"""

from __future__ import annotations

import asyncio
import logging

from . import apisix_admin, db, main, repo, usage, weights

_log = logging.getLogger("billing.aggregator")
_last_quota_period: str | None = None


async def reproject_quotas_for_new_period(pool) -> bool:
    """Re-push each active tenant's APISIX limit-count group when the calendar
    period changes, so the period-scoped quota key rolls over and the hard cap
    resets on the 1st. Idempotent; also runs once on startup. No-op unless APISIX
    is configured."""
    global _last_quota_period
    period = apisix_admin.current_period()
    if period == _last_quota_period:
        return False
    if apisix_admin.enabled():
        w = await weights.get_weights(pool)
        for spec in await repo.list_active_tenant_quota_specs(pool):
            try:
                await apisix_admin.ensure_consumer_group(
                    str(spec["tenant_id"]),
                    # credit quota → raw-request backstop (APISIX counts requests)
                    quota=weights.projected_request_cap(
                        w, int(spec.get("monthly_quota") or 0)
                    ),
                    hard_cap=bool(spec.get("hard_cap")),
                    rps=int(spec.get("rps") or 0),
                    period=period,
                )
            except Exception as e:  # noqa: BLE001 - reconciler heals drift later
                _log.warning(
                    "quota reprojection for tenant %s failed: %s", spec.get("tenant_id"), e
                )
    _last_quota_period = period
    return True


async def run(interval: float = 2.0) -> None:
    await db.ensure_database()
    pool = await db.create_pool()
    redis = main._make_redis()
    try:
        while True:
            try:
                await reproject_quotas_for_new_period(pool)
                await usage.flush_events(pool, redis)
            except Exception:  # noqa: BLE001 - keep the loop alive
                pass
            await asyncio.sleep(interval)
    finally:
        await pool.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(run())
