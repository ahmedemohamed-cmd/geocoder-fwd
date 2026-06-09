"""Standalone usage aggregator: drains the Redis usage-event list into Postgres
rollups on a loop. Runs as its own service so it's independent of the data plane
(APISIX) and the control plane.

    python -m billing.aggregator
"""
from __future__ import annotations

import asyncio

from . import db, main, usage


async def run(interval: float = 2.0) -> None:
    await db.ensure_database()
    pool = await db.create_pool()
    redis = main._make_redis()
    try:
        while True:
            try:
                await usage.flush_events(pool, redis)
            except Exception:  # noqa: BLE001 - keep the loop alive
                pass
            await asyncio.sleep(interval)
    finally:
        await pool.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(run())
