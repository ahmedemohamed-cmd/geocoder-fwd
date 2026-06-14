"""Wipe all indexed data so the pipeline can be re-run from scratch.

Deletes:
  - Elasticsearch index  (osm_places)
  - PostGIS table        (osm_geometries)
  - NATS JetStream stream (OSM)
"""

import asyncio

import asyncpg
from elasticsearch import AsyncElasticsearch

from shared.config import (
    ELASTICSEARCH_URL,
    NATS_STREAM,
    NATS_URL,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from shared.logging import get_logger

logger = get_logger("cleaner")

ES_INDEX = "osm_places"
PG_TABLE = "osm_geometries"
PG_ADDR_TABLE = "osm_addresses"


async def clean_elasticsearch():
    es = AsyncElasticsearch(ELASTICSEARCH_URL)
    try:
        if await es.indices.exists(index=ES_INDEX):
            await es.indices.delete(index=ES_INDEX)
            logger.info(f"[cleaner] Deleted ES index '{ES_INDEX}'")
        else:
            logger.warning(f"[cleaner] ES index '{ES_INDEX}' does not exist, skipping")
    except Exception as exc:
        logger.error(f"[cleaner] ES error: {exc}")
    finally:
        await es.close()


async def clean_postgis():
    try:
        pool = await asyncpg.create_pool(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            min_size=1,
            max_size=2,
        )
        async with pool.acquire() as conn:
            await conn.execute(f"DROP TABLE IF EXISTS {PG_TABLE} CASCADE")
            await conn.execute(f"DROP TABLE IF EXISTS {PG_ADDR_TABLE} CASCADE")
        await pool.close()
        logger.info(f"[cleaner] Dropped PostGIS tables '{PG_TABLE}', '{PG_ADDR_TABLE}'")
    except Exception as exc:
        logger.error(f"[cleaner] PostGIS error: {exc}")


async def clean_nats():
    import nats

    try:
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        try:
            await js.delete_stream(NATS_STREAM)
            logger.info(f"[cleaner] Deleted NATS stream '{NATS_STREAM}'")
        except Exception:
            logger.warning(f"[cleaner] NATS stream '{NATS_STREAM}' does not exist, skipping")
        await nc.close()
    except Exception as exc:
        logger.error(f"[cleaner] NATS error: {exc}")


async def clean():
    logger.info("[cleaner] Wiping all indexed data ...")
    await clean_elasticsearch()
    await clean_postgis()
    await clean_nats()
    logger.info("[cleaner] Done.")


if __name__ == "__main__":
    asyncio.run(clean())
