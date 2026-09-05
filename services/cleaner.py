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
            logger.info("[cleaner] Deleted ES index '%s'", ES_INDEX)
        else:
            logger.warning("[cleaner] ES index '%s' does not exist, skipping", ES_INDEX)
    except Exception as exc:
        logger.error("[cleaner] ES error: %s", exc)
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
        logger.info("[cleaner] Dropped PostGIS tables '%s', '%s'", PG_TABLE, PG_ADDR_TABLE)
    except Exception as exc:
        logger.error("[cleaner] PostGIS error: %s", exc)


async def clean_nats():
    import nats

    try:
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        try:
            await js.delete_stream(NATS_STREAM)
            logger.info("[cleaner] Deleted NATS stream '%s'", NATS_STREAM)
        except Exception:
            logger.warning("[cleaner] NATS stream '%s' does not exist, skipping", NATS_STREAM)
        await nc.close()
    except Exception as exc:
        logger.error("[cleaner] NATS error: %s", exc)


async def clean():
    logger.info("[cleaner] Wiping all indexed data ...")
    await clean_elasticsearch()
    await clean_postgis()
    await clean_nats()
    logger.info("[cleaner] Done.")


if __name__ == "__main__":
    asyncio.run(clean())
