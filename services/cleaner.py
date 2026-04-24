"""Wipe all indexed data so the pipeline can be re-run from scratch.

Deletes:
  - Elasticsearch index  (osm_places)
  - Typesense collection (osm_places)
  - PostGIS table        (osm_geometries)
  - NATS JetStream stream (OSM)
"""

import asyncio

import asyncpg
from elasticsearch import AsyncElasticsearch
import typesense
from typesense.exceptions import ObjectNotFound

from shared.config import (
    ELASTICSEARCH_URL,
    TYPESENSE_HOST,
    TYPESENSE_PORT,
    TYPESENSE_API_KEY,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    NATS_URL,
    NATS_STREAM,
)

ES_INDEX = "osm_places"
TS_COLLECTION = "osm_places"
PG_TABLE = "osm_geometries"


async def clean_elasticsearch():
    es = AsyncElasticsearch(ELASTICSEARCH_URL)
    try:
        if await es.indices.exists(index=ES_INDEX):
            await es.indices.delete(index=ES_INDEX)
            print(f"[cleaner] Deleted ES index '{ES_INDEX}'")
        else:
            print(f"[cleaner] ES index '{ES_INDEX}' does not exist, skipping")
    except Exception as exc:
        print(f"[cleaner] ES error: {exc}")
    finally:
        await es.close()


def clean_typesense():
    client = typesense.Client(
        {
            "nodes": [
                {
                    "host": TYPESENSE_HOST,
                    "port": str(TYPESENSE_PORT),
                    "protocol": "http",
                }
            ],
            "api_key": TYPESENSE_API_KEY,
            "connection_timeout_seconds": 10,
        }
    )
    try:
        client.collections[TS_COLLECTION].delete()
        print(f"[cleaner] Deleted TS collection '{TS_COLLECTION}'")
    except ObjectNotFound:
        print(f"[cleaner] TS collection '{TS_COLLECTION}' does not exist, skipping")
    except Exception as exc:
        print(f"[cleaner] TS error: {exc}")


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
        await pool.close()
        print(f"[cleaner] Dropped PostGIS table '{PG_TABLE}'")
    except Exception as exc:
        print(f"[cleaner] PostGIS error: {exc}")


async def clean_nats():
    import nats

    try:
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        try:
            await js.delete_stream(NATS_STREAM)
            print(f"[cleaner] Deleted NATS stream '{NATS_STREAM}'")
        except Exception:
            print(f"[cleaner] NATS stream '{NATS_STREAM}' does not exist, skipping")
        await nc.close()
    except Exception as exc:
        print(f"[cleaner] NATS error: {exc}")


async def clean():
    print("[cleaner] Wiping all indexed data ...")
    await clean_elasticsearch()
    clean_typesense()
    await clean_postgis()
    await clean_nats()
    print("[cleaner] Done.")


if __name__ == "__main__":
    asyncio.run(clean())
