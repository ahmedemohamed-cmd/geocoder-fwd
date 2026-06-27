#!/usr/bin/env python3
"""Launcher – run any service by name.

Usage:
    python run.py <service>

Services:
    downloader        Download OSM PBF files from .env URLs
    watcher           Parse PBF files and publish to NATS
    oa-watcher        Parse OpenAddresses CSV/GeoJSON files and publish to NATS
    gn-watcher        Parse GeoNames TSV dump files and publish to NATS
    places-watcher    Parse curated place JSON exports and publish to NATS
    es-inserter       NATS -> Elasticsearch
    postgis-inserter  NATS -> PostGIS
    geocoder          FastAPI geocoding HTTP server (includes /insert endpoint)
    traffic-aggregator  Map-match GPS probes -> per-edge speeds in Redis
    traffic-writer      Write Redis per-edge speeds into Valhalla traffic.tar
    cleaner           Wipe all indexed data (ES, PostGIS, NATS)
"""

import asyncio
import sys

from shared.logging import get_logger

logger = get_logger("run")


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        logger.info(__doc__.strip())
        sys.exit(0)

    service = sys.argv[1]

    if service == "downloader":
        from services.downloader import download

        download()

    elif service == "watcher":
        from services.watcher import run

        asyncio.run(run())

    elif service == "oa-watcher":
        from services.oa_watcher import run

        asyncio.run(run())

    elif service == "gn-watcher":
        from services.gn_watcher import run

        asyncio.run(run())

    elif service == "places-watcher":
        from services.places_watcher import run

        asyncio.run(run())

    elif service == "es-inserter":
        from services.es_inserter import run

        asyncio.run(run())

    elif service == "postgis-inserter":
        from services.postgis_inserter import run

        asyncio.run(run())

    elif service == "geocoder":
        import os

        import uvicorn

        # The geocoder is stateless (all state lives in ES/PostGIS/Redis/NATS), so
        # it scales out both vertically (uvicorn workers, one per core) and
        # horizontally (multiple replicas). GEOCODER_WORKERS controls per-process
        # workers; with >1 uvicorn needs an import string to fork.
        workers = int(os.getenv("GEOCODER_WORKERS", "1"))
        if workers > 1:
            uvicorn.run("services.geocoder:app", host="0.0.0.0", port=8000, workers=workers)
        else:
            from services.geocoder import app

            uvicorn.run(app, host="0.0.0.0", port=8000)

    elif service == "traffic-aggregator":
        from services.traffic_aggregator import run

        asyncio.run(run())

    elif service == "traffic-writer":
        from services.traffic_writer import run

        run()  # synchronous loop (mmap writes)

    elif service == "cleaner":
        from services.cleaner import clean

        asyncio.run(clean())

    else:
        logger.info(f"Unknown service: {service}")
        logger.info("Run  python run.py --help  for available services.")
        sys.exit(1)


if __name__ == "__main__":
    main()
