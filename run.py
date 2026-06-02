#!/usr/bin/env python3
"""Launcher – run any service by name.

Usage:
    python run.py <service>

Services:
    downloader        Download OSM PBF files from .env URLs
    watcher           Parse PBF files and publish to NATS
    oa-watcher        Parse OpenAddresses CSV/GeoJSON files and publish to NATS
    es-inserter       NATS -> Elasticsearch
    postgis-inserter  NATS -> PostGIS
    geocoder          FastAPI geocoding HTTP server (includes /insert endpoint)
    cleaner           Wipe all indexed data (ES, PostGIS, NATS)
"""

import asyncio
import sys


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__.strip())
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

    elif service == "es-inserter":
        from services.es_inserter import run

        asyncio.run(run())

    elif service == "postgis-inserter":
        from services.postgis_inserter import run

        asyncio.run(run())

    elif service == "geocoder":
        import uvicorn
        from services.geocoder import app

        uvicorn.run(app, host="0.0.0.0", port=8000)

    elif service == "cleaner":
        from services.cleaner import clean

        asyncio.run(clean())

    else:
        print(f"Unknown service: {service}")
        print("Run  python run.py --help  for available services.")
        sys.exit(1)


if __name__ == "__main__":
    main()
