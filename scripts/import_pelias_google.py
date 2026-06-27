"""Ad-hoc importer for a Pelias ``source=google`` export → the geocoder via NATS.

The startup path for this data is the ``places-watcher`` service (drop the file
in ``data/places/``). This script is the manual equivalent: it publishes a
Pelias google export (a JSON array of ``{"_id", "_source"}`` objects — see
``pelias_google.json``) to the OSM JetStream stream (``osm.elements``) without
needing to restart the watcher. The running ``es_inserter`` / ``postgis_inserter``
then index it. The record→message mapping lives in ``shared.places_mapping``.

Usage (inside a container on the compose network):

    python scripts/import_pelias_google.py --file /app/pelias_google.json
    python scripts/import_pelias_google.py --file /app/pelias_google.json --limit 100 --dry-run
"""

from __future__ import annotations

import asyncio

from _place_publish import run_cli  # sibling module (scripts/ is on sys.path[0])

from shared.places_mapping import map_pelias_google

if __name__ == "__main__":
    asyncio.run(run_cli("import-pelias-google", map_pelias_google, default_file="pelias_google.json"))
