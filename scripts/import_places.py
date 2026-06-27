"""Ad-hoc importer: publish a unified place export → the geocoder via NATS.

The startup path for ``data/places/`` is the ``places-watcher`` service; this
script is the manual equivalent for a single unified-schema file (see
``shared.places_mapping`` / ``scripts/unify_places.py``). It maps each record
with ``place_to_element`` and publishes to the OSM JetStream stream
(``osm.elements``); the running ``es_inserter`` / ``postgis_inserter`` index it.

Usage (inside a container on the compose network):

    python scripts/import_places.py --file /app/data/places/places_pg.json
    python scripts/import_places.py --file /app/data/places/pelias_google.part1.json --limit 100 --dry-run
"""

from __future__ import annotations

import asyncio

from _place_publish import run_cli  # sibling module (scripts/ is on sys.path[0])

from shared.places_mapping import place_to_element

if __name__ == "__main__":
    asyncio.run(
        run_cli("import-places", place_to_element, default_file="data/places/places_pg.json")
    )
