"""Ad-hoc importer for a Postgres ``places`` export → the geocoder via NATS.

The startup path for this data is the ``places-watcher`` service (drop the file
in ``data/places/``). This script is the manual equivalent: it publishes a
``places`` table export (a JSON array of ``{id, name, address, phone, rating,
layer, categories, lon, lat}`` rows — see ``places_pg.json``) to the OSM
JetStream stream (``osm.elements``) without restarting the watcher. The
record→message mapping lives in ``shared.places_mapping``.

Usage (inside a container on the compose network):

    python scripts/import_places_pg.py --file /app/places_pg.json
    python scripts/import_places_pg.py --file /app/places_pg.json --limit 100 --dry-run
"""

from __future__ import annotations

import asyncio

from _place_publish import run_cli  # sibling module (scripts/ is on sys.path[0])

from shared.places_mapping import map_pg_place

if __name__ == "__main__":
    asyncio.run(run_cli("import-places-pg", map_pg_place, default_file="places_pg.json"))
