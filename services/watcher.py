"""Watch data/ for OSM PBF files, parse them with osmium, publish elements to NATS JS."""

import asyncio
import glob
import json
import math
import os
import queue
import threading

import osmium
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from shared.config import DATA_DIR, NATS_SUBJECT
from shared import nats_client

SENTINEL = object()
BATCH_PUBLISH = 500


def _polygon_area_km2(coords: list[tuple[float, float]]) -> float:
    """Approximate area in km^2 using the shoelace formula on lon/lat.

    Uses a cos(mid-lat) correction to convert degrees to approximate km.
    """
    if len(coords) < 4:
        return 0.0
    mid_lat = sum(c[1] for c in coords) / len(coords)
    cos_lat = math.cos(math.radians(mid_lat))
    # degrees to km  (1 degree lat ~ 111.32 km)
    km_per_deg_lat = 111.32
    km_per_deg_lon = 111.32 * cos_lat

    n = len(coords)
    area_deg2 = 0.0
    for i in range(n):
        j = (i + 1) % n
        area_deg2 += coords[i][0] * coords[j][1]
        area_deg2 -= coords[j][0] * coords[i][1]
    area_deg2 = abs(area_deg2) / 2.0
    return area_deg2 * km_per_deg_lat * km_per_deg_lon


# ---------------------------------------------------------------------------
# OSM PBF handler – pushes parsed elements into a thread-safe queue
# ---------------------------------------------------------------------------
class OSMHandler(osmium.SimpleHandler):
    def __init__(self, q: queue.Queue):
        super().__init__()
        self.q = q
        self.count = 0

    def node(self, n):
        if not n.tags or not n.location.valid():
            return
        tags = dict(n.tags)
        self.q.put(
            {
                "osm_id": f"n{n.id}",
                "osm_type": "node",
                "tags": tags,
                "geom": {
                    "type": "Point",
                    "coordinates": [n.location.lon, n.location.lat],
                },
                "admin_level": int(tags.get("admin_level", 0) or 0),
                "area_km2": 0.0,
            }
        )
        self.count += 1

    def way(self, w):
        if not w.tags:
            return
        tags = dict(w.tags)
        try:
            coords = [(n.lon, n.lat) for n in w.nodes]
        except osmium.InvalidLocationError:
            return
        if len(coords) < 2:
            return
        closed = len(coords) >= 4 and coords[0] == coords[-1]
        geom = (
            {"type": "Polygon", "coordinates": [coords]}
            if closed
            else {"type": "LineString", "coordinates": coords}
        )
        area = _polygon_area_km2(coords) if closed else 0.0
        self.q.put(
            {
                "osm_id": f"w{w.id}",
                "osm_type": "way",
                "tags": tags,
                "geom": geom,
                "admin_level": int(tags.get("admin_level", 0) or 0),
                "area_km2": area,
            }
        )
        self.count += 1

    def relation(self, r):
        if not r.tags:
            return
        tags = dict(r.tags)
        self.q.put(
            {
                "osm_id": f"r{r.id}",
                "osm_type": "relation",
                "tags": tags,
                "geom": None,
                "admin_level": int(tags.get("admin_level", 0) or 0),
                "area_km2": 0.0,
            }
        )
        self.count += 1


# ---------------------------------------------------------------------------
# Publish a single PBF file to NATS (streaming via queue)
# ---------------------------------------------------------------------------
async def publish_file(filepath: str):
    nc, js = await nats_client.connect()
    q: queue.Queue = queue.Queue(maxsize=10_000)

    handler = OSMHandler(q)

    def _parse():
        handler.apply_file(filepath, locations=True, idx="flex_mem")
        q.put(SENTINEL)

    thread = threading.Thread(target=_parse, daemon=True)
    thread.start()
    print(f"[watcher] Parsing {os.path.basename(filepath)} ...")

    published = 0
    while True:
        batch: list[dict] = []
        try:
            item = q.get(timeout=0.5)
            if item is SENTINEL:
                break
            batch.append(item)
            # drain up to BATCH_PUBLISH
            while len(batch) < BATCH_PUBLISH:
                try:
                    item = q.get_nowait()
                    if item is SENTINEL:
                        q.put(SENTINEL)  # re-signal so outer loop exits
                        break
                    batch.append(item)
                except queue.Empty:
                    break
        except queue.Empty:
            continue

        for elem in batch:
            await js.publish(NATS_SUBJECT, json.dumps(elem).encode())
        published += len(batch)
        if published % 5000 < BATCH_PUBLISH:
            print(f"\r[watcher] Published {published} ...", end="", flush=True)

    # flush anything left
    while True:
        try:
            item = q.get_nowait()
            if item is SENTINEL:
                break
            await js.publish(NATS_SUBJECT, json.dumps(item).encode())
            published += 1
        except queue.Empty:
            break

    thread.join()
    print(f"\n[watcher] {os.path.basename(filepath)}: published {published} elements")
    await nc.close()


# ---------------------------------------------------------------------------
# Filesystem watcher (detects new PBF files dropped into data/)
# ---------------------------------------------------------------------------
class PBFHandler(FileSystemEventHandler):
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.processed: set[str] = set()

    def on_created(self, event):
        if event.src_path.endswith(".osm.pbf") and event.src_path not in self.processed:
            self.processed.add(event.src_path)
            asyncio.run_coroutine_threadsafe(publish_file(event.src_path), self.loop)


async def run():
    os.makedirs(DATA_DIR, exist_ok=True)

    # process existing files first
    for f in sorted(glob.glob(os.path.join(DATA_DIR, "*.osm.pbf"))):
        await publish_file(f)

    # then watch for new files
    loop = asyncio.get_event_loop()
    handler = PBFHandler(loop)
    observer = Observer()
    observer.schedule(handler, DATA_DIR, recursive=False)
    observer.start()
    print(f"[watcher] Watching {DATA_DIR} for new .osm.pbf files ...")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    asyncio.run(run())
