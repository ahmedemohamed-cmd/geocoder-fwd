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
BATCH_PUBLISH = 50  # Reduced from 100 to reduce load on NATS stream
QUEUE_MAXSIZE = 100_000


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
        self.node_count = 0
        self.way_count = 0
        self.relation_count = 0
        self.geom_factory = osmium.geom.GeoJSONFactory()

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
        self.node_count += 1

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
        self.way_count += 1

    def relation(self, r):
        if not r.tags:
            return
        tags = dict(r.tags)
        
        # Extract geometry for multipolygons and boundaries
        geom = None
        area = 0.0
        try:
            # Check if this is a multipolygon or boundary relation
            rel_type = tags.get("type", "")
            if rel_type in ("multipolygon", "boundary"):
                # Use osmium's geometry factory to create multipolygon geometry
                try:
                    geojson = self.geom_factory.create_multipolygon(r)
                    if geojson:
                        geom = json.loads(geojson)
                        # Calculate area for each polygon in the multipolygon
                        if geom.get("type") == "MultiPolygon":
                            for polygon in geom.get("coordinates", []):
                                if polygon and polygon[0]:
                                    coords = [(c[0], c[1]) for c in polygon[0]]
                                    area += _polygon_area_km2(coords)
                        elif geom.get("type") == "Polygon" and geom.get("coordinates"):
                            coords = [(c[0], c[1]) for c in geom["coordinates"][0]]
                            area = _polygon_area_km2(coords)
                except Exception:
                    pass
        except Exception:
            pass
        
        self.q.put(
            {
                "osm_id": f"r{r.id}",
                "osm_type": "relation",
                "tags": tags,
                "geom": geom,
                "admin_level": int(tags.get("admin_level", 0) or 0),
                "area_km2": area,
            }
        )
        self.count += 1
        self.relation_count += 1


# ---------------------------------------------------------------------------
# Publish a single PBF file to NATS (streaming via queue)
# ---------------------------------------------------------------------------
async def publish_file(filepath: str):
    nc, js = await nats_client.connect()
    
    # Ensure stream exists before publishing
    try:
        await js.stream_info(nats_client.NATS_STREAM)
    except Exception:
        print(f"[watcher] Stream {nats_client.NATS_STREAM} does not exist, creating it...")
        from nats.js.api import StreamConfig, RetentionPolicy
        await js.add_stream(
            StreamConfig(
                name=nats_client.NATS_STREAM,
                subjects=[nats_client.NATS_SUBJECT],
                retention=RetentionPolicy.LIMITS,
                max_age=86400,  # Keep messages for 24 hours (in seconds)
                max_bytes=10737418240,  # 10GB max storage
                storage="file",
                max_msg_size=1048576,  # 1MB max message size
                discard="old",  # Discard old messages when limits are reached
            )
        )
        print(f"[watcher] Stream {nats_client.NATS_STREAM} created successfully")
    
    q: queue.Queue = queue.Queue(maxsize=QUEUE_MAXSIZE)

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

        # Publish batch using batch publish for efficiency
        try:
            # Convert batch to JSON messages
            messages = [json.dumps(elem).encode() for elem in batch]
            
            # Publish all messages in the batch at once
            for msg in messages:
                max_retries = 5
                for attempt in range(max_retries):
                    try:
                        ack = await js.publish(NATS_SUBJECT, msg, timeout=120)
                        if ack:
                            published += 1
                            break  # Success, move to next message
                        else:
                            print(f"[watcher] No ack received for element (attempt {attempt + 1}/{max_retries})", flush=True)
                            if attempt < max_retries - 1:
                                await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                            else:
                                print(f"[watcher] Failed to publish element after {max_retries} attempts", flush=True)
                        # Longer delay to avoid overwhelming NATS
                        await asyncio.sleep(0.05)  # 50ms delay between publishes
                    except Exception as e:
                        print(f"[watcher] Error publishing element (attempt {attempt + 1}/{max_retries}): {e}", flush=True)
                        if attempt < max_retries - 1:
                            await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                        else:
                            print(f"[watcher] Failed to publish element after {max_retries} attempts: {e}", flush=True)
        except Exception as e:
            print(f"[watcher] Error in batch publishing: {e}", flush=True)
            await asyncio.sleep(0.1)
        
        if published % 5000 < BATCH_PUBLISH:
            print(f"\r[watcher] Published {published} ...", end="", flush=True)

    # flush anything left
    while True:
        try:
            item = q.get_nowait()
            if item is SENTINEL:
                break
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    ack = await js.publish(NATS_SUBJECT, json.dumps(item).encode(), timeout=120)
                    if ack:
                        published += 1
                        break  # Success, move to next item
                    else:
                        print(f"[watcher] No ack received during flush (attempt {attempt + 1}/{max_retries})", flush=True)
                        if attempt < max_retries - 1:
                            await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                        else:
                            print(f"[watcher] Failed to publish element during flush after {max_retries} attempts", flush=True)
                    await asyncio.sleep(0.05)  # Rate limiting (50ms delay)
                except Exception as e:
                    print(f"[watcher] Error publishing element during flush (attempt {attempt + 1}/{max_retries}): {e}", flush=True)
                    if attempt < max_retries - 1:
                        await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    else:
                        print(f"[watcher] Failed to publish element during flush after {max_retries} attempts: {e}", flush=True)
        except queue.Empty:
            break

    thread.join()
    print(f"\n[watcher] {os.path.basename(filepath)}: parsed {handler.count} elements (nodes: {handler.node_count}, ways: {handler.way_count}, relations: {handler.relation_count})")
    print(f"[watcher] {os.path.basename(filepath)}: published {published} elements")
    await nc.close()


# ---------------------------------------------------------------------------
# Filesystem watcher (detects new PBF files dropped into data/)
# ---------------------------------------------------------------------------
class PBFHandler(FileSystemEventHandler):
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.processed: set[str] = set()
        self.processing: set[str] = set()  # Track files currently being processed

    def on_created(self, event):
        if event.src_path.endswith(".osm.pbf") and event.src_path not in self.processed and event.src_path not in self.processing:
            self.processing.add(event.src_path)
            self.processed.add(event.src_path)
            asyncio.run_coroutine_threadsafe(self._process_with_cleanup(event.src_path), self.loop)
    
    async def _process_with_cleanup(self, filepath: str):
        try:
            await publish_file(filepath)
        finally:
            self.processing.discard(filepath)


async def run():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Use lock file to prevent reprocessing
    lock_file = os.path.join(DATA_DIR, ".watcher.lock")
    
    # process existing files first
    existing_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.osm.pbf")))
    for f in existing_files:
        # Create lock file specific to this PBF file
        file_lock = f"{f}.processed"
        if os.path.exists(file_lock):
            print(f"[watcher] Skipping {os.path.basename(f)} (already processed)")
            continue
        
        try:
            await publish_file(f)
            # Mark as processed
            with open(file_lock, 'w') as lock:
                lock.write(f"processed: {f}")
            print(f"[watcher] Completed {os.path.basename(f)}")
        except Exception as e:
            print(f"[watcher] Error processing {os.path.basename(f)}: {e}")
            continue

    print(f"[watcher] Completed processing all existing files. Exiting.")

    # Exit after processing (remove restart policy from docker-compose if needed)


if __name__ == "__main__":
    asyncio.run(run())
