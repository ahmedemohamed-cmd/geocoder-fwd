"""Watch data/ for OSM PBF files, parse them with osmium, publish elements to NATS JS."""

import asyncio
import glob
import json
import math
import os
import queue
import sys
import threading
import time

import osmium
import etcd3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from shared.config import DATA_DIR, NATS_SUBJECT, ETCD_HOST, ETCD_PORT
from shared import nats_client

SENTINEL = object()
BATCH_PUBLISH = 50  # Reduced from 100 to reduce load on NATS stream
QUEUE_MAXSIZE = 100_000


class ProgressTracker:
    """Simple progress tracker that logs updates at regular intervals."""
    def __init__(self, description: str, total: int = None, log_interval: int = 5):
        self.description = description
        self.total = total
        self.log_interval = log_interval  # seconds between log updates
        self.count = 0
        self.start_time = time.time()
        self.last_log_time = self.start_time

    def update(self, n: int = 1):
        """Update progress by n items."""
        self.count += n
        current_time = time.time()
        if current_time - self.last_log_time >= self.log_interval:
            self._log_progress()
            self.last_log_time = current_time

    def _log_progress(self):
        """Log current progress."""
        elapsed = time.time() - self.start_time
        if self.total and self.total > 0:
            percentage = (self.count / self.total) * 100
            rate = self.count / elapsed if elapsed > 0 else 0
            print(f"[{self.description}] {self.count}/{self.total} ({percentage:.1f}%) - {rate:.1f} items/sec")
        else:
            rate = self.count / elapsed if elapsed > 0 else 0
            print(f"[{self.description}] {self.count} items processed - {rate:.1f} items/sec")

    def close(self):
        """Final log when done."""
        self._log_progress()
        elapsed = time.time() - self.start_time
        print(f"[{self.description}] Completed in {elapsed:.1f} seconds")


def _has_identifiable_tags(tags: dict) -> bool:
    """Check if element has identifiable tags like name, address, reference, etc."""
    if not tags:
        return False

    # Check for name tags (any language, anywhere in key)
    for key in tags:
        if 'name' in key:
            return True

    # Check for address tags
    address_tags = ['addr:housenumber', 'addr:street', 'addr:postcode',
                    'addr:housename']
    for tag in address_tags:
        if tag in tags:
            return True

    # Check for reference tags
    ref_tags = ['ref', 'ref:1', 'ref:2', 'local_ref', 'nat_ref', 'int_ref',
                'iata', 'icao', 'pcode', 'phone', 'website', 'email']
    for tag in ref_tags:
        if tag in tags:
            return True

    # Check for other identifiable tags
    identifiable_tags = ['operator', 'brand', 'brand:wikidata', 'operator:wikidata',
                        'wikipedia', 'wikidata', 'description', 'note']
    for tag in identifiable_tags:
        if tag in tags:
            return True

    return False


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
    def __init__(self, q: queue.Queue, etcd_client, progress_tracker=None):
        super().__init__()
        self.q = q
        self.count = 0
        self.node_count = 0
        self.way_count = 0
        self.relation_count = 0
        self.geom_factory = osmium.geom.GeoJSONFactory()
        self.etcd = etcd_client
        self.etcd_prefix = "/osm/nodes/"
        self.progress_tracker = progress_tracker

    def _cache_node_location(self, node_id: int, lon: float, lat: float):
        """Cache node location in etcd."""
        try:
            key = f"{self.etcd_prefix}{node_id}"
            value = json.dumps({"lon": lon, "lat": lat})
            self.etcd.put(key, value)
        except Exception as e:
            print(f"[watcher] Error caching node {node_id} in etcd: {e}")

    def _get_node_location(self, node_id: int) -> tuple[float, float] | None:
        """Get node location from etcd."""
        try:
            key = f"{self.etcd_prefix}{node_id}"
            value, _ = self.etcd.get(key)
            if value:
                data = json.loads(value.decode())
                return data["lon"], data["lat"]
        except Exception as e:
            print(f"[watcher] Error getting node {node_id} from etcd: {e}")
        return None

    def node(self, n):
        if self.node_count == 0:
            print(f"[watcher] First node encountered: {n.id}")
        if self.node_count > 0 and self.node_count % 100000 == 0:
            print(f"[watcher] Processed {self.node_count} nodes...")

        if not n.location.valid():
            return

        # DON'T cache all nodes in etcd - this is too slow for large files
        # Instead, we'll use lazy caching in the way processor:
        # when a way can't be resolved from memory, we'll cache just the nodes we need

        # Only publish nodes with tags to the queue
        if not n.tags:
            return

        tags = dict(n.tags)

        # Skip nodes without identifiable tags
        if not _has_identifiable_tags(tags):
            return

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
        if self.progress_tracker is not None:
            self.progress_tracker.update(1)

    def way(self, w):
        try:
            if self.way_count == 0:
                print(f"[watcher] First way encountered: {w.id}")
            tags = dict(w.tags) if w.tags else {}

            # Skip ways without tags (they have no semantic meaning)
            if not tags:
                self.skipped_way_count = getattr(self, 'skipped_way_count', 0) + 1
                return

            # Skip ways without identifiable tags
            if not _has_identifiable_tags(tags):
                self.skipped_way_count = getattr(self, 'skipped_way_count', 0) + 1
                return
            
            coords = []
            
            # Use osmium's built-in node resolution
            try:
                for n in w.nodes:
                    if n.location.valid():
                        coords.append((n.lon, n.lat))
                    else:
                        # Node location not available - this way can't be fully resolved
                        if self.way_count < 10:
                            print(f"[watcher] Way {w.id}: Node {n.ref} has no location")
            except osmium.InvalidLocationError:
                # Node not in memory index - skip this way
                if self.way_count < 10:
                    print(f"[watcher] Way {w.id}: Skipped (nodes not in memory index)")
                self.skipped_way_count = getattr(self, 'skipped_way_count', 0) + 1
                return
        except Exception as e:
            print(f"[watcher] Error processing way {w.id}: {e}")
            return
        
        if len(coords) < 2:
            if self.way_count < 10:  # Only print first 10 skips to avoid spam
                print(f"[watcher] Way {w.id}: Skipped (not enough coordinates: {len(coords)})")
            self.skipped_way_count = getattr(self, 'skipped_way_count', 0) + 1
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
        if self.way_count <= 5:
            print(f"[watcher] Way {w.id}: Added to queue (total ways: {self.way_count})")
        if self.progress_tracker is not None:
            self.progress_tracker.update(1)

    def relation(self, r):
        if self.relation_count == 0:
            print(f"[watcher] First relation encountered: {r.id}")
        tags = dict(r.tags) if r.tags else {}

        # Extract geometry for multipolygons and boundaries
        geom = None
        area = 0.0
        try:
            # Check if this is a multipolygon or boundary relation
            rel_type = tags.get("type", "")
            if rel_type in ("multipolygon", "boundary"):
                # Check if relation has members before attempting geometry creation
                if not r.members:
                    if self.relation_count < 10:
                        print(f"[watcher] Relation {r.id}: Skipped (no members)")
                    self.skipped_relation_count = getattr(self, 'skipped_relation_count', 0) + 1
                    return

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
                    else:
                        if self.relation_count < 10:
                            print(f"[watcher] Relation {r.id}: Geometry factory returned None for type={rel_type}")
                        self.skipped_relation_count = getattr(self, 'skipped_relation_count', 0) + 1
                except TypeError as e:
                    # Handle pybind11 type casting error by skipping this relation
                    if "Unable to cast Python instance" in str(e):
                        if self.relation_count < 10:
                            print(f"[watcher] Relation {r.id}: Skipping due to pybind11 type casting error")
                        self.skipped_relation_count = getattr(self, 'skipped_relation_count', 0) + 1
                    else:
                        raise
                except Exception as e:
                    print(f"[watcher] Relation {r.id}: Error creating multipolygon geometry: {e}")
                    self.skipped_relation_count = getattr(self, 'skipped_relation_count', 0) + 1
            else:
                # Not a multipolygon or boundary relation
                if self.relation_count < 10:
                    print(f"[watcher] Relation {r.id}: Skipped (type={rel_type}, not multipolygon/boundary)")
                self.skipped_relation_count = getattr(self, 'skipped_relation_count', 0) + 1
        except Exception as e:
            print(f"[watcher] Relation {r.id}: Error processing relation: {e}")
            self.skipped_relation_count = getattr(self, 'skipped_relation_count', 0) + 1

        # Only add to queue if we have geometry
        if geom:
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
            if self.relation_count <= 5:
                print(f"[watcher] Relation {r.id}: Added to queue (total relations: {self.relation_count})")
            if self.progress_tracker is not None:
                self.progress_tracker.update(1)


# ---------------------------------------------------------------------------
# Publish a single PBF file to NATS (streaming via queue)
# ---------------------------------------------------------------------------
async def publish_file(filepath: str):
    nc, js = await nats_client.connect()
    
    # Ensure stream exists before publishing
    try:
        await js.stream_info(nats_client.NATS_STREAM)
        print(f"[watcher] Stream {nats_client.NATS_STREAM} already exists")
    except Exception:
        print(f"[watcher] Stream {nats_client.NATS_STREAM} does not exist, creating it...")
        from nats.js.api import StreamConfig, RetentionPolicy
        try:
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
        except Exception as e:
            print(f"[watcher] Failed to create stream {nats_client.NATS_STREAM}: {e}")
            # Try to delete and recreate if it exists in a bad state
            try:
                print(f"[watcher] Attempting to delete and recreate stream...")
                await js.delete_stream(nats_client.NATS_STREAM)
                await js.add_stream(
                    StreamConfig(
                        name=nats_client.NATS_STREAM,
                        subjects=[nats_client.NATS_SUBJECT],
                        retention=RetentionPolicy.LIMITS,
                        max_age=86400,
                        max_bytes=10737418240,
                        storage="file",
                        max_msg_size=1048576,
                        discard="old",
                    )
                )
                print(f"[watcher] Stream {nats_client.NATS_STREAM} recreated successfully")
            except Exception as e2:
                print(f"[watcher] Failed to recreate stream: {e2}")
                raise
    
    # Test publish to verify stream is working
    print(f"[watcher] Testing stream with a test message...")
    try:
        test_msg = json.dumps({"test": True, "osm_id": "test", "osm_type": "node"}).encode()
        ack = await js.publish(NATS_SUBJECT, test_msg, timeout=10)
        if ack:
            print(f"[watcher] Stream test successful")
        else:
            print(f"[watcher] Stream test failed: no ack received")
    except Exception as e:
        print(f"[watcher] Stream test failed with error: {e}")
        raise
    
    q: queue.Queue = queue.Queue(maxsize=QUEUE_MAXSIZE)

    # Initialize etcd client
    etcd_client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT)
    print(f"[watcher] Connected to etcd at {ETCD_HOST}:{ETCD_PORT}")

    # Create progress tracker for parsing (unknown total)
    parse_progress = ProgressTracker(f"Parsing {os.path.basename(filepath)}")

    handler = OSMHandler(q, etcd_client, progress_tracker=parse_progress)

    def _parse():
        # Use locations=True with idx="flex_mem" for node resolution
        # Note: For very large files, not all ways may be resolvable if they exceed memory capacity
        try:
            print(f"[watcher] Starting to parse {filepath}...")
            handler.apply_file(filepath, locations=True, idx="flex_mem")
            print(f"[watcher] Finished parsing {filepath}")
        except Exception as e:
            print(f"[watcher] Error during parsing: {e}")
            import traceback
            traceback.print_exc()
        finally:
            parse_progress.close()
            q.put(SENTINEL)

    thread = threading.Thread(target=_parse, daemon=True)
    thread.start()
    print(f"[watcher] Parsing {os.path.basename(filepath)} ...")

    published = 0
    total_elements = None
    publish_progress = None
    publishing_started = False
    consecutive_failures = 0
    max_consecutive_failures = 50  # Fail fast after too many consecutive failures

    try:
        while True:
            batch: list[dict] = []
            try:
                item = q.get(timeout=0.5)
                if item is SENTINEL:
                    print(f"[watcher] Received SENTINEL, exiting loop")
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

            # Initialize publishing progress tracker when we start publishing
            if not publishing_started and len(batch) > 0:
                publish_progress = ProgressTracker(f"Publishing {os.path.basename(filepath)}")
                publishing_started = True

            # Update total for publishing progress tracker when parsing completes
            if total_elements is None and not thread.is_alive() and parse_progress.count > 0:
                total_elements = handler.count
                if total_elements > 0 and publish_progress is not None:
                    publish_progress.total = total_elements

            # Publish batch using batch publish for efficiency
            try:
                # Convert batch to JSON messages
                messages = [json.dumps(elem).encode() for elem in batch]

                # Publish all messages in the batch at once
                for msg in messages:
                    max_retries = 300
                    for attempt in range(max_retries):
                        try:
                            ack = await js.publish(NATS_SUBJECT, msg, timeout=120)
                            if ack:
                                published += 1
                                consecutive_failures = 0  # Reset on success
                                if publish_progress is not None:
                                    publish_progress.update(1)
                                break  # Success, move to next message
                            else:
                                consecutive_failures += 1
                                print(f"[watcher] No ack received for element (attempt {attempt + 1}/{max_retries})", flush=True)
                                if consecutive_failures >= max_consecutive_failures:
                                    print(f"[watcher] Too many consecutive failures ({consecutive_failures}), giving up", flush=True)
                                    raise Exception("Too many consecutive NATS failures")
                                if attempt < max_retries - 1:
                                    await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                                else:
                                    print(f"[watcher] Failed to publish element after {max_retries} attempts", flush=True)
                            # Longer delay to avoid overwhelming NATS
                            await asyncio.sleep(0.05)  # 50ms delay between publishes
                        except Exception as e:
                            consecutive_failures += 1
                            print(f"[watcher] Error publishing element (attempt {attempt + 1}/{max_retries}): {e}", flush=True)
                            if consecutive_failures >= max_consecutive_failures:
                                print(f"[watcher] Too many consecutive failures ({consecutive_failures}), giving up", flush=True)
                                raise
                            if attempt < max_retries - 1:
                                await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                            else:
                                print(f"[watcher] Failed to publish element after {max_retries} attempts: {e}", flush=True)
            except Exception as e:
                print(f"[watcher] Error in batch publishing: {e}", flush=True)
                await asyncio.sleep(0.1)

    except Exception as e:
        print(f"[watcher] Fatal error during publishing: {e}", flush=True)
        import traceback
        traceback.print_exc()
        # Continue with cleanup even if publishing failed

    # flush anything left
    while True:
        try:
            item = q.get_nowait()
            if item is SENTINEL:
                break
            max_retries = 500
            for attempt in range(max_retries):
                try:
                    ack = await js.publish(NATS_SUBJECT, json.dumps(item).encode(), timeout=120)
                    if ack:
                        published += 1
                        consecutive_failures = 0  # Reset on success
                        if publish_progress is not None:
                            publish_progress.update(1)
                        break  # Success, move to next item
                    else:
                        consecutive_failures += 1
                        print(f"[watcher] No ack received during flush (attempt {attempt + 1}/{max_retries})", flush=True)
                        if consecutive_failures >= max_consecutive_failures:
                            print(f"[watcher] Too many consecutive failures during flush ({consecutive_failures}), skipping remaining items", flush=True)
                            break  # Skip remaining items in flush
                        if attempt < max_retries - 1:
                            await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                        else:
                            print(f"[watcher] Failed to publish element during flush after {max_retries} attempts", flush=True)
                    await asyncio.sleep(0.05)  # Rate limiting (50ms delay)
                except Exception as e:
                    consecutive_failures += 1
                    print(f"[watcher] Error publishing element during flush (attempt {attempt + 1}/{max_retries}): {e}", flush=True)
                    if consecutive_failures >= max_consecutive_failures:
                        print(f"[watcher] Too many consecutive failures during flush ({consecutive_failures}), skipping remaining items", flush=True)
                        break  # Skip remaining items in flush
                    if attempt < max_retries - 1:
                        await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    else:
                        print(f"[watcher] Failed to publish element during flush after {max_retries} attempts: {e}", flush=True)
            # If we had too many consecutive failures, break out of flush loop
            if consecutive_failures >= max_consecutive_failures:
                print(f"[watcher] Skipping remaining items in queue due to consecutive failures", flush=True)
                break
        except queue.Empty:
            break

    thread.join()

    # Close publishing progress tracker
    if publish_progress is not None:
        publish_progress.close()

    skipped_ways = getattr(handler, 'skipped_way_count', 0)
    skipped_relations = getattr(handler, 'skipped_relation_count', 0)
    print(f"\n[watcher] {os.path.basename(filepath)}: parsed {handler.count} elements (nodes: {handler.node_count}, ways: {handler.way_count}, relations: {handler.relation_count})")
    if skipped_ways > 0:
        print(f"[watcher] {os.path.basename(filepath)}: skipped {skipped_ways} ways (insufficient coordinates)")
    if skipped_relations > 0:
        print(f"[watcher] {os.path.basename(filepath)}: skipped {skipped_relations} relations (no geometry)")
    print(f"[watcher] {os.path.basename(filepath)}: published {published} elements")
    
    # Clear etcd cache after processing to free up space
    try:
        print(f"[watcher] Clearing etcd cache...")
        etcd_client.delete_prefix(handler.etcd_prefix)
        print(f"[watcher] Etcd cache cleared")
    except Exception as e:
        print(f"[watcher] Error clearing etcd cache: {e}")
    
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
    print("[watcher] Starting watcher service...")
    os.makedirs(DATA_DIR, exist_ok=True)

    # Use lock file to prevent reprocessing
    lock_file = os.path.join(DATA_DIR, ".watcher.lock")
    
    # process existing files first
    existing_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.osm.pbf")))
    print(f"[watcher] Found {len(existing_files)} PBF files to process")
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
