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
import redis
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from shared.config import DATA_DIR, NATS_SUBJECT, REDIS_HOST, REDIS_PORT
from shared import nats_client

SENTINEL = object()
BATCH_PUBLISH = 50  # Reduced from 100 to reduce load on NATS stream
QUEUE_MAXSIZE = 100_000

# Target 60 MB — leaves 4 MB headroom under the 64 MB NATS server ceiling.
# Simplification is only applied to messages that exceed this threshold, so
# normal elements (nodes, ways, small relations) are never touched.
_NATS_TARGET_BYTES = 60 * 1024 * 1024


# ---------------------------------------------------------------------------
# Geometry simplification helpers (no external dependencies)
# Used only for very large relation geometries that approach the NATS limit.
# ---------------------------------------------------------------------------

def _round_coords(obj, decimals: int):
    """Recursively round every float in a GeoJSON coordinates tree."""
    if isinstance(obj, float):
        return round(obj, decimals)
    if isinstance(obj, int):
        return obj
    return [_round_coords(x, decimals) for x in obj]


def _thin_ring(ring: list, stride: int) -> list:
    """Keep every *stride*-th point in a coordinate ring.

    Guarantees a valid closed ring (≥ 4 points, first == last).
    Returns the original ring unchanged if thinning would make it degenerate.
    """
    if len(ring) < 4:
        return ring
    thinned = ring[::stride]
    if thinned[0] != thinned[-1]:
        thinned.append(thinned[0])
    return thinned if len(thinned) >= 4 else ring


def _thin_geom(coords, gtype: str, stride: int):
    """Apply stride-based point thinning to a GeoJSON coordinates array."""
    if gtype == "Polygon":
        return [_thin_ring(ring, stride) for ring in coords]
    if gtype == "MultiPolygon":
        return [[_thin_ring(ring, stride) for ring in poly] for poly in coords]
    if gtype == "LineString":
        thinned = coords[::stride]
        return thinned if len(thinned) >= 2 else coords
    return coords


def _simplify_geom(geom: dict, osm_id: str) -> dict | None:
    """Reduce a geometry's JSON footprint to fit within _NATS_TARGET_BYTES.

    Strategy (applied in order, stopping as soon as the target is met):
      1. Round coordinates to 5 dp  (~1.1 m precision)
      2. Round to 4 dp              (~11 m precision)
      3. Round to 3 dp              (~111 m precision)
      4. Thin points: stride 2, 4, 8, 16  (still at 3 dp)

    Returns the simplified geometry, or None if the target cannot be met
    (caller will then skip the element and log a warning).
    """
    TARGET = _NATS_TARGET_BYTES - 512   # 512 B headroom for the rest of the message
    gtype = geom.get("type", "")
    coords = geom.get("coordinates")
    if not coords:
        return None

    for decimals in (5, 4, 3):
        coords = _round_coords(coords, decimals)
        candidate = {"type": gtype, "coordinates": coords}
        if len(json.dumps(candidate).encode()) <= TARGET:
            return candidate

    for stride in (2, 4, 8, 16):
        coords = _thin_geom(coords, gtype, stride)
        candidate = {"type": gtype, "coordinates": coords}
        sz = len(json.dumps(candidate).encode())
        if sz <= TARGET:
            print(
                f"[watcher] {osm_id}: geometry simplified to {sz / 1_048_576:.1f} MB "
                f"(3 dp, stride {stride})",
                flush=True,
            )
            return candidate

    final_sz = len(json.dumps({"type": gtype, "coordinates": coords}).encode())
    print(
        f"[watcher] {osm_id}: geometry still {final_sz / 1_048_576:.1f} MB after "
        f"maximum simplification — element will be skipped",
        flush=True,
    )
    return None


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
# First-pass handler: collect way IDs that are members of boundary/multipolygon
# relations so we can cache their coordinates during the second pass.
# ---------------------------------------------------------------------------
class RelationMemberCollector(osmium.SimpleHandler):
    """Lightweight first pass over the PBF file.

    Only implements relation() — nodes and ways are skipped automatically,
    making this pass very fast (just reads relation blocks).
    """

    def __init__(self):
        super().__init__()
        self.way_ids: set[int] = set()
        self.relation_count = 0

    def relation(self, r):
        tags = dict(r.tags) if r.tags else {}
        rel_type = tags.get("type", "")
        if rel_type in ("multipolygon", "boundary"):
            for member in r.members:
                if member.type == "w":
                    self.way_ids.add(member.ref)
            self.relation_count += 1


# ---------------------------------------------------------------------------
# Geometry helpers for assembling multipolygon relations from cached way coords
# ---------------------------------------------------------------------------
def _assemble_rings(
    way_coords_list: list[list[tuple[float, float]]],
) -> list[list[tuple[float, float]]]:
    """Join way coordinate sequences end-to-end into closed rings.

    OSM multipolygon relations often split a single ring across multiple way
    members. This function chains them (forward or reversed) until the ring
    closes, then moves on to the next ring.
    """
    if not way_coords_list:
        return []

    rings: list[list[tuple[float, float]]] = []
    remaining = [list(wc) for wc in way_coords_list]

    while remaining:
        ring = remaining.pop(0)

        changed = True
        while changed and ring[0] != ring[-1]:
            changed = False
            for i, way in enumerate(remaining):
                if way[0] == ring[-1]:
                    ring.extend(way[1:])
                    remaining.pop(i)
                    changed = True
                    break
                if way[-1] == ring[-1]:
                    ring.extend(list(reversed(way))[1:])
                    remaining.pop(i)
                    changed = True
                    break
                if way[-1] == ring[0]:
                    ring = way[:-1] + ring
                    remaining.pop(i)
                    changed = True
                    break
                if way[0] == ring[0]:
                    ring = list(reversed(way))[:-1] + ring
                    remaining.pop(i)
                    changed = True
                    break

        # Close ring if not already closed
        if ring[0] != ring[-1]:
            ring.append(ring[0])

        if len(ring) >= 4:
            rings.append(ring)

    return rings


def _assemble_multipolygon(
    members, way_coords: dict[int, list[tuple[float, float]]]
) -> tuple[dict | None, float]:
    """Build a GeoJSON geometry + area from relation members and cached way coords."""
    outer_ways: list[list[tuple[float, float]]] = []
    inner_ways: list[list[tuple[float, float]]] = []

    for member in members:
        if member.type == "w" and member.ref in way_coords:
            coords = way_coords[member.ref]
            if member.role == "inner":
                inner_ways.append(coords)
            else:                       # "outer" or "" (default to outer)
                outer_ways.append(coords)

    if not outer_ways:
        return None, 0.0

    outer_rings = _assemble_rings(outer_ways)
    inner_rings = _assemble_rings(inner_ways)

    if not outer_rings:
        return None, 0.0

    area = 0.0
    polygons: list[list[list[tuple[float, float]]]] = []
    for outer in outer_rings:
        polygons.append([outer])
        area += _polygon_area_km2(outer)

    # Assign inner rings (holes) to first polygon.  Virtually all OSM boundary
    # relations have a single outer ring, so this simplified assignment works.
    for inner in inner_rings:
        if polygons:
            polygons[0].append(inner)

    if len(polygons) == 1:
        geom: dict = {"type": "Polygon", "coordinates": polygons[0]}
    else:
        geom = {"type": "MultiPolygon", "coordinates": polygons}

    return geom, area


# ---------------------------------------------------------------------------
# OSM PBF handler – pushes parsed elements into a thread-safe queue
# ---------------------------------------------------------------------------
class OSMHandler(osmium.SimpleHandler):
    def __init__(self, q: queue.Queue, redis_client, progress_tracker=None,
                 relation_way_ids: set[int] | None = None):
        super().__init__()
        self.q = q
        self.count = 0
        self.node_count = 0
        self.way_count = 0
        self.relation_count = 0
        self.redis = redis_client
        self.redis_prefix = "osm:nodes:"
        self.progress_tracker = progress_tracker
        # Set of way IDs that are members of multipolygon/boundary relations.
        # Populated by the first-pass RelationMemberCollector.
        self.relation_way_ids = relation_way_ids or set()
        # Cached way coordinates keyed by way ID (for relation assembly).
        self.way_coords: dict[int, list[tuple[float, float]]] = {}

    def _cache_node_location(self, node_id: int, lon: float, lat: float):
        """Cache node location in Redis."""
        try:
            key = f"{self.redis_prefix}{node_id}"
            value = json.dumps({"lon": lon, "lat": lat})
            self.redis.set(key, value)
        except Exception as e:
            print(f"[watcher] Error caching node {node_id} in Redis: {e}")

    def _get_node_location(self, node_id: int) -> tuple[float, float] | None:
        """Get node location from Redis."""
        try:
            key = f"{self.redis_prefix}{node_id}"
            value = self.redis.get(key)
            if value:
                data = json.loads(value)
                return data["lon"], data["lat"]
        except Exception as e:
            print(f"[watcher] Error getting node {node_id} from Redis: {e}")
        return None

    def node(self, n):
        if self.node_count == 0:
            print(f"[watcher] First node encountered: {n.id}")
        if self.node_count > 0 and self.node_count % 100000 == 0:
            print(f"[watcher] Processed {self.node_count} nodes...")

        if not n.location.valid():
            return

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

            # --- Extract coordinates first (needed for relation-member caching) ---
            coords: list[tuple[float, float]] = []
            for n in w.nodes:
                try:
                    if n.location.valid():
                        coords.append((n.lon, n.lat))
                except osmium.InvalidLocationError:
                    continue  # node not in location index, skip it

            # Cache coordinates for ways that are members of relations.
            # Must happen *before* the tag filter because relation-member ways
            # often carry no tags of their own (the tags live on the relation).
            if self.relation_way_ids and w.id in self.relation_way_ids and len(coords) >= 2:
                self.way_coords[w.id] = coords

            # --- Tag filtering (for ways published on their own) ---
            tags = dict(w.tags) if w.tags else {}

            if not tags:
                self.skipped_way_count = getattr(self, 'skipped_way_count', 0) + 1
                return

            if not _has_identifiable_tags(tags):
                self.skipped_way_count = getattr(self, 'skipped_way_count', 0) + 1
                return

            if len(coords) < 2:
                if self.way_count < 10:
                    print(f"[watcher] Way {w.id}: Skipped (not enough coordinates: {len(coords)})")
                self.skipped_way_count = getattr(self, 'skipped_way_count', 0) + 1
                return
        except Exception as e:
            print(f"[watcher] Error processing way {w.id}: {e}")
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

        rel_type = tags.get("type", "")
        if rel_type not in ("multipolygon", "boundary"):
            if self.relation_count < 10:
                print(f"[watcher] Relation {r.id}: Skipped (type={rel_type})")
            self.skipped_relation_count = getattr(self, 'skipped_relation_count', 0) + 1
            return

        # Assemble geometry from cached way coordinates (populated during way pass)
        try:
            geom, area = _assemble_multipolygon(r.members, self.way_coords)
        except Exception as e:
            print(f"[watcher] Relation {r.id}: Error assembling geometry: {e}")
            geom, area = None, 0.0

        # Fallback: if full polygon assembly failed, compute a centroid from
        # whatever member-way coordinates we do have.  This ensures important
        # boundary relations (countries, governorates) are still indexed even
        # when some member ways are incomplete at the extract boundary.
        if geom is None:
            all_coords: list[tuple[float, float]] = []
            for member in r.members:
                if member.type == "w" and member.ref in self.way_coords:
                    all_coords.extend(self.way_coords[member.ref])
            if all_coords:
                avg_lon = sum(c[0] for c in all_coords) / len(all_coords)
                avg_lat = sum(c[1] for c in all_coords) / len(all_coords)
                geom = {"type": "Point", "coordinates": [avg_lon, avg_lat]}
                area = 0.0
            else:
                if self.relation_count < 10:
                    print(f"[watcher] Relation {r.id}: No way coords available, skipping")
                self.skipped_relation_count = getattr(self, 'skipped_relation_count', 0) + 1
                return

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
                    max_msg_size=-1,  # unlimited
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
                        max_msg_size=-1,  # unlimited
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

    # Initialize Redis client
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        redis_client.ping()
        print(f"[watcher] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        print(f"[watcher] Warning: Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT}: {e}")
        print(f"[watcher] Continuing without Redis caching")

    # ------------------------------------------------------------------
    # PASS 1 — lightweight scan of relations to collect member way IDs
    # ------------------------------------------------------------------
    print(f"[watcher] Pass 1: scanning relations in {os.path.basename(filepath)} ...")
    collector = RelationMemberCollector()
    collector.apply_file(filepath)
    relation_way_ids = collector.way_ids
    print(f"[watcher] Pass 1 complete: {collector.relation_count} boundary/multipolygon "
          f"relations reference {len(relation_way_ids)} unique ways")

    # ------------------------------------------------------------------
    # PASS 2 — full parse (nodes + ways + relations) with location index
    # ------------------------------------------------------------------
    parse_progress = ProgressTracker(f"Parsing {os.path.basename(filepath)}")

    handler = OSMHandler(q, redis_client, progress_tracker=parse_progress,
                         relation_way_ids=relation_way_ids)

    def _parse():
        try:
            print(f"[watcher] Pass 2: parsing {filepath} ...")
            handler.apply_file(filepath, locations=True, idx="flex_mem")
            print(f"[watcher] Pass 2 complete for {filepath}")
        except Exception as e:
            print(f"[watcher] Error during parsing: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Free the (potentially large) way-coord cache now that all
            # relations have been processed.
            handler.way_coords.clear()
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
                # Serialize + optional geometry simplification.
                # For the vast majority of elements this is just json.dumps.
                # Only large relation geometries that approach the 64 MB server
                # ceiling trigger _simplify_geom.
                messages: list[tuple[bytes, str]] = []   # (payload, osm_id)
                for elem in batch:
                    raw = json.dumps(elem).encode()
                    if len(raw) > _NATS_TARGET_BYTES and elem.get("geom"):
                        simplified = _simplify_geom(elem["geom"], elem.get("osm_id", "?"))
                        if simplified is None:
                            continue   # too large even after max reduction — skip
                        raw = json.dumps({**elem, "geom": simplified}).encode()
                    messages.append((raw, elem.get("osm_id", "?")))

                # Publish all messages in the batch
                for msg, osm_id in messages:
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
                        except Exception as e:
                            err_str = str(e).lower()
                            # Payload-too-large: shouldn't reach here after the
                            # pre-flight check, but handle defensively — try one
                            # more simplification pass then skip if still too big.
                            if (
                                "maximum payload" in err_str
                                or "payload exceeded" in err_str
                                or "message size exceeds" in err_str
                                or "10054" in err_str
                            ):
                                # Attempt emergency simplification
                                try:
                                    elem_dict = json.loads(msg)
                                    if elem_dict.get("geom"):
                                        simplified = _simplify_geom(
                                            elem_dict["geom"], osm_id
                                        )
                                        if simplified is not None:
                                            msg = json.dumps(
                                                {**elem_dict, "geom": simplified}
                                            ).encode()
                                            continue   # retry with smaller payload
                                except Exception:
                                    pass
                                print(
                                    f"[watcher] {osm_id}: payload too large "
                                    f"({len(msg):,} bytes) — skipping",
                                    flush=True,
                                )
                                break
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
    
    # Clear Redis cache after processing to free up space
    try:
        print(f"[watcher] Clearing Redis cache...")
        cursor = 0
        deleted = 0
        while True:
            cursor, keys = redis_client.scan(cursor, match=f"{handler.redis_prefix}*", count=1000)
            if keys:
                redis_client.delete(*keys)
                deleted += len(keys)
            if cursor == 0:
                break
        if deleted > 0:
            print(f"[watcher] Redis cache cleared ({deleted} keys)")
        else:
            print(f"[watcher] Redis cache was empty")
    except Exception as e:
        print(f"[watcher] Error clearing Redis cache: {e}")
    
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
