"""Watch data/ for OSM PBF files, parse them with osmium, publish elements to NATS JS."""

import asyncio
import glob
import json
import math
import os
import queue
import threading
import time

import osmium

from shared import nats_client
from shared.config import DATA_DIR, NATS_SUBJECT, REDIS_HOST, REDIS_PORT, WATCH_POLL_INTERVAL
from shared.logging import get_logger
from shared.processed import is_processed, load_processed, record_processed
from shared.redis_client import make_redis
from shared.valhalla import link_pbf_for_valhalla

logger = get_logger("watcher")

SENTINEL = object()
BATCH_PUBLISH = 50  # Reduced from 100 to reduce load on NATS stream
QUEUE_MAXSIZE = 100_000

# JetStream has a hard 32 MB RAFT-entry limit (err_code 10077) that cannot be
# overridden by max_payload or max_msg_size.  Target 30 MB to leave headroom.
# Simplification is only applied to messages that exceed this threshold, so
# normal elements (nodes, ways, small relations) are never touched.
_NATS_TARGET_BYTES = 30 * 1024 * 1024


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
    TARGET = _NATS_TARGET_BYTES - 512  # 512 B headroom for the rest of the message
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
            logger.info(
                "[watcher] %s: geometry simplified to %s MB (3 dp, stride %s)",
                osm_id,
                format(sz / 1_048_576, ".1f"),
                stride,
            )
            return candidate

    final_sz = len(json.dumps({"type": gtype, "coordinates": coords}).encode())
    logger.warning(
        "[watcher] %s: geometry still %s MB after maximum simplification — element will be skipped",
        osm_id,
        format(final_sz / 1_048_576, ".1f"),
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
            logger.info(
                "[%s] %s/%s (%s%%) - %s items/sec",
                self.description,
                self.count,
                self.total,
                format(percentage, ".1f"),
                format(rate, ".1f"),
            )
        else:
            rate = self.count / elapsed if elapsed > 0 else 0
            logger.info(
                "[%s] %s items processed - %s items/sec",
                self.description,
                self.count,
                format(rate, ".1f"),
            )

    def close(self):
        """Final log when done."""
        self._log_progress()
        elapsed = time.time() - self.start_time
        logger.info("[%s] Completed in %s seconds", self.description, format(elapsed, ".1f"))


def _safe_admin_level(tags: dict) -> int | None:
    """Return admin_level as int, or None when the tag is absent or non-numeric."""
    raw = tags.get("admin_level")
    if raw is None:
        return None
    try:
        return int(raw)
    except (ValueError, TypeError):
        return None


def _has_identifiable_tags(tags: dict) -> bool:
    """Check if element has identifiable tags like name, address, reference, etc."""
    if not tags:
        return False

    # Check for name tags (any language, anywhere in key)
    for key in tags:
        if "name" in key:
            return True

    # Check for address tags
    address_tags = ["addr:housenumber", "addr:street", "addr:postcode", "addr:housename"]
    for tag in address_tags:
        if tag in tags:
            return True

    # Check for reference tags
    ref_tags = [
        "ref",
        "ref:1",
        "ref:2",
        "local_ref",
        "nat_ref",
        "int_ref",
        "iata",
        "icao",
        "pcode",
        "phone",
        "website",
        "email",
    ]
    for tag in ref_tags:
        if tag in tags:
            return True

    # Check for other identifiable tags
    identifiable_tags = [
        "operator",
        "brand",
        "brand:wikidata",
        "operator:wikidata",
        "wikipedia",
        "wikidata",
        "description",
        "note",
    ]
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
            else:  # "outer" or "" (default to outer)
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
    def __init__(
        self,
        q: queue.Queue,
        redis_client,
        progress_tracker=None,
        relation_way_ids: set[int] | None = None,
    ):
        super().__init__()
        self.q = q
        self.count = 0
        self.node_count = 0
        self.way_count = 0
        self.relation_count = 0
        self.skipped_way_count = 0
        self.skipped_relation_count = 0
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
            logger.error("[watcher] Error caching node %s in Redis: %s", node_id, e)

    def _get_node_location(self, node_id: int) -> tuple[float, float] | None:
        """Get node location from Redis."""
        try:
            key = f"{self.redis_prefix}{node_id}"
            value = self.redis.get(key)
            if value:
                data = json.loads(value)
                return data["lon"], data["lat"]
        except Exception as e:
            logger.error("[watcher] Error getting node %s from Redis: %s", node_id, e)
        return None

    def node(self, n):
        if self.node_count == 0:
            logger.info("[watcher] First node encountered: %s", n.id)
        if self.node_count > 0 and self.node_count % 100000 == 0:
            logger.info("[watcher] Processed %s nodes...", self.node_count)

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
                "admin_level": _safe_admin_level(tags),
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
                logger.info("[watcher] First way encountered: %s", w.id)

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
                self.skipped_way_count += 1
                return

            if not _has_identifiable_tags(tags):
                self.skipped_way_count += 1
                return

            if len(coords) < 2:
                if self.way_count < 10:
                    logger.warning(
                        "[watcher] Way %s: Skipped (not enough coordinates: %s)", w.id, len(coords)
                    )
                self.skipped_way_count += 1
                return
        except Exception as e:
            logger.error("[watcher] Error processing way %s: %s", w.id, e)
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
                "admin_level": _safe_admin_level(tags),
                "area_km2": area,
            }
        )
        self.count += 1
        self.way_count += 1
        if self.way_count <= 5:
            logger.info("[watcher] Way %s: Added to queue (total ways: %s)", w.id, self.way_count)
        if self.progress_tracker is not None:
            self.progress_tracker.update(1)

    def relation(self, r):
        if self.relation_count == 0:
            logger.info("[watcher] First relation encountered: %s", r.id)
        tags = dict(r.tags) if r.tags else {}

        rel_type = tags.get("type", "")
        if rel_type not in ("multipolygon", "boundary"):
            if self.relation_count < 10:
                logger.info("[watcher] Relation %s: Skipped (type=%s)", r.id, rel_type)
            self.skipped_relation_count += 1
            return

        # Assemble geometry from cached way coordinates (populated during way pass)
        try:
            geom, area = _assemble_multipolygon(r.members, self.way_coords)
        except Exception as e:
            logger.error("[watcher] Relation %s: Error assembling geometry: %s", r.id, e)
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
                    logger.warning("[watcher] Relation %s: No way coords available, skipping", r.id)
                self.skipped_relation_count += 1
                return

        self.q.put(
            {
                "osm_id": f"r{r.id}",
                "osm_type": "relation",
                "tags": tags,
                "geom": geom,
                "admin_level": _safe_admin_level(tags),
                "area_km2": area,
            }
        )
        self.count += 1
        self.relation_count += 1
        if self.relation_count <= 5:
            logger.info(
                "[watcher] Relation %s: Added to queue (total relations: %s)",
                r.id,
                self.relation_count,
            )
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
        logger.info("[watcher] Stream %s already exists", nats_client.NATS_STREAM)
    except Exception:
        logger.info("[watcher] Stream %s does not exist, creating it...", nats_client.NATS_STREAM)
        try:
            await js.add_stream(nats_client.OSM_STREAM_CFG)
            logger.info("[watcher] Stream %s created successfully", nats_client.NATS_STREAM)
        except Exception as e:
            logger.error("[watcher] Failed to create stream %s: %s", nats_client.NATS_STREAM, e)
            # Try to delete and recreate if it exists in a bad state
            try:
                logger.info("[watcher] Attempting to delete and recreate stream...")
                await js.delete_stream(nats_client.NATS_STREAM)
                await js.add_stream(nats_client.OSM_STREAM_CFG)
                logger.info("[watcher] Stream %s recreated successfully", nats_client.NATS_STREAM)
            except Exception as e2:
                logger.error("[watcher] Failed to recreate stream: %s", e2)
                raise

    q: queue.Queue = queue.Queue(maxsize=QUEUE_MAXSIZE)

    # Initialize Redis client (standalone or cluster, per REDIS_MODE)
    redis_client = make_redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        redis_client.ping()
        logger.info("[watcher] Connected to Redis at %s:%s", REDIS_HOST, REDIS_PORT)
    except Exception as e:
        logger.warning(
            "[watcher] Warning: Could not connect to Redis at %s:%s: %s", REDIS_HOST, REDIS_PORT, e
        )
        logger.info("[watcher] Continuing without Redis caching")

    # ------------------------------------------------------------------
    # PASS 1 — lightweight scan of relations to collect member way IDs
    # ------------------------------------------------------------------
    logger.info("[watcher] Pass 1: scanning relations in %s ...", os.path.basename(filepath))
    collector = RelationMemberCollector()
    collector.apply_file(filepath)
    relation_way_ids = collector.way_ids
    logger.info(
        "[watcher] Pass 1 complete: %s boundary/multipolygon relations reference %s unique ways",
        collector.relation_count,
        len(relation_way_ids),
    )

    # ------------------------------------------------------------------
    # PASS 2 — full parse (nodes + ways + relations) with location index
    # ------------------------------------------------------------------
    parse_progress = ProgressTracker(f"Parsing {os.path.basename(filepath)}")

    handler = OSMHandler(
        q, redis_client, progress_tracker=parse_progress, relation_way_ids=relation_way_ids
    )

    parse_error: list[Exception] = []  # mutable container shared with thread

    def _parse():
        try:
            logger.info("[watcher] Pass 2: parsing %s ...", filepath)
            handler.apply_file(filepath, locations=True, idx="flex_mem")
            logger.info("[watcher] Pass 2 complete for %s", filepath)
        except Exception as e:
            logger.error("[watcher] Error during parsing: %s", e)
            import traceback

            traceback.print_exc()
            parse_error.append(e)
        finally:
            # Free the (potentially large) way-coord cache now that all
            # relations have been processed.
            handler.way_coords.clear()
            parse_progress.close()
            q.put(SENTINEL)

    thread = threading.Thread(target=_parse, daemon=True)
    thread.start()
    logger.info("[watcher] Parsing %s ...", os.path.basename(filepath))

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
                    logger.info("[watcher] Received SENTINEL, exiting loop")
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
                messages: list[tuple[bytes, str]] = []  # (payload, osm_id)
                for elem in batch:
                    raw = json.dumps(elem).encode()
                    if len(raw) > _NATS_TARGET_BYTES and elem.get("geom"):
                        simplified = _simplify_geom(elem["geom"], elem.get("osm_id", "?"))
                        if simplified is None:
                            continue  # too large even after max reduction — skip
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
                                logger.info(
                                    "[watcher] No ack received for element (attempt %s/%s)",
                                    attempt + 1,
                                    max_retries,
                                )
                                if consecutive_failures >= max_consecutive_failures:
                                    logger.error(
                                        "[watcher] Too many consecutive failures (%s), giving up",
                                        consecutive_failures,
                                    )
                                    raise Exception("Too many consecutive NATS failures")
                                if attempt < max_retries - 1:
                                    await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                                else:
                                    logger.error(
                                        "[watcher] Failed to publish element after %s attempts",
                                        max_retries,
                                    )
                        except Exception as e:
                            err_str = str(e).lower()
                            # Payload-too-large is permanent — skip immediately.
                            if (
                                "maximum payload" in err_str
                                or "payload exceeded" in err_str
                                or "message size exceeds" in err_str
                                or "to large" in err_str  # NATS typo in 10077
                                or "too large" in err_str
                                or "10054" in err_str
                                or "10077" in err_str
                            ):
                                logger.warning(
                                    "[watcher] %s: payload too large (%s bytes) — skipping",
                                    osm_id,
                                    format(len(msg), ","),
                                )
                                break
                            consecutive_failures += 1
                            logger.error(
                                "[watcher] Error publishing element (attempt %s/%s): %s",
                                attempt + 1,
                                max_retries,
                                e,
                            )
                            if consecutive_failures >= max_consecutive_failures:
                                logger.error(
                                    "[watcher] Too many consecutive failures (%s), giving up",
                                    consecutive_failures,
                                )
                                raise
                            if attempt < max_retries - 1:
                                await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                            else:
                                logger.error(
                                    "[watcher] Failed to publish element after %s attempts: %s",
                                    max_retries,
                                    e,
                                )
            except Exception as e:
                logger.error("[watcher] Error in batch publishing: %s", e)
                await asyncio.sleep(0.1)

    except Exception as e:
        logger.error("[watcher] Fatal error during publishing: %s", e)
        import traceback

        traceback.print_exc()
        # Continue with cleanup even if publishing failed

    # flush anything left
    consecutive_failures = 0  # reset: don't carry over failures from the main loop
    while True:
        try:
            item = q.get_nowait()
            if item is SENTINEL:
                break
            # Apply the same geometry simplification as the main publish loop
            msg = json.dumps(item).encode()
            if len(msg) > _NATS_TARGET_BYTES and item.get("geom"):
                simplified = _simplify_geom(item["geom"], item.get("osm_id", "?"))
                if simplified is None:
                    logger.warning(
                        "[watcher] Flush: %s too large after simplification — skipping",
                        item.get("osm_id", "?"),
                    )
                    continue
                msg = json.dumps({**item, "geom": simplified}).encode()
            max_retries = 300
            for attempt in range(max_retries):
                try:
                    ack = await js.publish(NATS_SUBJECT, msg, timeout=120)
                    if ack:
                        published += 1
                        consecutive_failures = 0  # Reset on success
                        if publish_progress is not None:
                            publish_progress.update(1)
                        break  # Success, move to next item
                    else:
                        consecutive_failures += 1
                        logger.info(
                            "[watcher] No ack received during flush (attempt %s/%s)",
                            attempt + 1,
                            max_retries,
                        )
                        if consecutive_failures >= max_consecutive_failures:
                            logger.error(
                                "[watcher] Too many consecutive failures during flush (%s), skipping remaining items",
                                consecutive_failures,
                            )
                            break  # Skip remaining items in flush
                        if attempt < max_retries - 1:
                            await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                        else:
                            logger.error(
                                "[watcher] Failed to publish element during flush after %s attempts",
                                max_retries,
                            )
                except Exception as e:
                    err_str = str(e).lower()
                    # Payload-too-large is permanent — skip immediately.
                    if (
                        "maximum payload" in err_str
                        or "payload exceeded" in err_str
                        or "message size exceeds" in err_str
                        or "to large" in err_str
                        or "too large" in err_str
                        or "10054" in err_str
                        or "10077" in err_str
                    ):
                        logger.warning("[watcher] Flush: payload too large — skipping")
                        break
                    consecutive_failures += 1
                    logger.error(
                        "[watcher] Error publishing element during flush (attempt %s/%s): %s",
                        attempt + 1,
                        max_retries,
                        e,
                    )
                    if consecutive_failures >= max_consecutive_failures:
                        logger.error(
                            "[watcher] Too many consecutive failures during flush (%s), skipping remaining items",
                            consecutive_failures,
                        )
                        break  # Skip remaining items in flush
                    if attempt < max_retries - 1:
                        await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    else:
                        logger.error(
                            "[watcher] Failed to publish element during flush after %s attempts: %s",
                            max_retries,
                            e,
                        )
            # If we had too many consecutive failures, break out of flush loop
            if consecutive_failures >= max_consecutive_failures:
                logger.error(
                    "[watcher] Skipping remaining items in queue due to consecutive failures",
                )
                break
        except queue.Empty:
            break

    thread.join()

    # Close publishing progress tracker
    if publish_progress is not None:
        publish_progress.close()

    skipped_ways = handler.skipped_way_count
    skipped_relations = handler.skipped_relation_count
    logger.info(
        "\n[watcher] %s: parsed %s elements (nodes: %s, ways: %s, relations: %s)",
        os.path.basename(filepath),
        handler.count,
        handler.node_count,
        handler.way_count,
        handler.relation_count,
    )
    if skipped_ways > 0:
        logger.warning(
            "[watcher] %s: skipped %s ways (insufficient coordinates)",
            os.path.basename(filepath),
            skipped_ways,
        )
    if skipped_relations > 0:
        logger.warning(
            "[watcher] %s: skipped %s relations (no geometry)",
            os.path.basename(filepath),
            skipped_relations,
        )
    logger.info("[watcher] %s: published %s elements", os.path.basename(filepath), published)

    # Clear Redis cache after processing to free up space. scan_iter + per-key
    # pipelined deletes work in both standalone and cluster mode (a multi-key
    # DELETE would be a cross-slot error on a cluster).
    try:
        logger.info("[watcher] Clearing Redis cache...")
        deleted = 0
        batch: list = []

        def _flush(keys: list) -> None:
            pipe = redis_client.pipeline(transaction=False)
            for k in keys:
                pipe.delete(k)
            pipe.execute()

        for key in redis_client.scan_iter(match=f"{handler.redis_prefix}*", count=1000):
            batch.append(key)
            if len(batch) >= 500:
                _flush(batch)
                deleted += len(batch)
                batch = []
        if batch:
            _flush(batch)
            deleted += len(batch)
        if deleted > 0:
            logger.info("[watcher] Redis cache cleared (%s keys)", deleted)
        else:
            logger.info("[watcher] Redis cache was empty")
    except Exception as e:
        logger.error("[watcher] Error clearing Redis cache: %s", e)

    await nc.close()

    # Propagate parsing errors so the caller knows the file was NOT fully processed
    if parse_error:
        raise parse_error[0]


# ---------------------------------------------------------------------------
# Directory scan (picks up new PBF files dropped into data/)
# ---------------------------------------------------------------------------
async def _scan_and_process() -> int:
    """Process any not-yet-processed *.osm.pbf files in DATA_DIR once.

    Returns the number of files successfully processed this pass.
    """
    done = load_processed(DATA_DIR)
    existing_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.osm.pbf")))

    # Keep data/valhalla/*.pbf symlinks in sync so the routing engine sees
    # every extract, including ones copied into data/ by hand (idempotent).
    for f in existing_files:
        try:
            link_pbf_for_valhalla(f, label="watcher")
        except OSError as e:
            logger.warning("[watcher] Could not link %s for Valhalla: %s", os.path.basename(f), e)

    pending = [f for f in existing_files if not is_processed(DATA_DIR, f, done)]
    if not pending:
        return 0

    logger.info("[watcher] Found %s new PBF file(s) to process", len(pending))
    processed = 0
    for f in pending:
        try:
            await publish_file(f)
            record_processed(DATA_DIR, f, done)
            logger.info("[watcher] Completed %s", os.path.basename(f))
            processed += 1
        except Exception as e:
            logger.error("[watcher] Error processing %s: %s", os.path.basename(f), e)
            continue
    return processed


async def run():
    """Continuously watch DATA_DIR, importing new *.osm.pbf files as they appear.

    Polls the directory rather than using inotify/watchdog: DATA_DIR is a Docker
    bind mount and host-side file drops don't reliably emit inotify events into
    the container.
    """
    logger.info("[watcher] Starting watcher service...")
    os.makedirs(DATA_DIR, exist_ok=True)
    logger.info(
        "[watcher] Watching %s for *.osm.pbf (re-scan every %ss)", DATA_DIR, WATCH_POLL_INTERVAL
    )

    first = True
    while True:
        n = await _scan_and_process()
        if first and n == 0:
            logger.info("[watcher] No new PBF files in %s yet; will keep watching.", DATA_DIR)
        first = False
        await asyncio.sleep(WATCH_POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(run())
