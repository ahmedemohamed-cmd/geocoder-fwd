"""traffic-writer — push aggregated live speeds into Valhalla's traffic.tar.

Valhalla memory-maps ``traffic.tar`` and reads a live speed per directed edge
from it. This service is the thing that *writes* those speeds: every
``TRAFFIC_WRITE_INTERVAL`` seconds it reads the per-edge speed aggregate that
``traffic_aggregator`` maintains in Redis and overwrites the matching 8-byte
``TrafficSpeed`` records in place. Because the tar lives on the shared
``valhalla-tiles`` volume and both containers mmap it, the running router sees
the new speeds with no restart.

Redis schema (written by traffic_aggregator, read here)
-------------------------------------------------------
    tf:e:{graphid}   hash  {kph, n, ts}     – current smoothed speed for an edge
    tf:idx           zset  member=graphid, score=last_update_epoch

Freshness is driven entirely by the zset score (no per-key TTL): edges whose
score is older than ``TRAFFIC_EDGE_TTL`` are reverted to UNKNOWN so Valhalla
falls back to predicted/base speeds, then dropped from the index.
"""

import mmap
import os
import struct
import time

import redis

from shared.config import (
    REDIS_HOST,
    REDIS_PORT,
    TRAFFIC_EXTRACT_PATH,
    TRAFFIC_WRITE_INTERVAL,
    TRAFFIC_EDGE_TTL,
)
from shared import traffic_tile as tt

_EDGE_KEY_PREFIX = "tf:e:"
_INDEX_KEY = "tf:idx"


def _log(msg: str) -> None:
    print(f"[traffic-writer] {msg}", flush=True)


def _wait_for_extract(path: str) -> None:
    """Block until traffic.tar exists and looks non-trivial.

    On first boot Valhalla spends minutes building the graph + extract; there is
    nothing to write until it lands, so we poll rather than crash-loop.
    """
    announced = False
    while True:
        try:
            if os.path.getsize(path) > tt.HEADER_SIZE:
                return
        except OSError:
            pass
        if not announced:
            _log(f"Waiting for Valhalla traffic extract at {path} ...")
            announced = True
        time.sleep(5)


def _open_mmap(path: str) -> mmap.mmap:
    fd = os.open(path, os.O_RDWR)
    try:
        return mmap.mmap(fd, 0)  # length 0 -> whole file, MAP_SHARED by default
    finally:
        os.close(fd)  # mmap keeps its own reference to the mapping


def run():
    _log(
        f"Starting. extract={TRAFFIC_EXTRACT_PATH} interval={TRAFFIC_WRITE_INTERVAL}s "
        f"ttl={TRAFFIC_EDGE_TTL}s"
    )
    _wait_for_extract(TRAFFIC_EXTRACT_PATH)

    index = tt.build_tar_index(TRAFFIC_EXTRACT_PATH)
    _log(f"Indexed {len(index)} traffic tiles from {TRAFFIC_EXTRACT_PATH}")

    mm = _open_mmap(TRAFFIC_EXTRACT_PATH)
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    def write_edge(graphid: int, kph: float | None, congestion: int = 0) -> bool:
        """Overwrite one edge's TrafficSpeed record. Returns True if written."""
        base = tt.tile_base_id(graphid)
        tile = index.get(base)
        if tile is None:
            return False  # edge in a level/tile not present in the extract
        data_off, edge_count = tile
        _, _, edge_index = tt.decode_graphid(graphid)
        if edge_index >= edge_count:
            return False  # stale graphid (tiles rebuilt?) — skip rather than corrupt
        off = tt.edge_record_offset(data_off, edge_index)
        mm[off:off + tt.SPEED_RECORD_SIZE] = tt.speed_to_bytes(kph, congestion)
        # Bump the tile header's last_update so consumers see the refresh.
        mm[data_off + 8:data_off + 16] = tt.pack_header_last_update()
        return True

    while True:
        cycle_start = time.monotonic()
        now = time.time()
        cutoff = now - TRAFFIC_EDGE_TTL

        try:
            # Expired edges first: revert to UNKNOWN, then drop from the index.
            expired = r.zrangebyscore(_INDEX_KEY, "-inf", f"({cutoff}")
            reverted = 0
            for gid_s in expired:
                gid = int(gid_s)
                if write_edge(gid, None):
                    reverted += 1
                r.zrem(_INDEX_KEY, gid_s)
                r.delete(f"{_EDGE_KEY_PREFIX}{gid_s}")

            # Fresh edges: write their current smoothed speed.
            fresh = r.zrangebyscore(_INDEX_KEY, cutoff, "+inf")
            written = 0
            skipped = 0
            for gid_s in fresh:
                h = r.hgetall(f"{_EDGE_KEY_PREFIX}{gid_s}")
                if not h or "kph" not in h:
                    continue
                kph = float(h["kph"])
                cong = int(float(h.get("congestion", 0)))
                if write_edge(int(gid_s), kph, cong):
                    written += 1
                else:
                    skipped += 1

            if written or reverted or skipped:
                mm.flush()
                _log(
                    f"flushed: {written} edges updated, {reverted} reverted, "
                    f"{skipped} skipped (not in extract)"
                )
        except redis.RedisError as e:
            _log(f"Redis error this cycle (will retry): {e}")
        except Exception as e:  # never let one bad cycle kill the writer
            _log(f"Unexpected error this cycle (will retry): {e}")

        elapsed = time.monotonic() - cycle_start
        time.sleep(max(1.0, TRAFFIC_WRITE_INTERVAL - elapsed))


if __name__ == "__main__":
    run()
