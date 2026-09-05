"""traffic-writer — push aggregated live speeds into Valhalla's traffic.tar.

Valhalla memory-maps ``traffic.tar`` and reads a live speed per directed edge
from it. This service is the thing that *writes* those speeds: every
``TRAFFIC_WRITE_INTERVAL`` seconds it reads the per-edge speed aggregate that
``traffic_aggregator`` maintains in Redis and overwrites the matching 8-byte
``TrafficSpeed`` records in place. Because the tar lives on the shared
``valhalla-tiles`` volume and both containers mmap it, the running router sees
the new speeds with no restart.

Horizontal scaling (active-active sharding)
-------------------------------------------
The writer's per-cycle work grows with the number of *distinct live edges*
(≈100k at 1000 probes/s city-wide), not with request rate. Two mechanisms keep
the cycle fast and allow N concurrent replicas:

* **Tile sharding** — a replica only writes edges whose
  ``tile_base_id(graphid) % TRAFFIC_WRITER_SHARDS == shard_index``. Ownership is
  per *tile*, so a tile's speed records AND its header ``last_update`` bytes are
  written by exactly one replica: concurrent writers touch disjoint mmap byte
  ranges and need no locking. The shard index defaults to the pod's StatefulSet
  ordinal (trailing ``-<n>`` of the hostname). 1 shard == the original
  single-writer behavior.
* **Pipelined reads** — per-edge HGETALLs are batched through a Redis pipeline
  (chunks of 500), turning ~100k sequential round-trips into ~200.

Each replica reads the full (cheap) ``tf:idx`` zset and filters client-side, so
the aggregator is unaware of sharding and resharding is just a replica-count
change. If a shard dies, its tiles stop refreshing until the pod restarts;
``TRAFFIC_EDGE_TTL`` bounds the staleness (edges revert to baseline speeds).

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
import socket
import time

import redis

from shared import traffic_tile as tt
from shared.config import (
    REDIS_HOST,
    REDIS_PORT,
    TRAFFIC_EDGE_TTL,
    TRAFFIC_EXTRACT_PATH,
    TRAFFIC_WRITE_INTERVAL,
    TRAFFIC_WRITER_SHARD_INDEX,
    TRAFFIC_WRITER_SHARDS,
)
from shared.logging import get_logger
from shared.redis_client import make_redis

logger = get_logger("traffic-writer")

_EDGE_KEY_PREFIX = "tf:e:"
_INDEX_KEY = "tf:idx"
_PIPELINE_BATCH = 500  # HGETALLs per pipeline round-trip


def _log(msg: str) -> None:
    logger.info("[traffic-writer] %s", msg)


def _resolve_shard() -> tuple[int, int]:
    """Return (shards, shard_index) for this replica.

    The index comes from TRAFFIC_WRITER_SHARD_INDEX when set, otherwise from the
    hostname's trailing ordinal (StatefulSet pods are named ``<name>-<n>``),
    otherwise 0. An out-of-range index is a deploy error worth failing loudly on.
    """
    shards = max(1, TRAFFIC_WRITER_SHARDS)
    if TRAFFIC_WRITER_SHARD_INDEX != "":
        index = int(TRAFFIC_WRITER_SHARD_INDEX)
    else:
        tail = socket.gethostname().rsplit("-", 1)[-1]
        index = int(tail) if tail.isdigit() else 0
    if not 0 <= index < shards:
        raise SystemExit(f"[traffic-writer] shard index {index} out of range for {shards} shards")
    return shards, index


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


def _chunks(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def run():
    shards, shard_index = _resolve_shard()
    _log(
        f"Starting. extract={TRAFFIC_EXTRACT_PATH} interval={TRAFFIC_WRITE_INTERVAL}s "
        f"ttl={TRAFFIC_EDGE_TTL}s shard={shard_index}/{shards}"
    )
    _wait_for_extract(TRAFFIC_EXTRACT_PATH)

    index = tt.build_tar_index(TRAFFIC_EXTRACT_PATH)
    _log(f"Indexed {len(index)} traffic tiles from {TRAFFIC_EXTRACT_PATH}")

    mm = _open_mmap(TRAFFIC_EXTRACT_PATH)
    r = make_redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    def owns(gid: int) -> bool:
        return tt.tile_base_id(gid) % shards == shard_index

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
        mm[off : off + tt.SPEED_RECORD_SIZE] = tt.speed_to_bytes(kph, congestion)
        # Bump the tile header's last_update so consumers see the refresh.
        mm[data_off + 8 : data_off + 16] = tt.pack_header_last_update()
        return True

    while True:
        cycle_start = time.monotonic()
        now = time.time()
        cutoff = now - TRAFFIC_EDGE_TTL

        try:
            # Expired edges first: revert this shard's to UNKNOWN, then drop them
            # from the index. Other shards clean up their own.
            expired = [
                gid_s
                for gid_s in r.zrangebyscore(_INDEX_KEY, "-inf", f"({cutoff}")
                if owns(int(gid_s))
            ]
            reverted = 0
            for batch in _chunks(expired, _PIPELINE_BATCH):
                for gid_s in batch:
                    if write_edge(int(gid_s), None):
                        reverted += 1
                pipe = r.pipeline(transaction=False)
                for gid_s in batch:
                    pipe.delete(f"{_EDGE_KEY_PREFIX}{gid_s}")
                pipe.execute()
                r.zrem(_INDEX_KEY, *batch)

            # Fresh edges owned by this shard: write their current smoothed speed.
            # HGETALLs are pipelined — one round-trip per _PIPELINE_BATCH edges.
            fresh = [
                gid_s for gid_s in r.zrangebyscore(_INDEX_KEY, cutoff, "+inf") if owns(int(gid_s))
            ]
            written = 0
            skipped = 0
            for batch in _chunks(fresh, _PIPELINE_BATCH):
                pipe = r.pipeline(transaction=False)
                for gid_s in batch:
                    pipe.hgetall(f"{_EDGE_KEY_PREFIX}{gid_s}")
                hashes = pipe.execute()
                for gid_s, h in zip(batch, hashes, strict=True):
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
                cycle_s = time.monotonic() - cycle_start
                _log(
                    f"shard {shard_index}/{shards} flushed: {written} edges updated, "
                    f"{reverted} reverted, {skipped} skipped (not in extract) "
                    f"in {cycle_s:.2f}s"
                )
        except redis.RedisError as e:
            _log(f"Redis error this cycle (will retry): {e}")
        except Exception as e:  # never let one bad cycle kill the writer
            _log(f"Unexpected error this cycle (will retry): {e}")

        elapsed = time.monotonic() - cycle_start
        time.sleep(max(1.0, TRAFFIC_WRITE_INTERVAL - elapsed))


if __name__ == "__main__":
    run()
