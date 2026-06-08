"""Valhalla live-traffic tile binary helpers.

Valhalla serves live traffic from a ``traffic.tar`` extract that it memory-maps.
Each member of the tar is a *traffic tile* that mirrors a routing graph tile: a
32-byte ``TrafficTileHeader`` followed by one 8-byte ``TrafficSpeed`` record per
directed edge, in edge-index order. Writing a live speed therefore means
overwriting the right 8 bytes in the right tile, in place — the running router
sees the change through the shared mmap with no restart.

This module is the delicate binary core. It is pure (no I/O beyond reading the
tar index) so it can be unit-tested in isolation: see tests/test_traffic_tile.py.

Layouts are taken from valhalla/baldr/graphid.h and valhalla/baldr/traffictile.h
and pinned by the unit tests against a real /locate response.

GraphId (64-bit)
----------------
    level   : 3 bits   (bits 0-2)
    tileid  : 22 bits  (bits 3-24)
    id      : 25 bits  (bits 25-49)   -> the directed-edge index within the tile

TrafficSpeed (64-bit, little-endian, fields from the LSB up)
------------------------------------------------------------
    overall_encoded_speed : 7   (kph / 2; 127 == UNKNOWN)
    encoded_speed1        : 7   (subsegment 0)
    encoded_speed2        : 7   (subsegment 1)
    encoded_speed3        : 7   (subsegment 2)
    breakpoint1           : 8   (subsegment 0->1 boundary, position = len*bp/255)
    breakpoint2           : 8   (subsegment 1->2 boundary)
    congestion1           : 6   (0 == unknown, 1..63 == none..max)
    congestion2           : 6
    congestion3           : 6
    has_incidents         : 1
    spare                 : 1

TrafficTileHeader (32 bytes)
----------------------------
    tile_id               : uint64   (GraphId of the tile, id == 0)
    last_update           : uint64   (unix epoch seconds)
    directed_edge_count   : uint32   (number of TrafficSpeed records that follow)
    traffic_tile_version  : uint32
    spare2                : uint32
    spare3                : uint32
"""

import struct
import tarfile
import time

# ── GraphId ────────────────────────────────────────────────────────────────
_LEVEL_BITS = 3
_TILEID_BITS = 22
_INDEX_BITS = 25

_LEVEL_MASK = (1 << _LEVEL_BITS) - 1            # 0x7
_TILEID_MASK = (1 << _TILEID_BITS) - 1          # 0x3FFFFF
_INDEX_MASK = (1 << _INDEX_BITS) - 1            # 0x1FFFFF
# Mask that keeps level+tileid and zeroes the edge index -> the tile's base id.
_TILE_BASE_MASK = (1 << (_LEVEL_BITS + _TILEID_BITS)) - 1   # 0x1FFFFFF


def decode_graphid(value: int) -> tuple[int, int, int]:
    """Split a Valhalla GraphId value into (level, tileid, edge_index)."""
    level = value & _LEVEL_MASK
    tileid = (value >> _LEVEL_BITS) & _TILEID_MASK
    index = (value >> (_LEVEL_BITS + _TILEID_BITS)) & _INDEX_MASK
    return level, tileid, index


def encode_graphid(level: int, tileid: int, index: int) -> int:
    """Pack (level, tileid, edge_index) back into a GraphId value."""
    return (
        (level & _LEVEL_MASK)
        | ((tileid & _TILEID_MASK) << _LEVEL_BITS)
        | ((index & _INDEX_MASK) << (_LEVEL_BITS + _TILEID_BITS))
    )


def tile_base_id(value: int) -> int:
    """Return the GraphId of the tile that owns this edge (edge index zeroed).

    This matches the ``tile_id`` stored in each ``TrafficTileHeader``, so it is
    the key used to find an edge's traffic tile in the tar index.
    """
    return value & _TILE_BASE_MASK


# ── TrafficSpeed ─────────────────────────────────────────────────────────────
# (field, bit-width) in LSB-first order; widths sum to 64.
_SPEED_FIELDS = (
    ("overall_encoded_speed", 7),
    ("encoded_speed1", 7),
    ("encoded_speed2", 7),
    ("encoded_speed3", 7),
    ("breakpoint1", 8),
    ("breakpoint2", 8),
    ("congestion1", 6),
    ("congestion2", 6),
    ("congestion3", 6),
    ("has_incidents", 1),
    ("spare", 1),
)

UNKNOWN_SPEED_RAW = 127           # 7-bit sentinel: "no live speed for this edge"
MAX_SPEED_RAW = 126               # 126 * 2 = 252 kph, the max representable speed
MAX_SPEED_KPH = MAX_SPEED_RAW * 2
SPEED_RECORD_SIZE = 8             # bytes per TrafficSpeed
HEADER_SIZE = 32                  # bytes per TrafficTileHeader
_HEADER_FMT = "<QQIIII"           # tile_id, last_update, edge_count, version, spare2, spare3

assert sum(b for _, b in _SPEED_FIELDS) == 64
assert struct.calcsize(_HEADER_FMT) == HEADER_SIZE


def _assemble_speed(**fields: int) -> int:
    """Pack named TrafficSpeed fields into a uint64 (missing fields default 0)."""
    value = 0
    shift = 0
    for name, bits in _SPEED_FIELDS:
        part = fields.get(name, 0) & ((1 << bits) - 1)
        value |= part << shift
        shift += bits
    return value


def unpack_speed(value: int) -> dict[str, int]:
    """Split a TrafficSpeed uint64 into its named fields."""
    out: dict[str, int] = {}
    shift = 0
    for name, bits in _SPEED_FIELDS:
        out[name] = (value >> shift) & ((1 << bits) - 1)
        shift += bits
    return out


def encode_speed_raw(kph: float) -> int:
    """Encode a speed in kph to Valhalla's 7-bit (kph/2) units, clamped to valid."""
    raw = int(round(kph / 2.0))
    if raw < 0:
        raw = 0
    if raw > MAX_SPEED_RAW:
        raw = MAX_SPEED_RAW
    return raw


def pack_speed(kph: float | None, congestion: int = 0) -> int:
    """Build a TrafficSpeed uint64 for a single uniform speed across the edge.

    ``kph=None`` produces the UNKNOWN record, which makes Valhalla fall back to
    predicted/constrained/base speeds for that edge (i.e. clears live traffic).
    ``congestion`` is optional 0..63 (0 = unknown); it does not affect routing
    speed but is surfaced in the API and useful for visualisation.
    """
    if kph is None:
        return _assemble_speed(
            overall_encoded_speed=UNKNOWN_SPEED_RAW,
            encoded_speed1=UNKNOWN_SPEED_RAW,
        )
    raw = encode_speed_raw(kph)
    cong = max(0, min(63, int(congestion)))
    # One subsegment covering the whole edge: breakpoint1 = 255 (== full length).
    return _assemble_speed(
        overall_encoded_speed=raw,
        encoded_speed1=raw,
        breakpoint1=255,
        congestion1=cong,
    )


def speed_to_bytes(kph: float | None, congestion: int = 0) -> bytes:
    """pack_speed() serialised to the 8 little-endian bytes stored in the tile."""
    return struct.pack("<Q", pack_speed(kph, congestion))


def overall_speed_kph(value: int) -> float | None:
    """Read the overall edge speed (kph) from a TrafficSpeed uint64.

    Returns None when the record is UNKNOWN (no live speed set).
    """
    raw = value & ((1 << 7) - 1)
    if raw == UNKNOWN_SPEED_RAW:
        return None
    return raw * 2.0


# ── traffic.tar index ─────────────────────────────────────────────────────────
def edge_record_offset(tile_data_offset: int, edge_index: int) -> int:
    """Absolute byte offset of an edge's TrafficSpeed record within traffic.tar."""
    return tile_data_offset + HEADER_SIZE + edge_index * SPEED_RECORD_SIZE


def build_tar_index(path: str) -> dict[int, tuple[int, int]]:
    """Index a traffic.tar so edges can be located by GraphId.

    Returns ``{tile_base_id: (data_offset, directed_edge_count)}`` where
    ``data_offset`` is the absolute byte offset (inside the tar) at which the
    tile's TrafficTileHeader begins. Tar member data starts on a 512-byte
    boundary, so each tile — and therefore every 8-byte speed record — is
    naturally aligned for in-place mmap writes.

    Reads each member's header to key by the real ``tile_id`` rather than trust
    the member filename, so it is robust to the tar's path layout.
    """
    index: dict[int, tuple[int, int]] = {}
    with tarfile.open(path, "r") as tf:
        fileobj = tf.fileobj
        for member in tf.getmembers():
            if not member.isfile() or member.size < HEADER_SIZE:
                continue
            data_off = member.offset_data
            fileobj.seek(data_off)
            hdr = fileobj.read(HEADER_SIZE)
            tile_id, _last_update, edge_count, *_ = struct.unpack(_HEADER_FMT, hdr)
            index[tile_id] = (data_off, edge_count)
    return index


def pack_header_last_update(epoch_seconds: int | None = None) -> bytes:
    """The 8 little-endian bytes for the header's ``last_update`` field.

    Write these at ``tile_data_offset + 8`` after touching a tile so consumers
    can see the tile was refreshed.
    """
    if epoch_seconds is None:
        epoch_seconds = int(time.time())
    return struct.pack("<Q", epoch_seconds)
