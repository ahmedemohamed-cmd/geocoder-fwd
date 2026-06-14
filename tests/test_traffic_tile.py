#!/usr/bin/env python3
"""Unit tests for shared/traffic_tile.py — the Valhalla traffic binary core.

Plain-asserts style (the repo has no pytest). Run directly:

    python3 tests/test_traffic_tile.py

These pin the GraphId bit layout and TrafficSpeed/Header encodings so a Valhalla
version bump that changed the binary format fails loudly here instead of silently
corrupting traffic.tar.
"""

import io
import os
import struct
import sys
import tarfile
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared import traffic_tile as tt


def test_graphid_decode_matches_locate_sample():
    # From a real Valhalla /locate verbose response:
    #   "edge_id": {"value": 257032954354, "id": 7660, "tile_id": 750654, "level": 2}
    value = 257032954354
    level, tileid, index = tt.decode_graphid(value)
    assert (level, tileid, index) == (2, 750654, 7660), (level, tileid, index)


def test_graphid_roundtrip():
    for level, tileid, index in [(0, 0, 0), (2, 750654, 7660), (7, 0x3FFFFF, 0x1FFFFF)]:
        v = tt.encode_graphid(level, tileid, index)
        assert tt.decode_graphid(v) == (level, tileid, index)


def test_tile_base_id_zeroes_edge_index():
    value = 257032954354  # level 2, tile 750654, edge 7660
    base = tt.tile_base_id(value)
    assert tt.decode_graphid(base) == (2, 750654, 0)
    # Two edges in the same tile share a base id.
    other = tt.encode_graphid(2, 750654, 999)
    assert tt.tile_base_id(other) == base


def test_speed_pack_unpack_roundtrip():
    for kph in [0, 2, 10, 30, 50, 100, 252, 260, 999]:
        packed = tt.pack_speed(kph)
        fields = tt.unpack_speed(packed)
        expected_raw = tt.encode_speed_raw(kph)
        assert fields["overall_encoded_speed"] == expected_raw
        assert fields["encoded_speed1"] == expected_raw
        assert fields["breakpoint1"] == 255
        # overall_speed_kph reverses the encoding (clamped to 252 max).
        assert tt.overall_speed_kph(packed) == min(kph - (kph % 2), tt.MAX_SPEED_KPH)


def test_speed_unknown():
    packed = tt.pack_speed(None)
    fields = tt.unpack_speed(packed)
    assert fields["overall_encoded_speed"] == tt.UNKNOWN_SPEED_RAW
    assert tt.overall_speed_kph(packed) is None


def test_speed_congestion_clamped():
    fields = tt.unpack_speed(tt.pack_speed(40, congestion=200))
    assert fields["congestion1"] == 63
    fields = tt.unpack_speed(tt.pack_speed(40, congestion=10))
    assert fields["congestion1"] == 10


def test_speed_bytes_are_8_le():
    b = tt.speed_to_bytes(50)
    assert len(b) == tt.SPEED_RECORD_SIZE == 8
    assert struct.unpack("<Q", b)[0] == tt.pack_speed(50)


def test_header_size_constants():
    assert tt.HEADER_SIZE == 32
    assert tt.SPEED_RECORD_SIZE == 8


def _make_tile_blob(tile_id: int, edge_count: int) -> bytes:
    """A header + edge_count UNKNOWN speed records (mimics a built traffic tile)."""
    hdr = struct.pack("<QQIIII", tile_id, 0, edge_count, 1, 0, 0)
    body = tt.speed_to_bytes(None) * edge_count
    return hdr + body


def test_build_tar_index_and_offsets():
    # Build a synthetic traffic.tar with two tiles and verify we can locate an
    # edge's record by GraphId and read back a speed we plant there.
    tile_a = tt.encode_graphid(2, 750654, 0)
    tile_b = tt.encode_graphid(2, 100, 0)
    blob_a = _make_tile_blob(tile_a, edge_count=20)
    blob_b = _make_tile_blob(tile_b, edge_count=5)

    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "traffic.tar")
        with tarfile.open(path, "w") as tf:
            for name, blob in [("2/000/750/654.gph", blob_a), ("2/000/000/100.gph", blob_b)]:
                ti = tarfile.TarInfo(name)
                ti.size = len(blob)
                tf.addfile(ti, io.BytesIO(blob))

        index = tt.build_tar_index(path)
        assert set(index.keys()) == {tile_a, tile_b}
        assert index[tile_a][1] == 20
        assert index[tile_b][1] == 5

        # Plant 60 kph on edge 7 of tile A, then read it straight back from the file.
        edge_value = tt.encode_graphid(2, 750654, 7)
        data_off, edge_count = index[tt.tile_base_id(edge_value)]
        assert 7 < edge_count
        off = tt.edge_record_offset(data_off, 7)
        with open(path, "r+b") as f:
            f.seek(off)
            f.write(tt.speed_to_bytes(60))
            f.seek(off)
            raw = struct.unpack("<Q", f.read(8))[0]
        assert tt.overall_speed_kph(raw) == 60


def _run():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  ok  {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL  {t.__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_run())
