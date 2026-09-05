"""Tests for oa_watcher and gn_watcher parsing and ID generation."""

import json
import os
import sys
import tempfile

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.gn_watcher import _parse_geonames_row
from services.oa_watcher import (
    _is_ndjson,
    _parse_csv_row,
    _parse_geojson_feature,
    _source_hash,
)

# ── OA ID collision fix ────────────────────────────────────────────────────


def test_oa_csv_id_uses_source_hash_when_no_hash():
    """Without HASH column, IDs include source_hash to prevent cross-file collisions."""
    row = {"LON": "-79.5", "LAT": "43.7", "NUMBER": "10", "STREET": "Main St"}
    msg_a = _parse_csv_row(row, 0, src_hash="aaa111")
    msg_b = _parse_csv_row(row, 0, src_hash="bbb222")
    assert msg_a is not None and msg_b is not None
    assert msg_a["osm_id"] != msg_b["osm_id"], "IDs should differ for different source files"
    assert "aaa111" in msg_a["osm_id"]
    assert "bbb222" in msg_b["osm_id"]


def test_oa_csv_id_uses_hash_when_available():
    """With HASH column, the hash itself is used (no source_hash needed)."""
    row = {"LON": "-79.5", "LAT": "43.7", "NUMBER": "10", "STREET": "Main St", "HASH": "deadbeef"}
    msg = _parse_csv_row(row, 0, src_hash="aaa111")
    assert msg is not None
    assert msg["osm_id"] == "oadeadbeef"


def test_oa_geojson_id_uses_source_hash_when_no_hash():
    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-79.5, 43.7]},
        "properties": {"NUMBER": "10", "STREET": "Main St"},
    }
    msg_a = _parse_geojson_feature(feature, 0, src_hash="aaa111")
    msg_b = _parse_geojson_feature(feature, 0, src_hash="bbb222")
    assert msg_a is not None and msg_b is not None
    assert msg_a["osm_id"] != msg_b["osm_id"]


def test_source_hash_deterministic():
    assert _source_hash("/some/path/file.csv") == _source_hash("/some/path/file.csv")
    assert _source_hash("/a.csv") != _source_hash("/b.csv")


# ── OA parsing edge cases ─────────────────────────────────────────────────


def test_oa_csv_missing_coords():
    row = {"LON": "", "LAT": "43.7", "NUMBER": "10", "STREET": "Main St"}
    assert _parse_csv_row(row, 0) is None


def test_oa_csv_invalid_coords():
    row = {"LON": "999", "LAT": "43.7", "NUMBER": "10", "STREET": "Main St"}
    assert _parse_csv_row(row, 0) is None


def test_oa_csv_no_address():
    row = {"LON": "-79.5", "LAT": "43.7", "NUMBER": "", "STREET": ""}
    assert _parse_csv_row(row, 0) is None


def test_oa_geojson_invalid_coord_type():
    """Coords that aren't numeric should be handled gracefully."""
    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": ["abc", "def"]},
        "properties": {"NUMBER": "10", "STREET": "Main St"},
    }
    assert _parse_geojson_feature(feature, 0) is None


# ── NDJSON detection ───────────────────────────────────────────────────────


def test_ndjson_detection_true():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as f:
        f.write(
            json.dumps(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {},
                }
            )
            + "\n"
        )
        f.flush()
        assert _is_ndjson(f.name) is True
    os.unlink(f.name)


def test_ndjson_detection_false_featurecollection():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as f:
        f.write(json.dumps({"type": "FeatureCollection", "features": []}) + "\n")
        f.flush()
        assert _is_ndjson(f.name) is False
    os.unlink(f.name)


def test_ndjson_detection_false_empty():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as f:
        f.write("")
        f.flush()
        assert _is_ndjson(f.name) is False
    os.unlink(f.name)


# ── GeoNames parsing ──────────────────────────────────────────────────────


def _make_gn_row(**overrides) -> list[str]:
    """Build a 19-column GeoNames TSV row."""
    defaults = [
        "6295630",
        "Earth",
        "Earth",
        "",  # geonameid, name, ascii, altnames
        "0.0",
        "0.0",  # lat, lon
        "L",
        "AREA",  # feature_class, feature_code
        "",
        "",  # country_code, cc2
        "",
        "",
        "",
        "",  # admin1-4
        "7800000000",
        "",
        "",
        "UTC",  # population, elevation, dem, tz
        "2023-01-01",  # modification_date
    ]
    for k, v in overrides.items():
        idx = (
            int(k)
            if k.isdigit()
            else {
                "geonameid": 0,
                "name": 1,
                "asciiname": 2,
                "alternatenames": 3,
                "latitude": 4,
                "longitude": 5,
                "feature_class": 6,
                "feature_code": 7,
                "country_code": 8,
                "population": 14,
            }[k]
        )
        defaults[idx] = v
    return defaults


def test_gn_basic_parsing():
    fields = _make_gn_row(
        geonameid="123",
        name="Ottawa",
        latitude="45.4",
        longitude="-75.7",
        feature_class="P",
        feature_code="PPLC",
        country_code="CA",
        population="1000000",
    )
    msg = _parse_geonames_row(fields)
    assert msg is not None
    assert msg["osm_id"] == "gn123"
    assert msg["tags"]["name"] == "Ottawa"
    assert msg["tags"]["place"] == "city"
    assert msg["tags"]["population"] == "1000000"
    assert msg["tags"]["addr:country"] == "CA"
    assert msg["geom"]["coordinates"] == [-75.7, 45.4]


def test_gn_country_place_tag():
    """Countries should get place=country from the expanded mapping."""
    fields = _make_gn_row(feature_class="A", feature_code="PCLI", country_code="CA")
    msg = _parse_geonames_row(fields)
    assert msg is not None
    assert msg["tags"].get("place") == "country"


def test_gn_admin_level_mapping():
    fields = _make_gn_row(feature_class="A", feature_code="ADM1")
    msg = _parse_geonames_row(fields)
    assert msg is not None
    assert msg["admin_level"] == 4
    assert msg["tags"].get("place") == "state"


def test_gn_altnames_limited():
    """Alternate names should be capped at 10."""
    lots_of_names = ",".join(f"name{i}" for i in range(50))
    fields = _make_gn_row(alternatenames=lots_of_names)
    msg = _parse_geonames_row(fields)
    assert msg is not None
    alt = msg["tags"].get("alt_name", "")
    assert alt.count(",") <= 9, "Should have at most 10 names (9 commas)"


def test_gn_missing_name():
    fields = _make_gn_row(name="")
    assert _parse_geonames_row(fields) is None


def test_gn_invalid_coords():
    fields = _make_gn_row(latitude="abc", longitude="xyz")
    assert _parse_geonames_row(fields) is None


def test_gn_out_of_range_coords():
    fields = _make_gn_row(latitude="200", longitude="0")
    assert _parse_geonames_row(fields) is None


def test_gn_too_few_columns():
    assert _parse_geonames_row(["only", "five", "cols", "here", "!"]) is None


# ── ProgressTracker ────────────────────────────────────────────────────────
