"""Unit tests for the pure helper functions behind the geocoder endpoints.

No mocks or I/O — just deterministic logic (confidence scoring, distance,
housenumber parsing, address assembly/parsing).
"""

from services.geocoder_helpers import (
    _distance_confidence,
    _haversine_m,
    _normalize_confidence,
)
from shared.address import build_full_address, has_address, is_address_query
from shared.interpolation import _norm_street, _parity, _parse_housenumber


def test_normalize_confidence():
    assert _normalize_confidence(5.0, 10.0) == 0.5
    assert _normalize_confidence(20.0, 10.0) == 1.0  # clamped
    assert _normalize_confidence(5.0, 0.0) == 0.0  # guard against div-by-zero


def test_distance_confidence_buckets():
    assert _distance_confidence(0.5) == 1.0
    assert _distance_confidence(5) == 0.9
    assert _distance_confidence(50) == 0.8
    assert _distance_confidence(200) == 0.7
    assert _distance_confidence(500) == 0.6
    assert _distance_confidence(3000) == 0.4
    assert _distance_confidence(10000) == 0.2


def test_haversine_zero_and_known_distance():
    assert _haversine_m(30.0, 31.0, 30.0, 31.0) == 0.0
    # ~1 degree of latitude ≈ 111 km
    d = _haversine_m(30.0, 31.0, 31.0, 31.0)
    assert 110_000 < d < 112_000


def test_parse_housenumber():
    assert _parse_housenumber("15") == 15
    assert _parse_housenumber("12A") == 12
    assert _parse_housenumber("14bis") == 14
    assert _parse_housenumber("") is None
    assert _parse_housenumber("abc") is None


def test_parity():
    assert _parity(3) == "odd"
    assert _parity(4) == "even"


def test_norm_street():
    # leading generic word stripped (Arabic + English), trailing kept
    assert _norm_street("شارع التحرير") == "التحرير"
    assert _norm_street("ش الهرم") == "الهرم"
    assert _norm_street("طريق النصر") == "النصر"
    assert _norm_street("Tahrir Street") == "tahrir street"
    # diacritics dropped, alef/ya/ta-marbuta variants unified, whitespace collapsed
    assert _norm_street("الجيزة") == "الجيزه"
    assert _norm_street("شارع  مصطفى  النحاس") == "مصطفي النحاس"
    assert _norm_street("شَارِع التَحرير") == "التحرير"
    # a prefix word that stands alone is not stripped
    assert _norm_street("شارع") == "شارع"
    assert _norm_street("") == ""
    assert _norm_street(None) == ""


def test_has_address():
    assert has_address({"addr:street": "Tahrir"}) is True
    assert has_address({"name": "Cairo Tower"}) is False
    assert has_address({}) is False


def test_build_full_address():
    tags = {
        "addr:housenumber": "15",
        "addr:street": "Tahrir Street",
        "addr:city": "Cairo",
    }
    assert build_full_address(tags) == "15 Tahrir Street, Cairo"
    assert build_full_address({}) == ""


def test_is_address_query():
    # A query carrying a housenumber should be recognised as an address query.
    assert is_address_query("15 Tahrir Street") is True
    # Structured (comma-separated) input is treated as an address.
    assert is_address_query("Main St, Cairo") is True
    # A bare place name without address structure is not.
    assert is_address_query("Pyramids") is False
    assert is_address_query("hello world") is False
