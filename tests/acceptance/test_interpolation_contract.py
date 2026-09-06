"""Address interpolation contract.

Interpolation is the headline capability: when a house number has no exact
record, its position is estimated between known addresses on the same street,
odd/even side aware — and the street is resolved cross-lingually first, so an
English query reaches Arabic-tagged data.

`interpolate_address` is driven through its public entry point with a fake
connection pool standing in for PostGIS. The geometry under test is real; only
the data source is faked, the same way FakeES and FakeJetStream are used
elsewhere in this suite.
"""

import json
import pathlib

import pytest

from shared.interpolation import interpolate_address

GOLDEN = pathlib.Path(__file__).parent / "interpolation_golden.json"

# A straight stretch of street: odds ascending on one side, evens on the other.
STREET = "Tahrir Street"
_ROWS = [
    {
        "osm_id": f"n{i}",
        "housenumber": str(n),
        "street": STREET,
        "city": "Cairo",
        "postcode": None,
        "country": "EG",
        "lat": 30.0400 + 0.0002 * i,
        "lon": 31.2300 + 0.0002 * i,
    }
    for i, n in enumerate([1, 3, 5, 11, 15, 21])
] + [
    {
        "osm_id": f"n1{i}",
        "housenumber": str(n),
        "street": STREET,
        "city": "Cairo",
        "postcode": None,
        "country": "EG",
        "lat": 30.0401 + 0.0002 * i,
        "lon": 31.2301 + 0.0002 * i,
    }
    for i, n in enumerate([2, 4, 6, 12, 16, 22])
]


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    async def execute(self, *args, **kwargs):
        # The gather sets a statement timeout before querying. Without this the
        # module's fail-open `except` swallows the AttributeError and returns
        # no addresses — which looked exactly like "interpolation impossible".
        return "SET"

    async def fetch(self, query, *params):
        # Street geometry lookups select from a different table; interpolation
        # falls back to the address points when no line geometry is found.
        if "osm_addresses" not in query:
            return []
        return list(self._rows)

    async def fetchrow(self, query, *params):
        return None


class _FakePool:
    def __init__(self, rows=_ROWS):
        self._rows = rows

    def acquire(self):
        rows = self._rows

        class _Ctx:
            async def __aenter__(self):
                return _FakeConn(rows)

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


CASES = [
    {"label": "odd-between-known", "hn": 7},
    {"label": "odd-wide-gap", "hn": 13},
    {"label": "even-between-known", "hn": 8},
    {"label": "even-wide-gap", "hn": 14},
    {"label": "below-range", "hn": 0},
    {"label": "above-range", "hn": 99},
    {"label": "exact-existing-odd", "hn": 5},
    {"label": "exact-existing-even", "hn": 6},
]


async def _run():
    out = []
    for case in CASES:
        got = await interpolate_address(
            _FakePool(),
            case["hn"],
            STREET,
            street_names=[STREET],
            near=(30.0405, 31.2305),
        )
        out.append(
            {
                "case": case["label"],
                "hn": case["hn"],
                "result": None
                if got is None
                else {
                    "lat": round(got.lat, 7),
                    "lon": round(got.lon, 7),
                    "side": got.side,
                    "bracket_low": got.bracket_low,
                    "bracket_high": got.bracket_high,
                    "confidence": got.confidence,
                },
            }
        )
    return json.loads(json.dumps(out, sort_keys=True))


@pytest.mark.asyncio
async def test_interpolation_matches_golden():
    expected = json.loads(GOLDEN.read_text(encoding="utf-8"))
    actual = await _run()
    drift = [e["case"] for e, a in zip(expected, actual, strict=True) if e != a]
    assert not drift, f"interpolated positions changed for: {drift}. If deliberate, --update."


# ── intent, asserted independently of the golden ──────────────────────────


@pytest.mark.asyncio
async def test_interpolated_point_lies_between_its_brackets():
    """The estimate must sit inside the span it claims to interpolate."""
    got = await interpolate_address(_FakePool(), 7, STREET, street_names=[STREET])
    assert got is not None
    assert got.bracket_low is not None and got.bracket_high is not None
    # Brackets carry the RAW housenumber text, not integers, because real
    # housenumbers include forms like "12A".
    low, high = int(got.bracket_low), int(got.bracket_high)
    assert low < 7 < high, f"7 is not inside its own bracket {low}..{high}"


@pytest.mark.asyncio
async def test_parity_is_respected():
    """Odd numbers interpolate against odd neighbours, evens against evens —
    the two sides of a street are physically different places."""
    odd = await interpolate_address(_FakePool(), 7, STREET, street_names=[STREET])
    even = await interpolate_address(_FakePool(), 8, STREET, street_names=[STREET])
    assert odd is not None and even is not None
    assert int(odd.bracket_low) % 2 == 1 and int(odd.bracket_high) % 2 == 1
    assert int(even.bracket_low) % 2 == 0 and int(even.bracket_high) % 2 == 0
    assert (odd.lat, odd.lon) != (even.lat, even.lon), "opposite sides must differ"


@pytest.mark.asyncio
async def test_confidence_ranks_certainty_correctly():
    """An exact hit must outrank an interpolation, which must outrank an
    extrapolation past the end of the known range. A caller uses this number to
    decide how much to trust the point."""
    exact = await interpolate_address(_FakePool(), 5, STREET, street_names=[STREET])
    between = await interpolate_address(_FakePool(), 7, STREET, street_names=[STREET])
    beyond = await interpolate_address(_FakePool(), 99, STREET, street_names=[STREET])
    assert exact and between and beyond
    assert exact.confidence > between.confidence > beyond.confidence, (
        f"exact={exact.confidence} between={between.confidence} beyond={beyond.confidence}"
    )
    assert exact.bracket_low is None, "an exact hit is not interpolated between anything"


@pytest.mark.asyncio
async def test_no_data_yields_no_guess():
    """An empty street must return None rather than inventing a position."""
    assert await interpolate_address(_FakePool([]), 7, STREET, street_names=[STREET]) is None
