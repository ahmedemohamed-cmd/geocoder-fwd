"""Routing narration contract — driven through HTTP, not internals.

`spec/routing.toml` holds the Arabic narration vocabulary and the traffic
colour bands. What a client actually receives is the translated Valhalla trip
returned by POST /route, so that is what this pins.

Valhalla itself is replaced with a canned response: the contract under test is
the translation the service applies, not the routing engine.
"""

import json
import pathlib

import httpx
import pytest

import services.geocoder as G
import services.routing as R

GOLDEN = pathlib.Path(__file__).parent / "routing_golden.json"


# One leg carrying a broad spread of maneuver types, both unit systems.
def _valhalla_trip(units="kilometers"):
    maneuvers = [
        {
            "type": t,
            "instruction": instr,
            "street_names": ["Tahrir Street"],
            "length": 1.234,
            "time": 60,
        }
        for t, instr in [
            (1, "Drive east on Tahrir Street."),
            (4, "You have arrived at your destination."),
            (10, "Turn right onto Ramses Street."),
            (15, "Turn left onto Corniche El Nil."),
            (16, "Turn sharp left onto Qasr Al Nil."),
            (17, "Bear right onto July 26 Street."),
            (23, "Keep left to take the ramp toward Downtown."),
            (26, "Enter the roundabout and take the 3rd exit."),
            (27, "Exit the roundabout onto Salah Salem."),
            (5, "Make a U-turn."),
        ]
    ]
    return {"trip": {"units": units, "legs": [{"maneuvers": maneuvers}], "language": "en"}}


CASES = [
    {"label": "arabic-km", "units": "kilometers", "language": "ar"},
    {"label": "arabic-miles", "units": "miles", "language": "ar"},
    {"label": "english-untouched", "units": "kilometers", "language": "en"},
    {"label": "no-language", "units": "kilometers", "language": None},
]


class _FakeResponse:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def json(self):
        return self._payload


class _FakeClient:
    """Stands in for the Valhalla HTTP call inside routing.proxy.

    Patched at the httpx seam rather than at `proxy`, because the Arabic
    translation happens INSIDE proxy — patching proxy itself would bypass the
    behaviour under test. (It did, on the first attempt: the golden captured
    untranslated English.)
    """

    def __init__(self, units):
        self._units = units

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None):
        return _FakeResponse(_valhalla_trip(self._units))

    async def get(self, url):
        return _FakeResponse(_valhalla_trip(self._units))


async def _drive():
    results = []
    original = R.httpx
    try:
        for case in CASES:
            units = case["units"]

            class _Shim:
                @staticmethod
                def AsyncClient(*a, **kw):
                    return _FakeClient(units)

            R.httpx = _Shim
            payload = {
                "locations": [{"lat": 30.04, "lon": 31.23}, {"lat": 30.06, "lon": 31.25}],
                "costing": "auto",
            }
            if case["language"]:
                payload["directions_options"] = {"language": case["language"]}
            transport = httpx.ASGITransport(app=G.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
                r = await c.post("/route", json=payload)
            results.append(
                {
                    "case": case["label"],
                    "status": r.status_code,
                    "body": r.json() if r.status_code == 200 else None,
                }
            )
    finally:
        R.httpx = original
    return json.loads(json.dumps(results, sort_keys=True, ensure_ascii=False))


@pytest.mark.asyncio
async def test_route_narration_matches_golden():
    expected = json.loads(GOLDEN.read_text(encoding="utf-8"))
    actual = await _drive()
    drift = [e["case"] for e, a in zip(expected, actual, strict=True) if e != a]
    assert not drift, f"routing narration changed for: {drift}. If deliberate, --update."


@pytest.mark.asyncio
async def test_arabic_requested_yields_arabic_trip():
    """The headline behaviour, asserted independently of the snapshot."""
    results = {r["case"]: r for r in await _drive()}
    ar = results["arabic-km"]["body"]["trip"]
    assert ar["language"] == "ar"
    instructions = [m["instruction"] for m in ar["legs"][0]["maneuvers"]]
    assert any(any("؀" <= ch <= "ۿ" for ch in i) for i in instructions), (
        "no Arabic text in a trip requested with language=ar"
    )
    en = results["english-untouched"]["body"]["trip"]
    assert en["language"] != "ar", "an English request must not be translated"


def test_traffic_bands_cover_the_ratio_range():
    from shared.spec import load

    bands = load("routing.toml")["traffic_bands"]
    assert bands[0][0] < 1.0, "the top band must admit ratios below free-flow"
    assert bands[-1][0] > 0.0, "the last band must leave room for 'red' below it"


if __name__ == "__main__":
    import asyncio
    import sys

    if "--update" in sys.argv:
        GOLDEN.write_text(
            json.dumps(asyncio.run(_drive()), indent=1, sort_keys=True, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"rewrote {GOLDEN}")
