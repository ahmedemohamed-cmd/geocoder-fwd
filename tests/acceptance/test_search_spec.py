"""Search specification contract.

`spec/search.toml` holds the Elasticsearch query tuning — field boosts, phrase
slops, geo-decay parameters, rescore sizing, vector candidate counts. Those
values decide what a user gets back, and none of them are derivable from the
architecture.

The generated ES query body is the observable those values control, and it can
be checked without a cluster: the `es` global is replaced with a fake that
records the query and returns an empty result set. Every request shape below is
pinned, so changing a spec value — or the code that reads it — fails here.

A deliberate tuning change is expected to fail. Regenerate with
`python tests/test_search_spec.py --update`, then run tests/run_recall.py
against docs/quality-baseline.md before committing (AD-9).
"""

import json
import pathlib

import httpx
import pytest

import services.geocoder as G

GOLDEN = pathlib.Path(__file__).parent / "search_golden.json"

CASES = [
    ("/geocode", {"q": "Cairo", "vector": "false"}),
    ("/geocode", {"q": "Cairo", "effort": "high", "vector": "false"}),
    ("/geocode", {"q": "Cairo", "effort": "optimized", "vector": "false"}),
    ("/geocode", {"q": "Cairo", "lat": 30.0444, "lon": 31.2357, "vector": "false"}),
    (
        "/geocode",
        {"q": "Cairo", "lat": 30.0444, "lon": 31.2357, "effort": "high", "vector": "false"},
    ),
    ("/geocode", {"q": "15 Tahrir Street", "lat": 30.0444, "lon": 31.2357, "vector": "false"}),
    ("/geocode", {"q": "hospital", "limit": 25, "vector": "false"}),
    ("/geocode", {"q": "hospital", "limit": 1, "offset": 100, "vector": "false"}),
    ("/geocode", {"q": "مستشفى", "lat": 30.0, "lon": 31.0, "vector": "false"}),
    ("/geocode", {"q": "cafe", "vector": "false"}),
    ("/geocode", {"q": "cafe", "vector": "true"}),
    ("/geocode", {"q": "a", "vector": "false"}),
    ("/geocode", {"q": "Nile Corniche Road", "effort": "high", "limit": 50, "vector": "false"}),
    ("/geocode", {"q": "Cairo", "vector": "true", "effort": "high"}),
    ("/geocode", {"q": "Cairo", "vector": "true", "effort": "optimized", "lat": 30.0, "lon": 31.0}),
    ("/address", {"q": "15 Tahrir Street, Cairo"}),
    ("/address", {"q": "Tahrir", "city": "Cairo"}),
    ("/address", {"q": "15 Tahrir", "limit": 25}),
    ("/address", {"q": "Tahrir", "postcode": "11511"}),
    ("/address", {"q": "شارع التحرير", "lat": 30.0, "lon": 31.0}),
    ("/address", {"q": "Tahrir", "country": "EG"}),
]


def _fake_embed(texts):
    """Deterministic stand-in: the real model is absent from the test env."""
    return [[0.01 * (i + 1) for i in range(384)] for _ in texts]


async def _capture():
    captured = []

    class FakeES:
        async def search(self, **kw):
            captured.append({"kwargs": kw})
            return {"hits": {"total": {"value": 0}, "max_score": None, "hits": []}}

        async def update(self, **kw):
            return {}

        async def get(self, **kw):
            return {"_source": {}}

    original_es, original_embed = G.es, G.embed_texts
    G.es, G.embed_texts = FakeES(), _fake_embed
    try:
        transport = httpx.ASGITransport(app=G.app)
        results = []
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            for path, params in CASES:
                captured.clear()
                try:
                    status = (await c.get(path, params=params)).status_code
                except Exception as e:  # noqa: BLE001 - snapshot records the failure shape
                    status = f"EXC:{type(e).__name__}"
                results.append(
                    {"path": path, "params": params, "status": status, "queries": captured.copy()}
                )
        return json.loads(json.dumps(results, sort_keys=True, default=str, ensure_ascii=False))
    finally:
        G.es, G.embed_texts = original_es, original_embed


@pytest.mark.asyncio
async def test_every_case_builds_a_query():
    """A case that stops reaching ES silently stops being covered."""
    results = await _capture()
    empty = [r["params"] for r in results if not r["queries"]]
    assert not empty, f"no ES query generated for: {empty}"


@pytest.mark.asyncio
async def test_query_bodies_match_golden():
    expected = json.loads(GOLDEN.read_text(encoding="utf-8"))
    actual = await _capture()
    assert len(actual) == len(expected), "case list changed; review, then --update"
    drift = [{"params": e["params"]} for e, a in zip(expected, actual, strict=True) if e != a]
    assert not drift, (
        f"{len(drift)} query bodies changed, first: {drift[:2]}. "
        "If deliberate, --update then measure recall against the quality baseline."
    )


def test_search_spec_has_expected_shape():
    from shared.spec import load

    spec = load("search.toml")
    assert set(spec) == {"geocode", "autocomplete", "address", "text_full", "text_lean"}
    # the two-tier decay is load-bearing; a single decay regressed recall
    assert spec["autocomplete"]["regional"]["scale"] == "300km"
    assert spec["autocomplete"]["local"]["scale"] == "15km"
    assert spec["autocomplete"]["regional"]["weight"] > spec["autocomplete"]["local"]["weight"]


if __name__ == "__main__":
    import asyncio
    import sys

    if "--update" in sys.argv:
        GOLDEN.write_text(
            json.dumps(asyncio.run(_capture()), indent=1, sort_keys=True, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"rewrote {GOLDEN} — now run tests/run_recall.py against the baseline")
