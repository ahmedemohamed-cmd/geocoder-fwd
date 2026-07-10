"""Tests for the /nearby endpoint.

Infra-free: validation runs before any backend, and the ES search is replaced
with a fake that captures the query body so we can assert its shape (the fake
doesn't execute filters — we verify the clauses are present). The dedicated
cache is disabled so no Redis is touched.
"""

import pytest

import services.nearby as nearby
from services.cache_service import ResultCache


class FakeES:
    """Captures the last search body and returns canned hits."""

    def __init__(self, hits, total=None):
        self._hits = hits
        self._total = len(hits) if total is None else total
        self.captured: dict = {}

    async def search(self, index, **body):
        self.captured = {"index": index, **body}
        return {
            "hits": {
                "total": {"value": self._total},
                "max_score": 2.0,
                "hits": self._hits,
            }
        }


def _hit(osm_id, name, lat, lon, *, cat=None, tags=None, score=2.0):
    src = {
        "osm_id": osm_id,
        "osm_type": "node",
        "name": name,
        "centroid": {"lat": lat, "lon": lon},
        "tags": tags or {},
    }
    if cat:
        src["category_key"], src["category_value"], src["category_group"] = cat
    # Real ES returns a `sort` array per hit when the query sorts; mirror that so
    # the cursor (search_after) path can build next_cursor from the last hit.
    return {"_source": src, "_score": score, "sort": [score, osm_id]}


@pytest.fixture
def patched(monkeypatch):
    """Patch the ES client + disable the cache. Returns the FakeES for assertions.

    Each test can swap the hit set via ``patched.es._hits`` before calling.
    """
    fake = FakeES(
        [
            _hit(
                "node/1",
                "Koshary Abou Tarek",
                30.0455,
                31.2440,
                cat=("amenity", "restaurant", "food"),
                tags={"amenity": "restaurant", "name": "Koshary Abou Tarek"},
            )
        ],
        total=5,  # more than one page, so offset-mode has_more is exercised
    )
    monkeypatch.setattr(nearby, "_es", fake)
    # Disabled cache: get() -> None (miss), set() -> no-op, no Redis needed.
    disabled = ResultCache(None, enabled=False, ttl=0, coord_precision=4, prefix="nearby")
    monkeypatch.setattr(nearby, "_cache", disabled)
    return fake


# ── validation (no backend needed) ────────────────────────────────────────────
@pytest.mark.parametrize(
    "url",
    [
        "/nearby",  # missing lat & lon
        "/nearby?lat=30.0",  # missing lon
        "/nearby?lon=31.2",  # missing lat
        "/nearby?lat=foo&lon=bar",  # non-numeric
        "/nearby?lat=30.0&lon=31.2&radius=99999999",  # over NEARBY_MAX_RADIUS_M
        "/nearby?lat=30.0&lon=31.2&radius=0",  # below min
        "/nearby?lat=30.0&lon=31.2&sort=whatever",  # bad sort
        "/nearby?lat=200&lon=31.2",  # lat out of range
    ],
)
async def test_bad_params_return_422(client, url):
    resp = await client.get(url)
    assert resp.status_code == 422


async def test_missing_es_returns_503(client, monkeypatch):
    monkeypatch.setattr(nearby, "_es", None)
    resp = await client.get("/nearby?lat=30.0&lon=31.2")
    assert resp.status_code == 503


# ── discovery ─────────────────────────────────────────────────────────────────
async def test_categories_discovery_needs_no_backend(client):
    resp = await client.get("/nearby/categories")
    assert resp.status_code == 200
    body = resp.json()
    assert "food" in body["groups"]
    assert "restaurant" in body["values"]["food"]


# ── query construction ────────────────────────────────────────────────────────
async def test_distance_sort_builds_geo_distance_query(client, patched):
    resp = await client.get(
        "/nearby?lat=30.0455&lon=31.2440&category=restaurant,cafe&sort=distance&radius=800"
    )
    assert resp.status_code == 200
    assert resp.headers["X-Cache"] == "MISS"

    body = patched.captured
    bool_q = body["query"]["bool"]
    # geo_distance radius filter present
    geo = [f for f in bool_q["filter"] if "geo_distance" in f]
    assert geo and geo[0]["geo_distance"]["distance"] == "800m"
    # POI guard present
    assert {"range": {"admin_level": {"gt": 0}}} in bool_q["must_not"]
    assert any("area_km2" in f.get("range", {}) for f in bool_q["must_not"])
    assert {"terms": {"category_group": ["place", "boundary"]}} in bool_q["must_not"]
    # category filter (comma-split into a list)
    assert {"terms": {"category_value": ["restaurant", "cafe"]}} in bool_q["filter"]
    # nearest-first sort
    assert "_geo_distance" in body["sort"][0]

    # each result carries a computed distance_m
    row = resp.json()["results"][0]
    assert row["distance_m"] is not None and row["distance_m"] >= 0
    assert row["category_value"] == "restaurant"


async def test_best_sort_is_default_and_uses_function_score(client, patched):
    resp = await client.get("/nearby?lat=30.0455&lon=31.2440")
    assert resp.status_code == 200
    fs = patched.captured["query"]["function_score"]
    assert fs["boost_mode"] == "replace"
    # best mode sorts by composite score + a deterministic tiebreak (for cursors)
    assert patched.captured["sort"] == [{"_score": {"order": "desc"}}, {"osm_id": "asc"}]
    assert resp.json()["query"]["sort"] == "best"


async def test_first_page_uses_offset_and_returns_next_cursor(client, patched):
    resp = await client.get("/nearby?lat=30.0455&lon=31.2440&sort=distance&limit=1")
    assert resp.status_code == 200
    # no cursor supplied → offset paging (from present, no search_after)
    assert patched.captured.get("from") == 0
    assert "search_after" not in patched.captured
    # a full page (1 hit, limit 1) with more available → a cursor to continue
    pg = resp.json()["pagination"]
    assert pg["has_more"] is True
    assert pg["next_cursor"]


async def test_cursor_page_uses_search_after_not_offset(client, patched):
    first = await client.get("/nearby?lat=30.0455&lon=31.2440&sort=distance&limit=1")
    token = first.json()["pagination"]["next_cursor"]

    resp = await client.get(f"/nearby?lat=30.0455&lon=31.2440&sort=distance&limit=1&cursor={token}")
    assert resp.status_code == 200
    # cursor present → search_after set, `from` omitted (ES forbids from>0 with it)
    assert "from" not in patched.captured
    assert patched.captured["search_after"] == [2.0, "node/1"]


async def test_offset_ignored_when_cursor_present(client, patched):
    first = await client.get("/nearby?lat=30.0455&lon=31.2440&limit=1")
    token = first.json()["pagination"]["next_cursor"]
    resp = await client.get(f"/nearby?lat=30.0455&lon=31.2440&limit=1&offset=40&cursor={token}")
    assert resp.status_code == 200
    assert "from" not in patched.captured  # offset ignored in favour of the cursor


async def test_invalid_cursor_returns_400(client, patched):
    resp = await client.get("/nearby?lat=30.0455&lon=31.2440&cursor=not-a-valid-token!!")
    assert resp.status_code == 400


async def test_group_filter_adds_category_group_terms(client, patched):
    # sort=distance keeps the bool query top-level (best nests it in function_score).
    resp = await client.get("/nearby?lat=30.0455&lon=31.2440&group=food&group=health&sort=distance")
    assert resp.status_code == 200
    bool_q = patched.captured["query"]["bool"]
    assert {"terms": {"category_group": ["food", "health"]}} in bool_q["filter"]


async def test_category_falls_back_to_classify_when_unindexed(client, patched):
    # Simulate a doc indexed before the category backfill: no category_* fields,
    # but tags say it's a restaurant. The response should still be classified.
    patched._hits = [_hit("node/9", "Old Place", 30.05, 31.25, tags={"amenity": "restaurant"})]
    resp = await client.get("/nearby?lat=30.0455&lon=31.2440")
    assert resp.status_code == 200
    row = resp.json()["results"][0]
    assert row["category_value"] == "restaurant"
    assert row["category_group"] == "food"
