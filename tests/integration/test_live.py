"""Opt-in integration tests against a live geocoder stack.

These are skipped by default (pyproject sets ``addopts = -m 'not integration'``).
Bring the stack up first, then run them explicitly::

    docker compose -f docker-compose.yaml -f docker-compose.ai.yaml up -d
    pytest -m integration --override-ini addopts=

The base URL can be overridden with GEOCODER_URL (default http://localhost:8000).
A query bias toward downtown Cairo is used since the default dataset is Egypt.
"""

import os

import httpx
import pytest

pytestmark = pytest.mark.integration

BASE_URL = os.getenv("GEOCODER_URL", "http://localhost:8000")
CAIRO = {"lat": 30.0444, "lon": 31.2357}


@pytest.fixture(scope="module")
def http():
    with httpx.Client(base_url=BASE_URL, timeout=30) as c:
        yield c


def test_health_is_up(http):
    resp = http.get("/health")
    assert resp.status_code in (200, 503)
    checks = resp.json()["checks"]
    # Critical dependencies must actually be reachable for a real stack.
    assert checks["elasticsearch"]["status"] == "ok"
    assert checks["postgis"]["status"] == "ok"


def test_geocode_returns_results(http):
    resp = http.get("/geocode", params={"q": "Cairo", "limit": 5, **CAIRO})
    assert resp.status_code == 200
    body = resp.json()
    results = body["results"] if isinstance(body, dict) else body
    assert isinstance(results, list)
    assert len(results) > 0


def test_reverse_returns_location(http):
    resp = http.get("/reverse", params=CAIRO)
    assert resp.status_code == 200
    assert resp.json()  # non-empty payload


def test_autocomplete_returns_suggestions(http):
    resp = http.get("/autocomplete", params={"q": "Cai", **CAIRO})
    assert resp.status_code == 200


def test_nearby_categories_discovery(http):
    resp = http.get("/nearby/categories")
    assert resp.status_code == 200
    body = resp.json()
    assert "food" in body["groups"]
    assert "restaurant" in body["values"]["food"]


def test_nearby_returns_pois_by_distance(http):
    resp = http.get("/nearby", params={**CAIRO, "radius": 3000, "sort": "distance"})
    assert resp.status_code == 200
    results = resp.json()["results"]
    assert len(results) > 0
    # nearest-first: distances are non-decreasing
    dists = [r["distance_m"] for r in results]
    assert dists == sorted(dists)
    # POI guard: no admin boundaries / place areas leak in
    for r in results:
        assert r["category_group"] not in ("place", "boundary")
        assert (r.get("admin_level") or 0) == 0


def test_nearby_category_filter(http):
    # Requires the category backfill to have run against the live index.
    resp = http.get("/nearby", params={**CAIRO, "radius": 5000, "category": "restaurant"})
    assert resp.status_code == 200
    results = resp.json()["results"]
    for r in results:
        assert r["category_value"] == "restaurant"
