"""Infra-free smoke tests: request validation and feature gating.

These assert the contract the framework enforces *before* a backend is touched
(422 on bad params) and the explicit 503 gating on optional features, so they
run without ES/PostGIS/NATS/Redis.
"""

from unittest.mock import AsyncMock

import pytest

import services.geocoder as geocoder


async def test_features_reports_flags(client):
    resp = await client.get("/features")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == {"vectors", "ai", "traffic"}
    assert all(isinstance(v, bool) for v in body.values())


@pytest.mark.parametrize(
    "url",
    [
        "/geocode",  # missing q
        "/geocode?q=",  # empty q (min_length=1)
        "/autocomplete",  # missing q
        "/reverse",  # missing lat/lon
        "/reverse?lat=30.0",  # missing lon
        "/reverse?lat=foo&lon=bar",  # non-numeric
        "/deep/forward?q=cairo",  # missing mandatory language
        "/traffic/edge?lat=30.0",  # missing lon
    ],
)
async def test_missing_or_bad_params_return_422(client, url):
    resp = await client.get(url)
    assert resp.status_code == 422


async def test_deep_forward_disabled_without_api_key(client, monkeypatch):
    # Valid params so we get past validation and reach the gating check.
    monkeypatch.setattr(geocoder, "ENABLE_DEEP", True)
    monkeypatch.setattr(geocoder, "GOOGLE_MAPS_API_KEY", "")
    resp = await client.get("/deep/forward?q=cairo&language=en")
    assert resp.status_code == 503


async def test_deep_forward_disabled_by_flag(client, monkeypatch):
    monkeypatch.setattr(geocoder, "ENABLE_DEEP", False)
    resp = await client.get("/deep/forward?q=cairo&language=en")
    assert resp.status_code == 503


async def test_traffic_edge_disabled_returns_503(client, monkeypatch):
    monkeypatch.setattr(geocoder, "ENABLE_TRAFFIC", False)
    resp = await client.get("/traffic/edge?lat=30.0&lon=31.2")
    assert resp.status_code == 503


async def test_health_reports_dependencies(client, monkeypatch):
    # Avoid the real Ollama network probe; the ES/PostGIS clients are unset
    # (lifespan didn't run), so the check reports them down and returns 503.
    monkeypatch.setattr(geocoder, "is_ollama_available", AsyncMock(return_value=False))
    resp = await client.get("/health")
    assert resp.status_code in (200, 503)
    body = resp.json()
    assert "checks" in body
    assert set(body["checks"]) >= {"elasticsearch", "postgis", "nats", "redis", "ollama"}
