"""Fixtures for the geocoder API smoke tests.

These tests are infra-free: the app is driven over httpx ``ASGITransport``,
which does not run the FastAPI lifespan, so the real ES/PostGIS/NATS/Redis
clients are never connected (the module globals stay ``None``). Validation and
feature-gating happen before any client is used, so most endpoints can be
exercised without mocks; tests that need a backend patch the relevant module
global explicitly.
"""

import os
import sys

import httpx
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

import services.geocoder as geocoder  # noqa: E402


@pytest_asyncio.fixture
async def client():
    transport = httpx.ASGITransport(app=geocoder.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
