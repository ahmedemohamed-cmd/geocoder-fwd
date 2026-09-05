"""Fixtures for the implementation-scoped suite.

These tests exercise internals of the current implementation. They are NOT the
regeneration contract — see tests/acceptance/ for that — and are expected to be
rewritten alongside any code that replaces them.
"""

import os
import sys

import httpx
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import services.geocoder as geocoder  # noqa: E402


@pytest_asyncio.fixture
async def client():
    transport = httpx.ASGITransport(app=geocoder.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
