"""Unit tests for services/enrichment.py.

Infra-free: `address_needs_refresh` is pure; `enrich_address` is driven with a
fake asyncpg pool (async `acquire()` context manager) and a fake ES exposing
`mget`/`update`, so the moved PostGIS→ES logic is exercised without a real stack.
"""

from services.enrichment import address_needs_refresh, enrich_address


# ── address_needs_refresh (pure) ──────────────────────────────────────────────
def test_none_needs_refresh():
    assert address_needs_refresh(None, "node/1", 0, None) is True


def test_fresh_point_address_is_kept():
    addr = {
        "nearest_street": {"osm_id": "way/5"},
        "parents": [{"osm_id": "rel/9", "area_km2": 100}],
    }
    assert address_needs_refresh(addr, "node/1", 0, None) is False


def test_parent_missing_area_km2_is_stale():
    addr = {"nearest_street": None, "parents": [{"osm_id": "rel/9"}]}  # no area_km2
    assert address_needs_refresh(addr, "node/1", 0, None) is True


def test_self_referential_parent_is_stale():
    addr = {"nearest_street": None, "parents": [{"osm_id": "node/1", "area_km2": 100}]}
    assert address_needs_refresh(addr, "node/1", 0, None) is True


def test_parent_not_larger_than_area_feature_is_stale():
    addr = {"nearest_street": None, "parents": [{"osm_id": "rel/9", "area_km2": 2.0}]}
    # feature area 5 km²; a 2 km² "parent" is really a sub-zone → refresh
    assert address_needs_refresh(addr, "rel/1", 5.0, None) is True


def test_street_on_admin_doc_is_stale():
    addr = {"nearest_street": {"osm_id": "way/5"}, "parents": []}
    # admin_level set → nearest_street should have been skipped → refresh
    assert address_needs_refresh(addr, "rel/1", 0, 8) is True


# ── enrich_address (fake clients) ─────────────────────────────────────────────
class _FakeConn:
    def __init__(self, lines, polys, closed):
        self._lines, self._polys, self._closed = lines, polys, closed

    async def execute(self, *a):
        return "SET"

    async def fetch(self, query, *a):
        if "ST_DWithin" in query:
            return self._lines
        if "ST_IsClosed" in query:
            return self._closed
        if "ST_Polygon" in query:
            return self._polys
        return []


class _FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *a):
        return False


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _FakeAcquire(self._conn)


class _FakeES:
    def __init__(self, docs):
        self.docs = docs
        self.updated = []

    async def mget(self, index, ids, request_timeout=None):
        return {
            "docs": [
                {"_id": i, "found": i in self.docs, "_source": self.docs.get(i, {})} for i in ids
            ]
        }

    async def update(self, index, id, body):
        self.updated.append((id, body))


async def test_enrich_address_none_centroid_returns_none():
    es = _FakeES({})
    pool = _FakePool(_FakeConn([], [], []))
    assert await enrich_address(pool, es, "osm_places", "node/1", None) is None
    assert es.updated == []  # nothing computed, nothing cached


async def test_enrich_address_builds_and_caches():
    conn = _FakeConn(
        lines=[{"osm_id": "way/5", "osm_type": "way"}],
        polys=[{"osm_id": "rel/9", "osm_type": "relation"}],
        closed=[],
    )
    es = _FakeES(
        {
            "way/5": {"name": "Talaat Harb"},
            "rel/9": {"name": "Cairo", "admin_level": 6, "area_km2": 100.0},
        }
    )
    addr = await enrich_address(
        _FakePool(conn), es, "osm_places", "node/1", {"lat": 30.0, "lon": 31.2}
    )
    assert addr["nearest_street"]["osm_id"] == "way/5"
    assert addr["nearest_street"]["name"] == "Talaat Harb"
    assert [p["osm_id"] for p in addr["parents"]] == ["rel/9"]
    assert addr["parents"][0]["area_km2"] == 100.0
    # cached back to ES on the same doc
    assert es.updated == [("node/1", {"doc": {"address": addr}})]


async def test_enrich_address_caches_empty_when_no_neighbours():
    es = _FakeES({})
    addr = await enrich_address(
        _FakePool(_FakeConn([], [], [])), es, "osm_places", "node/1", {"lat": 30.0, "lon": 31.2}
    )
    assert addr == {"nearest_street": None, "parents": []}
    assert es.updated == [("node/1", {"doc": {"address": {"nearest_street": None, "parents": []}}})]
