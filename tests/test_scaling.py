"""Horizontal-scalability primitives: leaderless cell scheduling, per-cell
provider fetch, and the Postgres-backed processed-file ledger.

Postgres tests reuse the billing_test database (same in-container runner as
tests/billing); Redis is fakeredis. Set POSTGRES_DB=billing_test to run the
ledger tests, they skip when Postgres is unreachable.
"""

import asyncio
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("POSTGRES_DB", "billing_test")

import fakeredis.aioredis

from services.traffic_aggregator import _schedule_window
from shared import processed
from shared.nats_client import TRAFFIC_CELLS_STREAM_CFG
from shared.traffic_providers import TomTomFlowProvider


# ── cell scheduler: leaderless NX dedupe ──────────────────────────────────────
class _FakeJS:
    def __init__(self):
        self.published: list[dict] = []

    async def publish(self, subject, data):
        self.published.append(json.loads(data))


def test_schedule_window_dedupes_across_replicas():
    async def main():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        points = [(30.0 + i * 0.1, 31.0 + i * 0.1) for i in range(5)]
        js_a, js_b = _FakeJS(), _FakeJS()

        # two "replicas" race the same window against shared Redis
        a, b = await asyncio.gather(
            _schedule_window(r, js_a, points, window=100, interval=120),
            _schedule_window(r, js_b, points, window=100, interval=120),
        )
        assert a + b == len(points)  # every cell enqueued exactly once
        assert len(js_a.published) + len(js_b.published) == len(points)
        cells = sorted(m["cell"] for m in js_a.published + js_b.published)
        assert cells == list(range(len(points)))

        # same window again: nothing left to enqueue
        assert await _schedule_window(r, _FakeJS(), points, window=100, interval=120) == 0
        # next window: a fresh round
        assert await _schedule_window(r, _FakeJS(), points, window=101, interval=120) == 5
        await r.aclose()

    asyncio.run(main())


def test_cells_stream_is_a_work_queue():
    # WORKQUEUE retention = each cell delivered to exactly one worker.
    assert TRAFFIC_CELLS_STREAM_CFG.retention.value == "workqueue"
    assert TRAFFIC_CELLS_STREAM_CFG.subjects == ["traffic.cells"]


# ── provider fetch_cell ───────────────────────────────────────────────────────
class _StubResp:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class _StubClient:
    def __init__(self, resp):
        self._resp = resp
        self.calls = 0

    async def get(self, url, params=None):
        self.calls += 1
        return self._resp


def test_fetch_cell_and_grid_fetch():
    async def main():
        prov = TomTomFlowProvider("29.7,31.0,30.2,31.5", 2, "key", "http://stub")

        ok = _StubClient(_StubResp(200, {"flowSegmentData": {"currentSpeed": 42}}))
        obs = await prov.fetch_cell(ok, 30.0, 31.2)
        assert obs == {"lat": 30.0, "lon": 31.2, "kph": 42.0}

        # whole-grid fetch is now a loop over fetch_cell (2x2 grid = 4 calls)
        assert len(await prov.fetch(ok)) == 4
        assert ok.calls == 5

        bad = _StubClient(_StubResp(500, {}))
        assert await prov.fetch_cell(bad, 30.0, 31.2) is None
        empty = _StubClient(_StubResp(200, {}))
        assert await prov.fetch_cell(empty, 30.0, 31.2) is None

    asyncio.run(main())


# ── processed-file ledger ─────────────────────────────────────────────────────
def test_file_ledger_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(processed, "LEDGER_MODE", "file")
    d = str(tmp_path)
    f = str(tmp_path / "a.csv")
    open(f, "w").write("x")

    done = processed.load_processed(d)
    assert not processed.is_processed(d, f, done)
    assert processed.claim(d, f, done)  # file mode: trivially claimable
    processed.record_processed(d, f, done)
    assert processed.is_processed(d, f, done)
    assert not processed.claim(d, f, done)  # already done
    assert processed.load_processed(d) == {"a.csv"}


def _pg_available() -> bool:
    import asyncpg

    async def ping():
        conn = await asyncpg.connect(processed._dsn())
        await conn.close()

    try:
        asyncio.run(ping())
        return True
    except Exception:
        return False


@pytest.fixture
def pg_ledger(tmp_path, monkeypatch):
    if not _pg_available():
        pytest.skip("postgres not reachable")
    monkeypatch.setattr(processed, "LEDGER_MODE", "pg")
    monkeypatch.setattr(processed, "_pg_ready", False)
    yield str(tmp_path)
    # cleanup rows for this test's unique source dir
    processed._pg_run(
        processed._pg_exec,
        [("DELETE FROM processed_files WHERE source=$1", (processed._source(str(tmp_path)),))],
    )


def test_pg_ledger_claim_exactly_once(pg_ledger, tmp_path):
    d = pg_ledger
    f = str(tmp_path / "file1.csv")
    open(f, "w").write("x")

    done = processed.load_processed(d)
    assert done == set()
    # two "replicas" (fresh done-sets) race the claim: exactly one wins
    wins = [processed.claim(d, f, set()), processed.claim(d, f, set())]
    assert sorted(wins) == [False, True]

    processed.record_processed(d, f, done)
    assert processed.load_processed(d) == {"file1.csv"}
    assert not processed.claim(d, f, set())  # completed files never reclaim


def test_pg_ledger_stale_claim_takeover(pg_ledger, tmp_path, monkeypatch):
    d = pg_ledger
    f = str(tmp_path / "file2.csv")
    open(f, "w").write("x")

    assert processed.claim(d, f, set())  # replica A claims, then "crashes"
    assert not processed.claim(d, f, set())  # fresh claim blocked
    monkeypatch.setattr(processed, "CLAIM_TTL", 0)  # claim is now stale
    assert processed.claim(d, f, set())  # replica B takes over


def test_pg_ledger_imports_file_ledger(pg_ledger, tmp_path):
    d = pg_ledger
    # legacy file-ledger entries are imported as completed on first pg use
    open(tmp_path / ".processed", "w").write("old1.csv\nold2.csv\n")
    assert processed.load_processed(d) == {"old1.csv", "old2.csv"}
    assert not processed.claim(d, str(tmp_path / "old1.csv"), set())


if __name__ == "__main__":
    test_schedule_window_dedupes_across_replicas()
    test_cells_stream_is_a_work_queue()
    test_fetch_cell_and_grid_fetch()
    print("in-memory tests OK (run under pytest for the pg-ledger tests)")
