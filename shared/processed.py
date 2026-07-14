"""Ledger of already-imported source files (file- or Postgres-backed).

File mode (default, ``PROCESSED_LEDGER=file``): one ledger file (``.processed``)
per data dir, one processed file name (path relative to the data dir) per line.
Legacy per-file ``<file>.processed`` markers are transparently migrated into the
ledger (and removed) the first time a file is seen.

Postgres mode (``PROCESSED_LEDGER=pg``): the ledger lives in a
``processed_files`` table in the shared PostGIS database, which makes it safe
for multiple watcher replicas on different nodes:

- :func:`claim` atomically claims a file (``INSERT .. ON CONFLICT`` — one winner)
  so two replicas never import the same file; a claim whose worker died is
  reclaimable after ``PROCESSED_CLAIM_TTL`` seconds.
- :func:`record_processed` marks the claim completed.
- The file ledger is still written best-effort in pg mode, and imported into
  Postgres on first use, so switching modes never re-imports old files.

The public functions stay synchronous (watchers call them inline between async
NATS publishes); pg mode bridges to asyncpg via a dedicated thread running its
own event loop — ledger calls are per-file, not per-row, so this is cold path.
"""

from __future__ import annotations

import asyncio
import os
import socket
from concurrent.futures import ThreadPoolExecutor

from shared.logging import get_logger

logger = get_logger("processed-ledger")

LEDGER_NAME = ".processed"

LEDGER_MODE = os.getenv("PROCESSED_LEDGER", "file").strip().lower()
# Seconds after which an unfinished claim (crashed worker) may be taken over.
CLAIM_TTL = int(os.getenv("PROCESSED_CLAIM_TTL", "21600"))  # 6 h

_SCHEMA = """
CREATE TABLE IF NOT EXISTS processed_files (
    source       TEXT NOT NULL,             -- normalized data dir
    key          TEXT NOT NULL,             -- path relative to the data dir
    claimed_by   TEXT,
    claimed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ,               -- NULL while an import is in flight
    PRIMARY KEY (source, key)
);
"""


def ledger_path(data_dir: str) -> str:
    return os.path.join(data_dir, LEDGER_NAME)


def _source(data_dir: str) -> str:
    return os.path.normpath(os.path.abspath(data_dir))


def _key(data_dir: str, filepath: str) -> str:
    return os.path.relpath(filepath, data_dir)


def _worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


# ── file backend ──────────────────────────────────────────────────────────────
def _file_load(data_dir: str) -> set[str]:
    try:
        with open(ledger_path(data_dir), encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}
    except FileNotFoundError:
        return set()


def _file_append(data_dir: str, key: str) -> None:
    try:
        with open(ledger_path(data_dir), "a", encoding="utf-8") as f:
            f.write(key + "\n")
    except OSError as e:
        # Best-effort in pg mode (pg is authoritative); fatal-ish in file mode
        # but raising here would abort a whole watcher pass over one fsync issue.
        logger.warning("ledger append failed for %s: %s", key, e)


# ── postgres backend (sync bridge over asyncpg) ───────────────────────────────
# One dedicated thread runs each ledger coroutine in its own event loop, so the
# sync API can be called from inside the watchers' running loops. Connections
# are per-call: ledger traffic is a handful of statements per file import.
_pg_thread = ThreadPoolExecutor(max_workers=1, thread_name_prefix="processed-pg")
_pg_ready = False


def _dsn() -> str:
    from shared.config import (
        POSTGRES_DB,
        POSTGRES_HOST,
        POSTGRES_PASSWORD,
        POSTGRES_PORT,
        POSTGRES_USER,
    )

    return (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )


def _pg_run(coro_fn, *args):
    return _pg_thread.submit(lambda: asyncio.run(coro_fn(*args))).result()


async def _pg_exec(sql_calls):
    """Open a connection, run [(sql, args)] and return the last statement's rows."""
    import asyncpg

    conn = await asyncpg.connect(_dsn())
    try:
        rows = None
        for sql, args in sql_calls:
            rows = await conn.fetch(sql, *args)
        return rows
    finally:
        await conn.close()


def _pg_init(data_dir: str) -> None:
    """Create the table and import the legacy file ledger once per process."""
    global _pg_ready
    if _pg_ready:
        return
    calls = [(_SCHEMA, ())]
    for key in sorted(_file_load(data_dir)):
        calls.append(
            (
                """INSERT INTO processed_files (source, key, claimed_by, processed_at)
                   VALUES ($1, $2, 'file-ledger-import', now())
                   ON CONFLICT (source, key) DO NOTHING""",
                (_source(data_dir), key),
            )
        )
    _pg_run(_pg_exec, calls)
    _pg_ready = True


def _pg_load(data_dir: str) -> set[str]:
    _pg_init(data_dir)
    rows = _pg_run(
        _pg_exec,
        [
            (
                "SELECT key FROM processed_files WHERE source=$1 AND processed_at IS NOT NULL",
                (_source(data_dir),),
            )
        ],
    )
    return {r["key"] for r in rows}


def _pg_mark_done(data_dir: str, key: str) -> None:
    _pg_run(
        _pg_exec,
        [
            (
                """INSERT INTO processed_files (source, key, claimed_by, processed_at)
                   VALUES ($1, $2, $3, now())
                   ON CONFLICT (source, key)
                   DO UPDATE SET processed_at = COALESCE(processed_files.processed_at, now())""",
                (_source(data_dir), key, _worker_id()),
            )
        ],
    )


def _pg_claim(data_dir: str, key: str) -> bool:
    """Atomically claim *key*: True for exactly one replica.

    A row inserts (new claim), or takes over an unfinished claim older than
    CLAIM_TTL (crashed worker). Completed files are never reclaimed.
    """
    rows = _pg_run(
        _pg_exec,
        [
            (
                f"""INSERT INTO processed_files (source, key, claimed_by)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (source, key) DO UPDATE
                       SET claimed_by = EXCLUDED.claimed_by, claimed_at = now()
                     WHERE processed_files.processed_at IS NULL
                       AND processed_files.claimed_at < now() - interval '{CLAIM_TTL} seconds'
                    RETURNING key""",
                (_source(data_dir), key, _worker_id()),
            )
        ],
    )
    return bool(rows)


# ── public API (used by oa/gn/places watchers) ───────────────────────────────
def load_processed(data_dir: str) -> set[str]:
    """Return the set of processed file keys recorded in the ledger."""
    if LEDGER_MODE == "pg":
        return _pg_load(data_dir)
    return _file_load(data_dir)


def is_processed(data_dir: str, filepath: str, done: set[str]) -> bool:
    """True if *filepath* was already imported.

    *done* is the in-memory set returned by :func:`load_processed` for this pass;
    it is updated in place. A legacy ``<filepath>.processed`` sidecar is migrated
    into the ledger (and deleted) so old imports keep being skipped.
    """
    key = _key(data_dir, filepath)
    if key in done:
        return True
    legacy = f"{filepath}.processed"
    if os.path.exists(legacy):
        record_processed(data_dir, filepath, done)
        try:
            os.remove(legacy)
        except OSError:
            pass
        return True
    return False


def record_processed(data_dir: str, filepath: str, done: set[str]) -> None:
    """Record *filepath* as processed in the ledger (idempotent)."""
    key = _key(data_dir, filepath)
    if key in done:
        return
    if LEDGER_MODE == "pg":
        _pg_mark_done(data_dir, key)
    _file_append(data_dir, key)  # best-effort mirror in pg mode
    done.add(key)


def claim(data_dir: str, filepath: str, done: set[str]) -> bool:
    """Atomically claim *filepath* for import; exactly one replica gets True.

    In file mode (single-writer dev setups) every not-yet-done file is
    trivially claimable. In pg mode this is the replica-safety gate: call it
    right before importing a pending file and skip the file when it returns
    False (another replica owns it)."""
    key = _key(data_dir, filepath)
    if key in done:
        return False
    if LEDGER_MODE == "pg":
        return _pg_claim(data_dir, key)
    return True
