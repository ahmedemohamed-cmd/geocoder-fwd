"""Single-file ledger of already-imported source files.

Instead of dropping a per-file ``<file>.processed`` marker next to every input,
each watcher keeps ONE ledger file (``.processed``) in its data dir and appends
one processed file name (path relative to the data dir) per line.

Legacy per-file ``<file>.processed`` markers are transparently migrated into the
ledger (and removed) the first time a file is seen, so existing imports are not
re-processed after the switch.
"""
import os

LEDGER_NAME = ".processed"


def ledger_path(data_dir: str) -> str:
    return os.path.join(data_dir, LEDGER_NAME)


def load_processed(data_dir: str) -> set[str]:
    """Return the set of processed file keys recorded in the ledger."""
    try:
        with open(ledger_path(data_dir), "r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}
    except FileNotFoundError:
        return set()


def _append(data_dir: str, key: str) -> None:
    with open(ledger_path(data_dir), "a", encoding="utf-8") as f:
        f.write(key + "\n")


def _key(data_dir: str, filepath: str) -> str:
    return os.path.relpath(filepath, data_dir)


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
        _append(data_dir, key)
        done.add(key)
        try:
            os.remove(legacy)
        except OSError:
            pass
        return True
    return False


def record_processed(data_dir: str, filepath: str, done: set[str]) -> None:
    """Record *filepath* as processed in the ledger (idempotent)."""
    key = _key(data_dir, filepath)
    if key not in done:
        _append(data_dir, key)
        done.add(key)
