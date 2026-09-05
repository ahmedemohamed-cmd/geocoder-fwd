-- Processed-file ledger schema specification.
--
-- Backs PROCESSED_LEDGER=pg, which is what makes multiple watcher replicas
-- safe on one data directory: a watcher claims a file here before parsing it,
-- so the same source file is never imported twice (AD-10).
CREATE TABLE IF NOT EXISTS processed_files (
    source       TEXT NOT NULL,             -- normalized data dir
    key          TEXT NOT NULL,             -- path relative to the data dir
    claimed_by   TEXT,
    claimed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ,               -- NULL while an import is in flight
    PRIMARY KEY (source, key)
);
