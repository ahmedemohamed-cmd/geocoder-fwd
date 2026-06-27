"""Convert raw place exports → the unified place schema, size-split for git.

Reads one or more raw export files (Pelias ``{_id,_source}`` lists and/or
Postgres ``places`` row lists — or already-unified files), normalises every
record with ``shared.places_mapping.to_unified`` (lossless), then writes the
result back out as one or more JSON arrays kept under ``--max-mb`` so each file
stays below GitHub's 100 MB limit.

Output naming: a single output part is ``<name>.json``; multiple parts are
``<name>.part1.json``, ``<name>.part2.json``, … All inputs are fully read
before anything is written, so output paths may overwrite inputs.

Examples:
    # Pelias google → unified, split into <=90 MB parts
    python scripts/unify_places.py --name pelias_google --out-dir data/places --max-mb 90 \
        data/places/pelias_google.part1.json data/places/pelias_google.part2.json

    # Postgres places → unified (single file)
    python scripts/unify_places.py --name places_pg --out-dir data/places \
        data/places/places_pg.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from shared.places_mapping import to_unified


def _load(paths: list[str]) -> list[dict]:
    records: list[dict] = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise SystemExit(f"{path}: expected a JSON array")
        records.extend(data)
    return records


def _split_by_size(records: list[dict], max_bytes: int) -> list[list[dict]]:
    """Greedily pack records into chunks whose encoded size stays < max_bytes."""
    parts: list[list[dict]] = [[]]
    size = 2  # "[]"
    for rec in records:
        enc = len(json.dumps(rec, ensure_ascii=False).encode()) + 1  # + comma
        if parts[-1] and size + enc > max_bytes:
            parts.append([])
            size = 2
        parts[-1].append(rec)
        size += enc
    return parts


def main() -> None:
    ap = argparse.ArgumentParser(description="Convert raw place exports to the unified schema")
    ap.add_argument("inputs", nargs="+", help="Raw export file(s) to convert")
    ap.add_argument("--name", required=True, help="Output base name (e.g. pelias_google)")
    ap.add_argument("--out-dir", required=True, help="Directory to write the unified file(s) into")
    ap.add_argument("--max-mb", type=float, default=90.0, help="Max size per output file (MB)")
    args = ap.parse_args()

    raw = _load(args.inputs)
    unified, skipped = [], 0
    for rec in raw:
        u = to_unified(rec)
        if u is None:
            skipped += 1
            continue
        unified.append(u)
    print(f"converted {len(unified)} records ({skipped} skipped) from {len(args.inputs)} file(s)")

    parts = _split_by_size(unified, int(args.max_mb * 1_000_000))
    os.makedirs(args.out_dir, exist_ok=True)
    written = []
    for i, part in enumerate(parts, 1):
        if len(parts) == 1:
            out = os.path.join(args.out_dir, f"{args.name}.json")
        else:
            out = os.path.join(args.out_dir, f"{args.name}.part{i}.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump(part, f, ensure_ascii=False)
        os.chmod(out, 0o644)
        mb = os.path.getsize(out) / 1e6
        written.append(out)
        print(f"  wrote {out}  ({len(part)} records, {mb:.1f} MB)")
        if mb >= 100:
            print(f"  WARNING: {out} is >= 100 MB", file=sys.stderr)
    print(f"done: {len(written)} file(s)")


if __name__ == "__main__":
    main()
