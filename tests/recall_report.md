# Cairo recall report (1000 popular places/addresses)

Queries geo-biased to downtown Cairo (30.0444, 31.2357), limit=10, vector=off, against the live /geocode API.
Each effort below is the `effort` query param: identical test set, identical index — only the query/scoring shape differs.

# Optimized effort (lean fuzzy + rescore, no exact hit count)

## Named places (750)

| metric | @1 | @5 | @10 |
|---|---|---|---|
| strict (same osm_id) | 86.5% | 96.8% | 98.3% |
| lenient (name or ≤150 m) | 95.3% | 98.8% | 99.2% |

## Addresses (250)

| metric | @1 | @5 | @10 |
|---|---|---|---|
| exact (same osm_id) | 92.8% | 97.6% | 99.2% |
| correct street | 99.6% | 100.0% | 100.0% |

## Interpolation probe (addresses, 250)

- Genuinely-absent (same-parity) house number on a known street returned an interpolated point: **138/196 (70.4%)**

- Excluded 54 cases where no absent number could be probed (every candidate already exists, so interpolation was never exercised).
