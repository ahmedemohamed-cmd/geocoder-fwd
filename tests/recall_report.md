# Cairo recall report (1000 popular places/addresses)

Queries geo-biased to downtown Cairo (30.0444, 31.2357), limit=10, vector=off, against the live /geocode API.
Each effort below is the `effort` query param: identical test set, identical index — only the query/scoring shape differs.

# High effort (default)

## Named places (750)

| metric | @1 | @5 | @10 |
|---|---|---|---|
| strict (same osm_id) | 87.5% | 97.3% | 98.8% |
| lenient (name or ≤150 m) | 96.8% | 99.1% | 99.3% |

## Addresses (250)

| metric | @1 | @5 | @10 |
|---|---|---|---|
| exact (same osm_id) | 90.0% | 94.4% | 96.0% |
| correct street | 96.0% | 97.6% | 99.2% |

## Interpolation probe (addresses, 250)

- Genuinely-absent (same-parity) house number on a known street returned an interpolated point: **139/197 (70.6%)**

- Excluded 53 cases where no absent number could be probed (every candidate already exists, so interpolation was never exercised).

# Optimized effort (lean fuzzy + rescore, no exact hit count)

## Named places (750)

| metric | @1 | @5 | @10 |
|---|---|---|---|
| strict (same osm_id) | 87.7% | 96.5% | 98.7% |
| lenient (name or ≤150 m) | 97.1% | 98.9% | 99.5% |

## Addresses (250)

| metric | @1 | @5 | @10 |
|---|---|---|---|
| exact (same osm_id) | 92.8% | 97.6% | 97.6% |
| correct street | 99.6% | 100.0% | 100.0% |

## Interpolation probe (addresses, 250)

- Genuinely-absent (same-parity) house number on a known street returned an interpolated point: **138/197 (70.1%)**

- Excluded 53 cases where no absent number could be probed (every candidate already exists, so interpolation was never exercised).
