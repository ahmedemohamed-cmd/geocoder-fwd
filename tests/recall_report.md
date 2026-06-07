# Cairo recall report (1000 popular places/addresses)

Queries geo-biased to downtown Cairo (30.0444, 31.2357), limit=10, vector=off.

## Named places (750)

| metric | @1 | @5 | @10 |
|---|---|---|---|
| strict (same osm_id) | 88.7% | 97.5% | 98.7% |
| lenient (name or ≤150 m) | 96.3% | 99.1% | 99.3% |

## Addresses (250)

| metric | @1 | @5 | @10 |
|---|---|---|---|
| exact (same osm_id) | 90.8% | 95.2% | 97.2% |
| correct street | 96.4% | 97.6% | 98.0% |

## Interpolation probe (addresses, 250)

- Non-existent house number on a known street returned an interpolated point: **63/250 (25.2%)**
