# Search-quality baseline

Unit tests prove the code runs. They say nothing about whether the geocoder
*finds the right place*. This file pins the numbers that do, so any change to
search, ranking, autocomplete, categories, or the ES mapping can be judged
against a fixed reference instead of a vibe.

**Status:** frozen 2026-09-05 from the reports generated on 2026-07-13
(`tests/recall_report.md`, `tests/autocomplete_report.md`, commit `caf061d`).

## How to reproduce

The harnesses query a **live** stack, so a full local build is required first.

```bash
docker compose up -d                       # stack must be indexed
python3 tests/build_testset.py             # regenerates tests/cairo_testset.json
python3 tests/run_recall.py                # -> tests/recall_report.md
python3 tests/run_autocomplete_recall.py   # -> tests/autocomplete_report.md
```

`tests/cairo_testset.json` (1,000 cases: 750 named places, 250 addresses) is
committed. **Do not regenerate it as part of a change you are measuring** — a
different test set makes the before/after incomparable. Rebuild it only as a
deliberate, separately-reviewed baseline refresh.

All queries are geo-biased to downtown Cairo (30.0444, 31.2357), never to the
case's own coordinates, so geo-decay cannot trivially surface the answer.

## Frozen numbers

### /geocode — named places (750), high effort

| metric | @1 | @5 | @10 |
|---|---|---|---|
| strict (same osm_id) | 86.8% | 96.9% | 98.4% |
| lenient (name or ≤150 m) | 95.5% | 98.9% | 99.3% |

### /geocode — addresses (250), high effort

| metric | @1 | @5 | @10 |
|---|---|---|---|
| exact (same osm_id) | 90.0% | 94.0% | 96.8% |
| correct street | 95.6% | 96.8% | 98.4% |

### /geocode — optimized effort

| metric | @1 | @5 | @10 |
|---|---|---|---|
| named — strict | 86.7% | 96.8% | 98.3% |
| named — lenient | 95.6% | 98.8% | 99.2% |
| address — exact | 92.8% | 97.6% | 99.2% |
| address — correct street | 99.6% | 100.0% | 100.0% |

### Interpolation

Genuinely-absent (same-parity) house number on a known street returned an
interpolated point: **138/196 = 70.4%** (both efforts). 54 cases excluded where
no absent number could be probed.

### /autocomplete — named (2,317 prefix probes)

| metric | @1 | @5 | @10 |
|---|---|---|---|
| strict (osm_id) | 33.3% | 58.0% | 68.2% |
| lenient (name or ≤150 m) | 35.5% | 59.5% | 70.0% |

By prefix length — recall **must increase monotonically** as the user types;
losing that monotonicity is a regression even if the overall average holds:

| prefix | probes | strict@1 | strict@5 | redis share | p50 ms |
|---|---|---|---|---|---|
| len3 | 749 | 12.8% | 33.4% | 97.1% | 8.3 |
| len5 | 743 | 31.1% | 60.2% | 78.1% | 7.5 |
| len8 | 665 | 54.9% | 79.7% | 33.4% | 12.4 |
| word | 160 | 49.4% | 72.5% | 58.1% | 7.4 |

Latency: **p50 8.5 ms**, **p90 26.3 ms**.

Backend split: redis 70.0% of probes (strict@1 17.0% within source),
elasticsearch 30.0% (strict@1 71.4%).

### Categories

Mean category hit rate across the 16-query probe set: **91.2%**.

Known failure in the baseline: `metro station` scores **0%** — it returns bus
stations and name-matches instead of railway stations, while the Arabic `مترو`
scores 100%. This is a *known open defect*, not a passing result; a change that
fixes it is an improvement, and one that leaves it alone is not thereby failing.

## Regression policy

A change to search, ranking, autocomplete, categories, or the ES mapping must
report these numbers before and after. Definition of Done:

- **@5 and @10 recall may not drop at all** (tolerance 0.5pp for run-to-run
  noise). These are the stable metrics.
- **strict@1 may drop at most 1.0pp, and only with a written explanation.**
  @1 is genuinely noisy across sources: a small strict@1 dip is sometimes a
  cross-source duplicate artifact rather than a quality loss (this is exactly
  what happened with ordinal 5th⇆fifth expansion, which improved real recall).
  An unexplained dip is a regression; an explained one can be accepted.
- **Mean category hit rate may not fall below 90%.**
- **Autocomplete latency**: p50 ≤ 12 ms, p90 ≤ 35 ms.
- **Prefix-length monotonicity must hold** (len3 < len5 < len8 on strict@1).

Interpret the redis/elasticsearch split as a diagnostic, not a target. A high
Redis share with low within-source strict@1 means Redis is hijacking queries it
cannot answer — but the measured A/B showed the Redis fast path beats ES on the
paths it owns, so do not "fix" the split by removing it. The per-source numbers
are confounded by which queries reach each backend.

## Corpus fingerprint — added 2026-09-05

The frozen numbers above were recorded without stating **which index produced
them**, which made the caveat below ("compare only runs against the same index")
impossible to honour. Any run may record a fingerprint here.

| Run | Docs | Deleted | Index ops | named strict@1 (high) | named strict@1 (optimized) |
| --- | --- | --- | --- | --- | --- |
| Frozen baseline (2026-07-13) | not recorded | — | — | 86.8% | 86.7% |
| Verification (2026-09-05) | 3,039,773 | 103 | 103 | 86.8% | 86.5% |

**High effort reproduced the baseline exactly**, on every metric. Optimized
effort moved −0.2pp at strict@1 and −0.3pp at lenient@1; @5 and @10 did not
move. Re-running optimized alone reproduced the same numbers, so this is not
run-to-run variance.

The cause is index state, not code:

* 103 documents were re-indexed while the stack was up (the watchers consume
  continuously), and 103 deleted docs were still counted in term statistics.
* A background force-merge was collapsing 22 segments into one. Lucene's IDF is
  computed from segment-level term statistics, so merging shifts text scores
  slightly and reorders results that were nearly tied.

That explains why only optimized effort moved. High effort orders by a per
document `function_score` over stored fields — `offline_rank`, geo decay,
popularity — which are unaffected by IDF. Optimized effort selects its rescore
window by raw text score first, which is exactly what shifted.

It also confirms the regenerated modules are not implicated: `offline_rank` is
computed at **index** time (`services/es_inserter.py`), so the values scored in
this run were written weeks ago by the previous implementation.

**Requirement for future runs:** record the fingerprint alongside the numbers. A
recall figure without its corpus is not comparable, and treating one as a
regression wastes a debugging session.

## Caveats — read before trusting a comparison

- **Cairo-only test set, global index.** Production indexes ~43.8M documents
  worldwide, not Egypt only. A Cairo-biased measurement can look flat while a
  global change (e.g. geo-decay tuning) shifts behaviour badly elsewhere. Probe a
  second city before shipping anything that touches geo scoring.
- **These numbers depend on the indexed corpus.** A re-import with different
  source data moves them independently of any code change. Compare only runs
  against the same index.
- **`README.md` disagrees with this file.** Its recall table quotes 88.7 / 96.3 /
  90.8 / 96.4 against the report's 86.8 / 95.5 / 90.0 / 95.6. The README numbers
  are hand-copied from an older run; the generated report is authoritative.
  Resolve by regenerating and updating the README, not by editing either table
  by hand.
