---
name: 'regeneration-contract'
type: contract
scope: 'What must be true for a regenerated implementation to be accepted as equivalent.'
status: active
created: '2026-09-05'
---

# Regeneration Contract

The goal: drop the implementation, regenerate it from specification, and accept
the result as equivalent.

## What "equivalent" means

**Not identical code.** Two implementations of the same specification will differ
in structure, naming, and decomposition, and demanding otherwise makes the goal
unreachable and the comparison useless.

**Equivalent means: passes every contract below, within stated tolerance.**

That definition is the whole point. It is checkable by machine, it does not
degrade as the code is rewritten, and it fails loudly when behavior moves.

## The four tiers

A regeneration must satisfy all four. They are listed in the order a
regeneration would consume them.

### 1. Structural — what may not diverge

`architecture/*/ARCHITECTURE-SPINE.md`, 25 architecture decisions across two
domains. These fix boundaries, store ownership, write paths, and plane
separation: the calls two independently-built units could get incompatibly
wrong. A regeneration that violates an AD is rejected regardless of test results.

**Status: complete.** Both domains have a lint-clean spine that survived an
adversarial review pass.

### 2. Tuning — what cannot be re-derived

Values that exist only because someone measured, tuned, and kept them. No
architecture implies them, and a regeneration cannot invent them correctly.
These live in `spec/` as declarative data that the implementation *reads*.

**Status: substantially complete for `shared/`. ~1,272 values externalized
across seven spec files; `services/geocoder.py` remains.**

| Area | Literals before → after | Spec file |
| --- | --- | --- |
| `shared/ranking.py` | 315 → 107 | `spec/ranking.toml` |
| `shared/categories.py` | taxonomy → 10 | `spec/categories.json` |
| `shared/places_mapping.py` | 37 → 19 | `spec/places-mapping.json` |
| `shared/address.py` | 68 → 68* | `spec/address.json` |
| `shared/autocomplete.py` | 136 → 119 | `spec/autocomplete.toml` |
| `shared/es_mapping.py` | mapping → 2 | `spec/es-mapping.json` |
| `shared/interpolation.py` | 165 → 157 | `spec/interpolation.json` |
| `services/geocoder.py` | 401 → 348 | `spec/search.toml` (53 query-tuning values) |

\* `address.py`'s count is unchanged because its literals are regex offsets and
slice bounds, not tuning; the eight *vocabularies* (abbreviations, street types,
city keywords, ordinals) are what moved.

Residual literals in the extracted modules are structural — clamps, scale
factors, regex indices — not tuning knowledge.

`services/geocoder.py` needed a different technique. Its tuning is not in
liftable tables but inline in Elasticsearch query construction: field boosts,
phrase slops, geo-decay scales, rescore sizing, vector candidate counts. Those
53 values are now in `spec/search.toml`, replaced in place by exact AST
position rather than by moving code, so the extraction did not require the
`geocode` handler to be split first. Its remaining 348 literals are pagination
bounds, HTTP limits, string offsets, and Painless-script constants.

Two values in `spec/es-mapping.json` are deliberately **not** frozen:
`number_of_replicas` and the vector `dims` are `${ES_INDEX_REPLICAS}` and
`${EMBEDDING_DIM}` placeholders resolved at load. They follow cluster topology
and the embedding model, not tuning. Externalising the mapping without them
would have silently decoupled the index from its configuration — a regression
the behavioural snapshot could not catch, because the defaults matched.

### 3. Behavioral — what it must do

The test suite. 201 tests, of which `tests/test_ranking_spec.py` is the model to
follow: it enumerates the full key space of a specification and pins every
result, so a single moved value fails loudly and specifically.

**Status: good for spec-backed behaviour.** `tests/test_spec_contract.py` and
`tests/test_ranking_spec.py` pin every spec-driven surface: the full key space
of every scoring table, category classification across all groups, address
parsing and ordinal expansion, place-record mapping, autocomplete scoring and
geohashing, and the Elasticsearch mapping itself. A single changed value in any
spec file fails them. Behaviour *not* driven by `spec/` — chiefly query
construction in `services/geocoder.py` — remains covered only by conventional
unit tests.

### 4. Quality — how well it must do it

`docs/quality-baseline.md`. Recall, autocomplete, and category numbers against a
1,000-case set, with an asymmetric regression policy: @5 and @10 may not drop,
strict@1 may move a point with a written explanation.

**Status: in place, with a known blind spot.** The set is Cairo-only while the
production index is global (~43.8M docs). A regeneration could pass it and still
regress elsewhere. Broadening it is a prerequisite for trusting a full
regeneration, not an optimization.

## The procedure

1. Regenerate the implementation from tiers 1 and 2.
2. Reject immediately on any AD violation.
3. Run the test suite. Every contract test must pass unchanged — a regeneration
   may not edit its own contract.
4. Build an index and run both recall harnesses against the frozen baseline.
5. Accept if tier 4 tolerances hold. Investigate any strict@1 movement rather
   than accepting it.

## Honest status

**A regeneration of `shared/` is now plausible; one of `services/geocoder.py` is
not.** The structural tier is complete. The tuning tier holds ~1,272 values
covering every domain module, with query construction the one remaining gap. The
behavioural tier pins every spec-backed surface. The quality tier still has the
Cairo-only blind spot, and that is now the weakest link: it is the instrument
every extraction was verified against.

The method that got here is the part worth keeping. For each module: capture a
behavioural snapshot first, extract the tables, re-run, and require the output to
be byte-identical. Six modules were extracted this way with zero behavioural
drift, and the one regression that did occur — freezing two env-driven values
into the ES mapping — was caught by reading the diff, not by the snapshot, which
is worth remembering: a snapshot proves what you exercised, not what you
changed.

Next, in order: broaden the quality baseline beyond Cairo (it gates everything
else), then restructure `services/geocoder.py` so its query constants become
spec rather than literals.
