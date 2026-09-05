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

**Status: partial. 208 values externalized, ~932 numeric literals remain in
code.**

| Area | Literals | Externalized |
| --- | --- | --- |
| `shared/ranking.py` | 315 → 107 | ✅ `spec/ranking.toml` (208 values) |
| `services/geocoder.py` | 401 | ❌ largest remaining block |
| `shared/interpolation.py` | 165 | ❌ |
| `shared/autocomplete.py` | 136 | ❌ |
| `shared/address.py` | 68 | ❌ |
| `shared/places_mapping.py` | 37 | ❌ |
| `shared/categories.py` | 10 + taxonomy | ❌ taxonomy is the real payload |
| `shared/es_mapping.py` | 8 + analyzers | ❌ mapping is already declarative |

The residual 107 in `ranking.py` are structural (`0.0`/`1.0` clamps, the 0..10
scale) and comment examples, not tuning.

### 3. Behavioral — what it must do

The test suite. 201 tests, of which `tests/test_ranking_spec.py` is the model to
follow: it enumerates the full key space of a specification and pins every
result, so a single moved value fails loudly and specifically.

**Status: partial.** Ranking has a complete contract test. Most other behavior
is covered by unit tests written against the current implementation rather than
against a specification, which is weaker — they can encode accidents.

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

**Today a regeneration would fail**, and the contract is what tells you so. The
structural tier is complete; the tuning tier covers roughly a fifth of what it
needs to; the behavioral tier is strong in one place and conventional elsewhere;
the quality tier has a geographic blind spot.

Nothing here is blocked on writing more prose. The remaining work is mechanical
extraction — the `ranking.toml` pattern applied to seven more areas — plus
broadening the test set. Each extraction is independently verifiable by the same
method used for ranking: pin the behavior first, extract, prove the output did
not move.
