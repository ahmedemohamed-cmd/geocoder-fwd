---
name: 'regeneration-experiment'
type: report
scope: 'Two modules dropped and rebuilt from spec/ alone. What worked, what did not.'
status: complete
created: '2026-09-05'
---

# Regeneration Experiment

Two modules were deleted and rewritten from `spec/` plus the acceptance suite,
on branch `experiment/regen-from-spec`. The working implementation was preserved
on `chore/sdd-bmad-foundation` throughout.

The point was not to prove the specs work. It was to find out where they don't.

## Result 1 — `shared/ranking.py`: passed first time

278 lines dropped, 195 written from `spec/ranking.toml`. All **570 golden cases
matched on the first run**, and the full suite stayed green.

Four things the spec did not state had to be inferred:

1. which signals besides admin/area are conditional in the denominator
2. how the waterway table combines with the natural signal
3. that population input is cleaned of thousands separators
4. the output rounding

All four guesses were right, and the golden exercises each. **That is not proof
the spec was complete** — it is a well-documented spec plus luck. `ranking.toml`
carries a prose header explaining the scoring model, and that header is why the
rebuild worked.

## Result 2 — `shared/categories.py`: 66.7% divergence

219 lines dropped. The first rebuild passed all 10 hand-written acceptance tests
and then failed **174 of 261 golden cases**.

One unstated rule caused nearly all of it: *an element carrying an `admin_level`
is a boundary regardless of its other tags.* A governorate tagged
`amenity=restaurant` is a boundary, not a restaurant. That rule was in neither
the spec nor the tests.

Five more surfaced as the divergence was driven down:

| Rule | Cases |
| --- | --- |
| Boundary wins over tags | 174 → 5 |
| Boundary key/value populated from the tag, not from `admin_level` | 5 → 1 |
| Sub-features are excluded from `category_text` only, not `classify` | included above |
| Unknown value with no group mapping gets group `null`, not the key name | 1 → 1 |
| `admin_level` of 0 is not a boundary | 1 → 1 |
| `place=*` is its own group and never a POI | 1 → 0 |

All six are now data in `spec/categories.json` under `CLASSIFY_RULES`, and the
regenerated module passes.

## The structural cause

`ranking.toml` succeeded and `categories.json` failed for one reason: **TOML
carries comments and JSON does not.**

When tables were extracted to TOML, the reasoning moved with them. When they
were extracted to JSON, the data moved and the reasoning stayed behind as Python
comments — including a comment recording that near Toronto the index holds 245
subway entrances against 68 stations, which is *why* sub-features are excluded.
A regeneration never sees that.

Six spec files are JSON: `categories`, `address`, `places-mapping`, `geonames`,
`interpolation`, `es-mapping`. Each carries data without semantics.

## Other defects found

- **A contract test was implementation-shaped.** `test_tables_are_not_inline_again`
  asserted the *assignment expression* contained the literal `_SPEC`, so a
  correct regeneration reading the spec through a local alias failed. It
  constrained how a module reads the spec, not that it does. Now an equivalence
  check. The acceptance boundary guard could not catch this — it was an AST-shape
  assertion, not a private-name binding.
- **The extraction passes missed constructor-wrapped constants.** The AST scan
  inspected bare `Dict`/`Set`/`List`/`Tuple` values, so `frozenset({...})` was
  invisible. Three street-keyword vocabularies in `shared/address.py` (47 values)
  and `_JUNK_VALUES` in `categories.py` are still in code. `JUNK_VALUES` has been
  moved to the spec; the address vocabularies have not.
- **Public exports were undocumented.** `CATEGORY_QUERY_TERMS`,
  `GROUP_BY_KEY_VALUE`, `GROUP_PLACE`, `GROUP_BOUNDARY` are imported across
  modules but appear in no spec. The regeneration failed to import until they
  were rediscovered from the original.
- **The acceptance suite has a coverage hole.** It never combined `admin_level`
  with a POI tag, which is why the single largest classification rule went
  unnoticed until the golden ran.

## Verdict

**Ready for module-at-a-time regeneration with a golden to check against. Not
ready for a whole-repo drop.**

Both experiments were caught by the golden snapshots, not by the hand-written
tests — the 10 categories tests passed while two-thirds of behaviour was wrong.
Any module without a golden would have regenerated silently incorrect.

Before attempting more:

1. ~~Convert the six JSON specs to a commented format.~~ **Done.** Five became
   documented TOML with their reasoning restored; `es-mapping.json` stays JSON
   because it is an Elasticsearch API payload. Each conversion was verified by
   round-trip equality before the loaders were switched.
2. ~~Extract the constructor-wrapped tables the AST scan missed.~~ **Done.**
   The three street-keyword vocabularies (47 values) are in
   `spec/address.toml`; a rescan reports zero remaining.
3. ~~Specify each contractual module's public exports.~~ **Done.**
   `spec/module-api.toml` declares 191 names across 30 modules, with a test
   that the spec cannot fall behind the code.
4. **Run the recall harness.** Still the open item, and now unblocked: the
   Elasticsearch volume survived and `osm_places` recovered green with
   3,039,773 documents. Every result in this report is snapshot equality; none
   of it measures search quality.

## What the fixes do not settle

Converting the specs restored the *reasoning*, which is what a human or an LLM
needs to make the same judgement call. It does not make the specs provably
complete. The categories rules were found by running a golden and chasing the
divergence to zero — a module without a golden would still regenerate silently
wrong, and most of `services/` has no golden.
