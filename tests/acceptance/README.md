# Acceptance suite — the regeneration contract

**This folder survives a regeneration. `tests/unit/` does not.**

Everything here must pass against a *different implementation of the same
specification*. That is only true because of one rule, enforced mechanically by
`test_acceptance_boundary.py`:

> An acceptance test may never touch a private name on a production module, and
> may only import modules declared contractual.

## Why the rule matters

A test calling `routing._translate_maneuver` fails with `AttributeError` against
any implementation that organised itself differently — the regeneration gets
judged non-compliant for cosmetic reasons.

The subtler failure is worse. If a regeneration is *told* to satisfy
internal-shaped tests, it must reproduce our private function names, and those
tests smuggle in knowledge the specs lack. The regeneration then appears to
succeed while the specification gaps stay hidden — which defeats the point of
regenerating at all.

## What counts as contractual

A module is contractual if the specification or an architecture spine names it
as a capability. `shared/interpolation.py` is (a headline feature),
`services/gn_watcher.py` is (a pipeline stage bound by AD-10),
`shared/google_maps.py` is (it backs `/deep`).

Utilities are not: `geocoder_helpers`, `progress`, `traffic_tile`. Their tests
live in `tests/unit/` and are expected to be rewritten with the code.

The list is explicit in `test_acceptance_boundary.py`. Adding to it widens what
a regeneration must reproduce, so add deliberately.

## What is here

| Kind | Covers |
| --- | --- |
| HTTP behaviour | Both apps driven over ASGI: geocode, address, routing, admin and tenant surfaces |
| API contract | `spec/api/*.openapi.json` — 53 paths, 36 schemas |
| Spec contracts | Every `spec/` file, with the invariants each must hold |
| Schema | `spec/schema/*.sql` bootstraps a real Postgres |
| Streams | Retention, caps, and WORK_QUEUE semantics |

## What is NOT here, and should be

The quality gate. `tests/run_recall.py` and `tests/run_autocomplete_recall.py`
need a live indexed Elasticsearch, so they are not part of this suite — but a
regeneration is not acceptable until they pass against
`docs/quality-baseline.md`. Snapshot equality proves faithfulness, not quality.

Pipeline-stage tests still call watcher parse functions directly. The stronger
contract is the NATS element message those stages publish (AD-1). Testing at
that boundary instead is open work.
