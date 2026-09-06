---
name: 'gap-register'
type: gap-register
scope: 'Delta between the as-built system and where it should go. Both domains.'
status: open
created: '2026-09-05'
sources:
  - 'architecture/architecture-geocoder-fwd-2026-09-05/'
  - 'architecture/architecture-billing-2026-09-05/'
---

# Gap Register

The architecture spines describe what **is**. This file holds what **should
change** — the half deliberately kept out of them, so the as-built docs stay
trustworthy and the backlog stays honest.

Every item states the as-built fact, the desired state, and why it matters.
Items are evidence-backed unless marked *unverified*.

## Security

### G-1 — `BILLING_AUTH_MODE` defaults to the weaker verifier

- **As-built:** code defaults to `dev` (local HS256 over a `users` table); compose
  sets `zitadel`. Dev mode ships a hardcoded default JWT secret.
- **Desired:** running the control plane outside compose must not silently
  select the weaker verifier, and no usable secret should ship as a default.
- **Why:** the failure is silent. Nothing logs that a weaker identity path was
  chosen, so a misconfigured deployment looks healthy.

### G-2 — Zitadel runs unpinned

- **As-built:** `ghcr.io/zitadel/zitadel:latest`.
- **Desired:** a pinned version, upgraded deliberately.
- **Why:** it is the identity provider. An unattended image change there has the
  largest blast radius in the stack.

### G-3 — Tenant isolation is by predicate, not by database

- **As-built:** every tenant shares one Postgres schema; isolation is a
  `tenant_id` predicate enforced in `repo.py`.
- **Desired:** decide whether predicate isolation is sufficient at target scale,
  or whether row-level security / schema-per-tenant is warranted.
- **Why:** one missing predicate in one query leaks across tenants, and nothing
  in the database would stop it.

## Correctness and reproducibility

### G-4 — Three more images are unpinned

- **As-built:** `valhalla:latest`, `ollama/ollama:latest`, `curlimages/curl:latest`,
  while everything else is version-pinned.
- **Desired:** pinned, with a deliberate upgrade path.
- **Why:** reproducibility is only as good as the weakest pin; a rebuild can
  silently change routing behavior.

### G-5 — Elasticsearch 8.11.0 upgrade assessment *(unverified)*

- **As-built:** pinned at 8.11.0.
- **Desired:** a decision, recorded, on whether to move.
- **Why:** likely well behind current 8.x. Flagged as unverified — versions were
  confirmed against the repo, not against upstream release notes.

### G-6 — Two stale docstrings contradict the code

- **As-built:** `traffic_writer.py` says `tf:e:*` is "written by
  traffic_aggregator, read here", but `routing.py` writes it too.
  `gateway.py` calls itself "the metered data plane", but nothing deploys it.
- **Desired:** both corrected.
- **Why:** a wrong docstring at the point of use is worse than no docstring; it
  is read at exactly the moment someone is deciding how to change the code.

### G-7 — README recall table disagrees with the frozen baseline

- **As-built:** README quotes 88.7 / 96.3 / 90.8 / 96.4; the generated report
  says 86.8 / 95.5 / 90.0 / 95.6.
- **Desired:** regenerate and update the README from the report.
- **Why:** two published quality claims, one wrong, and no way for a reader to
  tell which.

## Architecture

### G-8 — `services/geocoder.py` is 2,707 lines

- **As-built:** the whole serving path except `routing` and `nearby`, which are
  already extracted as mounted `APIRouter`s.
- **Desired:** continue the extraction along the proven seam.
- **Why:** the pattern is established and low-risk, and the quality baseline
  gives the refactor a pass/fail oracle. This is the strongest candidate for the
  first real spec-driven story.

### G-9 — `AD-7` has no mechanical enforcement

- **As-built:** "config is read only in `shared/config.py`" is a social rule.
  Seven files violated it until `a628ce7`.
- **Desired:** a lint rule that fails an `os.getenv` in a service module.
- **Why:** every unenforced convention regresses. The rules that stuck this week
  (`G004`, `T20`) stuck because a check fails them.

### G-10 — Quota accuracy has no recovery path after Redis loss

- **As-built:** the spine accepts that a Redis flush costs within-period quota
  accuracy; the rollups survive but the live counter does not.
- **Desired:** decide whether the current period's counter should be re-derivable
  from the rollups.
- **Why:** after a flush, a tenant at 95% of quota resets to zero and can
  overspend the plan for the rest of the month.

### G-11 — Redis client duplicated between `shared/` and `billing/`

- **As-built:** deliberate, documented, isolated by `REDIS_PREFIX`.
- **Desired:** confirm the duplication is still worth its cost, or extract a
  package both depend on.
- **Why:** independent deployability is a real benefit; a silently drifting
  second copy is a real cost. The trade is worth revisiting, not assuming.

## Measurement

### G-12 — The quality baseline is Cairo-only against a global index

- **As-built:** a 1,000-case Cairo test set stands in for ~43.8M documents
  worldwide.
- **Desired:** a second-city probe before any geo-scoring change ships.
- **Why:** a change can look flat in Cairo and regress everywhere else. The
  baseline currently cannot see that.

### G-13 — Performance and security NFRs are not measurable

- **As-built:** stated as an intent ("best practices of performance and
  security"). The scalability half is now concrete in both spines; these two are
  not.
- **Desired:** express each as something with a number and a check — latency
  budgets per endpoint, a dependency-audit gate, a secret-scanning step.
- **Why:** an unmeasurable requirement cannot fail, so it cannot hold.

### G-18 — Housenumber parsing only supports the leading-number convention

- **As-built:** `parse_address_query` extracts a housenumber only when it
  precedes the street. This works in every language tested, including
  Arabic-Indic digits (`١٥ شارع التحرير`). A trailing housenumber never parses:
  `شارع التحرير 15`, `rue de la Paix 15` and `Hauptstrasse 12` all return the
  whole string as the street with no housenumber.
- **Desired:** parse both conventions, or state deliberately that trailing-number
  addresses are out of scope.
- **Why:** trailing is the standard form across much of Europe — *Via Roma 12*,
  *Hauptstraße 12*, *Calle Mayor 12*. The index is global (~43.8M docs), so those
  queries silently lose both exact matching and interpolation. It compounds
  G-12: a Cairo-only baseline cannot see this at all, because Egyptian addresses
  use the leading form.
- **Found by:** writing an intent assertion beside a golden. The snapshot had
  pinned the behaviour happily for weeks.

## Regeneration readiness

Resolved 2026-09-05: the intent behind G-14 is that **code is machine-authored
from here on** — humans edit specs, not source. That reframes the register. The
items below are what stand between the current artifacts and a repo where
regeneration is a safe operation.

### G-14 — Code is machine-authored [POLICY]

- **Stated:** no human edits code moving forward.
- **Implication:** a requested change updates the spec or story and regenerates;
  it is never hand-patched into source. The spec, not the diff, is the artifact
  under review.
- **Note:** this is a governance rule, not the source reorganization the earlier
  reading assumed. No `src/` move is implied.

### G-15 — The specs are descriptive, not generative

- **As-built:** 8,169 words of specification describe 18,729 lines of code. The
  spines fix invariants — deliberately, since a spine is a consistency contract
  whose structural detail is owned by the code.
- **Desired:** enough specification that a regeneration passes the quality
  baseline.
- **Why:** regenerating today would satisfy every AD and still behave
  differently. Equivalence must be defined as "passes the baseline within
  tolerance", never as "identical code".

### G-16 — ~1,140 tuned constants live only in code

- **As-built:** `shared/ranking.py` alone holds 315 numeric literals
  (`city: 0.90`, `town: 0.75`, …); `services/geocoder.py` holds 401. None appear
  in any spec, and none are derivable from one.
- **Desired:** ranking weights, the category taxonomy, and the ES analyzer
  config externalized into declarative data files versioned as specification.
- **Why:** the single highest-leverage step toward regeneration. It converts the
  largest block of unspecifiable behavior into spec that a regeneration reads
  instead of reinventing.

### G-17 — No behavioral specs at capability altitude

- **As-built:** the spines cover invariants; nothing states what `/geocode` does
  with an unmatched housenumber, how cross-language street resolution behaves, or
  what interpolation guarantees.
- **Desired:** a behavioral spec per capability, with the recall harness as its
  executable check.
- **Why:** prose cannot verify a regeneration. The 198 tests and the 1,000-case
  benchmark already can — they are the real contract and should be treated as
  part of the specification.
