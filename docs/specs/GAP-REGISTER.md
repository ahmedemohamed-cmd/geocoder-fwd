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

### G-18 — Housenumber convention is locale-dependent, and the current rule is right here

**Revised after checking the data. The first version of this entry was wrong.**

- **As-built:** `parse_address_query` extracts a housenumber only when it
  precedes the street. Trailing numbers are kept as part of the street name:
  `شارع التحرير 15`, `rue de la Paix 15` and `Hauptstrasse 12` all return the
  whole string as the street.
- **Originally recorded as a defect.** It is not, in this corpus. Streets
  *named* with a trailing number are one of the most common patterns in the
  index:

  | Street | Addresses |
  | --- | --- |
  | `شارع 2` | 5,986 |
  | `شارع 1` | 5,867 |
  | `شارع 3` | 5,490 |
  | `شارع 9` | 4,276 |

  **222 distinct street names end in a number, covering 128,933 addresses —
  6.5% of the sampled corpus.** 128 of the 1,000 test-set entries have names
  ending in a number (`الحى 9` — District 9, `إبنى بيتك 5`). Parsing the
  trailing number as a housenumber would split `شارع 9` into street `شارع` plus
  housenumber 9 and destroy every one of them.
- **The real gap:** the rule is correct for Arabic and English and wrong for the
  languages where trailing *is* the convention — *Via Roma 12*, *Hauptstraße 12*,
  *Calle Mayor 12*. The index is global, so both populations are present.
- **Desired:** locale-aware parsing, not a regex change. A trailing number is a
  housenumber in de/it/es/nl/nordic addresses and part of the name in ar/en.
  Any implementation must be measured against a corpus containing both — which
  is G-12, and is why these two gaps conceal each other.
- **How this was caught:** by checking the data before changing the parser. A
  naive fix would have regressed 6.5% of addresses. The Cairo baseline *would*
  have caught it — 128 of 1,000 cases are exposed — which is the baseline
  earning its keep.

### G-19 — Fail-open error handling makes failures look like absent data

- **As-built:** 13 broad `except Exception` handlers return an empty or null
  default after logging. Several sit on the critical path:
  `shared/interpolation.py:616` (address gather — logs at **debug**),
  `services/enrichment.py:126` (PostGIS enrichment),
  `services/cache_service.py:70` (cache read),
  `services/traffic_aggregator.py:149` (Valhalla trace),
  `shared/traffic_providers.py:146` (provider speed — logs **nothing**).
- **Desired:** distinguish "no data" from "the lookup failed". At minimum, log
  infrastructure failures at warning or above and surface a health signal.
- **Why:** a PostGIS timeout, a connection failure, or a schema change all
  degrade silently to "interpolation unavailable" or "no traffic here". The
  feature stops working and nothing alerts.
- **Found by:** writing the interpolation contract. A missing method on a test
  fake produced eight `None` results that were indistinguishable from a street
  with no addresses — it took a trace to tell them apart. A user reporting
  "interpolation stopped working" would be equally hard to diagnose.

### G-20 — Two modules have no testable surface

- **As-built:** `services/traffic_aggregator.py` exposes only `run()`; its
  map-matching core (`_edge_speeds_from_match`, a pure function) and its Redis
  writer (`_update_edge`) are private, so the `tf:e:*` schema contract from
  AD-12 cannot be exercised without standing up NATS, Valhalla and Redis.
  `services/watcher.py` needs `osmium`, which is deliberately excluded from the
  test environment (see the CI note in `pyproject.toml`).
- **Desired:** for traffic, promote the pure map-matching function to the public
  surface so AD-12 can be asserted directly. For the PBF watcher, an
  integration-marked test against the real dependency.
- **Why:** these are the two largest uncovered modules — 427 and 1,039 lines. A
  regeneration of either has nothing checking it, and both goldens this session
  caught real defects in modules that *did* have coverage.

### G-21 — /geocode's single geo-decay fails outside Egypt [FIXED 2026-09-06]

- **As-built:** `/autocomplete` uses a two-tier decay (regional 300 km @ weight
  25, local 15 km @ weight 3). `/geocode` still uses a **single** 10 km gaussian
  at weight 2. The two-tier fix was applied to one endpoint and not the other.
- **Evidence.** With Riyadh and Dubai places in the index, biased to Riyadh:

  ```
  /geocode      q=school  →  5 results named "School", 1,612–1,789 km away
                             (all in Egypt, all offline_rank 1.029)
  /autocomplete q=school  →  Al Yasmin International School      9 km
                             مدرسة الشيخ عبدالعزيز بن باز الثانوية  2 km
  ```

  Across 9 category terms × 2 endpoints × 3 origins: Cairo-biased `/geocode`
  returns 0/10 far results, while Riyadh-biased returns 6–10/10 at a median of
  ~1,624 km and Dubai-biased 8–10/10 at ~2,420 km. `/autocomplete` returns 0/10
  far from every origin.
- **Why it happens:** an exact name match on POIs literally called "School"
  dominates, and a 10 km-scale term at weight 2 cannot pull local results up.
  This is exactly the failure the two-tier decay was introduced to fix — the
  reasoning is recorded in `spec/search.toml` under `[autocomplete]` — but
  `/geocode` never received it.
- **Desired:** port the two-tier decay to `/geocode`, then measure. The values
  are already in `spec/search.toml`; this is a scoring change, so it needs a
  before/after recall run on a corpus containing both regions.
- **Why it was invisible:** every document was in Egypt. From Cairo, `/geocode`
  scores perfectly. This is G-12 realised — the blind spot was hiding a
  production defect, not a hypothetical one.
- **Impact:** any user outside Egypt gets Egyptian results from `/geocode` for
  category queries. Directly relevant to Gulf-market expansion.
- **Fixed** in two parts, because the first was aimed at the wrong stage:
  1. A regional decay tier was added to `/geocode`'s `function_score`. This
     fixed `effort=high` and did nothing for `effort=optimized` — the default,
     and the one being kept.
  2. `optimized` retrieves on text alone and applies scoring in a rescore over
     the top 200. **A rescore reorders a window; it cannot introduce documents
     into it.** For `school` biased to Riyadh the first local result ranked
     #579 by text, so 0 of 200 window slots held a Riyadh document and no decay
     value could have helped. A `distance_feature` clause was added to the
     retrieval query so proximity influences what is retrieved.
- **Result:** Riyadh/Dubai leakage went from 6–10/10 foreign results to 0/10 on
  every term with local data. Cairo recall *improved*: optimized named strict@1
  86.3% → 87.7%, lenient@1 95.6% → 97.1%.
- **Residue is data, not scoring.** Terms still leaking (`mosque`, `market`)
  have almost nothing local to return: 12 mosques and 35 markets within 100 km
  of Riyadh, against 56,494 and 296,571 for Cairo. The Gulf corpus is GeoNames
  *places*; Egypt's is dense OSM *POIs*. This corpus validates direction, it
  does not simulate a production global index.
- **Open:** the `distance_feature` boost is 15; a sweep showed local coverage of
  the retrieval window saturates at **40**. Raising it is untested and deferred.

### G-22 — `addr_country` is not normalised

- **As-built:** the field mixes ISO codes, English names and Arabic names for
  the same country — `EG` (2,612,076), `Egypt` (76,930), `SA` (27,718),
  `Saudi Arabia` (9,446), `المملكة العربية السعودية` (9,006) — plus 310,043
  empty and a governorate name (`محافظة القاهرة`) leaked into the country field.
- **Desired:** normalise to ISO 3166-1 alpha-2 at index time.
- **Why:** the `/address` endpoint accepts a `country` filter. Filtering by `SA`
  silently misses the 9,446 documents that say `Saudi Arabia` and the 9,006 that
  say it in Arabic.

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
