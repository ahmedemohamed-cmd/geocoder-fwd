---
name: 'coverage-audit'
type: audit
scope: 'Every artifact in the repo, classified by whether a regeneration could reproduce it.'
status: active
created: '2026-09-05'
---

# Regeneration Coverage Audit

The question this answers: **drop the code — what could not be rebuilt?**

Earlier passes extracted tuning values out of Python and treated that as the
whole job. It was not. Infrastructure, schema, and the API contract were never
specified, and a regeneration needs those more than it needs ranking weights.

Three categories, and the middle one is the interesting one.

## 1. Specified as data the implementation reads

`spec/` is a runtime input, shipped in both images. Losing the code loses
nothing here.

| Area | Spec | Contract test |
| --- | --- | --- |
| Ranking weights and tables | `ranking.toml` | `test_ranking_spec.py` |
| Category taxonomy | `categories.json` | `test_spec_contract.py` |
| Address vocabularies | `address.json` | `test_spec_contract.py` |
| Place-record mapping | `places-mapping.json` | `test_spec_contract.py` |
| ES mapping and analyzers | `es-mapping.json` | `test_spec_contract.py` |
| Autocomplete scoring | `autocomplete.toml` | `test_spec_contract.py` |
| Interpolation vocabulary | `interpolation.json` | `test_spec_contract.py` |
| ES query tuning | `search.toml` | `test_search_spec.py` |
| Arabic narration, traffic bands | `routing.toml` | `test_services_spec.py` |
| GeoNames taxonomy | `geonames.json` | `test_services_spec.py` |
| Confidence ladder, street tokens | `matching.toml` | `test_services_spec.py` |
| Plan pricing, credit weights | `billing.toml` | `test_billing_spec.py` |
| **NATS stream topology** | `streams.toml` | `test_contract_surface.py` |
| **Database schema** | `schema/*.sql` | `test_contract_surface.py` |
| **API contract** | `api/*.openapi.json` | `test_contract_surface.py` |

## 2. Already declarative — specification in place

These are not Python with values buried inside; they *are* the specification,
in the format their runtime consumes. Extracting them would produce a copy that
drifts. What they needed was to be recognized as spec and covered by a test.

| Artifact | What it specifies |
| --- | --- |
| `docker-compose.yaml` (709 lines) | Service topology: 25 services, images, env, ports, healthchecks, resource limits, volumes |
| `docker-compose.ai.yaml` | The GPU/vector delta, layered over the base |
| `_docker-compose.override.yaml` | Production deltas: public host, TLS termination, path routing |
| `Dockerfile`, `billing/Dockerfile`, `billing/frontend/Dockerfile` | Build and packaging |
| `nats.conf` | JetStream server limits (the 64 MB ceiling `streams.toml` references) |
| `billing/apisix/config.yaml` | Data-plane bootstrap |
| `billing/frontend/nginx.conf` | SPA serving and `/console` base path |
| `.github/workflows/ci.yml` | The verification pipeline |
| `scripts/fresh-start.sh` | Teardown and rebuild procedure |
| `pyproject.toml` | Toolchain, lint rules, test configuration |

**Their weakness is version pinning, not format.** Four images run `:latest`
(G-2, G-4), so the same compose file yields different systems over time. That is
the reproducibility gap, and it is recorded in the gap register.

## 3. Not specified — a regeneration would invent these

Honest list. Nothing below is covered by `spec/` or a contract test.

| Area | Size | Why it matters |
| --- | --- | --- |
| **Query construction logic** | `geocode` 491 lines, `address_search` 337 | The *values* are in `search.toml`; the clause structure that assembles them is not. A regeneration would produce a different query shape with the same constants. |
| **Frontend** | 8 JSX pages, `api.js`, `claims.js` | No spec of routes, states, or the admin/tenant surface. The API contract pins what it calls, not what it shows. |
| **Ingest parsing** | `watcher.py` 992 lines, `oa/gn/places_watcher` | Feature taxonomies are specified; PBF/CSV/TSV traversal and geometry assembly are not. |
| **Traffic map-matching** | `traffic_aggregator.py` 429 lines | Probe→edge snapping and the mmap write path into `traffic.tar`. |
| **Interpolation geometry** | `interpolation.py` 659 lines | Vocabulary is specified; the odd/even side-aware positioning is not. |
| **Zitadel provisioning** | `provision_zitadel.py` 293 lines | Org, project, roles, MFA policy — described in prose, not as data. |
| **Operational knobs** | ~12 constants | Batch sizes, retries, queue bounds still in service modules, violating AD-7. |

## What this means

A regeneration today would reproduce **what the system knows** — every tuned
value, taxonomy, price, schema, stream and endpoint — and would have to
re-derive **how it works**: query assembly, parsing, geometry, and the UI.

That is a defensible split. Algorithms are the part a competent implementation
can rewrite from a clear contract; tuned constants and interface shapes are the
part it cannot guess. The contract tests decide whether the rewrite was correct.

The binding constraint is unchanged and is not a specification gap:
`docs/quality-baseline.md` measures one city against a global index (G-12).
Every extraction so far was verified by snapshot equality, which proves
faithfulness, not quality. A regeneration that must be judged *good* rather than
*identical* needs that baseline broadened first.
