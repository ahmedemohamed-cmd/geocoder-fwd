<!-- bmad:context -->
<!-- Verified 2026-09-05 against 1c112d5. Managed by bmad-project-context;
     edits inside this block are replaced on refresh. Keep anything you want
     preserved outside the markers. -->

## geocoder-fwd

An OSM geocoding platform and a metered billing/control plane sharing one repo.
Python 3.11 on FastAPI, over Elasticsearch, PostGIS, NATS, Redis and Valhalla;
the billing console is React under `billing/frontend/`. Planning artifacts live
in `docs/specs/`, stories in `docs/stories/`, the search-quality baseline in
`docs/quality-baseline.md`.

## Policy

- Never commit to `main` — branch and open a PR, including for small changes.
- Every serving-path component must be replica-safe: no in-process singletons,
  no leader election, no per-instance local state. Use the patterns already
  here — shared durable NATS consumers, the leaderless Redis `SET NX`
  scheduler, per-tile sharding (`TRAFFIC_WRITER_SHARDS`), `PROCESSED_LEDGER=pg`
  for multi-replica watchers.
- Changes to search, ranking, autocomplete, categories, or the ES mapping must
  be measured against `docs/quality-baseline.md` and must not regress it — the
  unit suite does not capture search quality.

## Where things are

- Search API: `services/geocoder.py` — 2.7k lines, only routing extracted so
  far. Don't grow it, and don't split it inside an unrelated change.
- Billing/control plane: `billing/` — a separate deployable with its own
  `requirements.txt` and venv (`.venv-billing`), not a geocoder subpackage.
- Prod deployment deltas: `_docker-compose.override.yaml`.

## Running and verifying

- Services run as `python -u -m services.<name>` — what compose executes.
  `run.py` is a dev-only launcher; editing it changes nothing in production.
- Integration tests need an explicit override:
  `pytest -m integration --override-ini addopts=`. Plain `pytest` deselects them.
- The billing suite needs a `billing_test` database on the compose `postgis`
  service; without it the whole suite errors with `ConnectionRefusedError`.
- CI installs only `.[dev,test]`, which excludes torch and osmium — a
  default-suite test importing them passes locally and fails in CI.

## Conventions that differ from defaults

- New tunables go in `shared/config.py` via `_safe_int`/`_safe_bool`/
  `_safe_float`, never `os.getenv` at the point of use — duplicate reads have
  already drifted here.
- Tests needing infra get `@pytest.mark.integration`; everything else must run
  with no network and no containers.

## Known pitfalls

- `places.nourbyte.com` is a different machine; local rebuilds never reach it.
- Prod deploys need both files:
  `-f docker-compose.yaml -f _docker-compose.override.yaml`. Bare compose
  serves a blank frontend.
- Frontend URLs must be origin-relative — the SPA is served from `/console`
  behind Nginx Proxy Manager.
- After a Docker or WSL restart the datastores do not return with the app
  containers: services crash-loop and login fails with "Failed to fetch".
  Start `postgis`, `redis`, `elasticsearch`, `nats`, `etcd`, `zitadel-db` first.
- A 502 on `places.nourbyte.com` after recreating the stack is usually NPM
  holding stale container IPs, not a broken app.

<!-- /bmad:context -->
