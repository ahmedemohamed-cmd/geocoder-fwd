---
name: 'billing-control-plane-solution-design'
type: solution-design
scope: 'The billing domain: billing/, APISIX + etcd, Zitadel, and the React console.'
companion: 'ARCHITECTURE-SPINE.md'
status: final
created: '2026-09-05'
---

# Billing / Control Plane — Solution Design

Companion to `ARCHITECTURE-SPINE.md`, which carries the plane-separation and
request-lifecycle diagrams. This document explains how the subsystem works
today, including what is awkward. Derived from `billing/` at `673045d`.

## What it does

It turns the geocoder into a product you can sell: it issues per-tenant API
keys, meters every request against a plan, enforces quotas at the edge, and
produces monthly invoices. It also runs the console tenants log into.

The whole design follows from one decision: **the thing that enforces is not the
thing that administers.**

## How a request gets metered

APISIX authenticates the `X-API-Key` against a per-key consumer, applies the
tenant's quota through a consumer-group, proxies to the geocoder, and ships a
usage event to the control plane's sink. Nothing in that path queries the
control plane, so administration and traffic fail independently.

The sink is where billing judgment happens. It discards anything that was not
actually served — no consumer, or a status of 400 or above — then resolves the
endpoint's credit weight, and pushes the event onto a Redis list while
incrementing live counters. The `aggregator` service drains that list into
Postgres rollups on a two-second loop.

That two-stage shape is deliberate. Redis absorbs per-request write pressure;
Postgres holds anything anyone will be billed for or shown.

## Why Postgres is the truth

The tenant usage display originally read Redis live counters. The shared Redis
instance is non-persistent and LRU-evicting, so a tenant's reported consumption
could drop to zero after an eviction or a restart. The display now reads the
Postgres rollups, and Redis is a counting tier only.

This is worth stating plainly because it is the same lesson the geocoder domain
encodes in its own spine: Redis holds nothing that cannot be rebuilt. Here the
cost of forgetting it was visible to customers.

## Quotas and the period boundary

A plan carries a monthly quota, an overage price, and a hard-cap flag. The hard
quota is enforced inline by APISIX's `limit-count` backed by Redis, which makes
it cluster-wide: ten gateway replicas share one counter rather than granting ten
allowances.

The subtle part is the period. The Redis counter key is scoped to the calendar
month — `<group>:<YYYY-MM>` — so the quota resets on the 1st in step with the
Postgres rollups. That key is **computed by the control plane in UTC and
projected into APISIX as a literal**; the data plane never derives it. The
benefit is that no container clock can split a billing month. The cost is an
operational coupling: the aggregator must re-project each tenant's consumer
group when the month rolls over, or tenants keep spending against the previous
month's bucket.

Rate limiting sits in front of the quota. A request refused for exceeding its
RPS cap never reaches the quota increment, so it is never charged.

## Keys and identity

An API key is stored twice: a sha256 hash for lookup, and a Fernet-encrypted
`key_enc` so the owning tenant can reveal and copy the key later. Plaintext is
never persisted, and the hash alone cannot reconstruct a key. Disabling a key
re-projects it to APISIX, so the change takes effect within milliseconds rather
than waiting for a cache to expire.

Identity runs through one `Identity(role, tenant_id)` shape with two
interchangeable verifiers behind it, selected by `BILLING_AUTH_MODE`. In `dev`
mode a local HS256 issuer signs tokens over a `users` table with PBKDF2
password hashing. In `zitadel` mode the control plane verifies Zitadel OIDC
tokens against its JWKS. Because both produce the same `Identity`, no handler
branches on the mode.

Zitadel is configured for TOTP-only MFA with self-registration disabled. Admins
provision tenant users through the control plane, which mirrors those operations
into Zitadel.

## Pricing

Cost is per endpoint, not per request. An autocomplete keystroke is a cheap
filtered query; a Valhalla matrix call or a deep vector search costs an order of
magnitude more. Weights live in `endpoint_weights` as integer **milli-credits**
(1 credit = 1000), which keeps every counter integral from the increment through
to the invoice — floating-point drift across millions of increments would
produce invoices that do not reconcile.

Weights are keyed on the first path segment, so `deep/forward` and `deep/nearby`
share one weight. Free endpoints are matched differently, on the full normalized
path, which is how `/traffic/probes` stays free while `/traffic/edge` bills.
Those two rules look inconsistent and are not: they do different jobs.

Invoicing is idempotent per tenant and period. Re-running a closed month updates
the existing invoice rather than issuing a second one.

## Known weaknesses

As-built facts. Fixes belong in the gap register.

1. **`gateway.py` is a complete data plane that nothing deploys.** Its docstring
   still calls it "the metered data plane — the distributed enforcement tier",
   but compose runs APISIX and no service uses `gateway_app`. The README is
   right and the docstring is wrong. Keeping it as an APISIX-independent
   reference is defensible; leaving it self-describing as production is not.
2. **`BILLING_AUTH_MODE` defaults differ by context** — `dev` in code, `zitadel`
   in compose — and dev mode ships a hardcoded default JWT secret. Running the
   control plane outside compose silently selects the weaker verifier.
3. **Zitadel is unpinned at `:latest`.** It is the identity provider; an
   unattended image change there has the highest blast radius in the stack.
4. **Quota accuracy depends on Redis.** The spine accepts that a flush costs
   within-period accuracy; no recovery path re-derives the current period's
   counter from the rollups.
5. **All tenants share one Postgres schema.** Isolation is by `tenant_id`
   predicate, enforced in `repo` rather than by the database.
6. **The Redis client is duplicated from `shared/`.** Deliberate, documented, and
   isolated by `REDIS_PREFIX` — but it is a second copy that can drift.

## Where to start reading

| Question | File |
| --- | --- |
| What does the management API expose? | `billing/control_plane.py` |
| How does a request become a billable event? | `billing/usage.py`, then `billing/aggregator.py` |
| How is a quota actually enforced? | `billing/apisix_admin.py` (`ensure_consumer_group`) |
| How is an invoice computed? | `billing/billing_engine.py` |
| How are keys stored and verified? | `billing/security.py` |
| What is tunable? | `billing/config.py` |
