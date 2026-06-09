"""API-key management, metering, billing and reporting subsystem.

Layout
------
config.py          - environment-driven configuration
db.py              - asyncpg pool, schema DDL, seeding
security.py        - password hashing, API-key generation, JWT issue/verify
auth.py            - FastAPI auth dependencies (roles: admin / tenant_user)
models.py          - pydantic request/response schemas
repo.py            - data-access layer (tenants, users, keys, usage, invoices)
usage.py           - Redis live counters + durable event buffer + aggregator
billing_engine.py  - monthly invoice generation from usage rollups + plans
control_plane.py   - FastAPI app: admin + tenant console (the management API)
gateway.py         - FastAPI app: distributed key validation + metering + proxy

The gateway is the metered *data plane*: a stateless, horizontally-scalable tier
that every replica runs identically, sharing state through Redis (live counters,
key cache) and Postgres (durable rollups). It is the testable analogue of the
Apache APISIX deployment described in the design (key-auth + limit-count with the
Redis policy + a usage-logger). The control plane is the *management plane*.
"""
