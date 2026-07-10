"""Contributory/liveness endpoints (feedback, traffic-probe uploads, place
inserts, health, features) are free: never billed and never counted against the
monthly quota. Exercised across both metering paths (APISIX usage sink + the
reference gateway) plus the APISIX projection helpers."""

import re

from conftest import bearer, create_key, insert_plan, make_tenant

from billing import apisix_admin, config

FREE_URIS = [
    "/health", "/status", "/features",
    "/feedback", "/insert", "/places", "/traffic/probe", "/traffic/probes",
    "/nearby/categories",
]
# Everything that answers a user query bills — including /traffic/edge, whose
# sibling /traffic/probe[s] is free (full-path, not first-segment, matching), and
# /nearby, whose sibling /nearby/categories (discovery metadata) is free.
PAID_URIS = [
    "/geocode", "/reverse", "/autocomplete", "/address", "/describe",
    "/deep/forward", "/deep/reverse", "/traffic/edge", "/nearby",
    "/route", "/optimized_route", "/isochrone", "/locate",
]


# ── APISIX projection helpers (pure) ──────────────────────────────────────────
def test_free_regex_matches_free_paths_only():
    rx = re.compile(config.free_endpoints_regex())
    for p in FREE_URIS + [u + "/" for u in FREE_URIS]:  # trailing slash tolerated
        assert rx.match(p), f"{p} should be free"
    for p in PAID_URIS + ["/traffic", "/", ""]:
        assert not rx.match(p), f"{p} should be paid"


def test_free_filter_is_a_negated_uri_regex():
    assert apisix_admin._free_filter() == {
        "_meta": {"filter": [["uri", "!", "~~", config.free_endpoints_regex()]]}
    }


# ── usage sink (APISIX http-logger backstop) ──────────────────────────────────
async def test_sink_does_not_bill_free_endpoints(cp_client, pool, redis):
    _, ttok = await make_tenant(cp_client, admin_email="free-sink@u.io", plan_id="starter")
    key = await create_key(cp_client, ttok)
    consumer = apisix_admin.consumer_name(key["id"])

    entries = [{"consumer": consumer, "uri": uri, "status": 200} for uri in FREE_URIS]
    entries.append({"consumer": consumer, "uri": "/geocode", "status": 200})  # one billable
    r = await cp_client.post("/internal/usage", json=entries)
    assert r.json()["recorded"] == 1  # only the /geocode call

    cur = await cp_client.get("/usage/current", headers=bearer(ttok))
    assert cur.json()["requests"] == 1


async def test_traffic_upload_free_but_edge_query_bills(cp_client, pool, redis):
    """Sibling paths under /traffic split: probe uploads (contributory) are free,
    /traffic/edge (a live-speed query) bills."""
    _, ttok = await make_tenant(cp_client, admin_email="tsplit@u.io", plan_id="starter")
    key = await create_key(cp_client, ttok)
    consumer = apisix_admin.consumer_name(key["id"])

    r = await cp_client.post(
        "/internal/usage",
        json=[
            {"consumer": consumer, "uri": "/traffic/probes", "status": 202},  # free
            {"consumer": consumer, "uri": "/traffic/probe", "status": 202},   # free
            {"consumer": consumer, "uri": "/traffic/edge?lat=30&lon=31", "status": 200},  # bills
        ],
    )
    assert r.json()["recorded"] == 1  # only /traffic/edge

    cur = await cp_client.get("/usage/current", headers=bearer(ttok))
    assert cur.json()["requests"] == 1


# ── reference gateway ─────────────────────────────────────────────────────────
async def test_gateway_free_endpoints_not_metered(cp_client, gw_client, redis):
    _, ttok = await make_tenant(cp_client, admin_email="free-gw@u.io", plan_id="starter")
    key = await create_key(cp_client, ttok)
    h = {"X-API-Key": key["api_key"]}

    for uri in FREE_URIS:
        r = await gw_client.post(uri, headers=h)
        assert r.status_code == 200, uri
        assert r.json()["upstream"] is True

    cur = await cp_client.get("/usage/current", headers=bearer(ttok))
    assert cur.json()["requests"] == 0


async def test_gateway_free_endpoints_bypass_hard_cap(cp_client, gw_client, pool):
    await insert_plan(pool, plan_id="tiny2", quota=2, hard_cap=True)
    _, ttok = await make_tenant(cp_client, admin_email="free-cap@u.io", plan_id="tiny2")
    key = await create_key(cp_client, ttok)
    h = {"X-API-Key": key["api_key"]}

    # burn the whole hard cap on billable calls
    assert (await gw_client.get("/geocode", headers=h)).status_code == 200
    assert (await gw_client.get("/geocode", headers=h)).status_code == 200
    assert (await gw_client.get("/geocode", headers=h)).status_code == 429  # over cap

    # free endpoints still succeed while over the cap, and don't advance the counter
    assert (await gw_client.post("/feedback", headers=h)).status_code == 200
    assert (await gw_client.post("/traffic/probes", headers=h)).status_code == 200

    cur = await cp_client.get("/usage/current", headers=bearer(ttok))
    assert cur.json()["requests"] == 2
