"""Contract for spec/billing.toml.

Plan pricing, per-endpoint credit weights, metering units and security minimums
decide what customers are charged. This pins the arithmetic that produces an
invoice, plus the invariants the price list must satisfy — a reordered or
mistyped spec should fail here, not on a customer's bill.

Regenerate deliberately with
`python tests/billing/test_billing_spec.py --update`.
"""

import datetime
import json
import pathlib

from billing import billing_engine as BE
from billing import config as C
from billing import db as DB
from billing import usage as U
from billing import weights as W
from billing.spec import load

GOLDEN = pathlib.Path(__file__).parent / "billing_golden.json"


def build_snapshot():
    out = {}
    out["default_plans"] = [list(p) for p in DB.DEFAULT_PLANS]
    out["default_weights"] = dict(W.DEFAULT_WEIGHTS)
    out["units"] = {
        "MILLI_PER_CREDIT": W.MILLI_PER_CREDIT,
        "DEFAULT_WEIGHT_MILLI": W.DEFAULT_WEIGHT_MILLI,
        "DEFAULT_MATRIX_ELEMENT_MILLI": W.DEFAULT_MATRIX_ELEMENT_MILLI,
        "MATRIX_ENDPOINT": W.MATRIX_ENDPOINT,
        "MATRIX_ELEMENT_KEY": W.MATRIX_ELEMENT_KEY,
    }
    charges = []
    for pid, _name, quota, base, over, _cap, _rps in DB.DEFAULT_PLANS:
        for used in (
            0,
            1,
            quota // 2,
            quota - 1,
            quota,
            quota + 1,
            quota + 999,
            quota * 2,
            quota * 10,
        ):
            amt, items = BE.compute_charge(
                total_milli_credits=int(used) * 1000,
                base_price_cents=base,
                overage_cents_per_credit=over,
                monthly_quota_credits=quota,
            )
            charges.append(
                {"plan": pid, "used_credits": used, "amount_cents": amt, "line_items": items}
            )
    for milli in (0, 1, 999, 1000, 1001, 1_500_000, 25_000_000, 25_000_001):
        amt, items = BE.compute_charge(
            total_milli_credits=milli,
            base_price_cents=2900,
            overage_cents_per_credit=0.03,
            monthly_quota_credits=25_000,
        )
        charges.append(
            {"plan": "boundary", "milli": milli, "amount_cents": amt, "line_items": items}
        )
    out["charges"] = charges

    wl = dict(W.DEFAULT_WEIGHTS)
    out["weight_for"] = [
        {"e": e, "w": W.weight_for(wl, e)}
        for e in sorted(W.DEFAULT_WEIGHTS) + ["unknown", "", "geocode", "deep"]
    ]
    out["min_weight"] = W.min_weight(wl)
    out["matrix_milli"] = [
        {"s": s, "t": t, "milli": W.matrix_milli(wl, s, t)}
        for s in (0, 1, 2, 5, 10, 50)
        for t in (0, 1, 2, 5, 10, 50)
    ]
    out["projected_cap"] = [
        {"quota": q, "cap": W.projected_request_cap(wl, q)}
        for q in (0, 1, 25_000, 250_000, 3_000_000, 12_000_000)
    ]
    out["matrix_size"] = [
        {"qs": qs, "r": list(W.matrix_size(qs, None))}
        for qs in [None, "", "sources=1|2&targets=3", "sources=1", "targets=1|2|3"]
    ]
    paths = [
        "/health",
        "/status",
        "/features",
        "/feedback",
        "/insert",
        "/places",
        "/traffic/probe",
        "/traffic/probes",
        "/nearby/categories",
        "/traffic/edge",
        "/geocode",
        "/geocode?q=x",
        "/GEOCODE",
        "/health/",
        "health",
        "",
        "/",
        "/deep/forward",
        "/nearby",
    ]
    out["free_paths"] = [{"p": p, "norm": C.norm_path(p), "free": U.is_free_path(p)} for p in paths]
    out["free_endpoints"] = sorted(C.FREE_ENDPOINTS)
    out["free_regex"] = C.free_endpoints_regex()
    out["now_parts"] = list(U.now_parts(datetime.datetime(2026, 3, 1, 0, 0, tzinfo=datetime.UTC)))
    return json.loads(json.dumps(out, sort_keys=True, default=str))


def test_pricing_matches_golden():
    expected = json.loads(GOLDEN.read_text())
    actual = build_snapshot()
    drifted = [k for k in expected if expected[k] != actual.get(k)]
    assert not drifted, f"billing behaviour changed in: {drifted}. If deliberate, --update."


def test_plans_form_a_coherent_ladder():
    """Bigger plans must include more and price overage lower."""
    plans = load()["plans"]
    quotas = [p["quota_credits"] for p in plans]
    assert quotas == sorted(quotas), "plans must be listed smallest quota first"
    paid = [p for p in plans if p["base_price_cents"] > 0]
    overages = [p["overage_cents_per_credit"] for p in paid]
    assert overages == sorted(overages, reverse=True), (
        "a larger plan must not price overage above a smaller one"
    )
    free = [p for p in plans if p["base_price_cents"] == 0]
    for p in free:
        assert p["hard_cap"], "a free plan must hard-cap; it has no overage to bill"


def test_weights_are_positive_integers():
    """A zero or negative weight would make an endpoint free or refund usage."""
    for name, milli in load()["weights"].items():
        if name == "free_endpoints":
            continue
        assert isinstance(milli, int) and milli > 0, f"weight {name} must be a positive int"


def test_describe_is_the_most_expensive_endpoint():
    """LLM inference is the heaviest op; underpricing it loses money per call."""
    weights = {k: v for k, v in load()["weights"].items() if k != "free_endpoints"}
    assert max(weights, key=weights.get) == "describe"


def test_security_minimums_are_not_lowered():
    sec = load()["security"]
    assert sec["pbkdf2_iterations"] >= 240_000, "never lower the password hashing cost"
    assert sec["api_key_bytes"] >= 24, "never shorten API keys"


def test_free_endpoints_never_bill():
    for path in load()["weights"]["free_endpoints"]:
        assert U.is_free_path(path), f"{path} is specified free but bills"


if __name__ == "__main__":
    import sys

    if "--update" in sys.argv:
        GOLDEN.write_text(json.dumps(build_snapshot(), indent=1, sort_keys=True))
        print(f"rewrote {GOLDEN}")
