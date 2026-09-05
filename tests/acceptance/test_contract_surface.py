"""Contracts for the surfaces a regeneration must reproduce exactly.

Three things that are not tuning values but are equally load-bearing:

* **The API** — `spec/api/*.openapi.json` is the contract consumers hold. A
  regenerated implementation that ranks identically but renames a field or
  drops a query parameter has still broken every caller.
* **The streams** — `spec/streams.toml` fixes retention, size caps and delivery
  semantics. Getting one wrong does not fail loudly; it drops messages under
  load or lets one stream starve another.
* **The schema** — `spec/schema/*.sql` is the shape of data that already
  exists. Renaming a column silently orphans it.

The API comparison is a normalized projection (paths, methods, parameters,
status codes, schema names) rather than the whole document, so a FastAPI patch
release does not fail the build while a real contract change still does.
"""

import json
import pathlib

import pytest

from shared.spec import SPEC_DIR

API_DIR = SPEC_DIR / "api"


def _projection(doc):
    """The parts of an OpenAPI document that consumers actually depend on."""
    out = {}
    for path, ops in sorted(doc.get("paths", {}).items()):
        for method, op in sorted(ops.items()):
            if not isinstance(op, dict):
                continue
            params = sorted(
                (p.get("name"), p.get("in"), bool(p.get("required")))
                for p in op.get("parameters", []) or []
            )
            out[f"{method.upper()} {path}"] = {
                "params": params,
                "responses": sorted((op.get("responses") or {}).keys()),
                "body": bool(op.get("requestBody")),
            }
    out["#schemas"] = sorted((doc.get("components", {}).get("schemas", {}) or {}).keys())
    return out


def _live_apps():
    import os

    os.environ.setdefault("BILLING_PG_DB", "billing_test")
    import services.geocoder as G
    from billing import main as M

    return {"geocoder": G.app, "control-plane": M.control_plane_app}


@pytest.mark.parametrize("name", ["geocoder", "control-plane"])
def test_api_contract_unchanged(name):
    pinned = json.loads((API_DIR / f"{name}.openapi.json").read_text(encoding="utf-8"))
    live = _live_apps()[name].openapi()
    exp, act = _projection(pinned), _projection(live)
    removed = sorted(set(exp) - set(act))
    added = sorted(set(act) - set(exp))
    changed = sorted(k for k in exp.keys() & act.keys() if exp[k] != act[k])
    assert not removed, f"{name}: endpoints/schemas removed from the public contract: {removed}"
    assert not (added or changed), (
        f"{name}: contract changed. added={added} changed={changed}. "
        f"If deliberate, regenerate spec/api/{name}.openapi.json."
    )


def test_stream_spec_matches_runtime():
    from shared import nats_client as N
    from shared.spec import load

    spec = load("streams.toml")
    live = {
        "osm": N.OSM_STREAM_CFG,
        "traffic": N.TRAFFIC_STREAM_CFG,
        "traffic_cells": N.TRAFFIC_CELLS_STREAM_CFG,
    }
    for key, cfg in live.items():
        s = spec[key]
        assert cfg.name == s["name"]
        assert list(cfg.subjects) == list(s["subjects"])
        assert int(cfg.max_age) == s["max_age"]
        assert int(cfg.max_bytes) == s["max_bytes"]


def test_work_queue_semantics_are_preserved():
    """The cells stream must stay WORK_QUEUE: LIMITS would deliver each poll
    cell to every replica and duplicate paid provider calls."""
    from shared.spec import load

    assert load("streams.toml")["traffic_cells"]["retention"] == "workqueue"
    assert load("streams.toml")["osm"]["retention"] == "limits"


def test_traffic_stream_cannot_starve_ingest():
    """The probe stream is capped well below the OSM stream's budget on
    purpose — a probe burst must not consume the ingest disk allowance."""
    from shared.spec import load

    spec = load("streams.toml")
    assert spec["traffic"]["max_bytes"] < spec["osm"]["max_bytes"]
    assert spec["traffic"]["max_age"] < spec["osm"]["max_age"]


def test_schema_files_are_present_and_nonempty():
    for name in ("billing.sql", "processed-ledger.sql"):
        p = SPEC_DIR / "schema" / name
        assert p.exists(), f"missing spec/schema/{name}"
        assert "CREATE TABLE" in p.read_text(encoding="utf-8")


def test_schema_is_loaded_from_spec_not_inline():
    import ast

    for mod, name in (("billing/db.py", "SCHEMA"), ("shared/processed.py", "_SCHEMA")):
        src = (SPEC_DIR.parent / mod).read_text()
        tree = ast.parse(src)
        for n in tree.body:
            if (
                isinstance(n, ast.Assign)
                and len(n.targets) == 1
                and getattr(n.targets[0], "id", None) == name
            ):
                assert "_SPEC_DIR" in ast.dump(n.value), f"{mod}: {name} must load from spec/"
                break
        else:
            raise AssertionError(f"{mod}: {name} not found")


def test_both_images_ship_the_spec():
    root = SPEC_DIR.parent
    for dockerfile in ("Dockerfile", "billing/Dockerfile"):
        assert "COPY spec" in (root / dockerfile).read_text(), (
            f"{dockerfile} must COPY spec/ — it is a runtime input, not documentation"
        )
