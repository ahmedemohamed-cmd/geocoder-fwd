"""Ranking spec contract.

`spec/ranking.toml` is specification: 208 tuning values that cannot be
re-derived from the architecture. `shared/ranking.py` is thin logic over it.

This test is the executable half of that contract. It enumerates the full key
space of every scoring table plus the scalar edge cases, scores each one, and
compares against pinned expectations. Any change to the spec OR to the scoring
logic that moves a single rank fails here.

A deliberate ranking change is expected to fail this test. Regenerate with
`python tests/test_ranking_spec.py --update`, then measure the result against
docs/quality-baseline.md before committing — see AD-9 in the geocoder spine.
"""

import json
import pathlib

from shared import ranking as R

GOLDEN = pathlib.Path(__file__).parent / "ranking_golden.json"

REQUIRED_TABLES = (
    "place",
    "landuse",
    "venue",
    "highway",
    "natural",
    "waterway",
    "building",
    "office",
    "brand",
)
REQUIRED_WEIGHTS = (
    "admin",
    "area",
    "place",
    "population",
    "highway",
    "natural",
    "metadata",
    "landuse",
    "poi",
    "brand",
)
REQUIRED_SCALARS = (
    "admin_decay_per_level",
    "admin_base_level",
    "population_log_divisor",
    "area_log_offset",
    "area_log_divisor",
    "metadata_wikidata",
    "metadata_wikipedia",
    "office_unknown_floor",
    "named_poi_floor",
)


def build_cases():
    """Enumerate the scoring surface. Derived from the spec, so a spec change
    changes the case set as well as the scores — both must be reviewed."""
    cases = []

    def add(label, tags, admin=None, area=0.0):
        cases.append({"label": label, "tags": tags, "admin": admin, "area": area})

    for k in R._PLACE_SCORES:
        add(f"place={k}", {"place": k})
    for k in R._LANDUSE_SCORES:
        add(f"landuse={k}", {"landuse": k})
    for k in R._VENUE_SCORES:
        for vk in ("amenity", "shop", "leisure", "tourism", "aeroway"):
            add(f"{vk}={k}", {vk: k})
    for k in R._HIGHWAY_SCORES:
        add(f"highway={k}", {"highway": k})
    for k in R._NATURAL_SCORES:
        add(f"natural={k}", {"natural": k})
    for k in R._WATERWAY_SCORES:
        add(f"waterway={k}", {"waterway": k})
    for k in R._BUILDING_SCORES:
        add(f"building={k}", {"building": k})
    for k in R._OFFICE_SCORES:
        add(f"office={k}", {"office": k})
    add("office=unknown-floor", {"office": "zzz-unknown"})
    add("building=unknown", {"building": "zzz-unknown"})

    for b in [
        "Microsoft",
        "Apple",
        "Google",
        "Amazon",
        "Samsung",
        "McDonald's",
        "Starbucks",
        "Coca-Cola",
        "Pepsi",
        "Nike",
        "Adidas",
        "Unknown Brand",
        "",
    ]:
        add(f"brand={b!r}", {"brand": b, "name": "X"})

    for k in R._POI_EVIDENCE_KEYS:
        add(f"poi-evidence={k}", {"name": "X", k: "v"})
    add("named-no-evidence", {"name": "X"})
    add("evidence-no-name", {"phone": "1"})

    for lvl in [None, -1, 0, 1, 2, 3, 4, 6, 8, 10, 12, 20]:
        add(f"admin={lvl}", {"place": "city"}, admin=lvl)
    for a in [0.0, -1.0, 0.001, 0.01, 1.0, 100.0, 10000.0, 1e6]:
        add(f"area={a}", {"place": "city"}, area=a)
    for p in ["0", "-5", "1000", "50000", "10000000", "1,234,567", "1 234", "abc", "", None]:
        add(f"pop={p!r}", {"place": "city", "population": p})

    add("meta-both", {"wikidata": "Q1", "wikipedia": "en:X"})
    add("meta-wd", {"wikidata": "Q1"})
    add("meta-wp", {"wikipedia": "en:X"})
    add("empty", {})
    add(
        "combo-city",
        {"place": "city", "population": "9500000", "wikidata": "Q1", "wikipedia": "en:Cairo"},
        admin=4,
        area=3000.0,
    )
    add(
        "combo-poi",
        {"amenity": "hospital", "name": "H", "phone": "1", "brand": "Nike", "building": "hospital"},
    )
    add("combo-street", {"highway": "primary", "name": "Tahrir"})
    add("combo-natural", {"natural": "water", "waterway": "river"})

    return [{**c, "rank": R.compute_offline_rank(c["tags"], c["admin"], c["area"])} for c in cases]


def test_spec_file_is_complete():
    """A regenerated implementation must find every value it needs."""
    assert R._load_spec.__module__
    for t in REQUIRED_TABLES:
        assert R._SPEC["tables"].get(t), f"spec table '{t}' missing or empty"
    for w in REQUIRED_WEIGHTS:
        assert w in R._SPEC["weights"], f"spec weight '{w}' missing"
    for s in REQUIRED_SCALARS:
        assert s in R._SPEC["scalars"], f"spec scalar '{s}' missing"
    assert R._SPEC["keys"]["venue_tags"]
    assert R._SPEC["keys"]["poi_evidence"]


def test_no_tuning_constants_left_in_code():
    """The tables must come from the spec, not from module literals."""
    src = pathlib.Path(R.__file__).read_text()
    for name in ("_PLACE_SCORES", "_VENUE_SCORES", "_BRAND_SCORES", "_OFFICE_SCORES"):
        assert (
            f"{name}: dict[str, float] = _SPEC" in src or f"{name}: dict[str, float] = _SPEC" in src
        ), f"{name} should be loaded from the spec, not defined inline"


def test_ranking_matches_golden():
    """Every scored case must match the pinned expectations exactly."""
    expected = json.loads(GOLDEN.read_text())
    actual = build_cases()
    assert len(actual) == len(expected), (
        f"case count changed ({len(expected)} -> {len(actual)}); "
        "the spec's key space moved. Review, then --update."
    )
    drift = [
        (e["label"], e["rank"], a["rank"])
        for e, a in zip(expected, actual, strict=True)
        if e["rank"] != a["rank"] or e["label"] != a["label"]
    ]
    assert not drift, f"{len(drift)} ranks changed, first 5: {drift[:5]}"


if __name__ == "__main__":
    import sys

    if "--update" in sys.argv:
        GOLDEN.write_text(json.dumps(build_cases(), indent=1, sort_keys=True))
        print(f"rewrote {GOLDEN} — now measure against docs/quality-baseline.md")
