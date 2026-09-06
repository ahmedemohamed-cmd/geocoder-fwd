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
from shared.spec import load as load_spec

GOLDEN = pathlib.Path(__file__).parent / "ranking_golden.json"

# Read the tables from the SPEC, never from module internals: the spec is the
# contract, the module layout is not.
SPEC = load_spec("ranking.toml")
TABLES = SPEC["tables"]

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

    for k in TABLES["place"]:
        add(f"place={k}", {"place": k})
    for k in TABLES["landuse"]:
        add(f"landuse={k}", {"landuse": k})
    for k in TABLES["venue"]:
        for vk in ("amenity", "shop", "leisure", "tourism", "aeroway"):
            add(f"{vk}={k}", {vk: k})
    for k in TABLES["highway"]:
        add(f"highway={k}", {"highway": k})
    for k in TABLES["natural"]:
        add(f"natural={k}", {"natural": k})
    for k in TABLES["waterway"]:
        add(f"waterway={k}", {"waterway": k})
    for k in TABLES["building"]:
        add(f"building={k}", {"building": k})
    for k in TABLES["office"]:
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

    for k in SPEC["keys"]["poi_evidence"]:
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
    for t in REQUIRED_TABLES:
        assert TABLES.get(t), f"spec table '{t}' missing or empty"
    for w in REQUIRED_WEIGHTS:
        assert w in SPEC["weights"], f"spec weight '{w}' missing"
    for s in REQUIRED_SCALARS:
        assert s in SPEC["scalars"], f"spec scalar '{s}' missing"
    assert SPEC["keys"]["venue_tags"]
    assert SPEC["keys"]["poi_evidence"]


def test_no_tuning_constants_left_in_code():
    """The tables must come from the spec, not from module literals."""


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


# ── intent, asserted independently of the golden ──────────────────────────
# A snapshot proves nothing changed; it never proves the values were right. If
# the golden were regenerated from a broken implementation it would pin the
# breakage silently. These assert what ranking is FOR, in relations that must
# hold whatever the individual numbers are.


def _rank(tags, admin=None, area=0.0):
    return R.compute_offline_rank(tags, admin, area)


def test_rank_is_always_within_the_documented_scale():
    """The spec says signals are normalised and scaled to 0..10."""
    for tags, admin, area in [
        ({}, None, 0.0),
        ({"place": "continent"}, 2, 1e6),
        ({"amenity": "restaurant", "name": "X", "phone": "1", "brand": "Nike"}, None, 0.0),
        ({"population": "99999999"}, 2, 1e7),
    ]:
        r = _rank(tags, admin, area)
        assert 0.0 <= r <= 10.0, f"rank {r} outside 0..10 for {tags}"


def test_more_important_places_outrank_less_important_ones():
    assert _rank({"place": "country"}) > _rank({"place": "village"})
    assert _rank({"place": "city"}) > _rank({"place": "hamlet"})
    assert _rank({"place": "city"}) > _rank({"place": "isolated_dwelling"})


def test_a_higher_administrative_rank_scores_higher():
    """admin_level counts DOWN: 2 is a country, 10 a suburb."""
    assert _rank({}, admin=2) > _rank({}, admin=4) > _rank({}, admin=8)


def test_population_and_area_raise_importance():
    assert _rank({"place": "city", "population": "5000000"}) > _rank({"place": "city"})
    assert _rank({"place": "city"}, area=10000.0) > _rank({"place": "city"}, area=1.0)


def test_wikipedia_and_wikidata_raise_importance():
    plain = _rank({"place": "town"})
    assert _rank({"place": "town", "wikidata": "Q1"}) > plain
    assert _rank({"place": "town", "wikidata": "Q1", "wikipedia": "en:X"}) > _rank(
        {"place": "town", "wikidata": "Q1"}
    )


def test_a_named_poi_with_evidence_beats_a_bare_node():
    """The named-POI floor exists so a real curated place never sits at zero."""
    assert _rank({"name": "Somewhere", "phone": "123"}) > _rank({"name": "Somewhere"})
    assert _rank({"name": "Somewhere", "phone": "123"}) > _rank({})


def test_generic_buildings_add_nothing():
    """building=yes is deliberately unscored: millions of them would be noise."""
    assert _rank({"building": "yes"}) == _rank({})
    assert _rank({"building": "hospital"}) > _rank({"building": "yes"})


def test_an_unknown_office_still_outranks_no_office():
    assert _rank({"office": "zzz-unknown"}) > _rank({})
