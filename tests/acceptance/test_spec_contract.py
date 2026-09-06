"""Specification contract for `spec/`.

`spec/` holds the tuning knowledge that cannot be re-derived from the
architecture: ranking weights, the category taxonomy, address-parsing
vocabularies, the Elasticsearch analyzer chain, autocomplete scoring. The
modules under `shared/` are thin logic over those files.

This test pins the *behaviour* those files produce. It is the executable half
of the regeneration contract (docs/specs/REGENERATION-CONTRACT.md): a
regenerated implementation reading the same spec must produce these same
answers, and an accidental edit to a spec file fails here rather than silently
changing search results.

A deliberate change is expected to fail. Regenerate with
`python tests/test_spec_contract.py --update`, then measure against
docs/quality-baseline.md before committing.
"""

import ast
import json
import pathlib

import pytest

from shared import address as A
from shared import autocomplete as AC
from shared import categories as C
from shared import es_mapping as EM
from shared import places_mapping as PM
from shared.spec import SPEC_DIR, load

GOLDEN = pathlib.Path(__file__).parent / "spec_golden.json"

SPEC_FILES = [
    "ranking.toml",
    "autocomplete.toml",
    "es-mapping.json",
    "categories.toml",
    "address.toml",
    "places-mapping.toml",
    "interpolation.toml",
]


@pytest.mark.parametrize("name", SPEC_FILES)
def test_spec_file_loads(name):
    """Every spec file must exist and parse — it is a runtime input."""
    assert (SPEC_DIR / name).exists(), f"missing spec/{name}"
    assert load(name), f"spec/{name} parsed empty"


def test_spec_ships_in_image():
    """spec/ is loaded at import, so the image must COPY it."""
    dockerfile = (SPEC_DIR.parent / "Dockerfile").read_text()
    assert "COPY spec/" in dockerfile, "Dockerfile must COPY spec/ or the container cannot start"


def test_tables_come_from_the_spec():
    """The loaded tables must equal the spec, whatever route the module takes.

    This asserts equivalence, not the shape of the assignment: a regeneration
    is free to read spec/ through an intermediate, a helper, or a class. An
    earlier version of this test required the literal `_SPEC` in the assignment
    expression and failed a correct regeneration that used a local alias.
    """
    assert C.GROUP_DEFS == load("categories.toml")["GROUP_DEFS"]
    assert C.CATEGORY_SYNONYMS == load("categories.toml")["CATEGORY_SYNONYMS"]
    assert PM.LAYER_FEATURE == load("places-mapping.toml")["LAYER_FEATURE"]
    assert EM.MAPPING["mappings"], "es mapping must be populated from spec"

    # Ranking is covered behaviourally by tests/acceptance/test_ranking_spec.py:
    # its 570-case golden moves if the module stops reading the spec tables.


def build_snapshot():
    """Exercise every spec-backed surface. Mirrors the pinned golden file."""
    out = {}
    out["es_mapping"] = EM.MAPPING
    out["es_replicas"] = EM.ES_INDEX_REPLICAS

    cat, tagsets = [], []
    for _g, d in C.GROUP_DEFS.items():
        for k in d if isinstance(d, dict) else {}:
            v = d[k]
            vals = v if isinstance(v, (list, tuple, set)) else [v]
            for val in list(vals)[:4]:
                if isinstance(val, str):
                    tagsets.append({k: val})
    tagsets += [
        {},
        {"name": "X"},
        {"amenity": "cafe", "shop": "bakery"},
        {"place": "city"},
        {"boundary": "administrative"},
        {"railway": "station"},
        {"amenity": "place_of_worship", "religion": "muslim"},
        {"shop": "supermarket", "brand": "Metro"},
        {"office": "lawyer"},
        {"tourism": "hotel"},
        {"leisure": "park"},
        {"highway": "primary"},
    ]
    for t in tagsets:
        for al in (None, 4, 8):
            cat.append(
                {
                    "tags": t,
                    "admin": al,
                    "result": repr(C.classify(t, al)),
                    "text": C.category_text(t),
                }
            )
    out["categories"] = cat
    out["category_keys"] = list(C.CATEGORY_KEYS)
    out["category_synonyms"] = {
        k: (sorted(v) if isinstance(v, list | set | tuple) else v)
        for k, v in C.CATEGORY_SYNONYMS.items()
    }

    queries = [
        "15 Tahrir Street, Cairo",
        "شارع التحرير 15",
        "5th Avenue",
        "fifth avenue",
        "12 rue de la Paix, Paris",
        "1600 Pennsylvania Ave NW",
        "Main St",
        "42 Blvd Saint-Germain",
        "شارع 9 المعادي",
        "21st street",
        "3rd ave apt 4",
        "no number here",
        "Cairo",
        "",
        "  ",
        "123",
        "St. John's Road",
        "Av. des Champs-Élysées",
    ]
    out["address"] = [
        {
            "q": q,
            "normalized": A.normalize_address_text(q),
            "is_addr": A.is_address_query(q),
            "parsed": A.parse_address_query(q),
            "ordinals": A.expand_ordinals(q),
        }
        for q in queries
    ]
    addr_tags = [
        {"addr:housenumber": "15", "addr:street": "Tahrir", "addr:city": "Cairo"},
        {"addr:street": "Tahrir"},
        {"addr:postcode": "11511"},
        {},
        {"addr:housenumber": "1", "addr:street": "شارع التحرير", "addr:city": "القاهرة"},
    ]
    out["address_tags"] = [
        {
            "tags": t,
            "has": A.has_address(t),
            "components": A.extract_address_components(t),
            "full": A.build_full_address(t),
        }
        for t in addr_tags
    ]

    recs = [
        {"layer": layer, "name": "X", "lat": 30.0, "lon": 31.0}
        for layer in list(PM.LAYER_FEATURE)[:40]
    ]
    recs += [
        {"category": c, "name": "Y", "lat": 30.0, "lon": 31.0}
        for c in list(PM.CATEGORY_FEATURE)[:15]
    ]
    pm = []
    for r in recs:
        try:
            u = PM.to_unified(dict(r))
        except Exception as e:  # noqa: BLE001 - snapshot records the failure shape
            u = f"ERR:{type(e).__name__}"
        pm.append({"rec": r, "unified": u})
    out["places_mapping"] = pm
    out["layer_feature"] = PM.LAYER_FEATURE
    out["category_feature"] = PM.CATEGORY_FEATURE
    out["admin_layer"] = PM.ADMIN_LAYER

    out["autocomplete_scalars"] = {
        k: getattr(AC, k)
        for k in (
            "TOP_K",
            "MIN_PREFIX",
            "MAX_PREFIX",
            "POPULARITY_CAP",
            "W_MATCH",
            "W_RANK",
            "W_POP",
            "W_GEO",
            "MQ_EXACT",
            "MQ_PREFIX",
            "MQ_FIRST_WORD",
            "MQ_ANY_WORD",
        )
    }
    out["autocomplete_score"] = [
        {"rank": r, "pop": p, "score": AC.compute_score(r, p)}
        for r in (0.0, 0.5, 1.0, 5.0, 10.0)
        for p in (0.0, 1.0, 100.0, 1000.0, 5000.0)
    ]
    out["geohash"] = [
        {"lat": la, "lon": lo, "p": pr, "gh": AC.encode_geohash(la, lo, pr)}
        for la, lo in ((30.0444, 31.2357), (0, 0), (-33.9, 18.4), (60.1, 24.9))
        for pr in (3, 4, 5)
    ]
    out["is_category_query"] = [
        {"q": q, "r": AC.is_category_query(q)}
        for q in ["metro", "hospital", "مستشفى", "cafe", "Starbucks", "15 Tahrir", ""]
    ]

    interp = load("interpolation.toml")
    out["interp_street_prefixes"] = sorted(interp["_STREET_PREFIXES"])
    out["interp_ar_delete"] = sorted(interp["_AR_DELETE"])
    return json.loads(json.dumps(out, sort_keys=True, default=str, ensure_ascii=False))


def test_spec_behaviour_matches_golden():
    expected = json.loads(GOLDEN.read_text(encoding="utf-8"))
    actual = build_snapshot()
    drifted = [k for k in expected if expected[k] != actual.get(k)]
    assert not drifted, (
        f"spec-backed behaviour changed in: {drifted}. "
        "If deliberate, re-run with --update and measure against the quality baseline."
    )


if __name__ == "__main__":
    import sys

    if "--update" in sys.argv:
        GOLDEN.write_text(
            json.dumps(build_snapshot(), indent=1, sort_keys=True, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"rewrote {GOLDEN} — now measure against docs/quality-baseline.md")


# ── intent, asserted independently of the golden ──────────────────────────
# The snapshot above is large and would pin a regression just as happily as
# correct behaviour. These state what the specs are FOR.


def test_cross_language_address_parsing_is_possible():
    """Cross-language interpolation is the headline feature: an English query
    must normalise to something that can meet Arabic-tagged data."""
    assert A.normalize_address_text("15 Tahrir Street, Cairo")
    assert A.is_address_query("15 Tahrir Street")
    parsed = A.parse_address_query("15 Tahrir Street, Cairo")
    assert parsed.get("housenumber") == "15"
    assert "tahrir" in (parsed.get("street") or "").lower()
    # Arabic parses too, including Arabic-Indic digits — leading number form.
    ar = A.parse_address_query("15 شارع التحرير")
    assert ar.get("housenumber") == "15"
    ar_digits = A.parse_address_query("١٥ شارع التحرير")
    assert ar_digits.get("housenumber") == "١٥", "Arabic-Indic digits must parse"


def test_housenumber_parsing_only_supports_the_leading_number_convention():
    """Documents a real limitation rather than hiding it (gap register G-18).

    Every language parses a LEADING housenumber. None parse a TRAILING one,
    which is the standard convention across much of Europe — Via Roma 12,
    Hauptstraße 12, Calle Mayor 12. On a global index that is a recall gap, and
    the Cairo-only baseline cannot see it.

    Change this test when the parser is fixed; do not delete it.
    """
    for q in ["شارع التحرير 15", "rue de la Paix 15", "Hauptstrasse 12"]:
        assert A.parse_address_query(q).get("housenumber") is None, (
            f"{q!r} now parses a trailing housenumber — update G-18 and this test"
        )


def test_ordinal_expansion_covers_both_spellings_only_for_explicit_ordinals():
    """5th <-> fifth must find each other; a bare cardinal must NOT expand —
    'District 5' is a different place from 'Fifth District'."""
    forms = {f.lower() for f in A.expand_ordinals("5th Avenue")}
    assert any("fifth" in f for f in forms), forms
    assert A.expand_ordinals("5 Avenue") == [], "a bare cardinal must not expand"


def test_classification_separates_destinations_from_locations():
    assert C.classify({"amenity": "restaurant"}).is_poi is True
    assert C.classify({"place": "city"}).is_poi is False
    assert C.classify({}, admin_level=4).is_poi is False


def test_subfeatures_are_dropped_from_type_search_but_keep_their_category():
    """Station doorways swamp real stations in free-text type search, but
    /nearby must still be able to filter for them deliberately."""
    doorway = {"railway": "subway_entrance", "name": "Some Entrance"}
    assert C.category_text(doorway) == "", "a sub-feature must not be type-searchable"
    assert C.classify(doorway).value == "subway_entrance", "but it keeps its category"
    assert C.category_text({"railway": "station"}), "a real station must be searchable"


def test_junk_values_never_become_search_terms():
    assert "yes" not in C.category_text({"building": "yes", "amenity": "cafe"}).split()


def test_autocomplete_score_rises_with_importance_and_popularity():
    assert AC.compute_score(5.0, 0.0) > AC.compute_score(1.0, 0.0)
    assert AC.compute_score(1.0, 1000.0) > AC.compute_score(1.0, 0.0)


def test_geohash_precision_controls_cell_size():
    a = AC.encode_geohash(30.0444, 31.2357, 3)
    b = AC.encode_geohash(30.0444, 31.2357, 5)
    assert len(a) == 3 and len(b) == 5
    assert b.startswith(a), "a finer geohash must refine the coarser cell"


def test_es_mapping_declares_the_fields_search_depends_on():
    props = EM.MAPPING["mappings"]["properties"]
    for field in ("name", "full_address", "offline_rank", "popularity", "centroid"):
        assert field in props, f"ES mapping is missing {field}, which search queries use"
