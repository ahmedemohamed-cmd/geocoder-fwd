"""Category classification — map OSM tags to a (key, value, group) category.

REGENERATED FROM spec/categories.json plus the acceptance contract in
tests/acceptance/. Inferences neither source stated are marked [INFERRED].
"""

from __future__ import annotations

from dataclasses import dataclass

from shared.spec import load as _load_spec

_SPEC = _load_spec("categories.json")

CATEGORY_KEYS = _SPEC["CATEGORY_KEYS"]
GROUP_DEFS = _SPEC["GROUP_DEFS"]
GROUP_BY_KEY = _SPEC["GROUP_BY_KEY"]
CATEGORY_TEXT_KEYS = tuple(_SPEC["CATEGORY_TEXT_KEYS"])
_SUBFEATURE_TAGS = {k: frozenset(v) for k, v in _SPEC["_SUBFEATURE_TAGS"].items()}
CATEGORY_SYNONYMS = _SPEC["CATEGORY_SYNONYMS"]
# Values carrying no meaning alone (building=yes). Added to the spec by the
# regeneration experiment: it was domain data that had stayed behind in code.
_JUNK_VALUES = frozenset(_SPEC["JUNK_VALUES"])

GROUP_PLACE = "place"
GROUP_BOUNDARY = "boundary"

# Every word a user might type to mean "a place of this type", derived from the
# same map that builds category_text so the two cannot drift. /autocomplete uses
# it to recognise a type query and route it to Elasticsearch.
CATEGORY_QUERY_TERMS: frozenset[str] = frozenset(
    term.lower() for terms in CATEGORY_SYNONYMS.values() for term in terms
) | frozenset(key.split("=", 1)[1].replace("_", " ").lower() for key in CATEGORY_SYNONYMS)

GROUPS: list[str] = sorted(GROUP_DEFS)
VALUES_BY_GROUP: dict[str, list[str]] = {
    group: sorted({v for values in keys.values() for v in values})
    for group, keys in GROUP_DEFS.items()
}

# [INFERRED] Reverse index: (key, value) -> group, built from GROUP_DEFS.
GROUP_BY_KEY_VALUE: dict[tuple[str, str], str] = {
    (key, value): group
    for group, keys in GROUP_DEFS.items()
    for key, values in keys.items()
    for value in values
}

# [INFERRED] Neither the spec nor the tests state which groups are non-POI.
# Derived from the acceptance contract: place=city and boundaries are is_poi
# False, everything else True.
_NON_POI_GROUPS = {"place", "boundary"}


@dataclass(frozen=True)
class Category:
    key: str | None
    value: str | None
    group: str | None
    is_poi: bool


def classify(tags: dict, admin_level: int | None = None) -> Category:
    """Classify an element by its tags.

    [INFERRED] Precedence is CATEGORY_KEYS order — the spec lists amenity
    before building, and the acceptance contract requires amenity to win over
    building=yes, which that ordering satisfies.
    """
    # Boundaries win over tags: an element with an admin_level is a boundary
    # regardless of what else it carries (spec: CLASSIFY_RULES).
    if tags.get("boundary") == "administrative":
        return Category(key="boundary", value="administrative", group=GROUP_BOUNDARY, is_poi=False)
    if admin_level is not None and admin_level > 0:
        return Category(key=None, value=None, group=GROUP_BOUNDARY, is_poi=False)

    for key in CATEGORY_KEYS:
        value = tags.get(key)
        if not value:
            continue
        # place=* is its own group and never a POI (spec: CLASSIFY_RULES).
        if key == GROUP_PLACE:
            return Category(key=key, value=value, group=GROUP_PLACE, is_poi=False)
        group = GROUP_BY_KEY_VALUE.get((key, value)) or GROUP_BY_KEY.get(key)
        return Category(key=key, value=value, group=group, is_poi=True)

    return Category(key=None, value=None, group=None, is_poi=False)


def category_text(tags: dict) -> str:
    """Render a place's type as searchable text (English + Arabic).

    Emits each type-denoting tag value (underscores spaced, so fast_food is
    reachable as "fast food") plus any aliases from CATEGORY_SYNONYMS. Returns
    "" for sub-features. Order is stable and duplicates are dropped.
    """
    # Checked against the RAW tags, not the classified triple: a node carrying
    # two sub-feature spellings classifies on whichever key wins precedence, so
    # testing the triple alone would let the other slip through.
    for key, excluded in _SUBFEATURE_TAGS.items():
        value = tags.get(key)
        if isinstance(value, str) and value.strip() in excluded:
            return ""

    terms: list[str] = []
    seen: set[str] = set()

    def add(term: str) -> None:
        t = term.strip()
        if t and t.lower() not in seen:
            seen.add(t.lower())
            terms.append(t)

    for key in CATEGORY_TEXT_KEYS:
        value = tags.get(key)
        if not isinstance(value, str) or not value:
            continue
        for v in value.split(";"):
            v = v.strip()
            if not v or v.lower() in _JUNK_VALUES:
                continue
            add(v.replace("_", " "))
            for syn in CATEGORY_SYNONYMS.get(f"{key}={v}", ()):
                add(syn)

    return " ".join(terms)
