"""Single source of truth for classifying a place into a filterable category.

The ``osm_places`` index maps ``tags`` as ``enabled: false`` (stored, not
indexed), so OSM feature tags (``amenity=restaurant``, ``shop=bakery``, …) cannot
be filtered on directly. To support ``/nearby?category=…`` we derive three
mapped keyword fields from the tags at ingest time:

* ``category_key``   – the OSM tag key that classifies the feature (``amenity``)
* ``category_value`` – its value, the precise type filter (``restaurant``)
* ``category_group`` – a coarse UI group for filter chips (``food``)

``classify`` is a **pure** function of ``tags`` + ``admin_level`` so the exact
same derivation is used by three callers and can never drift between them:
the ingest path (``services/es_inserter.py``), the in-place backfill
(``scripts/backfill_categories.py``), and ``/nearby`` response shaping
(``services/nearby.py``).

The category-key precedence list is the one previously duplicated in
``shared/llm.py`` (``_build_user_prompt``); the coarse groups follow the section
groupings already encoded in ``shared/places_mapping.py`` (``LAYER_FEATURE`` /
``CATEGORY_FEATURE``).
"""

from __future__ import annotations

from dataclasses import dataclass

from shared.spec import load as _load_spec

_SPEC = _load_spec("categories.toml")

# First-match-wins precedence over the OSM tag keys that denote a feature's type.
# Order matters: a bakery tagged both ``building=yes`` and ``shop=bakery`` must
# classify as the shop, so specific POI keys come before generic ``building`` and
# geographic keys. Kept identical to the list in shared/llm.py:_build_user_prompt.
CATEGORY_KEYS = _SPEC["CATEGORY_KEYS"]

# Coarse UI groups → {osm_key: [osm_value, ...]}. This is the authoritative
# taxonomy: VALUES_BY_GROUP (for the /nearby/categories discovery endpoint) and
# the (key, value) → group lookup are both derived from it below. Mirrors the
# groupings in shared/places_mapping.py so curated Google/Pelias places (already
# tagged amenity=restaurant etc.) land in the same buckets as native OSM data.
GROUP_DEFS = _SPEC["GROUP_DEFS"]

# Coarse key → group fallback for values not explicitly listed above, so an
# uncommon shop/tourism/leisure value still lands in a sensible chip. Keys that
# span several groups (amenity, office) are intentionally absent — they must
# resolve through the precise (key, value) lookup or stay ungrouped.
GROUP_BY_KEY = _SPEC["GROUP_BY_KEY"]

# Non-POI markers: features tagged with these are geographic areas / admin
# boundaries, never "places to explore". /nearby drops them via category_group.
GROUP_PLACE = "place"
GROUP_BOUNDARY = "boundary"

# Derived lookups — built once at import.
GROUP_BY_KEY_VALUE: dict[tuple[str, str], str] = {
    (key, value): group
    for group, keys in GROUP_DEFS.items()
    for key, values in keys.items()
    for value in values
}
GROUPS: list[str] = sorted(GROUP_DEFS)
VALUES_BY_GROUP: dict[str, list[str]] = {
    group: sorted({v for values in keys.values() for v in values})
    for group, keys in GROUP_DEFS.items()
}


@dataclass(frozen=True)
class Category:
    """A place's classified type. ``key``/``value``/``group`` may be ``None``
    when the feature carries no recognised category tag."""

    key: str | None
    value: str | None
    group: str | None
    is_poi: bool


def classify(tags: dict, admin_level: int | None = None) -> Category:
    """Classify an OSM element into ``(key, value, group, is_poi)``.

    Admin boundaries and ``place=*`` areas resolve to ``is_poi=False`` with a
    ``boundary``/``place`` group so /nearby can exclude them even after backfill.
    Everything else takes the first matching key in ``CATEGORY_KEYS`` and is a POI.
    """
    boundary = tags.get("boundary")
    if boundary == "administrative" or (admin_level is not None and admin_level > 0):
        return Category(
            key="boundary" if boundary else None,
            value=boundary,
            group=GROUP_BOUNDARY,
            is_poi=False,
        )

    key: str | None = None
    value: str | None = None
    for k in CATEGORY_KEYS:
        v = tags.get(k)
        if v:
            key, value = k, v
            break

    # No recognised category tag → not a place to surface in /nearby.
    if key is None:
        return Category(None, None, None, is_poi=False)

    # place=* (neighbourhood, city, suburb, …) is an area, not a POI.
    if key == "place":
        return Category(key=key, value=value, group=GROUP_PLACE, is_poi=False)

    group = GROUP_BY_KEY_VALUE.get((key, value)) or GROUP_BY_KEY.get(key)
    return Category(key=key, value=value, group=group, is_poi=True)


# ── searchable category text ─────────────────────────────────────────────────
#
# `build_text` (shared/embeddings.py) concatenates tag *values* only — tag keys
# are never emitted — so a Cairo metro station tagged
# ``{name: Sadat, railway: station, station: subway}`` yields the text
# "Sadat station subway". The token "metro" never appears, and a user typing
# "metro" can never reach it. `category_text` closes that gap: it renders a
# place's *type* as natural search vocabulary, in English and Arabic.
#
# Indexed into the `category_text` field (see shared/es_mapping.py) and queried
# at a deliberately LOW boost by /autocomplete, so a name match always outranks
# a type match — users searching "Metro" usually want the supermarket chain.

# Tag keys that denote a feature's *type*. Deliberately narrower than
# CATEGORY_KEYS: `building`, `place`, `highway`, `landuse`, `natural` and
# `waterway` are excluded because their values ("residential", "neighbourhood",
# "yes") are noise for type search rather than things people search *for*.
#
# `station`, `public_transport`, `healthcare`, `emergency` and `cuisine` are
# included here but are absent from CATEGORY_KEYS. Reading the raw tag dict lets
# us see `station=subway` WITHOUT touching `classify()` — adding keys there would
# silently change how /nearby buckets existing places.
CATEGORY_TEXT_KEYS = tuple(_SPEC["CATEGORY_TEXT_KEYS"])

# Values that carry no meaning on their own (`building=yes`).
_JUNK_VALUES = frozenset({"yes", "no", "true", "false", "unknown", "none"})

# Features that are a *part of* a place rather than a place. A `railway=station`
# is a destination; its `railway=subway_entrance` doorways are not — OSM models
# every street-level entrance as its own node, named after the street it opens
# onto. Left searchable, they swamp the real thing: a "metro" query near Toronto
# returned "Delaware Avenue", "Cresecent Road Entrance", "5775 Yonge St Entrance"
# — six doorways and one station. Within 50km of Vaughan the index holds 245
# subway_entrance nodes against 68 stations; within 50km of Cairo it holds 2
# against 88, which is exactly why this was invisible on the Egypt-only dev index.
#
# Excluded from `category_text` (free-text type search) ONLY. They keep their
# category_key/category_value, so /nearby can still filter for them deliberately.
_SUBFEATURE_TAGS = {k: frozenset(v) for k, v in _SPEC["_SUBFEATURE_TAGS"].items()}

# "key=value" → extra search terms. The raw value is always emitted anyway
# (`amenity=pharmacy` → "pharmacy"), so this map only needs to add *aliases* and
# non-English vocabulary.
CATEGORY_SYNONYMS = _SPEC["CATEGORY_SYNONYMS"]


# Every word a user might type to mean "a place of this type", derived from the
# same map that builds `category_text` so the two can never drift. Used by
# /autocomplete to recognise a *type* query and route it to Elasticsearch — the
# Redis prefix index holds names only, so left to itself it answers "metro" with
# the Metro supermarket chain and never surfaces a single station.
CATEGORY_QUERY_TERMS: frozenset[str] = frozenset(
    term.lower() for terms in CATEGORY_SYNONYMS.values() for term in terms
) | frozenset(key.split("=", 1)[1].replace("_", " ").lower() for key in CATEGORY_SYNONYMS)


def category_text(tags: dict) -> str:
    """Render a place's *type* as searchable text (English + Arabic).

    Pure function of the raw tag dict, mirroring `classify`'s contract. Emits
    each type-denoting tag value (underscores spaced out, so ``fast_food`` is
    reachable as "fast food") plus any aliases from `CATEGORY_SYNONYMS`.

    ``{"name": "Sadat", "railway": "station", "station": "subway"}``
        → ``"station train station railway station محطة … subway metro underground مترو …"``

    Returns ``""`` for sub-features (see `_SUBFEATURE_TAGS`) — a station's doorway
    is not a place anyone searches for, and there are many per station.

    Order is stable and duplicates are dropped so the output is deterministic.
    """
    # Checked against the RAW tags, not the classified triple: a node carrying both
    # `railway=subway_entrance` and `public_transport=platform` classifies on
    # whichever key wins CATEGORY_KEYS precedence, so testing the triple alone would
    # let one of the two spellings slip through.
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
        # Multi-values ("cuisine=pizza;burger") and junk ("building=yes").
        for v in value.split(";"):
            v = v.strip()
            if not v or v.lower() in _JUNK_VALUES:
                continue
            add(v.replace("_", " "))
            for syn in CATEGORY_SYNONYMS.get(f"{key}={v}", ()):
                add(syn)

    return " ".join(terms)
