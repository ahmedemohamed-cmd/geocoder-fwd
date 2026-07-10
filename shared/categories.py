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

# First-match-wins precedence over the OSM tag keys that denote a feature's type.
# Order matters: a bakery tagged both ``building=yes`` and ``shop=bakery`` must
# classify as the shop, so specific POI keys come before generic ``building`` and
# geographic keys. Kept identical to the list in shared/llm.py:_build_user_prompt.
CATEGORY_KEYS = [
    "amenity",
    "shop",
    "tourism",
    "leisure",
    "office",
    "historic",
    "building",
    "place",
    "natural",
    "highway",
    "railway",
    "aeroway",
    "waterway",
    "landuse",
]

# Coarse UI groups → {osm_key: [osm_value, ...]}. This is the authoritative
# taxonomy: VALUES_BY_GROUP (for the /nearby/categories discovery endpoint) and
# the (key, value) → group lookup are both derived from it below. Mirrors the
# groupings in shared/places_mapping.py so curated Google/Pelias places (already
# tagged amenity=restaurant etc.) land in the same buckets as native OSM data.
GROUP_DEFS: dict[str, dict[str, list[str]]] = {
    "food": {
        "amenity": [
            "restaurant",
            "fast_food",
            "cafe",
            "bar",
            "pub",
            "nightclub",
            "food_court",
            "ice_cream",
            "biergarten",
        ],
        "shop": [
            "bakery",
            "pastry",
            "confectionery",
            "deli",
            "butcher",
            "greengrocer",
            "seafood",
            "coffee",
        ],
    },
    "finance": {
        "amenity": ["bank", "atm", "bureau_de_change"],
        "office": ["accountant", "insurance", "financial", "financial_advisor"],
    },
    "health": {
        "amenity": [
            "hospital",
            "clinic",
            "doctors",
            "pharmacy",
            "dentist",
            "veterinary",
            "nursing_home",
        ],
        "shop": ["optician", "medical_supply", "chemist", "hearing_aids"],
    },
    "education": {
        "amenity": [
            "school",
            "university",
            "college",
            "kindergarten",
            "library",
            "language_school",
            "driving_school",
            "music_school",
        ],
    },
    "worship": {
        "amenity": ["place_of_worship"],
    },
    "government": {
        "amenity": [
            "police",
            "fire_station",
            "post_office",
            "townhall",
            "courthouse",
            "embassy",
            "prison",
        ],
        "office": ["government", "diplomatic"],
    },
    "automotive": {
        "amenity": [
            "fuel",
            "parking",
            "car_wash",
            "car_rental",
            "charging_station",
            "car_sharing",
        ],
        "shop": ["car_repair", "car", "car_parts", "tyres", "motorcycle"],
    },
    "lodging_tourism": {
        "tourism": [
            "hotel",
            "motel",
            "guest_house",
            "hostel",
            "apartment",
            "museum",
            "attraction",
            "zoo",
            "gallery",
            "camp_site",
            "theme_park",
            "viewpoint",
            "artwork",
            "information",
            "aquarium",
        ],
        "historic": ["monument", "memorial", "castle", "ruins", "archaeological_site"],
    },
    "leisure": {
        "leisure": [
            "park",
            "stadium",
            "fitness_centre",
            "spa",
            "sports_centre",
            "pitch",
            "playground",
            "garden",
            "swimming_pool",
            "golf_course",
            "water_park",
            "marina",
            "dog_park",
            "nature_reserve",
        ],
        "amenity": ["cinema", "theatre", "arts_centre", "community_centre", "amusement_arcade"],
    },
    "shopping": {
        "shop": [
            "mall",
            "supermarket",
            "convenience",
            "department_store",
            "houseware",
            "furniture",
            "hardware",
            "electronics",
            "clothes",
            "shoes",
            "jewelry",
            "books",
            "florist",
            "bicycle",
            "pet",
            "alcohol",
            "beauty",
            "hairdresser",
            "mobile_phone",
            "gift",
            "toys",
            "sports",
            "stationery",
            "cosmetics",
            "variety_store",
            "general",
            "kiosk",
            "travel_agency",
            "yes",
        ],
        "office": ["estate_agent", "lawyer"],
    },
    "transport": {
        "aeroway": ["aerodrome", "terminal"],
        "railway": ["station", "halt", "subway_entrance", "tram_stop"],
        "amenity": ["bus_station", "taxi", "ferry_terminal", "bicycle_rental"],
        "highway": ["bus_stop"],
    },
}

# Coarse key → group fallback for values not explicitly listed above, so an
# uncommon shop/tourism/leisure value still lands in a sensible chip. Keys that
# span several groups (amenity, office) are intentionally absent — they must
# resolve through the precise (key, value) lookup or stay ungrouped.
GROUP_BY_KEY: dict[str, str] = {
    "shop": "shopping",
    "tourism": "lodging_tourism",
    "leisure": "leisure",
    "historic": "lodging_tourism",
    "aeroway": "transport",
    "railway": "transport",
}

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
