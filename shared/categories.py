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
CATEGORY_TEXT_KEYS = (
    "amenity",
    "shop",
    "tourism",
    "leisure",
    "office",
    "historic",
    "healthcare",
    "emergency",
    "cuisine",
    "railway",
    "station",
    "public_transport",
    "aeroway",
)

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
_SUBFEATURE_TAGS: dict[str, frozenset[str]] = {
    "railway": frozenset(
        {"subway_entrance", "platform", "elevator", "construction", "proposed", "disused"}
    ),
    "public_transport": frozenset({"stop_position", "platform"}),
}

# "key=value" → extra search terms. The raw value is always emitted anyway
# (`amenity=pharmacy` → "pharmacy"), so this map only needs to add *aliases* and
# non-English vocabulary.
CATEGORY_SYNONYMS: dict[str, list[str]] = {
    # transit — the motivating case
    "station=subway": [
        "metro",
        "subway",
        "underground",
        "metro station",
        "مترو",
        "محطة مترو",
        "مترو الأنفاق",
    ],
    "railway=station": ["train station", "railway station", "محطة", "محطة قطار", "قطار", "gare"],
    "railway=halt": ["train stop", "محطة"],
    "railway=tram_stop": ["tram", "ترام", "محطة ترام"],
    "public_transport=station": ["station", "محطة"],
    "public_transport=stop_position": ["stop", "موقف"],
    "amenity=bus_station": ["bus station", "bus", "أتوبيس", "موقف أتوبيس", "محطة أتوبيس"],
    "highway=bus_stop": ["bus stop", "موقف أتوبيس"],
    "aeroway=aerodrome": ["airport", "مطار", "aéroport"],
    "aeroway=terminal": ["airport terminal", "صالة مطار", "مطار"],
    # health
    "amenity=hospital": ["hospital", "مستشفى", "مستشفي", "hôpital"],
    "amenity=clinic": ["clinic", "عيادة", "clinique"],
    "amenity=doctors": ["doctor", "طبيب", "عيادة"],
    "amenity=pharmacy": ["pharmacy", "chemist", "صيدلية", "pharmacie"],
    "amenity=dentist": ["dentist", "طبيب أسنان", "أسنان"],
    "healthcare=laboratory": ["lab", "laboratory", "معمل", "مختبر", "تحاليل"],
    # food & drink
    "amenity=restaurant": ["restaurant", "مطعم", "أكل"],
    "amenity=fast_food": ["fast food", "takeaway", "وجبات سريعة", "مطعم"],
    "amenity=cafe": ["cafe", "coffee", "coffee shop", "مقهى", "كافيه", "قهوة"],
    "amenity=bar": ["bar", "بار"],
    "amenity=food_court": ["food court", "مطاعم"],
    "shop=bakery": ["bakery", "مخبز", "فرن", "boulangerie"],
    "shop=pastry": ["pastry", "حلواني", "حلويات"],
    # shopping
    "shop=supermarket": ["supermarket", "grocery", "سوبر ماركت", "بقالة", "supermarché"],
    "shop=convenience": ["convenience store", "grocery", "بقالة", "ميني ماركت"],
    "shop=mall": ["mall", "shopping mall", "مول", "مركز تجاري"],
    "shop=clothes": ["clothes", "clothing", "ملابس"],
    "shop=butcher": ["butcher", "جزارة", "لحوم"],
    "shop=greengrocer": ["greengrocer", "خضار", "فاكهة"],
    "shop=mobile_phone": ["mobile", "phone shop", "محمول", "موبايل"],
    "shop=car_repair": ["car repair", "mechanic", "ورشة", "ميكانيكي"],
    # money
    "amenity=bank": ["bank", "بنك", "مصرف", "banque"],
    "amenity=atm": ["atm", "cash machine", "صراف آلي", "ماكينة صراف"],
    "amenity=bureau_de_change": ["exchange", "money exchange", "صرافة"],
    # fuel & car
    "amenity=fuel": ["fuel", "petrol", "gas station", "بنزين", "محطة بنزين", "وقود"],
    "amenity=parking": ["parking", "car park", "موقف", "جراج", "parking"],
    "amenity=car_wash": ["car wash", "غسيل سيارات"],
    # education
    "amenity=school": ["school", "مدرسة", "école"],
    "amenity=kindergarten": ["kindergarten", "nursery", "حضانة", "روضة"],
    "amenity=university": ["university", "جامعة", "université"],
    "amenity=college": ["college", "institute", "معهد", "كلية"],
    "amenity=library": ["library", "مكتبة"],
    # worship
    "amenity=place_of_worship": ["mosque", "masjid", "church", "مسجد", "جامع", "كنيسة", "زاوية"],
    # public & civic
    "amenity=police": ["police", "police station", "شرطة", "قسم شرطة", "بوليس"],
    "amenity=fire_station": ["fire station", "مطافي", "الحماية المدنية"],
    "amenity=post_office": ["post office", "بريد", "مكتب بريد"],
    "amenity=townhall": ["town hall", "مجلس المدينة", "حي"],
    "amenity=courthouse": ["court", "محكمة"],
    "amenity=embassy": ["embassy", "سفارة", "ambassade"],
    "office=government": ["government office", "مصلحة حكومية", "إدارة"],
    "amenity=toilets": ["toilet", "wc", "حمام", "دورة مياه"],
    # leisure / tourism
    "leisure=park": ["park", "حديقة", "منتزه", "parc"],
    "leisure=garden": ["garden", "حديقة"],
    "leisure=stadium": ["stadium", "استاد", "ملعب"],
    "leisure=fitness_centre": ["gym", "fitness", "جيم", "صالة رياضية"],
    "leisure=sports_centre": ["sports centre", "نادي", "مركز رياضي"],
    "leisure=pitch": ["pitch", "playground", "ملعب"],
    "tourism=hotel": ["hotel", "فندق", "hôtel"],
    "tourism=hostel": ["hostel", "نزل"],
    "tourism=museum": ["museum", "متحف", "musée"],
    "tourism=attraction": ["attraction", "معلم سياحي"],
    "tourism=viewpoint": ["viewpoint", "مطل"],
    "amenity=cinema": ["cinema", "movie theatre", "سينما"],
    "amenity=theatre": ["theatre", "مسرح"],
    "historic=monument": ["monument", "نصب تذكاري", "أثر"],
    "historic=archaeological_site": ["archaeological site", "ruins", "آثار", "موقع أثري"],
}


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
