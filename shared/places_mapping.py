"""Unified ``place`` schema + mapping into the watcher OSM-element NATS format.

Curated place datasets arrive in different raw shapes (a Pelias Elasticsearch
``source=google`` export of ``{_id,_source}`` objects; a flat Postgres
``places`` table dump). To keep ``data/places/`` homogeneous, every file is
first normalised to ONE **unified place schema** (lossless — anything not
promoted to a named field is preserved under ``extra``)::

    {
        "osm_id":     "g_<sha1(place_id)>" | "pg_<id>",   # stable element id
        "source":     "google" | "places_pg",
        "source_id":  "<google place_id | pg row id>",
        "name":       "<display name>",
        "names":      {"<lang>": "<name>", ...},          # language variants
        "lat": <float>, "lon": <float>,
        "layer":      "<layer>",
        "categories": ["..."],
        "address": {                                       # all keys always present
            "full","street","suburb","district","city",
            "state","postcode","country","country_code","state_code"
        },
        "phone":  "<phone>" | null,
        "rating": <float> | null,
        "extra":  { ... }                                  # source-specific leftovers
    }

The ``places-watcher`` (and the ``import_places`` script) then turn one unified
record into the exact message ``watcher.py`` / ``shared.google_maps`` publish,
so the ``es_inserter`` / ``postgis_inserter`` consumers index it unchanged::

    {"osm_id", "osm_type": "node", "tags": {...}, "geom": {Point}, "admin_level", "area_km2"}

``to_unified`` (raw → unified) is used by ``scripts/unify_places.py`` to convert
the on-disk files; ``place_to_element`` (unified → NATS message) is the single
runtime mapper. This module is the single source of truth for both.
"""

from __future__ import annotations

import re

from shared.google_maps import place_osm_id

GOOGLE_SOURCE = "google"
PG_SOURCE = "places_pg"

# ── layer → OSM feature tag (key, value) — union of both datasets' layers ───
LAYER_FEATURE = {
    # food / drink
    "restaurant": ("amenity", "restaurant"),
    "food": ("amenity", "restaurant"),
    "meal_takeaway": ("amenity", "fast_food"),
    "meal_delivery": ("amenity", "fast_food"),
    "cafe": ("amenity", "cafe"),
    "bar": ("amenity", "bar"),
    "night_club": ("amenity", "nightclub"),
    "bakery": ("shop", "bakery"),
    # finance
    "bank": ("amenity", "bank"),
    "atm": ("amenity", "atm"),
    "finance": ("amenity", "bank"),
    "accounting": ("office", "accountant"),
    "insurance_agency": ("office", "insurance"),
    # health
    "hospital": ("amenity", "hospital"),
    "health": ("amenity", "clinic"),
    "doctor": ("amenity", "doctors"),
    "doctors": ("amenity", "doctors"),
    "physiotherapist": ("amenity", "doctors"),
    "pharmacy": ("amenity", "pharmacy"),
    "drugstore": ("amenity", "pharmacy"),
    "dentist": ("amenity", "dentist"),
    "veterinary_care": ("amenity", "veterinary"),
    # education
    "school": ("amenity", "school"),
    "primary_school": ("amenity", "school"),
    "secondary_school": ("amenity", "school"),
    "university": ("amenity", "university"),
    "library": ("amenity", "library"),
    # worship
    "mosque": ("amenity", "place_of_worship"),
    "church": ("amenity", "place_of_worship"),
    "synagogue": ("amenity", "place_of_worship"),
    "hindu_temple": ("amenity", "place_of_worship"),
    "place_of_worship": ("amenity", "place_of_worship"),
    # public / government
    "police": ("amenity", "police"),
    "fire_station": ("amenity", "fire_station"),
    "post_office": ("amenity", "post_office"),
    "local_government_office": ("office", "government"),
    "government": ("office", "government"),
    "city_hall": ("amenity", "townhall"),
    "courthouse": ("amenity", "courthouse"),
    "embassy": ("amenity", "embassy"),
    "office": ("office", "yes"),
    # automotive / fuel
    "gas_station": ("amenity", "fuel"),
    "fuel": ("amenity", "fuel"),
    "parking": ("amenity", "parking"),
    "car_repair": ("shop", "car_repair"),
    "car_dealer": ("shop", "car"),
    "car": ("shop", "car"),
    "car_wash": ("amenity", "car_wash"),
    "car_rental": ("amenity", "car_rental"),
    # lodging / tourism / leisure
    "lodging": ("tourism", "hotel"),
    "museum": ("tourism", "museum"),
    "tourist_attraction": ("tourism", "attraction"),
    "zoo": ("tourism", "zoo"),
    "art_gallery": ("tourism", "gallery"),
    "campground": ("tourism", "camp_site"),
    "park": ("leisure", "park"),
    "stadium": ("leisure", "stadium"),
    "gym": ("leisure", "fitness_centre"),
    "spa": ("leisure", "spa"),
    "amusement_park": ("leisure", "amusement_arcade"),
    "movie_theater": ("amenity", "cinema"),
    # shops
    "shopping_mall": ("shop", "mall"),
    "supermarket": ("shop", "supermarket"),
    "grocery_or_supermarket": ("shop", "supermarket"),
    "convenience_store": ("shop", "convenience"),
    "department_store": ("shop", "department_store"),
    "home_goods_store": ("shop", "houseware"),
    "furniture_store": ("shop", "furniture"),
    "hardware_store": ("shop", "hardware"),
    "electronics_store": ("shop", "electronics"),
    "clothing_store": ("shop", "clothes"),
    "clothes": ("shop", "clothes"),
    "shoe_store": ("shop", "shoes"),
    "jewelry_store": ("shop", "jewelry"),
    "book_store": ("shop", "books"),
    "florist": ("shop", "florist"),
    "bicycle_store": ("shop", "bicycle"),
    "pet_store": ("shop", "pet"),
    "liquor_store": ("shop", "alcohol"),
    "beauty_salon": ("shop", "beauty"),
    "beauty": ("shop", "beauty"),
    "hair_care": ("shop", "hairdresser"),
    "hairdresser": ("shop", "hairdresser"),
    "store": ("shop", "yes"),
    "travel_agency": ("shop", "travel_agency"),
    "real_estate_agency": ("office", "estate_agent"),
    "lawyer": ("office", "lawyer"),
    # transport
    "airport": ("aeroway", "aerodrome"),
    "subway_station": ("railway", "station"),
    "train_station": ("railway", "station"),
    "transit_station": ("public_transport", "station"),
    "bus_station": ("amenity", "bus_station"),
}

# coarse fallback from the categories[] array when the layer isn't mapped
CATEGORY_FEATURE = {
    "shop": ("shop", "yes"),
    "food": ("amenity", "restaurant"),
    "health": ("amenity", "clinic"),
    "education": ("amenity", "school"),
    "religion": ("amenity", "place_of_worship"),
    "finance": ("amenity", "bank"),
    "accommodation": ("tourism", "hotel"),
    "entertainment": ("leisure", "yes"),
    "nightlife": ("amenity", "bar"),
    "government": ("office", "government"),
    "professional": ("office", "yes"),
    "transport": ("public_transport", "station"),
    "transportation": ("aeroway", "aerodrome"),
    "nature": ("leisure", "park"),
    "historic": ("historic", "yes"),
}

# admin layers → (OSM admin_level, place= value); POIs stay at level 0
ADMIN_LAYER = {
    "locality": (8, "city"),
    "localadmin": (8, "town"),
    "borough": (10, "suburb"),
    "neighbourhood": (10, "neighbourhood"),
    "region": (4, "state"),
    "macroregion": (4, "state"),
    "county": (6, "county"),
    "macrocounty": (6, "county"),
    "country": (2, "country"),
}

# Generic source layers that carry NO type information. A record with one of
# these (or an empty layer) and no usable category is unclassified — if its name
# looks like a district/area, the source almost certainly mislabeled a place as
# a POI (e.g. the "التجمع الخامس" district arrives as layer=venue), so we promote
# it to an OSM ``place`` below.
_GENERIC_LAYERS = {"", "venue", "point_of_interest", "poi", "establishment", "premise"}

# Admin-sounding name keyword → OSM ``place`` value. Applied ONLY to otherwise
# unclassified generic-layer records (see above), so businesses that merely
# contain one of these words keep their mapped feature tag and are unaffected.
# Heuristic and intentionally conservative — it assigns ``place`` (a modest rank
# boost) but never fabricates an admin_level on a point.
_PLACE_NAME_KEYWORDS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"ضاحي[ةه]|\bsuburb\b", re.IGNORECASE), "suburb"),
    (re.compile(r"قري[ةه]|\bvillage\b", re.IGNORECASE), "village"),
    (
        re.compile(
            r"التجمع|كم?بوند|\bcompound\b|\bحي\b|\bالحي\b|منطق[ةه]|مدين[ةه]"
            r"|\bdistrict\b|\bneighou?rhood\b|\bzone\b|\bquarter\b|\bsettlement\b",
            re.IGNORECASE,
        ),
        "neighbourhood",
    ),
]


def _place_from_name(name: str) -> str | None:
    """Return an OSM ``place`` value if *name* looks like a district/area."""
    for pattern, place_val in _PLACE_NAME_KEYWORDS:
        if pattern.search(name):
            return place_val
    return None


# Pelias parent keys promoted into address.* (the rest are kept under extra.wof)
_PELIAS_PROMOTED_PARENT = {
    "neighbourhood",
    "borough",
    "county",
    "locality",
    "localadmin",
    "region",
    "macroregion",
    "postalcode",
    "country",
    "country_a",
    "region_a",
}

_ADDRESS_KEYS = (
    "full",
    "street",
    "suburb",
    "district",
    "city",
    "state",
    "postcode",
    "country",
    "country_code",
    "state_code",
)

_POSTCODE_RE = re.compile(r"\b\d{5}\b")


def _empty_address() -> dict[str, str]:
    return {k: "" for k in _ADDRESS_KEYS}


def _first(val) -> str:
    """Pelias parent fields are single-element lists; unwrap to a clean str."""
    if isinstance(val, list):
        val = val[0] if val else None
    return "" if val is None else str(val).strip()


# ── Pelias google: raw → unified ───────────────────────────────────────────
def _pelias_name(src: dict) -> tuple[str, dict[str, str]]:
    """Clean display name + per-language name tags from a Pelias record.

    ``name.default`` is a noisy list interleaving the real name with the layer
    value, category values, an empty string and (sometimes) the formatted
    address; we strip those out. ``name.<lang>`` keys are clean single strings.
    """
    name = src.get("name") or {}
    layer = src.get("layer") or ""
    cats = {str(c) for c in (src.get("category") or [])}
    addr_vals = {str(v) for v in (src.get("address") or {}).values() if v}
    junk = {"", layer} | cats | addr_vals

    lang_names = {
        k: v.strip()
        for k, v in name.items()
        if k != "default" and isinstance(v, str) and v.strip()
    }

    default = name.get("default")
    candidates: list[str] = []
    if isinstance(default, list):
        for x in default:
            if isinstance(x, str):
                xs = x.strip()
                if xs and xs not in junk and "," not in xs:
                    candidates.append(xs)
    elif isinstance(default, str) and default.strip() and default.strip() not in junk:
        candidates.append(default.strip())

    display = (
        (candidates[0] if candidates else "")
        or lang_names.get("en")
        or lang_names.get("ar")
        or lang_names.get("fr")
        or ""
    )
    if not display and isinstance(default, list):
        toks = [
            x.strip()
            for x in default
            if isinstance(x, str) and x.strip() and x.strip() not in ({"", layer} | cats)
        ]
        if toks:
            display = max(toks, key=len)
    return display, lang_names


def pelias_to_unified(rec: dict) -> dict | None:
    """Normalise one Pelias ``{_id, _source}`` record to the unified schema."""
    src = rec.get("_source") or {}
    cp = src.get("center_point") or {}
    lat, lon = cp.get("lat"), cp.get("lon")
    if lat is None or lon is None:
        return None

    place_id = src.get("source_id") or ""
    display, lang_names = _pelias_name(src)
    parent = src.get("parent") or {}

    address_src = src.get("address") or {}
    formatted = address_src.get("default") or address_src.get("en") or address_src.get("ar") or ""
    if isinstance(formatted, list):
        formatted = next((str(x) for x in formatted if x), "")
    formatted = str(formatted).strip() if formatted else ""

    address = _empty_address()
    address["full"] = formatted
    address["suburb"] = _first(parent.get("neighbourhood")) or _first(parent.get("borough"))
    address["district"] = _first(parent.get("county"))
    address["city"] = _first(parent.get("locality")) or _first(parent.get("localadmin"))
    address["state"] = _first(parent.get("region")) or _first(parent.get("macroregion"))
    address["postcode"] = _first(parent.get("postalcode"))
    address["country"] = _first(parent.get("country"))
    address["country_code"] = _first(parent.get("country_a"))
    address["state_code"] = _first(parent.get("region_a"))
    if formatted:
        street = formatted.split(",")[0].strip()
        if street and street not in (address["city"], address["country"]):
            address["street"] = street

    extra: dict = {}
    if src.get("bounding_box"):
        extra["bounding_box"] = src["bounding_box"]
    wof = {k: v for k, v in parent.items() if k not in _PELIAS_PROMOTED_PARENT}
    if wof:
        extra["wof"] = wof
    if rec.get("_id"):
        extra["pelias_id"] = rec["_id"]

    return {
        "osm_id": place_osm_id(place_id, lat, lon, display),
        "source": GOOGLE_SOURCE,
        "source_id": place_id,
        "name": display,
        "names": lang_names,
        "lat": lat,
        "lon": lon,
        "layer": src.get("layer") or "",
        "categories": src.get("category") or [],
        "address": address,
        "phone": None,
        "rating": None,
        "extra": extra,
    }


# ── Postgres places: raw → unified ─────────────────────────────────────────
def _split_address(address: str) -> list[str]:
    """Split a (Saudi) formatted address on Arabic + Latin commas into segments."""
    return [p.strip() for p in re.split(r"[،,]", address) if p and p.strip()]


def pg_to_unified(row: dict) -> dict | None:
    """Normalise one Postgres ``places`` row to the unified schema."""
    lon, lat = row.get("lon"), row.get("lat")
    if lon is None or lat is None:
        return None

    pid = row.get("id")
    address_str = (row.get("address") or "").strip()
    address = _empty_address()
    address["full"] = address_str
    segs = _split_address(address_str)
    if segs:
        address["street"] = segs[0]
        address["country"] = segs[-1]
        for seg in segs:
            m = _POSTCODE_RE.search(seg)
            if m:
                address["postcode"] = m.group(0)
                city = _POSTCODE_RE.sub("", seg).strip()
                if city:
                    address["city"] = city
                break

    rating = row.get("rating")
    return {
        "osm_id": f"pg_{pid}",
        "source": PG_SOURCE,
        "source_id": "" if pid is None else str(pid),
        "name": (row.get("name") or "").strip(),
        "names": {},
        "lat": lat,
        "lon": lon,
        "layer": (row.get("layer") or "").strip(),
        "categories": row.get("categories") or [],
        "address": address,
        "phone": (row.get("phone") or "").strip() or None,
        "rating": rating,
        "extra": {},
    }


def to_unified(rec: dict) -> dict | None:
    """Normalise a raw record (Pelias or Postgres shape) to the unified schema.

    Records already in the unified shape (have ``osm_id`` + ``address`` dict)
    pass through unchanged, so re-running the converter is idempotent.
    """
    if not isinstance(rec, dict):
        return None
    if "osm_id" in rec and isinstance(rec.get("address"), dict):
        return rec  # already unified
    if "_source" in rec:
        return pelias_to_unified(rec)
    return pg_to_unified(rec)


# ── unified → OSM-element NATS message ─────────────────────────────────────
_ADDRESS_TAG = {
    "full": "addr:full",
    "street": "addr:street",
    "suburb": "addr:suburb",
    "district": "addr:district",
    "city": "addr:city",
    "state": "addr:state",
    "postcode": "addr:postcode",
    "country": "addr:country",
    "country_code": "addr:country_code",
    "state_code": "addr:state_code",
}


def place_to_element(p: dict) -> dict | None:
    """Map one unified place record to the watcher OSM-element NATS message."""
    lat, lon = p.get("lat"), p.get("lon")
    if lat is None or lon is None:
        return None

    source = p.get("source") or ""
    tags: dict[str, str] = {}
    if p.get("name"):
        tags["name"] = p["name"]
    for lang, val in (p.get("names") or {}).items():
        if val:
            tags[f"name:{lang}"] = val

    layer = p.get("layer") or ""
    cats = p.get("categories") or []
    feat = LAYER_FEATURE.get(layer)
    if feat is None:
        for c in cats:
            if c in CATEGORY_FEATURE:
                feat = CATEGORY_FEATURE[c]
                break
    if feat:
        tags.setdefault(feat[0], feat[1])

    admin_level = 0
    if layer in ADMIN_LAYER:
        admin_level, place_val = ADMIN_LAYER[layer]
        tags.setdefault("place", place_val)

    # Rescue districts the source mislabeled as generic POIs: an unclassified
    # generic-layer record whose name reads like a district/area gets a `place`
    # tag so it ranks as a neighbourhood instead of a bare 0.0-rank node.
    if feat is None and admin_level == 0 and layer in _GENERIC_LAYERS:
        place_val = _place_from_name(p.get("name") or "")
        if place_val:
            tags.setdefault("place", place_val)

    for key, val in (p.get("address") or {}).items():
        tag = _ADDRESS_TAG.get(key)
        if tag and val:
            tags[tag] = val

    if p.get("phone"):
        tags["phone"] = str(p["phone"])
    if p.get("rating") is not None:
        tags["rating"] = str(p["rating"])

    if source:
        tags["source"] = source
    source_id = p.get("source_id")
    if source_id:
        tags["ref:google_place_id" if source == GOOGLE_SOURCE else "ref:source_id"] = str(source_id)

    return {
        "osm_id": p["osm_id"],
        "osm_type": "node",
        "tags": tags,
        "geom": {"type": "Point", "coordinates": [lon, lat]},
        "admin_level": admin_level,
        "area_km2": 0.0,
    }
