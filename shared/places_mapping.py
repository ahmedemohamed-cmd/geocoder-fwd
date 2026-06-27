"""Map curated ``places`` exports into the watcher OSM-element NATS format.

Two on-disk export shapes are supported, auto-detected per record:

  * **Pelias google export** — a list of ``{"_id", "_source"}`` objects taken
    from a Pelias Elasticsearch index (``source=google`` POIs). Detected by the
    presence of a ``_source`` key.

  * **Postgres ``places`` export** — a list of flat rows from the ``places``
    table (``{id, name, address, phone, rating, layer, categories, lon, lat}``).

Both map to the *exact* message format ``watcher.py`` / ``shared.google_maps``
publish, so the existing ``es_inserter`` / ``postgis_inserter`` consumers index
them with no special handling::

    {
        "osm_id":   "g_<sha1(place_id)>"  |  "pg_<id>",
        "osm_type": "node",
        "tags":     {"name", "name:*", "addr:*", "amenity"/"shop"/..., "source", ...},
        "geom":     {"type": "Point", "coordinates": [lon, lat]},
        "admin_level": 0,
        "area_km2": 0.0,
    }

This module is the single source of truth for that mapping, shared by the
``places-watcher`` service and the ``scripts/import_*`` one-off importers.
"""

from __future__ import annotations

import re

from shared.google_maps import place_osm_id

# ── Pelias google: layer / category → OSM feature tag ──────────────────────
_PELIAS_LAYER_FEATURE = {
    "restaurant": ("amenity", "restaurant"),
    "food": ("amenity", "restaurant"),
    "meal_takeaway": ("amenity", "fast_food"),
    "meal_delivery": ("amenity", "fast_food"),
    "cafe": ("amenity", "cafe"),
    "bar": ("amenity", "bar"),
    "night_club": ("amenity", "nightclub"),
    "bakery": ("shop", "bakery"),
    "bank": ("amenity", "bank"),
    "atm": ("amenity", "atm"),
    "finance": ("amenity", "bank"),
    "accounting": ("office", "accountant"),
    "insurance_agency": ("office", "insurance"),
    "hospital": ("amenity", "hospital"),
    "health": ("amenity", "clinic"),
    "doctor": ("amenity", "doctors"),
    "physiotherapist": ("amenity", "doctors"),
    "pharmacy": ("amenity", "pharmacy"),
    "drugstore": ("amenity", "pharmacy"),
    "dentist": ("amenity", "dentist"),
    "veterinary_care": ("amenity", "veterinary"),
    "school": ("amenity", "school"),
    "primary_school": ("amenity", "school"),
    "secondary_school": ("amenity", "school"),
    "university": ("amenity", "university"),
    "library": ("amenity", "library"),
    "mosque": ("amenity", "place_of_worship"),
    "church": ("amenity", "place_of_worship"),
    "synagogue": ("amenity", "place_of_worship"),
    "hindu_temple": ("amenity", "place_of_worship"),
    "place_of_worship": ("amenity", "place_of_worship"),
    "police": ("amenity", "police"),
    "fire_station": ("amenity", "fire_station"),
    "post_office": ("amenity", "post_office"),
    "local_government_office": ("office", "government"),
    "city_hall": ("amenity", "townhall"),
    "courthouse": ("amenity", "courthouse"),
    "embassy": ("amenity", "embassy"),
    "gas_station": ("amenity", "fuel"),
    "parking": ("amenity", "parking"),
    "car_repair": ("shop", "car_repair"),
    "car_dealer": ("shop", "car"),
    "car_wash": ("amenity", "car_wash"),
    "car_rental": ("amenity", "car_rental"),
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
    "shoe_store": ("shop", "shoes"),
    "jewelry_store": ("shop", "jewelry"),
    "book_store": ("shop", "books"),
    "florist": ("shop", "florist"),
    "bicycle_store": ("shop", "bicycle"),
    "pet_store": ("shop", "pet"),
    "liquor_store": ("shop", "alcohol"),
    "beauty_salon": ("shop", "beauty"),
    "hair_care": ("shop", "hairdresser"),
    "store": ("shop", "yes"),
    "travel_agency": ("shop", "travel_agency"),
    "real_estate_agency": ("office", "estate_agent"),
    "lawyer": ("office", "lawyer"),
    "airport": ("aeroway", "aerodrome"),
    "subway_station": ("railway", "station"),
    "train_station": ("railway", "station"),
    "transit_station": ("public_transport", "station"),
    "bus_station": ("amenity", "bus_station"),
}

_PELIAS_CATEGORY_FEATURE = {
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
    "transport": ("public_transport", "station"),
    "nature": ("leisure", "park"),
    "historic": ("historic", "yes"),
}

# Pelias admin layers → (OSM admin_level, place= value); POIs stay at level 0.
_PELIAS_ADMIN_LAYER = {
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

# ── Postgres places: layer / category → OSM feature tag ────────────────────
PG_SOURCE = "places_pg"

_PG_LAYER_FEATURE = {
    "restaurant": ("amenity", "restaurant"),
    "cafe": ("amenity", "cafe"),
    "hospital": ("amenity", "hospital"),
    "pharmacy": ("amenity", "pharmacy"),
    "doctors": ("amenity", "doctors"),
    "bank": ("amenity", "bank"),
    "atm": ("amenity", "atm"),
    "fuel": ("amenity", "fuel"),
    "gym": ("leisure", "fitness_centre"),
    "supermarket": ("shop", "supermarket"),
    "clothes": ("shop", "clothes"),
    "beauty": ("shop", "beauty"),
    "hairdresser": ("shop", "hairdresser"),
    "car": ("shop", "car"),
    "car_repair": ("shop", "car_repair"),
    "car_wash": ("amenity", "car_wash"),
    "airport": ("aeroway", "aerodrome"),
    "office": ("office", "yes"),
    "government": ("office", "government"),
}

_PG_CATEGORY_FEATURE = {
    "food": ("amenity", "restaurant"),
    "health": ("amenity", "clinic"),
    "shop": ("shop", "yes"),
    "finance": ("amenity", "bank"),
    "transport": ("amenity", "fuel"),
    "transportation": ("aeroway", "aerodrome"),
    "government": ("office", "government"),
    "professional": ("office", "yes"),
}

_POSTCODE_RE = re.compile(r"\b\d{5}\b")


# ── Pelias google mapping ──────────────────────────────────────────────────
def _first(val) -> str:
    """Pelias parent fields are single-element lists; unwrap to a clean str."""
    if isinstance(val, list):
        val = val[0] if val else None
    return "" if val is None else str(val).strip()


def _pelias_name(src: dict) -> tuple[str, dict[str, str]]:
    """Pull a clean display name + per-language name tags from a Pelias record.

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


def map_pelias_google(rec: dict) -> dict | None:
    """Map one Pelias ``{_id, _source}`` record to an OSM-element message.

    The ``osm_id`` is derived from the Google place_id the same way the live
    ``/deep/*`` endpoints derive it, so re-importing or later deep-searching the
    same place merges onto the same document. Returns ``None`` if unplaceable.
    """
    src = rec.get("_source") or {}
    cp = src.get("center_point") or {}
    lat, lon = cp.get("lat"), cp.get("lon")
    if lat is None or lon is None:
        return None

    place_id = src.get("source_id") or ""
    layer = src.get("layer") or ""
    cats = src.get("category") or []

    display_name, lang_names = _pelias_name(src)

    tags: dict[str, str] = {}
    if display_name:
        tags["name"] = display_name
    for lang, val in lang_names.items():
        tags[f"name:{lang}"] = val

    feat = _PELIAS_LAYER_FEATURE.get(layer)
    if feat is None:
        for c in cats:
            if c in _PELIAS_CATEGORY_FEATURE:
                feat = _PELIAS_CATEGORY_FEATURE[c]
                break
    if feat:
        tags.setdefault(feat[0], feat[1])

    admin_level = 0
    if layer in _PELIAS_ADMIN_LAYER:
        admin_level, place_val = _PELIAS_ADMIN_LAYER[layer]
        tags.setdefault("place", place_val)

    parent = src.get("parent") or {}
    addr_map = {
        "addr:country": _first(parent.get("country")),
        "addr:state": _first(parent.get("region")) or _first(parent.get("macroregion")),
        "addr:city": _first(parent.get("locality")) or _first(parent.get("localadmin")),
        "addr:district": _first(parent.get("county")),
        "addr:suburb": _first(parent.get("neighbourhood")) or _first(parent.get("borough")),
        "addr:postcode": _first(parent.get("postalcode")),
    }
    for k, v in addr_map.items():
        if v:
            tags[k] = v

    address = src.get("address") or {}
    formatted = address.get("default") or address.get("en") or address.get("ar") or ""
    if isinstance(formatted, list):
        formatted = next((str(x) for x in formatted if x), "")
    if formatted:
        formatted = str(formatted)
        street = formatted.split(",")[0].strip()
        if street and street not in (tags.get("addr:city"), tags.get("addr:country")):
            tags.setdefault("addr:street", street)
        tags.setdefault("addr:full", formatted.strip())

    tags["source"] = "google"
    if place_id:
        tags["ref:google_place_id"] = place_id

    return {
        "osm_id": place_osm_id(place_id, lat, lon, display_name),
        "osm_type": "node",
        "tags": tags,
        "geom": {"type": "Point", "coordinates": [lon, lat]},
        "admin_level": admin_level,
        "area_km2": 0.0,
    }


# ── Postgres places mapping ────────────────────────────────────────────────
def _split_address(address: str) -> list[str]:
    """Split a (Saudi) formatted address on Arabic + Latin commas into segments."""
    return [p.strip() for p in re.split(r"[،,]", address) if p and p.strip()]


def map_pg_place(row: dict) -> dict | None:
    """Map one Postgres ``places`` row to an OSM-element message (or None)."""
    lon, lat = row.get("lon"), row.get("lat")
    if lon is None or lat is None:
        return None

    pid = row.get("id")
    name = (row.get("name") or "").strip()
    layer = (row.get("layer") or "").strip()
    cats = row.get("categories") or []

    tags: dict[str, str] = {}
    if name:
        tags["name"] = name

    feat = _PG_LAYER_FEATURE.get(layer)
    if feat is None:
        for c in cats:
            if c in _PG_CATEGORY_FEATURE:
                feat = _PG_CATEGORY_FEATURE[c]
                break
    if feat:
        tags.setdefault(feat[0], feat[1])

    address = (row.get("address") or "").strip()
    if address:
        tags["addr:full"] = address
        segs = _split_address(address)
        if segs:
            tags.setdefault("addr:street", segs[0])
            tags.setdefault("addr:country", segs[-1])
        for seg in segs:
            m = _POSTCODE_RE.search(seg)
            if m:
                tags.setdefault("addr:postcode", m.group(0))
                city = _POSTCODE_RE.sub("", seg).strip()
                if city:
                    tags.setdefault("addr:city", city)
                break

    phone = (row.get("phone") or "").strip()
    if phone:
        tags["phone"] = phone
    rating = row.get("rating")
    if rating is not None:
        tags["rating"] = str(rating)

    tags["source"] = PG_SOURCE
    if pid is not None:
        tags["ref:places_id"] = str(pid)

    return {
        "osm_id": f"pg_{pid}",
        "osm_type": "node",
        "tags": tags,
        "geom": {"type": "Point", "coordinates": [lon, lat]},
        "admin_level": 0,
        "area_km2": 0.0,
    }


def map_record(rec: dict) -> dict | None:
    """Dispatch a record to the right mapper by its on-disk shape."""
    if isinstance(rec, dict) and "_source" in rec:
        return map_pelias_google(rec)
    return map_pg_place(rec)
