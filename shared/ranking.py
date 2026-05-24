"""Offline ranking – compute a static importance score for each OSM element.

Primary signals (dominant):
  1. admin_level  – lower level = more important boundary (country=2 .. suburb=10)
  2. area (km²)   – log-scaled polygon area

Secondary signals (minor boost):
  3. place type   – city > town > village > hamlet > neighbourhood > ...
  4. population   – log-scaled
  5. highway      – motorway > primary > residential > footway > ...
  6. natural      – water / peak / coastline / wetland / ...
  7. metadata     – wikidata / wikipedia presence
  8. landuse      – residential > commercial > retail > industrial > ...
  9. POI type     – max of venue (amenity/shop/leisure/tourism/aeroway) and building
 10. brand        – known brand presence

The final ``offline_rank`` is the weighted sum, kept as a positive float
so it can be used directly as a Typesense sort field and an ES boost.

Signals that don't apply to a given element (e.g. area for point features,
admin_level for non-boundary elements) are excluded from the normalisation
denominator so that points and venues are not systematically penalised.
"""

import math
import re

# ── place-type importance (0..1) ──────────────────────────────────────────
_PLACE_SCORES: dict[str, float] = {
    "continent": 1.0,
    "country": 0.95,
    "state": 0.85,
    "region": 0.80,
    "province": 0.80,
    "city": 0.90,
    "town": 0.75,
    "village": 0.45,
    "suburb": 0.35,
    "hamlet": 0.30,
    "neighbourhood": 0.25,
    "quarter": 0.25,
    "borough": 0.35,
    "island": 0.50,
    "locality": 0.15,
    "isolated_dwelling": 0.10,
    "farm": 0.10,
}

# ── landuse importance (0..1) ─────────────────────────────────────────────
_LANDUSE_SCORES: dict[str, float] = {
    "residential": 0.70,
    "commercial": 0.65,
    "retail": 0.60,
    "industrial": 0.50,
    "education": 0.55,
    "healthcare": 0.60,
    "recreation_ground": 0.45,
    "park": 0.50,
    "garden": 0.40,
    "cemetery": 0.35,
    "forest": 0.30,
    "farmland": 0.25,
    "meadow": 0.25,
    "grass": 0.20,
    "construction": 0.30,
    "military": 0.20,
    "brownfield": 0.15,
    "greenfield": 0.15,
}

# ── venue importance (0..1) ───────────────────────────────────────────────
_VENUE_SCORES: dict[str, float] = {
    # amenity venues
    "restaurant": 0.80,
    "cafe": 0.75,
    "bar": 0.70,
    "pub": 0.70,
    "fast_food": 0.60,
    "food_court": 0.55,
    "cinema": 0.75,
    "theatre": 0.80,
    "arts_centre": 0.70,
    "music_venue": 0.75,
    "nightclub": 0.65,
    "library": 0.70,
    "museum": 0.80,
    "gallery": 0.65,
    "conference_centre": 0.75,
    "events_venue": 0.70,
    "hospital": 0.85,
    "clinic": 0.70,
    "pharmacy": 0.65,
    "doctors": 0.60,
    "dentist": 0.55,
    "veterinary": 0.50,
    "school": 0.70,
    "university": 0.80,
    "college": 0.70,
    "kindergarten": 0.60,
    "bank": 0.65,
    "atm": 0.50,
    "post_office": 0.60,
    "police": 0.70,
    "fire_station": 0.65,
    "townhall": 0.75,
    "courthouse": 0.70,
    "community_centre": 0.60,
    "place_of_worship": 0.65,
    "money_transfer": 0.50,
    "fuel": 0.60,
    "bus_station": 0.65,
    "taxi": 0.50,
    "embassy": 0.70,
    # aeroway venues
    "aerodrome": 0.90,
    "terminal": 0.75,
    # shop venues
    "supermarket": 0.75,
    "department_store": 0.70,
    "convenience": 0.60,
    "mall": 0.70,
    "marketplace": 0.65,
    "bakery": 0.55,
    "butcher": 0.50,
    "clothes": 0.55,
    "shoes": 0.50,
    "electronics": 0.60,
    "books": 0.55,
    "gift": 0.50,
    "jewelry": 0.55,
    "furniture": 0.55,
    "hardware": 0.55,
    "sports": 0.55,
    "toys": 0.50,
    "car": 0.55,
    "car_repair": 0.50,
    # leisure venues
    "sports_centre": 0.70,
    "stadium": 0.80,
    "swimming_pool": 0.65,
    "fitness_centre": 0.60,
    "golf_course": 0.60,
    "ice_rink": 0.55,
    "bowling_alley": 0.50,
    "park": 0.55,
    # tourism venues
    "hotel": 0.75,
    "hostel": 0.60,
    "motel": 0.55,
    "guest_house": 0.60,
    "apartment": 0.55,
    "camp_site": 0.50,
    "attraction": 0.70,
    "theme_park": 0.75,
    "zoo": 0.70,
    "aquarium": 0.65,
    "viewpoint": 0.50,
    "picnic_site": 0.45,
    "information": 0.40,
}

# ── highway importance (0..1) ─────────────────────────────────────────────
_HIGHWAY_SCORES: dict[str, float] = {
    "motorway": 0.90,
    "trunk": 0.80,
    "primary": 0.70,
    "secondary": 0.60,
    "tertiary": 0.50,
    "motorway_link": 0.45,
    "trunk_link": 0.40,
    "primary_link": 0.35,
    "unclassified": 0.30,
    "residential": 0.35,
    "living_street": 0.25,
    "service": 0.15,
    "pedestrian": 0.20,
    "track": 0.10,
    "footway": 0.10,
    "cycleway": 0.10,
    "path": 0.05,
}

# ── natural feature importance (0..1) ─────────────────────────────────────
_NATURAL_SCORES: dict[str, float] = {
    "coastline": 0.80,
    "water": 0.70,
    "peak": 0.70,
    "volcano": 0.75,
    "glacier": 0.65,
    "bay": 0.65,
    "beach": 0.60,
    "cliff": 0.50,
    "cave_entrance": 0.50,
    "wetland": 0.45,
    "wood": 0.30,
    "scrub": 0.15,
    "grassland": 0.20,
    "heath": 0.15,
    "sand": 0.15,
}

_WATERWAY_SCORES: dict[str, float] = {
    "river": 0.75,
    "canal": 0.60,
    "stream": 0.40,
    "drain": 0.20,
    "ditch": 0.10,
}


# ── admin_level → score  (OSM admin_level: 2=country … 10=suburb) ─────────
def _admin_score(admin_level: int) -> float:
    if admin_level <= 0:
        return 0.0
    # 2 → 1.0, 4 → 0.80, 6 → 0.60, 8 → 0.40, 10 → 0.20
    return min(1.0, max(0.0, 1.0 - (admin_level - 2) * 0.10))


def _place_score(tags: dict) -> float:
    place = tags.get("place", "")
    return _PLACE_SCORES.get(place, 0.0)


def _population_score(tags: dict) -> float:
    raw = tags.get("population", "")
    try:
        pop = int(str(raw).replace(",", "").replace(" ", ""))
    except (ValueError, TypeError):
        return 0.0
    if pop <= 0:
        return 0.0
    # log10(10M)=7 → score ~1.0;  log10(1000)=3 → ~0.43
    return min(1.0, math.log10(pop) / 7.0)


def _area_score(area_km2: float) -> float:
    if area_km2 <= 0:
        return 0.0
    # log10(10000 km²)=4 → 1.0;  log10(0.01)=-2 → 0
    return min(1.0, max(0.0, (math.log10(area_km2) + 2) / 6.0))


def _metadata_score(tags: dict) -> float:
    score = 0.0
    if tags.get("wikidata"):
        score += 0.5
    if tags.get("wikipedia"):
        score += 0.5
    return score


def _landuse_score(tags: dict) -> float:
    landuse = tags.get("landuse", "")
    return _LANDUSE_SCORES.get(landuse, 0.0)


def _venue_score(tags: dict) -> float:
    """Score based on venue tags (amenity, shop, leisure, tourism, aeroway)."""
    score = 0.0
    for key in ("amenity", "shop", "leisure", "tourism", "aeroway"):
        val = tags.get(key, "")
        if val:
            score = max(score, _VENUE_SCORES.get(val, 0.0))
    return score


def _building_score(tags: dict) -> float:
    """Score based on building type.

    Returns 0.0 for generic/unknown buildings (e.g. ``building=yes``)
    to avoid adding noise from the millions of unclassified buildings.
    """
    building = tags.get("building", "")
    if not building:
        return 0.0

    _BUILDING_SCORES: dict[str, float] = {
        "commercial": 0.65,
        "office": 0.60,
        "retail": 0.60,
        "supermarket": 0.70,
        "department_store": 0.65,
        "hotel": 0.70,
        "hospital": 0.80,
        "school": 0.65,
        "university": 0.75,
    }
    return _BUILDING_SCORES.get(building, 0.0)


def _highway_score(tags: dict) -> float:
    """Score based on highway type."""
    highway = tags.get("highway", "")
    return _HIGHWAY_SCORES.get(highway, 0.0)


def _natural_score(tags: dict) -> float:
    """Score based on natural feature or waterway type."""
    score = 0.0
    natural = tags.get("natural", "")
    if natural:
        score = max(score, _NATURAL_SCORES.get(natural, 0.0))
    waterway = tags.get("waterway", "")
    if waterway:
        score = max(score, _WATERWAY_SCORES.get(waterway, 0.0))
    return score


# Regex to strip non-alphanumeric characters for brand matching
_BRAND_CLEAN_RE = re.compile(r"[^a-z0-9]")


def _brand_score(tags: dict) -> float:
    """Score boost for known brands.

    Brand names are normalised (lowered, punctuation stripped) before
    lookup so that e.g. ``McDonald's`` matches the key ``mcdonalds``.
    """
    brand = tags.get("brand", "")
    if not brand:
        return 0.0
    brand_clean = _BRAND_CLEAN_RE.sub("", brand.lower())
    if not brand_clean:
        return 0.0

    major_brands = {
        "microsoft": 0.4,
        "apple": 0.4,
        "google": 0.4,
        "amazon": 0.4,
        "samsung": 0.35,
        "mcdonalds": 0.35,
        "starbucks": 0.35,
        "cocacola": 0.35,
        "pepsi": 0.35,
        "nike": 0.35,
        "adidas": 0.35,
    }
    return major_brands.get(brand_clean, 0.0)


# ── weights ───────────────────────────────────────────────────────────────
# admin_level and area are the dominant signals for boundaries/polygons.
# W_POI merges venue + building via max() to avoid double-counting.
W_ADMIN = 5.0
W_AREA = 4.0
W_PLACE = 2.5
W_POP = 1.5
W_HIGHWAY = 1.5
W_NATURAL = 1.0
W_META = 0.5
W_LANDUSE = 1.0
W_POI = 1.0
W_BRAND = 0.3

# Base weight total for signals that always participate in normalisation
_W_BASE = W_PLACE + W_POP + W_META + W_LANDUSE + W_POI + W_BRAND


def compute_offline_rank(tags: dict, admin_level: int, area_km2: float) -> float:
    """Return a positive float offline rank (higher = more important).

    Typical range: 0 (random POI) .. ~10 (major city / country).
    The result is scaled to 0..10 for readability.

    Signals that don't apply to an element (e.g. area for point features,
    admin_level for non-boundary elements) are excluded from the
    normalisation denominator so that points and venues are not
    systematically penalised.
    """
    raw = 0.0
    w_total = _W_BASE

    # Admin level (only counted when the element is an admin boundary)
    if admin_level > 0:
        raw += W_ADMIN * _admin_score(admin_level)
        w_total += W_ADMIN

    # Area (only counted for polygons with measurable area)
    if area_km2 > 0:
        raw += W_AREA * _area_score(area_km2)
        w_total += W_AREA

    # Always-applicable signals
    raw += W_PLACE * _place_score(tags)
    raw += W_POP * _population_score(tags)
    raw += W_META * _metadata_score(tags)
    raw += W_LANDUSE * _landuse_score(tags)
    raw += W_BRAND * _brand_score(tags)

    # POI signal: max of venue and building (avoids double-counting)
    raw += W_POI * max(_venue_score(tags), _building_score(tags))

    # Highway (only counted when tag is present)
    hw = _highway_score(tags)
    if hw > 0:
        raw += W_HIGHWAY * hw
        w_total += W_HIGHWAY

    # Natural feature (only counted when tag is present)
    nat = _natural_score(tags)
    if nat > 0:
        raw += W_NATURAL * nat
        w_total += W_NATURAL

    # Normalise to 0..10
    if w_total <= 0:
        return 0.0
    return round(raw / w_total * 10.0, 4)
