"""Offline ranking – compute a static importance score for each OSM element.

Primary signals (dominant):
  1. admin_level  – lower level = more important boundary (country=2 .. suburb=10)
  2. area (km^2)  – log-scaled polygon area

Secondary signals (minor boost):
  3. place type   – city > town > village > hamlet > neighbourhood > ...
  4. population   – log-scaled
  5. metadata     – wikidata / wikipedia presence
  6. landuse      – residential > commercial > retail > industrial > ...
  7. venue type   – amenity/shop/leisure/tourism (restaurant, cinema, hotel, ...)

The final ``offline_rank`` is the weighted sum, kept as a positive float
so it can be used directly as a Typesense sort field and an ES boost.
"""

import math

# ── place-type importance (0..1) ──────────────────────────────────────────
_PLACE_SCORES: dict[str, float] = {
    "continent": 1.0,
    "country": 0.95,
    "state": 0.85,
    "region": 0.80,
    "province": 0.80,
    "city": 0.75,
    "town": 0.60,
    "village": 0.45,
    "hamlet": 0.30,
    "suburb": 0.35,
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
    # leisure venues
    "sports_centre": 0.70,
    "stadium": 0.80,
    "swimming_pool": 0.65,
    "fitness_centre": 0.60,
    "golf_course": 0.60,
    "ice_rink": 0.55,
    "bowling_alley": 0.50,
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
}

# ── admin_level → score  (OSM admin_level: 2=country … 10=suburb) ─────────
def _admin_score(admin_level: int) -> float:
    if admin_level <= 0:
        return 0.0
    # 2 → 1.0, 4 → 0.75, 6 → 0.50, 8 → 0.25, 10 → 0.10
    return max(0.0, 1.0 - (admin_level - 2) * 0.12)


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
    # log10(10000 km^2)=4 → 1.0;  log10(0.01)=-2 → 0
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
    """Score based on venue tags (amenity, shop, leisure, tourism)."""
    score = 0.0
    # Check amenity tag
    amenity = tags.get("amenity", "")
    if amenity:
        score = max(score, _VENUE_SCORES.get(amenity, 0.0))
    # Check shop tag
    shop = tags.get("shop", "")
    if shop:
        score = max(score, _VENUE_SCORES.get(shop, 0.0))
    # Check leisure tag
    leisure = tags.get("leisure", "")
    if leisure:
        score = max(score, _VENUE_SCORES.get(leisure, 0.0))
    # Check tourism tag
    tourism = tags.get("tourism", "")
    if tourism:
        score = max(score, _VENUE_SCORES.get(tourism, 0.0))
    return score


def _building_score(tags: dict) -> float:
    """Score based on building type."""
    building = tags.get("building", "")
    if not building:
        return 0.0
    
    # Commercial buildings get higher score
    commercial_buildings = {
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
    return commercial_buildings.get(building, 0.3)  # default low score for any building


def _brand_score(tags: dict) -> float:
    """Score boost for known brands."""
    brand = tags.get("brand", "").lower()
    if not brand:
        return 0.0
    
    # Major international brands get a boost
    major_brands = {
        "microsoft": 0.4,
        "apple": 0.4,
        "google": 0.4,
        "amazon": 0.4,
        "samsung": 0.35,
        "mcdonalds": 0.35,
        "starbucks": 0.35,
        "coca-cola": 0.35,
        "pepsi": 0.35,
        "nike": 0.35,
        "adidas": 0.35,
    }
    return major_brands.get(brand, 0.1)  # small boost for any brand


# ── weights (admin_level and area are dominant) ───────────────────────────
W_ADMIN = 5.0
W_AREA = 4.0
W_PLACE = 2.0
W_POP = 1.5
W_META = 0.5
W_LANDUSE = 1.0
W_VENUE = 0.7
W_BUILDING = 0.5
W_BRAND = 0.3
_W_TOTAL = W_ADMIN + W_PLACE + W_POP + W_AREA + W_META + W_LANDUSE + W_VENUE + W_BUILDING + W_BRAND


def compute_offline_rank(tags: dict, admin_level: int, area_km2: float) -> float:
    """Return a positive float offline rank (higher = more important).

    Typical range: 0 (random POI) .. ~10 (major city / country).
    The result is scaled to 0..10 for readability.
    admin_level and area_km2 are the dominant signals.
    """
    raw = (
        W_ADMIN * _admin_score(admin_level)
        + W_AREA * _area_score(area_km2)
        + W_PLACE * _place_score(tags)
        + W_POP * _population_score(tags)
        + W_META * _metadata_score(tags)
        + W_LANDUSE * _landuse_score(tags)
        + W_VENUE * _venue_score(tags)
        + W_BUILDING * _building_score(tags)
        + W_BRAND * _brand_score(tags)
    )
    # normalise to 0..10
    return round(raw / _W_TOTAL * 10.0, 4)
