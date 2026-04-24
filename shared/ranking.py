"""Offline ranking – compute a static importance score for each OSM element.

Primary signals (dominant):
  1. admin_level  – lower level = more important boundary (country=2 .. suburb=10)
  2. area (km^2)  – log-scaled polygon area

Secondary signals (minor boost):
  3. place type   – city > town > village > hamlet > neighbourhood > ...
  4. population   – log-scaled
  5. metadata     – wikidata / wikipedia presence

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


# ── weights (admin_level and area are dominant) ───────────────────────────
W_ADMIN = 5.0
W_AREA = 4.0
W_PLACE = 2.0
W_POP = 1.5
W_META = 0.5
_W_TOTAL = W_ADMIN + W_PLACE + W_POP + W_AREA + W_META


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
    )
    # normalise to 0..10
    return round(raw / _W_TOTAL * 10.0, 4)
