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
  9. POI type     – max of venue (amenity/shop/leisure/tourism/aeroway),
                    building, office, and a named-POI floor
 10. brand        – known brand presence

The final ``offline_rank`` is the weighted sum, kept as a positive float
so it can be used directly as a Typesense sort field and an ES boost.

Signals that don't apply to a given element (e.g. area for point features,
admin_level for non-boundary elements) are excluded from the normalisation
denominator so that points and venues are not systematically penalised.
"""

import math
import re
import tomllib
from pathlib import Path

# ── ranking specification ─────────────────────────────────────────────────
# Every weight and table below is loaded from spec/ranking.toml. That file is
# specification, not configuration: the numbers are tuning decisions that
# cannot be re-derived from the architecture, so they live in one declarative
# place a regenerated implementation can read instead of reinventing.
SPEC_PATH = Path(__file__).resolve().parent.parent / "spec" / "ranking.toml"

with SPEC_PATH.open("rb") as _fh:
    _SPEC = tomllib.load(_fh)

_W = _SPEC["weights"]
_S = _SPEC["scalars"]

# ── place-type importance (0..1) ──────────────────────────────────────────
_PLACE_SCORES: dict[str, float] = _SPEC["tables"]["place"]

# ── landuse importance (0..1) ─────────────────────────────────────────────
_LANDUSE_SCORES: dict[str, float] = _SPEC["tables"]["landuse"]

# ── venue importance (0..1) ───────────────────────────────────────────────
_VENUE_SCORES: dict[str, float] = _SPEC["tables"]["venue"]

# ── highway importance (0..1) ─────────────────────────────────────────────
_HIGHWAY_SCORES: dict[str, float] = _SPEC["tables"]["highway"]

# ── natural feature importance (0..1) ─────────────────────────────────────
_NATURAL_SCORES: dict[str, float] = _SPEC["tables"]["natural"]

_WATERWAY_SCORES: dict[str, float] = _SPEC["tables"]["waterway"]


# ── admin_level → score  (OSM admin_level: 2=country … 10=suburb) ─────────
def _admin_score(admin_level: int | None) -> float:
    # None means no admin_level tag — treat as a very high level (non-boundary).
    # Lower admin_level value = higher administrative rank (2=country, 10=suburb).
    if admin_level is None or admin_level <= 0:
        return 0.0
    # 2 → 1.0, 4 → 0.80, 6 → 0.60, 8 → 0.40, 10 → 0.20
    decay = (admin_level - _S["admin_base_level"]) * _S["admin_decay_per_level"]
    return min(1.0, max(0.0, 1.0 - decay))


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
    return min(1.0, math.log10(pop) / _S["population_log_divisor"])


def _area_score(area_km2: float) -> float:
    if area_km2 <= 0:
        return 0.0
    # log10(10000 km²)=4 → 1.0;  log10(0.01)=-2 → 0
    scaled = (math.log10(area_km2) + _S["area_log_offset"]) / _S["area_log_divisor"]
    return min(1.0, max(0.0, scaled))


def _metadata_score(tags: dict) -> float:
    score = 0.0
    if tags.get("wikidata"):
        score += _S["metadata_wikidata"]
    if tags.get("wikipedia"):
        score += _S["metadata_wikipedia"]
    return score


def _landuse_score(tags: dict) -> float:
    landuse = tags.get("landuse", "")
    return _LANDUSE_SCORES.get(landuse, 0.0)


def _venue_score(tags: dict) -> float:
    """Score based on venue tags (amenity, shop, leisure, tourism, aeroway)."""
    score = 0.0
    for key in _SPEC["keys"]["venue_tags"]:
        val = tags.get(key, "")
        if val:
            score = max(score, _VENUE_SCORES.get(val, 0.0))
    return score


_BUILDING_SCORES: dict[str, float] = _SPEC["tables"]["building"]


def _building_score(tags: dict) -> float:
    """Score based on building type.

    Returns 0.0 for generic/unknown buildings (e.g. ``building=yes``)
    to avoid adding noise from the millions of unclassified buildings.
    """
    building = tags.get("building", "")
    if not building:
        return 0.0
    return _BUILDING_SCORES.get(building, 0.0)


# ── office importance (0..1) ──────────────────────────────────────────────
# ``office=*`` venues (banks-as-offices, lawyers, estate agents, government
# offices, …) are real POIs but were previously unscored, so every office
# element landed at offline_rank 0.0.  Unknown office types get a small floor
# rather than 0 so a named office still outranks a bare node.
_OFFICE_SCORES: dict[str, float] = _SPEC["tables"]["office"]


def _office_score(tags: dict) -> float:
    """Score based on office type (falls back to a small floor for unknowns)."""
    office = tags.get("office", "")
    if not office:
        return 0.0
    return _OFFICE_SCORES.get(office, _S["office_unknown_floor"])


# ── named-POI floor ───────────────────────────────────────────────────────
# A named place we couldn't otherwise classify (e.g. a Google/Pelias record
# whose ``layer`` is a generic ``point_of_interest``/``venue``) still deserves
# a small nonzero rank so it isn't buried at 0.0 behind classified venues.
# Gated on evidence that it is a real curated/imported POI (a source/contact/
# rating/ref tag) so we don't lift every trivially-named bare OSM node.
_POI_EVIDENCE_KEYS = tuple(_SPEC["keys"]["poi_evidence"])
_NAMED_POI_FLOOR = _S["named_poi_floor"]


def _named_poi_score(tags: dict) -> float:
    """Small floor for a named POI carrying real-place evidence tags."""
    if not tags.get("name"):
        return 0.0
    if any(tags.get(k) for k in _POI_EVIDENCE_KEYS):
        return _NAMED_POI_FLOOR
    return 0.0


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
_BRAND_SCORES: dict[str, float] = _SPEC["tables"]["brand"]


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

    return _BRAND_SCORES.get(brand_clean, 0.0)


# ── weights ───────────────────────────────────────────────────────────────
# admin_level and area are the dominant signals for boundaries/polygons.
# W_POI merges venue + building via max() to avoid double-counting.
W_ADMIN = _W["admin"]
W_AREA = _W["area"]
W_PLACE = _W["place"]
W_POP = _W["population"]
W_HIGHWAY = _W["highway"]
W_NATURAL = _W["natural"]
W_META = _W["metadata"]
W_LANDUSE = _W["landuse"]
W_POI = _W["poi"]
W_BRAND = _W["brand"]

# Base weight total for signals that always participate in normalisation
_W_BASE = W_PLACE + W_POP + W_META + W_LANDUSE + W_POI + W_BRAND


def compute_offline_rank(tags: dict, admin_level: int | None, area_km2: float) -> float:
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
    if admin_level is not None and admin_level > 0:
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

    # POI signal: max of venue / building / office / named-POI floor
    # (max avoids double-counting; the floor keeps unclassified named POIs > 0)
    raw += W_POI * max(
        _venue_score(tags),
        _building_score(tags),
        _office_score(tags),
        _named_poi_score(tags),
    )

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
