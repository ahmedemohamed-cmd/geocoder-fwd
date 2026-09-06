"""Offline ranking — a static importance score for each OSM element.

REGENERATED FROM spec/ranking.toml. Inferences the spec did not state are
marked [INFERRED]; each one is a gap in the specification, not a choice.

Model (from the spec header): each signal scores 0..1, is multiplied by its
weight, and the weighted sum is normalised by the total weight of the signals
that applied, then scaled to 0..10. Signals that do not apply to an element are
excluded from the denominator so points are not systematically penalised.
"""

from __future__ import annotations

import math
import re

from shared.spec import load as _load_spec

_SPEC = _load_spec("ranking.toml")
_W = _SPEC["weights"]
_S = _SPEC["scalars"]
_T = _SPEC["tables"]

_PLACE_SCORES: dict[str, float] = _T["place"]
_LANDUSE_SCORES: dict[str, float] = _T["landuse"]
_VENUE_SCORES: dict[str, float] = _T["venue"]
_HIGHWAY_SCORES: dict[str, float] = _T["highway"]
_NATURAL_SCORES: dict[str, float] = _T["natural"]
_WATERWAY_SCORES: dict[str, float] = _T["waterway"]
_BUILDING_SCORES: dict[str, float] = _T["building"]
_OFFICE_SCORES: dict[str, float] = _T["office"]
_BRAND_SCORES: dict[str, float] = _T["brand"]

_POI_EVIDENCE_KEYS = tuple(_SPEC["keys"]["poi_evidence"])
_VENUE_TAGS = tuple(_SPEC["keys"]["venue_tags"])
_NAMED_POI_FLOOR = _S["named_poi_floor"]

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

# [INFERRED] The spec names admin_level and area as conditional signals but does
# not say which others are. Assuming the remaining signals always participate.
_W_BASE = W_PLACE + W_POP + W_META + W_LANDUSE + W_POI + W_BRAND

_BRAND_CLEAN_RE = re.compile(r"[^a-z0-9]")


def _admin_score(admin_level: int | None) -> float:
    if admin_level is None or admin_level <= 0:
        return 0.0
    decay = (admin_level - _S["admin_base_level"]) * _S["admin_decay_per_level"]
    return min(1.0, max(0.0, 1.0 - decay))


def _place_score(tags: dict) -> float:
    return _PLACE_SCORES.get(tags.get("place", ""), 0.0)


def _population_score(tags: dict) -> float:
    raw = tags.get("population", "")
    try:
        # [INFERRED] The spec gives the formula but not the input cleaning;
        # thousands separators appear in OSM population tags.
        pop = int(str(raw).replace(",", "").replace(" ", ""))
    except (ValueError, TypeError):
        return 0.0
    if pop <= 0:
        return 0.0
    return min(1.0, math.log10(pop) / _S["population_log_divisor"])


def _area_score(area_km2: float) -> float:
    if area_km2 <= 0:
        return 0.0
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
    return _LANDUSE_SCORES.get(tags.get("landuse", ""), 0.0)


def _venue_score(tags: dict) -> float:
    score = 0.0
    for key in _VENUE_TAGS:
        val = tags.get(key, "")
        if val:
            score = max(score, _VENUE_SCORES.get(val, 0.0))
    return score


def _building_score(tags: dict) -> float:
    building = tags.get("building", "")
    if not building:
        return 0.0
    return _BUILDING_SCORES.get(building, 0.0)


def _office_score(tags: dict) -> float:
    office = tags.get("office", "")
    if not office:
        return 0.0
    return _OFFICE_SCORES.get(office, _S["office_unknown_floor"])


def _named_poi_score(tags: dict) -> float:
    if not tags.get("name"):
        return 0.0
    if any(tags.get(k) for k in _POI_EVIDENCE_KEYS):
        return _NAMED_POI_FLOOR
    return 0.0


def _highway_score(tags: dict) -> float:
    return _HIGHWAY_SCORES.get(tags.get("highway", ""), 0.0)


def _natural_score(tags: dict) -> float:
    # [INFERRED] The spec provides a waterway table and a single `natural`
    # weight but never says how the two combine. Assuming max().
    natural = _NATURAL_SCORES.get(tags.get("natural", ""), 0.0)
    waterway = _WATERWAY_SCORES.get(tags.get("waterway", ""), 0.0)
    return max(natural, waterway)


def _brand_score(tags: dict) -> float:
    brand = tags.get("brand", "")
    if not brand:
        return 0.0
    brand_clean = _BRAND_CLEAN_RE.sub("", brand.lower())
    if not brand_clean:
        return 0.0
    return _BRAND_SCORES.get(brand_clean, 0.0)


def compute_offline_rank(tags: dict, admin_level: int | None, area_km2: float) -> float:
    """Return a positive float importance score, scaled to 0..10."""
    raw = 0.0
    w_total = _W_BASE

    if admin_level is not None and admin_level > 0:
        raw += W_ADMIN * _admin_score(admin_level)
        w_total += W_ADMIN

    if area_km2 > 0:
        raw += W_AREA * _area_score(area_km2)
        w_total += W_AREA

    raw += W_PLACE * _place_score(tags)
    raw += W_POP * _population_score(tags)
    raw += W_META * _metadata_score(tags)
    raw += W_LANDUSE * _landuse_score(tags)
    raw += W_BRAND * _brand_score(tags)

    # The spec states poi merges venue/building/office/named-floor via max().
    raw += W_POI * max(
        _venue_score(tags),
        _building_score(tags),
        _office_score(tags),
        _named_poi_score(tags),
    )

    # [INFERRED] Treating highway and natural as conditional, by analogy with
    # the admin/area examples the spec gives.
    hw = _highway_score(tags)
    if hw > 0:
        raw += W_HIGHWAY * hw
        w_total += W_HIGHWAY

    nat = _natural_score(tags)
    if nat > 0:
        raw += W_NATURAL * nat
        w_total += W_NATURAL

    if w_total <= 0:
        return 0.0
    # [INFERRED] The spec says "scaled to 0..10" but not the rounding.
    return round(raw / w_total * 10.0, 4)
