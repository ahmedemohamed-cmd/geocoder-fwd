"""Stateless helpers for the geocoding service.

Pure functions extracted from ``geocoder.py``: confidence scoring, street-token
matching, Elasticsearch text-query builders, result shaping and geo math. None
of these touch the live clients (Elasticsearch / PostGIS / Redis / NATS), so they
import in isolation and are trivially unit-testable.
"""
import math
import re

from shared.address import (
    build_full_address,
    extract_address_components,
    normalize_address_text,
)
from shared.embeddings import build_text
from shared.interpolation import InterpolatedAddress
from shared.ranking import compute_offline_rank


def _normalize_confidence(score: float, max_score: float) -> float:
    """Normalize an Elasticsearch score to a 0.0-1.0 confidence value.

    Uses the max score in the result set as the reference point so the
    top result always gets 1.0 and others are proportional.
    """
    if max_score <= 0:
        return 0.0
    return round(min(score / max_score, 1.0), 4)


def _distance_confidence(distance_m: float) -> float:
    """Convert a distance in metres to a 0.0-1.0 confidence score.

    Follows the Pelias convention:
      <1 m   → 1.0
      1-10   → 0.9
      10-100 → 0.8
      100-250→ 0.7
      250-1k → 0.6
      1k-5k  → 0.4
      5k+    → 0.2
    """
    if distance_m < 1:
        return 1.0
    if distance_m < 10:
        return 0.9
    if distance_m < 100:
        return 0.8
    if distance_m < 250:
        return 0.7
    if distance_m < 1000:
        return 0.6
    if distance_m < 5000:
        return 0.4
    return 0.2


# Generic street-type tokens stripped before comparing street names, so that
# "Tahrir Street" vs "Tahrir St" still matches on the meaningful token "tahrir".
_STREET_GENERIC_TOKENS = {
    "street", "st", "road", "rd", "ave", "avenue", "alley", "lane", "drive", "dr",
    "square", "sq", "blvd", "boulevard",
    "شارع", "ش", "طريق", "حارة", "حاره", "زقاق", "ميدان", "كوبري",
}


def _street_token_match(parsed_street: str, addr_street: str) -> bool:
    """True if every meaningful token of ``parsed_street`` appears in ``addr_street``.

    Used to decide whether a result is genuinely on the requested street, so a
    housenumber match on the wrong street isn't treated as an exact address hit.
    Generic street-type words (Street, شارع, …) are ignored.
    """
    if not parsed_street:
        return True
    addr_low = (addr_street or "").lower()
    if not addr_low:
        return False
    tokens = [t for t in re.split(r"\W+", parsed_street.lower()) if t]
    core = [t for t in tokens if t not in _STREET_GENERIC_TOKENS] or tokens
    return all(t in addr_low for t in core)


def _interpolated_to_result(ia: InterpolatedAddress) -> dict:
    """Convert an InterpolatedAddress to a search-result dict."""
    return {
        "osm_id": ia.osm_id,
        "osm_type": "",
        "name": f"{ia.housenumber} {ia.street}",
        "name_en": "",
        "name_fr": "",
        "tags": {},
        "tags_text": "",
        "geom": {"type": "Point", "coordinates": [ia.lon, ia.lat]},
        "centroid": {"lat": ia.lat, "lon": ia.lon},
        "admin_level": 0,
        "area_km2": 0,
        "offline_rank": 0,
        "popularity": 0,
        "confidence": ia.confidence,
        "match_type": ia.match_type,
        "interpolation": {
            "side": ia.side,
            "bracket_low": ia.bracket_low,
            "bracket_high": ia.bracket_high,
        },
        "full_address": f"{ia.housenumber} {ia.street}, {ia.city}".strip(", "),
        "addr_housenumber": ia.housenumber,
        "addr_street": ia.street,
        "addr_city": ia.city,
        "addr_postcode": ia.postcode,
        "addr_country": ia.country,
        "addr_suburb": "",
        "addr_state": "",
        "address": None,
    }


def _text_should_full(q: str) -> list[dict]:
    """High-effort text recall.

    Broad fuzzy across all searchable fields (incl. edge-ngram autocomplete and
    tags_text) plus a closeness gradient — exact (0) > near-exact (≤1) > broad
    fuzzy — so typo / transliteration queries still rank the near-exact match
    first.  Lucene fuzzy is constant-score, so edit-distance sensitivity is
    recovered by stacking separate clauses per fuzziness level.  This matches
    the most candidates, and is the costlier path for ES to score.
    """
    return [
        # broad fuzzy recall (no boost — guarantees recall; ordering comes below)
        {"multi_match": {"query": q, "fields": [
            "name^5", "name.autocomplete^2", "name_en^5", "name_en.autocomplete^2",
            "name_fr^5", "name_fr.autocomplete^2", "tags_text"],
            "type": "best_fields", "fuzziness": "AUTO"}},
        # near-exact boost (edit distance ≤1, first char must match)
        {"multi_match": {"query": q, "fields": ["name^5", "name_en^5", "name_fr^5"],
            "type": "best_fields", "fuzziness": 1, "prefix_length": 1, "boost": 10}},
        # phrase boost: contiguous phrase
        {"multi_match": {"query": q, "fields": ["name", "name_en", "name_fr"],
            "type": "phrase", "boost": 10}},
        # exact all-tokens boost: every query word appears verbatim
        {"multi_match": {"query": q, "fields": ["name", "name_en", "name_fr"],
            "type": "best_fields", "operator": "and", "boost": 15}},
        # fuzzy all-tokens boost: keeps a precision boost for misspelled multiword
        {"multi_match": {"query": q, "fields": ["name^5", "name_en^5", "name_fr^5"],
            "type": "best_fields", "operator": "and", "fuzziness": "AUTO",
            "prefix_length": 1, "boost": 8}},
    ]


def _text_should_lean(q: str) -> list[dict]:
    """Optimized-effort text recall.

    Non-fuzzy across all fields (cheap), with fuzzy confined to the analyzed
    name fields under a tighter automaton (prefix_length 2, capped expansions).
    Dropping fuzzy from the edge-ngram autocomplete sub-fields and tags_text is
    what keeps the match set small — fuzzy expansion over those is the main
    driver of the huge candidate sets that stall the search thread pool.
    """
    return [
        {"multi_match": {"query": q, "fields": [
            "name^5", "name.autocomplete^2", "name_en^5", "name_en.autocomplete^2",
            "name_fr^5", "name_fr.autocomplete^2", "tags_text"],
            "type": "best_fields"}},
        {"multi_match": {"query": q, "fields": ["name^5", "name_en^5", "name_fr^5"],
            "type": "best_fields", "fuzziness": "AUTO", "prefix_length": 2,
            "max_expansions": 30, "boost": 4}},
        {"multi_match": {"query": q, "fields": ["name", "name_en", "name_fr"],
            "type": "phrase", "boost": 10}},
        {"multi_match": {"query": q, "fields": ["name", "name_en", "name_fr"],
            "type": "best_fields", "operator": "and", "boost": 15}},
    ]


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _element_to_geocode_result(extra: dict) -> dict:
    """Shape a mapped Google element exactly like a /geocode result row."""
    msg = extra["message"]
    tags = msg["tags"]
    admin_level = msg.get("admin_level", 0)
    area_km2 = msg.get("area_km2", 0.0)
    addr = extract_address_components(tags)
    full_addr = normalize_address_text(build_full_address(tags))
    return {
        "osm_id": msg["osm_id"],
        "osm_type": msg.get("osm_type", ""),
        "name": tags.get("name", ""),
        "name_en": tags.get("name:en", ""),
        "name_fr": tags.get("name:fr", ""),
        "tags": tags,
        "tags_text": build_text(tags),
        "geom": msg.get("geom"),
        "centroid": extra.get("centroid"),
        "admin_level": admin_level,
        "area_km2": area_km2,
        "offline_rank": compute_offline_rank(tags, admin_level, area_km2),
        "popularity": 0.0,
        "confidence": extra.get("confidence", 0.6),
        "full_address": full_addr,
        "addr_housenumber": addr.get("housenumber", ""),
        "addr_street":      addr.get("street", ""),
        "addr_city":        addr.get("city", ""),
        "addr_postcode":    addr.get("postcode", ""),
        "addr_country":     addr.get("country", ""),
        "addr_suburb":      addr.get("suburb", ""),
        "addr_state":       addr.get("state", ""),
        "address": None,
    }
