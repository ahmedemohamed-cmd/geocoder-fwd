"""Google Maps Geocoding client + Google→OSM mapping.

Used by the geocoder's /deep/forward and /deep/reverse endpoints. Results from
the Google Geocoding API are mapped into the *exact* OSM-element message format
that watcher.py publishes to NATS, so the existing es-inserter / postgis-inserter
pipeline can index them with no special handling:

    {
        "osm_id":   "gmaps_<place_id>",
        "osm_type": "node",
        "tags":     {"name": ..., "addr:street": ..., "amenity": ..., ...},
        "geom":     {"type": "Point", "coordinates": [lon, lat]},
        "admin_level": 0,
        "area_km2": 0.0,
    }

The mapping deliberately emits OSM ``addr:*`` and feature tags (amenity/shop/
tourism/place/…) so that downstream address extraction, full-address building,
and offline-rank scoring all behave the same as for native OSM data.
"""

from __future__ import annotations

import hashlib

from shared.config import (
    GOOGLE_MAPS_API_KEY,
    GOOGLE_MAPS_GEOCODE_URL,
    GOOGLE_PLACES_SEARCH_TEXT_URL,
)


class GoogleMapsError(RuntimeError):
    """Raised when the Google Maps API is unavailable or returns an error."""


# ── Google address_component type → OSM addr:* tag ────────────────────────
# (first matching component wins; we read long_name except for country which
#  uses short_name to get the ISO 3166-1 code OSM expects).
_ADDR_COMPONENT_MAP = {
    "street_number": "addr:housenumber",
    "route": "addr:street",
    "locality": "addr:city",
    "postal_town": "addr:city",
    "sublocality": "addr:suburb",
    "sublocality_level_1": "addr:suburb",
    "neighborhood": "addr:suburb",
    "administrative_area_level_2": "addr:district",
    "administrative_area_level_1": "addr:state",
    "postal_code": "addr:postcode",
    "country": "addr:country",
}

# ── Google place type → OSM feature tag (key, value) ──────────────────────
_FEATURE_TYPE_MAP = {
    "restaurant": ("amenity", "restaurant"),
    "cafe": ("amenity", "cafe"),
    "bar": ("amenity", "bar"),
    "meal_takeaway": ("amenity", "fast_food"),
    "bank": ("amenity", "bank"),
    "atm": ("amenity", "atm"),
    "hospital": ("amenity", "hospital"),
    "doctor": ("amenity", "doctors"),
    "pharmacy": ("amenity", "pharmacy"),
    "dentist": ("amenity", "dentist"),
    "school": ("amenity", "school"),
    "university": ("amenity", "university"),
    "library": ("amenity", "library"),
    "place_of_worship": ("amenity", "place_of_worship"),
    "mosque": ("amenity", "place_of_worship"),
    "church": ("amenity", "place_of_worship"),
    "synagogue": ("amenity", "place_of_worship"),
    "police": ("amenity", "police"),
    "fire_station": ("amenity", "fire_station"),
    "post_office": ("amenity", "post_office"),
    "gas_station": ("amenity", "fuel"),
    "parking": ("amenity", "parking"),
    "embassy": ("amenity", "embassy"),
    "courthouse": ("amenity", "courthouse"),
    "city_hall": ("amenity", "townhall"),
    "lodging": ("tourism", "hotel"),
    "museum": ("tourism", "museum"),
    "tourist_attraction": ("tourism", "attraction"),
    "zoo": ("tourism", "zoo"),
    "art_gallery": ("tourism", "gallery"),
    "park": ("leisure", "park"),
    "stadium": ("leisure", "stadium"),
    "gym": ("leisure", "fitness_centre"),
    "shopping_mall": ("shop", "mall"),
    "supermarket": ("shop", "supermarket"),
    "department_store": ("shop", "department_store"),
    "convenience_store": ("shop", "convenience"),
    "bakery": ("shop", "bakery"),
    "clothing_store": ("shop", "clothes"),
    "store": ("shop", "yes"),
    "airport": ("aeroway", "aerodrome"),
    "subway_station": ("railway", "station"),
    "train_station": ("railway", "station"),
    "transit_station": ("public_transport", "station"),
    "bus_station": ("amenity", "bus_station"),
    # offices / finance (Google types) — mirror the curated LAYER_FEATURE so
    # deep-geocoded offices get a scored office=* tag instead of a bare node.
    "finance": ("amenity", "bank"),
    "local_government_office": ("office", "government"),
    "lawyer": ("office", "lawyer"),
    "accounting": ("office", "accountant"),
    "insurance_agency": ("office", "insurance"),
    "real_estate_agency": ("office", "estate_agent"),
    "travel_agency": ("shop", "travel_agency"),
}

# ── Google admin/place type → (OSM admin_level, place= value) ──────────────
_ADMIN_TYPE_MAP = {
    "country": (2, "country"),
    "administrative_area_level_1": (4, "state"),
    "administrative_area_level_2": (6, "county"),
    "administrative_area_level_3": (7, "region"),
    "administrative_area_level_4": (8, "city"),
    "locality": (8, "city"),
    "postal_town": (8, "town"),
    "sublocality": (10, "suburb"),
    "sublocality_level_1": (10, "suburb"),
    "sublocality_level_2": (10, "neighbourhood"),
    "sublocality_level_3": (10, "neighbourhood"),
    "neighborhood": (10, "neighbourhood"),
}

# Google location_type → confidence (Pelias-ish).
_LOCATION_TYPE_CONFIDENCE = {
    "ROOFTOP": 1.0,
    "RANGE_INTERPOLATED": 0.9,
    "GEOMETRIC_CENTER": 0.7,
    "APPROXIMATE": 0.6,
}


async def _request(params: dict, url: str = GOOGLE_MAPS_GEOCODE_URL) -> dict:
    """Call a Google Maps API (Geocoding or Places, same status contract) and
    return its parsed JSON, or raise."""
    import httpx  # lazy: keep the pure mapping helpers importable without httpx

    if not GOOGLE_MAPS_API_KEY:
        raise GoogleMapsError("GOOGLE_MAPS_API_KEY is not configured")
    params = {**params, "key": GOOGLE_MAPS_API_KEY}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPError as e:
        raise GoogleMapsError(f"Google Maps request failed: {e}") from e

    status = data.get("status")
    if status == "ZERO_RESULTS":
        return {"results": []}
    if status != "OK":
        raise GoogleMapsError(
            f"Google Maps API error: {status} {data.get('error_message', '')}".strip()
        )
    return data


async def forward_geocode(
    query: str, language: str, *, region: str | None = None, bounds: str | None = None
) -> list[dict]:
    """Forward geocode. ``language`` is required (drives result name language)."""
    params = {"address": query, "language": language}
    if region:
        params["region"] = region
    if bounds:
        params["bounds"] = bounds
    return (await _request(params)).get("results", [])


async def reverse_geocode(lat: float, lon: float, language: str) -> list[dict]:
    """Reverse geocode. ``language`` is required (drives result name language)."""
    return (await _request({"latlng": f"{lat},{lon}", "language": language})).get("results", [])


# Field mask for Places API (New) Text Search. Only these fields are returned
# (and billed for); `nextPageToken` is required for scrolling.
_PLACES_FIELD_MASK = ",".join(
    [
        "places.id",
        "places.displayName",
        "places.location",
        "places.types",
        "places.formattedAddress",
        "places.businessStatus",
        "places.rating",
        "places.userRatingCount",
        "nextPageToken",
    ]
)


async def _places_post(body: dict, field_mask: str) -> dict:
    """POST to the Places API (New) and return parsed JSON, or raise.

    Unlike the legacy Geocoding API (GET + a ``status`` field), the new API
    authenticates via the ``X-Goog-Api-Key`` header, takes a required
    ``X-Goog-FieldMask``, and signals errors with HTTP status codes.
    """
    import httpx  # lazy: keep the pure mapping helpers importable without httpx

    if not GOOGLE_MAPS_API_KEY:
        raise GoogleMapsError("GOOGLE_MAPS_API_KEY is not configured")
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_MAPS_API_KEY,
        "X-Goog-FieldMask": field_mask,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(GOOGLE_PLACES_SEARCH_TEXT_URL, json=body, headers=headers)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        detail = ""
        try:
            detail = e.response.json().get("error", {}).get("message", "")
        except Exception:
            detail = e.response.text[:200]
        raise GoogleMapsError(
            f"Google Places API error: {e.response.status_code} {detail}".strip()
        ) from e
    except httpx.HTTPError as e:
        raise GoogleMapsError(f"Google Places request failed: {e}") from e


async def nearby_search(
    lat: float,
    lon: float,
    radius: int,
    *,
    language: str,
    place_type: str | None = None,
    keyword: str | None = None,
    rankby: str = "prominence",
    page_size: int = 20,
    page_token: str | None = None,
) -> tuple[list[dict], str | None]:
    """Places API (New) Text Search biased around a point, with pagination.

    Text Search (not searchNearby) is used because only it returns a
    ``nextPageToken``. ``radius`` is applied as a **location bias** circle (the
    new API's Text Search restricts only to rectangles, so the radius is a
    preference, not a hard cut). Requires a ``place_type`` or ``keyword`` (the
    Text Search query). ``rankby=distance`` → nearest-first.

    Returns ``(places, next_page_token)``. When ``page_token`` is given, every
    other argument MUST match the original call (a Google requirement).
    """
    text_query = keyword or place_type
    if not text_query:
        raise GoogleMapsError("nearby_search requires a place_type or keyword")

    body: dict = {
        "textQuery": text_query,
        "languageCode": language,
        "pageSize": page_size,
        "locationBias": {
            "circle": {
                "center": {"latitude": lat, "longitude": lon},
                "radius": float(radius),
            }
        },
        "rankPreference": "DISTANCE" if rankby == "distance" else "RELEVANCE",
    }
    if place_type:
        body["includedType"] = place_type
    if page_token:
        body["pageToken"] = page_token

    data = await _places_post(body, _PLACES_FIELD_MASK)
    return data.get("places", []), data.get("nextPageToken")


def place_osm_id(place_id: str, lat, lon, name: str) -> str:
    """Deterministic, fixed-length osm_id derived from the Google place_id.

    A hash keeps the id short/clean and—crucially—stable, so requesting the same
    place again (e.g. in another language) maps to the SAME document and the
    inserter can merge the new names into it. Falls back to lat/lon/name when no
    place_id is present.
    """
    basis = place_id or f"{lat},{lon},{name}"
    return "g_" + hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]


def _derive_name(result: dict, tags: dict) -> str:
    """Pick a human name for the place from a Geocoding result.

    POIs/premises expose their name as the leading chunk of formatted_address;
    routes/localities use the matching component. Falls back to the first
    formatted_address segment.
    """
    types = set(result.get("types", []))
    comps = result.get("address_components", [])

    def comp(*wanted):
        for c in comps:
            if set(c.get("types", [])) & set(wanted):
                return c.get("long_name", "")
        return ""

    if types & {"route"}:
        return comp("route") or ""
    for t in (
        "locality",
        "administrative_area_level_2",
        "administrative_area_level_1",
        "country",
        "neighborhood",
        "sublocality",
    ):
        if t in types:
            return comp(t)
    # establishment / point_of_interest / premise / natural_feature / etc.
    formatted = result.get("formatted_address", "")
    return formatted.split(",")[0].strip() if formatted else tags.get("addr:street", "")


def map_result_to_element(result: dict, language: str) -> dict:
    """Map a Google Geocoding result into an OSM-element NATS message + extras.

    ``language`` is the language the result was requested in; the place's name is
    written both as the default ``name`` and as the language-specific
    ``name:<language>`` tag, so re-requesting the same place in another language
    accumulates ``name:ar`` / ``name:en`` / … on the same document.

    Returns a dict with:
      message       – the NATS payload (watcher format) to publish for indexing
      centroid      – {"lat", "lon"}
      confidence    – 0..1 derived from location_type / partial_match
      formatted_address, place_id
    """
    loc = (result.get("geometry") or {}).get("location") or {}
    lat, lon = loc.get("lat"), loc.get("lng")
    place_id = result.get("place_id", "")
    types = result.get("types", [])

    tags: dict[str, str] = {}

    # 1) structured address → addr:* tags
    for comp in result.get("address_components", []):
        ctypes = comp.get("types", [])
        for ct in ctypes:
            osm_key = _ADDR_COMPONENT_MAP.get(ct)
            if osm_key and osm_key not in tags:
                val = comp.get("short_name") if ct == "country" else comp.get("long_name")
                if val:
                    tags[osm_key] = val

    # 2) feature tag (amenity/shop/tourism/…) from the most specific place type
    for t in types:
        if t in _FEATURE_TYPE_MAP:
            k, v = _FEATURE_TYPE_MAP[t]
            tags.setdefault(k, v)
            break

    # 3) admin/place classification
    admin_level = 0
    for t in types:
        if t in _ADMIN_TYPE_MAP:
            admin_level, place_val = _ADMIN_TYPE_MAP[t]
            tags.setdefault("place", place_val)
            break

    # 4) name (default + language-specific) + provenance
    name = _derive_name(result, tags)
    if name:
        tags["name"] = name
        if language:
            tags[f"name:{language}"] = name
    tags["source"] = "google"
    if place_id:
        tags["ref:google_place_id"] = place_id

    message = {
        "osm_id": place_osm_id(place_id, lat, lon, name),
        "osm_type": "node",
        "tags": tags,
        "geom": {"type": "Point", "coordinates": [lon, lat]},
        "admin_level": admin_level,
        "area_km2": 0.0,
    }

    loc_type = (result.get("geometry") or {}).get("location_type", "")
    confidence = _LOCATION_TYPE_CONFIDENCE.get(loc_type, 0.6)
    if result.get("partial_match"):
        confidence = round(confidence * 0.8, 4)

    return {
        "message": message,
        "centroid": {"lat": lat, "lon": lon},
        "confidence": confidence,
        "formatted_address": result.get("formatted_address", ""),
        "place_id": place_id,
    }


def map_place_to_element(place: dict, language: str) -> dict:
    """Map a Places API (New) Text Search place into an OSM-element NATS message
    + extras (same shape as :func:`map_result_to_element`).

    The new API's schema is camelCase: ``id``, ``displayName.text``,
    ``location.latitude/longitude``, ``types``, ``formattedAddress``. The place
    already carries a name and (in this field mask) no structured components — so
    no ``addr:*`` tags are set; the local reverse-geocode enrichment fills
    nearest-street/parents once the place is indexed. ``formattedAddress`` is
    surfaced as ``formatted_address`` for the response only.
    """
    loc = place.get("location") or {}
    lat, lon = loc.get("latitude"), loc.get("longitude")
    place_id = place.get("id", "")
    types = place.get("types", [])
    name = (place.get("displayName") or {}).get("text", "")

    tags: dict[str, str] = {}

    # feature tag (amenity/shop/tourism/…) from the most specific place type
    for t in types:
        if t in _FEATURE_TYPE_MAP:
            k, v = _FEATURE_TYPE_MAP[t]
            tags.setdefault(k, v)
            break

    # admin/place classification (rare for POIs, kept for parity)
    admin_level = 0
    for t in types:
        if t in _ADMIN_TYPE_MAP:
            admin_level, place_val = _ADMIN_TYPE_MAP[t]
            tags.setdefault("place", place_val)
            break

    if name:
        tags["name"] = name
        if language:
            tags[f"name:{language}"] = name
    tags["source"] = "google"
    if place_id:
        tags["ref:google_place_id"] = place_id

    message = {
        "osm_id": place_osm_id(place_id, lat, lon, name),
        "osm_type": "node",
        "tags": tags,
        "geom": {"type": "Point", "coordinates": [lon, lat]},
        "admin_level": admin_level,
        "area_km2": 0.0,
    }

    # No location_type in Text Search; a permanently-closed place is low value.
    confidence = 0.6
    if place.get("businessStatus") in ("CLOSED_PERMANENTLY", "CLOSED_TEMPORARILY"):
        confidence = 0.3

    return {
        "message": message,
        "centroid": {"lat": lat, "lon": lon},
        "confidence": confidence,
        "formatted_address": place.get("formattedAddress", ""),
        "place_id": place_id,
        "rating": place.get("rating"),
        "user_ratings_total": place.get("userRatingCount"),
    }
