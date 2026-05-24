"""Address extraction, normalization, and query-parsing utilities.

OSM encodes structured address information via ``addr:*`` tags.  This module
provides everything the pipeline needs to index and search addresses:

  - ``has_address(tags)``             → True when at least one addr: field present
  - ``extract_address_components()``  → dict of logical field → value
  - ``build_full_address(tags)``      → human-readable address string
  - ``is_address_query(q)``           → True when query looks like an address
  - ``parse_address_query(q)``        → structured dict from a free-form string

Supported OSM address tags
--------------------------
  addr:housenumber   Building/unit number
  addr:street        Street name
  addr:suburb        Suburb / neighborhood
  addr:district      District
  addr:city          City (falls back to addr:town / addr:village / addr:hamlet)
  addr:postcode      Postal / ZIP code
  addr:state         State / governorate
  addr:country       ISO 3166-1 country code (e.g. "EG")
  addr:place         Named place when no street exists
  addr:unit          Apartment / flat number
  addr:floor         Floor number
"""

import re

# ── OSM addr: tag → logical field name ───────────────────────────────────
ADDR_FIELD_MAP: dict[str, str] = {
    "housenumber": "addr:housenumber",
    "street":      "addr:street",
    "suburb":      "addr:suburb",
    "district":    "addr:district",
    "city":        "addr:city",
    "postcode":    "addr:postcode",
    "state":       "addr:state",
    "country":     "addr:country",
    "place":       "addr:place",
    "unit":        "addr:unit",
    "floor":       "addr:floor",
}

# City-level fallback order when addr:city is absent
_CITY_FALLBACKS = ("addr:city", "addr:town", "addr:village", "addr:hamlet")


# ── Component extraction ──────────────────────────────────────────────────

def has_address(tags: dict) -> bool:
    """Return True when *tags* contains at least one recognised addr:* field."""
    return any(tags.get(v) for v in ADDR_FIELD_MAP.values())


def extract_address_components(tags: dict) -> dict:
    """Extract OSM addr:* tags into a flat logical-field dict.

    Returns only the fields that are present and non-empty.
    """
    result: dict = {}
    for field, osm_key in ADDR_FIELD_MAP.items():
        val = tags.get(osm_key, "").strip()
        if val:
            result[field] = val

    # city fallback: try addr:town, addr:village, addr:hamlet
    if "city" not in result:
        for key in _CITY_FALLBACKS[1:]:
            val = tags.get(key, "").strip()
            if val:
                result["city"] = val
                break

    return result


def build_full_address(tags: dict) -> str:
    """Construct a normalised, human-readable address string from OSM tags.

    Format (each component only included when present)::

        [Unit X, ][Floor Y, ]housenumber street[, suburb][, district][, city]
        [, postcode][, state][, country]

    Returns an empty string when no address data is found.
    """
    addr = extract_address_components(tags)
    if not addr:
        return ""

    parts: list[str] = []

    if addr.get("unit"):
        parts.append(f"Unit {addr['unit']}")
    if addr.get("floor"):
        parts.append(f"Floor {addr['floor']}")

    # Street line
    housenumber = addr.get("housenumber", "")
    street      = addr.get("street", "")
    place       = addr.get("place", "")

    if housenumber and street:
        parts.append(f"{housenumber} {street}")
    elif housenumber:
        parts.append(housenumber)
    elif street:
        parts.append(street)
    elif place:
        parts.append(place)

    for field in ("suburb", "district", "city", "postcode", "state", "country"):
        if addr.get(field):
            parts.append(addr[field])

    return ", ".join(parts)


# ── Query detection & parsing ─────────────────────────────────────────────

# Street-type keywords — English + common Arabic equivalents
_STREET_RE = re.compile(
    r"\b(street|road|avenue|ave|blvd|boulevard|lane|drive|dr|place|pl|"
    r"court|ct|way|alley|square|sq|highway|hwy|st)\b"
    r"|شارع|طريق|ميدان|حارة|زقاق|كورنيش",
    re.IGNORECASE,
)


def is_address_query(q: str) -> bool:
    """Heuristically decide whether *q* looks like a structured address.

    Returns True when ANY of these conditions hold:

    1. Starts with a house-number token followed by words — ``"12B Main St"``
    2. Comma-separated with at least one numeric token — ``"Main St, Cairo, 11511"``
    3. Contains an explicit street-type keyword — ``"Tahrir Square"``
    """
    q = q.strip()
    if re.match(r"^\d[\w/-]*\s+\w", q):
        return True
    if "," in q and re.search(r"\b\d+\b", q):
        return True
    if _STREET_RE.search(q):
        return True
    return False


def parse_address_query(q: str) -> dict:
    """Parse a free-form address query into structured components.

    Supports formats like:

    * ``"123 Main Street, Cairo, 11511"``
    * ``"Main Street, Zamalek, Cairo"``
    * ``"شارع التحرير, القاهرة"``

    Returns a dict with any subset of:
      ``housenumber``, ``street``, ``city``, ``postcode``, ``state``, ``country``
    """
    parts = [p.strip() for p in q.split(",")]
    result: dict = {}

    if not parts:
        return result

    # First part: optional house-number + street
    first = parts[0]
    m = re.match(r"^(\d[\w/-]*)\s+(.+)", first)
    if m:
        result["housenumber"] = m.group(1).strip()
        result["street"]      = m.group(2).strip()
    else:
        result["street"] = first.strip()

    # Subsequent parts: city, postcode, state, country
    for i, part in enumerate(parts[1:], 1):
        part = part.strip()
        if not part:
            continue
        # Pure digits (4–6 chars) → postcode
        if re.match(r"^\d{4,6}$", part):
            result.setdefault("postcode", part)
        elif i == 1:
            result.setdefault("city", part)
        elif i == 2:
            result.setdefault("state", part)
        elif i == 3:
            result.setdefault("country", part)

    return result
