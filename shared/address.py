"""Address extraction, normalization, and query-parsing utilities.

OSM encodes structured address information via ``addr:*`` tags.  This module
provides everything the pipeline needs to index and search addresses:

  - ``has_address(tags)``             → True when at least one addr: field present
  - ``extract_address_components()``  → dict of logical field → value
  - ``build_full_address(tags)``      → human-readable address string
  - ``is_address_query(q)``           → True when query looks like an address
  - ``parse_address_query(q)``        → structured dict from a free-form string
  - ``normalize_address_text(s)``     → expand abbreviations, normalize whitespace

Multilingual support: Arabic, English, and French.

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

# ── Abbreviation / synonym expansion (English) ───────────────────────────
_EN_ABBREVS: dict[str, str] = {
    "st":    "street",
    "str":   "street",
    "rd":    "road",
    "ave":   "avenue",
    "av":    "avenue",
    "blvd":  "boulevard",
    "bvd":   "boulevard",
    "ln":    "lane",
    "dr":    "drive",
    "pl":    "place",
    "ct":    "court",
    "sq":    "square",
    "hwy":   "highway",
    "cres":  "crescent",
    "terr":  "terrace",
    "pkwy":  "parkway",
}

# ── Arabic street-type normalizations ─────────────────────────────────────
# Maps common short/variant Arabic street prefixes to canonical forms
_AR_STREET_TYPES: dict[str, str] = {
    "ش":     "شارع",
    "ش.":    "شارع",
    "شار":   "شارع",
    "ط":     "طريق",
    "م":     "ميدان",
}

# Arabic tokens that indicate a street/address context
_AR_STREET_KEYWORDS = frozenset([
    "شارع", "طريق", "ميدان", "حارة", "زقاق", "كورنيش", "حي",
    "منطقة", "مدينة", "محافظة", "عمارة", "برج", "مبنى", "عمائر",
])

# Arabic city/area names for detection (common ones)
# We store both original and normalized forms to match regardless of normalization
_AR_CITY_KEYWORDS_RAW = [
    "القاهرة", "الجيزة", "الاسكندرية", "الإسكندرية", "المنصورة",
    "الزمالك", "المعادي", "مصر الجديدة", "مدينة نصر", "المهندسين",
    "الدقي", "العجوزة", "شبرا", "حلوان", "المقطم", "التجمع",
    "الرحاب", "العبور", "أكتوبر", "الشيخ زايد",
]

# ── French street-type normalizations ─────────────────────────────────────
_FR_STREET_TYPES: dict[str, str] = {
    "r":     "rue",
    "r.":    "rue",
    "av":    "avenue",
    "av.":   "avenue",
    "bd":    "boulevard",
    "bd.":   "boulevard",
    "blvd":  "boulevard",
    "pl":    "place",
    "pl.":   "place",
    "ch":    "chemin",
    "ch.":   "chemin",
    "imp":   "impasse",
    "imp.":  "impasse",
    "all":   "allée",
    "all.":  "allée",
    "crs":   "cours",
    "crs.":  "cours",
    "rte":   "route",
    "rte.":  "route",
    "pass":  "passage",
    "pass.": "passage",
}

# French tokens that indicate a street/address context
_FR_STREET_KEYWORDS = frozenset([
    "rue", "avenue", "boulevard", "place", "square", "chemin",
    "impasse", "allée", "cour", "cours", "passage", "quai",
    "route", "rond-point", "carrefour", "voie",
])

# French city/area names for detection (common ones)
_FR_CITY_KEYWORDS_RAW = [
    "Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Nantes",
    "Strasbourg", "Montpellier", "Bordeaux", "Lille", "Rennes",
    "Reims", "Toulon", "Grenoble", "Dijon", "Angers", "Nîmes",
    "Casablanca", "Rabat", "Marrakech", "Fès", "Tanger", "Meknès",
    "Tunis", "Sfax", "Sousse", "Alger", "Oran", "Constantine",
    "Dakar", "Abidjan", "Douala", "Yaoundé", "Kinshasa",
    "Bruxelles", "Genève", "Lausanne", "Montréal", "Québec",
]

# English tokens that indicate a street/address context
_EN_STREET_KEYWORDS = frozenset([
    "street", "road", "avenue", "boulevard", "lane", "drive",
    "place", "court", "square", "highway", "crescent", "terrace",
    "parkway", "way", "alley", "circle", "trail",
])

# English city/area names for detection (common ones)
_EN_CITY_KEYWORDS_RAW = [
    "Cairo", "Giza", "Alexandria", "Luxor", "Aswan", "Hurghada",
    "Sharm El Sheikh", "Mansoura", "Zamalek", "Maadi", "Heliopolis",
    "Nasr City", "Mohandessin", "Dokki", "Agouza", "Helwan",
    "New Cairo", "6th of October", "Sheikh Zayed",
]


_RE_ALEF_VARIANTS = re.compile(r"[إأآا]")
_RE_WHITESPACE = re.compile(r"\s+")
_RE_EN_ABBREVS = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in _EN_ABBREVS) + r")\b",
    re.IGNORECASE,
)
_RE_FR_ABBREVS = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in _FR_STREET_TYPES) + r")\b",
    re.IGNORECASE,
)


def _normalize_ar(s: str) -> str:
    """Light Arabic normalization for keyword matching (no abbreviation expansion)."""
    s = s.replace("\u0640", "")
    s = _RE_ALEF_VARIANTS.sub("ا", s)
    s = s.replace("ى", "ي")
    s = s.replace("ة", "ه")
    return s


# Build keyword set with both original and normalized forms
_AR_CITY_KEYWORDS = frozenset(
    _AR_CITY_KEYWORDS_RAW + [_normalize_ar(c) for c in _AR_CITY_KEYWORDS_RAW]
)

# Build French city keyword set (case-insensitive matching via lowercased set)
_FR_CITY_KEYWORDS = frozenset(
    _FR_CITY_KEYWORDS_RAW + [c.lower() for c in _FR_CITY_KEYWORDS_RAW]
)

# Build English city keyword set (case-insensitive matching via lowercased set)
_EN_CITY_KEYWORDS = frozenset(
    _EN_CITY_KEYWORDS_RAW + [c.lower() for c in _EN_CITY_KEYWORDS_RAW]
)

# Combined city keywords for all languages
_ALL_CITY_KEYWORDS = _AR_CITY_KEYWORDS | _FR_CITY_KEYWORDS | _EN_CITY_KEYWORDS


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


# ── Text normalization ────────────────────────────────────────────────────

def normalize_address_text(s: str) -> str:
    """Expand abbreviations, normalize Arabic variants, collapse whitespace.

    Used at both index-time (full_address) and query-time to ensure
    abbreviations in the query match expanded forms in the index.
    Supports Arabic, English, and French abbreviations.
    """
    if not s:
        return s

    # Normalize Arabic characters: remove tatweel, normalize alef/yaa
    s = s.replace("\u0640", "")              # tatweel
    s = _RE_ALEF_VARIANTS.sub("ا", s)        # normalize alef variants → bare alef
    s = s.replace("ى", "ي")                  # alef maqsura → yaa
    s = s.replace("ة", "ه")                  # taa marbuta → haa (common search behaviour)

    # Expand Arabic abbreviated street types
    tokens = s.split()
    for i, tok in enumerate(tokens):
        canonical = _AR_STREET_TYPES.get(tok)
        if canonical:
            tokens[i] = canonical
    s = " ".join(tokens)

    # Expand English abbreviations (word-boundary aware)
    def _expand_en(m: re.Match) -> str:
        return _EN_ABBREVS.get(m.group(0).lower(), m.group(0))

    s = _RE_EN_ABBREVS.sub(_expand_en, s)

    # Expand French abbreviations (word-boundary aware)
    def _expand_fr(m: re.Match) -> str:
        return _FR_STREET_TYPES.get(m.group(0).lower(), m.group(0))

    s = _RE_FR_ABBREVS.sub(_expand_fr, s)

    # Collapse multiple spaces
    s = _RE_WHITESPACE.sub(" ", s).strip()
    return s


# ── Query detection & parsing ─────────────────────────────────────────────

# Street-type keywords — English + Arabic + French
_STREET_RE = re.compile(
    r"\b(street|road|avenue|ave|blvd|boulevard|lane|drive|dr|place|pl|"
    r"court|ct|way|alley|square|sq|highway|hwy|st|crescent|terrace|parkway"
    r"|rue|chemin|impasse|allée|cour|cours|passage|quai|route|voie"
    r"|rond-point|carrefour)\b"
    r"|شارع|طريق|ميدان|حارة|زقاق|كورنيش|حي|منطقة|ش\s",
    re.IGNORECASE,
)

# Postcode pattern: 4-6 digits (standalone)
_POSTCODE_RE = re.compile(r"^\d{4,6}$")

# House-number pattern: digits optionally followed by a letter or slash-suffix
_HOUSENUMBER_RE = re.compile(r"^\d[\w/-]*$")


def is_address_query(q: str) -> bool:
    """Heuristically decide whether *q* looks like a structured address.

    Returns True when ANY of these conditions hold:

    1. Starts with a house-number token followed by words — ``"12B Main St"``
    2. Contains comma-separated parts (structured input) — ``"Main St, Cairo"``
    3. Contains a street-type keyword (English, Arabic, or French) — ``"Tahrir Square"``
    4. Contains address-related keywords in any supported language
    5. Contains a postcode-like token within a longer string
    """
    q = q.strip()
    if not q:
        return False
    # Starts with housenumber + words
    if re.match(r"^\d[\w/-]*\s+\w", q):
        return True
    # Has commas (structured address)
    if "," in q:
        return True
    # Contains street-type keyword (English, Arabic, or French)
    if _STREET_RE.search(q):
        return True
    # Check keywords in all languages
    for tok in q.split():
        if tok in _AR_STREET_KEYWORDS or tok in _AR_CITY_KEYWORDS:
            return True
        tok_lower = tok.lower()
        if tok_lower in _FR_STREET_KEYWORDS or tok_lower in _FR_CITY_KEYWORDS:
            return True
        if tok_lower in _EN_STREET_KEYWORDS or tok_lower in _EN_CITY_KEYWORDS:
            return True
    return False


def _detect_arabic_street(text: str) -> tuple[str, str]:
    """Try to split an Arabic text into (street_name, area/city).

    Arabic addresses commonly follow the pattern:
      شارع <name>            → street only
      شارع <name>, <city>    → already comma-separated (handled elsewhere)
      <name> - <area>        → dash separator

    Returns (street, city) — either may be empty.
    """
    # Pattern: شارع/طريق/ميدان + name (the street keyword + everything after)
    m = re.match(r"^(شارع|طريق|ميدان|حارة|كورنيش|ش)\s+(.+)", text.strip())
    if m:
        street_part = f"{m.group(1)} {m.group(2)}"
        # Check if a known city/area appears at the end
        for city in _AR_CITY_KEYWORDS:
            if street_part.endswith(city):
                street_name = street_part[: -len(city)].strip()
                return street_name, city
        return street_part, ""

    return text.strip(), ""


def _detect_french_street(text: str) -> tuple[str, str]:
    """Try to split a French text into (street_name, area/city).

    French addresses commonly follow the pattern:
      rue <name>            → street only
      rue <name>, <city>    → already comma-separated (handled elsewhere)
      avenue <name>         → street only

    Returns (street, city) — either may be empty.
    """
    fr_prefixes = "|".join(re.escape(k) for k in sorted(_FR_STREET_KEYWORDS, key=len, reverse=True))
    m = re.match(rf"^({fr_prefixes})\s+(.+)", text.strip(), re.IGNORECASE)
    if m:
        street_part = f"{m.group(1)} {m.group(2)}"
        # Check if a known city appears at the end
        for city in _FR_CITY_KEYWORDS:
            if street_part.lower().endswith(city.lower()):
                street_name = street_part[: -len(city)].strip()
                return street_name, city
        return street_part, ""

    return text.strip(), ""


def parse_address_query(q: str) -> dict:
    """Parse a free-form address query into structured components.

    Supports formats like:

    * ``"123 Main Street, Cairo, 11511"``
    * ``"Main Street, Zamalek, Cairo"``
    * ``"شارع التحرير, القاهرة"``
    * ``"Cairo, 12 Tahrir St"``          (reversed order)
    * ``"المهندسين شارع لبنان"``          (Arabic: area + street)
    * ``"Rue de Rivoli, Paris"``         (French address)
    * ``"Boulevard Mohammed V, Casablanca"``
    * ``"Tahrir"``                       (single-token fallback)

    Returns a dict with any subset of:
      ``housenumber``, ``street``, ``city``, ``postcode``, ``suburb``,
      ``state``, ``country``, ``raw`` (normalized full query)
    """
    q_norm = normalize_address_text(q)
    parts = [p.strip() for p in q_norm.split(",")]
    result: dict = {"raw": q_norm}

    if not parts:
        return result

    # ── Pass 1: extract postcodes from any position ───────────────────────
    remaining_parts: list[str] = []
    for p in parts:
        if _POSTCODE_RE.match(p):
            result.setdefault("postcode", p)
        else:
            remaining_parts.append(p)

    if not remaining_parts:
        return result

    # ── Pass 2: detect house number + street in any part ──────────────────
    street_part_idx: int | None = None
    for i, part in enumerate(remaining_parts):
        m = re.match(r"^(\d[\w/-]*)\s+(.+)", part)
        if m:
            result["housenumber"] = m.group(1).strip()
            result["street"] = m.group(2).strip()
            street_part_idx = i
            break

    # ── Pass 3: if no housenumber found, detect street via keywords ───────
    if street_part_idx is None:
        for i, part in enumerate(remaining_parts):
            # Arabic street detection
            street_candidate, city_candidate = _detect_arabic_street(part)
            if city_candidate:
                result["street"] = street_candidate
                result.setdefault("city", city_candidate)
                street_part_idx = i
                break
            # French street detection
            fr_street, fr_city = _detect_french_street(part)
            if fr_city:
                result["street"] = fr_street
                result.setdefault("city", fr_city)
                street_part_idx = i
                break
            # English/French/Arabic street keyword detection
            if _STREET_RE.search(part):
                result["street"] = part
                street_part_idx = i
                break

    # ── Pass 4: if still no street, use heuristic position ────────────────
    if street_part_idx is None:
        # If only one part, it could be a street name, area, or POI
        if len(remaining_parts) == 1:
            tok = remaining_parts[0]
            # Check if it's a known city/area (any language)
            if tok in _ALL_CITY_KEYWORDS or tok.lower() in _ALL_CITY_KEYWORDS:
                result["city"] = tok
            else:
                result["street"] = tok
            street_part_idx = 0
        else:
            # Multiple parts: first = street, rest = locality hierarchy
            result["street"] = remaining_parts[0]
            street_part_idx = 0

    # ── Pass 5: assign remaining parts (city, suburb, state, country) ─────
    locality_parts = [
        p for i, p in enumerate(remaining_parts) if i != street_part_idx
    ]

    for i, part in enumerate(locality_parts):
        if not part:
            continue
        # Detect postcodes that slipped through
        if _POSTCODE_RE.match(part):
            result.setdefault("postcode", part)
            continue
        # Short 2-3 uppercase letters → country code
        if re.match(r"^[A-Z]{2,3}$", part):
            result.setdefault("country", part)
            continue
        # Known city? (check all languages)
        if part in _ALL_CITY_KEYWORDS or part.lower() in _ALL_CITY_KEYWORDS:
            result.setdefault("city", part)
            continue
        # First unassigned locality → city (or suburb if city already set)
        if "city" not in result:
            result["city"] = part
        elif "suburb" not in result:
            result["suburb"] = part
        elif "state" not in result:
            result["state"] = part

    return result
