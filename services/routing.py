"""Valhalla routing proxy with Arabic narration support.

Valhalla 3.5.1 has no ar.json locale — ``language: ar`` silently falls back to
en-US.  This module proxies requests to Valhalla and, when the caller requests
Arabic (any ``ar*`` BCP-47 tag), rewrites every maneuver's instruction fields
using structured Arabic templates driven by the maneuver ``type`` and the
``street_names`` list (which the OSM data already provides in Arabic script).
"""
from __future__ import annotations

import re
from typing import Any

import httpx

from shared.config import VALHALLA_URL

# ── Arabic direction words ────────────────────────────────────────────────────
_DIRECTIONS: dict[str, str] = {
    "north": "شمالاً", "south": "جنوباً", "east": "شرقاً", "west": "غرباً",
    "northeast": "شمال شرقاً", "northwest": "شمال غرباً",
    "southeast": "جنوب شرقاً", "southwest": "جنوب غرباً",
}

# Arabic ordinals for roundabout exit counts 1-10
_ORDINALS: dict[int, str] = {
    1: "الأول", 2: "الثاني", 3: "الثالث", 4: "الرابع", 5: "الخامس",
    6: "السادس", 7: "السابع", 8: "الثامن", 9: "التاسع", 10: "العاشر",
}

_ONTO_RE = re.compile(r'\bonto (.+?)\.$', re.IGNORECASE)
_TOWARD_RE = re.compile(r'\btoward (.+?)\.$', re.IGNORECASE)


def _street(names: list[str]) -> str:
    return names[0] if names else ""


def _dist(length_km: float, units: str) -> str:
    if units == "miles":
        yards = round(length_km * 1760)
        return f"{yards} ياردة" if yards < 300 else f"{length_km * 0.621:.1f} ميل"
    m = round(length_km * 1000)
    return f"{m} متراً" if m < 1000 else f"{length_km:.1f} كيلومتراً"


def _direction_from(instruction: str) -> str:
    lower = instruction.lower()
    for eng, ar in _DIRECTIONS.items():
        if f" {eng} " in lower or lower.endswith(f" {eng}.") or f" {eng}\n" in lower:
            return ar
    return ""


def _onto(instruction: str) -> str:
    m = _ONTO_RE.search(instruction)
    return m.group(1) if m else ""


def _toward(instruction: str) -> str:
    m = _TOWARD_RE.search(instruction)
    return m.group(1) if m else ""


def _translate_maneuver(maneuver: dict, units: str) -> dict:
    typ = maneuver.get("type", 0)
    streets = maneuver.get("street_names", [])
    s = _street(streets)
    raw = maneuver.get("instruction", "")
    length = maneuver.get("length", 0.0)
    n = maneuver.get("roundabout_exit_count", 1)
    ordinal = _ORDINALS.get(n, str(n))
    dist_str = _dist(length, units)
    direction = _direction_from(raw)

    def _onto_s() -> str:
        return _onto(raw) or s

    def _toward_s() -> str:
        return _toward(raw) or s

    ar: str
    if typ in (1, 2, 3):       # Start (various directions)
        ar = f"اتجه {direction} على {s}." if s else f"اتجه {direction}."
    elif typ == 4:              # Destination (arrived)
        ar = "لقد وصلت إلى وجهتك."
    elif typ == 5:              # Destination right
        ar = "وجهتك على اليمين."
    elif typ == 6:              # Destination left
        ar = "وجهتك على اليسار."
    elif typ in (7, 8):         # Becomes / Continue
        ar = f"استمر على {s}." if s else "استمر مباشرة."
    elif typ == 9:              # Slight right
        ar = f"انحرف يميناً نحو {_onto_s()}." if _onto_s() else "انحرف قليلاً يميناً."
    elif typ == 10:             # Right
        ar = f"انعطف يميناً نحو {_onto_s()}." if _onto_s() else "انعطف يميناً."
    elif typ == 11:             # Sharp right
        ar = f"انعطف يميناً حاداً نحو {_onto_s()}." if _onto_s() else "انعطف يميناً حاداً."
    elif typ in (12, 13):       # U-turn
        ar = "استدر."
    elif typ == 14:             # Sharp left
        ar = f"انعطف يساراً حاداً نحو {_onto_s()}." if _onto_s() else "انعطف يساراً حاداً."
    elif typ == 15:             # Left
        ar = f"انعطف يساراً نحو {_onto_s()}." if _onto_s() else "انعطف يساراً."
    elif typ == 16:             # Slight left
        ar = f"انحرف يساراً نحو {_onto_s()}." if _onto_s() else "انحرف قليلاً يساراً."
    elif typ == 17:             # Ramp straight
        ar = f"خذ المنحدر نحو {_onto_s()}." if _onto_s() else "خذ المنحدر."
    elif typ == 18:             # Ramp right
        ar = f"خذ المنحدر على اليمين نحو {_onto_s()}." if _onto_s() else "خذ المنحدر على اليمين."
    elif typ == 19:             # Ramp left
        ar = f"خذ المنحدر على اليسار نحو {_onto_s()}." if _onto_s() else "خذ المنحدر على اليسار."
    elif typ == 20:             # Exit right
        ar = f"اخرج من اليمين نحو {_onto_s()}." if _onto_s() else "اخرج من اليمين."
    elif typ == 21:             # Exit left
        ar = f"اخرج من اليسار نحو {_onto_s()}." if _onto_s() else "اخرج من اليسار."
    elif typ == 22:             # Stay straight
        ar = f"استمر مباشرة على {s}." if s else "استمر مباشرة."
    elif typ == 23:             # Stay right
        ar = f"ابقَ على اليمين نحو {_onto_s() or _toward_s()}." if (_onto_s() or _toward_s()) else "ابقَ على اليمين."
    elif typ == 24:             # Stay left
        ar = f"ابقَ على اليسار نحو {_onto_s() or _toward_s()}." if (_onto_s() or _toward_s()) else "ابقَ على اليسار."
    elif typ == 25:             # Merge
        ar = f"اندمج على {_onto_s()}." if _onto_s() else "اندمج."
    elif typ == 26:             # Roundabout enter
        exit_street = _onto(raw)
        roundabout_name = s
        if roundabout_name and exit_street:
            ar = f"ادخل {roundabout_name} وخذ المخرج {ordinal} نحو {exit_street}."
        elif exit_street:
            ar = f"ادخل الدوار وخذ المخرج {ordinal} نحو {exit_street}."
        else:
            ar = f"ادخل الدوار وخذ المخرج {ordinal}."
    elif typ == 27:             # Roundabout exit
        ar = f"اخرج من الدوار نحو {_onto_s()}." if _onto_s() else "اخرج من الدوار."
    elif typ == 28:             # Ferry enter
        ar = f"استقل العبارة نحو {_onto_s()}." if _onto_s() else "استقل العبارة."
    elif typ == 29:             # Ferry exit
        ar = "غادر العبارة."
    elif typ in (37, 38):       # Merge right/left
        side = "يميناً" if typ == 37 else "يساراً"
        ar = f"اندمج {side} على {_onto_s()}." if _onto_s() else f"اندمج {side}."
    else:
        return maneuver  # transit / unknown — leave as-is

    m = dict(maneuver)
    m["instruction"] = ar
    if "verbal_pre_transition_instruction" in m:
        m["verbal_pre_transition_instruction"] = ar
    if "verbal_transition_alert_instruction" in m:
        m["verbal_transition_alert_instruction"] = ar
    if "verbal_succinct_transition_instruction" in m:
        m["verbal_succinct_transition_instruction"] = ar.rstrip(".")
    if "verbal_post_transition_instruction" in m and length > 0:
        m["verbal_post_transition_instruction"] = f"استمر لمسافة {dist_str}."
    return m


def _translate_response(body: dict) -> dict:
    """Rewrite all maneuver instructions in a Valhalla trip response to Arabic."""
    trip = body.get("trip")
    if not trip:
        return body
    units = trip.get("units", "kilometers")
    for leg in trip.get("legs", []):
        leg["maneuvers"] = [_translate_maneuver(m, units) for m in leg.get("maneuvers", [])]
    trip["language"] = "ar"
    return body


def _wants_arabic(payload: dict) -> bool:
    lang = (payload.get("directions_options") or {}).get("language", "")
    return isinstance(lang, str) and lang.lower().startswith("ar")


def _strip_arabic_language(payload: dict) -> dict:
    """Remove the ar language tag so Valhalla doesn't fall back silently."""
    opts = payload.get("directions_options")
    if opts and opts.get("language", "").lower().startswith("ar"):
        opts = dict(opts)
        del opts["language"]
        payload = dict(payload, directions_options=opts)
    return payload


async def proxy(path: str, method: str, body: dict | None,
                timeout: float = 30.0) -> Any:
    """Forward a routing request to Valhalla, applying Arabic translation if needed."""
    translate = body is not None and _wants_arabic(body)
    if translate:
        body = _strip_arabic_language(body)

    url = VALHALLA_URL.rstrip("/") + "/" + path.lstrip("/")
    async with httpx.AsyncClient(timeout=timeout) as client:
        if method == "GET":
            resp = await client.get(url)
        else:
            resp = await client.post(url, json=body)

    result = resp.json()
    if translate and resp.status_code == 200:
        result = _translate_response(result)
    return resp.status_code, result
