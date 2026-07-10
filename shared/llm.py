"""Ollama LLM client for generating place descriptions.

Provides async helpers to generate trilingual (Arabic + English + French)
titles and descriptions for geocoded places using a small local model
served by Ollama.
"""

import json
import logging
import os
from typing import Any

import httpx

from shared.categories import CATEGORY_KEYS

logger = logging.getLogger(__name__)

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b")

# Timeout for the generation call (seconds).
# Small models on GPU should respond in <2s; CPU may take 5-10s.
# Cold starts (first request after model load) on CPU can take 30-40s.
_GENERATE_TIMEOUT = 90.0

# ── prompt template ───────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
You are a concise geographic content writer. Given structured data about a \
place, produce a short JSON object with exactly six keys:

  "title_en"       – a clean English title (the place name, possibly with \
type, e.g. "Cairo Tower – Observation Tower")
  "title_ar"       – the same in Arabic (use the Arabic name if available, \
otherwise transliterate)
  "title_fr"       – the same in French (use the French name if available, \
otherwise translate)
  "description_en" – 1-2 sentence English description of the place
  "description_ar" – 1-2 sentence Arabic description of the place
  "description_fr" – 1-2 sentence French description of the place

Rules:
- Output ONLY valid JSON, no markdown, no explanation.
- Keep descriptions factual. Use the tags and address to infer what the \
place is.
- If the place has no meaningful tags, write a generic geographic description.
- Do NOT invent information that is not in the input.\
"""


def _build_user_prompt(place: dict[str, Any]) -> str:
    """Build the user-facing prompt from a place document."""
    parts = []

    if place.get("name"):
        parts.append(f"Name: {place['name']}")
    if place.get("name_en") and place["name_en"] != place.get("name"):
        parts.append(f"English name: {place['name_en']}")
    if place.get("name_fr") and place["name_fr"] != place.get("name"):
        parts.append(f"French name: {place['name_fr']}")

    # Category from OSM tags (same key precedence as the /nearby classifier)
    tags = place.get("tags", {})
    for key in CATEGORY_KEYS:
        if key in tags:
            parts.append(f"Type: {key}={tags[key]}")
            break

    if tags.get("cuisine"):
        parts.append(f"Cuisine: {tags['cuisine']}")
    if tags.get("opening_hours"):
        parts.append(f"Hours: {tags['opening_hours']}")

    # Address
    addr_parts = []
    for field in (
        "addr_housenumber",
        "addr_street",
        "addr_city",
        "addr_suburb",
        "addr_state",
        "addr_postcode",
        "addr_country",
    ):
        val = place.get(field) or tags.get(field.replace("addr_", "addr:"), "")
        if val:
            addr_parts.append(f"{field.replace('addr_', '')}: {val}")
    if addr_parts:
        parts.append("Address: " + ", ".join(addr_parts))
    elif place.get("full_address"):
        parts.append(f"Address: {place['full_address']}")

    # Coordinates
    centroid = place.get("centroid")
    if centroid:
        lat = centroid.get("lat", centroid.get("coordinates", [None, None])[1])
        lon = centroid.get("lon", centroid.get("coordinates", [None, None])[0])
        if lat is not None and lon is not None:
            parts.append(f"Coordinates: {lat}, {lon}")

    if place.get("admin_level"):
        parts.append(f"Admin level: {place['admin_level']}")

    return "\n".join(parts) if parts else "Unknown place"


async def generate_description(place: dict[str, Any]) -> dict[str, str] | None:
    """Generate a trilingual title + description for a place via Ollama.

    Returns a dict with keys: title_en, title_ar, title_fr,
    description_en, description_ar, description_fr
    or None if generation fails.
    """
    user_prompt = _build_user_prompt(place)

    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0.3,
            "num_predict": 450,
        },
        "keep_alive": -1,
    }

    try:
        async with httpx.AsyncClient(timeout=_GENERATE_TIMEOUT) as client:
            resp = await client.post(f"{OLLAMA_URL}/api/chat", json=payload)
            resp.raise_for_status()

        body = resp.json()
        content = body.get("message", {}).get("content", "")

        result = json.loads(content)

        # Validate expected keys
        expected = {
            "title_en",
            "title_ar",
            "title_fr",
            "description_en",
            "description_ar",
            "description_fr",
        }
        if not expected.issubset(result.keys()):
            logger.warning("LLM response missing keys: %s", expected - result.keys())
            # Fill in missing keys with empty strings
            for key in expected:
                result.setdefault(key, "")

        return {k: result[k] for k in expected}

    except httpx.HTTPStatusError as e:
        logger.error("Ollama HTTP error: %s", e)
        return None
    except (json.JSONDecodeError, KeyError) as e:
        logger.error("Failed to parse LLM response: %s", e)
        return None
    except httpx.ConnectError:
        logger.error("Cannot connect to Ollama at %s", OLLAMA_URL)
        return None
    except httpx.TimeoutException:
        logger.error("Ollama request timed out after %ss (model may be loading)", _GENERATE_TIMEOUT)
        return None
    except Exception as e:
        logger.error("Unexpected LLM error: %s: %s", type(e).__name__, e)
        return None


async def is_ollama_available() -> bool:
    """Quick check whether the Ollama server is reachable."""
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"{OLLAMA_URL}/api/tags")
            return resp.status_code == 200
    except Exception:
        return False


async def warm_up_model() -> bool:
    """Pre-load the model so the first real request doesn't pay cold-start cost.

    Sends a tiny generation request with keep_alive=-1 (keep loaded forever).
    Returns True if the model was loaded successfully.
    """
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [{"role": "user", "content": "hi"}],
        "stream": False,
        "options": {"num_predict": 1},
        "keep_alive": -1,
    }
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(f"{OLLAMA_URL}/api/chat", json=payload)
            resp.raise_for_status()
        logger.info("Ollama model %s warmed up successfully", OLLAMA_MODEL)
        return True
    except Exception as e:
        logger.warning("Failed to warm up Ollama model: %s: %s", type(e).__name__, e)
        return False
