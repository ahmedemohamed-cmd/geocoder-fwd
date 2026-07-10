"""``/nearby`` — explore nearby places, filterable by category.

Given a point, return the places within a radius, optionally filtered by precise
OSM type (``?category=restaurant,cafe``) or a coarse UI group (``?group=food``),
each annotated with its ``distance_m``.

This is a self-contained ``APIRouter`` mounted onto the geocoder app (same pattern
as ``services/routing.py``). It deliberately does **not** import
``services.geocoder`` — that would create an import cycle, since geocoder imports
this router at the bottom of its module. Instead the geocoder lifespan hands us the
shared Elasticsearch client via :func:`init`.

Filtering relies on the ``category_key`` / ``category_value`` / ``category_group``
keyword fields written at ingest by :func:`shared.categories.classify`; the same
classifier is used here to fill those fields in the response for any doc indexed
before the category backfill ran.
"""

from __future__ import annotations

import base64
import binascii
import json

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from services.cache_service import ResultCache
from services.geocoder_helpers import _haversine_m
from shared.categories import GROUPS, VALUES_BY_GROUP, classify
from shared.config import (
    NEARBY_CACHE_COORD_PRECISION,
    NEARBY_CACHE_ENABLED,
    NEARBY_CACHE_TTL,
    NEARBY_DEFAULT_RADIUS_M,
    NEARBY_MAX_AREA_KM2,
    NEARBY_MAX_RADIUS_M,
    REDIS_HOST,
    REDIS_PORT,
)
from shared.logging import get_logger
from shared.redis_client import make_redis_async

logger = get_logger("nearby")

router = APIRouter(tags=["nearby"])

INDEX = "osm_places"

# Fields returned per result. Explicit list so the 384-dim name_vector is never
# pulled back over the wire. `tags` is included so the category fallback below can
# reclassify docs indexed before the backfill.
_SRC = [
    "osm_id",
    "osm_type",
    "name",
    "name_en",
    "name_fr",
    "tags",
    "tags_text",
    "geom",
    "centroid",
    "admin_level",
    "area_km2",
    "offline_rank",
    "popularity",
    "full_address",
    "addr_housenumber",
    "addr_street",
    "addr_city",
    "addr_postcode",
    "addr_country",
    "addr_suburb",
    "addr_state",
    "category_key",
    "category_value",
    "category_group",
]

# ── shared ES client (injected by the geocoder lifespan) ──────────────────────
_es = None


def init(es) -> None:
    """Receive the geocoder's Elasticsearch client. Called from its lifespan."""
    global _es
    _es = es


# ── dedicated result cache (lazy, like routing._get_traffic_redis) ────────────
_cache: ResultCache | None = None


def _get_cache() -> ResultCache:
    global _cache
    if _cache is None:
        redis = make_redis_async(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_connect_timeout=5
        )
        _cache = ResultCache(
            redis,
            enabled=NEARBY_CACHE_ENABLED,
            ttl=NEARBY_CACHE_TTL,
            coord_precision=NEARBY_CACHE_COORD_PRECISION,
            prefix="nearby",
        )
    return _cache


def _split_multi(values: list[str] | None) -> list[str]:
    """Flatten repeated and/or comma-separated query params.

    Accepts both ``?category=restaurant&category=cafe`` and
    ``?category=restaurant,cafe`` (and a mix), returning a de-duplicated list.
    """
    if not values:
        return []
    out: list[str] = []
    for v in values:
        for part in v.split(","):
            part = part.strip()
            if part and part not in out:
                out.append(part)
    return out


# Tiebreak appended to every sort so the total order is deterministic — required
# for search_after (offset-free deep scrolling). osm_id is a unique keyword.
_TIEBREAK_SORT = {"osm_id": "asc"}


def _encode_cursor(sort_values: list) -> str:
    """Encode an ES ``sort`` array into an opaque, URL-safe pagination token."""
    raw = json.dumps(sort_values, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode()


def _decode_cursor(cursor: str) -> list:
    """Decode a pagination token back into an ES ``search_after`` array."""
    try:
        raw = base64.urlsafe_b64decode(cursor.encode())
        values = json.loads(raw)
    except (binascii.Error, ValueError, UnicodeDecodeError) as e:
        raise HTTPException(status_code=400, detail="invalid cursor") from e
    if not isinstance(values, list) or not values:
        raise HTTPException(status_code=400, detail="invalid cursor")
    return values


@router.get("/nearby/categories")
async def nearby_categories():
    """Discovery: the coarse groups and the precise values in each, for building
    filter chips. Static (derived from shared.categories), so no ES round-trip."""
    return {"groups": GROUPS, "values": VALUES_BY_GROUP}


@router.get("/nearby")
async def nearby(
    request: Request,
    lat: float = Query(..., ge=-90, le=90, description="Latitude of the search point"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude of the search point"),
    radius: int = Query(
        NEARBY_DEFAULT_RADIUS_M,
        ge=1,
        le=NEARBY_MAX_RADIUS_M,
        description="Search radius in metres",
    ),
    category: list[str] | None = Query(
        None, description="OSM type value(s), e.g. restaurant, pharmacy, cafe"
    ),
    group: list[str] | None = Query(
        None, description="Coarse group(s), e.g. food, health, shopping"
    ),
    sort: str = Query(
        "best",
        pattern="^(best|distance)$",
        description="best = distance blended with importance/popularity; distance = nearest first",
    ),
    limit: int = Query(10, ge=1, le=50),
    offset: int = Query(0, ge=0, le=9950, description="Shallow paging; ignored when cursor is set"),
    cursor: str | None = Query(
        None,
        description="Opaque token from a previous response's pagination.next_cursor; "
        "scrolls past the offset window (keep the other params identical)",
    ),
):
    """Return nearby POIs within ``radius`` metres of ``(lat, lon)``.

    Admin boundaries and area features (``place=*``, large polygons) are excluded
    so results are places to visit, never "Cairo"/"Egypt". Each result carries a
    ``distance_m`` from the query point.

    Pagination has two modes. ``offset`` is the simple shallow pager (bounded to
    ES's 10k window). For deep scrolling, follow ``pagination.next_cursor``: pass
    it back as ``cursor`` to get the next page with no depth limit (``search_after``).
    When ``cursor`` is set, ``offset`` is ignored.
    """
    if _es is None:
        raise HTTPException(status_code=503, detail="nearby unavailable: search backend not ready")

    categories = _split_multi(category)
    groups = _split_multi(group)

    # ── cache read (fail-open) ────────────────────────────────────────────────
    cache = _get_cache()
    cache_key = cache.key(
        "nearby",
        lat=cache.coord(lat),
        lon=cache.coord(lon),
        radius=radius,
        category=",".join(sorted(categories)),
        group=",".join(sorted(groups)),
        sort=sort,
        limit=limit,
        offset=offset,
        cursor=cursor or "",
    )
    cached = await cache.get(cache_key)
    if cached is not None:
        request.state.result_count = -1
        return Response(content=cached, media_type="application/json", headers={"X-Cache": "HIT"})

    # ── POI guard + filters ───────────────────────────────────────────────────
    # geo_distance requires a valid centroid, so every hit is guaranteed one for
    # distance_m. The must_not works on existing fields (admin_level/area_km2) so
    # it excludes boundaries/areas even for docs not yet category-backfilled.
    flt: list[dict] = [
        {"geo_distance": {"distance": f"{radius}m", "centroid": {"lat": lat, "lon": lon}}}
    ]
    must_not: list[dict] = [
        {"range": {"admin_level": {"gt": 0}}},
        {"range": {"area_km2": {"gt": NEARBY_MAX_AREA_KM2}}},
        {"terms": {"category_group": ["place", "boundary"]}},
    ]
    if categories:
        flt.append({"terms": {"category_value": categories}})
    if groups:
        flt.append({"terms": {"category_group": groups}})

    bool_query = {"bool": {"filter": flt, "must_not": must_not}}

    body: dict = {
        "size": limit,
        "track_total_hits": True,
        "_source": _SRC,
    }
    # Cursor scroll (search_after) and offset paging are mutually exclusive: ES
    # rejects a non-zero `from` alongside search_after, so cursor wins when set.
    if cursor:
        body["search_after"] = _decode_cursor(cursor)
    else:
        body["from"] = offset

    if sort == "distance":
        body["query"] = bool_query
        body["sort"] = [
            {
                "_geo_distance": {
                    "centroid": {"lat": lat, "lon": lon},
                    "order": "asc",
                    "unit": "m",
                    "distance_type": "arc",
                    "mode": "min",
                }
            },
            _TIEBREAK_SORT,
        ]
    else:
        # "best": distance decay blended with importance + popularity. No text
        # query, so boost_mode=replace makes the composite the final score.
        scale_m = max(radius // 3, 100)
        body["query"] = {
            "function_score": {
                "query": bool_query,
                "functions": [
                    {
                        "field_value_factor": {
                            "field": "offline_rank",
                            "modifier": "log1p",
                            "factor": 1,
                            "missing": 0,
                        },
                        "weight": 1.5,
                    },
                    {
                        "gauss": {
                            "centroid": {
                                "origin": {"lat": lat, "lon": lon},
                                "scale": f"{scale_m}m",
                                "offset": "0m",
                                "decay": 0.5,
                            }
                        },
                        "weight": 2,
                    },
                    {
                        "field_value_factor": {
                            "field": "popularity",
                            "modifier": "log1p",
                            "factor": 1,
                            "missing": 0,
                        },
                        "weight": 1,
                    },
                ],
                "score_mode": "sum",
                "boost_mode": "replace",
            }
        }
        # Sort by the composite score, then the tiebreak, so hits carry a stable
        # `sort` array that search_after can resume from (same order as scoring).
        body["sort"] = [{"_score": {"order": "desc"}}, _TIEBREAK_SORT]

    resp = await _es.search(index=INDEX, **body)

    total_obj = resp["hits"].get("total")
    total_hits = total_obj.get("value") if total_obj else None
    hits = resp["hits"]["hits"]

    results = []
    for h in hits:
        src = h["_source"]
        centroid = src.get("centroid") or {}
        distance_m = None
        if "lat" in centroid and "lon" in centroid:
            distance_m = round(_haversine_m(lat, lon, centroid["lat"], centroid["lon"]), 1)

        # Prefer the indexed category; fall back to classifying tags for docs that
        # predate the backfill so the response is always populated.
        ckey = src.get("category_key") or ""
        cval = src.get("category_value") or ""
        cgrp = src.get("category_group") or ""
        if not (ckey or cval or cgrp):
            cat = classify(src.get("tags", {}), src.get("admin_level"))
            ckey, cval, cgrp = cat.key or "", cat.value or "", cat.group or ""

        results.append(
            {
                "osm_id": src["osm_id"],
                "osm_type": src.get("osm_type", ""),
                "name": src.get("name", ""),
                "name_en": src.get("name_en", ""),
                "name_fr": src.get("name_fr", ""),
                "category_key": ckey,
                "category_value": cval,
                "category_group": cgrp,
                "distance_m": distance_m,
                "tags": src.get("tags", {}),
                "geom": src.get("geom"),
                "centroid": src.get("centroid"),
                "admin_level": src.get("admin_level", 0),
                "area_km2": src.get("area_km2", 0),
                "offline_rank": src.get("offline_rank", 0),
                "popularity": src.get("popularity", 0),
                "full_address": src.get("full_address", ""),
                "addr_housenumber": src.get("addr_housenumber", ""),
                "addr_street": src.get("addr_street", ""),
                "addr_city": src.get("addr_city", ""),
                "addr_postcode": src.get("addr_postcode", ""),
                "addr_country": src.get("addr_country", ""),
                "addr_suburb": src.get("addr_suburb", ""),
                "addr_state": src.get("addr_state", ""),
            }
        )

    # has_more: with a cursor we have no absolute position, so a full page implies
    # there may be more; with offset we can use the exact total.
    if cursor:
        has_more = len(hits) == limit
    elif total_hits is not None:
        has_more = offset + len(hits) < total_hits
    else:
        has_more = len(hits) >= limit
    # next_cursor resumes after the last hit's sort values (both modes now sort).
    last_sort = hits[-1].get("sort") if hits else None
    next_cursor = _encode_cursor(last_sort) if has_more and last_sort else None

    request.state.result_count = len(results)
    response_body = {
        "query": {
            "lat": lat,
            "lon": lon,
            "radius": radius,
            "sort": sort,
            "category": categories,
            "group": groups,
        },
        "pagination": {
            "limit": limit,
            "offset": offset,
            "total": total_hits,
            "has_more": has_more,
            "next_cursor": next_cursor,
        },
        "results": results,
    }
    jr = JSONResponse(content=response_body, headers={"X-Cache": "MISS"})
    await cache.set(cache_key, jr.body)
    return jr
