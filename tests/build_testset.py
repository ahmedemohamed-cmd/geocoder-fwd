#!/usr/bin/env python3
"""Build a test set of ~1000 popular Cairo places/addresses from the OSM data
already indexed in Elasticsearch.

"Popular" is approximated by ``offline_rank`` (importance) since the
feedback-driven ``popularity`` field is ~0 on a fresh local build.

Two pools:
  - named  : prominent named POIs/places (landmarks, malls, hospitals, …),
             deduplicated by display name so each query is unambiguous.
  - address: real address points (non-empty addr_housenumber + addr_street),
             used to exercise structured-address search and interpolation.

Output: tests/cairo_testset.json
"""
import json
import urllib.request

ES = "http://localhost:9200/osm_places/_search"
OUT = "tests/cairo_testset.json"

# Cairo governorate-ish bounding box
BBOX = {"top_left": {"lat": 30.25, "lon": 31.00}, "bottom_right": {"lat": 29.70, "lon": 31.60}}
GEO = {"geo_bounding_box": {"centroid": BBOX}}

N_NAMED = 750
N_ADDR = 250


def es(body):
    req = urllib.request.Request(ES, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def has_latin(s):
    return any("a" <= c.lower() <= "z" for c in s)


def fetch_named(target):
    """Top named Cairo docs by offline_rank, deduped by display name."""
    body = {
        "size": target * 6,
        "query": {"bool": {
            "must": [{"wildcard": {"name.keyword": "?*"}}],
            "filter": [GEO],
        }},
        "sort": [{"offline_rank": "desc"}],
        "_source": ["osm_id", "name", "name_en", "centroid", "offline_rank",
                    "admin_level", "tags.place", "tags.amenity", "tags.shop",
                    "tags.tourism", "addr_city"],
    }
    seen, out = set(), []
    for h in es(body)["hits"]["hits"]:
        s = h["_source"]
        disp = (s.get("name_en") or s.get("name") or "").strip()
        if not disp:
            continue
        key = disp.lower()
        if key in seen:
            continue
        c = s.get("centroid")
        if not c:
            continue
        seen.add(key)
        q = s.get("name_en") if (s.get("name_en") and has_latin(s["name_en"])) else s.get("name")
        out.append({
            "osm_id": s["osm_id"], "kind": "named", "query": q,
            "name": s.get("name", ""), "name_en": s.get("name_en", ""),
            "lat": c["lat"], "lon": c["lon"], "offline_rank": s.get("offline_rank", 0),
        })
        if len(out) >= target:
            break
    return out


def fetch_addresses(target):
    """Top Cairo address points by offline_rank with real hn + street."""
    body = {
        "size": target * 4,
        "query": {"bool": {
            "must": [
                {"wildcard": {"addr_housenumber": "?*"}},
                {"wildcard": {"addr_street.keyword": "?*"}},
            ],
            "filter": [GEO],
        }},
        "sort": [{"offline_rank": "desc"}],
        "_source": ["osm_id", "name", "name_en", "centroid", "offline_rank",
                    "addr_housenumber", "addr_street", "addr_city"],
    }
    out, seen = [], set()
    for h in es(body)["hits"]["hits"]:
        s = h["_source"]
        hn = (s.get("addr_housenumber") or "").strip()
        st = (s.get("addr_street") or "").strip()
        if not hn or not st:
            continue
        c = s.get("centroid")
        if not c:
            continue
        city = (s.get("addr_city") or "Cairo").strip() or "Cairo"
        key = (hn.lower(), st.lower(), city.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "osm_id": s["osm_id"], "kind": "address",
            "query": f"{hn} {st}, {city}",
            "addr_housenumber": hn, "addr_street": st, "addr_city": city,
            "name": s.get("name", ""), "name_en": s.get("name_en", ""),
            "lat": c["lat"], "lon": c["lon"], "offline_rank": s.get("offline_rank", 0),
        })
        if len(out) >= target:
            break
    return out


def main():
    named = fetch_named(N_NAMED)
    addr = fetch_addresses(N_ADDR)
    cases = named + addr
    with open(OUT, "w", encoding="utf-8") as fh:
        json.dump(cases, fh, ensure_ascii=False, indent=1)
    print(f"named={len(named)} address={len(addr)} total={len(cases)} -> {OUT}")
    print("sample named:", [c["query"] for c in named[:8]])
    print("sample addr :", [c["query"] for c in addr[:5]])


if __name__ == "__main__":
    main()
