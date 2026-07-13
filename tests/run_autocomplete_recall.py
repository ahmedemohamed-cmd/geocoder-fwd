#!/usr/bin/env python3
"""Recall harness for the live /autocomplete API.

Mirrors tests/run_recall.py (which measures /geocode) — the metric helpers are
imported from it rather than reimplemented.

Keystroke simulation
    Autocomplete is typed, not pasted. For each named case we derive several
    prefixes of the target name (3 / 5 / 8 chars, plus the first whole word) and
    fire each as its own query, so we measure the endpoint the way a user drives
    it. Results are broken out per prefix length: a good index should get
    *better* as the user types, and that monotonicity is itself a signal.

Metrics (named cases)
    strict@k   expected osm_id appears in the top k
    lenient@k  same name OR within 150 m appears in the top k
    source     which backend answered (redis fast path vs elasticsearch)
    latency    p50 / p90 wall-clock

    `source` is the key diagnostic. The Redis path only helps if the answers it
    returns are *right*; a high redis share with low strict@1 means Redis is
    hijacking queries it cannot answer.

Category probe
    A separate hand-written set ("metro", "hospital", "مستشفى", ...). /autocomplete
    does not return categories, so the returned osm_ids are looked up in ES and
    scored against the expected category_key/category_value. Measures whether a
    type query returns places *of that type*.

All queries are geo-biased to downtown Cairo, never to the case's own
coordinates, so geo-decay cannot trivially surface the answer.

Outputs: tests/autocomplete_report.md  and  tests/autocomplete_failures.json

Usage:
    python3 tests/run_autocomplete_recall.py
    AC_BASE=http://localhost:8000 python3 tests/run_autocomplete_recall.py
"""

import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Single source of truth for the metric helpers — do not reimplement.
from run_recall import haversine, norm, rank_of_osmid  # noqa: E402

BASE = os.getenv("AC_BASE", "http://localhost:8000")
ES = os.getenv("ES_URL", "http://localhost:9200")
INDEX = "osm_places"
IN = "tests/cairo_testset.json"
REPORT = "tests/autocomplete_report.md"
FAILS = "tests/autocomplete_failures.json"

CAIRO = (30.0444, 31.2357)  # geo-bias origin for every query
LIMIT = 10
WORKERS = 8
NEAR_M = 150  # lenient match radius, same as run_recall

# Prefix lengths to simulate. "word" = the first whole word of the name.
PREFIX_LENS = [3, 5, 8]


# ── category probe set ───────────────────────────────────────────────────────
# Each entry: query -> set of acceptable (category_key, category_value) pairs.
# A hit is a top-k result whose ES category matches. Names that merely *contain*
# the word (the "Metro" supermarket chain) are NOT counted — this probe measures
# type search specifically.
CATEGORY_QUERIES = [
    ("metro", {("railway", "station"), ("railway", "subway_entrance")}),
    ("metro station", {("railway", "station"), ("railway", "subway_entrance")}),
    ("مترو", {("railway", "station"), ("railway", "subway_entrance")}),
    ("hospital", {("amenity", "hospital"), ("amenity", "clinic")}),
    ("مستشفى", {("amenity", "hospital"), ("amenity", "clinic")}),
    ("cafe", {("amenity", "cafe")}),
    ("مقهى", {("amenity", "cafe")}),
    ("pharmacy", {("amenity", "pharmacy")}),
    ("صيدلية", {("amenity", "pharmacy")}),
    ("school", {("amenity", "school")}),
    ("مدرسة", {("amenity", "school")}),
    ("mosque", {("amenity", "place_of_worship")}),
    ("مسجد", {("amenity", "place_of_worship")}),
    ("restaurant", {("amenity", "restaurant"), ("amenity", "fast_food")}),
    ("bank", {("amenity", "bank")}),
    ("supermarket", {("shop", "supermarket"), ("shop", "convenience")}),
]


def autocomplete(q, limit=LIMIT):
    """Query the live endpoint. Returns (results, source, elapsed_ms)."""
    qs = urllib.parse.urlencode(
        {"q": q, "lat": CAIRO[0], "lon": CAIRO[1], "limit": limit}
    )
    url = f"{BASE}/autocomplete?{qs}"
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            payload = json.loads(r.read())
    except Exception as e:
        return [], f"error:{type(e).__name__}", (time.perf_counter() - t0) * 1000
    dt = (time.perf_counter() - t0) * 1000
    return payload.get("results", []), payload.get("source", "?"), dt


def es_categories(osm_ids):
    """Look up category_key/category_value for a batch of osm_ids.

    /autocomplete does not return categories, so the category probe has to
    resolve them from ES directly.
    """
    if not osm_ids:
        return {}
    body = json.dumps(
        {
            "size": len(osm_ids),
            "query": {"terms": {"osm_id": list(osm_ids)}},
            "_source": ["osm_id", "category_key", "category_value", "name", "name_en"],
        }
    ).encode()
    req = urllib.request.Request(
        f"{ES}/{INDEX}/_search",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            hits = json.loads(r.read())["hits"]["hits"]
    except Exception as e:
        print(f"  ES lookup failed: {e}", file=sys.stderr)
        return {}
    out = {}
    for h in hits:
        s = h["_source"]
        out[s.get("osm_id")] = (s.get("category_key") or "", s.get("category_value") or "")
    return out


def prefixes_for(case):
    """Derive the keystroke prefixes to probe for one case."""
    name = case.get("name_en") or case.get("name") or ""
    name = name.strip()
    if not name:
        return []
    out = {}
    for n in PREFIX_LENS:
        if len(name) >= n:
            p = name[:n].strip()
            if len(p) >= 2:
                out.setdefault(p, f"len{n}")
    first_word = name.split()[0] if name.split() else ""
    if len(first_word) >= 2:
        out.setdefault(first_word, "word")
    return list(out.items())


def eval_case(case):
    """Probe every prefix of one named case. Returns a list of per-probe rows."""
    exp_names = {norm(case.get("name_en")), norm(case.get("name"))} - {""}
    exp_c = (case["lat"], case["lon"])
    rows = []

    for prefix, kind in prefixes_for(case):
        results, source, ms = autocomplete(prefix)
        strict = rank_of_osmid(results, case["osm_id"])

        lenient = None
        for i, r in enumerate(results):
            rn = {norm(r.get("name_en")), norm(r.get("name"))} - {""}
            c = r.get("centroid") or {}
            near = (
                c.get("lat") is not None
                and haversine(exp_c, (c["lat"], c["lon"])) <= NEAR_M
            )
            if (exp_names & rn) or near:
                lenient = i + 1
                break

        rows.append(
            {
                "osm_id": case["osm_id"],
                "expected": case.get("name_en") or case.get("name"),
                "prefix": prefix,
                "prefix_kind": kind,
                "source": source,
                "strict_rank": strict,
                "lenient_rank": lenient,
                "ms": ms,
                "top": (results[0].get("name_en") or results[0].get("name"))
                if results
                else None,
                "n_results": len(results),
            }
        )
    return rows


def eval_category(entry):
    q, expected = entry
    results, source, ms = autocomplete(q, limit=5)
    ids = [r.get("osm_id") for r in results if r.get("osm_id")]
    cats = es_categories(ids)
    matched = [i for i in ids if cats.get(i) in expected]
    return {
        "query": q,
        "source": source,
        "ms": ms,
        "n_results": len(results),
        "n_matched": len(matched),
        "hit_rate": (len(matched) / len(results)) if results else 0.0,
        "top5": [
            {
                "name": (r.get("name_en") or r.get("name")),
                "category": "/".join(cats.get(r.get("osm_id"), ("", ""))).strip("/"),
            }
            for r in results
        ],
    }


def pct(num, den):
    return (100.0 * num / den) if den else 0.0


def main():
    with open(IN) as f:
        cases = json.load(f)
    named = [c for c in cases if c.get("kind") == "named"]
    print(f"Autocomplete recall vs {BASE}", file=sys.stderr)
    print(f"{len(named)} named cases -> keystroke prefixes {PREFIX_LENS} + first word\n", file=sys.stderr)

    with ThreadPoolExecutor(WORKERS) as ex:
        nested = list(ex.map(eval_case, named))
    rows = [r for sub in nested for r in sub]

    with ThreadPoolExecutor(WORKERS) as ex:
        cat_rows = list(ex.map(eval_category, CATEGORY_QUERIES))

    total = len(rows)
    sources = Counter(r["source"] for r in rows)
    lat_all = sorted(r["ms"] for r in rows)
    p50 = statistics.median(lat_all) if lat_all else 0
    p90 = lat_all[int(len(lat_all) * 0.90)] if lat_all else 0

    def at(rs, field, k):
        return sum(1 for r in rs if r[field] and r[field] <= k)

    # ── report ──────────────────────────────────────────────────────────────
    L = []
    L.append("# Autocomplete recall report\n")
    L.append(f"- Base: `{BASE}`")
    L.append(f"- Named cases: **{len(named)}** → **{total}** prefix probes "
             f"(lengths {PREFIX_LENS} + first word)")
    L.append(f"- Geo-bias: downtown Cairo {CAIRO}, limit {LIMIT}, lenient radius {NEAR_M} m\n")

    L.append("## Named — overall\n")
    L.append("| metric | @1 | @5 | @10 |")
    L.append("|---|---|---|---|")
    for label, field in [("strict (osm_id)", "strict_rank"), ("lenient (name or ≤150 m)", "lenient_rank")]:
        L.append(
            f"| {label} | {pct(at(rows, field, 1), total):.1f}% "
            f"| {pct(at(rows, field, 5), total):.1f}% "
            f"| {pct(at(rows, field, 10), total):.1f}% |"
        )
    L.append("")

    L.append("## Named — by prefix length\n")
    L.append("| prefix | probes | strict@1 | strict@5 | lenient@1 | redis share | p50 ms |")
    L.append("|---|---|---|---|---|---|---|")
    by_kind = defaultdict(list)
    for r in rows:
        by_kind[r["prefix_kind"]].append(r)
    for kind in ["len3", "len5", "len8", "word"]:
        rs = by_kind.get(kind, [])
        if not rs:
            continue
        n = len(rs)
        redis_share = pct(sum(1 for r in rs if r["source"] == "redis"), n)
        med = statistics.median(sorted(r["ms"] for r in rs))
        L.append(
            f"| {kind} | {n} | {pct(at(rs, 'strict_rank', 1), n):.1f}% "
            f"| {pct(at(rs, 'strict_rank', 5), n):.1f}% "
            f"| {pct(at(rs, 'lenient_rank', 1), n):.1f}% "
            f"| {redis_share:.1f}% | {med:.1f} |"
        )
    L.append("")

    L.append("## Which backend answered\n")
    L.append("| source | probes | share | strict@1 within source |")
    L.append("|---|---|---|---|")
    for src, n in sources.most_common():
        rs = [r for r in rows if r["source"] == src]
        L.append(
            f"| `{src}` | {n} | {pct(n, total):.1f}% "
            f"| {pct(at(rs, 'strict_rank', 1), len(rs)):.1f}% |"
        )
    L.append("")
    L.append(f"Latency: **p50 {p50:.1f} ms**, **p90 {p90:.1f} ms**\n")

    L.append("## Category queries\n")
    L.append("A type query should return places *of that type*. `hit rate` = share of "
             "top-5 whose ES category matches. Name-only matches (e.g. the \"Metro\" "
             "supermarket) do not count.\n")
    L.append("| query | source | hit rate | top-5 (category) |")
    L.append("|---|---|---|---|")
    for c in cat_rows:
        top = ", ".join(
            f"{t['name']}" + (f" *({t['category']})*" if t["category"] else "")
            for t in c["top5"][:3]
        ) or "—"
        L.append(f"| `{c['query']}` | {c['source']} | {c['hit_rate'] * 100:.0f}% | {top} |")
    cat_mean = statistics.mean([c["hit_rate"] for c in cat_rows]) if cat_rows else 0
    L.append(f"\n**Mean category hit rate: {cat_mean * 100:.1f}%**\n")

    report = "\n".join(L)
    with open(REPORT, "w") as f:
        f.write(report)

    # failures: probes where the expected place never showed up at all
    fails = [r for r in rows if not r["lenient_rank"]]
    with open(FAILS, "w") as f:
        json.dump(fails[:300], f, ensure_ascii=False, indent=2)

    print(report)
    print(f"\nReport  -> {REPORT}", file=sys.stderr)
    print(f"Failures-> {FAILS} ({len(fails)} total, first 300 saved)", file=sys.stderr)


if __name__ == "__main__":
    main()
