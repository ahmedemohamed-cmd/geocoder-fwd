#!/usr/bin/env python3
"""Recall harness: for each Cairo test case, query the live /geocode API and
check whether the expected place/address is found in the results.

For named places  : strict (same osm_id) and lenient (same name OR within 150 m).
For addresses     : exact (same osm_id), or street-only, or miss — at rank 1/5/10.
Interpolation     : a second probe queries a *non-existent* house number on the
                    same street and checks for an interpolated result.

All queries are geo-biased to downtown Cairo (a realistic user location), never
to the case's own coordinates, so geo-decay can't trivially surface the answer.

Outputs: tests/recall_report.md  and  tests/recall_failures.json
"""
import json
import math
import os
import re
import sys
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = os.getenv("RECALL_BASE", "http://localhost:8000")
IN = "tests/cairo_testset.json"
REPORT = "tests/recall_report.md"
FAILS = "tests/recall_failures.json"
CAIRO = (30.0444, 31.2357)          # geo-bias origin for every query
LIMIT = 10
WORKERS = 8
# Scoring efforts to measure, in report order. Each is the /geocode `effort`
# query param: "high" (full fuzzy + per-doc function_score) and "optimized"
# (lean fuzzy + rescore over top hits + bounded timeout). Override with EFFORTS.
EFFORTS = [e.strip() for e in os.getenv("EFFORTS", "high,optimized").split(",") if e.strip()]
EFFORT_LABELS = {
    "high": "High effort (default)",
    "optimized": "Optimized effort (lean fuzzy + rescore + 800 ms timeout)",
}

_GENERIC = {"street", "st", "road", "rd", "ave", "avenue", "alley", "lane",
            "square", "sq", "el", "al", "the",
            "شارع", "ش", "طريق", "حارة", "حاره", "زقاق", "ميدان"}


def norm(s):
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", " ", (s or "").lower())).strip()


def street_match(expected, actual):
    a = norm(actual)
    if not a:
        return False
    toks = [t for t in norm(expected).split() if t and t not in _GENERIC]
    if not toks:
        toks = norm(expected).split()
    return bool(toks) and all(t in a for t in toks)


def haversine(a, b):
    (la1, lo1), (la2, lo2) = a, b
    r = 6371000
    p1, p2 = math.radians(la1), math.radians(la2)
    dp = math.radians(la2 - la1)
    dl = math.radians(lo2 - lo1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(h))


def geocode(q, effort="high"):
    qs = urllib.parse.urlencode({"q": q, "lat": CAIRO[0], "lon": CAIRO[1],
                                 "limit": LIMIT, "vector": "false", "effort": effort})
    url = f"{BASE}/geocode?{qs}"
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            return json.loads(r.read()).get("results", [])
    except Exception:
        return []


def rank_of_osmid(results, osm_id):
    for i, r in enumerate(results):
        if r.get("osm_id") == osm_id:
            return i + 1
    return None


def eval_named(case, effort="high"):
    results = geocode(case["query"], effort)
    osm_rank = rank_of_osmid(results, case["osm_id"])
    exp_names = {norm(case.get("name_en")), norm(case.get("name"))} - {""}
    exp_c = (case["lat"], case["lon"])
    lenient_rank = None
    for i, r in enumerate(results):
        rn = {norm(r.get("name_en")), norm(r.get("name"))} - {""}
        c = r.get("centroid") or {}
        near = c and haversine(exp_c, (c.get("lat"), c.get("lon"))) <= 150
        if exp_names & rn or near:
            lenient_rank = i + 1
            break
    return {"kind": "named", "query": case["query"], "osm_id": case["osm_id"],
            "strict_rank": osm_rank, "lenient_rank": lenient_rank,
            "top": (results[0].get("name_en") or results[0].get("name")) if results else None}


def eval_address(case, effort="high"):
    results = geocode(case["query"], effort)
    osm_rank = rank_of_osmid(results, case["osm_id"])
    street_rank = None
    for i, r in enumerate(results):
        if street_match(case["addr_street"], r.get("addr_street", "")):
            street_rank = i + 1
            break
    # interpolation probe: query a likely-nonexistent number on the same street
    try:
        probe_hn = int(re.sub(r"\D", "", case["addr_housenumber"]) or "0") + 1
    except ValueError:
        probe_hn = None
    interp_ok = False
    if probe_hn:
        pq = f"{probe_hn} {case['addr_street']}, {case['addr_city']}"
        for r in geocode(pq, effort):
            if r.get("match_type") == "interpolated" and street_match(case["addr_street"], r.get("addr_street", "")):
                interp_ok = True
                break
    return {"kind": "address", "query": case["query"], "osm_id": case["osm_id"],
            "exact_rank": osm_rank, "street_rank": street_rank, "interp_ok": interp_ok,
            "top": (results[0].get("name_en") or results[0].get("name") or results[0].get("full_address")) if results else None}


def _at(rs, key, k):
    return sum(1 for r in rs if r[key] and r[key] <= k)


def run_effort(cases, effort):
    """Query the live API for every case at the given effort and return the
    per-case eval rows."""
    out = [None] * len(cases)

    def work(i):
        c = cases[i]
        out[i] = eval_named(c, effort) if c["kind"] == "named" else eval_address(c, effort)
        if (i + 1) % 100 == 0:
            print(f"  [{effort}] ...{i + 1}/{len(cases)}", file=sys.stderr)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(work, range(len(cases))))
    return out


def section_lines(out, effort):
    """Build the markdown lines for one effort's results section."""
    named = [r for r in out if r["kind"] == "named"]
    addr = [r for r in out if r["kind"] == "address"]
    nN, nA = len(named), len(addr)
    lines = ["# %s\n" % EFFORT_LABELS.get(effort, effort)]
    lines.append("## Named places (%d)\n" % nN)
    lines.append("| metric | @1 | @5 | @10 |")
    lines.append("|---|---|---|---|")
    for key, label in [("strict_rank", "strict (same osm_id)"), ("lenient_rank", "lenient (name or ≤150 m)")]:
        lines.append("| %s | %.1f%% | %.1f%% | %.1f%% |" % (
            label, 100 * _at(named, key, 1) / nN, 100 * _at(named, key, 5) / nN, 100 * _at(named, key, 10) / nN))
    lines.append("")
    lines.append("## Addresses (%d)\n" % nA)
    lines.append("| metric | @1 | @5 | @10 |")
    lines.append("|---|---|---|---|")
    for key, label in [("exact_rank", "exact (same osm_id)"), ("street_rank", "correct street")]:
        lines.append("| %s | %.1f%% | %.1f%% | %.1f%% |" % (
            label, 100 * _at(addr, key, 1) / nA, 100 * _at(addr, key, 5) / nA, 100 * _at(addr, key, 10) / nA))
    interp = sum(1 for r in addr if r["interp_ok"])
    lines.append("")
    lines.append("## Interpolation probe (addresses, %d)\n" % nA)
    lines.append(f"- Non-existent house number on a known street returned an "
                 f"interpolated point: **{interp}/{nA} ({100*interp/nA:.1f}%)**\n")
    return lines


def failures(out):
    named = [r for r in out if r["kind"] == "named"]
    addr = [r for r in out if r["kind"] == "address"]
    return {
        "named_strict_miss": [r for r in named if not r["strict_rank"]][:60],
        "named_lenient_miss": [r for r in named if not r["lenient_rank"]][:60],
        "address_street_miss": [r for r in addr if not r["street_rank"]][:60],
        "address_exact_miss": [r for r in addr if not r["exact_rank"]][:60],
    }


def main():
    cases = json.load(open(IN, encoding="utf-8"))

    header = [
        "# Cairo recall report (%d popular places/addresses)\n" % len(cases),
        f"Queries geo-biased to downtown Cairo {CAIRO}, limit={LIMIT}, vector=off, "
        f"against the live /geocode API.",
        "Each effort below is the `effort` query param: identical test set, "
        "identical index — only the query/scoring shape differs.\n",
    ]

    sections, fails = [], {}
    for effort in EFFORTS:
        print(f"== running effort={effort} ==", file=sys.stderr)
        out = run_effort(cases, effort)
        sections.extend(section_lines(out, effort))
        fails[effort] = failures(out)

    report = "\n".join(header + sections)
    open(REPORT, "w", encoding="utf-8").write(report)
    print(report)

    json.dump(fails, open(FAILS, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"\nFailures sample -> {FAILS}", file=sys.stderr)


if __name__ == "__main__":
    main()
