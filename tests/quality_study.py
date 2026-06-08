#!/usr/bin/env python3
"""Controlled search-quality study of query optimizations #1 (rescore) and
#2 (lean fuzzy), plus #3 (timeout) exposure.

Each variant is the *faithful* geocoder ES query (same should-clauses, functions
and address handling, reusing shared.address), run directly against ES over the
test set so we can compare recall metrics identical to run_recall.py without
redeploying. #3 is reported as the fraction of queries whose ES `took` exceeds
the proposed timeout (those would return partial results). #4 (concurrency cap)
has no ranking effect and is not a quality lever.
"""
import json
import os
import statistics
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # tests/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root
import run_recall as rr               # norm, street_match, haversine
from shared.address import is_address_query, parse_address_query, normalize_address_text

ES = "http://localhost:9200/osm_places/_search"
CAIRO = (30.0444, 31.2357)
LIMIT = 10
N_CASES = int(os.getenv("N_CASES", "500"))
TIMEOUT_MS = 800
WORKERS = 8
SOURCE = ["osm_id", "name", "name_en", "centroid", "addr_street"]


# ---- faithful query construction (mirrors services/geocoder.py) ------------
def _funcs(lat, lon, parsed, addr_detected):
    f = [{"weight": 1.0},
         {"field_value_factor": {"field": "offline_rank", "modifier": "log1p",
          "factor": 1, "missing": 0}, "weight": 1.5}]
    if lat is not None:
        f.append({"gauss": {"centroid": {"origin": {"lat": lat, "lon": lon},
                  "scale": "10km", "offset": "1km", "decay": 0.5}}, "weight": 2})
    f.append({"field_value_factor": {"field": "popularity", "modifier": "log1p",
              "factor": 1, "missing": 0}, "weight": 1})
    if addr_detected and parsed.get("housenumber"):
        try:
            hn = int(parsed["housenumber"])
            fn = {"script_score": {"script": {"source":
                  ("if (doc['addr_housenumber'].size() == 0) { return 0; } try { "
                   "long h = Long.parseLong(doc['addr_housenumber'].value); "
                   "double d = Math.abs(h - params.h); return 1.0/(1.0+d); } "
                   "catch (NumberFormatException e) { return 0; }"),
                  "params": {"h": hn}}}, "weight": 5}
            if parsed.get("street"):
                fn["filter"] = {"match_phrase": {"addr_street": {"query": parsed["street"], "slop": 1}}}
            f.append(fn)
        except ValueError:
            pass
    return f


def _should(q, parsed, addr_detected, lean):
    if lean:
        should = [
            {"multi_match": {"query": q, "fields": [
                "name^5", "name.autocomplete^2", "name_en^5", "name_en.autocomplete^2",
                "name_fr^5", "name_fr.autocomplete^2", "tags_text"], "type": "best_fields"}},
            {"multi_match": {"query": q, "fields": ["name^5", "name_en^5", "name_fr^5"],
                "type": "best_fields", "fuzziness": "AUTO", "prefix_length": 2,
                "max_expansions": 30, "boost": 4}},
            {"multi_match": {"query": q, "fields": ["name", "name_en", "name_fr"],
                "type": "phrase", "boost": 10}},
            {"multi_match": {"query": q, "fields": ["name", "name_en", "name_fr"],
                "type": "best_fields", "operator": "and", "boost": 15}},
        ]
    else:
        should = [
            {"multi_match": {"query": q, "fields": [
                "name^5", "name.autocomplete^2", "name_en^5", "name_en.autocomplete^2",
                "name_fr^5", "name_fr.autocomplete^2", "tags_text"],
                "type": "best_fields", "fuzziness": "AUTO"}},
            {"multi_match": {"query": q, "fields": ["name^5", "name_en^5", "name_fr^5"],
                "type": "best_fields", "fuzziness": 1, "prefix_length": 1, "boost": 10}},
            {"multi_match": {"query": q, "fields": ["name", "name_en", "name_fr"],
                "type": "phrase", "boost": 10}},
            {"multi_match": {"query": q, "fields": ["name", "name_en", "name_fr"],
                "type": "best_fields", "operator": "and", "boost": 15}},
            {"multi_match": {"query": q, "fields": ["name^5", "name_en^5", "name_fr^5"],
                "type": "best_fields", "operator": "and", "fuzziness": "AUTO",
                "prefix_length": 1, "boost": 8}},
        ]
    if addr_detected:
        should.append({"multi_match": {"query": q, "fields": [
            "addr_street^5", "addr_street.autocomplete^2", "addr_city^3",
            "addr_city.autocomplete^1.5", "addr_suburb^2", "addr_suburb.autocomplete",
            "full_address^3", "full_address.autocomplete^1.5"],
            "type": "cross_fields", "operator": "or", "minimum_should_match": "75%", "boost": 4}})
        should.append({"match_phrase": {"full_address": {"query": q, "boost": 6, "slop": 2}}})
        if parsed.get("street"):
            st = parsed["street"]
            should += [
                {"match_phrase": {"addr_street": {"query": st, "boost": 10, "slop": 1}}},
                {"match": {"addr_street": {"query": st, "fuzziness": "AUTO", "boost": 5}}},
                {"match": {"addr_street.autocomplete": {"query": st, "boost": 3}}},
                {"match_phrase": {"name": {"query": st, "boost": 4, "slop": 1}}}]
        if parsed.get("housenumber"):
            hn = parsed["housenumber"].lower()
            if parsed.get("street"):
                should.append({"bool": {"must": [
                    {"term": {"addr_housenumber": {"value": hn}}},
                    {"match_phrase": {"addr_street": {"query": parsed["street"], "slop": 1}}}],
                    "boost": 50}})
            else:
                should.append({"term": {"addr_housenumber": {"value": hn, "boost": 15}}})
        if parsed.get("city"):
            cv = parsed["city"]
            should += [{"match": {"addr_city": {"query": cv, "boost": 3}}},
                       {"match": {"addr_city.autocomplete": {"query": cv, "boost": 1.5}}},
                       {"match": {"name": {"query": cv, "boost": 2}}}]
        if parsed.get("suburb"):
            sv = parsed["suburb"]
            should += [{"match": {"addr_suburb": {"query": sv, "boost": 2}}},
                       {"match": {"addr_suburb.autocomplete": {"query": sv, "boost": 1}}}]
        if parsed.get("postcode"):
            should.append({"term": {"addr_postcode": {"value": parsed["postcode"], "boost": 6}}})
        if parsed.get("country"):
            should.append({"term": {"addr_country": {"value": parsed["country"], "boost": 4}}})
        should.append({"term": {"has_address": {"value": True, "boost": 2}}})
    return should


def build_body(query, lat, lon, variant):
    q = normalize_address_text(query)
    addr_detected = is_address_query(query)
    parsed = parse_address_query(query) if addr_detected else {}
    lean = "lean" in variant
    rescore = "rescore" in variant
    should = _should(q, parsed, addr_detected, lean)
    funcs = _funcs(lat, lon, parsed, addr_detected)
    text_query = {"bool": {"should": should, "minimum_should_match": 1}}
    body = {"size": LIMIT, "track_total_hits": False, "_source": SOURCE}
    if rescore:
        body["query"] = text_query
        body["rescore"] = {"window_size": 200, "query": {
            "rescore_query": {"function_score": {"query": text_query, "functions": funcs,
                "score_mode": "sum", "boost_mode": "multiply"}},
            "query_weight": 0, "rescore_query_weight": 1}}
    else:
        body["query"] = {"function_score": {"query": text_query, "functions": funcs,
            "score_mode": "sum", "boost_mode": "multiply"}}
    return body


def search(body):
    req = urllib.request.Request(ES, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        d = json.loads(r.read())
    return d.get("took", 0), [h["_source"] for h in d["hits"]["hits"]]


# ---- metrics (mirror run_recall.py) ----------------------------------------
def eval_named(case, results):
    osm_rank = next((i + 1 for i, r in enumerate(results) if r.get("osm_id") == case["osm_id"]), None)
    exp = {rr.norm(case.get("name_en")), rr.norm(case.get("name"))} - {""}
    ec = (case["lat"], case["lon"])
    len_rank = None
    for i, r in enumerate(results):
        rn = {rr.norm(r.get("name_en")), rr.norm(r.get("name"))} - {""}
        c = r.get("centroid") or {}
        near = c and rr.haversine(ec, (c.get("lat"), c.get("lon"))) <= 150
        if (exp & rn) or near:
            len_rank = i + 1
            break
    return osm_rank, len_rank


def eval_address(case, results):
    osm_rank = next((i + 1 for i, r in enumerate(results) if r.get("osm_id") == case["osm_id"]), None)
    st_rank = next((i + 1 for i, r in enumerate(results)
                    if rr.street_match(case["addr_street"], r.get("addr_street", ""))), None)
    return osm_rank, st_rank


VARIANTS = ["baseline", "rescore", "lean", "rescore_lean"]


def main():
    cases = json.load(open("tests/cairo_testset.json", encoding="utf-8"))[:N_CASES]
    named = [c for c in cases if c["kind"] == "named"]
    addr = [c for c in cases if c["kind"] == "address"]
    print(f"# quality study: {len(named)} named + {len(addr)} address  (geo-bias Cairo, limit {LIMIT})\n")

    for variant in VARIANTS:
        res = {"n_strict": [], "n_lenient": [], "a_exact": [], "a_street": [], "took": []}

        def work(case):
            took, hits = search(build_body(case["query"], CAIRO[0], CAIRO[1], variant))
            res["took"].append(took)
            if case["kind"] == "named":
                sr, lr = eval_named(case, hits)
                res["n_strict"].append(sr); res["n_lenient"].append(lr)
            else:
                er, srk = eval_address(case, hits)
                res["a_exact"].append(er); res["a_street"].append(srk)

        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            list(ex.map(work, cases))

        def at(lst, k):
            v = [r for r in lst if r is not None]
            return 100 * sum(1 for r in v if r <= k) / len(lst) if lst else 0
        tk = res["took"]
        over = 100 * sum(1 for t in tk if t > TIMEOUT_MS) / len(tk)
        print(f"## {variant}")
        print(f"  named   strict @1/5/10 = {at(res['n_strict'],1):.1f} / {at(res['n_strict'],5):.1f} / {at(res['n_strict'],10):.1f}"
              f"   lenient @1 = {at(res['n_lenient'],1):.1f}")
        print(f"  address exact  @1/5/10 = {at(res['a_exact'],1):.1f} / {at(res['a_exact'],5):.1f} / {at(res['a_exact'],10):.1f}"
              f"   street @1/5 = {at(res['a_street'],1):.1f} / {at(res['a_street'],5):.1f}")
        print(f"  took p50/p90 = {statistics.median(tk):.0f} / {sorted(tk)[int(.9*len(tk))]:.0f} ms"
              f"   >{TIMEOUT_MS}ms = {over:.1f}%\n")


if __name__ == "__main__":
    main()
