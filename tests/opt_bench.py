#!/usr/bin/env python3
"""Benchmark candidate /geocode query optimizations against the real query shape.

For each variant we measure Elasticsearch `took` (p50/p90) over a realistic query
corpus AND the top-10 overlap vs the current production query, so we can see the
speed win and the ranking drift (recall risk) at the same time. Relative numbers
are robust even while the box is busy (Valhalla build), because every variant is
measured under the same conditions.
"""

import json
import statistics
import sys
import urllib.request

sys.path.insert(0, "tests")
import stress as s  # corpus + ES endpoint

CAIRO = s.CAIRO
ES = s.ES


def post(body):
    req = urllib.request.Request(
        ES, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


# ---- building blocks -------------------------------------------------------
def text_should(q):
    """The production non-address text should-clauses."""
    return [
        {
            "multi_match": {
                "query": q,
                "fields": [
                    "name^5",
                    "name.autocomplete^2",
                    "name_en^5",
                    "name_en.autocomplete^2",
                    "name_fr^5",
                    "name_fr.autocomplete^2",
                    "tags_text",
                ],
                "type": "best_fields",
                "fuzziness": "AUTO",
            }
        },
        {
            "multi_match": {
                "query": q,
                "fields": ["name^5", "name_en^5", "name_fr^5"],
                "type": "best_fields",
                "fuzziness": 1,
                "prefix_length": 1,
                "boost": 10,
            }
        },
        {
            "multi_match": {
                "query": q,
                "fields": ["name", "name_en", "name_fr"],
                "type": "phrase",
                "boost": 10,
            }
        },
        {
            "multi_match": {
                "query": q,
                "fields": ["name", "name_en", "name_fr"],
                "type": "best_fields",
                "operator": "and",
                "boost": 15,
            }
        },
        {
            "multi_match": {
                "query": q,
                "fields": ["name^5", "name_en^5", "name_fr^5"],
                "type": "best_fields",
                "operator": "and",
                "fuzziness": "AUTO",
                "prefix_length": 1,
                "boost": 8,
            }
        },
    ]


def text_should_lean(q):
    """Cheaper recall: prefix_length 2, no fuzzy on edge-ngram/tags_text."""
    return [
        # non-fuzzy recall incl. autocomplete + tags_text (cheap)
        {
            "multi_match": {
                "query": q,
                "fields": [
                    "name^5",
                    "name.autocomplete^2",
                    "name_en^5",
                    "name_en.autocomplete^2",
                    "name_fr^5",
                    "name_fr.autocomplete^2",
                    "tags_text",
                ],
                "type": "best_fields",
            }
        },
        # fuzzy only on the analyzed name fields, tighter automaton
        {
            "multi_match": {
                "query": q,
                "fields": ["name^5", "name_en^5", "name_fr^5"],
                "type": "best_fields",
                "fuzziness": "AUTO",
                "prefix_length": 2,
                "max_expansions": 30,
                "boost": 4,
            }
        },
        {
            "multi_match": {
                "query": q,
                "fields": ["name", "name_en", "name_fr"],
                "type": "phrase",
                "boost": 10,
            }
        },
        {
            "multi_match": {
                "query": q,
                "fields": ["name", "name_en", "name_fr"],
                "type": "best_fields",
                "operator": "and",
                "boost": 15,
            }
        },
    ]


def funcs(with_geo=True):
    f = [
        {"weight": 1.0},
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
            "field_value_factor": {
                "field": "popularity",
                "modifier": "log1p",
                "factor": 1,
                "missing": 0,
            },
            "weight": 1,
        },
    ]
    if with_geo:
        f.insert(
            2,
            {
                "gauss": {
                    "centroid": {
                        "origin": {"lat": CAIRO[0], "lon": CAIRO[1]},
                        "scale": "10km",
                        "offset": "1km",
                        "decay": 0.5,
                    }
                },
                "weight": 2,
            },
        )
    return f


def distance_feature():
    # Cheap proximity boost via BKD index (a query, not a per-doc function).
    return {
        "distance_feature": {
            "field": "centroid",
            "origin": [CAIRO[1], CAIRO[0]],
            "pivot": "10km",
            "boost": 8,
        }
    }


# ---- variants (each returns a full search body) ----------------------------
def v0_baseline(q):
    return {
        "size": 10,
        "query": {
            "function_score": {
                "query": {"bool": {"should": text_should(q), "minimum_should_match": 1}},
                "functions": funcs(),
                "score_mode": "sum",
                "boost_mode": "multiply",
            }
        },
    }


def v1_no_total(q):
    b = v0_baseline(q)
    b["track_total_hits"] = False
    return b


def v2_rescore(q):
    # phase1 = text only; phase2 = same multiply scoring over top-N
    return {
        "size": 10,
        "track_total_hits": False,
        "query": {"bool": {"should": text_should(q), "minimum_should_match": 1}},
        "rescore": {
            "window_size": 200,
            "query": {
                "rescore_query": {
                    "function_score": {
                        "query": {"bool": {"should": text_should(q), "minimum_should_match": 1}},
                        "functions": funcs(),
                        "score_mode": "sum",
                        "boost_mode": "multiply",
                    }
                },
                "query_weight": 0,
                "rescore_query_weight": 1,
            },
        },
    }


def v3_distance_feature(q):
    should = text_should(q) + [distance_feature()]
    return {
        "size": 10,
        "track_total_hits": False,
        "query": {
            "function_score": {
                "query": {"bool": {"should": should, "minimum_should_match": 1}},
                "functions": funcs(with_geo=False),
                "score_mode": "sum",
                "boost_mode": "multiply",
            }
        },
    }


def v4_lean_fuzzy(q):
    return {
        "size": 10,
        "track_total_hits": False,
        "query": {
            "function_score": {
                "query": {"bool": {"should": text_should_lean(q), "minimum_should_match": 1}},
                "functions": funcs(),
                "score_mode": "sum",
                "boost_mode": "multiply",
            }
        },
    }


def v5_combo(q):
    # lean fuzzy + distance_feature + rescore the importance/pop over top-N
    should = text_should_lean(q) + [distance_feature()]
    return {
        "size": 10,
        "track_total_hits": False,
        "query": {"bool": {"should": should, "minimum_should_match": 1}},
        "rescore": {
            "window_size": 200,
            "query": {
                "rescore_query": {
                    "function_score": {
                        "query": {"match_all": {}},
                        "functions": funcs(with_geo=False),
                        "score_mode": "sum",
                        "boost_mode": "multiply",
                    }
                },
                "query_weight": 1,
                "rescore_query_weight": 1,
            },
        },
    }


VARIANTS = [
    ("v0 baseline (current)", v0_baseline),
    ("v1 +track_total_hits=false", v1_no_total),
    ("v2 rescore(multiply,top200)", v2_rescore),
    ("v3 distance_feature geo", v3_distance_feature),
    ("v4 lean fuzzy", v4_lean_fuzzy),
    ("v5 combo lean+distfeat+rescore", v5_combo),
]


def top_ids(resp):
    return [h.get("_id") for h in resp.get("hits", {}).get("hits", [])][:10]


def main():
    corpus = s.QUERIES[:80]
    # baseline top-10 per query for overlap comparison
    base_ids = {}
    base_took = []
    for q in corpus:
        r = post(v0_baseline(q))
        base_ids[q] = top_ids(r)
        base_took.append(r.get("took", 0))

    print(f"# corpus={len(corpus)}  (relative numbers; box may be busy)\n")
    print("| variant | took p50 | took p90 | speedup | top10 overlap |")
    print("|---|---|---|---|---|")
    b50 = statistics.median(base_took)
    for name, fn in VARIANTS:
        tooks, overlaps = [], []
        for q in corpus:
            r = post(fn(q))
            tooks.append(r.get("took", 0))
            ids = top_ids(r)
            b = base_ids[q]
            if b:
                overlaps.append(len(set(ids) & set(b)) / len(set(b)))
        p50 = statistics.median(tooks)
        p90 = sorted(tooks)[int(0.9 * len(tooks))]
        ov = 100 * statistics.mean(overlaps) if overlaps else 0
        print(
            "| %s | %.0f | %.0f | %.1fx | %.0f%% |"
            % (name, p50, p90, (b50 / p50 if p50 else 0), ov)
        )


if __name__ == "__main__":
    main()
