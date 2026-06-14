#!/usr/bin/env python3
"""Closed-loop concurrency sweep for /geocode and direct Elasticsearch.

For each concurrency level we run a fixed-duration closed loop (C worker threads
each firing requests back-to-back), then report throughput, latency percentiles,
error rate, and Elasticsearch-side CPU + search-thread-pool queue/rejected so we
can locate the saturation knee and see whether the geocoder process or ES is the
limiter.

Usage:
  python3 tests/stress.py geocode   # end-to-end via :8000
  python3 tests/stress.py es        # direct to :9200 with the same query shape
"""

import json
import random
import statistics
import sys
import threading
import time
import urllib.parse
import urllib.request

GEO = "http://localhost:8000/geocode"
ES = "http://localhost:9200/osm_places/_search"
CAIRO = (30.0444, 31.2357)
LEVELS = [1, 2, 4, 8, 16, 24, 32, 48, 64, 96, 128]
DURATION = 12.0  # seconds of load per level
COOLDOWN = 2.0  # seconds between levels


# ---- query corpus (realistic mix from the test set) ------------------------
def load_queries(n=400):
    cases = json.load(open("tests/cairo_testset.json", encoding="utf-8"))
    qs = [c["query"] for c in cases if c.get("query")]
    random.seed(42)
    random.shuffle(qs)
    return qs[:n]


QUERIES = load_queries()


# A function_score body that mirrors the shape the geocoder builds (text
# multi_match should-clauses + offline_rank/geo/popularity scoring functions).
def es_body(q):
    return {
        "size": 10,
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "minimum_should_match": 1,
                        "should": [
                            {
                                "multi_match": {
                                    "query": q,
                                    "fields": [
                                        "name^5",
                                        "name.autocomplete^2",
                                        "name_en^5",
                                        "name_en.autocomplete^2",
                                        "name_fr^5",
                                        "tags_text",
                                    ],
                                    "type": "best_fields",
                                    "fuzziness": "AUTO",
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
                                    "fields": ["name^5", "name_en^5", "name_fr^5"],
                                    "type": "best_fields",
                                    "operator": "and",
                                    "boost": 15,
                                }
                            },
                        ],
                    }
                },
                "functions": [
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
                "boost_mode": "multiply",
            }
        },
    }


def geocode_url(q):
    qs = urllib.parse.urlencode(
        {"q": q, "lat": CAIRO[0], "lon": CAIRO[1], "limit": 10, "vector": "false", "ai": "false"}
    )
    return f"{GEO}?{qs}"


# ---- ES instrumentation ----------------------------------------------------
def es_stats():
    """Return (process_cpu_percent, search_queue, search_rejected)."""
    try:
        u = (
            "http://localhost:9200/_nodes/stats/process,thread_pool"
            "?filter_path=nodes.*.process.cpu.percent,"
            "nodes.*.thread_pool.search.queue,nodes.*.thread_pool.search.rejected"
        )
        with urllib.request.urlopen(u, timeout=5) as r:
            d = json.loads(r.read())
        node = next(iter(d["nodes"].values()))
        return (
            node["process"]["cpu"]["percent"],
            node["thread_pool"]["search"]["queue"],
            node["thread_pool"]["search"]["rejected"],
        )
    except Exception:
        return (None, None, None)


# ---- closed-loop worker ----------------------------------------------------
def run_level(target, conc, duration):
    lats, errors, done = [], 0, 0
    stop = time.time() + duration
    lock = threading.Lock()
    cpu_samples = []

    def worker():
        nonlocal errors, done
        while time.time() < stop:
            q = random.choice(QUERIES)
            t0 = time.perf_counter()
            try:
                if target == "geocode":
                    req = urllib.request.Request(geocode_url(q))
                else:
                    req = urllib.request.Request(
                        ES,
                        data=json.dumps(es_body(q)).encode(),
                        headers={"Content-Type": "application/json"},
                    )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    resp.read()
                    ok = resp.status == 200
                dt = (time.perf_counter() - t0) * 1000
                with lock:
                    if ok:
                        lats.append(dt)
                        done += 1
                    else:
                        errors += 1
            except Exception:
                with lock:
                    errors += 1

    def sampler():
        while time.time() < stop:
            c = es_stats()[0]
            if c is not None:
                cpu_samples.append(c)
            time.sleep(1.0)

    _, _, rej0 = es_stats()
    threads = [threading.Thread(target=worker) for _ in range(conc)]
    smp = threading.Thread(target=sampler)
    t_start = time.time()
    for t in threads:
        t.start()
    smp.start()
    for t in threads:
        t.join()
    smp.join()
    elapsed = time.time() - t_start
    _, queue, rej1 = es_stats()

    n = len(lats)
    lats.sort()

    def pct(p):
        return lats[min(n - 1, int(p * n))] if n else 0.0

    return {
        "conc": conc,
        "rps": round(done / elapsed, 1),
        "ok": done,
        "err": errors,
        "p50": round(statistics.median(lats), 1) if n else 0,
        "p90": round(pct(0.90), 1),
        "p99": round(pct(0.99), 1),
        "max": round(lats[-1], 1) if n else 0,
        "es_cpu": round(statistics.mean(cpu_samples)) if cpu_samples else None,
        "es_q": queue,
        "es_rej": (rej1 - rej0) if (rej1 is not None and rej0 is not None) else None,
    }


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else "geocode"
    print(f"# stress target={target}  duration={DURATION}s/level  corpus={len(QUERIES)} queries\n")
    hdr = (
        "conc",
        "rps",
        "ok",
        "err",
        "p50ms",
        "p90ms",
        "p99ms",
        "maxms",
        "es_cpu%",
        "es_q",
        "es_rej",
    )
    print("| " + " | ".join(hdr) + " |")
    print("|" + "|".join(["---"] * len(hdr)) + "|")
    for c in LEVELS:
        r = run_level(target, c, DURATION)
        print(
            "| {conc} | {rps} | {ok} | {err} | {p50} | {p90} | {p99} | {max} | {es_cpu} | {es_q} | {es_rej} |".format(
                **r
            )
        )
        sys.stdout.flush()
        time.sleep(COOLDOWN)


if __name__ == "__main__":
    main()
