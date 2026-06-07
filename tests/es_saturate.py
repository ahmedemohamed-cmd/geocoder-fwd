#!/usr/bin/env python3
"""Saturate Elasticsearch directly with a MULTI-PROCESS client (bypasses the GIL).

The single-process threaded client tops out on Python/GIL overhead long before ES
does. Here each load level uses `procs` OS processes × `threads` each, with
pre-encoded request bodies, so the client can actually push ES to its knee. We
ramp total concurrency and watch ES-side search queue/rejected + CPU to locate
the real saturation point (queue climbing / rejections / CPU ~100% = ES-bound).
"""
import json
import multiprocessing as mp
import random
import statistics
import time
import urllib.request

ES = "http://localhost:9200/osm_places/_search"
CAIRO = (30.0444, 31.2357)
DURATION = 10.0
THREADS_PER_PROC = 16
PROC_LEVELS = [1, 2, 3, 4, 6, 8, 12]   # total conc = procs * THREADS_PER_PROC


def _bodies():
    import os, sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import stress as s
    random.seed(7)
    qs = random.sample(s.QUERIES, min(200, len(s.QUERIES)))
    return [json.dumps(s.es_body(q)).encode() for q in qs]

BODIES = _bodies()


def worker(args):
    duration, threads = args
    import threading
    stop = time.time() + duration
    counts = [0, 0]  # ok, err
    lats = []
    lock = threading.Lock()

    def run():
        local_ok = local_err = 0
        local_lats = []
        while time.time() < stop:
            body = random.choice(BODIES)
            t0 = time.perf_counter()
            try:
                req = urllib.request.Request(ES, data=body,
                                             headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=30) as r:
                    r.read()
                    ok = r.status == 200
                dt = (time.perf_counter() - t0) * 1000
                if ok:
                    local_ok += 1
                    if len(local_lats) < 2000:
                        local_lats.append(dt)
                else:
                    local_err += 1
            except Exception:
                local_err += 1
        with lock:
            counts[0] += local_ok
            counts[1] += local_err
            lats.extend(local_lats)

    ts = [threading.Thread(target=run) for _ in range(threads)]
    for t in ts: t.start()
    for t in ts: t.join()
    return counts[0], counts[1], lats


def es_stats():
    try:
        u = ("http://localhost:9200/_nodes/stats/process,thread_pool"
             "?filter_path=nodes.*.process.cpu.percent,"
             "nodes.*.thread_pool.search.queue,nodes.*.thread_pool.search.rejected")
        with urllib.request.urlopen(u, timeout=5) as r:
            d = json.loads(r.read())
        node = next(iter(d["nodes"].values()))
        return (node["process"]["cpu"]["percent"],
                node["thread_pool"]["search"]["queue"],
                node["thread_pool"]["search"]["rejected"])
    except Exception:
        return (None, None, None)


def main():
    print(f"# ES saturation  duration={DURATION}s/level  threads/proc={THREADS_PER_PROC}  bodies={len(BODIES)}\n")
    hdr = ("procs", "conc", "rps", "ok", "err", "p50ms", "p90ms", "p99ms", "es_cpu%", "es_q_end", "es_rej")
    print("| " + " | ".join(hdr) + " |")
    print("|" + "|".join(["---"] * len(hdr)) + "|")
    for procs in PROC_LEVELS:
        _, _, rej0 = es_stats()
        cpu_samples = []
        stop_sampling = mp.Event()

        def sample():
            while not stop_sampling.is_set():
                c = es_stats()[0]
                if c is not None:
                    cpu_samples.append(c)
                time.sleep(0.5)

        import threading
        smp = threading.Thread(target=sample); smp.start()
        t0 = time.time()
        with mp.Pool(procs) as pool:
            results = pool.map(worker, [(DURATION, THREADS_PER_PROC)] * procs)
        elapsed = time.time() - t0
        stop_sampling.set(); smp.join()
        _, q_end, rej1 = es_stats()

        ok = sum(r[0] for r in results)
        err = sum(r[1] for r in results)
        lats = sorted(l for r in results for l in r[2])
        n = len(lats)
        def pct(p): return round(lats[min(n - 1, int(p * n))], 1) if n else 0
        row = {
            "procs": procs, "conc": procs * THREADS_PER_PROC,
            "rps": round(ok / elapsed, 1), "ok": ok, "err": err,
            "p50": round(statistics.median(lats), 1) if n else 0,
            "p90": pct(0.90), "p99": pct(0.99),
            "cpu": round(statistics.mean(cpu_samples)) if cpu_samples else None,
            "q": q_end, "rej": (rej1 - rej0) if (rej1 is not None and rej0 is not None) else None,
        }
        print("| {procs} | {conc} | {rps} | {ok} | {err} | {p50} | {p90} | {p99} | {cpu} | {q} | {rej} |".format(**row))
        import sys; sys.stdout.flush()
        time.sleep(2.0)


if __name__ == "__main__":
    main()
