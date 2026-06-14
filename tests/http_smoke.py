#!/usr/bin/env python3
"""Smoke-test every request in requests.http against a running geocoder.

GET requests are executed and summarised. Mutating POST requests
(/feedback, /places, /insert) are SKIPPED by default so the test does not
pollute the dataset; pass --include-post to run them too.
"""

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request

HTTP_FILE = "requests.http"
DEFAULT_BASE = "http://localhost:8000"


def parse_requests(path, base):
    """Yield (method, url, body_or_none) tuples from a .http file."""
    with open(path, encoding="utf-8") as fh:
        lines = fh.readlines()
    i = 0
    reqs = []
    while i < len(lines):
        line = lines[i].rstrip("\n")
        # URL may contain literal spaces (the VS Code REST client tolerates
        # them), so capture the rest of the line, dropping a trailing HTTP/x.y.
        m = re.match(r"^(GET|POST|PUT|DELETE)\s+(.+?)(?:\s+HTTP/[\d.]+)?$", line)
        if not m:
            i += 1
            continue
        method, url = m.group(1), m.group(2).strip()
        url = url.replace("{{baseUrl}}", base)
        # gather a JSON body (lines after an optional header, until a `###`)
        body = None
        j = i + 1
        buf = []
        while j < len(lines):
            l = lines[j].rstrip("\n")
            if re.match(r"^(GET|POST|PUT|DELETE)\s+\S+", l) or l.strip() == "###":
                break
            if l.lower().startswith("content-type:"):
                j += 1
                continue
            buf.append(l)
            j += 1
        text = "\n".join(buf).strip()
        if text.startswith("{"):
            body = text
        reqs.append((method, url, body))
        i = j
    return reqs


def fetch(method, url, body):
    # requests.http leaves spaces/commas unencoded in query strings; encode them.
    parts = urllib.parse.urlsplit(url)
    q = urllib.parse.quote(parts.query, safe="=&%")
    url = urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, q, ""))
    data = body.encode() if body else None
    headers = {"Content-Type": "application/json"} if body else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            code = resp.status
    except urllib.error.HTTPError as e:
        raw, code = e.read(), e.code
    except Exception as e:
        return None, str(e), round((time.monotonic() - t0) * 1000)
    return code, raw, round((time.monotonic() - t0) * 1000)


def summarize(path, raw):
    """One-line summary of a JSON response for a given endpoint path."""
    try:
        d = json.loads(raw)
    except Exception:
        return f"{len(raw)}B non-json"
    p = urllib.parse.urlsplit(path).path
    if p == "/geocode" or p == "/address":
        res = d.get("results", [])
        top = res[0] if res else None
        name = (top.get("name_en") or top.get("name") or "—") if top else "—"
        extra = ""
        if d.get("address_detected") or p == "/address":
            pa = d.get("address_parsed") or d.get("parsed") or {}
            extra = f" parsed={ {k: v for k, v in pa.items() if k != 'raw' and v} }"
        interp = sum(1 for r in res if r.get("match_type") == "interpolated")
        ip = f" interp={interp}" if interp else ""
        return f"{len(res)} results, top='{name}'{ip}{extra}"
    if p == "/autocomplete":
        return f"source={d.get('source')} n={len(d.get('results', []))}"
    if p == "/reverse":
        na = d.get("nearest_address")
        ia = d.get("interpolated_address")
        nl = d.get("nearest_line")
        return (
            f"nearest_addr={'Y' if na else '-'} interp={'Y' if ia else '-'} "
            f"nearest_line={'Y' if nl else '-'} polys={len(d.get('enclosing_polygons', []))}"
        )
    if p == "/health":
        ch = d.get("checks", {})
        return d.get("status") + " " + ",".join(f"{k}:{v.get('status')}" for k, v in ch.items())
    if p == "/features":
        return json.dumps(d)
    if p == "/describe":
        return "title=" + str(d.get("title") or d.get("name") or list(d)[:3])
    return json.dumps(d)[:80]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=DEFAULT_BASE)
    ap.add_argument("--file", default=HTTP_FILE)
    ap.add_argument("--include-post", action="store_true")
    args = ap.parse_args()

    reqs = parse_requests(args.file, args.base)
    npass = nfail = nskip = 0
    print(f"{'STATUS':7} {'MS':>5}  METHOD PATH — summary")
    print("-" * 100)
    for method, url, body in reqs:
        path = url.replace(args.base, "")
        if method != "GET" and not args.include_post:
            print(f"{'SKIP':7} {'':>5}  {method} {path[:48]}  (mutating; use --include-post)")
            nskip += 1
            continue
        code, raw, ms = fetch(method, url, body)
        if code is None:
            print(f"{'ERR':7} {ms:>5}  {method} {path[:60]}  {raw}")
            nfail += 1
            continue
        ok = 200 <= code < 300
        summary = summarize(url, raw) if ok else (raw[:80] if isinstance(raw, bytes) else str(raw))
        if isinstance(summary, bytes):
            summary = summary.decode(errors="replace")
        print(f"{code:<7} {ms:>5}  {method} {path[:48]}\n            {summary}")
        npass += ok
        nfail += not ok
    print("-" * 100)
    print(f"PASS={npass} FAIL={nfail} SKIP={nskip} TOTAL={len(reqs)}")
    return 1 if nfail else 0


if __name__ == "__main__":
    sys.exit(main())
