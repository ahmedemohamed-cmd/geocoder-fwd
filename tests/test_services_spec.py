"""Contract for the services/ specification files.

`spec/routing.toml`, `spec/geonames.json` and `spec/matching.toml` hold domain
knowledge that shapes user-visible output: the Arabic narration vocabulary, the
traffic colour bands, the GeoNames feature taxonomy, the distance→confidence
ladder, and the tokens that make two street names "the same street".

The snapshot below pins what those files produce. A changed value fails here
rather than silently altering a route's narration or a result's confidence.

Regenerate deliberately with `python tests/test_services_spec.py --update`.
"""

import json
import pathlib

from services import geocoder_helpers as H
from services import gn_watcher as GN
from services import routing as R
from shared.spec import load

GOLDEN = pathlib.Path(__file__).parent / "services_golden.json"


def _row(code, cls, name="Test", pop="1000", lat="30.0", lon="31.0"):
    f = [""] * GN.NUM_COLUMNS
    f[GN.COL_GEONAMEID] = "1"
    f[GN.COL_NAME] = name
    f[GN.COL_ASCIINAME] = name
    f[GN.COL_LATITUDE] = lat
    f[GN.COL_LONGITUDE] = lon
    f[GN.COL_FEATURE_CLASS] = cls
    f[GN.COL_FEATURE_CODE] = code
    f[GN.COL_COUNTRY_CODE] = "EG"
    f[GN.COL_POPULATION] = pop
    return f


def build_snapshot():
    out = {}
    mans = []
    for typ in range(0, 30):
        for units in ("kilometers", "miles"):
            m = {
                "type": typ,
                "instruction": "Turn left onto Tahrir Street toward Downtown",
                "street_names": ["Tahrir Street"],
                "length": 1.234,
                "time": 60,
            }
            try:
                res = R._translate_maneuver(dict(m), units)
            except Exception as e:  # noqa: BLE001 - snapshot records the failure shape
                res = f"ERR:{type(e).__name__}"
            mans.append({"in": m, "units": units, "out": res})
    out["maneuvers"] = mans
    out["directions"] = [
        {"i": i, "r": R._direction_from(i)}
        for i in [
            "Turn left",
            "Turn right",
            "Turn sharp left",
            "Bear right",
            "Continue straight",
            "Make a U-turn",
            "Keep left",
            "Keep right",
            "unknown phrase",
        ]
    ]
    out["ordinals"] = {str(k): R._ORDINALS.get(k) for k in range(0, 13)}
    out["onto_toward"] = [
        {"q": s, "onto": R._onto(s), "toward": R._toward(s)}
        for s in ["Turn left onto Tahrir Street", "Head north toward Downtown", "no keywords"]
    ]
    out["traffic_classify"] = [
        {"live": lv, "free": ff, "r": R._classify(lv, ff)}
        for lv in (None, 0.0, 10.0, 25.0, 40.0, 60.0, 100.0)
        for ff in (None, 0.0, 50.0, 80.0)
    ]
    out["dist"] = [
        {"km": k, "u": u, "r": R._dist(k, u)}
        for k in (0.0, 0.05, 0.5, 1.0, 12.345)
        for u in ("kilometers", "miles")
    ]

    rows = []
    for code in sorted(GN._FEATURE_CODE_TO_PLACE) + sorted(GN._FEATURE_CODE_ADMIN_LEVEL) + ["ZZZZ"]:
        for cls in sorted(GN._FEATURE_CLASS_TAGS) + ["X"]:
            try:
                r = GN._parse_geonames_row(_row(code, cls))
            except Exception as e:  # noqa: BLE001 - snapshot records the failure shape
                r = f"ERR:{type(e).__name__}"
            rows.append({"code": code, "cls": cls, "out": r})
    out["geonames_rows"] = rows

    out["confidence"] = [
        {"d": d, "r": H._distance_confidence(d)}
        for d in (0, 5, 9, 10, 11, 50, 99, 100, 101, 200, 249, 250, 251, 500, 1000, 10000)
    ]
    out["norm_conf"] = [
        {"s": s, "m": m, "r": H._normalize_confidence(s, m)}
        for s in (0.0, 0.5, 1.0, 5.0)
        for m in (0.0, 1.0, 5.0, 10.0)
    ]
    out["street_tokens"] = [
        {"a": a, "b": b, "r": H._street_token_match(a, b)}
        for a, b in [
            ("Tahrir", "Tahrir Street"),
            ("Tahrir Street", "Tahrir"),
            ("Main", "Main Road"),
            ("street", "street"),
            ("El Nasr", "Nasr Road"),
            ("", ""),
            ("شارع التحرير", "التحرير"),
        ]
    ]
    out["text_full"] = [
        {"q": q, "clauses": H._text_should_full(q)}
        for q in ["Cairo", "15 Tahrir Street", "مستشفى", "a", "Nile Corniche Road"]
    ]
    out["text_lean"] = [
        {"q": q, "clauses": H._text_should_lean(q)}
        for q in ["Cairo", "15 Tahrir Street", "مستشفى", "a", "Nile Corniche Road"]
    ]
    out["haversine"] = [
        {"p": p, "r": round(H._haversine_m(*p), 6)}
        for p in [
            (30.0, 31.0, 30.0, 31.0),
            (30.0, 31.0, 30.1, 31.1),
            (0, 0, 0, 1),
            (-33.9, 18.4, 60.1, 24.9),
        ]
    ]
    return json.loads(json.dumps(out, sort_keys=True, default=str, ensure_ascii=False))


def test_spec_files_load():
    for name in ("routing.toml", "geonames.json", "matching.toml"):
        assert load(name), f"spec/{name} missing or empty"


def test_confidence_ladder_is_monotonic():
    """Confidence must never rise with distance — a reordered spec would break it."""
    ladder = load("matching.toml")["distance_confidence"]
    thresholds = [t for t, _ in ladder]
    confidences = [c for _, c in ladder]
    assert thresholds == sorted(thresholds), "distance thresholds must ascend"
    assert confidences == sorted(confidences, reverse=True), "confidence must descend"
    assert confidences[-1] > load("matching.toml")["distance_confidence_floor"]


def test_traffic_bands_descend():
    bands = load("routing.toml")["traffic_bands"]
    ratios = [r for r, _ in bands]
    assert ratios == sorted(ratios, reverse=True), "traffic bands are evaluated top-down"


def test_services_behaviour_matches_golden():
    expected = json.loads(GOLDEN.read_text(encoding="utf-8"))
    actual = build_snapshot()
    drifted = [k for k in expected if expected[k] != actual.get(k)]
    assert not drifted, f"spec-backed behaviour changed in: {drifted}. If deliberate, --update."


if __name__ == "__main__":
    import sys

    if "--update" in sys.argv:
        GOLDEN.write_text(
            json.dumps(build_snapshot(), indent=1, sort_keys=True, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"rewrote {GOLDEN}")
