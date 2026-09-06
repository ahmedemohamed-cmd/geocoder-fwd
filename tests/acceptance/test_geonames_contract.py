"""GeoNames ingest contract — the element message, not the parser.

`spec/geonames.toml` translates the GeoNames feature class/code scheme into OSM
tags. The pipeline contract (AD-1) is the element message a watcher publishes to
NATS, so that is what this pins: feed a TSV through the public `publish_tsv`
entry point with a fake JetStream, and capture what comes out.

Deliberately not testing `_parse_geonames_row`: a regeneration is free to parse
however it likes, provided the published message is the same.
"""

import json
import pathlib

import pytest

from services import gn_watcher as GN

GOLDEN = pathlib.Path(__file__).parent / "geonames_golden.json"


class _FakeJetStream:
    def __init__(self):
        self.published = []

    async def publish(self, subject, payload, timeout=None):
        self.published.append({"subject": subject, "message": json.loads(payload)})


# The GeoNames "geoname" table layout, from the GeoNames export documentation.
# Owned by this test rather than read from the module: the column order belongs
# to the external file format, so a regeneration must honour it whatever it
# names its own constants.
_COLUMNS = 19
_GEONAMEID, _NAME, _ASCIINAME = 0, 1, 2
_LATITUDE, _LONGITUDE = 4, 5
_FEATURE_CLASS, _FEATURE_CODE, _COUNTRY = 6, 7, 8
_POPULATION = 14


def _row(geonameid, name, cls, code, lat="30.0", lon="31.0", pop="1000", country="EG"):
    f = [""] * _COLUMNS
    f[_GEONAMEID] = str(geonameid)
    f[_NAME] = name
    f[_ASCIINAME] = name
    f[_LATITUDE] = lat
    f[_LONGITUDE] = lon
    f[_FEATURE_CLASS] = cls
    f[_FEATURE_CODE] = code
    f[_COUNTRY] = country
    f[_POPULATION] = pop
    return "\t".join(f)


def _corpus():
    """One row per feature code the spec knows, plus the awkward cases."""
    from shared.spec import load

    spec = load("geonames.toml")
    rows, n = [], 1
    for code in sorted(spec["feature_code_to_place"]):
        rows.append(_row(n, f"Place {code}", "P", code))
        n += 1
    for code in sorted(spec["feature_code_admin_level"]):
        rows.append(_row(n, f"Admin {code}", "A", code))
        n += 1
    for cls in sorted(spec["feature_class_tags"]):
        rows.append(_row(n, f"Class {cls}", cls, "ZZZZ"))
        n += 1
    # awkward rows: unknown code, blank name, bad coords, big population, short row
    rows.append(_row(n, "Unknown", "X", "NOPE"))
    n += 1
    rows.append(_row(n, "", "P", "PPL"))
    n += 1
    rows.append(_row(n, "Bad coords", "P", "PPL", lat="not-a-number"))
    n += 1
    rows.append(_row(n, "Out of range", "P", "PPL", lat="999", lon="999"))
    n += 1
    rows.append(_row(n, "Megacity", "P", "PPLC", pop="9500000"))
    n += 1
    rows.append("too\tfew\tcolumns")
    n += 1
    rows.append("")  # blank line
    rows.append("# comment")  # comment line
    return "\n".join(rows) + "\n"


async def _run(tmp_path):
    f = tmp_path / "test_geonames.txt"
    f.write_text(_corpus(), encoding="utf-8")
    js = _FakeJetStream()
    await GN.publish_tsv(str(f), js)
    return json.loads(json.dumps(js.published, sort_keys=True, ensure_ascii=False))


@pytest.mark.asyncio
async def test_published_messages_match_golden(tmp_path):
    expected = json.loads(GOLDEN.read_text(encoding="utf-8"))
    actual = await _run(tmp_path)
    assert len(actual) == len(expected), (
        f"published {len(actual)} messages, expected {len(expected)}"
    )
    drift = [e for e, a in zip(expected, actual, strict=True) if e != a]
    assert not drift, f"{len(drift)} published messages changed, first: {drift[0]}"


@pytest.mark.asyncio
async def test_message_shape_is_the_pipeline_contract(tmp_path):
    """AD-1: every watcher publishes the same element message shape."""
    published = await _run(tmp_path)
    assert published, "no messages published"
    for entry in published:
        m = entry["message"]
        assert set(m) >= {"osm_id", "osm_type", "tags"}, f"missing pipeline fields: {sorted(m)}"
        assert m["osm_id"], "osm_id must be present"
        assert isinstance(m["tags"], dict)


@pytest.mark.asyncio
async def test_malformed_rows_are_skipped_not_published(tmp_path):
    """A short row, a blank line and a comment must not reach the stream."""
    published = await _run(tmp_path)
    names = [e["message"]["tags"].get("name", "") for e in published]
    assert "" not in names, "a nameless row was published"
    assert not any(n == "too" for n in names), "a malformed row was published"


if __name__ == "__main__":
    import asyncio
    import sys
    import tempfile

    if "--update" in sys.argv:
        with tempfile.TemporaryDirectory() as d:
            out = asyncio.run(_run(pathlib.Path(d)))
        GOLDEN.write_text(
            json.dumps(out, indent=1, sort_keys=True, ensure_ascii=False), encoding="utf-8"
        )
        print(f"rewrote {GOLDEN} ({len(out)} messages)")
