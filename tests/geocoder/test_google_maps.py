"""Unit tests for the Places API (New) mapping (shared/google_maps.py).

Infra-free: `map_place_to_element` is pure; `nearby_search` is exercised with a
stubbed `_places_post` so the Text Search request body is verified without HTTP.
"""

import shared.google_maps as gm
from shared.google_maps import map_place_to_element


def test_map_place_sets_feature_name_and_provenance():
    place = {
        "id": "ChIJPID123",
        "displayName": {"text": "Koshary Abou Tarek", "languageCode": "en"},
        "types": ["restaurant", "food", "point_of_interest"],
        "location": {"latitude": 30.05, "longitude": 31.24},
        "formattedAddress": "16 Marouf, Cairo",
        "rating": 4.3,
        "userRatingCount": 1200,
    }
    extra = map_place_to_element(place, "en")
    tags = extra["message"]["tags"]
    assert tags["amenity"] == "restaurant"  # from the most specific type
    assert tags["name"] == "Koshary Abou Tarek"
    assert tags["name:en"] == "Koshary Abou Tarek"  # language-specific tag
    assert tags["source"] == "google"
    assert tags["ref:google_place_id"] == "ChIJPID123"
    assert extra["message"]["geom"] == {"type": "Point", "coordinates": [31.24, 30.05]}
    assert extra["centroid"] == {"lat": 30.05, "lon": 31.24}
    assert extra["formatted_address"] == "16 Marouf, Cairo"  # response-only
    assert extra["user_ratings_total"] == 1200
    assert extra["message"]["osm_id"].startswith("g_")  # stable id from place id


def test_map_place_untyped_has_no_feature_tag():
    place = {
        "id": "P",
        "displayName": {"text": "Some Place"},
        "types": ["point_of_interest", "establishment"],
        "location": {"latitude": 30.0, "longitude": 31.0},
    }
    tags = map_place_to_element(place, "ar")["message"]["tags"]
    assert "amenity" not in tags and "shop" not in tags
    assert tags["name"] == "Some Place" and tags["name:ar"] == "Some Place"


def test_map_place_closed_business_low_confidence():
    place = {
        "id": "P",
        "displayName": {"text": "Gone"},
        "types": ["restaurant"],
        "location": {"latitude": 30.0, "longitude": 31.0},
        "businessStatus": "CLOSED_PERMANENTLY",
    }
    assert map_place_to_element(place, "en")["confidence"] == 0.3


async def test_nearby_search_builds_text_search_body(monkeypatch):
    captured = {}

    async def fake_post(body, field_mask):
        captured["body"] = body
        captured["mask"] = field_mask
        return {"places": [{"id": "P"}], "nextPageToken": "TOKEN2"}

    monkeypatch.setattr(gm, "_places_post", fake_post)
    places, token = await gm.nearby_search(
        30.0, 31.0, 800, language="en", place_type="restaurant", page_size=15
    )
    body = captured["body"]
    assert body["textQuery"] == "restaurant"  # type drives the text query
    assert body["includedType"] == "restaurant"
    assert body["languageCode"] == "en"
    assert body["pageSize"] == 15
    assert body["locationBias"]["circle"]["center"] == {"latitude": 30.0, "longitude": 31.0}
    assert body["locationBias"]["circle"]["radius"] == 800.0
    assert body["rankPreference"] == "RELEVANCE"
    assert "pageToken" not in body
    assert "nextPageToken" in captured["mask"]
    # returns (places, nextPageToken) for scrolling
    assert (len(places), token) == (1, "TOKEN2")


async def test_nearby_search_distance_and_pagination(monkeypatch):
    captured = {}

    async def fake_post(body, field_mask):
        captured["body"] = body
        return {"places": []}  # last page → no nextPageToken

    monkeypatch.setattr(gm, "_places_post", fake_post)
    places, token = await gm.nearby_search(
        30.0, 31.0, 800, language="en", keyword="pharmacy", rankby="distance", page_token="TOK"
    )
    assert captured["body"]["rankPreference"] == "DISTANCE"
    assert captured["body"]["textQuery"] == "pharmacy"  # keyword drives the query
    assert captured["body"]["pageToken"] == "TOK"  # forwarded for scrolling
    assert token is None  # no more pages
