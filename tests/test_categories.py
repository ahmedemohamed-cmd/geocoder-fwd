"""Unit tests for the place category classifier (shared/categories.py).

Pure, infra-free: no ES/Redis. Locks the precedence order and the POI/non-POI
split that /nearby's filtering and POI guard depend on.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.categories import (
    GROUPS,
    VALUES_BY_GROUP,
    Category,
    classify,
)


def test_amenity_restaurant_classifies_to_food():
    cat = classify({"amenity": "restaurant", "name": "Koshary"})
    assert cat == Category(key="amenity", value="restaurant", group="food", is_poi=True)


def test_pharmacy_is_health():
    cat = classify({"amenity": "pharmacy"})
    assert (cat.value, cat.group, cat.is_poi) == ("pharmacy", "health", True)


def test_shop_bakery_is_food():
    cat = classify({"shop": "bakery"})
    assert (cat.key, cat.value, cat.group) == ("shop", "bakery", "food")


def test_precedence_amenity_beats_building():
    # A restaurant that is also tagged building=yes must classify as the amenity,
    # not the generic building — first-match over CATEGORY_KEYS.
    cat = classify({"building": "yes", "amenity": "restaurant"})
    assert cat.key == "amenity" and cat.value == "restaurant"


def test_unlisted_shop_value_falls_back_to_shopping_group():
    cat = classify({"shop": "wombats"})  # not in GROUP_DEFS
    assert cat.key == "shop" and cat.value == "wombats" and cat.group == "shopping"


def test_place_city_is_not_poi():
    cat = classify({"place": "city", "name": "Cairo"})
    assert cat.is_poi is False and cat.group == "place"


def test_boundary_is_not_poi():
    cat = classify({"boundary": "administrative", "name": "Cairo Governorate"}, admin_level=4)
    assert cat.is_poi is False and cat.group == "boundary"


def test_admin_level_alone_marks_non_poi():
    # An element with admin_level but no boundary tag is still an area.
    cat = classify({"name": "Some Region"}, admin_level=8)
    assert cat.is_poi is False and cat.group == "boundary"


def test_no_category_tag_is_not_poi():
    cat = classify({"name": "Nameless node"})
    assert cat.key is None and cat.is_poi is False


def test_discovery_constants_are_consistent():
    assert "food" in GROUPS
    assert "restaurant" in VALUES_BY_GROUP["food"]
    assert "pharmacy" in VALUES_BY_GROUP["health"]
    # every group in VALUES_BY_GROUP is advertised in GROUPS
    assert set(VALUES_BY_GROUP) == set(GROUPS)
