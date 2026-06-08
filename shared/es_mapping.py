"""Shared Elasticsearch index mapping for osm_places.

Imported by both services/geocoder.py and services/es_inserter.py so
there is a single source of truth for the index schema.
"""

from shared.config import EMBEDDING_DIM

MAPPING = {
    "settings": {
        "index": {"number_of_replicas": 0},
        "analysis": {
            "char_filter": {
                # Normalize Arabic characters at char level before tokenization
                "arabic_normalize_char": {
                    "type": "pattern_replace",
                    "pattern": "[ـ]",  # tatweel
                    "replacement": "",
                },
            },
            "filter": {
                # English street-type synonyms (bidirectional)
                "street_synonyms_en": {
                    "type": "synonym",
                    "synonyms": [
                        "st, street",
                        "rd, road",
                        "ave, av, avenue",
                        "blvd, bvd, boulevard",
                        "ln, lane",
                        "dr, drive",
                        "pl, place",
                        "ct, court",
                        "sq, square",
                        "hwy, highway",
                        "cres, crescent",
                        "terr, terrace",
                        "pkwy, parkway",
                    ],
                },
                # Arabic street-type synonyms
                "street_synonyms_ar": {
                    "type": "synonym",
                    "synonyms": [
                        "ش, شارع",
                        "ط, طريق",
                        "م, ميدان",
                    ],
                },
                # French street-type synonyms
                "street_synonyms_fr": {
                    "type": "synonym",
                    "synonyms": [
                        "r, rue",
                        "av, ave, avenue",
                        "bd, blvd, boulevard",
                        "pl, place",
                        "ch, chemin",
                        "imp, impasse",
                        "all, allée",
                        "crs, cours",
                        "rte, route",
                        "pass, passage",
                    ],
                },
                # Edge n-gram for autocomplete / prefix matching
                "edge_ngram_filter": {
                    "type": "edge_ngram",
                    "min_gram": 2,
                    "max_gram": 15,
                },
                # Arabic normalization (alef, taa marbuta, etc.)
                "arabic_normalization": {
                    "type": "arabic_normalization",
                },
            },
            "normalizer": {
                "lowercase": {
                    "type": "custom",
                    "filter": ["lowercase"],
                },
            },
            "analyzer": {
                # Primary address analyzer: synonyms + lowercase
                "address_standard": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "char_filter": ["arabic_normalize_char"],
                    "filter": [
                        "lowercase",
                        "arabic_normalization",
                        "street_synonyms_en",
                        "street_synonyms_ar",
                        "street_synonyms_fr",
                    ],
                },
                # Edge n-gram analyzer for autocomplete (index-time only)
                "address_autocomplete": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "char_filter": ["arabic_normalize_char"],
                    "filter": [
                        "lowercase",
                        "arabic_normalization",
                        "street_synonyms_en",
                        "street_synonyms_ar",
                        "street_synonyms_fr",
                        "edge_ngram_filter",
                    ],
                },
                # Search analyzer: same as address_standard but NO edge n-gram
                "address_search": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "char_filter": ["arabic_normalize_char"],
                    "filter": [
                        "lowercase",
                        "arabic_normalization",
                        "street_synonyms_en",
                        "street_synonyms_ar",
                        "street_synonyms_fr",
                    ],
                },
                # Arabic-optimized analyzer for name fields
                "arabic_name": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "char_filter": ["arabic_normalize_char"],
                    "filter": [
                        "lowercase",
                        "arabic_normalization",
                    ],
                },
            },
        },
    },
    "mappings": {
        # dynamic:false — store any field not declared below in _source, but do
        # NOT add it to the mapping. OSM elements carry thousands of distinct tag
        # keys; if those (or any other ad-hoc object) were dynamically mapped the
        # field count explodes past the 1000-field limit and ES starts rejecting
        # docs ("Limit of total fields [1000] has been exceeded"). With dynamic
        # disabled the mapping is fixed-size and ingest can never blow it up,
        # while cached/undeclared fields are still returned on _source retrieval.
        "dynamic": False,
        "properties": {
            "osm_id": {"type": "keyword"},
            "osm_type": {"type": "keyword"},
            "name": {
                "type": "text",
                "analyzer": "arabic_name",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "address_autocomplete",
                        "search_analyzer": "address_search",
                    },
                },
            },
            "name_en": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "address_autocomplete",
                        "search_analyzer": "address_search",
                    },
                },
            },
            "name_fr": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "address_autocomplete",
                        "search_analyzer": "address_search",
                    },
                },
            },
            "tags_text": {
                "type": "text",
                "analyzer": "arabic_name",
            },
            "tags": {"type": "object", "enabled": False},
            # ignore_malformed: a single bad geometry (e.g. a self-intersecting
            # polygon from an OSM relation) must NOT reject the whole document.
            # Without this, such docs — often high-importance admin boundaries /
            # cities — are dropped entirely, vanishing from search and from the
            # autocomplete warm-up (which then falls back to ES). With it, the
            # bad shape is skipped and the doc still indexes (name, tags, centroid).
            "geom": {"type": "geo_shape", "ignore_malformed": True},
            "centroid": {"type": "geo_point", "ignore_malformed": True},
            "admin_level": {"type": "integer"},
            "area_km2": {"type": "float"},
            "offline_rank": {"type": "float"},
            "popularity": {"type": "float"},
            "name_vector": {
                "type": "dense_vector",
                "dims": EMBEDDING_DIM,
                "index": True,
                "similarity": "cosine",
            },
            # ── address fields ────────────────────────────────────────────
            "addr_housenumber": {
                "type": "keyword",
                "normalizer": "lowercase",
            },
            "addr_street": {
                "type": "text",
                "analyzer": "address_standard",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "address_autocomplete",
                        "search_analyzer": "address_search",
                    },
                },
            },
            "addr_city": {
                "type": "text",
                "analyzer": "address_standard",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "address_autocomplete",
                        "search_analyzer": "address_search",
                    },
                },
            },
            "addr_postcode": {"type": "keyword"},
            "addr_country":  {"type": "keyword"},
            "addr_suburb": {
                "type": "text",
                "analyzer": "address_standard",
                "fields": {
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "address_autocomplete",
                        "search_analyzer": "address_search",
                    },
                },
            },
            "addr_state": {
                "type": "text",
                "analyzer": "address_standard",
            },
            "full_address": {
                "type": "text",
                "analyzer": "address_standard",
                "fields": {
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "address_autocomplete",
                        "search_analyzer": "address_search",
                    },
                },
            },
            "has_address": {"type": "boolean"},
            # AI-generated place description (cached, not indexed)
            "ai_description": {"type": "object", "enabled": False},
            # Reverse-geocode enrichment cache written by geocoder.py
            # (nearest_street + enclosing parents). Stored for retrieval only,
            # never queried — keep it disabled so its nested keys are not mapped.
            "address": {"type": "object", "enabled": False},
        }
    },
}
