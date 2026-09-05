"""Shared Elasticsearch index mapping for osm_places.

Imported by both services/geocoder.py and services/es_inserter.py so
there is a single source of truth for the index schema.
"""

from shared.config import EMBEDDING_DIM, ES_INDEX_REPLICAS
from shared.spec import load as _load_spec

_SPEC = _load_spec("es-mapping.json")


def _resolve(node, subs):
    """Substitute ``${VAR}`` placeholders in the spec with runtime config.

    Two values in the mapping are environment-driven rather than tuned:
    replica count follows the cluster topology and the vector dimension
    follows the embedding model. They stay placeholders in the spec so that
    externalising the mapping cannot silently freeze them.
    """
    if isinstance(node, dict):
        return {k: _resolve(v, subs) for k, v in node.items()}
    if isinstance(node, list):
        return [_resolve(v, subs) for v in node]
    if isinstance(node, str) and node.startswith("${") and node.endswith("}"):
        return subs[node[2:-1]]
    return node


MAPPING = _resolve(
    _SPEC["MAPPING"],
    {"ES_INDEX_REPLICAS": ES_INDEX_REPLICAS, "EMBEDDING_DIM": EMBEDDING_DIM},
)
