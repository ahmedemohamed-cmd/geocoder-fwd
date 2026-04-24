from shared.config import EMBEDDING_MODEL, EMBEDDING_DIM, ENABLE_VECTORS

_model = None

# High-priority keys placed first so they carry more weight in the text
_PRIORITY_KEYS = (
    "name", "name:en", "name:ar", "alt_name", "int_name",
    "place", "boundary", "amenity", "shop", "tourism",
    "highway", "building", "natural", "leisure", "landuse",
    "addr:street", "addr:city", "addr:country",
)

# Keys to skip – not useful for search text
_SKIP_PREFIXES = (
    "source", "created_by", "note", "fixme", "FIXME",
    "tiger:", "gnis:", "ref", "is_in", "check_date",
)


def get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(EMBEDDING_MODEL)
    return _model


def build_text(tags: dict) -> str:
    """Build a searchable text string from ALL string-valued OSM tags.

    Priority keys come first, then remaining string tags (sorted for
    determinism), skipping keys that are not useful for search.
    All tags are considered to maximise search coverage.
    """
    parts: list[str] = []
    seen: set[str] = set()

    # priority keys first
    for k in _PRIORITY_KEYS:
        if k in tags:
            v = tags[k]
            if isinstance(v, str) and v:
                parts.append(v)
                seen.add(k)

    # remaining string tags (includes all name:* variants and every other tag)
    for k in sorted(tags):
        if k in seen:
            continue
        if any(k.startswith(p) for p in _SKIP_PREFIXES):
            continue
        v = tags[k]
        if isinstance(v, str) and v:
            parts.append(v)

    return " ".join(parts)


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Return embedding vectors for a list of texts.

    Returns empty list-of-zeros when ENABLE_VECTORS is False.
    """
    if not ENABLE_VECTORS:
        return [[0.0] * EMBEDDING_DIM for _ in texts]
    model = get_model()
    return model.encode(texts, show_progress_bar=False).tolist()
