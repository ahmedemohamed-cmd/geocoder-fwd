import logging
import os

from shared.config import (
    EMBEDDING_DIM,
    EMBEDDING_MODEL,
    ENABLE_VECTORS,
    TRANSFORMERS_CACHE,
)

logger = logging.getLogger(__name__)

_model = None

# High-priority keys placed first so they carry more weight in the text
_PRIORITY_KEYS = (
    "name",
    "name:en",
    "name:ar",
    "alt_name",
    "int_name",
    "place",
    "boundary",
    "amenity",
    "shop",
    "tourism",
    "highway",
    "building",
    "natural",
    "leisure",
    "landuse",
    "addr:street",
    "addr:city",
    "addr:country",
)

# Keys to skip – not useful for search text
_SKIP_PREFIXES = (
    "source",
    "created_by",
    "note",
    "fixme",
    "FIXME",
    "tiger:",
    "gnis:",
    "ref",
    "is_in",
    "check_date",
)


def get_model():
    """Lazy-load and return the SentenceTransformer model (singleton).

    Uses GPU if available, falls back to CPU.  Loads from local path if
    EMBEDDING_MODEL points to a directory, otherwise downloads from HF Hub.
    """
    global _model
    if _model is not None:
        return _model

    try:
        import torch
        from sentence_transformers import SentenceTransformer
    except ImportError as e:
        raise RuntimeError(
            "sentence-transformers and torch are required when ENABLE_VECTORS=true. "
            f"Install them with: pip install sentence-transformers torch\n{e}"
        ) from e

    try:
        device = "cuda" if torch.cuda.is_available() else "cpu"
    except Exception:
        device = "cpu"
    logger.info("Using device: %s", device)

    cache_folder = TRANSFORMERS_CACHE

    try:
        if os.path.exists(EMBEDDING_MODEL):
            logger.info("Loading local model from: %s", EMBEDDING_MODEL)
            os.environ["HF_HUB_OFFLINE"] = "1"
            os.environ["TRANSFORMERS_OFFLINE"] = "1"
            _model = SentenceTransformer(EMBEDDING_MODEL, device=device)
            logger.info("Local model loaded successfully")
        elif cache_folder:
            os.makedirs(cache_folder, exist_ok=True)
            logger.info("Downloading model to: %s", cache_folder)
            _model = SentenceTransformer(EMBEDDING_MODEL, device=device, cache_folder=cache_folder)
            logger.info("Model downloaded and cached successfully")
        else:
            logger.info("Loading model with default cache")
            _model = SentenceTransformer(EMBEDDING_MODEL, device=device)
    except Exception as e:
        raise RuntimeError(f"Failed to load embedding model '{EMBEDDING_MODEL}': {e}") from e

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
    Optimized for GPU with larger batch sizes.
    """
    if not ENABLE_VECTORS:
        return [[0.0] * EMBEDDING_DIM for _ in texts]

    model = get_model()

    # Use larger batch size on GPU for better throughput
    try:
        import torch

        batch_size = 128 if torch.cuda.is_available() else 32
    except Exception:
        batch_size = 32

    try:
        return model.encode(
            texts, batch_size=batch_size, show_progress_bar=False, convert_to_numpy=True
        ).tolist()
    except Exception as e:
        logger.error("Embedding failed: %s", e)
        return [[0.0] * EMBEDDING_DIM for _ in texts]
