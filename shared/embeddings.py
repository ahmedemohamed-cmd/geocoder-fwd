import os
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
        import torch

        # Detect and use GPU if available
        device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"[embeddings] Using device: {device}")

        # Check if EMBEDDING_MODEL is a local path
        if os.path.exists(EMBEDDING_MODEL):
            print(f"[embeddings] Loading local model from: {EMBEDDING_MODEL}")
            _model = SentenceTransformer(EMBEDDING_MODEL, device=device)
        else:
            # Use local cache directory if specified
            cache_folder = os.environ.get('TRANSFORMERS_CACHE')
            if cache_folder:
                print(f"[embeddings] Using local model cache: {cache_folder}")
                _model = SentenceTransformer(
                    EMBEDDING_MODEL,
                    device=device,
                    cache_folder=cache_folder,
                    local_files_only=True  # Force use of local files only
                )
            else:
                _model = SentenceTransformer(EMBEDDING_MODEL, device=device)
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
    import torch
    batch_size = 128 if torch.cuda.is_available() else 32
    
    return model.encode(
        texts, 
        batch_size=batch_size,
        show_progress_bar=False,
        convert_to_numpy=True
    ).tolist()
