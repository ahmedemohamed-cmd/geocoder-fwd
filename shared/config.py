import logging
import os
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Try to load .env file if it exists (won't fail if missing)
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)


def _safe_int(env_var: str, default: int) -> int:
    """Parse an environment variable as int with a safe fallback."""
    raw = os.getenv(env_var, str(default))
    try:
        return int(raw)
    except (ValueError, TypeError):
        logger.warning("Invalid integer for %s=%r, using default %d", env_var, raw, default)
        return default


def _safe_bool(env_var: str, default: bool) -> bool:
    """Parse an environment variable as bool."""
    raw = os.getenv(env_var)
    if raw is None:
        return default
    return raw.strip().lower() in ("true", "1", "yes")


# OSM download
OSM_URL = os.getenv("osm_url", "")
DATA_DIR = str(Path(__file__).resolve().parent.parent / "data")

# OpenAddresses data directory (CSV/GeoJSON files)
OA_DATA_DIR = os.getenv("OA_DATA_DIR", str(Path(DATA_DIR) / "openaddresses"))

# GeoNames data directory (TSV dump files)
GN_DATA_DIR = os.getenv("GN_DATA_DIR", str(Path(DATA_DIR) / "geonames"))

# NATS
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
NATS_STREAM = "OSM"
NATS_SUBJECT = "osm.elements"

# PostGIS
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = _safe_int("POSTGRES_PORT", 5432)
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# Elasticsearch
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")

# Redis (used by watcher for node location caching)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = _safe_int("REDIS_PORT", 6379)

# Embeddings
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "paraphrase-multilingual-MiniLM-L12-v2")
EMBEDDING_DIM = 384

# Feature flags
ENABLE_VECTORS = _safe_bool("ENABLE_VECTORS", True)
ENABLE_AI = _safe_bool("ENABLE_AI", True)

# Performance tuning
# When vectors are enabled, use smaller batch sizes to avoid timeouts
_BATCH_SIZE = _safe_int("BATCH_SIZE", 100)
BATCH_SIZE = (_BATCH_SIZE // 2) if ENABLE_VECTORS else _BATCH_SIZE

_DEFAULT_WORKERS = 2 if ENABLE_VECTORS else 4
MAX_CONCURRENT_BATCHES = _safe_int("MAX_CONCURRENT_BATCHES", _DEFAULT_WORKERS)
