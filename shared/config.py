import os
from pathlib import Path
from dotenv import load_dotenv

# Try to load .env file if it exists (won't fail if missing)
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

# OSM download
OSM_URL = os.getenv("osm_url", "")
DATA_DIR = str(Path(__file__).resolve().parent.parent / "data")

# NATS
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
NATS_STREAM = "OSM"
NATS_SUBJECT = "osm.elements"

# PostGIS
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# Elasticsearch
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")

# Redis (used by watcher for node location caching)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Embeddings
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "paraphrase-multilingual-MiniLM-L12-v2")  # Hugging Face model name
EMBEDDING_DIM = 384

# Feature flags
ENABLE_VECTORS = os.getenv("ENABLE_VECTORS", "true").lower() in ("true", "1", "yes")
ENABLE_AI = os.getenv("ENABLE_AI", "true").lower() in ("true", "1", "yes")

# Performance tuning
# When vectors are enabled, use smaller batch sizes to avoid timeouts
_BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))  # Reduced from 2000 to reduce NATS load
BATCH_SIZE = _BATCH_SIZE // 2 if ENABLE_VECTORS else _BATCH_SIZE  # Smaller batches when generating embeddings
MAX_CONCURRENT_BATCHES = int(os.getenv("MAX_CONCURRENT_BATCHES", "2")) if ENABLE_VECTORS else int(os.getenv("MAX_CONCURRENT_BATCHES", "4"))
