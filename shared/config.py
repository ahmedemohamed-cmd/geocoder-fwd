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

# Curated place exports (JSON arrays: Pelias google + Postgres `places` dumps)
PLACES_DATA_DIR = os.getenv("PLACES_DATA_DIR", str(Path(DATA_DIR) / "places"))

# How often the file-importing services (watcher, oa-watcher, gn-watcher)
# re-scan their data dirs for newly-added files, in seconds. They poll rather
# than rely on inotify/watchdog because the data dirs are Docker bind mounts and
# host-side file drops don't reliably emit inotify events into the container.
WATCH_POLL_INTERVAL = _safe_int("WATCH_POLL_INTERVAL", 30)

# NATS
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
NATS_STREAM = "OSM"
NATS_SUBJECT = "osm.elements"
# JetStream replication factor for our streams. 1 for the single-node compose
# deployment; set 3 against a clustered NATS so streams survive a server loss.
NATS_STREAM_REPLICAS = _safe_int("NATS_STREAM_REPLICAS", 1)

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
# Topology: "standalone" (default; compose) or "cluster" (Redis Cluster; k8s).
# All clients are built via shared.redis_client so the mode is applied uniformly.
REDIS_MODE = os.getenv("REDIS_MODE", "standalone").strip().lower()
# Cluster bootstrap nodes, "host1:6379,host2:6379". Empty falls back to
# REDIS_HOST:REDIS_PORT as the sole startup node (fine behind a k8s Service).
REDIS_NODES = os.getenv("REDIS_NODES", "")

# Redis result cache for /geocode and /reverse (cache-aside; falls back to ES on
# miss). TTL bounds staleness from background enrichment / popularity feedback.
GEOCODE_CACHE_ENABLED = _safe_bool("GEOCODE_CACHE_ENABLED", True)
GEOCODE_CACHE_TTL = _safe_int("GEOCODE_CACHE_TTL", 300)  # seconds
# lat/lon rounded to this many decimals for the cache key (2 ≈ 1.1 km, well
# inside the 10 km geo-decay scale — big hit-rate gain, negligible ranking shift).
GEOCODE_CACHE_COORD_PRECISION = _safe_int("GEOCODE_CACHE_COORD_PRECISION", 2)

# ── /nearby (explore nearby places, filterable by category) ────────────────
# Its own cache instance, NOT the /geocode one: nearby returns a per-result
# distance_m, so the coarse 2-decimal (~1.1 km) /geocode key rounding would
# corrupt both the result set and the distances. 4 decimals ≈ 11 m.
NEARBY_CACHE_ENABLED = _safe_bool("NEARBY_CACHE_ENABLED", True)
NEARBY_CACHE_TTL = _safe_int("NEARBY_CACHE_TTL", 120)  # seconds
NEARBY_CACHE_COORD_PRECISION = _safe_int("NEARBY_CACHE_COORD_PRECISION", 4)  # ~11 m
NEARBY_DEFAULT_RADIUS_M = _safe_int("NEARBY_DEFAULT_RADIUS_M", 1000)
NEARBY_MAX_RADIUS_M = _safe_int("NEARBY_MAX_RADIUS_M", 50000)
# Drop area features bigger than this (districts, cities) while keeping large
# legitimate POIs like airports and big parks. Stricter than the 0.1 km² used
# for nearest-street centroid reliability in geocoder.py.
NEARBY_MAX_AREA_KM2 = float(os.getenv("NEARBY_MAX_AREA_KM2", "5.0"))

# Embeddings
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "paraphrase-multilingual-MiniLM-L12-v2")
EMBEDDING_DIM = 384

# Feature flags
ENABLE_VECTORS = _safe_bool("ENABLE_VECTORS", True)
ENABLE_AI = _safe_bool("ENABLE_AI", True)

# Ollama LLM (place descriptions)
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b")

# Google Maps (deep forward/reverse geocoding via an external provider)
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")
GOOGLE_MAPS_GEOCODE_URL = os.getenv(
    "GOOGLE_MAPS_GEOCODE_URL", "https://maps.googleapis.com/maps/api/geocode/json"
)
# Places API (New) Text Search — powers /deep/nearby. Same API key, but the
# "Places API (New)" product must be enabled on the project. Text Search is used
# (not searchNearby) because only it returns a nextPageToken for scrolling.
GOOGLE_PLACES_SEARCH_TEXT_URL = os.getenv(
    "GOOGLE_PLACES_SEARCH_TEXT_URL",
    "https://places.googleapis.com/v1/places:searchText",
)
# Note: result language is a mandatory per-request query param on /deep/*,
# not an env default.
# ENABLE_DEEP gates the /deep/* endpoints; they also require an API key.
ENABLE_DEEP = _safe_bool("ENABLE_DEEP", True)

# ── Live traffic ───────────────────────────────────────────────────────────
# Crowdsourced GPS probes from app users (and an optional external flow API) are
# aggregated into per-edge speeds and written into Valhalla's memory-mapped
# traffic.tar. ENABLE_TRAFFIC gates the probe-ingestion endpoints on the geocoder.
ENABLE_TRAFFIC = _safe_bool("ENABLE_TRAFFIC", False)

# Valhalla routing engine (used for map-matching probes -> edges and for /locate).
VALHALLA_URL = os.getenv("VALHALLA_URL", "http://valhalla:8002")

# Dedicated NATS stream for the high-volume, disposable probe firehose. Kept
# separate from the OSM stream (different retention) so probe load can't evict
# OSM ingest messages.
TRAFFIC_STREAM = "TRAFFIC"
TRAFFIC_SUBJECT = "traffic.probes"

# Work-queue stream distributing provider poll cells across aggregator replicas
# (WORKQUEUE retention: each cell message is consumed by exactly one worker).
TRAFFIC_CELLS_STREAM = "TRAFFIC_CELLS"
TRAFFIC_CELLS_SUBJECT = "traffic.cells"

# Path to Valhalla's traffic extract, as seen by the traffic-writer container
# (shares the ./data bind mounted at /custom_files; Valhalla keeps its files
# under the valhalla/ subdir via path_extension=valhalla).
TRAFFIC_EXTRACT_PATH = os.getenv("TRAFFIC_EXTRACT_PATH", "/custom_files/valhalla/traffic.tar")

# Writer cadence and aggregation tuning.
TRAFFIC_WRITE_INTERVAL = _safe_int("TRAFFIC_WRITE_INTERVAL", 30)  # seconds between flushes
TRAFFIC_EDGE_TTL = _safe_int("TRAFFIC_EDGE_TTL", 600)  # secs before an edge speed expires
TRAFFIC_MIN_SAMPLES = _safe_int("TRAFFIC_MIN_SAMPLES", 3)  # min probes before a speed is trusted

# Horizontal sharding for the traffic-writer: each replica owns the edges whose
# tile satisfies tile_base_id % SHARDS == SHARD_INDEX, so concurrent writers
# touch disjoint tiles (and disjoint mmap byte ranges) with no locking.
# SHARD_INDEX defaults to the trailing ordinal of the hostname (StatefulSet pod
# names are <name>-<ordinal>); 1 shard = the single-writer compose behavior.
TRAFFIC_WRITER_SHARDS = _safe_int("TRAFFIC_WRITER_SHARDS", 1)
TRAFFIC_WRITER_SHARD_INDEX = os.getenv("TRAFFIC_WRITER_SHARD_INDEX", "")


def _safe_float(env_var: str, default: float) -> float:
    raw = os.getenv(env_var, str(default))
    try:
        return float(raw)
    except (ValueError, TypeError):
        logger.warning("Invalid float for %s=%r, using default %s", env_var, raw, default)
        return default


TRAFFIC_EWMA_ALPHA = _safe_float("TRAFFIC_EWMA_ALPHA", 0.3)  # smoothing for per-edge speed
TRAFFIC_MAX_TRACE = _safe_int("TRAFFIC_MAX_TRACE", 50)  # max probes per map-match call

# Optional external flow provider (gap-filler / cold-start booster). "none"
# disables it; "tomtom" needs TOMTOM_API_KEY. Probe data always takes priority.
TRAFFIC_PROVIDER = os.getenv("TRAFFIC_PROVIDER", "none")
TRAFFIC_PROVIDER_INTERVAL = _safe_int(
    "TRAFFIC_PROVIDER_INTERVAL", 120
)  # secs between provider polls
TRAFFIC_PROVIDER_WEIGHT = _safe_float("TRAFFIC_PROVIDER_WEIGHT", 0.5)  # confidence vs. fresh probes
# Bounding box the provider polls: "min_lat,min_lon,max_lat,max_lon" (default: Greater Cairo).
TRAFFIC_PROVIDER_BBOX = os.getenv("TRAFFIC_PROVIDER_BBOX", "29.7,31.0,30.2,31.5")
# Provider sampling grid: N×N query points across the bbox per poll. Keep small
# to stay within free-tier rate limits (8×8 = 64 calls/poll by default).
TRAFFIC_PROVIDER_GRID = _safe_int("TRAFFIC_PROVIDER_GRID", 8)
# In-process consumers of the traffic.cells work queue per aggregator replica
# (cross-replica scaling comes from running more replicas on the same durable).
TRAFFIC_CELL_WORKERS = _safe_int("TRAFFIC_CELL_WORKERS", 2)
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "")
TOMTOM_FLOW_URL = os.getenv(
    "TOMTOM_FLOW_URL",
    "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json",
)

# ── Demand-driven traffic fetch (the /route?traffic=true fallback) ──────────
# Instead of (or besides) the fixed grid poller, fetch live speeds from the
# external provider ON DEMAND for the edges of a traffic-annotated route that
# have no recent data in Redis. Cache-first: only misses cost a call; results
# are written to the same tf:e:* keys (TTL below) so repeat routes over the same
# corridor are free. Needs TOMTOM_API_KEY. Off by default (compose enables it).
TRAFFIC_FETCH_ON_DEMAND = _safe_bool("TRAFFIC_FETCH_ON_DEMAND", False)
# Per-request ceiling on provider calls (one call covers one ~window of road).
TRAFFIC_FETCH_MAX_CALLS = _safe_int("TRAFFIC_FETCH_MAX_CALLS", 15)
# Uncovered edges are grouped into windows of up to this length; one provider
# call per window, its speed applied to every edge in the window.
TRAFFIC_FETCH_WINDOW_KM = _safe_float("TRAFFIC_FETCH_WINDOW_KM", 2.0)
# TTL (secs) on fetched per-edge speeds — how long a corridor stays "warm".
TRAFFIC_FETCH_TTL = _safe_int("TRAFFIC_FETCH_TTL", 600)
# Global daily provider-call budget (Redis counter, resets on the calendar day).
# Keep under the plan's daily limit; over budget → edges stay unknown (no error).
TRAFFIC_FETCH_DAILY_BUDGET = _safe_int("TRAFFIC_FETCH_DAILY_BUDGET", 2000)
# TTL (secs) on the "provider has no data here" negative marker (tf:nd:*), so we
# don't re-query the provider every request for roads it doesn't cover.
TRAFFIC_FETCH_NEGATIVE_TTL = _safe_int("TRAFFIC_FETCH_NEGATIVE_TTL", 300)

# Performance tuning
# When vectors are enabled, use smaller batch sizes to avoid timeouts
_BATCH_SIZE = _safe_int("BATCH_SIZE", 500)
BATCH_SIZE = (_BATCH_SIZE // 2) if ENABLE_VECTORS else _BATCH_SIZE

_DEFAULT_WORKERS = 2 if ENABLE_VECTORS else 4
MAX_CONCURRENT_BATCHES = _safe_int("MAX_CONCURRENT_BATCHES", _DEFAULT_WORKERS)
