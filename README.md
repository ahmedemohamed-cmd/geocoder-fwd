# OSM Geocoding Service

A scalable OpenStreetMap geocoding stack with full-text search and geometry
storage. Supports both CPU-only and AI-accelerated (GPU) modes with vector
embeddings.

## Quick start

```bash
# Standard (CPU-only)
docker compose up -d
docker compose run --rm billing-zitadel-init

# AI mode (GPU + vector embeddings) — base file + AI override
docker compose -f docker-compose.yaml -f docker-compose.ai.yaml up -d
```

AI mode requires the NVIDIA Container Toolkit and a compatible GPU. The two
compose files are layered (base + override) so the AI variant only carries the
GPU/vector deltas and can't drift from the base stack.

### Fresh start (wipe and rebuild)

To tear everything down, clear generated data, and rebuild from scratch:

```bash
scripts/fresh-start.sh          # standard stack
scripts/fresh-start.sh --ai     # AI/GPU stack
```

This removes Docker volumes and generated artifacts under `./data` (raw inputs
like `*.osm.pbf` are left untouched). See [scripts/fresh-start.sh](scripts/fresh-start.sh).

## Architecture

The system processes OSM data through a NATS-based pipeline:

1. **Downloader** — Fetches `.osm.pbf` files from Geofabrik
2. **Watcher** — Parses PBF using osmium and publishes elements to NATS JetStream
3. **Inserters** — Consume from NATS and index into:
   - **Elasticsearch** — Full-text + vector search
   - **PostGIS** — Geometry storage (points, lines, polygons, multipolygons)
4. **Geocoder** — FastAPI search service (port 8000)

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NATS_URL` | `nats://nats:4222` | NATS server address |
| `ELASTICSEARCH_URL` | `http://elasticsearch:9200` | Elasticsearch endpoint |
| `REDIS_HOST` | `redis` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `POSTGRES_HOST` | `postgis` | PostGIS host |
| `POSTGRES_PORT` | `5432` | PostGIS port |
| `POSTGRES_DB` | `postgres` | PostGIS database |
| `POSTGRES_USER` | `postgres` | PostGIS user |
| `POSTGRES_PASSWORD` | `postgres` | PostGIS password |
| `EMBEDDING_MODEL` | `paraphrase-multilingual-MiniLM-L12-v2` | Multilingual sentence transformer (50+ languages incl. Arabic) |
| `ENABLE_VECTORS` | `true` (AI) / `false` (standard) | Enable vector embeddings |
| `ENABLE_AI` | `true` (AI) / `false` (standard) | Enable AI-powered features |
| `ENABLE_DEEP` | `true` | Gate the `/deep/*` Google Maps geocoding endpoints |
| `GOOGLE_MAPS_API_KEY` | _(empty)_ | API key for `/deep/*` (endpoints return 503 without it) |
| `OLLAMA_URL` | `http://ollama:11434` | Ollama endpoint for `/describe` AI descriptions |
| `OLLAMA_MODEL` | `qwen2.5:1.5b` | Ollama model used for descriptions |
| `LOG_LEVEL` | `INFO` | Logging verbosity for the Python services |
| `BATCH_SIZE` | `50` (AI) / `100` (standard) | Messages per batch |
| `MAX_CONCURRENT_BATCHES` | `2` (AI) / `4` (standard) | Concurrent batch workers |
| `osm_url` | Egypt PBF | OSM data source URL |
| `DATA_DIR` | `/app/data` | Directory for PBF files |

### OSM Data Source

By default the system downloads Egypt's OSM data from Geofabrik. To use a
different region:

```bash
export osm_url=https://download.geofabrik.de/europe/germany-latest.osm.pbf
docker compose up downloader
```

### Embedding model

The vector model is auto-downloaded on first startup in AI mode and cached under
`./models`. For manual/offline download, alternative models, and cache
configuration, see [docs/MODELS.md](docs/MODELS.md).

## API Usage

Once running, the FastAPI docs are at `http://localhost:8000/docs`.

### Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /geocode` | Full-text + vector + geo search, with address-interpolation fallback |
| `GET /address` | Structured address search (housenumber / street / city / postcode) |
| `GET /autocomplete` | Fast prefix suggestions (Redis sorted sets, ~1–3 ms) |
| `GET /reverse` | Reverse geocoding (nearest address, interpolated address, street, boundaries) |
| `GET /deep/forward`, `GET /deep/reverse` | Deep geocoding via Google Maps (maps→OSM tags, indexed back into the stack) |
| `GET /describe` | AI-generated title + description for a place (Ollama) |
| `POST /feedback` | Popularity feedback loop (boosts future ranking) |
| `POST /places`, `POST /insert` | Add a custom place / raw OSM element |
| `POST /route`, `/optimized_route`, `/sources_to_targets`, `/isochrone`, `/locate`, `GET /status` | Valhalla routing proxy (with Arabic narration) |
| `POST /traffic/probe`, `/traffic/probes`, `GET /traffic/edge` | Live-traffic probe ingestion / per-edge speeds |
| `GET /health`, `GET /features` | Dependency health check / feature-flag discovery |

See [requests.http](requests.http) for a full, runnable set of examples.

### Horizontal scaling

Every serving-path service (geocoder, apisix, billing services, zitadel) is
stateless and replica-safe. The background workers scale as follows:

- **es-inserter / postgis-inserter** — shared durable NATS consumers; run N
  replicas to raise ingest throughput.
- **traffic-aggregator** — probes and provider polling both distribute across
  replicas (`--scale traffic-aggregator=N`): probe batches via a shared durable
  consumer, provider cells via the `TRAFFIC_CELLS` work-queue stream with a
  leaderless Redis `SET NX` scheduler (no duplicate provider calls, no leader).
- **traffic-writer** — per-tile sharding: run N writers with
  `TRAFFIC_WRITER_SHARDS=N` and distinct `TRAFFIC_WRITER_SHARD_INDEX` values;
  each owns disjoint tiles/mmap ranges. On a multi-node deployment run one
  writer next to each Valhalla replica against the node-local `traffic.tar`
  (Redis is the shared source of truth for speeds).
- **oa/gn/places-watchers** — set `PROCESSED_LEDGER=pg` to move the
  processed-file ledger into Postgres with atomic claims, making multiple
  watcher replicas safe (each file imported exactly once).
- **watcher (pbf) / downloader** — one-shot batch jobs by design; idempotent
  re-runs, not replicas.

**Search by name:**
```bash
curl "http://localhost:8000/geocode?q=Cairo&limit=5"
```

**Reverse geocode:**
```bash
curl "http://localhost:8000/reverse?lat=30.0444&lon=31.2357"
```

**Forward address with interpolation (cross-language):**
```bash
# Even though the address points are tagged in Arabic (شارع التحرير), an English
# query resolves the street via Elasticsearch and interpolates the housenumber.
curl "http://localhost:8000/geocode?q=15%20Tahrir%20Street,%20Cairo&lat=30.0444&lon=31.2357&limit=5"
```

### Address interpolation

When a query carries a housenumber that has no exact record, the position is
estimated by linearly interpolating between known addresses on the same street
(odd/even side aware). The street name is resolved through Elasticsearch first,
so a query in one language reaches address points tagged in another (e.g. English
"Tahrir Street" → Arabic شارع التحرير), and `lat`/`lon` disambiguate between
same-named streets by snapping to the nearest cluster. Interpolated results are
returned first with `match_type: "interpolated"`, a `side`, and `bracket_low`/`bracket_high`.

### Search quality (recall)

Measured against a 1,000-place Cairo test set (`tests/run_recall.py`,
`tests/recall_report.md`), queries geo-biased to downtown Cairo:

| metric | @1 | @5 | @10 |
|---|---|---|---|
| named place — strict (same osm_id) | 88.7% | 97.5% | 98.7% |
| named place — lenient (name or ≤150 m) | 96.3% | 99.1% | 99.3% |
| address — exact (same osm_id) | 90.8% | 95.2% | 97.2% |
| address — correct street | 96.4% | 97.6% | 98.0% |

Cross-language address interpolation raises the interpolation hit-rate on
unmatched housenumbers from 9.6% to 25.2%.

## Development

### Running locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run services individually
python -m services.watcher
python -m services.es_inserter
python -m services.geocoder
```

### Tests & linting

```bash
pip install -e ".[dev,test]"

pytest                  # unit + smoke suite (no infra; integration tests skipped)
pytest -m integration --override-ini addopts=   # against a live stack

ruff check .            # lint
ruff format .           # format
```

CI (`.github/workflows/ci.yml`) runs ruff and the infra-free test suite on every
push/PR.

### Building the Docker image

```bash
docker build -t geocoder:latest .
```

## Maintenance

### Clearing data

```bash
docker compose run cleaner
```

## Project Structure

```
.
├── services/                 # Python microservices
│   ├── watcher.py            # PBF parser
│   ├── es_inserter.py        # Elasticsearch indexer
│   ├── postgis_inserter.py   # PostGIS indexer
│   ├── geocoder.py           # FastAPI search service
│   ├── routing.py            # Valhalla routing proxy + router
│   ├── downloader.py         # OSM data downloader
│   └── cleaner.py            # Data cleanup utility
├── shared/                   # Shared utilities (config, logging, NATS client, …)
├── billing/                  # API-key management / metering / billing subsystem
├── tests/                    # billing, geocoder smoke, helpers, integration
├── docs/                     # MODELS, performance & parallel-processing guides
├── docker-compose.yaml       # Standard deployment
├── docker-compose.ai.yaml    # AI/GPU override (use with the base file)
└── requirements.txt          # Python dependencies
```

## Performance

See [docs/PERFORMANCE_OPTIMIZATIONS.md](docs/PERFORMANCE_OPTIMIZATIONS.md) and
[docs/PARALLEL_PROCESSING_GUIDE.md](docs/PARALLEL_PROCESSING_GUIDE.md).

## License

[Add your license here]
