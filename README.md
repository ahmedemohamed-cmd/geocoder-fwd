# OSM Geocoding Service

A scalable OpenStreetMap geocoding stack with full-text search, typo-tolerant search, and geometry storage. Supports both CPU-only and AI-accelerated (GPU) modes with vector embeddings.

## Architecture

The system processes OSM data through a NATS-based pipeline:

1. **Downloader** — Fetches `.osm.pbf` files from Geofabrik
2. **Watcher** — Parses PBF using osmium and publishes elements to NATS JetStream
3. **Inserters** — Consume from NATS and index into:
   - **Elasticsearch** — Full-text + vector search
   - **Typesense** — Typo-tolerant search
   - **PostGIS** — Geometry storage (points, lines, polygons, multipolygons)
4. **Geocoder** — FastAPI search service (port 8000)

## Deployment Options

### Docker Compose

**Standard mode (CPU-only):**
```bash
docker-compose up -d
```

**AI mode (GPU + vector embeddings):**
```bash
docker-compose -f docker-compose-ai.yaml up -d
```

Requires NVIDIA Container Toolkit and a compatible GPU.

### Kubernetes (Helm)

**Standard mode (CPU-only):**
```bash
helm install geocoder ./helm/geocoder
```

**AI mode (GPU + vector embeddings):**
```bash
helm install geocoder ./helm/geocoder -f ./helm/geocoder/values-ai.yaml
```

See [helm/geocoder/README.md](helm/geocoder/README.md) for detailed Helm chart documentation, including scheduling, storage classes, and configuration options.

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NATS_URL` | `nats://nats:4222` | NATS server address |
| `ELASTICSEARCH_URL` | `http://elasticsearch:9200` | Elasticsearch endpoint |
| `TYPESENSE_HOST` | `typesense` | Typesense host |
| `TYPESENSE_PORT` | `8108` | Typesense port |
| `TYPESENSE_API_KEY` | `typesense-secret-key` | Typesense API key |
| `POSTGRES_HOST` | `postgis` | PostGIS host |
| `POSTGRES_PORT` | `5432` | PostGIS port |
| `POSTGRES_DB` | `postgres` | PostGIS database |
| `POSTGRES_USER` | `postgres` | PostGIS user |
| `POSTGRES_PASSWORD` | `postgres` | PostGIS password |
| `EMBEDDING_MODEL` | `paraphrase-multilingual-MiniLM-L12-v2` | Multilingual sentence transformer model (supports 50+ languages including Arabic) |
| `ENABLE_VECTORS` | `false` | Enable vector embeddings |
| `ENABLE_AI` | `false` | Enable AI-powered features |
| `osm_url` | Egypt PBF | OSM data source URL |
| `DATA_DIR` | `/app/data` | Directory for PBF files |

### OSM Data Source

By default, the system downloads Egypt's OSM data from Geofabrik. To use a different region:

**Docker Compose:**
```bash
export osm_url=https://download.geofabrik.de/europe/germany-latest.osm.pbf
docker-compose up downloader
```

**Helm:**
```bash
helm install geocoder ./helm/geocoder --set osm.url=https://download.geofabrik.de/europe/germany-latest.osm.pbf
```

## API Usage

Once the geocoder service is running, access the FastAPI documentation at:

```
http://localhost:8000/docs
```

### Example Endpoints

**Search by name:**
```bash
curl "http://localhost:8000/search?q=Cairo"
```

**Reverse geocode:**
```bash
curl "http://localhost:8000/reverse?lat=30.0444&lon=31.2357"
```

**Autocomplete:**
```bash
curl "http://localhost:8000/autocomplete?q=Tahr"
```

## Manual Model Download

If you need to download the embedding model manually (e.g., for offline environments or to avoid download delays), you can download it to the `models/` directory:

### Using Hugging Face CLI

```bash
# Install huggingface-cli if not already installed
pip install huggingface-hub

# Download the default model (paraphrase-multilingual-MiniLM-L12-v2)
huggingface-cli download sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2 --local-dir models/paraphrase-multilingual-MiniLM-L12-v2
```

### Using Python

```bash
# Install sentence-transformers
pip install sentence-transformers

# Download the model using Python
python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2').save('models/paraphrase-multilingual-MiniLM-L12-v2')"
```

### Using a Different Model

To use a different model, download it to the `models/` directory and update the `EMBEDDING_MODEL` environment variable to point to the local path:

```bash
# Download a different model
huggingface-cli download sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2 --local-dir models/paraphrase-multilingual-MiniLM-L12-v2

# Set the environment variable to use the local model
export EMBEDDING_MODEL=models/paraphrase-multilingual-MiniLM-L12-v2
```

### Docker Compose with Local Model

When using Docker Compose with a locally downloaded model, mount the models directory and set the model path:

```bash
docker-compose -f docker-compose-ai.yaml up -d
```

The `docker-compose-ai.yaml` already includes the volume mount for the models directory and sets `EMBEDDING_MODEL=/app/models/paraphrase-multilingual-MiniLM-L12-v2`.

## Development

### Running locally

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run services individually
python -m services.watcher
python -m services.es_inserter
python -m services.geocoder
```

### Building the Docker image

```bash
docker build -t ahmedemohamed-cmd:latest .
```

## Maintenance

### Clearing data

**Docker Compose:**
```bash
docker-compose run cleaner
```

**Helm:**
```bash
helm upgrade geocoder ./helm/geocoder --set cleaner.enabled=true
```

### Manual download trigger (Helm)

```bash
kubectl create job --from=cronjob/geocoder-downloader manual-$(date +%s)
```

## Project Structure

```
.
├── services/              # Python microservices
│   ├── watcher.py        # PBF parser
│   ├── es_inserter.py    # Elasticsearch indexer
│   ├── ts_inserter.py    # Typesense indexer
│   ├── postgis_inserter.py # PostGIS indexer
│   ├── geocoder.py       # FastAPI search service
│   ├── downloader.py     # OSM data downloader
│   └── cleaner.py        # Data cleanup utility
├── shared/               # Shared utilities (NATS client, etc.)
├── models/               # Sentence transformer models
├── docker-compose.yaml   # Standard deployment
├── docker-compose-ai.yaml # AI/GPU deployment
├── helm/geocoder/        # Kubernetes Helm chart
└── requirements.txt      # Python dependencies
```

## License

[Add your license here]
