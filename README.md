# OSM Geocoding Service

A scalable OpenStreetMap geocoding stack with full-text search and geometry storage. Supports both CPU-only and AI-accelerated (GPU) modes with vector embeddings.

## Architecture

The system processes OSM data through a NATS-based pipeline:

1. **Downloader** — Fetches `.osm.pbf` files from Geofabrik
2. **Watcher** — Parses PBF using osmium and publishes elements to NATS JetStream
3. **Inserters** — Consume from NATS and index into:
   - **Elasticsearch** — Full-text + vector search
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
| `REDIS_HOST` | `redis` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `POSTGRES_HOST` | `postgis` | PostGIS host |
| `POSTGRES_PORT` | `5432` | PostGIS port |
| `POSTGRES_DB` | `postgres` | PostGIS database |
| `POSTGRES_USER` | `postgres` | PostGIS user |
| `POSTGRES_PASSWORD` | `postgres` | PostGIS password |
| `EMBEDDING_MODEL` | `paraphrase-multilingual-MiniLM-L12-v2` | Multilingual sentence transformer model (supports 50+ languages including Arabic) |
| `ENABLE_VECTORS` | `true` (AI mode) / `false` (standard) | Enable vector embeddings |
| `ENABLE_AI` | `true` (AI mode) / `false` (standard) | Enable AI-powered features |
| `BATCH_SIZE` | `50` (AI mode) / `100` (standard) | Messages per batch (reduced in AI mode for GPU efficiency) |
| `MAX_CONCURRENT_BATCHES` | `2` (AI mode) / `4` (standard) | Concurrent batch processing workers |
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

## Model Information

The system uses the `paraphrase-multilingual-MiniLM-L12-v2` sentence transformer model for vector embeddings. This model supports 50+ languages including Arabic and provides high-quality semantic search capabilities.

### Automatic Model Download (Recommended)

When running in AI mode, the embedding model is automatically downloaded from Hugging Face on first startup and cached in the `models/` directory. No manual download is required.

**How it works:**
1. Start the services with `docker-compose -f docker-compose-ai.yaml up -d`
2. The inserter services check if the model exists locally
3. If not found, they automatically download from Hugging Face
4. The model is cached in `/app/models` inside the container
5. Volume mount persists the model to `./models/` on the host
6. Subsequent restarts use the cached model

**Automatic download locations:**
- **Inside container**: `/app/models/` (set via `TRANSFORMERS_CACHE` and `HF_HOME` environment variables)
- **Host mount**: `./models/` (mounted from host to container for persistence)

**Automatic download trigger:**
The system downloads automatically when:
- `EMBEDDING_MODEL` is set to a Hugging Face model name (e.g., `paraphrase-multilingual-MiniLM-L12-v2`)
- The model directory doesn't exist locally
- The service starts and needs to generate embeddings

**Verification:**
```bash
# Check if models exist
ls -la models/

# Start services (will download if missing)
docker-compose -f docker-compose-ai.yaml up -d

# Check logs for download progress
docker-compose -f docker-compose-ai.yaml logs -f es-inserter
```

**Important notes:**
- The download happens on first service startup, not during docker build
- Download time varies by model size (100-500MB) and network speed
- The model is persisted across container restarts via volume mount
- If you delete the `models/` directory, it will be re-downloaded on next startup

### Manual Model Download

If you prefer to download the model manually (e.g., for offline environments, pre-warming caches, or troubleshooting), you can use one of the following methods:

#### Method 1: Hugging Face CLI (Recommended)

1. On many enterprise Linux systems, python does not exist — use python3.
```bash
python3 --version```
On many enterprise Linux systems, python does not exist — use python3.

2. Ensure huggingface_hub is installed (user install is fine)

```bash
pip install --upgrade huggingface_hub```

3. download the model
```bash
python3 - << 'EOF'
from huggingface_hub import snapshot_download

snapshot_download(
    repo_id="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
    local_dir="./models/paraphrase-multilingual-Mini

```
#### Method 2: Python Script

```bash
# Install sentence-transformers
pip install sentence-transformers

# Download the model using Python
python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2').save('./models/paraphrase-multilingual-MiniLM-L12-v2')"
```

#### Method 3: Git Clone from Hugging Face

```bash
# Clone the model repository (includes metadata and some files)
git clone https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2 ./models/paraphrase-multilingual-MiniLM-L12-v2
```

**Note**: Git clone may not include all model weights. Use Method 1 or 2 for complete downloads.

### Model Cache Configuration

The system uses the following environment variables to control model caching:

| Variable | Default | Purpose |
|----------|---------|---------|
| `TRANSFORMERS_CACHE` | `/app/models` | Cache directory for Hugging Face transformers |
| `HF_HOME` | `/app/models` | Hugging Face home directory |
| `EMBEDDING_MODEL` | `paraphrase-multilingual-MiniLM-L12-v2` | Hugging Face model name or local path |

**Docker Compose volume mount:**
```yaml
volumes:
  - ./models:/app/models  # Persist models on host
```

### Using a Different Model

To use a different embedding model, set the `EMBEDDING_MODEL` environment variable to the Hugging Face model name:

```bash
# Docker Compose
export EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
docker-compose -f docker-compose-ai.yaml up -d

# Or modify docker-compose-ai.yaml directly
environment:
  - EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
```

**Popular sentence transformer models:**
- `sentence-transformers/all-MiniLM-L6-v2` - English only, faster (384 dim)
- `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` - Multilingual, 50+ languages (384 dim)
- `sentence-transformers/all-mpnet-base-v2` - English only, higher accuracy (768 dim)

### Model Storage and .gitignore

The `models/` directory is included in `.gitignore` to prevent committing large model files to the repository. Model files are typically 100-500MB each.

**Current .gitignore rules:**
```
models/.locks/
models/models--*/
models/xet/
models/CACHEDIR.TAG
*.lock
*.incomplete
```

**To commit model files (not recommended):**
- Remove or modify `.gitignore` rules
- Use Git LFS for large file management
- Consider using git submodules (see below)

### Using Git Submodules for Models (Advanced)

Hugging Face models can be used as git submodules, which allows you to track specific model versions and update them easily. This approach requires Git LFS to be installed.

#### Benefits of Git Submodules:
- Track specific model versions via git commits
- Easy updates with `git submodule update`
- Separates model code from application code
- Smaller main repository (models stored separately)

#### Drawbacks of Git Submodules:
- Requires Git LFS installation and setup
- Adds complexity to git workflow
- Larger clone times (downloads LFS files)
- Requires additional setup for new developers

#### Setting Up Git Submodules:

**Step 1: Install Git LFS**
```bash
# Ubuntu/Debian
sudo apt-get install git-lfs

# macOS
brew install git-lfs

# Initialize Git LFS
git lfs install
```

**Step 2: Add Model as Submodule**
```bash
# Remove existing models directory if present
rm -rf models/paraphrase-multilingual-MiniLM-L12-v2

# Add the model as a git submodule
git submodule add https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2 models/paraphrase-multilingual-MiniLM-L12-v2

# Add another model if needed
git submodule add https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2 models/all-MiniLM-L6-v2
```

**Step 3: Update .gitmodules (Optional)**
The `.gitmodules` file will be created automatically:
```ini
[submodule "models/paraphrase-multilingual-MiniLM-L12-v2"]
	path = models/paraphrase-multilingual-MiniLM-L12-v2
	url = https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2
```

**Step 4: Commit Submodule Changes**
```bash
git add .gitmodules models/
git commit -m "Add Hugging Face models as git submodules"
```

#### Cloning Repository with Submodules:

**New developers cloning the repo:**
```bash
# Clone repository with submodules
git clone --recurse-submodules https://github.com/ahmedemohamed-cmd/geocoder-fwd.git

# Or if already cloned:
git submodule update --init --recursive
```

#### Updating Model Submodules:

```bash
# Update all submodules to latest commits
git submodule update --remote

# Update specific submodule
cd models/paraphrase-multilingual-MiniLM-L12-v2
git pull origin main
cd ../..
git add models/paraphrase-multilingual-MiniLM-L12-v2
git commit -m "Update paraphrase-multilingual-MiniLM-L12-v2 model"
```

#### Removing Submodules:

```bash
# Remove submodule
git submodule deinit -f models/paraphrase-multilingual-MiniLM-L12-v2
git rm -f models/paraphrase-multilingual-MiniLM-L12-v2
rm -rf .git/modules/models/paraphrase-multilingual-MiniLM-L12-v2
```

#### Recommendation:

**Use automatic download (default)** for most cases:
- Simpler setup
- No Git LFS required
- Automatic updates when model changes
- Smaller repository size

**Use git submodules** if you need:
- Strict version control over model files
- Offline capability with pre-downloaded models
- Ability to audit model changes
- Integration with existing git-based workflows

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

## Performance Configuration

### AI Mode (GPU + Vectors)
- **Batch Size**: 50 messages per batch (reduced for GPU efficiency)
- **Concurrent Workers**: 2 workers per inserter
- **Vector Generation**: GPU-accelerated using CUDA
- **Expected Throughput**: ~100-300 docs/sec per inserter

### Standard Mode (CPU-only)
- **Batch Size**: 100 messages per batch
- **Concurrent Workers**: 4 workers per inserter
- **Vector Generation**: Disabled by default
- **Expected Throughput**: ~500-1000 docs/sec per inserter

For more detailed performance tuning information, see [PERFORMANCE_OPTIMIZATIONS.md](PERFORMANCE_OPTIMIZATIONS.md) and [PARALLEL_PROCESSING_GUIDE.md](PARALLEL_PROCESSING_GUIDE.md).

## License

[Add your license here]
