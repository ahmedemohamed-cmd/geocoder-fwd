# Embedding Models

The system uses the `paraphrase-multilingual-MiniLM-L12-v2` sentence-transformer
model for vector embeddings. It supports 50+ languages (including Arabic) and
produces 384-dimensional vectors for semantic search.

Vectors are only used in **AI mode**
(`docker compose -f docker-compose.yaml -f docker-compose.ai.yaml up -d`).

## Automatic download (recommended)

In AI mode the model is downloaded from Hugging Face on first startup and cached
in the `models/` directory — no manual step required.

How it works:
1. Start the services in AI mode.
2. The inserter services check whether the model exists locally.
3. If not found, they download it from Hugging Face.
4. It is cached at `/app/models` inside the container.
5. The `./models:/app/models` volume mount persists it on the host.
6. Subsequent restarts reuse the cache.

Locations:
- **Inside container**: `/app/models/` (set via `TRANSFORMERS_CACHE` and `HF_HOME`).
- **Host mount**: `./models/`.

Notes:
- The download happens on first service startup, not during `docker build`.
- Download size is ~100–500 MB depending on the model.
- Delete `models/` to force a re-download on next startup.

## Manual download

For offline environments or pre-warming, download ahead of time.

### Hugging Face CLI

```bash
# On many enterprise Linux systems use python3, not python.
pip install --upgrade huggingface_hub
python3 - <<'EOF'
from huggingface_hub import snapshot_download
snapshot_download(
    repo_id="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
    local_dir="./models/paraphrase-multilingual-MiniLM-L12-v2",
)
EOF
```

### sentence-transformers

```bash
pip install sentence-transformers
python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2').save('./models/paraphrase-multilingual-MiniLM-L12-v2')"
```

### git clone (may omit weights)

```bash
git clone https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2 ./models/paraphrase-multilingual-MiniLM-L12-v2
```

> Git clone may not include all model weights; prefer the CLI or
> sentence-transformers methods for complete downloads.

## Cache configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `TRANSFORMERS_CACHE` | `/app/models` | Hugging Face transformers cache dir |
| `HF_HOME` | `/app/models` | Hugging Face home dir |
| `EMBEDDING_MODEL` | `paraphrase-multilingual-MiniLM-L12-v2` | HF model name or local path |

Docker Compose volume mount:

```yaml
volumes:
  - ./models:/app/models  # persist models on host
```

## Using a different model

Set `EMBEDDING_MODEL` to any Hugging Face model name:

```bash
export EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
docker compose -f docker-compose.yaml -f docker-compose.ai.yaml up -d
```

Popular options:
- `sentence-transformers/all-MiniLM-L6-v2` — English only, faster (384 dim)
- `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` — multilingual, 50+ languages (384 dim)
- `sentence-transformers/all-mpnet-base-v2` — English only, higher accuracy (768 dim)

> Changing the embedding dimension requires reindexing; `EMBEDDING_DIM` in
> `shared/config.py` must match the model.

## Storage and .gitignore

Model files (100–500 MB each) are excluded from git:

```
models/.locks/
models/models--*/
models/xet/
models/CACHEDIR.TAG
*.lock
*.incomplete
```

For strict version pinning or offline workflows you can vendor a model as a Git
LFS submodule instead of relying on auto-download — at the cost of requiring Git
LFS and larger clones. Auto-download is recommended for most cases.
