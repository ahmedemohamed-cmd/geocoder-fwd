# Geocoder Helm Chart

Deploys the OSM geocoding stack on Kubernetes, mirroring the two Docker Compose
files (`docker-compose.yaml` and `docker-compose-ai.yaml`).

For project overview and Docker Compose deployment, see the [main README](../../README.md).

## Components

**Infrastructure (StatefulSets with persistent storage):**
- `nats` — JetStream message queue (pipeline backbone)
- `redis` — key/value cache (node location lookups)
- `elasticsearch` — full-text + vector index
- `postgis` — geometry storage

**Application services (Deployments):**
- `es-inserter` — consumes NATS and indexes into Elasticsearch
- `postgis-inserter` — consumes NATS and stores geometry in PostGIS
- `geocoder` — FastAPI search service (port 8000)

**Scheduled jobs (CronJobs, monthly):**
- `downloader` — pulls the latest `.osm.pbf` (default: `0 0 1 * *` → 1st of each month, 00:00)
- `watcher` — parses the PBF and publishes to NATS (default: `30 0 1 * *` → 30 min after the downloader)

**Manual Job:**
- `cleaner` — wipes all indices/tables/streams (disabled by default)

## Installing

### Standard (CPU-only) mode

Mirrors `docker-compose.yaml`:

```bash
helm install geocoder ./helm/geocoder
```

### AI mode (GPU + vector embeddings)

Mirrors `docker-compose-ai.yaml`. Requires a Kubernetes cluster with the NVIDIA
device plugin installed (`nvidia.com/gpu` resource available).

```bash
helm install geocoder ./helm/geocoder -f ./helm/geocoder/values-ai.yaml
```

## Scheduling

The downloader and watcher run **monthly** by default. The watcher is
scheduled 30 minutes after the downloader so the file has time to land on the
shared `-data` PVC before parsing begins.

Override with standard cron expressions:

```yaml
downloader:
  schedule: "0 0 1 * *"    # 1st of month at 00:00
watcher:
  schedule: "30 0 1 * *"   # 1st of month at 00:30
```

If you prefer the watcher to run continuously (as in Docker Compose), set:

```yaml
watcher:
  runAsDeployment: true
```

## Shared data volume

The downloader writes PBFs into a `ReadWriteOnce` PVC named
`<release>-data`; the watcher reads from the same PVC. If your cluster cannot
co-schedule these jobs on the same node, switch to a `ReadWriteMany`
storage class (e.g. NFS, CephFS) by setting
`dataVolume.accessMode: ReadWriteMany` and `global.storageClass: <rwx-class>`.

## Building the app image

The chart expects the application image to be pushed to a registry reachable by
the cluster:

```bash
docker build -t <registry>/ahmedemohamed-cmd:latest .
docker push <registry>/ahmedemohamed-cmd:latest
```

Then:

```bash
helm install geocoder ./helm/geocoder \
  --set global.imageRegistry=<registry> \
  --set app.image.tag=latest
```

## Common overrides

| Key | Default | Purpose |
| --- | --- | --- |
| `global.aiEnabled` | `false` | Switch to AI/vector mode |
| `gpu.enabled` | `false` | Request `nvidia.com/gpu` resources |
| `osm.url` | Egypt PBF | Which region to download |
| `downloader.schedule` | `0 0 1 * *` | Monthly download |
| `watcher.schedule` | `30 0 1 * *` | Monthly parse |
| `geocoder.ingress.enabled` | `false` | Expose the API via Ingress |
| `cleaner.enabled` | `false` | Run the cleaner Job on upgrade |

See `values.yaml` for the full list.

## Uninstalling

```bash
helm uninstall geocoder
```

PVCs are retained by default; delete them explicitly if you want a clean slate:

```bash
kubectl delete pvc -l app.kubernetes.io/instance=geocoder
```
