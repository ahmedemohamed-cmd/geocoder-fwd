# Performance Optimization Guide

## Implemented Optimizations

### 1. Increased Batch Size (✅ Applied)
- **Before**: 100 messages per batch
- **After**: 500 messages per batch (configurable via `BATCH_SIZE` env var)
- **Impact**: 5x reduction in network overhead and database round-trips
- **Configuration**: Set `BATCH_SIZE` environment variable (default: 500)

## Additional Optimization Strategies

### 2. Parallel Processing (Recommended)
**Current**: Sequential processing of batches
**Suggested**: Process multiple batches in parallel

```python
# Add to inserters for concurrent batch processing
async def process_batch_concurrently(elements, batch_size=100):
    """Process elements in smaller chunks concurrently"""
    chunks = [elements[i:i + batch_size] for i in range(0, len(elements), batch_size)]
    tasks = [process_chunk(chunk) for chunk in chunks]
    return await asyncio.gather(*tasks)
```

### 3. Typesense Optimizations

#### Increase Typesense Memory
```yaml
# In docker-compose.yaml
typesense:
  environment:
    - TYPESENSE_MEMORY_LIMIT=4G  # Increase from default
```

#### Optimize Indexing
```python
# In ts_inserter.py, add bulk import parameters
client.collections[COLLECTION].documents.import_(
    docs, 
    {"action": "upsert", "dirty_values": "coerce"}
)
```

### 4. Elasticsearch Optimizations

#### Increase Refresh Interval
```python
# In es_inserter.py, modify index settings
await es.indices.put_settings(
    index=INDEX,
    body={
        "index": {
            "refresh_interval": "30s"  # Reduce refresh frequency during bulk load
        }
    }
)
```

#### Disable Replication During Import
```python
"settings": {
    "index": {
        "number_of_replicas": 0,  # Already set, keep for bulk loads
        "refresh_interval": "30s"
    }
}
```

### 5. PostGIS Optimizations

#### Increase Work Mem
```yaml
# In docker-compose.yaml
postgis:
  environment:
    - POSTGRES_WORK_MEM=256MB
    - POSTGRES_MAINTENANCE_WORK_MEM=512MB
```

#### Batch Insert Optimization
```python
# Already using executemany, but can increase batch size
await conn.executemany(query, rows, timeout=30)
```

### 6. NATS Optimizations

#### Increase Stream Max Messages
```python
# In nats_client.py
StreamConfig(
    name=NATS_STREAM,
    subjects=[NATS_SUBJECT],
    retention=RetentionPolicy.LIMITS,
    max_age=0,
    max_bytes=-1,
    storage="file",
    max_msg_size=-1,  # Allow larger messages
    max_msgs_per_subject=-1,  # Unlimited messages
)
```

### 7. Consumer Configuration

#### Increase Consumer Max Waiting
```python
# In nats_client.py, modify subscribe function
async def subscribe(js, consumer_name):
    """Create a durable pull subscription for a consumer."""
    return await js.pull_subscribe(
        NATS_SUBJECT, 
        durable=consumer_name, 
        stream=NATS_STREAM,
        config=ConsumerConfig(
            max_waiting=500,  # Increase from default
            max_deliver=1,  # Only deliver once
            ack_wait=30,  # 30 second ack window
        )
    )
```

## Environment Variables for Performance Tuning

```bash
# Batch Sizes
BATCH_SIZE=1000  # Increase for better throughput (default: 500)

# Vector Processing
ENABLE_VECTORS=false  # Disable if not needed for 2-3x speedup

# Database Connections
POSTGRES_MAX_CONNECTIONS=20
ELASTICSEARCH_MAX_CONNECTIONS=20
TYPESENSE_CONNECTION_TIMEOUT=30
```

## Monitoring Performance

### Track Throughput
```bash
# Monitor message processing rate
docker logs ahmedemohamed-cmd-ts-inserter-1 | grep "Fetched" | wc -l

# Monitor database sizes
curl -s http://localhost:9200/osm_places/_stats
curl -s http://localhost:8108/collections/osm_places
```

### Identify Bottlenecks
```python
# Add timing to inserters
import time
start = time.time()
# ... processing ...
print(f"Processed {len(docs)} docs in {time.time() - start:.2f}s")
```

## Expected Performance Improvements

- **Batch size increase (100→500)**: 3-5x improvement
- **Disable vectors**: 2-3x improvement (if AI search not needed)
- **Parallel processing**: 2-4x improvement (CPU dependent)
- **Database tuning**: 1.5-2x improvement
- **Combined optimizations**: 10-20x total improvement possible

## Implementation Priority

1. ✅ **Batch size increase** - Applied, rebuild required
2. **Disable vectors** - If AI search not needed
3. **Database tuning** - Memory and configuration
4. **Parallel processing** - Code changes required
5. **Consumer optimization** - NATS configuration
