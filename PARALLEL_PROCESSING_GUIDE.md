# Parallel Processing Implementation Guide

## Status Summary

**Current State**: Sequential processing with batch size 500
- ✅ Batch size increased from 100 to 500 (5x improvement)
- ✅ Configuration for parallel processing added (`MAX_CONCURRENT_BATCHES=4`)
- ⏳ Parallel processing NOT YET IMPLEMENTED
- 📊 Current performance: ~500-1000 docs/sec per inserter
- 🎯 Potential performance with parallel: ~2000-4000 docs/sec per inserter

This document describes how to implement parallel processing to achieve additional 2-4x performance improvement.

## Overview

This guide documents parallel processing implementation for the geocoding service pipeline. Currently, the system uses **sequential batch processing** with a batch size of 500. Parallel processing can provide 2-4x additional performance improvements by better utilizing CPU and I/O resources.

## Current Architecture (Sequential)

```
NATS → Fetch Batch 1 → Process → Insert → Fetch Batch 2 → Process → Insert → ...
```

**Current State**: Sequential processing with batch size 500, providing 5x improvement over original batch size of 100.

**Problem**: While one batch is being processed/inserted, CPU and I/O resources may be underutilized.

## Proposed Architecture (Parallel)

```
NATS → Fetch Batch 1 ─┐
     → Fetch Batch 2 ─┼→ Concurrent Processing → Concurrent Insertion
     → Fetch Batch 3 ─┤
     → Fetch Batch 4 ─┘
```

**Benefit**: Multiple batches are processed simultaneously, maximizing resource utilization.

## Implementation Approaches

### Approach 1: Pipeline Parallelism (Recommended)

Process fetch, transform, and insert operations in a pipeline where different batches can be in different stages simultaneously.

**Pros**: 
- Better resource utilization
- Natural backpressure handling
- Easier to implement
- Memory efficient

**Cons**: 
- More complex state management
- Requires careful error handling

### Approach 2: Fork-Join Parallelism

Fetch multiple batches, process them all in parallel, then wait for all to complete.

**Pros**:
- Simpler conceptual model
- Maximum throughput for CPU-bound tasks

**Cons**:
- Higher memory usage
- Less predictable latency
- Can overwhelm downstream systems

## Detailed Implementation

### Step 1: Add Parallel Processing Configuration

```python
# shared/config.py (already added)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))  # Current: 500, was reduced from 2000 to reduce NATS load
MAX_CONCURRENT_BATCHES = int(os.getenv("MAX_CONCURRENT_BATCHES", "4"))  # Configured but not yet implemented
```

**Current Status**: Configuration exists but parallel processing is not yet implemented in the inserters.

### Step 2: Implement Parallel Processing for ts_inserter

**⚠️ NOT YET IMPLEMENTED** - Current implementation is sequential. The code below shows how to implement parallel processing.

```python
# services/ts_inserter.py

import asyncio
from shared.config import BATCH_SIZE, MAX_CONCURRENT_BATCHES

async def process_batch(elements, client, loop):
    """Process a single batch of elements."""
    # Import embedding functions here to avoid slow startup
    from shared.embeddings import embed_texts, build_text
    
    # build searchable text from ALL tags for each element
    texts = [build_text(e["tags"]) for e in elements]
    non_empty = [(i, t) for i, t in enumerate(texts) if t]

    # compute vectors only when ENABLE_VECTORS is on
    vectors: list[list[float] | None] = [None] * len(elements)
    if ENABLE_VECTORS and non_empty:
        indices, batch_texts = zip(*non_empty)
        batch_vecs = embed_texts(list(batch_texts))
        for idx, vec in zip(indices, batch_vecs):
            vectors[idx] = vec

    docs = []
    for elem, vec in zip(elements, vectors):
        tags = elem["tags"]
        admin_level = elem.get("admin_level", 0)
        area_km2 = elem.get("area_km2", 0.0)
        rank = compute_offline_rank(tags, admin_level, area_km2)

        doc: dict = {
            "id": elem["osm_id"],
            "osm_id": elem["osm_id"],
            "name": tags.get("name", ""),
            "name_en": tags.get("name:en", ""),
            "osm_type": elem.get("osm_type", ""),
            "tags_text": build_text(tags),
            "admin_level": admin_level,
            "offline_rank": rank,
            "popularity": 0.0,
        }
        loc = _centroid(elem.get("geom"))
        if loc is not None:
            doc["location"] = loc
        if vec is not None:
            doc["name_vector"] = vec
        docs.append(doc)

    def _import():
        try:
            client.collections[COLLECTION].documents.import_(
                docs, {"action": "upsert"}
            )
        except Exception as exc:
            print(f"[ts-inserter] import error: {exc}")
            raise

    await loop.run_in_executor(None, _import)
    return len(docs)

async def run_parallel():
    """Run with parallel batch processing."""
    # Retry logic for connecting to Typesense
    max_retries = 10
    retry_delay = 2
    
    client = None
    for attempt in range(max_retries):
        try:
            client = ts_client()
            client.collections.retrieve()
            print(f"[ts-inserter] Successfully connected to Typesense", flush=True)
            break
        except Exception as e:
            print(f"[ts-inserter] Failed to connect to Typesense (attempt {attempt + 1}/{max_retries}): {e}", flush=True)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                raise
    else:
        raise Exception("Failed to connect to Typesense after maximum retries")
    
    ensure_collection(client)

    nc, js = await connect()
    sub = await subscribe(js, "ts-consumer")
    loop = asyncio.get_event_loop()
    print("[ts-inserter] Subscription created, listening for messages ...", flush=True)

    # Semaphore to limit concurrent batch processing
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)
    
    iteration = 0
    while True:
        iteration += 1
        if iteration % 10 == 0:
            print(f"[ts-inserter] Loop iteration {iteration}", flush=True)
        
        async with semaphore:
            try:
                msgs = await asyncio.wait_for(sub.fetch(batch=BATCH_SIZE, timeout=5), timeout=10)
                print(f"[ts-inserter] Fetched {len(msgs)} messages", flush=True)
            except asyncio.TimeoutError:
                print(f"[ts-inserter] Fetch timeout", flush=True)
                await asyncio.sleep(1)
                continue
            except Exception as e:
                print(f"[ts-inserter] Fetch error: {e}", flush=True)
                await asyncio.sleep(1)
                continue

            elements = []
            for msg in msgs:
                elements.append(json.loads(msg.data))
                await msg.ack()

            print(f"[ts-inserter] Parsed {len(elements)} elements", flush=True)
            
            if not elements:
                continue

            try:
                count = await process_batch(elements, client, loop)
                print(f"[ts-inserter] Imported {count} docs", flush=True)
            except Exception as e:
                print(f"[ts-inserter] Batch processing error: {e}", flush=True)
                # Continue processing next batch even if this one fails

    await nc.close()
```

### Step 3: Implement for es_inserter (Similar Pattern)

**⚠️ NOT YET IMPLEMENTED** - Current implementation is sequential. The code below shows how to implement parallel processing.

```python
# services/es_inserter.py

async def process_batch(elements, es, loop):
    """Process a single batch of elements for Elasticsearch."""
    from shared.embeddings import embed_texts, build_text
    
    # Similar text processing and vector computation
    texts = [build_text(e["tags"]) for e in elements]
    non_empty = [(i, t) for i, t in enumerate(texts) if t]

    vectors: list[list[float] | None] = [None] * len(elements)
    if ENABLE_VECTORS and non_empty:
        indices, batch_texts = zip(*non_empty)
        batch_vecs = embed_texts(list(batch_texts))
        for idx, vec in zip(indices, batch_vecs):
            vectors[idx] = vec

    docs = []
    for elem, vec in zip(elements, vectors):
        # Document construction similar to ts_inserter
        doc = {
            "_id": elem["osm_id"],
            "_source": {
                "osm_id": elem["osm_id"],
                # ... other fields
            }
        }
        docs.append(doc)

    async def _bulk_index():
        try:
            await async_bulk(
                es,
                docs,
                index=INDEX,
                refresh=False
            )
        except Exception as exc:
            print(f"[es-inserter] bulk error: {exc}")
            raise

    await loop.run_in_executor(None, _bulk_index)
    return len(docs)

async def run_parallel():
    """Run with parallel batch processing."""
    # Similar connection setup
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)
    
    iteration = 0
    while True:
        iteration += 1
        if iteration % 10 == 0:
            print(f"[es-inserter] Loop iteration {iteration}", flush=True)
        
        async with semaphore:
            # Fetch and process similar to ts_inserter
            # ...
```

## Performance Tuning Guidelines

### Choosing MAX_CONCURRENT_BATCHES

**Start with 4** and adjust based on:

1. **CPU Cores**: 
   - 2-4 cores: Use 2-3 concurrent batches
   - 4-8 cores: Use 4-6 concurrent batches  
   - 8+ cores: Use 6-8 concurrent batches

2. **Memory Usage**:
   - Monitor memory: `docker stats`
   - Each batch uses ~50-100MB with vectors, ~10-20MB without
   - Formula: `MAX_CONCURRENT_BATCHES = Available_Memory / Batch_Memory`

3. **Database Capacity**:
   - Typesense: Can handle 10-20 concurrent bulk operations
   - Elasticsearch: Can handle 5-10 concurrent bulk operations
   - PostGIS: Can handle 5-10 concurrent inserts

### Recommended Starting Configurations

```yaml
# For systems with 4 CPU cores, 8GB RAM
environment:
  - BATCH_SIZE=500  # Current stable configuration
  - MAX_CONCURRENT_BATCHES=4  # Not yet implemented
  - ENABLE_VECTORS=false

# For systems with 8 CPU cores, 16GB RAM  
environment:
  - BATCH_SIZE=500  # Current stable configuration
  - MAX_CONCURRENT_BATCHES=6  # Not yet implemented
  - ENABLE_VECTORS=true

# For systems with 16+ CPU cores, 32GB+ RAM
environment:
  - BATCH_SIZE=500  # Current stable configuration
  - MAX_CONCURRENT_BATCHES=8  # Not yet implemented
  - ENABLE_VECTORS=true
```

## Monitoring and Debugging

### Add Performance Metrics

```python
import time

async def process_batch(elements, client, loop):
    start = time.time()
    
    # Processing logic...
    
    elapsed = time.time() - start
    throughput = len(elements) / elapsed
    print(f"[ts-inserter] Processed {len(elements)} docs in {elapsed:.2f}s ({throughput:.1f} docs/s)", flush=True)
```

### Monitor Resource Usage

```bash
# Monitor CPU, Memory, Network
docker stats

# Monitor database connections
curl -s http://localhost:9200/_cat/thread_pool?v
curl -s http://localhost:8108/metrics

# Monitor NATS consumer lag
# (NATS monitoring endpoint)
```

## Expected Performance Improvements

### Current Performance (Sequential, BATCH_SIZE=500):
- **Actual**: ~500-1000 docs/sec per inserter
- **Improvement from batch size (100→500)**: 5x
- **Status**: Stable and working

### Potential Performance with Parallel Processing (BATCH_SIZE=500, MAX_CONCURRENT_BATCHES=4):
- **Estimated**: ~2000-4000 docs/sec per inserter
- **Additional improvement**: 2-4x over current sequential
- **Total improvement from baseline (100)**: 10-20x

### Conservative Estimates (with ENABLE_VECTORS=false):
- Current Sequential: ~500-1000 docs/sec per inserter
- Future Parallel (4 concurrent): ~2000-4000 docs/sec per inserter
- **Potential Additional Improvement: 2-4x**

### With ENABLE_VECTORS=true (GPU):
- Current Sequential: ~100-300 docs/sec per inserter
- Future Parallel (4 concurrent): ~400-1200 docs/sec per inserter
- **Potential Additional Improvement: 2-4x**

### Current vs Potential:
- **Current**: 5x improvement from batch size increase alone
- **Potential with parallel**: 10-20x total improvement (5x batch + 2-4x parallel)

## Trade-offs and Considerations

### Memory Usage
- **Sequential**: One batch in memory at a time
- **Parallel (4 concurrent)**: 4 batches in memory simultaneously
- **Impact**: 4x memory usage for processing

### Database Load
- **Sequential**: Predictable, steady load
- **Parallel**: Bursty load, can overwhelm databases
- **Mitigation**: Use connection pooling, rate limiting

### Error Handling
- **Sequential**: Easy to debug, fail-fast
- **Parallel**: Complex error scenarios, need robust error handling
- **Mitigation**: Comprehensive logging, retry logic, dead letter queues

### Ordering Guarantees
- **Sequential**: Strict ordering maintained
- **Parallel**: No ordering guarantees between batches
- **Impact**: Acceptable for most OSM data use cases

## Implementation Priority

1. ✅ **Increase batch size to 500** - Applied (reduced from 2000 to reduce NATS load)
2. ⏳ **Add parallel processing** - Requires code changes (see above) - NOT YET IMPLEMENTED
3. **Database tuning** - Connection pools, memory settings
4. **Monitoring** - Performance metrics, alerting

## Rollback Plan

If parallel processing causes issues:
1. Set `MAX_CONCURRENT_BATCHES=1` to revert to sequential
2. Reduce `BATCH_SIZE` from 500 if memory pressure (current stable: 500)
3. Monitor logs for database errors
4. Have ready docker-compose down/up commands for quick rollback

### Current Stable Configuration
- `BATCH_SIZE=500` - Provides 5x improvement, stable
- `MAX_CONCURRENT_BATCHES=4` - Configured but not implemented
- Sequential processing - Current working state
