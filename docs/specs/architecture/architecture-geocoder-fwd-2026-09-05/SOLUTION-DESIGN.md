---
name: 'geocoder-platform-solution-design'
type: solution-design
scope: 'Geocoder platform: services/, shared/, and the compose topology.'
companion: 'ARCHITECTURE-SPINE.md'
status: final
created: '2026-09-05'
---

# Geocoder Platform — Solution Design

Companion to `ARCHITECTURE-SPINE.md`. The spine fixes the invariants; this
document explains how the system actually works today, including the parts that
are awkward. Derived from the code at `1c112d5`, not from intent. The pipeline, traffic,
and deployment diagrams live in the spine; read them alongside this.

## What the system does

The platform answers geocoding questions over a global OSM-derived corpus:
forward search
("15 Tahrir Street, Cairo" → a point), reverse ("this lat/lon" → an address),
prefix autocomplete, category search ("pharmacy"), routing with live traffic, and
AI-generated place descriptions. It is multilingual by requirement rather than by
feature — Arabic and English names coexist on the same records, and a query in
one language must reach data tagged in the other.

## Ingest

Four source families feed one pipeline:

| Source | Watcher | Shape |
| --- | --- | --- |
| OSM PBF extracts (Geofabrik) | `watcher` | osmium-parsed nodes/ways/relations |
| OpenAddresses | `oa_watcher` | CSV / GeoJSON address points |
| GeoNames | `gn_watcher` | TSV dumps |
| Curated place exports | `places_watcher` | JSON arrays |

Each watcher normalizes its input into one element message — `osm_id`,
`osm_type`, `tags`, `geom`, `admin_level`, `area_km2` — and publishes to the
NATS `OSM` stream on `osm.elements`. That message *is* the pipeline contract; the
inserters know nothing about which source produced a record.

Two inserters consume the stream as shared durable consumers, independently of
each other:
`es_inserter` writes searchable documents into the Elasticsearch `osm_places`
index, `postgis_inserter` writes geometry and address points into PostGIS. They
never talk to each other. Running N replicas of either raises throughput without
coordination, because the durable consumer distributes work.

Re-running a watcher is safe. Before parsing a file, a watcher claims it in the
processed ledger; a claim left unfinished by a crashed worker becomes reclaimable
after `PROCESSED_CLAIM_TTL`. In `file` mode the ledger sits beside the data; set
`PROCESSED_LEDGER=pg` and it moves into Postgres with atomic claims, which is
what makes multiple watcher replicas safe on one data directory.

The batch stages — `downloader` and the PBF `watcher` — are one-shot jobs by
design, idempotent on re-run rather than replicated.

## Serving

`services/geocoder.py` is the FastAPI app. It mounts two extracted routers
(`routing`, `nearby`) and holds everything else inline. Requests read from
Elasticsearch and PostGIS; writes publish to NATS instead of touching either
store (spine AD-1).

**Forward search** has two scoring modes, selected by the `effort` query
parameter. `high` casts a wide fuzzy net with per-document function scoring.
`optimized` uses leaner fuzzy matching plus a bounded rescore over the top hits.
The recall report measures both: they are within noise on named places, but
`optimized` is materially better on addresses (99.6% vs 95.6% correct-street at
rank 1) and cheaper to run.

**Address interpolation** is the notable piece of domain logic. When a query
carries a house number with no exact record, the position is estimated by
interpolating between known addresses on the same street, odd/even side aware.
The street is resolved through Elasticsearch first, so an English query reaches
Arabic-tagged address points — which is how cross-language interpolation works at
all. Interpolated results are returned first, marked `match_type: "interpolated"`
with a side and a bracket.

**Autocomplete** is two-tier. A Redis prefix index answers what it can (about 70%
of probes, p50 ~8ms); everything else falls through to Elasticsearch. The split is not a quality ranking. Measured strict@1 within the Redis path is
17% against 71% for Elasticsearch, but the two serve different query
populations: Redis answers the short prefixes, where almost nothing is
determinable yet. The A/B test
showed the fast path wins on the queries it owns; the per-source numbers are
confounded and should not be read as "Redis is worse".

**Enrichment** runs off the request path where it can: `/describe` generates
titles and descriptions through Ollama, and the result is written back onto the
document. Deep geocoding via Google Maps (`/deep/*`) maps results into OSM tags
and feeds them back into the pipeline, so a lookup improves the corpus.

## Traffic and routing

Routing proxies Valhalla, adding Arabic narration and traffic colouring.

Live traffic has two producers writing one Redis schema. Probes posted to
`/traffic/probe` land on a short-retention NATS stream; `traffic_aggregator`
map-matches them to road edges and folds an EWMA into `tf:e:<graphid>`. Separately,
`/route?traffic=true` fetches provider speeds on demand for uncovered edges and
writes the same keys, cache-first and daily-budgeted. `traffic_writer` then bakes
those speeds into Valhalla's `traffic.tar` via mmap.

Two details carry real weight. The EWMA fold runs as a Lua script server-side
because a client-side read-modify-write races and loses updates on hot edges. And
`traffic_writer` shards by tile — N writers with distinct
`TRAFFIC_WRITER_SHARD_INDEX` values own disjoint tiles and byte ranges, which is
what allows more than one writer at all.

Provider polling distributes without a leader: cells go onto a `WORK_QUEUE`
stream where each message is delivered to exactly one worker, and a Redis
`SET NX` scheduler prevents duplicate provider calls. There is no elected
coordinator anywhere in the traffic path.

## Data ownership

| Store | Owns | Rebuildable from |
| --- | --- | --- |
| Elasticsearch `osm_places` | Searchable documents, popularity, AI descriptions | Re-ingest |
| PostGIS | Geometry, address points, `processed_files` ledger | Re-ingest |
| Redis | Autocomplete index, traffic edges, caches | ES / PostGIS / provider |
| Valhalla | Routing tiles, `traffic.tar` | PBF + Redis |
| NATS | In-flight elements and probes | Sources |

Redis holds nothing that cannot be rebuilt. This is deliberate: the shared Redis
is non-persistent and LRU-evicting, and the billing subsystem already learned
what happens when durable counts live there — its usage display had to move to
Postgres rollups.

## Scaling model

Every serving component is stateless and replica-safe, and the mechanism
differs by component — durable consumers for the inserters, tile sharding for
the traffic writer, ledger claims for the watchers. The README's *Horizontal
scaling* section is the maintained per-service list; it is not repeated here.

What matters architecturally is the constraint behind it: nothing on the serving
path elects a leader or holds state in process, so every component scales by
adding replicas rather than by growing one.

## Operational envelope

Local development is a 25-service compose stack with ports bound to `127.0.0.1`.
An AI variant layers `docker-compose.ai.yaml` for GPU and vector embeddings.

Production is a **different machine** at `places.nourbyte.com`, fronted by Nginx
Proxy Manager terminating TLS. Zitadel owns the domain root because it has no
sub-path support, so the console moves to `/console`, the metered geocoder sits
behind `/api/*`, and the management API behind `/billing-api/*`. Deploying there
requires both compose files; the base file alone serves a blank frontend.

## How quality is measured

The unit suite cannot see search quality. `docs/quality-baseline.md` freezes
recall, autocomplete, and category numbers against a 1,000-case Cairo test set,
and any change to search, ranking, autocomplete, categories, or the ES mapping is
measured against it. The regression policy is asymmetric: @5 and @10 may not drop,
while strict@1 may move a point on cross-source duplicate artifacts. A move
there needs an explanation, not an automatic failure.

## Known weaknesses

These are as-built facts, not proposals. Fixes belong in the gap register.

1. **`services/geocoder.py` is 2,707 lines.** The extraction pattern works —
   `routing` and `nearby` are already mounted routers — but the rest of the
   serving path is one file.
2. **Two direct-write exceptions bypass the stream** (`popularity`,
   `ai_description`). Both are field updates on existing documents, so they do
   not corrupt the pipeline, but they are precedents that invite more.
3. **Four images are unpinned** (`valhalla`, `ollama`, `zitadel`, `curl`) while
   everything else is version-pinned. Reproducibility is only as good as the
   weakest pin.
4. **The quality baseline is Cairo-only against a global index.** One city's test
   set stands in for roughly 43.8M documents worldwide, so a geo-scoring change
   can look flat locally and regress elsewhere.
5. **`traffic_writer`'s docstring is already wrong** — it says `tf:e:*` is
   "written by traffic_aggregator, read here", but `routing.py` writes it too.
6. **`AD-7` (config in one module) has no mechanical enforcement.** Seven files
   violated it until commit `a628ce7`; nothing stops it recurring.
7. **Elasticsearch is pinned at 8.11.0**, which is likely well behind current 8.x.
   No upgrade assessment has been done.

## Where to start reading

| Question | File |
| --- | --- |
| How does search scoring work? | `services/geocoder.py` (`/geocode`), `shared/ranking.py` |
| How are addresses parsed and interpolated? | `shared/address.py`, `shared/interpolation.py` |
| What does the ES document look like? | `shared/es_mapping.py` |
| How does the pipeline move data? | `shared/nats_client.py`, `services/es_inserter.py` |
| How does traffic get into Valhalla? | `services/traffic_aggregator.py`, `services/traffic_writer.py` |
| What is tunable? | `shared/config.py` — every environment variable, in one place |
