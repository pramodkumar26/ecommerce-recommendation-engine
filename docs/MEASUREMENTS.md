# Measurement Ledger

Authoritative registry for every numeric claim in this project.

Rules:

- No number goes in the README, `docs/benchmark_results.md`, or a resume bullet unless it has a
  row here with a metric ID, a git commit SHA, and the environment it was measured in.
- Raw output lives under `benchmarks/raw/`. This file points at it.
- `docs/benchmark_results.md` is a presentation report. It references metric IDs and never
  holds independent values. If the two disagree, this ledger wins and the report is corrected.
- Local streaming numbers and ADLS cloud-integration numbers are separate metric IDs. Never
  combine them into one figure.
- A value stays `TBD` until it is actually measured. Do not pre-fill expected results.

## Metric ID convention

```text
<AREA>-<WHAT>-<NNN>

AREA:  DATASET, IDENTITY, PRODUCER, STREAM, RECOVERY, CLOUD, DQ, MODEL, RETRIEVAL, API, COST
```

## Environments

```text
ENV-LOCAL-HOST
  MacBook Pro Mac17,2, Apple M5, 10 cores, 16 GB RAM
  macOS 26.6.2 (25G83), arm64
  Python 3.11.16 in .venv, pandas 2.3.3
  Docker not running during these measurements

ENV-LOCAL-DOCKER
  as above, plus Docker Desktop 29.7.2, 10 CPU, 7.75 GiB limit
  Kafka Confluent 7.9.2 KRaft, Spark 3.5.7, Redis 7.4
```

## Dataset provenance

Every `DATASET-*` and `IDENTITY-*` row below was measured against these exact files:

```text
events.csv                  2,756,101 rows  sha256 3745aa83238b1e6d44d8fda209807899f420084398f94ddf745f3cbcfecbf9e7
item_properties_part1.csv  10,999,999 rows  sha256 30aad5aeca58b2dc27dcc73e1708565f5818e45adb3eb57401f91e87355b0b81
item_properties_part2.csv   9,275,903 rows  sha256 d5e7d1a91dc40f522aeb596b267e6c87d8aed689a7192d12369cfb165eb987e5
category_tree.csv               1,669 rows  sha256 94e865eb0a3d48cbbfe3b79079018dd92509315c88f5fd8d00d0b4b5af434f5b
```

## Index

| metric_id | metric | value | date | commit | evidence |
|---|---|---|---:|---|---|
| DATASET-EVENTS-001 | total source events | 2,756,101 | 2026-09-15 | `8dbe1f4` | `dataset_profile.json` |
| DATASET-VISITORS-001 | unique visitors | 1,407,580 | 2026-09-15 | `8dbe1f4` | `dataset_profile.json` |
| DATASET-ITEMS-001 | unique items in events | 235,061 | 2026-09-15 | `8dbe1f4` | `dataset_profile.json` |
| DATASET-TRANSACTIONS-001 | transaction events | 22,457 | 2026-09-15 | `8dbe1f4` | `dataset_profile.json` |
| DATASET-SPAN-001 | source time span | 138.0 days | 2026-09-15 | `8dbe1f4` | `dataset_profile.json` |
| DATASET-PITJOIN-001 | point-in-time join miss rate | 14.27% | 2026-09-15 | `8dbe1f4` | `dataset_joins.json` |
| DATASET-ELIGIBLE-001 | visitors with 5+ interactions | 81,620 | 2026-09-15 | `8dbe1f4` | `dataset_joins.json` |
| IDENTITY-REPRO-001 | event id collisions over full source | 0 of 2,756,101 | 2026-09-15 | `8dbe1f4` | `event_id_reproducibility.json` |
| REPLAY-DETERMINISM-001 | bounded replay identical across two runs | 20,000 records, identical | 2026-09-15 | `97c267c` | `replay_verification.json` |
| REPLAY-PARTITION-001 | visitor keys spanning multiple partitions | 0 of 11,420 | 2026-09-15 | `97c267c` | `replay_verification.json` |
| PRODUCER-RATECTL-001 | rate control accuracy at 500/2000/10000 eps | 500.0 / 1999.9 / 9992.7 | 2026-09-15 | `97c267c` | `producer_rate.txt` |
| PRODUCER-CEILING-001 | unthrottled producer-only emit rate | 53,807 events/sec | 2026-09-15 | `97c267c` | `producer_rate.txt` |
| SCHEMA-COMPAT-001 | v2 accepted, v3 rejected under BACKWARD | both as expected | 2026-09-15 | `a36dd19` | `schema_dlq_verification.json` |
| SCHEMA-RESOLUTION-001 | v2 record read by a v1 reader | decodes, 2 unknown fields dropped | 2026-09-15 | `a36dd19` | `schema_dlq_verification.json` |
| DQ-DLQROUTE-001 | malformed records routed to DLQ | 960 of 960 on the wire | 2026-09-15 | `a36dd19` | `schema_dlq_verification.json` |
| DQ-NOLOSS-001 | valid plus DLQ equals consumed | 19,462 + 960 = 20,422 | 2026-09-15 | `a36dd19` | `dlq_router_stats.json` |
| STREAM-BRONZE-001 | events reaching Bronze from Kafka | 50,000 of 50,000, all ids unique | 2026-09-15 | `34773b0` | `streaming_verification.json` |
| STREAM-WINDOW-001 | 5 minute event-time window counts vs source | 777 of 777 windows exact | 2026-09-15 | `34773b0` | `streaming_verification.json` |
| STREAM-APPROX-001 | HyperLogLog distinct visitor accuracy | 0.28% mean, 3.06% worst | 2026-09-15 | `34773b0` | `streaming_verification.json` |
| STREAM-LATENESS-001 | MODELED replay lateness distribution | p50 104 min, p95 845 min, max 959 min | 2026-09-15 | `34773b0` | `lateness_distribution.json` |
| RECOVERY-DEDUP-001 | injected duplicates removed from the aggregate | 2,389 of 2,389, 777 windows exact | 2026-09-15 | `34773b0` | `duplicate_test.json` |
| RECOVERY-LATE-001 | events dropped by watermark, two settings | 0.56% at 24h, 3.45% at 1 min | 2026-09-15 | `34773b0` | `late_event_test.json` |
| RECOVERY-RESTART-001 | checkpoint restart, loss and inflation | 0 lost, 0 duplicated, 50,000 of 50,000 | 2026-09-15 | `34773b0` | `restart_test.json` |
| SILVER-PITJOIN-001 | rows enriched from a future property | 0 of 100,000, matches independent as-of join | 2026-09-16 | `08e71a6` | `silver_verification.json` |
| SILVER-SCD-001 | property snapshots collapsed to validity intervals | 2.29M rows to 1.03M intervals | 2026-09-16 | `08e71a6` | `item_scd_stats.json` |
| BACKFILL-RANGE-001 | bounded Silver rebuild, surgical | 2 rebuilt identical, 5 untouched | 2026-09-16 | `08e71a6` | `backfill_range_test.json` |
| STORAGE-PARTITION-001 | time partitioning, files skipped vs wall time | 5x fewer files, 0.85x speed | 2026-09-16 | `08e71a6` | `partition_benchmark.json` |
| ENRICH-ANYPROP-001 | full-dataset any-property miss rate, vs Phase 2 | 14.266%, 0.0 point difference | 2026-09-16 | `08e71a6` | `enrichment_measurement.json` |
| ENRICH-CATEGORY-001 | full-dataset categoryid enrichment miss rate | 23.835% (2,099,173 of 2,756,101 enriched) | 2026-09-16 | `08e71a6` | `enrichment_measurement.json` |
| SILVER-LEAK-002 | rows enriched from the future, full dataset | 0 of 2,756,101 | 2026-09-16 | `08e71a6` | `enrichment_measurement.json` |
| SILVER-REJECT-001 | contract rejects fired and categorised | 610 of 610, 3 categories exact | 2026-09-16 | `08e71a6` | `silver_rejects_test.json` |
| TIME-ROUNDTRIP-001 | epoch ms to timestamp and back, exactness | 0 mismatches, timezone independent | 2026-09-16 | `08e71a6` | `timestamp_roundtrip.json` |
| STREAM-FULLSCALE-001 | full dataset through the streaming chain | 2,756,101 events in 238.1 s | 2026-09-16 | `08e71a6` | `full_enrichment_run.json` |
| LAKEHOUSE-PERSIST-001 | Delta survives restart, rebuild reproduces | 10 tables identical, both checks | 2026-09-16 | `08e71a6` | `restart_persistence_test.json` |
| REGRESSION-SUITE-001 | phases 3 to 7 verification suite | 12 of 12 passed | 2026-09-16 | `08e71a6` | superseded by REGRESSION-SUITE-002 |
| BACKFILL-TRANSFORM-001 | transformation change reprocessed from Bronze | 2 partitions changed, 5 untouched, 9 of 9 checks | 2026-09-16 | `71f76d4` | `backfill_transformation_test.json` |
| BACKFILL-SCOPE-001 | bounded backfill blast radius and timing | 28,321 of 100,000 rows, 35.8 s | 2026-09-16 | `71f76d4` | `backfill_report.json` |
| REGRESSION-SUITE-002 | phases 3 to 8 verification suite | 13 of 13 passed | 2026-09-16 | `71f76d4` | `regression_suite.json` |

The eight `DATASET-*` and `IDENTITY-*` rows were produced by the code at commit
`8dbe1f4a997b584b29120dfefcd5706e35a9746d`.

The four `REPLAY-*` and `PRODUCER-*` rows were produced by the code at commit
`97c267c61e77380d7a4fc6c8914366bcc50e7dfb`.

The four `SCHEMA-*` and `DQ-*` rows were produced by the code at commit `a36dd199b38406aef7e381ba220a3df9fd0ded9c`.

The three `STREAM-*` rows from Phase 5 and the four Phase 6 rows (`STREAM-LATENESS-001` and
the `RECOVERY-*` set) were produced by the code at commit `34773b0ff7104b1c891d3bfbd094a2754db2d577`.

## Records

### DATASET-EVENTS-001

```text
metric_id: DATASET-EVENTS-001
metric: total rows in events.csv
value: 2,756,101
date: 2026-09-15
git_commit: 8dbe1f4a997b584b29120dfefcd5706e35a9746d
environment: ENV-LOCAL-HOST
dataset slice: full source, events.csv sha256 3745aa83...
command / test: scripts/profile_dataset.py
config: pandas read_csv, explicit dtypes, full file
evidence: benchmarks/raw/dataset_profile.json
notes: confirms the roadmap's "roughly 2.75M" expectation. Includes 460 byte-identical
       duplicate rows, which are real source records and are not removed at this stage.
```

### DATASET-VISITORS-001

```text
metric_id: DATASET-VISITORS-001
metric: unique visitorid values in events.csv
value: 1,407,580
date: 2026-09-15
git_commit: 8dbe1f4a997b584b29120dfefcd5706e35a9746d
environment: ENV-LOCAL-HOST
dataset slice: full source
command / test: scripts/profile_dataset.py
config: nunique over visitorid
evidence: benchmarks/raw/dataset_profile.json
notes: 71.16% of these visitors appear exactly once. Do not quote this as an "active user"
       count. It is the raw visitor cardinality.
```

### DATASET-ITEMS-001

```text
metric_id: DATASET-ITEMS-001
metric: unique itemid values in events.csv
value: 235,061
date: 2026-09-15
git_commit: 8dbe1f4a997b584b29120dfefcd5706e35a9746d
environment: ENV-LOCAL-HOST
dataset slice: full source
command / test: scripts/profile_dataset.py
config: nunique over itemid
evidence: benchmarks/raw/dataset_profile.json
notes: the property files contain 417,053 items, but 49,815 items seen in events have no
       property record. Item coverage for enrichment is 78.81%, not 100%.
```

### DATASET-TRANSACTIONS-001

```text
metric_id: DATASET-TRANSACTIONS-001
metric: transaction events in events.csv
value: 22,457
date: 2026-09-15
git_commit: 8dbe1f4a997b584b29120dfefcd5706e35a9746d
environment: ENV-LOCAL-HOST
dataset slice: full source
command / test: scripts/profile_dataset.py
config: count where event == "transaction"
evidence: benchmarks/raw/dataset_profile.json
notes: 0.81% of all events, resolving to 17,672 unique transaction ids across 11,719 visitors.
       This sparsity is why the model trains on weighted interactions rather than purchases
       alone.
```

### DATASET-SPAN-001

```text
metric_id: DATASET-SPAN-001
metric: elapsed time covered by events.csv
value: 138.0 days, 2015-05-03T03:00:04Z to 2015-09-18T02:59:47Z
date: 2026-09-15
git_commit: 8dbe1f4a997b584b29120dfefcd5706e35a9746d
environment: ENV-LOCAL-HOST
dataset slice: full source
command / test: scripts/profile_dataset.py
config: max(timestamp) - min(timestamp), epoch milliseconds
evidence: benchmarks/raw/dataset_profile.json
notes: sets the boundaries for the chronological split in Phase 12.
```

### DATASET-PITJOIN-001

```text
metric_id: DATASET-PITJOIN-001
metric: share of events with no item property at or before the event timestamp
value: 14.27% (393,198 of 2,756,101)
date: 2026-09-15
git_commit: 8dbe1f4a997b584b29120dfefcd5706e35a9746d
environment: ENV-LOCAL-HOST
dataset slice: full source, events joined against both item_properties parts
command / test: scripts/profile_joins.py
config: as-of join against 18 discrete weekly property snapshots
evidence: benchmarks/raw/dataset_joins.json
notes: causes overlap. 255,585 events have an item with no property record at all, 137,613
       occur before their item's first snapshot, 137,193 occur before the first global
       snapshot. These events get null enrichment rather than being backfilled from a later
       snapshot, which would be leakage. Becomes a tracked data-quality metric in Phase 10.
```

### DATASET-ELIGIBLE-001

```text
metric_id: DATASET-ELIGIBLE-001
metric: visitors with at least 5 interactions
value: 81,620 of 1,407,580 (5.80%)
date: 2026-09-15
git_commit: 8dbe1f4a997b584b29120dfefcd5706e35a9746d
environment: ENV-LOCAL-HOST
dataset slice: full source
command / test: scripts/profile_joins.py
config: groupby visitorid, count >= 5
evidence: benchmarks/raw/dataset_joins.json
notes: the proposed minimum-history threshold excludes 1,325,960 visitors, 94.20%. Of the
       81,620 eligible, only 7,610 have a transaction. Threshold is revisited in Phase 12
       against the actual split, and any change updates this row.
```

### IDENTITY-REPRO-001

```text
metric_id: IDENTITY-REPRO-001
metric: deterministic event id collisions and cross-run stability
value: 0 collisions over 2,756,101 rows, identical digest across two independent passes
date: 2026-09-15
git_commit: 8dbe1f4a997b584b29120dfefcd5706e35a9746d
environment: ENV-LOCAL-HOST
dataset slice: full events.csv
command / test: scripts/verify_event_id.py
config: SHA256 over source_file|source_row_number|timestamp|visitor_id|item_id|event_type|transaction_id
evidence: benchmarks/raw/event_id_reproducibility.json
notes: both passes produced digest 16a58583137991e1b0895a684460fea2ceab428d5a78616b07d13e2f73ad5561.
       Nine unit checks also pass. Source position is in the hash so the 460 byte-identical
       source rows remain distinct records.
```

### REPLAY-DETERMINISM-001

```text
metric_id: REPLAY-DETERMINISM-001
metric: byte-identical replay of a bounded range across two independent runs
value: 20,000 records identical, ignoring ingestion_timestamp
date: 2026-09-15
git_commit: 97c267c61e77380d7a4fc6c8914366bcc50e7dfb
environment: ENV-LOCAL-DOCKER
dataset slice: first 20,000 events after sorting by (event_timestamp, source_row_number)
command / test: scripts/verify_replay.py
config: --seed 42, dry-run mode, compared with and without injections at 2% duplicate,
        1% malformed, 5% delayed
evidence: benchmarks/raw/replay_verification.json
notes: ingestion_timestamp is excluded from the comparison because it is wall clock, and the
       test separately asserts that it does vary between runs. Injection decisions come from
       the seeded RNG up front, and delays shift by position rather than wall-clock time, so
       emitted order does not depend on machine speed.
```

### REPLAY-PARTITION-001

```text
metric_id: REPLAY-PARTITION-001
metric: visitor keys observed on more than one Kafka partition
value: 0 of 11,420 distinct visitor keys
date: 2026-09-15
git_commit: 97c267c61e77380d7a4fc6c8914366bcc50e7dfb
environment: ENV-LOCAL-DOCKER
dataset slice: 20,000 events across item_view, add_to_cart, transaction
command / test: scripts/verify_replay.py
config: 3 partitions per topic, key = visitor_id as UTF-8 string, default partitioner
evidence: benchmarks/raw/replay_verification.json
notes: this is the property per-visitor session logic depends on in Phases 5, 6, and 11.
       Kafka guarantees order within a partition only.
```

### PRODUCER-RATECTL-001

```text
metric_id: PRODUCER-RATECTL-001
metric: achieved rate versus requested rate
value: requested 500 -> 500.0, requested 2000 -> 1999.9, requested 10000 -> 9992.7 events/sec
date: 2026-09-15
git_commit: 97c267c61e77380d7a4fc6c8914366bcc50e7dfb
environment: ENV-LOCAL-DOCKER
dataset slice: 30,000 events per run
command / test: scripts/benchmark_producer.sh
config: single producer process, acks=all, idempotence on, snappy, linger.ms=20
evidence: benchmarks/raw/producer_rate.txt
notes: confirms --rate is honoured, which matters because Phase 21 load tests depend on being
       able to hold a chosen rate.
```

### PRODUCER-CEILING-001

```text
metric_id: PRODUCER-CEILING-001
metric: unthrottled emit rate, producer side only
value: 53,807 events/sec
date: 2026-09-15
git_commit: 97c267c61e77380d7a4fc6c8914366bcc50e7dfb
environment: ENV-LOCAL-DOCKER
dataset slice: 30,000 events
command / test: scripts/benchmark_producer.sh with --rate 1000000
config: single producer process, no consumer running, no Spark running
evidence: benchmarks/raw/producer_rate.txt
notes: NOT an end-to-end throughput number and must never be quoted as one. Nothing is
       consuming, nothing is processing, and no Delta write is involved. The end-to-end
       figure comes from Phase 21 and will be lower. This row exists to show the producer is
       not the bottleneck when Phase 5 measures the stream.
```

### SCHEMA-COMPAT-001

```text
metric_id: SCHEMA-COMPAT-001
metric: Schema Registry compatibility verdicts under BACKWARD
value: v2 compatible, v3 rejected
date: 2026-09-15
git_commit: a36dd199b38406aef7e381ba220a3df9fd0ded9c
environment: ENV-LOCAL-DOCKER
dataset slice: n/a, schema level check
command / test: scripts/verify_schema_dlq.py
config: registry compatibility level BACKWARD, subject item_view-value
evidence: benchmarks/raw/schema_dlq_verification.json
notes: v2 adds session_id and device_type as optional with null defaults, which is why it
       passes. v3 changes item_id from long to string, which Avro cannot promote, so a v3
       reader could not read existing v1 data and the registry refuses it. v3 is never
       registered, it exists only to prove the guard fires.
```

### SCHEMA-RESOLUTION-001

```text
metric_id: SCHEMA-RESOLUTION-001
metric: a v1 reader decoding a record written with v2
value: decoded successfully, 10 fields, session_id and device_type dropped
date: 2026-09-15
git_commit: a36dd199b38406aef7e381ba220a3df9fd0ded9c
environment: ENV-LOCAL-DOCKER
dataset slice: one synthetic v2 record
command / test: scripts/verify_schema_dlq.py
config: AvroSerializer on v2, AvroDeserializer pinned to v1
evidence: benchmarks/raw/schema_dlq_verification.json
notes: this is forward read, distinct from the BACKWARD registration rule. It is what allows
       producers and consumers to be upgraded independently instead of in lockstep.
```

### DQ-DLQROUTE-001

```text
metric_id: DQ-DLQROUTE-001
metric: malformed records correctly routed to clickstream_dlq
value: 960 of 960 malformed records on the wire, 4.70% DLQ rate
date: 2026-09-15
git_commit: a36dd199b38406aef7e381ba220a3df9fd0ded9c
environment: ENV-LOCAL-DOCKER
dataset slice: 20,422 records, 20,000 source events, seed 42
command / test: scripts/verify_schema_dlq.py
config: --malformed-rate 0.05 --duplicate-rate 0.02, topics reset before the run
evidence: benchmarks/raw/schema_dlq_verification.json
notes: split by layer, 312 deserialization_failed, 320 unknown_schema_id, 328
       validation_failed. Compared against malformed records ON THE WIRE, not distinct
       malformed records, because a record that is both malformed and duplicated is emitted
       twice and must appear in the DLQ twice.
```

### DQ-NOLOSS-001

```text
metric_id: DQ-NOLOSS-001
metric: conservation of records through the router
value: 19,462 valid + 960 DLQ = 20,422 consumed
date: 2026-09-15
git_commit: a36dd199b38406aef7e381ba220a3df9fd0ded9c
environment: ENV-LOCAL-DOCKER
dataset slice: same run as DQ-DLQROUTE-001
command / test: scripts/verify_schema_dlq.py
config: as above
evidence: benchmarks/raw/dlq_router_stats.json
notes: the property this phase exists to prove. Every consumed record is either passed through
       or routed, never silently dropped, and the valid stream keeps flowing while bad records
       are present. Not a throughput measurement.
```

### STREAM-BRONZE-001

```text
metric_id: STREAM-BRONZE-001
metric: events carried from Kafka into the Bronze Delta table
value: 50,000 of 50,000, 50,000 distinct event ids, 0 undecodable
date: 2026-09-15
git_commit: 34773b0ff7104b1c891d3bfbd094a2754db2d577
environment: ENV-LOCAL-DOCKER
dataset slice: first 50,000 events after sorting by (event_timestamp, source_row_number)
command / test: scripts/verify_streaming.py
config: standalone cluster, driver 1g, executor 1g, 4 cores, maxOffsetsPerTrigger 5000,
        Spark 3.5.7, Delta 3.3.2, local Delta volume
evidence: benchmarks/raw/streaming_verification.json
notes: event type counts, event time range, and date partitioning all match the source, checked
       independently in pandas rather than by Spark. Not a throughput measurement.
```

### STREAM-WINDOW-001

```text
metric_id: STREAM-WINDOW-001
metric: 5 minute event-time window counts against independently computed expectations
value: 777 of 777 windows exact, 0 mismatches across events, views, carts, transactions
date: 2026-09-15
git_commit: 34773b0ff7104b1c891d3bfbd094a2754db2d577
environment: ENV-LOCAL-DOCKER
dataset slice: same 50,000 event fixture
command / test: scripts/verify_streaming.py
config: update output mode, Delta MERGE upsert on window_start, no watermark
evidence: benchmarks/raw/streaming_verification.json
notes: expectations computed with pandas from events.csv, so Spark is not grading itself.
       Append mode was measured first and dropped most of the later windows, some holding 120
       Bronze events down to 1 or 2, because replay compresses 138 days into minutes and the
       watermark outruns records still arriving on other partitions. Update mode plus MERGE
       counts every record. Watermark policy is Phase 6.
```

### STREAM-APPROX-001

```text
metric_id: STREAM-APPROX-001
metric: approx_count_distinct error for unique visitors per window
value: 0.28% mean, 3.06% worst relative error, 3 worst absolute error
date: 2026-09-15
git_commit: 34773b0ff7104b1c891d3bfbd094a2754db2d577
environment: ENV-LOCAL-DOCKER
dataset slice: 422 windows with 50 or more distinct visitors, of 777 total
command / test: scripts/verify_streaming.py
config: approx_count_distinct with rsd 0.01, compared against exact countDistinct over Bronze
evidence: benchmarks/raw/streaming_verification.json
notes: exact countDistinct is rejected on a streaming aggregate because it needs unbounded
       state, so the live metric is HyperLogLog. Relative error is judged only on windows with
       50 or more distinct visitors, since a window with 10 distinct visitors estimated at 9 is
       off by one record and scores a meaningless 10%. Absolute error is checked on all 777.
```

### STREAM-LATENESS-001

```text
metric_id: STREAM-LATENESS-001
metric: MODELED replay lateness, how far behind the running maximum event time each record
        is reconstructed to have arrived
value: p50 104 min, p90 737 min, p95 845 min, p99 916 min, max 959 min (16.0 h)
date: 2026-09-15
git_commit: 34773b0ff7104b1c891d3bfbd094a2754db2d577
environment: ENV-LOCAL-DOCKER
dataset slice: 50,000 event fixture, modelled at 5,000 per trigger
command / test: streaming/jobs/measure_lateness.py
config: batch composition reconstructed from Bronze source_partition and source_offset
evidence: benchmarks/raw/lateness_distribution.json
notes: MODELED, not directly observed. streaming/jobs/measure_lateness.py reconstructs
       micro-batch composition analytically from Bronze source_partition and source_offset; it
       does not read Spark's own per-batch arrival records. Treat these as a model of replay
       behaviour, not as production arrival latency. The EMPIRICAL counterpart is
       RECOVERY-LATE-001, which measures what the pipeline actually dropped.

       This is EVENT-TIME lateness created by replay compression, not network delay. 138 days
       of 2015 are pushed through in minutes, so records legitimately sit hours behind the
       running maximum. A live deployment would see seconds. Counterintuitively smaller batches
       produce more lateness: p50 is 220 min at 1,000 per trigger and 0 at 20,000, because with
       fewer larger batches most records have no preceding maximum to be late against. Nothing
       exceeded 24 hours at any batch size, which is where the watermark default comes from.
       The roadmap's suggested 10 minute development watermark would have dropped 73% of
       records here.
```

### RECOVERY-DEDUP-001

```text
metric_id: RECOVERY-DEDUP-001
metric: injected replay duplicates removed before the windowed aggregate
value: 2,389 of 2,389 removed, all 777 windows match the source exactly
date: 2026-09-15
git_commit: 34773b0ff7104b1c891d3bfbd094a2754db2d577
environment: ENV-LOCAL-DOCKER
dataset slice: 50,000 source events, 5% duplicate injection, seed 42
command / test: scripts/test_duplicates.py
config: dropDuplicates on event_id with a 24 hour watermark, in its own query between Bronze
        and the aggregate
evidence: benchmarks/raw/duplicate_test.json
notes: controlled both ways. With dedup enabled the aggregate totals 50,000 events matching the
       source. With dedup disabled it totals 52,389, inflated by exactly the 2,389 injected
       duplicates across 626 windows. Bronze keeps all 52,389 rows because it is immutable raw
       history, while holding exactly 50,000 distinct event ids.
```

### RECOVERY-LATE-001

```text
metric_id: RECOVERY-LATE-001
metric: events excluded from the deduplicated stream by the watermark
value: 0.560% (280 of 50,000) at a 24 hour watermark, 3.448% (1,724) at 1 minute
date: 2026-09-15
git_commit: 34773b0ff7104b1c891d3bfbd094a2754db2d577
environment: ENV-LOCAL-DOCKER
dataset slice: 50,000 source events with 10% additional delay injected on top of replay lateness
command / test: scripts/test_late_events.py
config: same stream run twice, only the watermark differs
evidence: benchmarks/raw/late_event_test.json
notes: measures the watermark tradeoff rather than asserting zero loss. The 0.56% figure comes
       from deliberately injected extra delay; with normal replay traffic the same 24 hour
       watermark loses nothing, proven by RECOVERY-DEDUP-001 where the deduplicated table came
       out at exactly 50,000. Every dropped event is still in Bronze and recoverable by the
       Phase 8 backfill, which is the documented policy.
```

### RECOVERY-RESTART-001

```text
metric_id: RECOVERY-RESTART-001
metric: data loss and duplicate inflation across a mid-stream driver kill and restart
value: 0 events lost, 0 duplicates introduced, 50,000 of 50,000 recovered
date: 2026-09-15
git_commit: 34773b0ff7104b1c891d3bfbd094a2754db2d577
environment: ENV-LOCAL-DOCKER
dataset slice: 50,000 source events, 4,000 per trigger
command / test: scripts/test_restart.py
config: driver killed with pkill after 5 raw commits, no clean shutdown, then restarted against
        the same checkpoint directory
evidence: benchmarks/raw/restart_test.json, docs/evidence/phase6/restart_recovery.log
notes: killed at 19,980 of 50,000 rows in Bronze with 30,020 remaining. Commits advanced from
       raw 5 / dedup 1 / metrics 1 to raw 13 / dedup 11 / metrics 5, so the query resumed rather
       than restarting from the beginning. Final state is exactly 50,000 Bronze rows, 50,000
       distinct event ids, 50,000 deduplicated rows, and 777 windows. Not a recovery TIME
       measurement; that belongs to Phase 21.
```

### ENRICH-ANYPROP-001

```text
metric_id: ENRICH-ANYPROP-001
metric: full-dataset share of events with no item property at or before the event time
value: 14.266% (2,362,903 of 2,756,101 joinable)
date: 2026-09-16
git_commit: 08e71a604f6d93898a83657a4135ca69ab466365
environment: ENV-LOCAL-DOCKER
dataset slice: complete Retailrocket source, all 2,756,101 events, all 1,104 property names
command / test: batch/jobs/measure_enrichment.py --run-label fulldata
config: Spark range join against dim_items_scd validity intervals
evidence: benchmarks/raw/enrichment_measurement.json
notes: this is the CORRECTNESS CHECK for the point-in-time join, not the headline enrichment
       number. It reproduces DATASET-PITJOIN-001, measured independently with pandas in Phase 2,
       to a difference of 0.000 points. Every component matches exactly: 255,585 events whose
       item has no property record at all, and 137,613 events preceding their item's first
       property. Two independent implementations agreeing to the record is what makes the join
       trustworthy.
```

### ENRICH-CATEGORY-001

```text
metric_id: ENRICH-CATEGORY-001
metric: full-dataset share of events that cannot be enriched with a category
value: 23.835% (2,099,173 of 2,756,101 enriched)
date: 2026-09-16
git_commit: 08e71a604f6d93898a83657a4135ca69ab466365
environment: ENV-LOCAL-DOCKER
dataset slice: complete Retailrocket source
command / test: batch/jobs/measure_enrichment.py --run-label fulldata
config: as above, categoryid property only
evidence: benchmarks/raw/enrichment_measurement.json
notes: THIS is the number that matters for the recommender, and it is higher than
       ENRICH-ANYPROP-001 for a real reason rather than a defect. 263,730 events involve an item
       that already has some property recorded but does not yet have a category. Quoting 14.266%
       as the enrichment rate would overstate coverage by 9.6 points.

       Do not compare this figure to DATASET-PITJOIN-001. They answer different questions, and
       conflating them produced an apparent 9.5 point regression during Phase 7 that turned out
       to be a category error, not a bug.

       Silver produced exactly 2,099,173 enriched rows, matching what the source makes possible.
       The remaining rows are NULL, never future-filled.
```

### SILVER-LEAK-002

```text
metric_id: SILVER-LEAK-002
metric: Silver rows carrying a property whose validity begins after the event, full dataset
value: 0 of 2,756,101
date: 2026-09-16
git_commit: 08e71a604f6d93898a83657a4135ca69ab466365
environment: ENV-LOCAL-DOCKER
dataset slice: complete Retailrocket source
command / test: batch/jobs/measure_enrichment.py --run-label fulldata
config: assertion evaluated over every row, not a sample
evidence: benchmarks/raw/enrichment_measurement.json
notes: extends SILVER-PITJOIN-001, which proved the same property over 100,000 rows against an
       independent pandas merge_asof. This is the full-scale confirmation.
```

### SILVER-REJECT-001

```text
metric_id: SILVER-REJECT-001
metric: contract-invalid records routed to silver_rejected with the correct category
value: 610 of 610, split 203 / 192 / 215 exactly as injected
date: 2026-09-16
git_commit: 08e71a604f6d93898a83657a4135ca69ab466365
environment: ENV-LOCAL-DOCKER
dataset slice: 30,000 events, 6% malformed injection, seed 42
command / test: scripts/test_silver_rejects.py
config: simulator invalid_field modes mapped to Silver rejection reasons
evidence: benchmarks/raw/silver_rejects_test.json
notes: before this test the rejection rules had never once executed, because every earlier
       fixture was clean and rejected_rows was 0 on every run. Rules that have never fired are
       not tested rules.

       Also confirms the two reject surfaces are distinct: 1,170 wire-level records
       (raw_garbage, unknown_schema_id) never reach Silver at all, while 610 decodable but
       contract-invalid records do and are categorised.
```

### TIME-ROUNDTRIP-001

```text
metric_id: TIME-ROUNDTRIP-001
metric: epoch millis to Spark timestamp and back, exactness and timezone independence
value: 0 mismatches across 200,000 table rows and 12 synthetic cases, identical in 5 timezones
date: 2026-09-16
git_commit: 08e71a604f6d93898a83657a4135ca69ab466365
environment: ENV-LOCAL-DOCKER
dataset slice: Bronze and Silver `silver` label, plus crafted DST-boundary instants
command / test: batch/jobs/test_timestamp_roundtrip.py
config: timestamp_millis then unix_millis, run under UTC, America/Denver, Asia/Kolkata,
        Australia/Sydney, Pacific/Chatham
evidence: benchmarks/raw/timestamp_roundtrip.json
notes: validates the decision to store event time as a plain Avro long rather than the
       timestamp-millis logical type. Cases include both sides of US and EU daylight-saving
       transitions and Pacific/Chatham, which has a 45 minute offset and observes DST. All five
       session timezones produce byte-identical results, so correctness does not depend on where
       the host machine is.
```

### STREAM-FULLSCALE-001

```text
metric_id: STREAM-FULLSCALE-001
metric: complete dataset through Kafka, the three-query Spark chain, and into Bronze
value: 2,756,101 events in 238.1 s
date: 2026-09-16
git_commit: 08e71a604f6d93898a83657a4135ca69ab466365
environment: ENV-LOCAL-DOCKER
dataset slice: complete Retailrocket source
command / test: scripts/run_full_enrichment.py
config: 50,000 maxOffsetsPerTrigger, 56 micro-batches, driver 1g, executor 1600m, 6 cores,
        local Delta sink
evidence: benchmarks/raw/full_enrichment_run.json
notes: this is a WALL-CLOCK STAGE TIMING for one run, not a sustained throughput benchmark, and
       must not be quoted as an events-per-second figure. It was not run repeatedly, latency
       percentiles were not captured, and nothing was tuned. Phase 21 owns throughput and
       latency measurement under controlled load. Recorded here only as evidence that the
       pipeline handles the full dataset rather than only fixtures.

       Other stages: producer 115.0 s, item SCD 20.4 s, Silver 57.0 s.
```

### LAKEHOUSE-PERSIST-001

```text
metric_id: LAKEHOUSE-PERSIST-001
metric: Delta survives a full service restart, and rebuilds reproduce byte-identical content
value: 10 tables identical after restart, 10 identical after rebuild
date: 2026-09-16
git_commit: 08e71a604f6d93898a83657a4135ca69ab466365
environment: ENV-LOCAL-DOCKER
dataset slice: `silver` label lakehouse, 100,000 events
command / test: scripts/test_restart_persistence.py
config: docker compose down, verify files on host with zero containers, up, fingerprint,
        rebuild Silver and Gold, fingerprint again
evidence: benchmarks/raw/restart_persistence_test.json
notes: fingerprints are order-independent sums over per-row hashes, so a rebuild writing the
       same content into a different file layout still compares equal while any content change
       does not. Comparing row counts alone would pass even if every value changed.
```

### REGRESSION-SUITE-001

```text
metric_id: REGRESSION-SUITE-001
metric: every phase verification run together after the Phase 7 changes
value: 12 of 12 passed in 1000.2 s
date: 2026-09-16
git_commit: 08e71a604f6d93898a83657a4135ca69ab466365
environment: ENV-LOCAL-DOCKER
dataset slice: each check builds its own fixture
command / test: scripts/run_regression.py
config: phases 3, 4, 5, 6 and 7 checks plus the validation parity unit tests
evidence: benchmarks/raw/regression_suite.json
notes: run because Phase 7 changed behaviour earlier phases depend on: failOnDataLoss now
       defaults true, Spark validates schema ids against the registry, idle detection watches
       batch ids, and a failed query raises instead of exiting zero. No earlier guarantee
       regressed.
```

### SILVER-PITJOIN-001

```text
metric_id: SILVER-PITJOIN-001
metric: Silver rows enriched with a property whose validity begins after the event
value: 0 of 100,000, and 0 disagreements against an independent as-of join
date: 2026-09-16
git_commit: 08e71a604f6d93898a83657a4135ca69ab466365
environment: ENV-LOCAL-DOCKER
dataset slice: source rows 100,000 to 200,000, chosen to straddle the first property snapshot
command / test: scripts/verify_silver.py
config: range join on dim_items_scd half-open [valid_from, valid_to), left join so unmatched
        events survive with null
evidence: benchmarks/raw/silver_verification.json
notes: expectations recomputed with pandas merge_asof directly from the CSVs, so Spark is not
       grading itself. Spark and pandas both enriched exactly 31,408 rows with zero
       disagreements across all 100,000 events. The tightest case was an event 9,663 ms AFTER
       its property snapshot, positive, so nothing leaks at the boundary either.

       The 68.6% miss rate on THIS slice is not the project's miss rate and must not be quoted
       as one. This fixture sits early in the timeline where many items have no property
       snapshot yet. The full-dataset figure is DATASET-PITJOIN-001 at 14.27%.
```

### SILVER-SCD-001

```text
metric_id: SILVER-SCD-001
metric: item property snapshots collapsed into validity intervals
value: 2,291,853 tracked property rows to 1,031,473 intervals, 2.22x compression
date: 2026-09-16
git_commit: 08e71a604f6d93898a83657a4135ca69ab466365
environment: ENV-LOCAL-DOCKER
dataset slice: all categoryid and available rows across both property files
command / test: batch/jobs/build_item_scd.py
config: 18 discrete snapshots collapsed where the value did not change between them
evidence: benchmarks/raw/item_scd_stats.json
notes: categoryid produces 442,672 intervals over 417,053 items, so 1.06 per item. Categories
       barely change, roughly 25,600 changes in total. available produces 588,801 intervals,
       1.41 per item. Open intervals total 834,106, which is exactly 417,053 items times 2
       properties, confirming every item-property pair has precisely one current interval.
```

### BACKFILL-RANGE-001

```text
metric_id: BACKFILL-RANGE-001
metric: bounded Silver rebuild from Bronze, correctness and blast radius
value: 2 partitions rebuilt byte-identically, 5 outside the range untouched
date: 2026-09-16
git_commit: 08e71a604f6d93898a83657a4135ca69ab466365
environment: ENV-LOCAL-DOCKER
dataset slice: 100,000 event fixture across 7 daily partitions, rebuild window 2015-05-10 to
               2015-05-12 exclusive
command / test: scripts/test_backfill_range.py
config: Delta replaceWhere on an event_date predicate
evidence: benchmarks/raw/backfill_range_test.json
notes: partitions fingerprinted before and after with an order-independent per-row hash, so a
       rewrite preserving content is provably identical regardless of file layout. Total row
       count unchanged at 100,000, partition set unchanged, Delta version advanced 0 to 1.
       Proves Silver is a pure function of Bronze plus the SCD, which is what Phase 8 depends on.
```

### STORAGE-PARTITION-001

```text
metric_id: STORAGE-PARTITION-001
metric: time partitioning, files skipped and wall clock effect
value: 5x fewer files scanned, 0.85x speedup (15% slower)
date: 2026-09-16
git_commit: 08e71a604f6d93898a83657a4135ca69ab466365
environment: ENV-LOCAL-DOCKER
dataset slice: same 100,000 row Silver written twice, partitioned by event_date and flat
command / test: batch/jobs/benchmark_partitioning.py
config: single-day filter, count and groupBy, median of 5 repeats
evidence: benchmarks/raw/partition_benchmark.json
notes: pruning demonstrably works, 1 file scanned versus 5, and both return the same 16,178
       rows. But the partitioned table is SLOWER in wall time here: partitioning produced 15
       small files versus 6, and per-file overhead dominates at this data size while Spark's
       fixed ~150 ms startup swamps the I/O saved. The mechanism is correct and the benefit
       scales with data volume; at 100,000 rows it has not arrived yet. Do not claim
       partitioning made queries faster on the basis of this run.
```

### BACKFILL-TRANSFORM-001

```text
metric_id: BACKFILL-TRANSFORM-001
metric: a changed transformation rule reprocessed from Bronze over a bounded range
value: 2 partitions updated, 5 left untouched, 9 of 9 checks pass
date: 2026-09-16
git_commit: 71f76d4d98ded9e9d0fb1b17620f0a969c60e410
environment: ENV-LOCAL-DOCKER
dataset slice: `silver` label, 100,000 events across 7 daily partitions, range 2015-05-10 to
               2015-05-12 exclusive
command / test: scripts/test_backfill_transformation.py
config: additive rule change adding category_known_since_ms and category_age_at_event_ms to
        fact_events, applied via scripts/backfill.py
evidence: benchmarks/raw/backfill_transformation_test.json
notes: the test recreates the pre-change schema first, so it is repeatable rather than only
       passing on a table that happens to be mid-migration.

       Verified: the new columns appear only inside the backfilled range, partitions outside it
       still hold NULL, row counts are unchanged at 100,000 before and after, a subsequent full
       rebuild propagates the change everywhere, and Bronze is byte-identical throughout.

       The change is additive by design, so enrichment coverage is untouched and
       ENRICH-CATEGORY-001 at 23.835% remains valid. Measured category age at event ranges from
       9,663 ms to 313,883,766 ms, always non-negative, which is a second independent check that
       no property predates its own validity.
```

### BACKFILL-SCOPE-001

```text
metric_id: BACKFILL-SCOPE-001
metric: what a bounded backfill actually rewrites, and how long it takes
value: 28,321 of 100,000 rows reprocessed in 35.8 s
date: 2026-09-16
git_commit: 71f76d4d98ded9e9d0fb1b17620f0a969c60e410
environment: ENV-LOCAL-DOCKER
dataset slice: `silver` label, 2 of 7 daily partitions
command / test: scripts/backfill.py --start-date 2015-05-10 --end-date 2015-05-12
config: Delta replaceWhere plus mergeSchema, driver 1g, executor 1600m, 6 cores
evidence: benchmarks/raw/backfill_report.json
notes: Silver 18.9 s, Gold 16.9 s. Three tables rebuilt surgically (fact_events,
       fact_transactions, mart_daily_item_metrics) and two rebuilt in full
       (mart_item_funnel, mart_category_performance) because they aggregate across all history
       with no date in their grain. Rebuilding those from the bounded slice would silently
       discard every other day, so they read all of Silver instead. Reported by the job rather
       than hidden.

       A single wall-clock timing on one machine, not a throughput benchmark.
```

### REGRESSION-SUITE-002

```text
metric_id: REGRESSION-SUITE-002
metric: every phase verification run together after the Phase 8 changes
value: 13 of 13 passed in 1121.8 s
date: 2026-09-16
git_commit: 71f76d4d98ded9e9d0fb1b17620f0a969c60e410
environment: ENV-LOCAL-DOCKER
dataset slice: each check builds its own fixture
command / test: scripts/run_regression.py
config: phases 3 through 8 plus the validation parity unit tests
evidence: benchmarks/raw/regression_suite.json
notes: supersedes REGRESSION-SUITE-001. Run because Phase 8 changed the Gold writer to use
       mergeSchema and added a bounded-rebuild path, either of which could have broken the
       Phase 7 guarantees. Nothing regressed.
```
