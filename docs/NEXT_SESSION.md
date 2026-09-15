# Next Session

Session handoff file. Updated at the end of every work session so a fresh chat can resume
without rereading the whole roadmap. Overwrite the Current State block each time; append to
the log at the bottom.

## Current state

```text
phase:                Phase 4 complete
last task completed:  Avro schemas, Schema Registry compatibility, and DLQ routing verified.
branch / commit:      main, Phase 4 changes not yet committed
services running:     streaming profile up (kafka, schema-registry, spark, redis)
services stopped:     none
next command to run:  make verify-schema-dlq   (resets topics, reruns every Phase 4 check)
unresolved error:     none
next test:            Phase 5, window metrics correct on a known fixture
Azure left alive:     none, no Azure resources provisioned yet
```

## Phase 4 result

Definition of done, all met. Ten checks pass:

- v2 accepted as backward compatible, v3 rejected by the registry
- a v1 reader decodes a v2 record and drops the two unknown fields
- every produced record consumed, 20,422 of 20,422
- 960 malformed records routed to the DLQ, matching what was put on the wire
- valid traffic kept flowing, 19,462 records
- valid + DLQ == consumed exactly, nothing lost or invented
- all three error types observed
- DLQ records carry topic, partition, offset, key, and original bytes

Evidence in `docs/evidence/phase4/` and `benchmarks/raw/schema_dlq_verification.json`,
`benchmarks/raw/dlq_router_stats.json`. Four ledger rows added, `pending` until commit.

### What was built

Four Avro schemas in `producer/schemas/`. `kafka/register_schemas.py` registers them and runs
the compatibility checks. `kafka/validation.py` holds the decode and validation rules.
`kafka/dlq_router.py` consumes, routes bad records, and keeps going.
`scripts/verify_schema_dlq.py` runs the full Phase 4 proof after resetting topics and subjects.

The simulator now serialises Avro through Schema Registry. Its malformed modes were reworked to
match Avro's failure layers: `raw_garbage`, `unknown_schema_id`, and `invalid_field`.

### Three decisions worth remembering

Timestamps are plain `long` epoch milliseconds, not the Avro `timestamp-millis` logical type.
fastavro decodes that logical type into timezone-aware datetimes and Spark maps it to
`TimestampType` with session-timezone conversion. Phases 5 and 6 depend on exact event-time
semantics, and a silent timezone shift there would corrupt every windowed aggregate in a way
that is very hard to spot.

The `transaction_id` validation rule (present on transaction events, absent on all others) is
only enforceable because Phase 2 measured it holding with zero exceptions across 2,756,101 rows.

Validation rules live in a shared module so Phase 5's Spark job applies exactly the same rules
as the Phase 4 router rather than a drifting second copy.

### Two bugs found and fixed during the phase

The first Avro conversion called the corruption function twice per record, consuming the seeded
RNG twice and selecting the mode twice. It showed up as double-counted malformed modes in the
stats. The simulator was rewritten so every injection decision is drawn once, in a fixed order,
inside `plan()`.

The DLQ router reused the `record` variable across loop iterations, so a decode failure would
have attached the previous record's `schema_version` to the DLQ entry. Now reset per iteration.

## Phase order change

Phase 1B, Terraform and Azure, is deferred. The university tenant (colorado.edu) blocks the
Azure CLI application, so `az login` fails with AADSTS50105 and Terraform cannot authenticate.
An Azure for Students subscription exists with 100 USD of credit valid until 2027-09-15, so the
credit is worth keeping. A request is with CU OIT to either grant Azure CLI access or allow app
registration so a service principal can be used instead.

Nothing before Phase 7 depends on Azure, so work continues locally through Phases 2 to 6 and
Phase 1B slots in whenever access is resolved. Fallback if OIT declines: a personal Microsoft
account at pay-as-you-go, roughly 5 USD a month.

## Phase 2 result (earlier)

Definition of done, both met:

- `docs/dataset_profile.md` contains measured counts for every field the roadmap lists
- deterministic replay produces identical IDs across two independent passes, zero collisions

Evidence in `benchmarks/raw/`: `dataset_profile.json`, `dataset_joins.json`,
`event_id_reproducibility.json`. First eight rows written to `docs/MEASUREMENTS.md`.

### What the data actually says

The roadmap's headline figures all confirm: 2,756,101 events, 1,407,580 visitors, 235,061
items, 22,457 transactions, 138 days.

Three findings that change downstream design:

Interaction sparsity. 71.16% of visitors appear exactly once, median interactions per visitor
is 1. At the proposed minimum-history threshold of 5, only 81,620 visitors remain, 5.80%. Of
those just 7,610 ever transacted. Transaction-only positives are not viable, so the project uses
weighted interactions and compares weightings in Phase 13.

Item properties are 18 weekly snapshots, not a continuous change log. Every property row carries
one of 18 timestamps, all at 03:00:00 UTC, spaced 7 or 14 days apart. This makes the Phase 7
point-in-time join an as-of lookup against 18 known dates rather than an arbitrary temporal
join, which is considerably simpler and fully testable.

Point-in-time join miss rate is 14.27%. 255,585 events have an item with no property record at
all, 137,613 occur before their item's first snapshot, and events begin 7 days before the first
snapshot exists. Policy set in the profile: those events get null enrichment and are never
backfilled from a later snapshot, because that would be the exact leakage the join prevents.

### Identity design

`producer/event_id.py` hashes source file, source row number, timestamp, visitor id, item id,
event type, and transaction id. Source position is included because the dataset contains 460
byte-identical rows. Without position they would collapse to one ID and Phase 6 dedup would
silently drop 460 real records.

Verified with two full passes producing an identical digest, zero collisions across 2,756,101
rows, plus nine unit checks.

## Phase 1A result (earlier)

Definition of done, all met:

- a fresh copy containing only tracked files booted the `streaming` profile using the README
  commands (`make venv`, `make install`, `make env`, `make up`) with no manual fixes
- Spark executes a job on the standalone cluster, including a shuffle across 8 partitions
- Kafka producer and consumer smoke test passes, keys map to stable partitions
- `docker stats` confirms the profile fits the budget: Compose limits total 5.4 GiB of the
  7.75 GiB available, idle usage about 1.2 GiB

Evidence in `docs/evidence/phase1a/`: `compose_config.yml`, `startup.log`,
`smoke_and_stats.txt`.

## What was built

`docker-compose.yml` with a `streaming` profile holding Kafka 7.9.2 in KRaft mode, Schema
Registry 7.9.2, Spark 3.5.7 master and worker, and Redis 7.4. Explicit memory limits on every
service. `.env.example` for versions, ports, and paths. A `Makefile` as the entry point, a
`README.md` documenting the boot sequence, and three smoke tests under `scripts/`.

Decisions recorded in `docs/ENVIRONMENT.md`:

- Kafka runs in KRaft mode, no Zookeeper, saving a container and roughly 0.5 GB
- Spark 3.5.7 over 4.0.1 because it bundles Hadoop 3.3.4, the best documented pairing with
  `hadoop-azure` and Delta 3.x, which is where the Phase 7 risk day is budgeted
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE` is false so Phase 3 has to design topics explicitly
- Kafka and Redis use named volumes, so `make down` keeps data and only `make clean` drops it

The Kafka smoke test was rewritten after the first version passed for the wrong reason. It was
reading messages left over from a previous run, so it would have passed even if the current run
produced nothing. It now tags each run with a unique id, uses a fresh consumer group, and
asserts that the partitions read match the partitions written.

## Constraints carried forward

Docker memory is 7.75 GiB on a 16 GB host, left at the default on purpose so the host does not
swap during later latency benchmarks. The `all` profile will not be run on this machine.

The Spark image ships Python 3.8.10 while the project venv is 3.11.16. This is fine because
Spark code runs entirely in the container where driver and executor share one interpreter, and
the venv serves the producer, ML, and API. If a Spark job ever needs a newer interpreter the
answer is a custom image.

`JAVA_HOME` is still not set in the shell profile. Only matters for host-side PySpark.

## Next up

Phase 5, core Spark streaming. Read all three topics from Kafka in Structured Streaming, parse
event time, compute windowed aggregates, write live metrics to Redis, and write raw events to
Delta on the local volume.

Phase 5 is done when the stream processes all three topics and basic window metrics are correct
on a known fixture.

Watch for: Spark needs the Kafka and Avro connector JARs, and the Spark image is 3.5.7 with
Hadoop 3.3.4. Pin the matching `spark-sql-kafka-0-10` and `spark-avro` versions and record them
in `docs/ENVIRONMENT.md`. This is the first phase that touches the JAR compatibility risk the
roadmap budgets a day for in Phase 7.

Estimate: 2 to 3 days.

## Open questions

- Azure CLI access in the colorado.edu tenant. Waiting on CU OIT.
- Whether to collapse repeated visitor-item interactions or keep them. 15.53% of pairs repeat.
  Decided in Phase 12.
- Terraform remote state backend, or local state for now. Decide at the start of Phase 1B.
- Azure region. Pick one close by and use it consistently for cost and latency comparability.

## Measurement ledger

Eight rows written in Phase 2: DATASET-EVENTS-001, DATASET-VISITORS-001, DATASET-ITEMS-001,
DATASET-TRANSACTIONS-001, DATASET-SPAN-001, DATASET-PITJOIN-001, DATASET-ELIGIBLE-001, and
IDENTITY-REPRO-001.

All eight are pinned to commit `8dbe1f4a997b584b29120dfefcd5706e35a9746d` and measured against
the source file checksums recorded in the ledger. They are safe to quote.

## Session log

### 2026-09-15

Read the roadmap in full. Created `CLAUDE.md` with conventions, hard rules, and the section 44
errata note that the core target is 32 to 44 working days. Created the docs scaffolding.

Ran Phase 0 host preflight. Installed OpenJDK 17, Terraform 1.16.2, Azure CLI 2.90.0, and
Python 3.11.16. Recorded host facts and Docker limits. Both container smoke tests ran native
arm64.

Checked the GitHub repo that was suggested as a starting point,
`pramodkumar26/E-commerce-Streaming-Pipeline`. It holds a different project built on Google
Cloud Pub/Sub with the Olist dataset, so it was left untouched and a new repo,
`pramodkumar26/ecommerce-recommendation-engine`, was used instead.

Ran Phase 1A. Built the Compose stack, Makefile, README, and smoke tests. Verified the whole
setup from a clean copy of the tracked files. Took everything down at the end. No Azure
resources created, nothing billing.

Ran Phase 2 after deferring Phase 1B on the Azure CLI block. Profiled all four source files,
confirmed the roadmap's headline figures, and found three things that shape later phases:
interaction sparsity with 71% of visitors appearing once, item properties being 18 weekly
snapshots rather than a change log, and a 14.27% point-in-time join miss rate. Built and
verified the deterministic event id. Wrote the first eight measurement ledger rows. Docker was
not needed and stayed down for the whole phase.
