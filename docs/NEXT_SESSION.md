# Next Session

Session handoff file. Updated at the end of every work session so a fresh chat can resume
without rereading the whole roadmap. Overwrite the Current State block each time; append to
the log at the bottom.

## Current state

```text
phase:                Phase 7 LOCAL COMPLETE, ADLS sync deferred to Azure access
last task completed:  Integration fixes, medallion layers, full-dataset enrichment verified.
branch / commit:      main, Phase 7 uncommitted at time of writing, see below
services running:     streaming profile up (kafka, schema-registry, spark, redis)
services stopped:     no Spark application running, all batch jobs finished
next command to run:  make up && .venv/bin/python scripts/run_regression.py
unresolved error:     none, 12 of 12 regression checks green
Azure left alive:     none, no Azure resources provisioned
```

## Verification commands

```bash
.venv/bin/python scripts/run_regression.py          # all 12 checks, about 17 minutes
.venv/bin/python -m pytest tests/ -q                # validation parity, under a second
.venv/bin/python scripts/run_full_enrichment.py     # full 2.75M dataset, about 8 minutes
```

Individual checks, if the suite is too slow:

```bash
.venv/bin/python scripts/verify_silver.py           # point-in-time, no future leakage
.venv/bin/python scripts/test_silver_rejects.py     # contract rejects fire and categorise
.venv/bin/python scripts/test_backfill_range.py     # bounded rebuild is surgical
.venv/bin/python scripts/test_restart_persistence.py  # cycles the stack, Delta survives
make verify-streaming                               # phase 5
make verify-reliability                             # phase 6
```

## Row counts

Full dataset, run label `fulldata`:

```text
source events        2,756,101
bronze_events        2,756,101
silver_events        2,756,101
enriched with category  2,099,173   (76.165%)
unenriched (NULL)         656,928   (23.835%)
rejected                        0   (clean source, no injection)
```

Reject path, run label `rejects`, 30,000 events with 6% malformed injection:

```text
produced                30,000
wire-level, never decoded  1,170   raw_garbage + unknown_schema_id
bronze_events           28,830
silver_rejected            610   203 negative_visitor_id
                               192 event_timestamp_out_of_range
                               215 non_transaction_with_transaction_id
silver_events           28,220
```

## Phase 7 status

| Definition of done | Status |
|---|---|
| Bronze immutable decoded history | done |
| contract validation between Bronze and Silver | done, parity tested |
| durable reject path for decodable-but-invalid | done, `silver_rejected`, categorised |
| one valid row per deterministic event_id | done |
| canonical epoch timestamps exact, UTC explicit | done, TIME-ROUNDTRIP-001 |
| point-in-time enrichment, no future values | done, 0 of 2,756,101 |
| enrichment miss rate measured | done, 23.835% category, 14.266% any-property |
| Gold built from Silver, reconciles | done |
| Silver and Gold deterministically rebuildable | done |
| local Delta persists across restart | done, LAKEHOUSE-PERSIST-001 |
| failOnDataLoss deliberately configured | done, defaults true |
| replay vs live watermark semantics documented | done, `docs/lakehouse.md` |
| Phase 1-6 tests still pass | done, 12 of 12 |
| **scheduled local-to-ADLS sync** | **BLOCKED on Azure** |
| **cloud timing recorded separately** | **BLOCKED on Azure** |

Local work is complete. The phase closes when the two Azure items land.

## Integration issues fixed this session

Five real problems, three found while fixing the other two.

**failOnDataLoss was hardcoded false.** Now defaults true with an explicit `--allow-data-loss`
escape hatch for the topic-recreation recovery case. A no-silent-loss claim cannot have silent
loss as its default.

**The DLQ router never committed offsets.** Confirmed: `enable.auto.commit=false` with no commit
call. Rather than turning it into a service, it is documented as what it actually is, a Phase 4
verification harness that must re-read the whole topic each run. The Spark Bronze to Silver path
owns durable reject handling, with checkpoints. Reasoning recorded in the module docstring.

**Spark was not validating schema ids.** `from_avro` takes a static schema and ignores the
Confluent header entirely, so 613 records carrying an unregistered schema id decoded against v1
and reached Bronze looking valid. The registry is now queried at startup and untrusted ids route
to `undecodable`.

**`run_bounded` could not tell a dead query from a finished one.** A query killed by executor
memory truncated a run at 3 of 5 batches and the job exited zero reporting success. It now calls
`query.exception()` and raises. Executor memory also went 1g to 1600m, since 6 cores sharing 1g
caused the OOM.

**Idle detection summed `numInputRows`.** With three chained queries a Delta source can commit a
zero-row batch while still working, so all three read zero at once and the timer started with
data pending. Now tracks batch ids, which only advance when a query is genuinely working.

Also corrected: the `windowed_metrics` docstring contradicted itself after three rewrites, and
STREAM-LATENESS-001 was labelled "measured" when it is reconstructed, now "MODELED replay
lateness" with the empirical counterpart named.

## The 9.5 point discrepancy that was not a bug

The full-dataset run first reported a 23.835% enrichment miss rate against Phase 2's 14.266%.
Investigated rather than adjusted.

The two figures answer different questions. Phase 2 asked whether an item had **any** property
at or before the event, across all 1,104 property names. Silver asks whether it has a
**categoryid**. 263,730 events involve items with some property recorded but no category yet.

Reproducing Phase 2's exact definition in Spark gives 14.266%, a difference of 0.000 points,
with every component matching: 255,585 items with no property, 137,613 events preceding their
item's first property. Two independent implementations agreeing to the record.

Both are now recorded. ENRICH-ANYPROP-001 is the correctness check. ENRICH-CATEGORY-001 at
23.835% is the number to quote for enrichment coverage; using 14.266% would overstate it by 9.6
points.

## Phase order change

Phase 1B, Terraform and Azure, is deferred, and Phase 7 is now also partially blocked on it. The university tenant (colorado.edu) blocks the
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

Azure access was reported as ticketed with CU OIT and expected within 2 to 3 hours. Two paths:

**If Azure access works.** Close Phase 7 by adding the scheduled local-to-ADLS sync and a
separately labelled cloud-integration smoke test, then Phase 1B for the Terraform foundation.
Do NOT write Spark micro-batches directly to ADLS; that would mix WAN latency and Azure
transaction cost into the local streaming benchmark. Sync selected Delta snapshots on a
schedule, and record cloud timing under its own metric ids, never merged with local figures.

Pinned versions for the ABFS work, verified in this repo, not assumed:

```text
Spark 3.5.7, Hadoop 3.3.4 (bundled), Delta 3.3.2, Scala 2.12, Java 17
hadoop-azure must match Hadoop 3.3.4
```

The roadmap budgets a day for exactly this dependency chain, and Apple Silicon can add JAR
friction. Do not start it while the tenant question is open.

**If Azure access does not work.** Phase 8, backfill and replay, needs nothing cloud. Shared
transformation logic, a bounded backfill command, and one intentional transformation change
reprocessed from Bronze with before and after counts. `test_backfill_range.py` already proves
the surgical rebuild works, so Phase 8 is mostly changing a rule and showing Gold updates.

First command either way:

```bash
make up
.venv/bin/python scripts/run_regression.py
```

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
