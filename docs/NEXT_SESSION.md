# Next Session

Session handoff file. Updated at the end of every work session so a fresh chat can resume
without rereading the whole roadmap. Overwrite the Current State block each time; append to
the log at the bottom.

## Current state

```text
phase:                Phase 6 complete
last task completed:  Watermark measured, dedup, late-event policy, checkpoint restart proven.
branch / commit:      main, 34773b0
services running:     streaming profile up, no Spark application running
services stopped:     all test streams self-terminated and released their cores
next command to run:  make verify-reliability
unresolved error:     none
next test:            Phase 7, historical event never receives a future property value
Azure left alive:     none, no Azure resources provisioned yet
```

## Phase 6 result

Definition of done, all four met.

| Requirement | Result |
|---|---|
| injected duplicate does not alter final count | 2,389 removed, 777 windows exact |
| within-watermark event handled correctly | 0.56% dropped at 24h vs 3.45% at 1 min |
| Spark restarts from checkpoint | resumed at 19,980 of 50,000, finished at 50,000 |
| no silent loss in controlled test | 0 lost, 0 duplicates introduced |

Phase 5's 13 checks were re-run afterwards and still pass, so the restructure caused no
regression. Evidence in `docs/evidence/phase6/`. Four ledger rows added, pinned to `34773b0`.

### The watermark, measured instead of guessed

`streaming/jobs/measure_lateness.py` measures how far behind the running maximum event time each
record actually arrives, which is exactly what Spark compares against.

```text
p50  104 min    p90  737 min    p95  845 min    p99  916 min    max  959 min (16.0 h)
```

Nothing exceeded 24 hours at any batch size, so the watermark is 24 hours. The roadmap's
suggested 10 minute development default would have dropped 73% of records here.

Two things worth remembering. This is event-time lateness created by replay compression, not
network delay; a live deployment would see seconds. And smaller batches produce MORE lateness,
p50 220 min at 1,000 per trigger versus 0 at 20,000, because with fewer larger batches most
records have no preceding maximum to be late against. That is why the Phase 5 attempt at smaller
batches made things worse.

### The architecture changed, and why

The job is now three queries chained through Delta:

```text
Kafka   -> bronze_events     stateless, keeps duplicates
Bronze  -> deduped_events    dropDuplicates on event_id, 24h watermark
deduped -> metrics_5min      5 min windows, update mode, MERGE on window_start
```

Spark supports chaining multiple stateful operators only in append mode. Putting dedup and the
windowed aggregate in one update-mode query silently produced wrong results: 76% of events
vanished and the 24% that survived were exactly the records with zero lateness. Switching that
query to append mode then meant the aggregate lagged a full day of event time behind the
watermark, so on a 65 hour fixture almost nothing ever closed.

Splitting them gives each query at most one stateful operator, restores the proven update mode
plus MERGE aggregate, and matches the medallion layering Phase 7 needs anyway.

Two knock-on fixes: chained Delta streaming reads fail with DELTA_SCHEMA_NOT_SET until the table
has been written once, so `ensure_tables` bootstraps both tables empty with the exact schema.
Worker cores went 4 to 6 because three queries on four cores starve each other.

### A test assertion that was wrong, not the code

The late-event test first asserted the 24 hour watermark loses nothing, and it dropped 280
events. That run deliberately injects 10% extra delay on top of replay lateness, so a small tail
falling outside is the policy working. The zero-loss case is proven separately by the duplicate
test, where the same watermark with normal traffic yields exactly 50,000 deduplicated rows.

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

Phase 7, Bronze / Silver / Gold and point-in-time enrichment. Silver is where the actual data
cleaning happens: type and timestamp normalization, deduplication, invalid-record removal, and
the point-in-time item property join. Then Gold analytics and training tables, a time-based
partition strategy, and the scheduled ADLS sync.

Most of Phase 7 is local and does not need Azure. Only the scheduled sync and the
cloud-integration smoke test do, so start on Silver and Gold and slot the sync in when the
Azure CLI question is resolved.

Phase 7 is done when a historical event never receives a property value whose timestamp is in
the future, Bronze can regenerate Silver for a bounded range, one local-to-ADLS sync completes,
and cloud timing is recorded separately from local streaming timing.

What Phase 2 already established for this phase: item properties are 18 discrete weekly
snapshots rather than a change log, so the join is an as-of lookup against 18 known dates. The
measured miss rate is 14.27%, and those events get null enrichment rather than being backfilled
from a later snapshot, which would be the exact leakage the join exists to prevent.

This phase carries the roadmap's only budgeted risk day, for ABFS and JAR compatibility. The
connector chain is already pinned and recorded in `docs/ENVIRONMENT.md`, and Spark 3.5.7 bundles
Hadoop 3.3.4, so `hadoop-azure` must match 3.3.4.

Estimate: 3 to 4 days.

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
