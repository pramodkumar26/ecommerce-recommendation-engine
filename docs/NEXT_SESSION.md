# Next Session

Session handoff file. Updated at the end of every work session so a fresh chat can resume
without rereading the whole roadmap. Overwrite the Current State block each time; append to
the log at the bottom.

## Current state

```text
phase:                Phase 2 complete
last task completed:  Dataset profiled, deterministic event id built and verified.
branch / commit:      main, Phase 2 changes not yet committed
services running:     none, Docker not needed for this phase
services stopped:     streaming profile down, volumes kept
next command to run:  make profile   (reproduces every Phase 2 number)
unresolved error:     none
next test:            Phase 3, deterministic replay of a bounded range twice
Azure left alive:     none, no Azure resources provisioned yet
```

## Phase order change

Phase 1B, Terraform and Azure, is deferred. The university tenant (colorado.edu) blocks the
Azure CLI application, so `az login` fails with AADSTS50105 and Terraform cannot authenticate.
An Azure for Students subscription exists with 100 USD of credit valid until 2027-09-15, so the
credit is worth keeping. A request is with CU OIT to either grant Azure CLI access or allow app
registration so a service principal can be used instead.

Nothing before Phase 7 depends on Azure, so work continues locally through Phases 2 to 6 and
Phase 1B slots in whenever access is resolved. Fallback if OIT declines: a personal Microsoft
account at pay-as-you-go, roughly 5 USD a month.

## Phase 2 result

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

## Phase 1A result (previous session)

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

Phase 3, Kafka producer and topic design. Three behavioral topics (`item_view`, `add_to_cart`,
`transaction`) plus `clickstream_dlq`, partitioned by visitor id, with a configurable replay
rate, burst mode, a fixed random seed, and injectable duplicates, malformed messages, and
delayed events.

Phase 3 is done when the simulator can replay a bounded source range twice deterministically,
partitioning is verified, and the producer rate is configurable.

The event id from Phase 2 is what makes deterministic replay testable, so that dependency is
already satisfied.

Estimate: 2 days.

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

Every one currently has `git_commit: pending`. The measurements are real but the commit
containing the producing code has not been made yet. Fill the SHA in before any of these numbers
is used outside this repository.

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
