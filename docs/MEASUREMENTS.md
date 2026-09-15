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
| DATASET-EVENTS-001 | total source events | 2,756,101 | 2026-09-15 | pending | `dataset_profile.json` |
| DATASET-VISITORS-001 | unique visitors | 1,407,580 | 2026-09-15 | pending | `dataset_profile.json` |
| DATASET-ITEMS-001 | unique items in events | 235,061 | 2026-09-15 | pending | `dataset_profile.json` |
| DATASET-TRANSACTIONS-001 | transaction events | 22,457 | 2026-09-15 | pending | `dataset_profile.json` |
| DATASET-SPAN-001 | source time span | 138.0 days | 2026-09-15 | pending | `dataset_profile.json` |
| DATASET-PITJOIN-001 | point-in-time join miss rate | 14.27% | 2026-09-15 | pending | `dataset_joins.json` |
| DATASET-ELIGIBLE-001 | visitors with 5+ interactions | 81,620 | 2026-09-15 | pending | `dataset_joins.json` |
| IDENTITY-REPRO-001 | event id collisions over full source | 0 of 2,756,101 | 2026-09-15 | pending | `event_id_reproducibility.json` |

`pending` means the measurement is real but the commit that contains the producing code has not
been made yet. Fill each in before any of these numbers is used outside this repository.

## Records

### DATASET-EVENTS-001

```text
metric_id: DATASET-EVENTS-001
metric: total rows in events.csv
value: 2,756,101
date: 2026-09-15
git_commit: pending
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
git_commit: pending
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
git_commit: pending
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
git_commit: pending
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
git_commit: pending
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
git_commit: pending
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
git_commit: pending
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
git_commit: pending
environment: ENV-LOCAL-HOST
dataset slice: full events.csv
command / test: scripts/verify_event_id.py
config: SHA256 over source_file|source_row_number|timestamp|visitor_id|item_id|event_type|transaction_id
evidence: benchmarks/raw/event_id_reproducibility.json
notes: both passes produced digest 16a58583137991e1b0895a684460fea2ceab428d5a78616b07d13e2f73ad5561.
       Nine unit checks also pass. Source position is in the hash so the 460 byte-identical
       source rows remain distinct records.
```
