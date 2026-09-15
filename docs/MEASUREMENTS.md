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

AREA:  DATASET, PRODUCER, STREAM, RECOVERY, CLOUD, DQ, MODEL, RETRIEVAL, API, COST
```

Examples: `DATASET-EVENTCOUNT-001`, `STREAM-THROUGHPUT-001`, `CLOUD-SYNC-001`.

## Row template

```text
metric_id:
metric:
value:
date:
git_commit:
environment:
dataset slice:
command / test used:
config:
evidence file:
notes:
```

## Index

| metric_id | metric | value | date | commit | evidence |
|---|---|---|---|---|---|
| | | | | | |

## Records

No measurements recorded yet. Phase 2 is the first phase that produces rows here, starting with
the dataset profile counts.
