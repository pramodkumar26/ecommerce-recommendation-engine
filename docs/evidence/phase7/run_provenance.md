# Phase 7 run provenance

Recorded before the long full-dataset enrichment run so that a run which fails partway through
can still be attributed to a known environment.

## Git state at time of run

```text
branch:          main
HEAD commit:     5c7801e67868a46c4ef1d779867e430f79138cef
working tree:    DIRTY, 24 uncommitted files
```

The tree is deliberately dirty. Phase 7 is being committed once, after the full regression suite
is green, rather than in several "almost done" commits. Every measurement taken during this
window is therefore attributed to the FINAL Phase 7 commit, not to 5c7801e, and the ledger rows
are filled in after that commit exists. Any row still showing 5c7801e would be wrong.

## Environment

```text
host:            MacBook Pro Mac17,2, Apple M5, 10 cores, 16 GB
macOS:           26.6.2 (25G83), arm64
Docker Desktop:  29.7.2, 10 CPU, 7.75 GiB limit
Spark:           3.5.7 (ecomm-rec/spark:3.5.7, custom image over apache/spark)
Hadoop:          3.3.4 (bundled with Spark 3.5.7)
Delta:           3.3.2
Kafka:           Confluent 7.9.2, KRaft
Schema Registry: Confluent 7.9.2, BACKWARD
Python (venv):   3.11.16
Python (Spark):  3.8.10
```

Cluster at time of run: 1 worker, 6 cores, 2560m container limit.
Submits use driver 1g, executor 1600m, total-executor-cores 6.

## Exact commands

Full-dataset enrichment run:

```bash
.venv/bin/python scripts/run_full_enrichment.py
```

which performs, in order:

```bash
# 1 reset topics and re-register schemas
.venv/bin/python kafka/create_topics.py
.venv/bin/python kafka/register_schemas.py

# 2 replay the entire source, 2,756,101 events
.venv/bin/python producer/simulator.py --limit 2756101 --rate 25000 --seed 42

# 3 stream into Bronze
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --driver-memory 1g --executor-memory 1600m \
  --total-executor-cores 6 --packages "$SPARK_PACKAGES" \
  /opt/spark/project/streaming/jobs/stream_events.py --run-label fulldata \
  --max-offsets-per-trigger 50000 --await-seconds 7200 --idle-seconds 120

# 4 build the SCD and Silver
docker compose exec -T spark-master /opt/spark/bin/spark-submit ... \
  /opt/spark/project/batch/jobs/build_item_scd.py --run-label fulldata
docker compose exec -T spark-master /opt/spark/bin/spark-submit ... \
  /opt/spark/project/batch/jobs/build_silver.py --run-label fulldata
```

## What this run is for

Reproducing the enrichment miss rate over the whole dataset and comparing it against
DATASET-PITJOIN-001, the 14.27% measured independently in Phase 2 with pandas.

The Phase 7 fixture (source rows 100,000 to 200,000) showed 68.6%, which is expected and not
comparable: that slice sits early in the timeline where most items have no property snapshot
yet. Only the full dataset is comparable to the Phase 2 figure.

If the implemented result differs materially from 14.27%, the join is wrong and gets
investigated. It does not get forced to match.

## Known risk

This replays 2.75M events rather than the 50,000 to 100,000 used so far, roughly 27x. If the run
fails, the most likely causes in order are executor memory during the range join, Kafka disk
under the 7.75 GiB Docker limit, and wall-clock time on the Bronze stream.
