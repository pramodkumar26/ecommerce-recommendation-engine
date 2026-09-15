# E-commerce Real-Time Recommendation Engine

A streaming data platform and recommendation service built on the Retailrocket clickstream
dataset. Events are replayed into Kafka, processed with PySpark Structured Streaming into
Bronze, Silver, and Gold Delta layers, turned into point-in-time features, and served as
Top-N recommendations through FAISS and FastAPI.

This is a work in progress. Sections are added as each phase is built and verified. No
performance or model numbers appear here until they have been measured and recorded in
`docs/MEASUREMENTS.md`.

## Current state

Local development stack running: Kafka, Schema Registry, Spark standalone cluster, Redis.
Nothing else is built yet.

## Architecture split

Stateful development services run locally in Docker Compose. Azure holds the durable and
deployable pieces: ADLS Gen2 for Delta snapshots and artifacts, Azure SQL serverless for dbt
models, Container Registry and Container Apps for the API, all provisioned with Terraform.

During development Spark writes Delta to a local bind-mounted volume, and selected snapshots
sync to ADLS on a schedule. A separate cloud-integration path writes directly to ADLS and is
benchmarked separately, so local streaming timings never mix with cloud storage timings.

## Requirements

- Docker Desktop
- Python 3.11
- Java 17 (only needed for host-side Spark work; Spark itself runs in containers)

## Setup

Create the virtualenv:

```bash
make venv
```

Install dependencies:

```bash
make install
```

Create the local env file:

```bash
make env
```

## Running the streaming stack

Start Kafka, Schema Registry, Spark master and worker, and Redis:

```bash
make up
```

Check status:

```bash
make ps
```

Verify all three services work:

```bash
make smoke
```

Stop the stack, keeping Kafka and Redis data:

```bash
make down
```

## Service endpoints

| Service | Endpoint |
|---|---|
| Kafka (from host) | `localhost:9092` |
| Kafka (in network) | `kafka:29092` |
| Schema Registry | http://localhost:8081 |
| Spark master UI | http://localhost:8080 |
| Spark worker UI | http://localhost:8082 |
| Redis | `localhost:6379` |

## Compose profiles

Profiles keep the resource footprint down. Only the services a phase needs get started.

| Profile | Services |
|---|---|
| `streaming` | Kafka, Schema Registry, Spark master, Spark worker, Redis |

Further profiles are added when the phases that need them are built.

## Repository layout

```text
producer/     event simulator and Avro schemas
kafka/        topic and broker configuration
streaming/    Spark Structured Streaming jobs and transforms
batch/        backfill and feature generation jobs
dbt/          warehouse models and tests
ml/           features, baselines, two-tower model, evaluation, retrieval, export
api/          FastAPI serving layer
airflow/      lifecycle DAGs
monitoring/   Prometheus and Grafana configuration
terraform/    persistent and deploy stacks
scripts/      helper and benchmark scripts
benchmarks/   raw benchmark output
docs/         architecture, dataset profile, measurements, evidence
```

## Dataset

Retailrocket Recommender System Dataset from Kaggle. Place the source CSVs in `data/raw/`.
They are not committed. Measured counts go in `docs/dataset_profile.md` once profiling runs.
