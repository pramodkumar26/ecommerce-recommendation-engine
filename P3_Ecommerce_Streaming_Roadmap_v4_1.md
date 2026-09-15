# E-commerce Real-Time Recommendation Engine - P3 Roadmap (v4.1, Azure + Local Dev)

**Purpose of this version:** make the project a strong Data Engineering + ML Engineering portfolio piece while keeping the development environment realistic, affordable, and recoverable across many work sessions.

**This v4.1 corrects the planning and storage details in v4.** The main changes are:

- Run the heavy stateful development stack locally with Docker Compose profiles instead of on a small Azure VM
- Keep Azure focused on durable storage, relational analytics, image registry, deployment, and infrastructure provisioning
- Move Terraform to the beginning of the project instead of adding it at the end
- Restore an explicit cost and shutdown discipline section
- Split the roadmap into a shippable core milestone and stretch work
- Replace the optimistic 33-day estimate with phase-derived timing: roughly 32 to 44 days for core and 48 to 67 days across all planned phases
- Make dataset quirks explicit, including deterministic replay IDs, sparse transaction signal, minimum interaction filters, and point-in-time item-property joins
- Define full-catalog versus sampled recommendation evaluation before model training
- Record Hadoop/HDFS and Flask as deliberate omissions rather than silent removals
- Add per-phase definition of done, evidence to save, interview angle, resume candidate, and stop-services instructions
- Add a measurement ledger so every future resume metric can be traced back to a test
- Correct the core milestone estimate to match the actual phase estimates
- Keep development Delta writes local so streaming benchmarks do not measure home-network or cloud-storage latency
- Make Phase 0 a true host/tool preflight instead of depending on Compose profiles created in Phase 1A
- Add reproducibility fields to the measurement ledger and make it the single source of truth for metrics
- Budget explicitly for Spark/Delta/ADLS dependency compatibility risk

**Primary stack:** Azure | Docker Compose | Kafka | Avro | Schema Registry | PySpark Structured Streaming | Azure Blob Storage / ADLS Gen2 | Delta Lake | Azure SQL Database | dbt | Redis | PyTorch | MLflow | FAISS | ONNX | FastAPI | Airflow | Prometheus | Grafana | GitHub Actions | Azure Container Registry | Azure Container Apps | Terraform

**Dataset:** Retailrocket Recommender System Dataset from Kaggle. The source contains roughly 2.75M clickstream events from about 1.4M visitors and 235K items collected over roughly 4.5 months. The exact event-type counts must be recomputed during Phase 2 and recorded in the measurement ledger before being used in project copy or resume bullets.

**Estimated build time:**

- **Core portfolio milestone:** about 32 to 44 working days at roughly 3 hours per day
- **Stretch and hardening work:** another 16 to 23 working days
- **Phase-estimate total:** about 48 to 67 working days
- **Planning target:** roughly 50 to 65 working days, with the upper end allowed to move if Spark/Delta/Azure compatibility or model debugging takes longer

The core estimate is the sum of Phase 0 through Phase 15, including the compatibility buffer in Phase 7. The project should be publishable at that milestone. The total is intentionally less tidy than a fixed calendar promise because these tools have real debugging variance.

**Resume rule:** do not update the resume to claim this Azure architecture, throughput, latency, recovery behavior, model performance, or cost result until the corresponding component has actually been built and measured.

## 1. Project Goal

Build one system that shows both sides of the target role profile.

### Data Engineering side

- Reliable event ingestion with Kafka
- Explicit event contracts and schema evolution
- Stateful stream processing with PySpark Structured Streaming
- Event-time processing, windows, watermarks, late-event handling, and deduplication
- Durable Bronze / Silver / Gold data layers on Delta Lake
- Replay and historical backfill support
- Point-in-time enrichment using versioned item properties
- Warehouse modeling with dbt and Azure SQL
- Data-quality checks and operational monitoring
- Failure recovery and reproducible benchmarking
- CI/CD and infrastructure as code

### ML Engineering side

- Reproducible user, item, and interaction features
- Temporal train / validation / test splits
- Full-catalog recommendation evaluation as the primary protocol
- Popularity and category baselines before a neural model
- Two-tower recommendation model in PyTorch
- Negative-sampling experiments
- MLflow experiment tracking and model registry
- FAISS approximate-nearest-neighbor retrieval
- Cold-start fallback logic
- ONNX export and FastAPI serving
- Candidate-versus-production model evaluation
- Retraining orchestration and model monitoring

The project should answer one question clearly:

> Can I take messy behavioral events from ingestion to a reliable recommendation path, and can I explain every engineering decision in between?

## 2. Infrastructure Split

The project uses a deliberate hybrid setup.

### Local MacBook via Docker Compose

Run the stateful development services locally:

- Kafka
- Schema Registry
- Spark master and worker
- Redis
- Airflow
- MLflow
- Prometheus
- Grafana
- supporting metadata database where needed
- bind-mounted local disk for Delta tables and Spark checkpoints during normal development

Reasons:

- faster development loop than repeatedly provisioning cloud infrastructure
- lower cost
- easier access to logs and local test data
- easier failure injection
- avoids forcing Kafka, Spark, Airflow, and monitoring into an undersized VM

### Azure

Keep cloud resources focused on persistent and deployable pieces:

- Azure Blob Storage or ADLS Gen2 as the cloud mirror for Delta snapshots, durable artifacts, and final cloud-integration runs
- Azure SQL Database serverless for dbt models and historical analytics
- Azure Container Registry for FastAPI images
- Azure Container Apps for the deployed API
- Azure resource group and supporting configuration

### Development storage path versus cloud storage path

Normal development must not write every Spark micro-batch directly to ADLS. That would mix WAN latency and Azure storage transaction time into the streaming benchmark.

Use two clearly labeled paths:

```text
Development / performance path
Kafka -> Spark -> local Delta volume
                    |
                    +-> scheduled snapshot/sync -> ADLS

Cloud-integration validation path
Kafka -> Spark -> ADLS Delta
```

Rules:

- local Delta is the default sink while developing and tuning the stream
- local Spark checkpoints use a persistent bind mount or Docker volume
- sync selected Delta snapshots/partitions to ADLS with AzCopy or a small Azure SDK job on a schedule, not per micro-batch
- run a separate final cloud-integration test against ADLS
- never combine the local streaming-latency number with the ADLS integration number; label them as different benchmarks
- MinIO is optional if object-store semantics are useful during local testing, but it is not required for the core build

### Why the B1s VM approach was dropped

The earlier design tried to run Kafka, Spark, HDFS, Redis, Airflow, MLflow, Prometheus, and Grafana on a small Azure VM. That would turn the project into a memory-pressure exercise rather than a data engineering project.

The v4 decision is intentional:

> Heavy stateful development infrastructure runs locally. Azure is used where cloud persistence, deployment, or infrastructure automation adds real value.

The project still demonstrates Azure without paying to keep a development cluster running.

## 3. Local Host Preflight

Before Phase 1 starts, record the actual MacBook specification in `docs/ENVIRONMENT.md`:

```text
chip:
memory:
macOS version:
Docker Desktop version:
Docker memory limit:
Docker CPU limit:
architecture: arm64 or x86_64
```

Do not assume every required image behaves correctly on Apple Silicon. Verify multi-architecture support during the first setup session.

### Rough local memory budget

These are development targets, not guarantees. Actual RSS varies by image, JVM settings, workload, and Spark batch size.

| Service | Rough target RAM |
|---|---:|
| Kafka | 0.75 to 1.5 GB |
| Schema Registry | 0.5 to 0.8 GB |
| Spark master | 0.25 to 0.5 GB |
| Spark worker | 2 to 4 GB |
| Redis | 0.1 to 0.25 GB |
| Airflow webserver / API | 0.3 to 0.6 GB |
| Airflow scheduler | 0.3 to 0.6 GB |
| Airflow metadata DB | 0.25 to 0.5 GB |
| MLflow | 0.25 to 0.5 GB |
| Prometheus | 0.5 to 1 GB |
| Grafana | 0.25 to 0.5 GB |
| Docker overhead / sidecars | 0.5 to 1.5 GB |

Running everything at once can easily use roughly **6 to 12 GB** before the application code, browser, IDE, or host OS are counted.

### Docker Compose profiles

Do not run every service all day.

Suggested profiles:

```text
streaming
  Kafka
  Schema Registry
  Spark
  Redis

mlops
  Airflow
  MLflow
  metadata DB

monitoring
  Prometheus
  Grafana

all
  everything above
```

Example workflow:

```bash
docker compose --profile streaming up -d
```

Later:

```bash
docker compose --profile mlops --profile monitoring up -d
```

Use the smallest service set needed for the current phase.

## 4. Cost and Shutdown Discipline

This project should be cheap to build, but not every Azure service is free.

### What can bill

#### Azure Blob Storage / ADLS Gen2

- storage capacity can bill
- read, write, and transaction operations can bill
- network egress can bill
- eligible new Azure accounts can have a limited 12-month free Blob allowance, but this must be checked against the actual subscription before relying on it

#### Azure SQL Database serverless

- compute is billed while the database is active unless covered by an eligible free allowance
- General Purpose serverless can auto-pause when idle
- while paused, compute billing stops but storage still bills
- Azure currently offers a monthly free serverless allowance on eligible subscriptions, but the portal setting must be checked before provisioning
- application code must tolerate resume delay and transient connection retries

#### Azure Container Registry

- the registry tier can have a recurring charge
- image storage beyond the included tier allowance can bill
- network transfer can bill
- do not assume ACR is free simply because the image volume is small

#### Azure Container Apps

- consumption usage can bill when the app is active
- with minimum replicas set to zero, Container Apps can scale to zero
- Microsoft currently provides a monthly consumption free grant, but usage above that grant bills
- deployment and scale settings should be checked after every configuration change

#### Local services

- no Azure compute bill
- local electricity and machine resources only

### Current Azure free-service notes to verify in the portal

As of September 2026, Microsoft publishes the following relevant offers for eligible subscriptions:

- Azure SQL Database: up to 100,000 serverless vCore-seconds per month and up to 32 GB per database under the current free offer
- Azure Blob Storage: a limited 12-month free allowance for eligible new Azure customers
- Azure Container Apps: monthly free consumption grant when using the consumption model

These offers can change. The Azure Portal and billing page are the source of truth for the account actually used.

### End-of-session checklist

Every work session should end with:

```text
[ ] Stop the event simulator
[ ] Stop long-running Spark queries
[ ] Stop local Docker profiles that are no longer needed
[ ] Check that Container Apps minimum replicas are still 0 where intended
[ ] Check that Azure SQL serverless auto-pause is enabled
[ ] Confirm no temporary Azure resource was left running
[ ] Commit or stash the current work
[ ] Update docs/NEXT_SESSION.md
[ ] Record any new measured result in docs/MEASUREMENTS.md
```

When the project conversation ends with wording such as "continue tomorrow," "stop here," or "we'll do the rest later," explicitly remind Pramod to run the shutdown checklist.

## 5. Terraform Strategy

Terraform moves to the start of the project.

### Phase 1B resources

Define roughly five core Azure resources up front:

- Resource Group
- Storage Account / ADLS Gen2
- Azure SQL Database and logical server
- Azure Container Registry
- Azure Container Apps environment and API app skeleton

Add supporting identity, network, or monitoring resources only when needed.

### Do not use `terraform destroy` as the automatic nightly shutdown

Persistent data should not disappear every evening.

Use two logical groups or modules:

```text
terraform/
  persistent/
    resource_group
    storage
    sql

  deploy/
    container_registry
    container_apps
    supporting_config
```

Normal work-session ending:

```text
scale compute down
leave persistent data intact
```

Full teardown when the environment is no longer needed:

```bash
terraform destroy
```

Before destructive commands, verify that no important Delta tables, SQL data, model artifacts, or benchmark evidence exists only in the resources being removed.

### Why Terraform comes first

- the Azure environment is reproducible from the beginning
- cost-bearing resources are visible in code review
- accidental portal drift is reduced
- teardown is predictable
- Terraform becomes part of the actual project rather than a late keyword addition

## 6. Why Retailrocket

Retailrocket is a better fit than a one-purchase-heavy order dataset because it contains real anonymized behavioral events:

- item views
- add-to-cart events
- transactions
- visitor IDs
- item IDs
- time-varying item properties
- category hierarchy

The original timestamps are preserved so event-time logic can be tested properly.

## 7. Dataset Reality and Modeling Constraints

This section exists so the ML part does not accidentally become optimistic or leaky.

### 7.1 Recompute the exact source profile first

During Phase 2, record:

- total rows
- unique visitors
- unique items
- view count
- add-to-cart count
- transaction count
- source timestamp range
- duplicate source rows
- missing transaction IDs
- item-property update frequency
- visitors with 1, 2, 3, 5, 10, and 20+ interactions

The transaction count is expected to be roughly **22K out of about 2.75M total events**, but that number must be confirmed from the local dataset before it is treated as final.

### 7.2 Deterministic `event_id`

Replay deduplication only works if the same source row produces the same ID every time.

Generate:

```text
SHA256(
  source_file
  + source_row_number
  + timestamp
  + visitor_id
  + item_id
  + event_type
  + transaction_id
)
```

Why include source file and row number:

- two legitimate rows can otherwise have identical business fields
- deterministic replay must not collapse distinct source records
- re-running the simulator should produce the same event IDs

### 7.3 Interaction signal

Purchases are sparse, so do not train a recommender using transaction events alone without first checking whether enough users remain.

Candidate positive-signal design:

```text
view        -> weak positive
add_to_cart -> stronger positive
transaction -> strongest positive
```

Possible initial weights:

```text
view = 1
cart = 3
purchase = 5
```

These are experiment parameters, not truths. Compare multiple weighting strategies.

### 7.4 Minimum interaction filter

Recommendation evaluation becomes meaningless for visitors with almost no history.

During profiling, choose and document a minimum-history rule for model evaluation, for example:

```text
at least 5 historical interactions before the test event
```

The final threshold must be based on the dataset distribution, not chosen only to make the metric look better.

Report how many visitors are excluded by the rule.

### 7.5 Point-in-time item-property joins

`item_properties_part1.csv` and `item_properties_part2.csv` contain time-varying metadata.

For an event at time `t`, use the latest item property known at or before `t`:

```text
event(item_id=42, event_time=t)

join to

latest item property
where property_timestamp <= t
```

Do not enrich historical events with the final property value from the end of the dataset. That would leak future state.

The same point-in-time rule applies to:

- category
- price-like properties
- any derived item metadata that changes over time

### 7.6 Interpreting Recall@K depends on protocol

The catalog is large, interaction histories are sparse, and most visitors interact with few items.

Do not pre-label a low result as acceptable. Instead:

> Interpret Recall@K against the exact candidate-set protocol and simple baselines. Full-catalog retrieval will naturally be harder than sampled evaluation, so results should never be compared without stating the evaluation protocol.

## 8. Source Files

| File | Contents | Use |
|---|---|---|
| `events.csv` | timestamp, visitorid, event, itemid, transactionid | Kafka replay source |
| `item_properties_part1.csv` | item properties over time | point-in-time item enrichment |
| `item_properties_part2.csv` | item properties over time | point-in-time item enrichment |
| `category_tree.csv` | categoryid, parentid | category hierarchy |

## 9. Final Architecture

```text
[Retailrocket Dataset]
        |
[Python Event Simulator]
        |
        v
[Kafka Producers - local]
        |
        +-------------------+-------------------+
        |                   |                   |
   item_view            add_to_cart        transaction
        |                   |                   |
        +------------- Kafka Topics ------------+
                         |
               [Avro + Schema Registry]
                         |
               +---------+---------+
               |                   |
             valid               invalid
               |                   |
               v                   v
 [PySpark Structured Streaming]  [Kafka DLQ]
               |
      event-time processing
      watermarks
      deterministic dedup
      checkpointing
      windowed aggregations
               |
        +------+-----------------------------+
        |                                    |
        v                                    v
 [Redis - local]                 [Local Delta volume]
 online features                  BRONZE - immutable raw
 hot popularity                           |
 recent user state                        v
                                  SILVER - validated,
                                  typed, deduplicated,
                                  point-in-time enriched
                                          |
                                          v
                                  GOLD - analytics marts,
                                  user/item features,
                                  training tables
                                          |
                           scheduled snapshot/sync
                                          |
                                          v
                                  [Azure Blob / ADLS]
                                  cloud mirror + artifacts
                                          |
                          +---------------+----------------+
                          |                                |
                          v                                v
                  [Azure SQL + dbt]                 [PySpark Batch]
                  historical marts                 feature generation
                          |                                |
                          |                                v
                          |                        temporal ML split
                          |                                |
                          |                  +-------------+-------------+
                          |                  |                           |
                          |                  v                           v
                          |          popularity/category            two-tower
                          |              baselines                   PyTorch
                          |                                              |
                          |                                           MLflow
                          |                                              |
                          |                                     item embeddings
                          |                                              |
                          |                                            FAISS
                          |                                              |
                          |                                             ONNX
                          |                                              |
                          +----------------------> [FastAPI] <-----------+
                                                   |
                                                   v
                                      [Azure Container Apps]
```

Supporting layers:

```text
Airflow local          -> dbt jobs, feature snapshots, training, evaluation,
                          model promotion, FAISS rebuild, backfills
GitHub Actions         -> lint, tests, build, scan, deploy, health check
Prometheus + Grafana   -> Kafka, Spark, API, ML, and data-quality monitoring
Terraform              -> Azure infrastructure provisioning
```

## 10. Deliberate Scope Decisions

### Hadoop / HDFS is intentionally omitted

Azure object storage plus Delta Lake is the more natural durable storage layer for this architecture.

Hadoop is not being removed because it is unimportant. It is already represented elsewhere in Pramod's background and project history. Adding single-node HDFS here would duplicate a skill while making the Azure architecture less coherent.

### Flask is intentionally omitted

FastAPI already owns the serving layer. Adding Flask would create a second backend framework without a separate responsibility.

Flask experience is represented in other portfolio data-platform work, so this project should spend that complexity budget on Kafka, Spark, feature generation, and recommendation serving.

### Kubernetes is intentionally omitted

Azure Container Apps gives a deployable container target without turning this project into another Kubernetes exercise.

### No LLM, RAG, or agent layer

The recommender already provides enough ML depth. LLM functionality would blur the project story and overlap with other portfolio projects.

## 11. Kafka Design

### Topics

Primary topics:

```text
item_view
add_to_cart
transaction
```

Operational topic:

```text
clickstream_dlq
```

### Partitioning strategy

Partition behavioral topics by `visitor_id`.

Reasons:

- preserves order for a visitor within a Kafka partition
- makes user/session feature processing easier to reason about
- distributes traffic across many visitor keys

Do not confuse Kafka partitioning with Delta storage partitioning.

### Producer behavior

The simulator should support:

- configurable events per second
- burst traffic
- event timestamp preservation
- ingestion timestamp generation
- batching
- retries
- compression
- deterministic replay
- duplicate injection
- malformed-event injection
- delayed-event injection
- fixed random seed for repeatable tests

### Consumer concepts to demonstrate

- consumer groups
- offsets
- partition assignment
- offset reset / replay
- retention
- consumer lag
- restart behavior

## 12. Avro and Schema Registry

Use explicit event schemas rather than free-form JSON.

Core logical event structure:

```text
event_id
visitor_id
item_id
event_type
event_timestamp
ingestion_timestamp
transaction_id
schema_version
source_file
source_row_number
```

Possible later fields:

```text
session_id
device_type
```

### Schema evolution exercise

Create at least two versions and test compatibility.

Example:

```text
v1: visitor_id, item_id, event_type, event_timestamp
v2: v1 fields + optional session_id + optional device_type
```

Document:

- backward-compatible changes
- forward-compatible changes
- incompatible changes
- how consumers behave during the transition

## 13. Dead Letter Queue

Invalid records should not stop the stream.

Malformed or incompatible events go to:

```text
clickstream_dlq
```

Store:

```text
original_payload
error_type
error_message
source_topic
partition
offset
ingestion_timestamp
schema_version
```

Track DLQ rate as a data-quality metric.

## 14. PySpark Structured Streaming

This is one of the main technical sections of the project.

### Required concepts

Implement:

- event-time processing
- processing-time versus event-time comparison
- watermarking
- stateful windowed aggregations
- deterministic deduplication by `event_id`
- checkpointing
- recovery from checkpoint
- late-event handling
- out-of-order-event handling
- micro-batch metrics
- graceful restart behavior

### Windowed metrics

At minimum:

- item views per 5-minute window
- unique visitors per 5-minute window
- add-to-cart count
- transaction count
- view-to-cart rate
- cart-to-purchase rate
- view-to-purchase rate
- category popularity
- trending items
- active visitors

### Late events

Start with a development watermark such as 10 minutes, then justify or revise it from the delay distribution used by the simulator.

Test:

- event within watermark
- event outside watermark
- out-of-order event within a visitor stream

### Checkpointing

During development, persist checkpoints to a bind-mounted local path or named Docker volume so restart behavior is durable without adding WAN latency. Test an ADLS-backed checkpoint path separately during cloud-integration validation if the final architecture needs it.

Do not use a cloud-backed checkpoint during the primary local latency benchmark.

Test:

1. process a known event range
2. stop Spark mid-stream
3. restart from checkpoint
4. verify the unprocessed range is consumed
5. verify no silent data loss
6. verify no duplicate aggregate inflation

## 15. Bronze / Silver / Gold Lakehouse

Use Delta Lake on a bind-mounted local disk during normal development. Sync selected snapshots or partitions to Azure Blob Storage / ADLS Gen2 on a schedule and run a separate final cloud-integration validation against ADLS.

This separation keeps the streaming benchmark about Kafka/Spark/Delta processing rather than the quality of the development internet connection.

### Bronze

Immutable raw replay history.

Suggested columns:

```text
event_id
visitor_id
item_id
event_type
event_timestamp
ingestion_timestamp
transaction_id
schema_version
source_file
source_row_number
source_topic
partition
offset
```

### Silver

Clean and trustworthy events.

Actions:

- type normalization
- timestamp normalization
- deduplication
- point-in-time item-property join
- category enrichment
- invalid-record removal
- session enrichment when implemented

### Gold

Business and ML-ready datasets.

Candidate tables:

```text
fact_events
fact_transactions
dim_items_scd
dim_categories
mart_item_funnel
mart_category_performance
mart_daily_item_metrics
user_features
item_features
user_item_interactions
recommendation_training_data
```

If `dim_items_scd` is implemented, document the slowly changing dimension behavior explicitly.

## 16. Delta Lake Features to Demonstrate

Use Delta for real reasons:

- ACID writes
- schema enforcement
- schema evolution
- MERGE / upsert where appropriate
- time travel for debugging
- partition pruning
- batch and streaming reads from the same durable layer

Test one correction or reprocessing scenario and show how Silver or Gold is updated safely.

## 17. Storage Partitioning

Use a time-based strategy for large event tables, for example:

```text
year / month / day
```

Benchmark at least one query against:

- unpartitioned data
- time-partitioned data

Explain why partitioning by `visitor_id` would create a high-cardinality storage layout.

## 18. Replay and Backfill

Support both paths:

```text
REAL TIME
Kafka -> Spark Structured Streaming -> Delta
```

and

```text
BACKFILL
Bronze Delta -> Spark batch -> Silver / Gold
```

Use shared transformation logic where practical.

Demonstrate one real backfill:

- intentionally change a transformation rule
- reprocess a bounded historical range
- verify the downstream Gold result
- record before/after row counts and validation results

## 19. Azure SQL + dbt

Suggested dbt layout:

```text
models/
  staging/
    stg_events.sql
    stg_items.sql
    stg_categories.sql

  intermediate/
    int_sessions.sql
    int_user_item_interactions.sql
    int_item_daily_metrics.sql

  marts/
    fct_events.sql
    fct_transactions.sql
    dim_items.sql
    dim_categories.sql
    mart_item_funnel.sql
    mart_category_performance.sql
```

### dbt tests

Standard tests:

- `unique`
- `not_null`
- `relationships`
- `accepted_values`

Custom tests:

- transaction item exists in item dimension
- event type is valid
- timestamps are parseable
- counts are non-negative
- item-property effective timestamp is not after the event timestamp used for the join

Do not enforce business inequalities that the source semantics do not guarantee.

## 20. Data Quality Monitoring

Track data health separately from infrastructure health.

At minimum:

- incoming events per minute
- null rate by field
- invalid schema rate
- duplicate-event rate
- unknown item rate
- unknown category rate
- DLQ count
- late-event count
- event-type distribution
- point-in-time join miss rate
- unexpected volume drops

## 21. Sessionization

Add sessionization only after the core stream is stable.

Possible assumption:

```text
new session after 30 minutes of inactivity
```

Document it as a project rule, not a universal truth.

Candidate session features:

- duration
- event count
- unique items
- category diversity
- carts per session
- transactions per session

## 22. ML Feature Pipeline

### User features

Candidates:

```text
views_last_1h
views_last_24h
views_last_7d
carts_last_7d
purchases_last_30d
days_since_last_interaction
days_since_last_purchase
session_frequency
preferred_category
category_affinity_vector
recent_item_ids
```

### Item features

Candidates:

```text
category_id
category_embedding
price_bucket
views_24h
views_7d
cart_rate
purchase_rate
historical_popularity
recent_popularity
```

### Interaction table

```text
visitor_id
item_id
interaction_type
interaction_timestamp
interaction_weight
```

### Feature leakage rule

Every feature used for a training example at time `t` must be computable from data at or before `t`.

## 23. Offline and Online Features

### Offline

Use Delta / SQL for:

- training
- evaluation
- historical backfills
- reproducibility

### Online

Use Redis for:

- current trending items
- recent user interactions
- latest user feature vector
- category popularity

Do not add a feature-store platform unless a concrete problem appears that Redis plus Delta cannot handle for this project.

## 24. Train / Validation / Test Design

Do not randomly split interactions.

Use chronological splits:

```text
earliest period -> training
next period     -> validation
latest period   -> test
```

The exact boundaries are chosen after Phase 2 profiling.

Document:

- split timestamps
- users per split
- items per split
- interactions per split
- cold-start users/items
- minimum-history filter
- whether repeated interactions are retained or collapsed

## 25. Recommendation Evaluation Protocol

This section must be fixed before comparing models.

### Primary protocol: full-catalog retrieval

For each eligible test user:

```text
user representation
       |
       v
score/search against the full eligible item catalog
       |
       v
Top-K recommendations
       |
       v
Recall@K / NDCG@K
```

Rules to define:

- what makes an item eligible
- whether previously interacted items are excluded
- whether test positives are transaction-only or weighted interactions
- how multiple held-out positives are handled
- how cold-start users are reported

This is the primary number used in the final benchmark.

### Secondary protocol: sampled candidate evaluation

Use only if useful for debugging or comparison with literature.

If used, record:

```text
candidate-set size
number of positives
negative-sampling method
random seed
whether seen items are excluded
```

Never compare a full-catalog Recall@10 with a sampled-candidate Recall@10 as though they are the same metric.

### Interpretation rule

Full-catalog retrieval is much harder than sampled evaluation. Judge the two-tower model against simple baselines under the same protocol.

## 26. Recommendation Baselines

### Baseline A: global popularity

Recommend the most interacted-with eligible items.

### Baseline B: category popularity

Recommend popular items within categories the visitor has shown interest in.

### Optional Baseline C: matrix factorization

Add only if the first two baselines and two-tower model are already complete.

The two-tower model must be evaluated against at least Baselines A and B.

## 27. Two-Tower Recommendation Model

### User tower

Candidate inputs:

- visitor embedding
- recency
- session frequency
- category affinity
- recent interaction summary

### Item tower

Candidate inputs:

- item embedding
- category embedding
- price bucket
- historical popularity
- recent popularity
- conversion statistics

### Training objective

Start with in-batch negatives or sampled-softmax style training.

### Negative-sampling experiments

Compare at least:

- random negatives
- popularity-weighted or category-aware negatives

Document whether harder negatives improve retrieval quality.

## 28. Model Evaluation Metrics

Primary:

- Recall@10
- Recall@20
- NDCG@10

Optional:

- Hit Rate@10
- catalog coverage
- recommendation diversity

Example final table:

```text
Model / Baseline        Protocol       Recall@10    Recall@20    NDCG@10
----------------------------------------------------------------------------
Global Popularity       full catalog   measured values only
Category Popularity     full catalog   measured values only
Two-Tower               full catalog   measured values only
```

Do not pre-fill results.

## 29. MLflow Tracking

Track:

```text
embedding_dimension
learning_rate
batch_size
optimizer
epochs
interaction_weight_strategy
negative_sampling_strategy
training_window
minimum_history_filter
evaluation_protocol
candidate_set_size if sampled
model_version
Recall@10
Recall@20
NDCG@10
training_time
```

Use clear aliases such as:

```text
candidate
production
```

## 30. Model Promotion Rules

A retrained model should not deploy automatically just because training finished.

Flow:

```text
train candidate
      |
      v
evaluate candidate
      |
      v
compare against current production model
      |
      +--> fail gate -> reject
      |
      +--> pass gate -> register/export/deploy
```

Candidate gates can include:

- Recall@10 not materially worse than production
- NDCG@10 meets the agreed threshold
- data-quality checks passed
- ONNX export passed
- inference smoke test passed
- retrieval latency stays within target

Targets should be defined before the final benchmark, but resume metrics must use measured results only.

## 31. FAISS Retrieval

Use the two-tower model as a retrieval system.

```text
item tower
   |
   v
item embeddings
   |
   v
FAISS index

visitor features
   |
   v
user tower
   |
   v
user embedding
   |
   v
FAISS Top-K search
```

Measure:

- index build time
- index size
- Top-K retrieval latency
- end-to-end recommendation latency

If approximate search is used instead of exact search, compare recall/latency against an exact baseline on a manageable subset.

## 32. Cold Start

### New visitor

```text
known user
  -> two-tower retrieval

new user with category interactions
  -> category popularity

brand-new visitor
  -> global popularity / trending
```

### New item

Use metadata-derived features such as category and price bucket so an item can receive an embedding before accumulating much interaction history.

Document what metadata is required before a new item becomes eligible.

## 33. ONNX Export

Export the trained model or serving tower to ONNX.

Validate numerical consistency against PyTorch within a documented tolerance.

Measure:

- PyTorch inference latency
- ONNX Runtime inference latency
- artifact size

Only claim ONNX as an optimization if the benchmark supports it.

## 34. FastAPI Serving Layer

Suggested endpoints:

| Endpoint | Source | Purpose |
|---|---|---|
| `GET /recommendations/{visitor_id}` | ONNX + FAISS + Redis | Top-N recommendations |
| `GET /metrics/live` | Redis | current popularity / funnel metrics |
| `GET /metrics/history` | Azure SQL | historical analytics |
| `GET /health` | dependency checks | service health |
| `GET /model` | MLflow metadata | active model version |

Recommendation responses should contain enough metadata to debug the path without exposing unnecessary internal details.

## 35. Airflow Responsibilities

Airflow handles batch and lifecycle work, not the long-lived streaming consumer.

DAGs can orchestrate:

- daily dbt jobs
- Silver-to-Gold batch transformations
- feature snapshots
- training dataset generation
- model retraining
- model evaluation
- model promotion
- ONNX export
- FAISS index rebuild
- bounded backfills
- data-quality reports

## 36. Failure Testing

Required before calling the stretch phase complete.

### Test A: Spark process dies

Expected:

- Kafka retains unconsumed messages
- Spark restarts from checkpoint
- processing resumes
- no silent loss
- no duplicate inflation

### Test B: duplicate event

Expected:

- deterministic `event_id` catches replay duplicate
- downstream aggregates do not double count

### Test C: malformed schema

Expected:

- event goes to DLQ
- valid traffic continues

### Test D: late event

Expected:

- within watermark -> processed
- outside watermark -> handled according to documented policy

### Test E: Redis unavailable

Expected:

- durable Delta writes continue
- online-serving path fails gracefully or falls back
- cache can be rebuilt

### Test F: model candidate underperforms

Expected:

- candidate is rejected
- production model remains active

### Test G: Azure SQL is auto-paused

Expected:

- API/database client retries appropriately during resume
- application recovers without manual intervention

## 37. Performance and Recovery Benchmarks

### Streaming

- producer events/sec
- processed events/sec
- Kafka consumer lag
- Spark micro-batch duration
- p50 end-to-end event latency
- p95 end-to-end event latency
- p99 end-to-end event latency

### Recovery

- Spark restart time
- catch-up time
- messages replayed
- events lost
- duplicates introduced

### ML

- training time
- Recall@10
- Recall@20
- NDCG@10
- FAISS retrieval latency
- ONNX inference latency

### API

- requests/sec
- p50 latency
- p95 latency
- p99 latency
- error rate

### Cost

- Azure storage cost during benchmark period
- Azure SQL cost / free allowance usage
- ACR cost
- Container Apps usage
- total cloud cost for the benchmark session

## 38. Load Testing

Use k6 or Locust.

Example concurrency steps:

```text
10 users
50 users
100 users
higher only if the machine and deployed service remain stable
```

Record hardware/container settings with every benchmark.

## 39. Monitoring

### Kafka

- messages/sec
- consumer lag
- partition lag
- DLQ rate

### Spark

- input rows/sec
- processed rows/sec
- micro-batch duration
- active queries
- failed/restarted queries

### Data quality

- invalid events
- duplicate events
- late events
- null rates
- unknown item/category rate
- point-in-time join miss rate
- volume anomalies

### API

- requests/sec
- p50 / p95 / p99 latency
- error rate
- dependency failures

### ML

- production model version
- latest evaluation metrics
- last successful retrain
- recommendation latency
- cold-start fallback rate
- candidate promotion / rejection history

## 40. CI/CD

Suggested GitHub Actions flow:

```text
pull request / push
       |
       v
lint + formatting
       |
       v
unit tests
       |
       v
schema compatibility tests
       |
       v
Spark transformation tests
       |
       v
dbt tests
       |
       v
model smoke test
       |
       v
FastAPI tests
       |
       v
Docker build
       |
       v
container vulnerability scan
       |
       v
push to ACR
       |
       v
deploy to Container Apps
       |
       v
post-deploy health check
```

Model retraining does not happen on every code push.

## 41. Measurement Ledger

Create:

```text
docs/MEASUREMENTS.md
```

Every measurable claim should include:

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

Example:

```text
metric_id: STREAM-THROUGHPUT-001
metric: sustained Spark processing throughput
value: TBD
date: TBD
git_commit: <40-character commit SHA>
environment: MacBook ..., Docker Desktop ...
dataset slice: TBD
command / test: scripts/benchmark_streaming.sh
config: 3 Kafka partitions, Spark worker memory ...
evidence: benchmarks/raw/streaming_YYYYMMDD.json
notes: no resume claim until repeated successfully
```

`docs/MEASUREMENTS.md` is the authoritative registry for numeric claims. Raw benchmark output lives under `benchmarks/raw/`. `docs/benchmark_results.md` is a human-readable report generated from or manually reconciled against metric IDs in the ledger; it must not become a second independent source of numbers.

Any resume or portfolio claim that contains a measured number should map internally to one or more metric IDs.

## 42. Session Handoff File

Create:

```text
docs/NEXT_SESSION.md
```

At the end of each work session, record:

- phase
- exact task completed
- current branch / commit
- services currently stopped or still running
- next command to run
- unresolved error
- next test
- any Azure resource intentionally left alive

This keeps separate chats from needing to reread the full roadmap.

## 43. Suggested Repository Structure

```text
ecommerce-recommendation-engine/
|
|-- README.md
|-- docker-compose.yml
|-- docker-compose.override.yml
|-- .env.example
|-- Makefile
|
|-- producer/
|   |-- simulator.py
|   |-- schemas/
|
|-- kafka/
|   |-- config/
|   |-- topics/
|
|-- streaming/
|   |-- jobs/
|   |-- transforms/
|   |-- tests/
|
|-- batch/
|   |-- backfill/
|   |-- feature_jobs/
|
|-- dbt/
|   |-- models/
|   |-- tests/
|
|-- ml/
|   |-- features/
|   |-- baselines/
|   |-- two_tower/
|   |-- evaluation/
|   |-- retrieval/
|   |-- export/
|
|-- api/
|   |-- app/
|   |-- tests/
|
|-- airflow/
|   |-- dags/
|
|-- monitoring/
|   |-- prometheus/
|   |-- grafana/
|
|-- terraform/
|   |-- persistent/
|   |-- deploy/
|   |-- modules/
|
|-- scripts/
|   |-- bootstrap.sh
|   |-- benchmark_streaming.sh
|   |-- load_test.sh
|   |-- failure_tests.sh
|   |-- shutdown.sh
|
|-- benchmarks/
|
|-- docs/
    |-- architecture.md
    |-- data_contracts.md
    |-- dataset_profile.md
    |-- model_evaluation.md
    |-- failure_testing.md
    |-- benchmark_results.md
    |-- MEASUREMENTS.md
    |-- NEXT_SESSION.md
    |-- ENVIRONMENT.md
```

## 44. Core Versus Stretch Scope

### Core portfolio milestone

The project is portfolio-ready when this path works end to end:

```text
Retailrocket
   -> deterministic simulator
   -> Kafka + schemas
   -> Spark Structured Streaming
   -> dedup + watermark + checkpoint
   -> Bronze / Silver / Gold Delta
   -> point-in-time enrichment
   -> user/item interaction features
   -> temporal split
   -> global/category baseline
   -> basic two-tower model
   -> full-catalog evaluation
   -> basic FAISS retrieval
   -> FastAPI recommendation endpoint
   -> Azure Container Apps deployment
```

Core also requires:

- a clean README
- architecture diagram
- dataset profile
- at least one measured benchmark
- no unsupported resume claims

Target: **about Day 20 to Day 25**.

### Stretch and hardening

After the core milestone:

- richer negative sampling
- sessionization
- full MLflow registry workflow
- model promotion gate
- ONNX benchmark
- Airflow lifecycle DAGs
- advanced Grafana dashboards
- failure injection suite
- backfill hardening
- API load testing
- detailed cost benchmark
- CI/CD security scanning
- Terraform refinement and teardown verification
- optional matrix factorization baseline

The project can be shown before every stretch item is done.

## 45. Phase Plan With Resume Hooks

### Phase 0 - Host and tool preflight

**Goal:** verify the host tools and container runtime before the project Compose file exists.

**Work:**

- record Mac chip and RAM
- set Docker CPU/RAM limits
- install Docker, Python, Java if required, Terraform, Azure CLI, GitHub CLI
- verify each CLI reports a usable version
- run one architecture-safe container smoke test such as `hello-world` or a small JDK/Python image
- create `docs/ENVIRONMENT.md`

**Definition of done:**

- Docker can pull and run one container for the host architecture
- Python, Java, Terraform, Azure CLI, and GitHub CLI versions are recorded
- Docker CPU/RAM limits are recorded
- `docs/ENVIRONMENT.md` exists

**Evidence to save:**

- environment file
- CLI version output
- container smoke-test output

**Interview angle:**

- why host constraints and architecture compatibility were checked before choosing images

**Resume candidate:**

- none yet

**Stop services:**

- remove the smoke-test container if it is still present
- no project Compose stack exists yet

**Estimate:** 1 to 2 days

### Phase 1A - Repository and local stack

**Goal:** create the project skeleton and reproducible local development setup.

**Work:**

- repository structure
- Docker Compose profiles
- `.env.example`
- local Kafka, Schema Registry, Spark, Redis
- helper scripts / Makefile

**Definition of done:**

- a fresh clone can boot the streaming profile from documented commands
- Spark executes a simple job
- Kafka producer/consumer smoke test passes
- `docker stats` confirms the profile fits within the recorded Docker memory budget

**Evidence to save:**

- compose config
- startup logs

**Interview angle:**

- why local stateful services and cloud persistence are separated

**Resume candidate:**

- none yet

**Stop services:**

```bash
docker compose down
```

**Estimate:** 1 to 2 days

### Phase 1B - Terraform and Azure foundation

**Goal:** provision the minimal cloud foundation from code.

**Work:**

- resource group
- storage / ADLS
- Azure SQL serverless
- ACR
- Container Apps environment
- cost tags
- outputs and remote-state decision

**Definition of done:**

- `terraform plan` is clean
- `terraform apply` creates the environment
- storage and SQL connectivity are tested
- Container Apps skeleton exists with min replicas configured as intended

**Evidence to save:**

- Terraform plan
- Azure resource list
- cost configuration screenshot

**Interview angle:**

- why infrastructure as code was used from the start

**Resume candidate:**

- "Provisioned Azure storage, SQL, registry, and container deployment infrastructure with Terraform" only after the resources are actually created

**Stop services:**

- do not destroy persistent resources nightly
- verify Container Apps scale settings
- verify SQL auto-pause

**Estimate:** 2 to 3 days

### Phase 2 - Dataset profiling and deterministic identity

**Goal:** understand the data before designing the stream or recommender.

**Work:**

- exact event counts
- visitor/item cardinality
- event timestamp distribution
- transaction count
- interaction histogram per visitor
- item-property update distribution
- deterministic event ID
- initial minimum-history analysis

**Definition of done:**

- `docs/dataset_profile.md` contains measured counts
- deterministic replay produces identical IDs across two runs

**Evidence to save:**

- profile tables
- event-id reproducibility test

**Interview angle:**

- why sparse transactions change the recommendation target design
- why deterministic IDs are necessary for replay

**Resume candidate:**

- dataset scale numbers only after confirmed here

**Stop services:**

- local services are optional for this phase

**Estimate:** 2 days

### Phase 3 - Kafka producer and topic design

**Goal:** replay real source events as controlled traffic.

**Work:**

- three behavioral topics
- partition by visitor ID
- configurable replay rate
- burst mode
- fixed seed
- duplicates, malformed messages, and delayed events

**Definition of done:**

- simulator can replay a bounded source range twice deterministically
- partitioning is verified
- producer rate is configurable

**Evidence to save:**

- topic configuration
- producer benchmark output

**Interview angle:**

- why visitor ID is the partition key

**Resume candidate:**

- wait for throughput benchmark before using a rate

**Stop services:**

```bash
# stop simulator first
docker compose --profile streaming down
```

**Estimate:** 2 days

### Phase 4 - Avro, Schema Registry, and DLQ

**Goal:** make the event contract explicit and failure-safe.

**Work:**

- Avro schemas
- two schema versions
- compatibility checks
- DLQ routing
- DLQ metrics

**Definition of done:**

- compatible v2 event is accepted
- incompatible or malformed record is rejected or routed correctly
- valid stream continues while bad events exist

**Evidence to save:**

- schemas
- compatibility-test output
- DLQ sample

**Interview angle:**

- schema evolution and why bad data should not halt the stream

**Resume candidate:**

- schema validation and DLQ can be mentioned after tests pass

**Stop services:**

```bash
docker compose --profile streaming down
```

**Estimate:** 2 days

### Phase 5 - Core Spark streaming

**Goal:** build the first stateful processing path.

**Work:**

- Kafka read
- event-time parsing
- windowed aggregates
- Redis live metrics
- raw Delta output

**Definition of done:**

- stream processes all three topics
- basic window metrics are correct on a known fixture

**Evidence to save:**

- query progress logs
- sample aggregates

**Interview angle:**

- event time versus processing time

**Resume candidate:**

- no latency claim yet

**Stop services:**

- stop Spark query cleanly
- stop streaming profile

**Estimate:** 2 to 3 days

### Phase 6 - Spark reliability

**Goal:** make replay, late data, and restart behavior defensible.

**Work:**

- watermarking
- dedup by deterministic event ID
- checkpointing
- late-event policy
- restart test

**Definition of done:**

- injected duplicate does not alter final count
- within-watermark event is handled correctly
- Spark restart resumes from checkpoint
- no silent loss in controlled test

**Evidence to save:**

- before/after counts
- checkpoint recovery log

**Interview angle:**

- Kafka offsets versus Spark checkpoints
- watermark tradeoffs

**Resume candidate:**

- reliability language can be drafted, but no recovery number until benchmarked

**Stop services:**

- stop Spark query
- stop producer
- stop streaming profile

**Estimate:** 2 to 3 days

### Phase 7 - Bronze, Silver, Gold and point-in-time enrichment

**Goal:** create a replayable lakehouse path without future leakage.

**Work:**

- Bronze raw Delta on the local development volume
- Silver validated / deduplicated events
- point-in-time item-property joins
- Gold analytics and training tables
- partition strategy
- scheduled sync of selected Delta snapshots/partitions to ADLS
- one explicitly labeled ADLS cloud-integration smoke test

**Compatibility risk to budget:**

Open-source Spark writing Delta through the ABFS connector may require compatible `hadoop-azure`, Azure storage, Hadoop, Spark, and Delta JAR versions. Apple Silicon can add image-architecture friction. Do not discover this dependency chain during the final benchmark. Pin versions, record them in `docs/ENVIRONMENT.md`, and reserve up to one extra day in this phase for connector/JAR debugging. The normal local benchmark remains on local Delta even if the ADLS integration path takes longer.

**Definition of done:**

- historical event never receives a property value whose timestamp is in the future
- Bronze can regenerate Silver for a bounded range
- one scheduled local-to-ADLS sync completes and can be verified
- cloud-integration timing is recorded separately from local streaming timing

**Evidence to save:**

- Delta table schemas
- point-in-time join test
- partition comparison

**Interview angle:**

- data leakage can happen during enrichment, not just model splitting

**Resume candidate:**

- Bronze/Silver/Gold and point-in-time feature language becomes usable

**Stop services:**

- stop Spark
- local Kafka can be stopped if no replay is running

**Estimate:** 3 to 4 days, including up to 1 day of ABFS/JAR compatibility debugging

### Phase 8 - Backfill and replay

**Goal:** prove historical reprocessing works.

**Work:**

- shared transformations
- bounded backfill command
- one intentional transformation change

**Definition of done:**

- bounded historical range is reprocessed from Bronze
- downstream row counts and expected values change correctly

**Evidence to save:**

- before/after comparison
- backfill command

**Interview angle:**

- why immutable raw data matters

**Resume candidate:**

- replayable backfill claim can be used

**Stop services:**

- stop batch job

**Estimate:** 1 to 2 days

### Phase 9 - Azure SQL and dbt

**Goal:** add relational analytics and tested warehouse models.

**Work:**

- staging, intermediate, marts
- dbt tests
- historical funnel queries

**Definition of done:**

- `dbt build` passes
- key mart results match known source checks

**Evidence to save:**

- dbt lineage graph
- test output

**Interview angle:**

- why Delta and Azure SQL both exist

**Resume candidate:**

- dbt + Azure SQL can now appear in project stack

**Stop services:**

- close open DB sessions so serverless can auto-pause
- verify no local dbt process is holding a connection

**Estimate:** 2 days

### Phase 10 - Data quality monitoring

**Goal:** make bad data visible.

**Work:**

- DQ metrics
- point-in-time join misses
- volume anomalies
- DLQ rate

**Definition of done:**

- deliberate bad-data fixture changes the expected quality metric

**Evidence to save:**

- data-quality report

**Interview angle:**

- infrastructure can be healthy while the data is wrong

**Resume candidate:**

- automated data-quality checks can be claimed

**Stop services:**

- stop monitoring profile if not needed

**Estimate:** 1 to 2 days

### Phase 11 - Feature pipeline

**Goal:** create reproducible offline and online ML features.

**Work:**

- user features
- item features
- interaction table
- Redis online state
- leakage checks

**Definition of done:**

- feature for an example at time `t` can be reproduced from only pre-`t` data

**Evidence to save:**

- feature schema
- leakage unit tests

**Interview angle:**

- offline versus online feature responsibilities

**Resume candidate:**

- feature pipeline language becomes usable

**Stop services:**

- stop Spark and Redis when finished

**Estimate:** 2 to 3 days

### Phase 12 - Temporal split and baselines

**Goal:** establish an honest recommendation benchmark before neural training.

**Work:**

- chronological split
- minimum-history rule
- full-catalog evaluation code
- global popularity
- category popularity

**Definition of done:**

- baseline Recall@10, Recall@20, and NDCG@10 are measured under the full-catalog protocol
- eligible user count is recorded

**Evidence to save:**

- split report
- baseline metrics

**Interview angle:**

- why random splits inflate recommender results
- why full-catalog metrics are harder

**Resume candidate:**

- no two-tower claim yet

**Stop services:**

- local infrastructure can be mostly down

**Estimate:** 2 days

### Phase 13 - Two-tower model

**Goal:** train the main ML model and compare it fairly with simple baselines.

**Work:**

- user tower
- item tower
- weighted interactions
- negative sampling
- two or more configurations

**Definition of done:**

- model runs are reproducible
- two-tower result is compared against both baselines under the same full-catalog protocol

**Evidence to save:**

- training configs
- metrics table

**Interview angle:**

- whether the neural model earned its complexity

**Resume candidate:**

- two-tower model can be mentioned once the comparison exists

**Stop services:**

- stop training job
- stop MLflow if it was started locally

**Estimate:** 3 to 4 days

### Phase 14 - FAISS and basic serving

**Goal:** turn model embeddings into a usable retrieval path.

**Work:**

- item embedding index
- user embedding query
- Top-K retrieval
- cold-start fallback
- FastAPI recommendation endpoint

**Definition of done:**

- known visitor returns Top-K recommendations
- unknown visitor follows the documented fallback
- API smoke test passes

**Evidence to save:**

- retrieval benchmark
- API response examples

**Interview angle:**

- why two-tower retrieval avoids scoring every item individually

**Resume candidate:**

- FAISS + FastAPI can now be used

**Stop services:**

- stop FastAPI locally
- stop Redis if unused

**Estimate:** 2 to 3 days

### Phase 15 - Core Azure deployment

**Goal:** reach the first shippable portfolio milestone.

**Work:**

- containerize FastAPI
- push image to ACR
- deploy to Container Apps
- configure min replicas 0
- health check
- document live architecture

**Definition of done:**

- deployed endpoint returns a valid health response
- recommendation request works with the intended deployed dependencies
- scale-to-zero configuration is confirmed

**Evidence to save:**

- deployment output
- live health check
- architecture diagram

**Interview angle:**

- what stayed local and what moved to Azure, and why

**Resume candidate:**

- Azure deployment can be claimed

**Stop services:**

- verify Container Apps min replicas
- close SQL connections
- local Compose down

**Estimate:** 2 to 3 days

### Core milestone checkpoint

At this point, the project is good enough to publish if the README is honest and the core metrics are measured.

Target elapsed work: **about 32 to 44 working days** at roughly 3 hours per day. This is the sum of Phase 0 through Phase 15, including the optional Phase 7 compatibility buffer.

### Phase 16 - MLflow registry and promotion gate

**Goal:** add model lifecycle control.

**Work:**

- MLflow tracking
- aliases
- candidate-versus-production comparison
- rejection path

**Definition of done:**

- intentionally worse candidate is rejected

**Evidence to save:**

- MLflow comparison
- rejection log

**Interview angle:**

- retraining is not the same as deployment

**Resume candidate:**

- model registry / promotion logic can be used

**Stop services:**

- stop MLflow profile

**Estimate:** 2 days

### Phase 17 - ONNX benchmark

**Goal:** verify whether ONNX actually helps.

**Work:**

- export
- numerical parity test
- latency comparison

**Definition of done:**

- parity tolerance passes
- latency is recorded for both runtimes

**Evidence to save:**

- benchmark table

**Interview angle:**

- optimization should be measured, not assumed

**Resume candidate:**

- say "exported to ONNX" regardless of speed; say "optimized" only if measured

**Stop services:**

- none beyond local API/test process

**Estimate:** 1 to 2 days

### Phase 18 - Airflow lifecycle workflows

**Goal:** orchestrate batch and ML lifecycle work.

**Work:**

- dbt DAG
- feature snapshot DAG
- retraining/evaluation DAG
- FAISS rebuild
- bounded backfill

**Definition of done:**

- DAGs complete from clean start
- failed task can be retried safely

**Evidence to save:**

- DAG graph
- successful run

**Interview angle:**

- why Airflow does not own the long-running stream

**Resume candidate:**

- Airflow orchestration claim becomes usable

**Stop services:**

```bash
docker compose --profile mlops down
```

**Estimate:** 2 to 3 days

### Phase 19 - Monitoring dashboards

**Goal:** make the system observable.

**Work:**

- Kafka lag
- Spark processing
- DQ metrics
- API latency/errors
- model metadata

**Definition of done:**

- dashboards visibly change during a controlled load or failure test

**Evidence to save:**

- Grafana screenshots

**Interview angle:**

- which metric detects which failure class

**Resume candidate:**

- monitoring claim becomes usable

**Stop services:**

```bash
docker compose --profile monitoring down
```

**Estimate:** 2 days

### Phase 20 - Failure suite

**Goal:** prove recovery behavior rather than describing it.

**Work:**

- Spark crash
- duplicate replay
- bad schema
- late event
- Redis outage
- candidate rejection
- SQL auto-resume

**Definition of done:**

- every test has expected versus observed behavior documented

**Evidence to save:**

- `docs/failure_testing.md`

**Interview angle:**

- concrete failure and recovery stories

**Resume candidate:**

- recovery language can now include measured facts

**Stop services:**

- shutdown all test profiles
- verify cloud resources

**Estimate:** 2 to 3 days

### Phase 21 - Load and performance benchmark

**Goal:** generate the numbers that can safely appear on a resume.

**Work:**

- Kafka throughput
- Spark throughput
- consumer lag
- end-to-end latency
- FAISS latency
- API load
- recovery timing

**Definition of done:**

- benchmark is repeatable
- hardware/configuration is recorded
- result is added to `docs/MEASUREMENTS.md` with a metric ID and commit SHA
- `docs/benchmark_results.md` references those metric IDs rather than re-entering independent values

**Evidence to save:**

- benchmark raw output
- final tables

**Interview angle:**

- benchmark context matters more than a naked throughput number

**Resume candidate:**

- measured numbers only

**Stop services:**

- full shutdown checklist

**Estimate:** 2 to 4 days

### Phase 22 - CI/CD hardening

**Goal:** automate test, build, deploy, and health checks.

**Work:**

- schema tests
- Spark tests
- dbt tests
- model smoke test
- API tests
- security scan
- ACR push
- Container Apps deployment

**Definition of done:**

- clean push deploys successfully
- failed test blocks deployment

**Evidence to save:**

- Actions run

**Interview angle:**

- code deployment and model promotion are separate workflows

**Resume candidate:**

- GitHub Actions deployment claim becomes usable

**Stop services:**

- cloud shutdown checklist

**Estimate:** 2 to 3 days

### Phase 23 - Cost and teardown verification

**Goal:** prove the project can be paused or removed cleanly.

**Work:**

- inspect Azure cost analysis
- record current daily cost
- confirm SQL auto-pause
- confirm Container Apps scale-to-zero
- test Terraform plan for teardown
- back up evidence before destructive tests

**Definition of done:**

- no unexpected always-on compute remains
- persistent versus disposable resources are documented

**Evidence to save:**

- cost screenshot/export
- teardown notes

**Interview angle:**

- cost-aware architecture and resource lifecycle

**Resume candidate:**

- avoid resume cost claims unless the result is useful and repeatable

**Stop services:**

- normal scale-down or full `terraform destroy` only if intentionally ending the environment

**Estimate:** 1 to 2 days

### Phase 24 - Final documentation and portfolio packaging

**Goal:** turn the engineering work into a clear portfolio story.

**Work:**

- README
- architecture diagram
- benchmark report
- model evaluation report
- failure report
- cost note
- resume bullets
- portfolio screenshots

**Definition of done:**

- a recruiter can understand the architecture and results without opening the code first
- every metric maps to the measurement ledger
- no unsupported claim remains

**Evidence to save:**

- tagged release
- final README

**Interview angle:**

- entire project walkthrough

**Resume candidate:**

- final bullets are written here from measured evidence only

**Stop services:**

- final shutdown checklist

**Estimate:** 2 days

## 46. Timeline Summary

A realistic sequence at roughly 3 hours per day. The day ranges below are cumulative planning ranges, not fixed calendar promises.

```text
Days 1-7
Host preflight, repository, local Compose stack, Terraform, Azure foundation

Days 8-13
Dataset profiling, deterministic IDs, Kafka, schemas, DLQ

Days 14-24
Spark streaming, reliability, local Bronze/Silver/Gold Delta, point-in-time enrichment,
ADLS sync/integration smoke test

Days 25-31
Backfill, dbt, data quality, feature pipeline

Days 32-44
Temporal split, baselines, two-tower model, FAISS, basic FastAPI, core Azure deployment

CORE PORTFOLIO MILESTONE

Days 45-53
MLflow, model promotion, ONNX, Airflow, monitoring

Days 54-63
Failure suite, load/performance benchmark, CI/CD hardening

Days 64-67
Cost/teardown verification and final documentation
```

The individual phase estimates sum to approximately **48 to 67 working days** because several later phases can overlap or finish faster once the core environment is stable. Plan around **50 to 65 working days**, and use 67 as a reasonable upper planning bound before adding optional experiments.

The project should be published once the core milestone is honest, measured, and documented. Stretch work can continue afterward without holding the portfolio release hostage.

## 47. Core Definition of Done

The core milestone is complete when:

### Data Engineering

- deterministic event IDs exist
- Kafka partitioning is documented
- schemas are versioned
- malformed events reach the DLQ
- Spark uses event time
- dedup works
- checkpoint recovery works in a controlled test
- Bronze, Silver, and Gold Delta tables exist on the local development path
- scheduled sync/cloud integration to ADLS is demonstrated separately
- point-in-time item enrichment is tested
- at least one bounded backfill works
- dbt tests pass
- data-quality metrics exist

### ML Engineering

- chronological split is documented
- minimum-history rule is documented
- full-catalog evaluation is implemented
- global and category baselines exist
- two-tower model is evaluated under the same protocol
- Recall@10, Recall@20, and NDCG@10 are measured
- FAISS retrieval works
- cold-start fallback exists

### Deployment

- FastAPI is containerized
- image is in ACR
- API is deployed to Container Apps
- health check works
- scale-to-zero setting is documented
- Terraform can reproduce the Azure foundation

## 48. Stretch Definition of Done

Stretch work is complete when:

- MLflow model registry is in place
- candidate rejection is tested
- ONNX parity and latency are benchmarked
- Airflow lifecycle DAGs run reliably
- Grafana dashboards cover pipeline, DQ, API, and ML
- failure tests have written results
- load tests have written results
- CI/CD blocks bad builds and deploys good ones
- cost behavior is documented
- teardown is tested safely

## 49. Final Benchmark Report

Create:

```text
docs/benchmark_results.md
```

This file is a presentation report, not the source of truth. Every numeric value in it must reference a `metric_id` from `docs/MEASUREMENTS.md`. If the two disagree, the ledger wins and the report must be corrected.

Include:

```text
Environment
- MacBook chip / RAM
- Docker limits
- Spark configuration
- Kafka partition count
- dataset slice
- Azure region and relevant deployment settings
- git commit SHA

Streaming - local performance path
- producer events/sec
- sustained processed events/sec
- p50 / p95 / p99 event latency
- max consumer lag
- local Delta write latency / batch duration

Cloud integration
- local-to-ADLS sync duration
- direct ADLS smoke-test duration if run
- storage path and connector/JAR versions
- do not merge these values into the local streaming latency number

Recovery
- restart recovery time
- catch-up time
- events lost
- duplicates after recovery

Recommendation
- full-catalog eligible users/items
- baseline Recall@10 / Recall@20 / NDCG@10
- two-tower Recall@10 / Recall@20 / NDCG@10
- sampled evaluation only if clearly labeled
- FAISS Top-K latency
- cold-start fallback rate

Serving
- requests/sec
- p50 / p95 / p99 latency
- error rate

Cost
- storage
- SQL
- ACR
- Container Apps
- benchmark-session total
```

## 50. Resume Bullet Rules

Do not use draft bullets until each statement is true.

A future structure could look like:

```text
E-commerce Real-Time Recommendation Engine | Azure | Kafka | PySpark | Delta Lake | PyTorch | MLflow

- Built a Kafka and PySpark Structured Streaming pipeline over [measured event count],
  using Avro contracts, event-time watermarks, deterministic deduplication, checkpoints,
  and a DLQ for malformed traffic

- Modeled Bronze, Silver, and Gold Delta layers with point-in-time item enrichment,
  replayable backfills, and dbt marts in Azure SQL with automated data-quality checks

- Trained a PyTorch two-tower recommender against popularity baselines using temporal
  validation and full-catalog Recall@K/NDCG, then served Top-N retrieval through FAISS,
  ONNX, and FastAPI

- Measured [throughput], [p95 latency], and [recovery result] under controlled load and
  failure tests, with Prometheus/Grafana monitoring and GitHub Actions deployment to
  Azure Container Apps
```

Every bracketed number comes from `docs/MEASUREMENTS.md` and maps to a specific `metric_id` recorded at a specific Git commit.

## 51. Interview Stories This Project Should Create

### Data Engineering

Be able to explain:

- why Kafka is partitioned by visitor ID
- why deterministic event IDs matter
- Kafka offsets versus Spark checkpoints
- what happens when Spark crashes
- how duplicates and late events are handled
- why a DLQ exists
- how schema evolution is controlled
- why point-in-time joins prevent leakage
- why Delta is partitioned by time rather than visitor
- how Bronze enables replay
- why Airflow handles batch work instead of the long-running stream

### ML Engineering

Be able to explain:

- why transaction-only positives may be too sparse
- how interaction weights were tested
- why random train/test splits are wrong
- how full-catalog and sampled evaluation differ
- whether the neural model actually beats popularity
- how negative sampling changes training difficulty
- why a two-tower model is useful for retrieval
- why FAISS is used
- how cold-start users and items are handled
- why a candidate model can be rejected
- whether ONNX actually improved latency

### Cloud and system design

Be able to explain:

- why stateful development services were kept local
- why Azure still matters in the architecture
- why the B1s VM was abandoned
- which resources bill while idle
- why Terraform was introduced before feature development
- what scales to zero and what remains persistent

## 52. Things Not to Add Unless a Real Need Appears

Avoid adding tools only to increase keyword count:

- TensorFlow
- MongoDB
- Cassandra
- Elasticsearch
- Kubernetes
- another cloud provider
- another backend framework
- Spark MLlib solely for exposure
- RAG
- unrelated vector database
- LLM chatbot
- agents

The architecture is already large enough. Depth, failure behavior, data correctness, evaluation rigor, and measured performance matter more.

## 53. Final Positioning

This project should sit in the portfolio as the bridge between Data Engineering and ML Engineering.

GreenMLOps can prove model lifecycle and MLOps depth.

Grand Prix Insights can prove model judgment and experimentation.

This project should prove the system underneath an ML product:

```text
raw events
-> reliable stream processing
-> durable temporal data model
-> reproducible point-in-time features
-> honest model evaluation
-> low-latency retrieval
-> monitored deployed service
```

That is the story to protect while building it.

*Last updated: September 2026 | Version 4 | Local Docker + Azure + Kafka + PySpark + Delta Lake + dbt + PyTorch + MLflow + FAISS + FastAPI + Airflow + Grafana + Terraform*
