# CLAUDE.md

Project: P3 E-commerce Real-Time Recommendation Engine.

`P3_Ecommerce_Streaming_Roadmap_v4_1.md` at the repo root is the single source of truth for
scope, architecture, phases, and rules. Read it before acting. This file records the working
conventions so they do not have to be restated each session.

## Known roadmap errata

Section 44 says the core target is "about Day 20 to Day 25". That line is stale from v4.
The correct core target is 32 to 44 working days, matching the section 45 core milestone
checkpoint and the section 46 timeline. Use 32 to 44.

## How to work a phase

1. Before building, explain the phase: what we are building, why it matters, how it works.
   Then wait for Pramod's go.
2. Build one step at a time. Do not jump ahead. Do not scaffold future phases.
3. At the end of a phase, check the work against the roadmap's definition of done for that
   phase, save the evidence the phase lists, and update `docs/NEXT_SESSION.md`.
4. Justify decisions with reasoning before committing to them. Give the simplest method that
   works. Do not offer alternatives unless the choice genuinely matters.

## Hard rules

- Do not add any tool, service, or layer that is not in the roadmap. Section 52 lists what to
  refuse: TensorFlow, MongoDB, Cassandra, Elasticsearch, Kubernetes, a second cloud, a second
  backend framework, Spark MLlib for exposure, RAG, an unrelated vector database, LLM chatbot,
  agents. No LLM, RAG, or agent layer in this project at all.
- Hadoop/HDFS and Flask are deliberate omissions, not oversights. Do not reintroduce them.
- Delta writes go to the local bind-mounted volume during development. Never write every Spark
  micro-batch straight to ADLS. Cloud sync is scheduled, and the cloud-integration path is
  benchmarked separately and labeled separately. Never merge a local streaming latency number
  with an ADLS integration number.
- Spark checkpoints use a persistent local bind mount or named Docker volume during
  development. No cloud-backed checkpoint during the primary local latency benchmark.
- No number appears in the README, a benchmark report, or a resume bullet unless it has a row
  in `docs/MEASUREMENTS.md` with a metric ID, a git commit SHA, and the environment.
  `docs/MEASUREMENTS.md` is the authoritative registry. `docs/benchmark_results.md` is a
  presentation report that references metric IDs and never becomes a second source of numbers.
  If the two disagree, the ledger wins.
- Terraform is split into `terraform/persistent/` (resource group, storage, SQL) and
  `terraform/deploy/` (ACR, Container Apps, supporting config). Never destroy persistent
  storage or the database as part of a routine shutdown. Before any destructive command,
  verify nothing important exists only in the resources being removed.
- Keep Docker Compose profiles separate: `streaming`, `mlops`, `monitoring`, `all`. Run the
  smallest set the current phase needs. Do not run every service at once.
- Dataset numbers, event counts, and model metrics are measured, never assumed. The roughly
  2.75M events and roughly 22K transactions figures must be confirmed from the local files in
  Phase 2 before appearing anywhere.

## Writing and output conventions

- Terminal commands and code always go in separate blocks. Every command gets its own block.
  Never mix a command and code in one block.
- Minimal code comments, only where genuinely needed.
- No emojis anywhere: code, output, docs, commit messages.
- Write naturally. Avoid AI-sounding phrasing. No em dashes.
- Keep responses short and direct. No padding.

## Environment

MacBook, zsh, VS Code, Docker Desktop. GitHub under `pramodkumar26`.

## Cost discipline

When Pramod says we are stopping for now, continuing tomorrow, or coming back later, remind him
to run the section 4 end-of-session checklist before ending:

- stop the event simulator
- stop long-running Spark queries
- bring down Compose profiles that are no longer needed
- confirm Container Apps minimum replicas is still 0 where intended
- confirm Azure SQL serverless auto-pause is enabled
- confirm no temporary Azure resource was left running
- commit or stash current work
- update `docs/NEXT_SESSION.md`
- record any new measured result in `docs/MEASUREMENTS.md`

## Phase order

Phase 0 host preflight, 1A repo and local stack, 1B Terraform and Azure foundation, 2 dataset
profiling and deterministic event IDs, 3 Kafka producer and topics, 4 Avro/Schema Registry/DLQ,
5 core Spark streaming, 6 Spark reliability, 7 Bronze/Silver/Gold and point-in-time enrichment,
8 backfill and replay, 9 Azure SQL and dbt, 10 data quality, 11 feature pipeline, 12 temporal
split and baselines, 13 two-tower model, 14 FAISS and serving, 15 core Azure deployment.

Core milestone ends at Phase 15. Phases 16 through 24 are stretch.
