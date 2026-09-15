# Next Session

Session handoff file. Updated at the end of every work session so a fresh chat can resume
without rereading the whole roadmap. Overwrite the Current State block each time; append to
the log at the bottom.

## Current state

```text
phase:                Phase 1A complete
last task completed:  Local streaming stack built and verified from a clean copy.
branch / commit:      main, Phase 1A changes not yet committed
services running:     none, all containers down
services stopped:     streaming profile down, kafka-data and redis-data volumes kept
next command to run:  make up
unresolved error:     none
next test:            Phase 1B, terraform plan against the Azure foundation
Azure left alive:     none, no Azure resources provisioned yet
```

## Phase 1A result

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

Phase 1B, Terraform and Azure foundation. Resource group, storage account with ADLS Gen2,
Azure SQL serverless, Container Registry, Container Apps environment, cost tags, outputs, and
a remote state decision. Split across `terraform/persistent/` and `terraform/deploy/`.

Phase 1B is done when `terraform plan` is clean, `terraform apply` creates the environment,
storage and SQL connectivity are tested, and the Container Apps skeleton exists with minimum
replicas configured as intended.

This is the first phase that can cost money. Before applying, confirm in the portal which free
allowances actually apply to the subscription rather than trusting the roadmap's September 2026
notes. `az login` has not been run yet.

Estimate: 2 to 3 days.

## Open questions

- Terraform remote state backend, or local state for now. Decide at the start of Phase 1B.
- Azure region. Pick one close by and use it consistently for cost and latency comparability.

## Measurement ledger

Still empty. Phase 2 produces the first rows, starting with the dataset profile counts. The
idle memory figures above are environment facts recorded in `docs/ENVIRONMENT.md` with
evidence, not performance claims, and they do not appear in the README.

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
