# Next Session

Session handoff file. Updated at the end of every work session so a fresh chat can resume
without rereading the whole roadmap. Overwrite the Current State block each time; append to
the log at the bottom.

## Current state

```text
phase:                Phase 0 complete
last task completed:  Host preflight. Tools installed and verified, ENVIRONMENT.md filled in.
branch / commit:      repo not initialized yet, git init happens in Phase 1A
services running:     Docker Desktop (started during preflight, no project containers)
services stopped:     no project Compose stack exists yet
next command to run:  waiting for go on Phase 1A
unresolved error:     none
next test:            Phase 1A, Kafka producer/consumer smoke test and a simple Spark job
Azure left alive:     none, no Azure resources provisioned yet
```

## Phase 0 result

Definition of done, all met:

- Docker pulls and runs containers for this host architecture, verified with `hello-world` and
  `eclipse-temurin:17-jre`, both native aarch64 with no emulation
- Python, Java, Terraform, Azure CLI, and GitHub CLI versions recorded
- Docker CPU limit (10) and memory limit (7.75 GiB) recorded
- `docs/ENVIRONMENT.md` exists and is filled in

Evidence saved to `docs/evidence/phase0/preflight.txt`.

Installed during this phase: OpenJDK 17.0.20.1, Terraform 1.16.2, Azure CLI 2.90.0.
Already present: Docker 29.7.2, Compose v5.5.1, Python 3.14.7, gh 2.97.0, git 2.50.1.

## Constraints carried into Phase 1A

Two findings from preflight that shape the next phase.

Docker memory is 7.75 GiB on a 16 GB host, left at Docker's default deliberately so the host
does not swap during later latency benchmarks. The `streaming` profile must fit inside that,
which caps the Spark worker at roughly 2 to 3 GB rather than the 4 GB top of the section 3
range. The `all` profile will not be run on this machine.

System Python is 3.14.7, which is too new for PySpark, delta-spark, torch, and faiss wheels.
Phase 1A needs a project virtualenv on Python 3.11 or 3.12, pinned in the repo.

Also pending: `JAVA_HOME` is not set in the shell profile. `openjdk@17` is keg-only, so
`/usr/bin/java` is still the macOS stub. Set `JAVA_HOME=/opt/homebrew/opt/openjdk@17` in the
project environment during Phase 1A. Only matters for host-side PySpark, since Spark runs in
containers on the main path.

## Next up

Phase 1A, repository and local stack. Repository structure per roadmap section 43, Docker
Compose profiles, `.env.example`, local Kafka, Schema Registry, Spark, and Redis, plus helper
scripts or a Makefile. Includes `git init` and the first commit, which the measurement ledger
needs before any benchmark row can cite a commit SHA.

Phase 1A is done when a fresh clone can boot the `streaming` profile from documented commands,
Spark executes a simple job, a Kafka producer/consumer smoke test passes, and `docker stats`
confirms the profile fits inside the recorded Docker memory budget.

Estimate: 1 to 2 days.

## Open questions

- Project Python version, 3.11 or 3.12. Decide at the start of Phase 1A.

## Session log

### 2026-09-15

Read `P3_Ecommerce_Streaming_Roadmap_v4_1.md` in full. Created `CLAUDE.md` with conventions,
hard rules, and the section 44 errata note that the core target is 32 to 44 working days, not
Day 20 to Day 25. Created `docs/ENVIRONMENT.md`, `docs/NEXT_SESSION.md`, `docs/MEASUREMENTS.md`,
and `docs/dataset_profile.md` as scaffolding.

Ran Phase 0 host preflight. Found Terraform, Azure CLI, and a Java runtime missing, and Docker
Desktop not running. Installed OpenJDK 17 via the Homebrew formula after the `temurin@17` cask
failed on a non-interactive sudo prompt, installed Terraform 1.16.2 from `hashicorp/tap` since
it is no longer in homebrew-core, installed Azure CLI 2.90.0. Started Docker Desktop and
recorded its default limits. Ran both container smoke tests, native arm64, no emulation.
Filled in `docs/ENVIRONMENT.md` and saved the raw capture. No project code written. No Azure
resources created. No measurements recorded, Phase 2 produces the first ledger rows.
