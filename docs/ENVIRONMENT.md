# Environment

Host and tooling facts for this project. Every benchmark row in `docs/MEASUREMENTS.md` points
back to an environment described here. Fill this in during Phase 0 before any project code is
written. Do not guess values.

Recorded in Phase 0 on 2026-09-15. Raw capture: `docs/evidence/phase0/preflight.txt`.

## Host

```text
model:                  MacBook Pro, Mac17,2
chip:                   Apple M5, 10 cores (4 performance, 6 efficiency)
memory:                 16 GB
macOS version:          26.6.2 (build 25G83)
Docker Desktop version: 29.7.2
Docker memory limit:    8319504384 bytes (7.75 GiB)
Docker CPU limit:       10
Docker storage driver:  containerd snapshotter enabled
architecture:           arm64 (Docker VM reports aarch64)
```

Docker Desktop is on default resource settings. `settings-store.json` holds no explicit `Cpus`
or `MemoryMiB` key, so the 7.75 GiB figure is Docker's default of roughly half the host RAM.

Decision: leave the limit at the default rather than raise it. The host has 16 GB total and
macOS, VS Code, and a browser need most of the remaining 8 GB. A Docker limit high enough to
push the host into swap would contaminate every streaming latency measurement taken later.

Consequence for Phase 1A: the `streaming` profile must fit inside 7.75 GiB, which puts the
Spark worker at roughly 2 to 3 GB rather than the 4 GB top of the section 3 range. The `all`
profile is not runnable on this machine. This is consistent with the rule that profiles stay
separate.

Apple Silicon note: do not assume every required image has an arm64 build. Verify multi-arch
support for each image during first setup and record anything that needed an emulated or
substituted image. Nothing so far has required emulation.

## CLI versions

Recorded in Phase 0 from actual version output.

| Tool | Version | Command used | Notes |
|---|---|---|---|
| Docker | 29.7.2 (build a7dcaa6) | `docker --version` | client at `~/.docker/bin/docker` |
| Docker Compose | v5.5.1 | `docker compose version` | plugin, not standalone `docker-compose` |
| Python | 3.14.7 | `python3 --version` | Homebrew. Too new for the stack, see below |
| Java | OpenJDK 17.0.20.1 | `/opt/homebrew/opt/openjdk@17/bin/java -version` | Homebrew `openjdk@17`, keg-only |
| Terraform | v1.16.2 | `terraform version` | from `hashicorp/tap`, not homebrew-core |
| Azure CLI | 2.90.0 | `az version` | core 2.90.0, telemetry 1.1.0, no extensions |
| GitHub CLI | 2.97.0 | `gh --version` | |
| git | 2.50.1 (Apple Git-155) | `git --version` | |
| Homebrew | 6.0.22 | `brew --version` | |

### Java install note

The `temurin@17` cask requires an interactive sudo password and could not be installed
non-interactively. The `openjdk@17` formula was used instead. It installs under
`/opt/homebrew` with no sudo and is the same JDK 17 build line.

Version choice: Spark 3.5.x officially supports Java 8, 11, and 17. Java 17 is the current LTS
inside that supported set, so it removes one variable from the Phase 7 JAR compatibility work.
Java 21 is not fully supported by Spark 3.5 and was rejected.

`openjdk@17` is keg-only, so `/usr/bin/java` on PATH is still the macOS stub. `JAVA_HOME` must
point at the Homebrew JDK for any host-side Spark or PySpark work:

```text
JAVA_HOME=/opt/homebrew/opt/openjdk@17
```

This is not yet set in the shell profile. Spark runs inside containers for the main path, so it
only matters for local PySpark. Set it in the project environment during Phase 1A.

### Python version constraint

System Python is 3.14.7. PySpark, delta-spark, torch, and faiss wheel availability trails the
newest CPython release by a long margin, so the system interpreter cannot be used directly.

Decision deferred to Phase 1A: create a project virtualenv on Python 3.11 or 3.12 and pin it in
the repo. Recorded here so the constraint is not rediscovered later.

## Container smoke test

Both images ran natively on aarch64. No emulation, no platform warnings.

```text
test 1
image:   hello-world
command: docker run --rm hello-world
result:  pass, "Hello from Docker!"

test 2
image:   eclipse-temurin:17-jre
command: docker run --rm eclipse-temurin:17-jre sh -c 'java -version; uname -m'
result:  pass, OpenJDK Temurin 17.0.20+8, container arch aarch64

date: 2026-09-15
```

## Pinned versions

Filled in as each component is added. Phase 7 depends on this being accurate, since Spark
writing Delta through the ABFS connector needs compatible hadoop-azure, Azure storage, Hadoop,
Spark, and Delta JARs.

| Component | Version | Pinned where | Notes |
|---|---|---|---|
| Java | OpenJDK 17.0.20.1 | host, Homebrew `openjdk@17` | Spark 3.5 supports 8/11/17 |
| Kafka | | | |
| Schema Registry | | | |
| Spark | | | |
| Delta Lake | | | |
| hadoop-azure | | | |
| azure-storage | | | |
| Hadoop | | | |
| Redis | | | |
| Python (project venv) | | | not the system 3.14.7, see above |
| Python packages | | | |

## Local memory budget

Rough development targets from roadmap section 3, to compare against actual `docker stats`
output once the streaming profile runs in Phase 1A.

| Service | Rough target RAM | Measured |
|---|---:|---|
| Kafka | 0.75 to 1.5 GB | |
| Schema Registry | 0.5 to 0.8 GB | |
| Spark master | 0.25 to 0.5 GB | |
| Spark worker | 2 to 4 GB | |
| Redis | 0.1 to 0.25 GB | |
| Airflow webserver / API | 0.3 to 0.6 GB | |
| Airflow scheduler | 0.3 to 0.6 GB | |
| Airflow metadata DB | 0.25 to 0.5 GB | |
| MLflow | 0.25 to 0.5 GB | |
| Prometheus | 0.5 to 1 GB | |
| Grafana | 0.25 to 0.5 GB | |
| Docker overhead / sidecars | 0.5 to 1.5 GB | |

Everything at once can reach roughly 6 to 12 GB before application code, browser, IDE, and the
host OS. This is why Compose profiles stay separate.

Against this host's 7.75 GiB Docker limit: the `streaming` profile (Kafka, Schema Registry,
Spark master, Spark worker, Redis, plus overhead) budgets to roughly 4.1 GB at the low end and
8.5 GB at the high end. It fits only if the Spark worker is held to 2 to 3 GB. The `all`
profile does not fit and will not be run on this machine.

## Storage paths

```text
local Delta root:
local Spark checkpoint root:
dataset root:
```

## Azure

Filled in during Phase 1B from Terraform outputs.

```text
subscription:
region:
resource group:
storage account / ADLS container:
Azure SQL server / database:
container registry:
container apps environment:
```
