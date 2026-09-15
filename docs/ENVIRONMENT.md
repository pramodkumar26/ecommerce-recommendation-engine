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
| Java (host) | OpenJDK 17.0.20.1 | Homebrew `openjdk@17` | Spark 3.5 supports 8/11/17 |
| Java (Spark image) | OpenJDK 17 | `apache/spark:3.5.7-python3` | matches host major version |
| Kafka | Confluent 7.9.2 | `.env`, `CONFLUENT_VERSION` | KRaft mode, no Zookeeper |
| Schema Registry | Confluent 7.9.2 | `.env`, `CONFLUENT_VERSION` | same version as broker |
| Spark | 3.5.7 | `.env`, `SPARK_VERSION` | `apache/spark:3.5.7-python3` |
| Hadoop (bundled) | 3.3.4 | bundled in Spark 3.5.7 | drives the Phase 7 ABFS JAR choice |
| Delta Lake | | | Phase 7 |
| hadoop-azure | | | Phase 7, must match Hadoop 3.3.4 |
| azure-storage | | | Phase 7 |
| Redis | 7.4-alpine | `.env`, `REDIS_VERSION` | appendonly, 200 MB maxmemory |
| Python (project venv) | 3.11.16 | `.venv`, Homebrew `python@3.11` | not the system 3.14.7, see above |
| Python (Spark image) | 3.8.10 | `apache/spark:3.5.7-python3` | see Spark version note below |
| Python packages | see `requirements.txt` | `requirements.txt` | confluent-kafka 2.12.0, redis 6.4.0 |

### Spark version choice

Spark 3.5.7 was chosen over Spark 4.0.1 deliberately.

Spark 3.5.7 bundles Hadoop 3.3.4, which is the most widely documented pairing with
`hadoop-azure` and Delta Lake 3.x. Phase 7 carries the roadmap's only budgeted risk day for
ABFS and JAR compatibility, and Spark 4.0 bundles Hadoop 3.4, which would trade a well
understood problem for an unfamiliar one.

The cost is that `apache/spark:3.5.7-python3` ships Python 3.8.10 while the project venv is
3.11.16. This is acceptable because all Spark code runs inside the container, where driver and
executor share the same 3.8 interpreter. The 3.11 venv serves the producer, ML training, and
the API, none of which share a Python process with Spark. If a Spark job ever needs a newer
interpreter, the fix is a custom Spark image, not a version downgrade elsewhere.

Spark 4.0.1 was verified to have an arm64 build with Python 3.10.12 and Java 17, so the option
remains open if Phase 7 goes badly.

## Local memory budget

Rough development targets from roadmap section 3, to compare against actual `docker stats`
output once the streaming profile runs in Phase 1A.

| Service | Rough target RAM | Compose limit | Measured idle |
|---|---:|---:|---:|
| Kafka | 0.75 to 1.5 GB | 1536 MB | 497 MiB |
| Schema Registry | 0.5 to 0.8 GB | 768 MB | 306 MiB |
| Spark master | 0.25 to 0.5 GB | 512 MB | 169 MiB |
| Spark worker | 2 to 4 GB | 2560 MB | 239 MiB |
| Redis | 0.1 to 0.25 GB | 256 MB | 20 MiB |
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

Measured in Phase 1A: Compose limits sum to 5.4 GiB of the available 7.75 GiB. Actual idle
usage across the five services is about 1.2 GiB. Idle is not load, so these figures will be
retaken under the Phase 21 benchmark. Evidence: `docs/evidence/phase1a/smoke_and_stats.txt`.

## Storage paths

Bind mounts from the repo, all gitignored. Paths inside the Spark containers are stable so job
code does not depend on host layout.

```text
local Delta root:           ./data/delta       -> /opt/spark/project/data/delta
local Spark checkpoint root: ./data/checkpoints -> /opt/spark/project/data/checkpoints
dataset root:               ./data/raw
job code:                   ./streaming        -> /opt/spark/project/streaming
                            ./batch            -> /opt/spark/project/batch
                            ./scripts          -> /opt/spark/project/scripts
```

Kafka and Redis use named Docker volumes (`kafka-data`, `redis-data`) rather than bind mounts,
so `make down` preserves them and only `make clean` removes them.

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
