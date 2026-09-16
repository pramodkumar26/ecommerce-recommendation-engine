SHELL := /bin/bash
PY := .venv/bin/python
PIP := .venv/bin/pip
PYTHON311 := /opt/homebrew/opt/python@3.11/bin/python3.11
SPARK_PACKAGES := $(shell grep '^SPARK_PACKAGES=' .env 2>/dev/null | cut -d= -f2-)

.PHONY: help venv install env up down ps logs stats smoke smoke-kafka smoke-spark smoke-redis topic-smoke profile verify-event-id topics simulate verify-replay bench-producer schemas dlq-router verify-schema-dlq stream verify-streaming measure-lateness test-duplicates test-late-events test-restart verify-reliability scd silver gold verify-silver test-backfill bench-partitioning lakehouse test-timestamps test-rejects test-persistence full-enrichment regression backfill test-backfill-change clean

help:
	@echo "setup"
	@echo "  make venv           create .venv on Python 3.11"
	@echo "  make install        install requirements into .venv"
	@echo "  make env            create .env from .env.example if missing"
	@echo ""
	@echo "streaming profile"
	@echo "  make up             start kafka, schema registry, spark, redis"
	@echo "  make down           stop the streaming profile, keep volumes"
	@echo "  make ps             container status"
	@echo "  make logs           follow logs"
	@echo "  make stats          one-shot memory and cpu usage"
	@echo ""
	@echo "smoke tests"
	@echo "  make smoke          run kafka, spark, and redis smoke tests"
	@echo ""
	@echo "dataset"
	@echo "  make profile        profile the retailrocket source files"
	@echo "  make verify-event-id  prove event ids are reproducible"
	@echo ""
	@echo "kafka"
	@echo "  make topics         create project topics from kafka/topics/topics.yml"
	@echo "  make simulate       replay events into kafka (LIMIT=, RATE=)"
	@echo "  make verify-replay  phase 3 determinism and partitioning checks"
	@echo "  make bench-producer producer rate control check"
	@echo ""
	@echo "schemas and dlq"
	@echo "  make schemas          register avro schemas, test compatibility"
	@echo "  make dlq-router       consume topics, route bad records to the dlq"
	@echo "  make verify-schema-dlq  phase 4 checks (resets topics)"
	@echo ""
	@echo "spark streaming"
	@echo "  make stream           run the phase 5 streaming job (RUN_LABEL=, LIMIT_PER_TRIGGER=)"
	@echo "  make verify-streaming phase 5 correctness checks against the fixture"
	@echo ""
	@echo "reliability (phase 6)"
	@echo "  make measure-lateness   measure event-time lateness distribution"
	@echo "  make test-duplicates    injected duplicates must not change counts"
	@echo "  make test-late-events   watermark boundary, both sides"
	@echo "  make test-restart       kill mid-stream, restart, prove no loss"
	@echo "  make verify-reliability run all three reliability tests"
	@echo ""
	@echo "lakehouse (phase 7)"
	@echo "  make scd              build dim_items_scd from property snapshots"
	@echo "  make silver           bronze to silver, point-in-time enriched"
	@echo "  make gold             silver to analytics marts"
	@echo "  make lakehouse        scd, silver and gold in order"
	@echo "  make verify-silver    prove no future enrichment"
	@echo "  make test-backfill    bounded rebuild, surgical"
	@echo "  make bench-partitioning  partitioned vs flat query comparison"
	@echo ""
	@echo "phase 7 verification"
	@echo "  make test-timestamps    epoch ms round trip, timezone independence"
	@echo "  make test-rejects       contract rejects fire and categorise"
	@echo "  make test-persistence   cycles the stack, delta survives, rebuild matches"
	@echo "  make full-enrichment    full 2.75M dataset end to end"
	@echo "  make regression         every phase check, about 17 minutes"
	@echo ""
	@echo "phase 8 backfill"
	@echo "  make backfill START=2015-05-10 END=2015-05-12   reprocess a bounded range"
	@echo "  make test-backfill-change   transformation change verification"
	@echo ""
	@echo "danger"
	@echo "  make clean          stop and DELETE kafka and redis volumes"

venv:
	$(PYTHON311) -m venv .venv
	$(PY) -m pip install --upgrade pip

install:
	$(PIP) install -r requirements.txt

env:
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example")

up: env
	docker compose --profile streaming up -d

down:
	docker compose --profile streaming down

ps:
	docker compose --profile streaming ps

logs:
	docker compose --profile streaming logs -f

stats:
	docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}' \
		kafka schema-registry spark-master spark-worker redis

topic-smoke:
	docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 \
		--create --if-not-exists --topic smoke_test --partitions 3 --replication-factor 1

smoke-kafka: topic-smoke
	$(PY) scripts/smoke_kafka.py

smoke-spark:
	docker compose exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 --executor-memory 1g --total-executor-cores 2 \
		/opt/spark/project/scripts/smoke_spark.py

smoke-redis:
	$(PY) scripts/smoke_redis.py

smoke: smoke-kafka smoke-spark smoke-redis

profile:
	$(PY) scripts/profile_dataset.py
	$(PY) scripts/profile_joins.py
	$(PY) scripts/verify_event_id.py

verify-event-id:
	$(PY) scripts/verify_event_id.py

topics:
	$(PY) kafka/create_topics.py

simulate: topics schemas
	$(PY) producer/simulator.py --limit $(or $(LIMIT),20000) --rate $(or $(RATE),2000)

verify-replay: topics
	$(PY) scripts/verify_replay.py

bench-producer: topics
	./scripts/benchmark_producer.sh

schemas:
	$(PY) kafka/register_schemas.py

dlq-router:
	$(PY) kafka/dlq_router.py

verify-schema-dlq: topics
	$(PY) scripts/verify_schema_dlq.py

stream:
	docker compose exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 --driver-memory 1g --executor-memory 1g \
		--total-executor-cores 6 --packages $(SPARK_PACKAGES) \
		/opt/spark/project/streaming/jobs/stream_events.py \
		--run-label $(or $(RUN_LABEL),dev) \
		--max-offsets-per-trigger $(or $(LIMIT_PER_TRIGGER),5000) \
		--await-seconds $(or $(AWAIT),1200) --idle-seconds 40

verify-streaming:
	$(PY) scripts/verify_streaming.py

measure-lateness:
	docker compose exec -T spark-master /opt/spark/bin/spark-submit \
		--master local[2] --driver-memory 900m --packages $(SPARK_PACKAGES) \
		/opt/spark/project/streaming/jobs/measure_lateness.py \
		--run-label $(or $(RUN_LABEL),fixture) --batch-size $(or $(BATCH),5000)

test-duplicates:
	$(PY) scripts/test_duplicates.py

test-late-events:
	$(PY) scripts/test_late_events.py

test-restart:
	$(PY) scripts/test_restart.py

verify-reliability: test-duplicates test-late-events test-restart

SPARK_BATCH = docker compose exec -T spark-master /opt/spark/bin/spark-submit \
	--master spark://spark-master:7077 --driver-memory 1g --executor-memory 2g \
	--total-executor-cores 6 --packages $(SPARK_PACKAGES)
LAKEHOUSE_LABEL = $(or $(RUN_LABEL),silver)

scd:
	$(SPARK_BATCH) /opt/spark/project/batch/jobs/build_item_scd.py --run-label $(LAKEHOUSE_LABEL)

silver:
	$(SPARK_BATCH) /opt/spark/project/batch/jobs/build_silver.py --run-label $(LAKEHOUSE_LABEL)

gold:
	$(SPARK_BATCH) /opt/spark/project/batch/jobs/build_gold.py --run-label $(LAKEHOUSE_LABEL)

lakehouse: scd silver gold

verify-silver:
	$(PY) scripts/verify_silver.py

test-backfill:
	$(PY) scripts/test_backfill_range.py

bench-partitioning:
	$(SPARK_BATCH) /opt/spark/project/batch/jobs/benchmark_partitioning.py \
		--run-label $(LAKEHOUSE_LABEL)

test-timestamps:
	$(SPARK_BATCH) /opt/spark/project/batch/jobs/test_timestamp_roundtrip.py \
		--run-label $(LAKEHOUSE_LABEL)

test-rejects:
	$(PY) scripts/test_silver_rejects.py

test-persistence:
	$(PY) scripts/test_restart_persistence.py

full-enrichment:
	$(PY) scripts/run_full_enrichment.py

regression:
	$(PY) scripts/run_regression.py

backfill:
	$(PY) scripts/backfill.py --run-label $(LAKEHOUSE_LABEL) \
		--start-date $(START) --end-date $(END)

test-backfill-change:
	$(PY) scripts/test_backfill_transformation.py

clean:
	docker compose --profile streaming down -v
