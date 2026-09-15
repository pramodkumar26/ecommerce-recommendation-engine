SHELL := /bin/bash
PY := .venv/bin/python
PIP := .venv/bin/pip
PYTHON311 := /opt/homebrew/opt/python@3.11/bin/python3.11

.PHONY: help venv install env up down ps logs stats smoke smoke-kafka smoke-spark smoke-redis topic-smoke profile verify-event-id topics simulate verify-replay bench-producer schemas dlq-router verify-schema-dlq clean

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

clean:
	docker compose --profile streaming down -v
