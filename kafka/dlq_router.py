"""Phase 4 contract-validation harness. NOT a durable pipeline service.

Consumes the behavioral topics, routes unusable records to clickstream_dlq with enough context
to reproduce each failure, and keeps going, demonstrating that bad data does not stop a stream.

WHAT THIS IS, AND WHY IT DOES NOT COMMIT OFFSETS

It runs with enable.auto.commit=false and deliberately never commits. That is not an oversight.
This is a verification harness: every run is expected to read the topic from the beginning and
produce a complete, repeatable picture of which records violate the contract. Committing
offsets would make the second run of scripts/verify_schema_dlq.py see almost nothing and
silently weaken the test. Each run also uses a fresh group.id for the same reason.

The consequence is stated plainly: restarting this router reprocesses everything and rewrites
DLQ records it has already written. Acceptable for a harness, unacceptable for a service.

WHO OWNS THE DURABLE PATH INSTEAD

The Spark Bronze -> Silver path owns durable invalid-record handling for the actual pipeline.
Spark checkpoints track progress, and batch/jobs/build_silver.py writes contract failures to the
silver_rejected Delta table. That path is exactly-once with respect to its checkpoint and is
what Phase 6's restart test exercises.

Two reject surfaces exist and they catch different things:

    clickstream_dlq   wire-level failures. Bytes that are not valid Confluent Avro framing, or
                      carry a schema id the registry does not know. These never become rows at
                      all, so only a Kafka-level consumer can see them.

    silver_rejected   records that decoded cleanly and then broke a business rule, for example
                      a transaction with no transaction_id. These are rows, so Spark handles
                      them.

The overlap is validation_failed: this harness classifies it from Kafka, and Silver classifies
it again in Spark. Both apply the same rules, and tests/test_validation_parity.py asserts the
two implementations agree record for record.
"""

import argparse
import json
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kafka.validation import Invalid, decode, dlq_record, validate  # noqa: E402

SCHEMAS = Path(__file__).resolve().parents[1] / "producer" / "schemas"
DLQ_TOPIC = "clickstream_dlq"
EXPECTED_TYPE_BY_TOPIC = {
    "item_view": "view",
    "add_to_cart": "addtocart",
    "transaction": "transaction",
}


def build(args):
    sr = SchemaRegistryClient({"url": args.schema_registry})
    event_schema = (SCHEMAS / "clickstream_event_v1.avsc").read_text()
    dlq_schema = (SCHEMAS / "dlq_record.avsc").read_text()

    deserializer = AvroDeserializer(sr, event_schema, lambda rec, ctx: rec)
    dlq_serializer = AvroSerializer(sr, dlq_schema, lambda rec, ctx: rec)

    # No offsets are committed, by design. See the module docstring: this is a verification
    # harness that must re-read the whole topic on every run, not a service that resumes.
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": "all"})
    return consumer, producer, deserializer, dlq_serializer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--schema-registry", default="http://localhost:8081")
    ap.add_argument("--group", default="dlq-router")
    ap.add_argument("--topics", nargs="+", default=list(EXPECTED_TYPE_BY_TOPIC))
    ap.add_argument("--max-records", type=int, default=None)
    ap.add_argument("--idle-timeout", type=float, default=10.0)
    ap.add_argument("--stats-out", default=None)
    args = ap.parse_args()

    consumer, producer, deserializer, dlq_serializer = build(args)
    consumer.subscribe(args.topics)

    stats = {
        "consumed": 0,
        "valid": 0,
        "routed_to_dlq": 0,
        "by_error_type": {},
        "by_topic": {t: {"valid": 0, "dlq": 0} for t in args.topics},
        "first_error_examples": {},
    }

    idle_since = time.time()
    print(f"routing from {args.topics} into {DLQ_TOPIC}, group={args.group}")

    try:
        while True:
            if args.max_records and stats["consumed"] >= args.max_records:
                break
            msg = consumer.poll(1.0)
            if msg is None:
                if time.time() - idle_since > args.idle_timeout:
                    break
                continue
            if msg.error():
                raise KafkaException(msg.error())

            idle_since = time.time()
            stats["consumed"] += 1
            topic = msg.topic()
            key = msg.key().decode() if msg.key() else None

            record = None
            try:
                record = decode(deserializer, topic, msg.value())
                validate(record, EXPECTED_TYPE_BY_TOPIC.get(topic))
            except Invalid as bad:
                schema_version = record.get("schema_version") if record else None
                entry = dlq_record(
                    msg.value(),
                    bad.error_type,
                    bad.message,
                    topic,
                    msg.partition(),
                    msg.offset(),
                    key,
                    int(time.time() * 1000),
                    schema_version,
                )
                producer.produce(
                    DLQ_TOPIC,
                    key=(key or "unknown").encode(),
                    value=dlq_serializer(
                        entry, SerializationContext(DLQ_TOPIC, MessageField.VALUE)
                    ),
                )
                producer.poll(0)
                stats["routed_to_dlq"] += 1
                stats["by_topic"][topic]["dlq"] += 1
                stats["by_error_type"][bad.error_type] = (
                    stats["by_error_type"].get(bad.error_type, 0) + 1
                )
                stats["first_error_examples"].setdefault(bad.error_type, bad.message[:200])
                continue

            stats["valid"] += 1
            stats["by_topic"][topic]["valid"] += 1
    finally:
        producer.flush(30)
        consumer.close()

    print(f"\nconsumed {stats['consumed']}")
    print(f"valid    {stats['valid']}")
    print(f"dlq      {stats['routed_to_dlq']}")
    print(f"by error type: {stats['by_error_type']}")
    for t, counts in stats["by_topic"].items():
        print(f"  {t:<14} valid={counts['valid']} dlq={counts['dlq']}")

    if args.stats_out:
        Path(args.stats_out).write_text(json.dumps(stats, indent=2))
        print(f"\nwrote {args.stats_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
