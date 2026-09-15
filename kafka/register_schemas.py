"""Register Avro schemas and prove the compatibility rules actually bite.

Subject naming is TopicNameStrategy: <topic>-value. Each behavioral topic gets its own subject
so a schema could diverge per topic later, even though all three share ClickstreamEvent today.
"""

import argparse
import sys
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

SCHEMAS = Path(__file__).resolve().parents[1] / "producer" / "schemas"
EVENT_TOPICS = ["item_view", "add_to_cart", "transaction"]
DLQ_TOPIC = "clickstream_dlq"


def read(name):
    return SCHEMAS.joinpath(name).read_text()


def subject(topic):
    return f"{topic}-value"


def register(client, subj, schema_str):
    schema = Schema(schema_str, schema_type="AVRO")
    sid = client.register_schema(subj, schema)
    registered = client.lookup_schema(subj, schema)
    return sid, registered.version


def check_compatible(client, subj, schema_str):
    return client.test_compatibility(subj, Schema(schema_str, schema_type="AVRO"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8081")
    ap.add_argument("--register-v2", action="store_true", help="also register v2, not just test it")
    args = ap.parse_args()

    client = SchemaRegistryClient({"url": args.url})

    v1 = read("clickstream_event_v1.avsc")
    v2 = read("clickstream_event_v2.avsc")
    v3 = read("clickstream_event_v3_incompatible.avsc")
    dlq = read("dlq_record.avsc")

    print(f"registry compatibility level: {client.get_compatibility()}")
    print()

    print("registering v1 on behavioral topics")
    for topic in EVENT_TOPICS:
        sid, ver = register(client, subject(topic), v1)
        print(f"  {subject(topic):<24} schema_id={sid} version={ver}")

    print("\nregistering dlq schema")
    sid, ver = register(client, subject(DLQ_TOPIC), dlq)
    print(f"  {subject(DLQ_TOPIC):<24} schema_id={sid} version={ver}")

    print("\ncompatibility checks against item_view-value")
    subj = subject("item_view")

    v2_ok = check_compatible(client, subj, v2)
    print(f"  v2, adds two optional fields with defaults: {'compatible' if v2_ok else 'REJECTED'}")

    v3_ok = check_compatible(client, subj, v3)
    print(f"  v3, changes item_id from long to string:    {'compatible' if v3_ok else 'rejected'}")

    if args.register_v2:
        print("\nregistering v2 on behavioral topics")
        for topic in EVENT_TOPICS:
            sid, ver = register(client, subject(topic), v2)
            print(f"  {subject(topic):<24} schema_id={sid} version={ver}")

    print("\nsubjects and versions")
    for subj in sorted(client.get_subjects()):
        versions = client.get_versions(subj)
        print(f"  {subj:<24} versions={versions}")

    ok = v2_ok and not v3_ok
    print("\nPASS" if ok else "\nFAIL expected v2 compatible and v3 rejected")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
