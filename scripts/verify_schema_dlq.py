"""Phase 4 definition of done.

1. A compatible v2 schema is accepted, an incompatible v3 is rejected.
2. A v1 reader can still decode records written with v2 (schema resolution).
3. Malformed records are routed to the DLQ with the right error type and full context.
4. Valid traffic keeps flowing while bad records exist.

Resets the behavioral topics first so counts are exact rather than polluted by earlier runs.
"""

import json
import re
import subprocess
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

ROOT = Path(__file__).resolve().parents[1]
PY = str(ROOT / ".venv" / "bin" / "python")
SCHEMAS = ROOT / "producer" / "schemas"
BOOTSTRAP = "localhost:9092"
SR_URL = "http://localhost:8081"
EVENT_TOPICS = ["item_view", "add_to_cart", "transaction"]
DLQ_TOPIC = "clickstream_dlq"
ALL_TOPICS = EVENT_TOPICS + [DLQ_TOPIC]

LIMIT = 20000
SEED = 42
MALFORMED_RATE = 0.05
DUPLICATE_RATE = 0.02


def reset_topics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = [t for t in ALL_TOPICS if t in admin.list_topics(timeout=15).topics]
    if existing:
        for name, fut in admin.delete_topics(existing, operation_timeout=30).items():
            try:
                fut.result(timeout=30)
            except Exception as e:
                print(f"  delete {name}: {e}")
    for _ in range(30):
        time.sleep(1)
        if not any(t in admin.list_topics(timeout=15).topics for t in ALL_TOPICS):
            break
    r = subprocess.run([PY, str(ROOT / "kafka" / "create_topics.py")], capture_output=True, text=True, cwd=ROOT)
    if r.returncode != 0:
        raise RuntimeError(r.stdout + r.stderr)
    return [ln for ln in r.stdout.splitlines() if ln.startswith("created")]


def reset_subjects():
    import urllib.request

    for subj in [f"{t}-value" for t in ALL_TOPICS]:
        for url in (
            f"{SR_URL}/subjects/{subj}",
            f"{SR_URL}/subjects/{subj}?permanent=true",
        ):
            req = urllib.request.Request(url, method="DELETE")
            try:
                urllib.request.urlopen(req, timeout=10).read()
            except Exception:
                pass


def compatibility_checks():
    client = SchemaRegistryClient({"url": SR_URL})
    r = subprocess.run([PY, str(ROOT / "kafka" / "register_schemas.py")], capture_output=True, text=True, cwd=ROOT)
    if r.returncode != 0:
        raise RuntimeError(r.stdout + r.stderr)

    subj = "item_view-value"
    v2 = (SCHEMAS / "clickstream_event_v2.avsc").read_text()
    v3 = (SCHEMAS / "clickstream_event_v3_incompatible.avsc").read_text()
    return {
        "registry_level": client.get_compatibility(),
        "v2_backward_compatible": client.test_compatibility(subj, Schema(v2, "AVRO")),
        "v3_rejected": not client.test_compatibility(subj, Schema(v3, "AVRO")),
        "subjects": sorted(client.get_subjects()),
    }


def schema_resolution_check():
    """Write with v2, read with v1. The reader should drop the fields it does not know."""
    from confluent_kafka import Producer

    client = SchemaRegistryClient({"url": SR_URL})
    v1 = (SCHEMAS / "clickstream_event_v1.avsc").read_text()
    v2 = (SCHEMAS / "clickstream_event_v2.avsc").read_text()

    client.register_schema("item_view-value", Schema(v2, "AVRO"))
    ser_v2 = AvroSerializer(client, v2, lambda r, c: r)
    de_v1 = AvroDeserializer(client, v1, lambda r, c: r)

    record = {
        "event_id": "a" * 64,
        "visitor_id": 999999,
        "item_id": 12345,
        "event_type": "view",
        "event_timestamp": 1433221332117,
        "ingestion_timestamp": int(time.time() * 1000),
        "transaction_id": None,
        "schema_version": 2,
        "source_file": "events.csv",
        "source_row_number": 1,
        "session_id": "sess-abc",
        "device_type": "mobile",
    }
    payload = ser_v2(record, SerializationContext("item_view", MessageField.VALUE))
    decoded = de_v1(payload, SerializationContext("item_view", MessageField.VALUE))

    p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all"})
    p.produce("item_view", key=b"999999", value=payload)
    p.flush(15)

    return {
        "v2_record_decoded_by_v1_reader": decoded is not None,
        "v1_reader_sees_known_fields": decoded.get("visitor_id") == 999999,
        "v1_reader_drops_v2_only_fields": "session_id" not in decoded,
        "decoded_keys": sorted(decoded.keys()),
    }


def produce():
    cmd = [
        PY, str(ROOT / "producer" / "simulator.py"),
        "--limit", str(LIMIT),
        "--rate", "20000",
        "--seed", str(SEED),
        "--malformed-rate", str(MALFORMED_RATE),
        "--duplicate-rate", str(DUPLICATE_RATE),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    if r.returncode != 0:
        raise RuntimeError(r.stdout + r.stderr)
    emitted = int(re.search(r"emitted (\d+) records", r.stdout).group(1))
    malformed = int(re.search(r"malformed injected: (\d+)", r.stdout).group(1))
    duplicates = int(re.search(r"duplicates injected: (\d+)", r.stdout).group(1))
    on_wire = int(re.search(r"malformed records on the wire: (\d+)", r.stdout).group(1))
    modes = json.loads(re.search(r"malformed injected: \d+ (\{.*\})", r.stdout).group(1).replace("'", '"'))
    return {
        "emitted": emitted,
        "malformed_distinct": malformed,
        "malformed_on_wire": on_wire,
        "duplicates": duplicates,
        "modes": modes,
    }


def run_router(stats_path):
    cmd = [
        PY, str(ROOT / "kafka" / "dlq_router.py"),
        "--group", f"verify-{int(time.time())}",
        "--idle-timeout", "8",
        "--stats-out", stats_path,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    if r.returncode != 0:
        raise RuntimeError(r.stdout + r.stderr)
    return json.loads(Path(stats_path).read_text())


def sample_dlq(n=3):
    client = SchemaRegistryClient({"url": SR_URL})
    de = AvroDeserializer(client, (SCHEMAS / "dlq_record.avsc").read_text(), lambda r, c: r)
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"dlq-sample-{int(time.time())}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    md = c.list_topics(DLQ_TOPIC, timeout=15)
    c.assign([TopicPartition(DLQ_TOPIC, p, 0) for p in md.topics[DLQ_TOPIC].partitions])
    out = []
    empty = 0
    seen_types = set()
    while len(out) < n and empty < 15:
        msg = c.poll(1.0)
        if msg is None:
            empty += 1
            continue
        rec = de(msg.value(), SerializationContext(DLQ_TOPIC, MessageField.VALUE))
        if rec["error_type"] in seen_types:
            continue
        seen_types.add(rec["error_type"])
        out.append({
            "error_type": rec["error_type"],
            "error_message": rec["error_message"][:160],
            "source_topic": rec["source_topic"],
            "partition": rec["partition"],
            "offset": rec["offset"],
            "message_key": rec["message_key"],
            "original_payload_bytes": len(rec["original_payload"]),
            "schema_version": rec["schema_version"],
        })
    c.close()
    return out


def main():
    results = {}

    print("1. resetting topics and subjects")
    reset_subjects()
    created = reset_topics()
    results["topics_created"] = created

    print("2. schema compatibility")
    results["compatibility"] = compatibility_checks()

    print("3. schema resolution, v2 written and v1 read")
    results["schema_resolution"] = schema_resolution_check()

    print("4. producing with injections")
    results["produced"] = produce()

    print("5. running dlq router")
    stats_path = str(ROOT / "benchmarks" / "raw" / "dlq_router_stats.json")
    results["router"] = run_router(stats_path)

    print("6. sampling dlq")
    results["dlq_samples"] = sample_dlq()

    prod = results["produced"]
    router = results["router"]
    comp = results["compatibility"]
    res = results["schema_resolution"]

    # the extra record is the v2 probe written in step 3
    expected_consumed = prod["emitted"] + 1

    checks = {
        "v2_is_backward_compatible": comp["v2_backward_compatible"],
        "v3_is_rejected": comp["v3_rejected"],
        "v1_reader_decodes_v2_record": res["v2_record_decoded_by_v1_reader"],
        "v1_reader_drops_unknown_fields": res["v1_reader_drops_v2_only_fields"],
        "every_produced_record_consumed": router["consumed"] == expected_consumed,
        "dlq_count_matches_malformed_on_wire": router["routed_to_dlq"] == prod["malformed_on_wire"],
        "valid_traffic_kept_flowing": router["valid"]
        == expected_consumed - prod["malformed_on_wire"],
        "nothing_lost_or_invented": router["valid"] + router["routed_to_dlq"]
        == router["consumed"],
        "all_three_error_types_seen": set(router["by_error_type"]) == {
            "deserialization_failed", "unknown_schema_id", "validation_failed"
        },
        "dlq_records_carry_context": all(
            s["source_topic"] and s["offset"] >= 0 and s["original_payload_bytes"] >= 0
            for s in results["dlq_samples"]
        ),
    }
    results["expected_consumed"] = expected_consumed
    results["checks"] = checks
    results["passed"] = all(checks.values())

    out = ROOT / "benchmarks" / "raw" / "schema_dlq_verification.json"
    out.write_text(json.dumps(results, indent=2, default=str))

    print()
    print(json.dumps({k: results[k] for k in ("compatibility", "schema_resolution", "produced", "router", "dlq_samples")}, indent=2, default=str))
    print()
    for name, ok in checks.items():
        print(f"  {'ok  ' if ok else 'FAIL'} {name}")
    print(f"\nwrote {out}")
    print("PASS" if results["passed"] else "FAIL")
    return 0 if results["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
