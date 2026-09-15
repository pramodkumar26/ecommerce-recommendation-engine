"""Phase 3 definition of done.

1. Replaying a bounded range twice is deterministic, with and without injections.
2. Injections are deterministic and change the stream in the way they claim to.
3. Every visitor's events land on exactly one Kafka partition.
"""

import json
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, TopicPartition

ROOT = Path(__file__).resolve().parents[1]
PY = str(ROOT / ".venv" / "bin" / "python")
SIM = str(ROOT / "producer" / "simulator.py")
BOOTSTRAP = "localhost:9092"
TOPICS = ["item_view", "add_to_cart", "transaction"]
LIMIT = 20000


def run_sim(out_path, extra=()):
    cmd = [PY, SIM, "--limit", str(LIMIT), "--dry-run", out_path, *extra]
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    if r.returncode != 0:
        raise RuntimeError(r.stderr)
    return r.stdout


def read_jsonl(path):
    out = []
    with open(path) as f:
        for line in f:
            rec = json.loads(line)
            try:
                value = json.loads(rec["value"])
            except json.JSONDecodeError:
                value = {"__malformed__": rec["value"]}
            out.append((rec["topic"], rec["key"], value))
    return out


def comparable(records):
    """Everything except ingestion_timestamp, which is wall clock and must differ."""
    out = []
    for topic, key, value in records:
        v = {k: val for k, val in value.items() if k != "ingestion_timestamp"}
        out.append((topic, key, json.dumps(v, sort_keys=True)))
    return out


def test_determinism(tmp):
    a, b = f"{tmp}/a.jsonl", f"{tmp}/b.jsonl"
    run_sim(a)
    run_sim(b)
    ra, rb = read_jsonl(a), read_jsonl(b)
    same = comparable(ra) == comparable(rb)
    ingestion_differs = any(
        x[2].get("ingestion_timestamp") != y[2].get("ingestion_timestamp")
        for x, y in zip(ra, rb, strict=True)
        if "ingestion_timestamp" in x[2]
    )
    return {
        "records": len(ra),
        "identical_ignoring_ingestion_time": same,
        "ingestion_timestamp_actually_varies": ingestion_differs,
    }


def test_injection_determinism(tmp):
    flags = ["--duplicate-rate", "0.02", "--malformed-rate", "0.01", "--delay-rate", "0.05"]
    a, b = f"{tmp}/ia.jsonl", f"{tmp}/ib.jsonl"
    run_sim(a, flags)
    run_sim(b, flags)
    ra, rb = read_jsonl(a), read_jsonl(b)
    return {
        "records": len(ra),
        "identical_ignoring_ingestion_time": comparable(ra) == comparable(rb),
    }


def test_injection_effects(tmp):
    plain = f"{tmp}/plain.jsonl"
    run_sim(plain)
    base = read_jsonl(plain)

    dup = f"{tmp}/dup.jsonl"
    run_sim(dup, ["--duplicate-rate", "0.02"])
    dup_recs = read_jsonl(dup)
    ids = [v.get("event_id") for _, _, v in dup_recs if "event_id" in v]
    dup_count = len(ids) - len(set(ids))

    bad = f"{tmp}/bad.jsonl"
    run_sim(bad, ["--malformed-rate", "0.05"])
    bad_recs = read_jsonl(bad)
    malformed = sum(1 for _, _, v in bad_recs if "__malformed__" in v or "visitor_id" not in v)

    late = f"{tmp}/late.jsonl"
    run_sim(late, ["--delay-rate", "0.1"])
    late_recs = read_jsonl(late)

    def out_of_order(recs):
        ts = [v["event_timestamp"] for _, _, v in recs if "event_timestamp" in v]
        return sum(1 for i in range(1, len(ts)) if ts[i] < ts[i - 1])

    return {
        "baseline_records": len(base),
        "baseline_out_of_order_pairs": out_of_order(base),
        "duplicate_ids_when_injecting_2pct": dup_count,
        "malformed_records_when_injecting_5pct": malformed,
        "out_of_order_pairs_when_delaying_10pct": out_of_order(late_recs),
    }


def produce_real(run_tag):
    cmd = [PY, SIM, "--limit", str(LIMIT), "--rate", "20000", "--bootstrap", BOOTSTRAP]
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    if r.returncode != 0:
        raise RuntimeError(r.stderr)
    return r.stdout


def end_offsets(consumer, topic):
    md = consumer.list_topics(topic, timeout=15)
    out = {}
    for p in md.topics[topic].partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
        out[p] = high
    return out


def test_partitioning():
    """Read everything currently in the behavioral topics, check key to partition stability."""
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "verify-replay-partitioning",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )

    partitions_by_visitor = defaultdict(set)
    per_topic = {}
    total = 0

    for topic in TOPICS:
        highs = end_offsets(consumer, topic)
        expected = sum(highs.values())
        per_topic[topic] = {"partitions": len(highs), "records": expected}
        if expected == 0:
            continue

        consumer.assign([TopicPartition(topic, p, 0) for p in highs])
        seen = 0
        empty = 0
        while seen < expected and empty < 15:
            msg = consumer.poll(1.0)
            if msg is None:
                empty += 1
                continue
            if msg.error():
                raise KafkaException(msg.error())
            partitions_by_visitor[msg.key().decode()].add(msg.partition())
            seen += 1
            total += 1
        consumer.unassign()

    consumer.close()

    split = {k: sorted(v) for k, v in partitions_by_visitor.items() if len(v) > 1}
    return {
        "records_read": total,
        "distinct_visitor_keys": len(partitions_by_visitor),
        "keys_spanning_multiple_partitions": len(split),
        "per_topic": per_topic,
        "examples_of_split_keys": dict(list(split.items())[:5]),
    }


def main():
    results = {}
    with tempfile.TemporaryDirectory() as tmp:
        print("1. determinism, no injections")
        results["determinism"] = test_determinism(tmp)
        print("2. determinism, with injections")
        results["determinism_with_injections"] = test_injection_determinism(tmp)
        print("3. injection effects")
        results["injection_effects"] = test_injection_effects(tmp)

    print("4. producing to kafka for partition check")
    produce_real("verify")
    print("5. partition verification")
    results["partitioning"] = test_partitioning()

    checks = {
        "replay_is_deterministic": results["determinism"]["identical_ignoring_ingestion_time"],
        "ingestion_timestamp_is_wall_clock": results["determinism"][
            "ingestion_timestamp_actually_varies"
        ],
        "injections_are_deterministic": results["determinism_with_injections"][
            "identical_ignoring_ingestion_time"
        ],
        "baseline_stream_is_time_ordered": results["injection_effects"][
            "baseline_out_of_order_pairs"
        ]
        == 0,
        "duplicate_injection_creates_duplicates": results["injection_effects"][
            "duplicate_ids_when_injecting_2pct"
        ]
        > 0,
        "malformed_injection_creates_bad_records": results["injection_effects"][
            "malformed_records_when_injecting_5pct"
        ]
        > 0,
        "delay_injection_creates_out_of_order": results["injection_effects"][
            "out_of_order_pairs_when_delaying_10pct"
        ]
        > 0,
        "every_visitor_on_one_partition": results["partitioning"][
            "keys_spanning_multiple_partitions"
        ]
        == 0,
    }
    results["checks"] = checks
    results["passed"] = all(checks.values())

    out = ROOT / "benchmarks" / "raw" / "replay_verification.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2))

    print()
    print(json.dumps(results, indent=2))
    print()
    for name, ok in checks.items():
        print(f"  {'ok  ' if ok else 'FAIL'} {name}")
    print(f"\nwrote {out}")
    print("PASS" if results["passed"] else "FAIL")
    return 0 if results["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
