"""Replay Retailrocket events into Kafka as controlled traffic.

Determinism contract: for a fixed --seed and a fixed bounded range, the sequence of records
emitted is identical across runs, except ingestion_timestamp which is wall clock by definition.
Every injection decision is drawn from the seeded RNG exactly once, in a fixed order, so the
plan is a pure function of the seed. Verified by scripts/verify_replay.py.

Ordering: the source file is not sorted by timestamp (1,377,377 adjacent pairs are out of
order). Replay sorts by (event_timestamp, source_row_number) so the baseline is ordered and
out-of-order arrival is something we inject on purpose rather than inherit by accident.
"""

import argparse
import json
import random
import sys
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from producer.event_id import event_id, normalize_transaction_id  # noqa: E402

SOURCE_FILE = "events.csv"
RAW = Path("data/raw")
SCHEMAS = Path(__file__).resolve().parent / "schemas"
SCHEMA_VERSION = 1
MAGIC_BYTE = 0

TOPIC_BY_EVENT = {
    "view": "item_view",
    "addtocart": "add_to_cart",
    "transaction": "transaction",
}

CORRUPTION_MODES = ["raw_garbage", "unknown_schema_id", "invalid_field"]
INVALID_FIELD_MODES = ["negative_visitor", "zero_timestamp", "orphan_transaction_id"]


def load_sorted(limit=None, start_row=0):
    df = pd.read_csv(
        RAW / SOURCE_FILE,
        dtype={
            "timestamp": "int64",
            "visitorid": "int64",
            "event": "str",
            "itemid": "int64",
            "transactionid": "float64",
        },
    )
    df["source_row_number"] = range(len(df))
    df = df.sort_values(["timestamp", "source_row_number"], kind="stable").reset_index(drop=True)
    if start_row:
        df = df.iloc[start_row:].reset_index(drop=True)
    if limit is not None:
        df = df.iloc[:limit].reset_index(drop=True)
    return df


def build_record(row, ingestion_ms):
    eid = event_id(
        SOURCE_FILE,
        row["source_row_number"],
        row["timestamp"],
        row["visitorid"],
        row["itemid"],
        row["event"],
        row["transactionid"],
    )
    tx = normalize_transaction_id(row["transactionid"])
    return {
        "event_id": eid,
        "visitor_id": int(row["visitorid"]),
        "item_id": int(row["itemid"]),
        "event_type": row["event"],
        "event_timestamp": int(row["timestamp"]),
        "ingestion_timestamp": ingestion_ms,
        "transaction_id": tx or None,
        "schema_version": SCHEMA_VERSION,
        "source_file": SOURCE_FILE,
        "source_row_number": int(row["source_row_number"]),
    }


def apply_invalid_field(record, which):
    """A record that serialises fine but violates a documented business rule."""
    bad = dict(record)
    if which == "negative_visitor":
        bad["visitor_id"] = -1
    elif which == "zero_timestamp":
        bad["event_timestamp"] = 0
    elif bad["event_type"] != "transaction":
        bad["transaction_id"] = "not-a-real-transaction"
    else:
        bad["transaction_id"] = None
    return bad


def corrupt_bytes(good_payload, mode, garbage, bogus_schema_id):
    """Bytes a schema-aware consumer must reject, one failure mode per decoding layer."""
    if mode == "raw_garbage":
        return garbage
    return bytes([MAGIC_BYTE]) + bogus_schema_id.to_bytes(4, "big") + good_payload[5:]


def plan(df, rng, duplicate_rate, malformed_rate, delay_rate, max_delay_positions):
    """Decide every injection up front, drawing from the RNG in a fixed order.

    Delays shift an event by a number of positions rather than by wall-clock time, so the
    emitted order is reproducible regardless of how fast the machine runs.
    """
    pending = []
    deferred = {}
    for i, row in enumerate(df.itertuples(index=False)):
        r = {
            "timestamp": row.timestamp,
            "visitorid": row.visitorid,
            "itemid": row.itemid,
            "event": row.event,
            "transactionid": row.transactionid,
            "source_row_number": row.source_row_number,
        }
        is_dupe = rng.random() < duplicate_rate
        is_bad = rng.random() < malformed_rate
        is_late = rng.random() < delay_rate
        shift = rng.randint(1, max_delay_positions) if is_late else 0

        entry = {"row": r, "duplicate": is_dupe, "malformed": is_bad}
        if is_bad:
            entry["corruption"] = rng.choice(CORRUPTION_MODES)
            entry["invalid_field"] = rng.choice(INVALID_FIELD_MODES)
            entry["garbage"] = bytes(rng.randrange(256) for _ in range(24))
            entry["bogus_schema_id"] = rng.randrange(900000, 999999)

        if shift:
            deferred.setdefault(i + shift, []).append(entry)
        else:
            pending.append((i, entry))

    out = []
    for i, entry in pending:
        out.append(entry)
        for late in deferred.pop(i, []):
            out.append(late)
    for remaining in sorted(deferred):
        out.extend(deferred[remaining])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--schema-registry", default="http://localhost:8081")
    ap.add_argument("--format", choices=["avro", "json"], default="avro")
    ap.add_argument("--limit", type=int, default=None, help="events to replay after sorting")
    ap.add_argument("--start-row", type=int, default=0)
    ap.add_argument("--rate", type=float, default=500.0, help="target events per second")
    ap.add_argument("--burst-rate", type=float, default=None)
    ap.add_argument("--burst-every", type=float, default=10.0, help="seconds between bursts")
    ap.add_argument("--burst-duration", type=float, default=2.0)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--duplicate-rate", type=float, default=0.0)
    ap.add_argument("--malformed-rate", type=float, default=0.0)
    ap.add_argument("--delay-rate", type=float, default=0.0)
    ap.add_argument("--max-delay-positions", type=int, default=50)
    ap.add_argument("--dry-run", metavar="PATH", default=None, help="write JSONL, skip Kafka")
    args = ap.parse_args()

    rng = random.Random(args.seed)

    print(f"loading source, limit={args.limit} start_row={args.start_row}")
    df = load_sorted(args.limit, args.start_row)
    print(f"replaying {len(df)} events sorted by (event_timestamp, source_row_number)")

    emission = plan(
        df,
        rng,
        args.duplicate_rate,
        args.malformed_rate,
        args.delay_rate,
        args.max_delay_positions,
    )

    stats = {
        "emitted": 0,
        "duplicates": 0,
        "malformed": 0,
        "malformed_emitted": 0,
        "by_topic": dict.fromkeys(TOPIC_BY_EVENT.values(), 0),
        "malformed_modes": {},
    }

    out_file = open(args.dry_run, "w") if args.dry_run else None
    producer = None
    serializer = None
    if not args.dry_run:
        if args.format == "avro":
            sr = SchemaRegistryClient({"url": args.schema_registry})
            schema_str = (SCHEMAS / "clickstream_event_v1.avsc").read_text()
            serializer = AvroSerializer(sr, schema_str, lambda rec, ctx: rec)
        producer = Producer(
            {
                "bootstrap.servers": args.bootstrap,
                "linger.ms": 20,
                "batch.num.messages": 10000,
                "compression.type": "snappy",
                "acks": "all",
                "retries": 5,
                "enable.idempotence": True,
            }
        )

    def encode(topic, record):
        if serializer is None:
            return json.dumps(record).encode("utf-8")
        return serializer(record, SerializationContext(topic, MessageField.VALUE))

    def emit(topic, key, payload):
        if out_file:
            out_file.write(
                json.dumps(
                    {
                        "topic": topic,
                        "key": key,
                        "value": payload.decode("utf-8", "replace"),
                    }
                )
                + "\n"
            )
        else:
            producer.produce(topic, key=key.encode("utf-8"), value=payload)
            producer.poll(0)

    start = time.time()
    for n, entry in enumerate(emission):
        row = entry["row"]
        topic = TOPIC_BY_EVENT[row["event"]]
        key = str(row["visitorid"])
        record = build_record(row, int(time.time() * 1000))

        if entry["malformed"]:
            mode = entry["corruption"]
            if mode == "invalid_field":
                bad = apply_invalid_field(record, entry["invalid_field"])
                payload = encode(topic, bad)
                mode = f"invalid_field:{entry['invalid_field']}"
            elif out_file:
                payload = json.dumps({"__corrupted__": mode}).encode("utf-8")
            else:
                payload = corrupt_bytes(
                    encode(topic, record), mode, entry["garbage"], entry["bogus_schema_id"]
                )
            stats["malformed"] += 1
            stats["malformed_emitted"] += 1
            stats["malformed_modes"][mode] = stats["malformed_modes"].get(mode, 0) + 1
        else:
            payload = encode(topic, record)

        emit(topic, key, payload)
        stats["emitted"] += 1
        stats["by_topic"][topic] += 1

        if entry["duplicate"]:
            emit(topic, key, payload)
            stats["emitted"] += 1
            stats["by_topic"][topic] += 1
            stats["duplicates"] += 1
            if entry["malformed"]:
                stats["malformed_emitted"] += 1

        if not args.dry_run:
            pace(n + 1, start, args)

    if producer:
        producer.flush(60)
    if out_file:
        out_file.close()

    elapsed = time.time() - start
    print(f"\nemitted {stats['emitted']} records in {elapsed:.2f}s")
    if elapsed > 0:
        print(f"effective rate {stats['emitted'] / elapsed:.1f} events/sec")
    print(f"by topic: {stats['by_topic']}")
    print(f"duplicates injected: {stats['duplicates']}")
    print(f"malformed injected: {stats['malformed']} {stats['malformed_modes']}")
    print(f"malformed records on the wire: {stats['malformed_emitted']}")
    if args.dry_run:
        print(f"wrote {args.dry_run}")
    return 0


def pace(count, start, args):
    """Sleep so the emitted count tracks the target rate, with optional bursts."""
    elapsed = time.time() - start
    rate = args.rate
    if args.burst_rate and (elapsed % args.burst_every) < args.burst_duration:
        rate = args.burst_rate
    drift = count / rate - elapsed
    if drift > 0:
        time.sleep(drift)


if __name__ == "__main__":
    sys.exit(main())
