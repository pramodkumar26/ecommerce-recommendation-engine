"""Create the project topics from kafka/topics/topics.yml.

Idempotent. Existing topics are reported and left alone, because changing partition count on a
live topic would reshuffle key-to-partition mapping and break per-visitor ordering.
"""

import argparse
import sys
from pathlib import Path

import yaml
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

SPEC = Path(__file__).parent / "topics" / "topics.yml"


def load_spec():
    return yaml.safe_load(SPEC.read_text())["topics"]


def describe(admin, names):
    meta = admin.list_topics(timeout=15)
    out = {}
    for name in names:
        t = meta.topics.get(name)
        if t is None:
            continue
        out[name] = {
            "partitions": len(t.partitions),
            "replication_factor": len(next(iter(t.partitions.values())).replicas),
        }
    return out


def fetch_configs(admin, names):
    resources = [ConfigResource(ConfigResource.Type.TOPIC, n) for n in names]
    futures = admin.describe_configs(resources)
    out = {}
    for resource, fut in futures.items():
        entries = fut.result(timeout=15)
        out[resource.name] = {
            k: v.value for k, v in entries.items() if k in ("retention.ms", "cleanup.policy")
        }
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    spec = load_spec()
    admin = AdminClient({"bootstrap.servers": args.bootstrap})

    existing = describe(admin, [t["name"] for t in spec])
    to_create = [t for t in spec if t["name"] not in existing]

    for name, info in existing.items():
        print(f"exists   {name}  partitions={info['partitions']} rf={info['replication_factor']}")

    if not to_create:
        print("\nnothing to create")
    elif args.dry_run:
        for t in to_create:
            print(f"would create  {t['name']}  partitions={t['partitions']}")
    else:
        new = [
            NewTopic(
                t["name"],
                num_partitions=t["partitions"],
                replication_factor=t["replication_factor"],
                config={k: str(v) for k, v in t.get("config", {}).items()},
            )
            for t in to_create
        ]
        for name, fut in admin.create_topics(new).items():
            try:
                fut.result(timeout=30)
                print(f"created  {name}")
            except Exception as e:
                print(f"FAIL     {name}: {e}")
                return 1

    names = [t["name"] for t in spec]
    final = describe(admin, names)
    missing = [n for n in names if n not in final]
    if missing:
        print(f"\nFAIL missing topics: {missing}")
        return 1

    configs = fetch_configs(admin, names)
    print("\nfinal state")
    for t in spec:
        n = t["name"]
        f = final[n]
        c = configs.get(n, {})
        ok = f["partitions"] == t["partitions"]
        retention_days = int(c.get("retention.ms", 0)) / 86_400_000
        print(
            f"  {'ok  ' if ok else 'WARN'} {n:<18} partitions={f['partitions']} "
            f"rf={f['replication_factor']} retention={retention_days:g}d "
            f"cleanup={c.get('cleanup.policy')}"
        )
        if not ok:
            print(f"       expected {t['partitions']} partitions, found {f['partitions']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
