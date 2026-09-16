"""Shared helpers for the Phase 6 reliability tests."""

import json
import re
import subprocess
import time
from pathlib import Path

from confluent_kafka.admin import AdminClient

ROOT = Path(__file__).resolve().parents[1]
PY = str(ROOT / ".venv" / "bin" / "python")
BOOTSTRAP = "localhost:9092"
TOPICS = ["item_view", "add_to_cart", "transaction", "clickstream_dlq"]


def env():
    return dict(
        line.split("=", 1)
        for line in (ROOT / ".env").read_text().splitlines()
        if "=" in line and not line.startswith("#")
    )


def reset_topics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = [t for t in TOPICS if t in admin.list_topics(timeout=15).topics]
    if existing:
        for _, fut in admin.delete_topics(existing, operation_timeout=30).items():
            try:
                fut.result(timeout=30)
            except Exception:
                pass
    for _ in range(30):
        time.sleep(1)
        if not any(t in admin.list_topics(timeout=15).topics for t in TOPICS):
            break
    run([PY, str(ROOT / "kafka" / "create_topics.py")])
    run([PY, str(ROOT / "kafka" / "register_schemas.py")])


def run(cmd, timeout=3600):
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(" ".join(cmd[:3]) + "\n" + r.stdout[-3000:] + r.stderr[-3000:])
    return r.stdout


def produce(limit, seed=42, duplicate_rate=0.0, delay_rate=0.0, malformed_rate=0.0):
    out = run([
        PY, str(ROOT / "producer" / "simulator.py"),
        "--limit", str(limit), "--rate", "25000", "--seed", str(seed),
        "--duplicate-rate", str(duplicate_rate),
        "--delay-rate", str(delay_rate),
        "--malformed-rate", str(malformed_rate),
    ])
    modes_match = re.search(r"malformed injected: \d+ (\{.*\})", out)
    return {
        "emitted": int(re.search(r"emitted (\d+) records", out).group(1)),
        "duplicates": int(re.search(r"duplicates injected: (\d+)", out).group(1)),
        "malformed_on_wire": int(re.search(r"malformed records on the wire: (\d+)", out).group(1)),
        "modes": json.loads(modes_match.group(1).replace("'", '"')) if modes_match else {},
    }


def stream(run_label, extra=(), await_seconds=900, per_trigger=5000, background=False):
    e = env()
    cmd = [
        "docker", "compose", "exec", "-T", "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--driver-memory", "1g", "--executor-memory", "1600m", "--total-executor-cores", "6",
        "--packages", e["SPARK_PACKAGES"],
        "/opt/spark/project/streaming/jobs/stream_events.py",
        "--run-label", run_label,
        "--max-offsets-per-trigger", str(per_trigger),
        "--await-seconds", str(await_seconds),
        "--idle-seconds", "60",
        *extra,
    ]
    if background:
        return subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return run(cmd)


def export(run_label):
    e = env()
    out_path = f"/opt/spark/project/data/delta/export_{run_label}.json"
    run([
        "docker", "compose", "exec", "-T", "spark-master",
        "/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "900m",
        "--packages", e["SPARK_PACKAGES"],
        "/opt/spark/project/streaming/jobs/export_tables.py",
        "--run-label", run_label, "--out", out_path,
    ])
    return json.loads((ROOT / "data" / "delta" / f"export_{run_label}.json").read_text())


def clear_run(run_label):
    for p in (
        ROOT / "data" / "delta" / run_label,
        ROOT / "data" / "checkpoints" / run_label,
        ROOT / "data" / "delta" / f"export_{run_label}.json",
    ):
        subprocess.run(["rm", "-rf", str(p)], check=False)


def window_totals(export_json):
    m = export_json["metrics"]
    return {
        "windows": len(m),
        "events": sum(x["events"] for x in m),
        "views": sum(x["views"] for x in m),
        "carts": sum(x["carts"] for x in m),
        "transactions": sum(x["transactions"] for x in m),
    }
