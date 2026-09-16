"""Prove the lakehouse survives a full service restart and rebuilds deterministically.

Two distinct claims, tested together because they share a restart:

  PERSISTENCE   Delta tables written before `docker compose down` are still there, byte for
                byte, after `docker compose up`. If Delta lived on a container filesystem this
                would silently fail and nobody would notice until a restart lost a day of work.

  DETERMINISM   Re-running the batch transformations after the restart reproduces identical
                Silver and Gold. Silver is a pure function of Bronze plus the item SCD, and Gold
                a pure function of Silver, so no hidden state may leak in.

Fingerprints are order-independent xor-style sums over per-row hashes, so a rebuild that
produces the same content in a different file layout still compares equal. Comparing row counts
alone would pass even if every value changed.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from _reliability_lib import env, run  # noqa: E402

LABEL = "silver"
FINGERPRINT_OUT = ROOT / "data" / "delta" / "lakehouse_fingerprint.json"


def spark_batch(script, extra=()):
    e = env()
    return run(
        [
            "docker", "compose", "exec", "-T", "spark-master",
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "--driver-memory", "1g", "--executor-memory", "1600m",
            "--total-executor-cores", "6",
            "--packages", e["SPARK_PACKAGES"],
            f"/opt/spark/project/{script}", *extra,
        ],
        timeout=7200,
    )


def fingerprint():
    spark_batch("batch/jobs/fingerprint_lakehouse.py", ["--run-label", LABEL])
    return json.loads(FINGERPRINT_OUT.read_text())


def compose(*args, timeout=600):
    return subprocess.run(
        ["docker", "compose", *args], cwd=ROOT, capture_output=True, text=True, timeout=timeout
    )


def wait_healthy(timeout=420):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Health.Status}}", "schema-registry"],
            capture_output=True, text=True,
        )
        if r.stdout.strip() == "healthy":
            # the worker has no healthcheck, give it a moment to register with the master
            time.sleep(15)
            return True
        time.sleep(5)
    return False


def main():
    print("fingerprinting the lakehouse before restart")
    before = fingerprint()
    print(f"  {len(before['tables'])} tables, {sum(t['rows'] for t in before['tables'].values())} rows")

    print("\nbringing the whole stack down")
    down = compose("--profile", "streaming", "down")
    if down.returncode != 0:
        raise RuntimeError(down.stderr[-2000:])

    running = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True
    ).stdout.split()
    project_running = [n for n in running if n in
                       {"kafka", "redis", "schema-registry", "spark-master", "spark-worker"}]

    # Delta lives on a bind mount, so the files must still be on the host with everything down.
    delta_dir = ROOT / "data" / "delta" / LABEL
    tables_on_disk_while_down = sorted(p.name for p in delta_dir.iterdir() if p.is_dir())

    print(f"  containers still running: {project_running or 'none'}")
    print(f"  tables present on host while down: {len(tables_on_disk_while_down)}")

    print("\nbringing the stack back up")
    up = compose("--profile", "streaming", "up", "-d")
    if up.returncode != 0:
        raise RuntimeError(up.stderr[-2000:])
    if not wait_healthy():
        raise RuntimeError("stack did not become healthy after restart")

    print("fingerprinting after restart, before any rebuild")
    after_restart = fingerprint()

    print("\nrebuilding silver and gold from scratch")
    spark_batch("batch/jobs/build_silver.py", ["--run-label", LABEL])
    spark_batch("batch/jobs/build_gold.py", ["--run-label", LABEL])

    print("fingerprinting after rebuild")
    after_rebuild = fingerprint()

    def same(a, b, table):
        return a["tables"].get(table) == b["tables"].get(table)

    persisted = [t for t in before["tables"] if same(before, after_restart, t)]
    rebuilt_same = [t for t in before["tables"] if same(before, after_rebuild, t)]
    changed_by_rebuild = [t for t in before["tables"] if not same(before, after_rebuild, t)]

    gold_tables = [t for t in before["tables"] if t.startswith(("fact_", "mart_"))]

    checks = {
        "all_containers_stopped": len(project_running) == 0,
        "delta_files_survive_on_host": len(tables_on_disk_while_down) > 0,
        "every_table_identical_after_restart": len(persisted) == len(before["tables"]),
        "rebuild_reproduces_every_table": len(rebuilt_same) == len(before["tables"]),
        "gold_tables_present": len(gold_tables) > 0,
        "row_counts_unchanged": (
            sum(t["rows"] for t in before["tables"].values())
            == sum(t["rows"] for t in after_rebuild["tables"].values())
        ),
    }

    results = {
        "run_label": LABEL,
        "tables": sorted(before["tables"]),
        "rows_before": {k: v["rows"] for k, v in before["tables"].items()},
        "rows_after_rebuild": {k: v["rows"] for k, v in after_rebuild["tables"].items()},
        "containers_running_while_down": project_running,
        "tables_on_host_while_down": tables_on_disk_while_down,
        "tables_identical_after_restart": sorted(persisted),
        "tables_reproduced_by_rebuild": sorted(rebuilt_same),
        "tables_changed_by_rebuild": sorted(changed_by_rebuild),
        "checks": checks,
        "passed": all(checks.values()),
    }

    out = ROOT / "benchmarks" / "raw" / "restart_persistence_test.json"
    out.write_text(json.dumps(results, indent=2, default=str))
    print()
    print(json.dumps({k: v for k, v in results.items() if k != "checks"}, indent=2, default=str))
    print()
    for k, v in checks.items():
        print(f"  {'ok  ' if v else 'FAIL'} {k}")
    print(f"\nwrote {out}")
    print("PASS" if results["passed"] else "FAIL")
    return 0 if results["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
