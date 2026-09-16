"""Phase 6 test A: kill Spark mid-stream, restart, prove nothing is lost or double counted.

This is the test the whole checkpoint design exists for, and it is where Kafka offsets and
Spark checkpoints become distinguishable.

Kafka retains the messages regardless. What the checkpoint holds is how far this query got and
what its stateful operators contained. Deleting the checkpoint would reprocess from the
configured startingOffsets; keeping it resumes exactly where the query stopped.

Procedure:
  1 produce a known range
  2 start the stream, let it commit some batches, then kill the driver mid-stream
  3 record what landed
  4 restart against the same checkpoint
  5 assert the remaining range was consumed, with no loss and no duplicate inflation
"""

import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _reliability_lib import ROOT, clear_run, export, produce, reset_topics, stream  # noqa: E402

LIMIT = 50000
LABEL = "restart"
PER_TRIGGER = 4000
KILL_AFTER_COMMITS = 4


def commits(query):
    d = ROOT / "data" / "checkpoints" / LABEL / query / "commits"
    if not d.exists():
        return 0
    return len([f for f in d.iterdir() if f.name.isdigit()])


def kill_spark_app():
    """Kill the driver the way a crash would, without a clean query.stop()."""
    subprocess.run(["pkill", "-f", "stream_events.py"], check=False)
    subprocess.run(
        ["docker", "compose", "exec", "-T", "spark-master", "pkill", "-f", "stream_events.py"],
        cwd=ROOT, check=False, capture_output=True,
    )


def wait_for_cluster_idle(timeout=180):
    import urllib.request

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            d = json.loads(urllib.request.urlopen("http://localhost:8080/json/", timeout=5).read())
            if not d.get("activeapps"):
                return True
        except Exception:
            pass
        time.sleep(3)
    return False


def main():
    print("resetting topics and producing a known range")
    reset_topics()
    clear_run(LABEL)
    prod = produce(LIMIT)
    print(f"produced: {prod}")

    print(f"\nstarting stream, will kill after {KILL_AFTER_COMMITS} raw commits")
    proc = stream(LABEL, per_trigger=PER_TRIGGER, await_seconds=1800, background=True)

    deadline = time.time() + 900
    while time.time() < deadline:
        if commits("raw") >= KILL_AFTER_COMMITS:
            break
        if proc.poll() is not None:
            raise RuntimeError("stream exited before the kill point")
        time.sleep(2)

    commits_before = {q: commits(q) for q in ("raw", "dedup", "metrics")}
    print(f"killing driver mid-stream at commits={commits_before}")
    kill_spark_app()
    proc.wait(timeout=120)
    wait_for_cluster_idle()

    partial = export(LABEL)
    print(f"after kill: bronze={partial['bronze_rows']} deduped={partial['deduped_rows']}")

    if partial["bronze_rows"] >= LIMIT:
        raise RuntimeError("kill happened too late, nothing left to resume")

    print("\nrestarting against the same checkpoint")
    stream(LABEL, per_trigger=PER_TRIGGER, await_seconds=1800)
    final = export(LABEL)

    commits_after = {q: commits(q) for q in ("raw", "dedup", "metrics")}
    final_events = sum(m["events"] for m in final["metrics"])

    checks = {
        "kill_happened_mid_stream": partial["bronze_rows"] < LIMIT,
        "restart_resumed_rather_than_restarted": commits_after["raw"] > commits_before["raw"],
        "all_events_present_after_restart": final["bronze_rows"] == LIMIT,
        "no_duplicate_inflation_in_bronze": final["bronze_distinct_event_ids"] == LIMIT,
        "deduped_table_exact": final["deduped_rows"] == LIMIT,
        "windowed_events_exact": final_events == LIMIT,
        "no_records_lost": final["bronze_rows"] >= partial["bronze_rows"],
    }

    results = {
        "limit": LIMIT,
        "per_trigger": PER_TRIGGER,
        "produced": prod,
        "commits_before_kill": commits_before,
        "commits_after_restart": commits_after,
        "bronze_rows_at_kill": partial["bronze_rows"],
        "deduped_rows_at_kill": partial["deduped_rows"],
        "events_remaining_at_kill": LIMIT - partial["bronze_rows"],
        "bronze_rows_final": final["bronze_rows"],
        "bronze_distinct_event_ids_final": final["bronze_distinct_event_ids"],
        "deduped_rows_final": final["deduped_rows"],
        "windowed_events_final": final_events,
        "windows_final": len(final["metrics"]),
        "checks": checks,
        "passed": all(checks.values()),
    }

    out = ROOT / "benchmarks" / "raw" / "restart_test.json"
    out.write_text(json.dumps(results, indent=2))
    print()
    print(json.dumps({k: v for k, v in results.items() if k != "checks"}, indent=2))
    print()
    for k, v in checks.items():
        print(f"  {'ok  ' if v else 'FAIL'} {k}")
    print(f"\nwrote {out}")
    print("PASS" if results["passed"] else "FAIL")
    return 0 if results["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
