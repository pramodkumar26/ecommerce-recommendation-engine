"""Run every phase verification in order and report a single verdict.

Phase 7 changed things that earlier phases depend on: failOnDataLoss now defaults to true,
Spark validates schema ids against the registry, idle detection watches batch ids, and a failed
query raises instead of exiting zero. Any of those could have broken an earlier guarantee, so
the whole suite runs before Phase 7 is called done.

Ordered cheapest first so a fast failure surfaces in seconds rather than after an hour.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from _reliability_lib import clear_run, env, produce, reset_topics, run, stream  # noqa: E402

PY = str(ROOT / ".venv" / "bin" / "python")


def spark_batch(script, extra=()):
    e = env()
    return run(
        [
            "docker", "compose", "exec", "-T", "spark-master",
            "/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077",
            "--driver-memory", "1g", "--executor-memory", "1600m",
            "--total-executor-cores", "6", "--packages", e["SPARK_PACKAGES"],
            f"/opt/spark/project/{script}", *extra,
        ],
        timeout=7200,
    )


def rebuild_phase5_fixture():
    """Phase 5's checks read the `fixture` tables, which other tests reset the topics under."""
    reset_topics()
    clear_run("fixture")
    produce(50000)
    stream("fixture", per_trigger=5000, await_seconds=2400)
    return "rebuilt"


SUITE = [
    ("phase7_validation_parity", "unit",
     lambda: run([PY, "-m", "pytest", "tests/test_validation_parity.py", "-q"])),
    ("phase7_timestamp_roundtrip", "7",
     lambda: spark_batch("batch/jobs/test_timestamp_roundtrip.py", ["--run-label", "silver"])),
    ("phase3_replay_determinism", "3", lambda: run([PY, "scripts/verify_replay.py"], timeout=7200)),
    ("phase4_schema_and_dlq", "4", lambda: run([PY, "scripts/verify_schema_dlq.py"], timeout=7200)),
    ("phase5_fixture_rebuild", "5", rebuild_phase5_fixture),
    ("phase5_streaming_correctness", "5",
     lambda: run([PY, "scripts/verify_streaming.py"], timeout=7200)),
    ("phase6_duplicates", "6", lambda: run([PY, "scripts/test_duplicates.py"], timeout=7200)),
    ("phase6_late_events", "6", lambda: run([PY, "scripts/test_late_events.py"], timeout=7200)),
    ("phase6_restart_recovery", "6", lambda: run([PY, "scripts/test_restart.py"], timeout=7200)),
    ("phase7_silver_rejects", "7", lambda: run([PY, "scripts/test_silver_rejects.py"], timeout=7200)),
    ("phase7_point_in_time", "7", lambda: run([PY, "scripts/verify_silver.py"], timeout=7200)),
    ("phase7_bounded_rebuild", "7",
     lambda: run([PY, "scripts/test_backfill_range.py"], timeout=7200)),
]

OUT = ROOT / "benchmarks" / "raw" / "regression_suite.json"


def main():
    only = sys.argv[1:] or None
    state = {
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "results": {},
    }

    for name, phase, fn in SUITE:
        if only and name not in only:
            continue
        print(f"\n=== {name} (phase {phase}) ===", flush=True)
        t0 = time.time()
        try:
            fn()
            status = "PASS"
            error = None
        except subprocess.TimeoutExpired:
            status = "TIMEOUT"
            error = "exceeded timeout"
        except Exception as e:
            status = "FAIL"
            error = str(e)[-1500:]
        elapsed = round(time.time() - t0, 1)
        state["results"][name] = {"phase": phase, "status": status, "seconds": elapsed}
        if error:
            state["results"][name]["error"] = error
        OUT.write_text(json.dumps(state, indent=2, default=str))
        print(f"--- {name}: {status} in {elapsed}s", flush=True)
        if status != "PASS":
            print(f"    {error}", flush=True)

    passed = sum(1 for r in state["results"].values() if r["status"] == "PASS")
    total = len(state["results"])
    state.update({
        "finished_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "passed": passed,
        "total": total,
        "all_green": passed == total,
        "total_seconds": round(sum(r["seconds"] for r in state["results"].values()), 1),
    })
    OUT.write_text(json.dumps(state, indent=2, default=str))

    print("\n" + "=" * 62)
    for name, r in state["results"].items():
        print(f"  {r['status']:8} phase {r['phase']:5} {name:34} {r['seconds']:>8.1f}s")
    print("=" * 62)
    print(f"{passed}/{total} passed in {state['total_seconds']}s")
    print(f"wrote {OUT}")
    return 0 if state["all_green"] else 1


if __name__ == "__main__":
    sys.exit(main())
