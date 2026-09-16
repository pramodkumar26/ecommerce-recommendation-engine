"""Run the whole pipeline over the complete Retailrocket dataset and measure enrichment.

Purpose: reproduce the point-in-time enrichment miss rate from the real implementation and
compare it against DATASET-PITJOIN-001, the 14.27% measured independently with pandas in
Phase 2. Every fixture so far has been 50,000 to 100,000 events; this is 2,756,101.

The Phase 7 fixture reported 68.6%, which is NOT comparable: source rows 100,000 to 200,000 sit
early in the timeline where most items have no property snapshot yet. Only the full dataset is
comparable to the Phase 2 number.

If the result differs materially from 14.27%, the join is investigated, not adjusted.

Stages are timed and partial results are written after each one, so a run that dies halfway
still leaves evidence of how far it got and how long each stage took.
"""

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from _reliability_lib import clear_run, env, produce, reset_topics, run, stream  # noqa: E402

LABEL = "fulldata"
FULL_LIMIT = 2_756_101
PER_TRIGGER = 50_000
PHASE2_MISS_RATE_PCT = 14.266
TOLERANCE_PCT = 0.01

OUT = ROOT / "benchmarks" / "raw" / "full_enrichment_run.json"


def spark_batch(script, extra=(), executor_memory="1600m"):
    e = env()
    return run(
        [
            "docker", "compose", "exec", "-T", "spark-master",
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "--driver-memory", "1g", "--executor-memory", executor_memory,
            "--total-executor-cores", "6",
            "--packages", e["SPARK_PACKAGES"],
            f"/opt/spark/project/{script}", *extra,
        ],
        timeout=14400,
    )


def save(state):
    OUT.write_text(json.dumps(state, indent=2, default=str))


def main():
    state = {
        "label": LABEL,
        "full_limit": FULL_LIMIT,
        "per_trigger": PER_TRIGGER,
        "phase2_expected_miss_rate_pct": PHASE2_MISS_RATE_PCT,
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "stages": {},
    }

    def stage(name, fn):
        print(f"\n=== {name} ===", flush=True)
        t0 = time.time()
        try:
            result = fn()
        except Exception as e:
            state["stages"][name] = {"status": "FAILED", "seconds": round(time.time() - t0, 1),
                                     "error": str(e)[-2000:]}
            save(state)
            raise
        elapsed = round(time.time() - t0, 1)
        state["stages"][name] = {"status": "ok", "seconds": elapsed, "result": result}
        save(state)
        print(f"--- {name}: {elapsed}s", flush=True)
        return result

    stage("reset_topics", lambda: (reset_topics(), clear_run(LABEL), "reset")[-1])
    prod = stage("produce_full_dataset", lambda: produce(FULL_LIMIT))
    stage("stream_to_bronze", lambda: stream(
        LABEL, per_trigger=PER_TRIGGER, await_seconds=7200,
        extra=["--idle-seconds", "120"],
    ) and "streamed")
    stage("build_item_scd", lambda: spark_batch(
        "batch/jobs/build_item_scd.py", ["--run-label", LABEL]) and "built")
    stage("build_silver", lambda: spark_batch(
        "batch/jobs/build_silver.py", ["--run-label", LABEL]) and "built")

    stage("measure_enrichment", lambda: spark_batch(
        "batch/jobs/measure_enrichment.py", ["--run-label", LABEL]) and "measured")

    silver_stats = json.loads((ROOT / "data" / "delta" / "silver_stats.json").read_text())
    enrichment = json.loads(
        (ROOT / "data" / "delta" / "enrichment_measurement.json").read_text()
    )
    state["silver_stats"] = silver_stats
    state["enrichment"] = enrichment

    # The correctness check compares LIKE WITH LIKE. Phase 2 measured "does the item have ANY
    # property at or before the event", so that is what must reproduce 14.266%. Comparing the
    # categoryid-specific rate against it produced a 9.5 point gap that looked like a bug and
    # was purely a difference in the question being asked.
    measured = enrichment["any_property"]["miss_rate_pct"]
    delta = abs(measured - PHASE2_MISS_RATE_PCT)

    checks = {
        "all_stages_completed": all(s["status"] == "ok" for s in state["stages"].values()),
        "bronze_holds_expected_rows": silver_stats["bronze_rows_total"] == prod["emitted"],
        "no_rows_lost_through_silver": (
            silver_stats["silver_rows"] + silver_stats["rejected_rows"]
            == silver_stats["after_dedup"]
        ),
        "any_property_rate_reproduces_phase2": delta <= TOLERANCE_PCT,
        "no_future_enrichment": enrichment["silver_rows_enriched_from_the_future"] == 0,
        "enrichment_measurement_passed": enrichment["passed"],
    }

    state.update({
        "finished_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "any_property_miss_rate_pct": measured,
        "categoryid_miss_rate_pct": enrichment["categoryid"]["miss_rate_pct"],
        "difference_from_phase2_pct_points": round(delta, 3),
        "tolerance_pct_points": TOLERANCE_PCT,
        "total_seconds": round(sum(s["seconds"] for s in state["stages"].values()), 1),
        "checks": checks,
        "passed": all(checks.values()),
    })
    save(state)

    print()
    print(json.dumps({k: v for k, v in state.items() if k != "stages"}, indent=2, default=str))
    print("\nstage timings:")
    for name, s in state["stages"].items():
        print(f"  {name:24} {s['status']:8} {s['seconds']:>8.1f}s")
    print()
    for k, v in checks.items():
        print(f"  {'ok  ' if v else 'FAIL'} {k}")
    print(f"\nany-property miss {measured}% vs Phase 2 {PHASE2_MISS_RATE_PCT}%, "
          f"difference {round(delta, 3)} points")
    print(f"categoryid miss {enrichment['categoryid']['miss_rate_pct']}% "
          f"({enrichment['events_with_a_property_but_no_category_yet']} events have a property "
          f"but no category yet)")
    print(f"wrote {OUT}")
    print("PASS" if state["passed"] else "FAIL")
    return 0 if state["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
