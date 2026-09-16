"""Phase 8 definition of done: a transformation change, reprocessed from Bronze, verified.

Changing a rule and rerunning is easy. Proving the rerun did exactly what it should, and
nothing else, is the part worth testing:

  1 the changed rule is visible in the backfilled range
  2 partitions OUTSIDE the range are untouched by the change
  3 row counts are unchanged, a backfill must not invent or lose records
  4 a full rebuild propagates the change everywhere and converges
  5 Bronze was never written

Point 2 is the one that matters. A backfill that quietly rewrites the whole table is not a
backfill, it is a full rebuild with extra steps, and it would pass a naive "did the new column
appear" check.
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from _reliability_lib import env, run  # noqa: E402

LABEL = "silver"
RANGE_START = "2015-05-10"
RANGE_END = "2015-05-12"
NEW_COLUMNS = ["category_known_since_ms", "category_age_at_event_ms"]


def spark_batch(script, extra):
    e = env()
    return run(
        [
            "docker", "compose", "exec", "-T", "spark-master",
            "/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077",
            "--driver-memory", "1g", "--executor-memory", "1600m",
            "--total-executor-cores", "6", "--packages", e["SPARK_PACKAGES"],
            f"/opt/spark/project/batch/jobs/{script}", *extra,
        ],
        timeout=7200,
    )


def probe():
    """Per-partition view of how far the new columns have propagated."""
    spark_batch("probe_backfill.py", ["--run-label", LABEL])
    return json.loads((ROOT / "data" / "delta" / "backfill_probe.json").read_text())


def bronze_state():
    spark_batch("fingerprint_lakehouse.py", ["--run-label", LABEL,
                                             "--out", "/opt/spark/project/data/delta/fp_bronze.json"])
    fp = json.loads((ROOT / "data" / "delta" / "fp_bronze.json").read_text())
    return fp["tables"].get("bronze_events")


def main():
    print("bronze state before anything")
    bronze_before = bronze_state()

    # Recreate the pre-change state so the demonstration is repeatable. Without this the test
    # only passes on a table that happens to be mid-migration, which is not a test.
    print("\nresetting fact_events to the pre-change schema")
    spark_batch("build_gold.py", ["--run-label", LABEL, "--legacy-schema"])
    before_change = probe()
    print(f"  columns now: {[c for c in NEW_COLUMNS if c in before_change['columns']] or 'neither'}")

    print(f"\nbackfilling {RANGE_START} to {RANGE_END} with the changed rule")
    run([str(ROOT / ".venv" / "bin" / "python"), str(ROOT / "scripts" / "backfill.py"),
         "--run-label", LABEL, "--start-date", RANGE_START, "--end-date", RANGE_END],
        timeout=7200)

    print("\nprobing after the bounded backfill")
    after_backfill = probe()

    in_range = [p for p in after_backfill["partitions"] if RANGE_START <= p["event_date"] < RANGE_END]
    out_range = [p for p in after_backfill["partitions"]
                 if not (RANGE_START <= p["event_date"] < RANGE_END)]

    # The provenance column is NULL exactly where there is no category, by design. So a
    # correctly backfilled partition has the column set on every ENRICHED row, not on every row.
    backfilled_populated = [
        p for p in in_range if p["rows_with_new_column"] == p["rows_with_category"] > 0
    ]
    untouched_still_null = [p for p in out_range if p["rows_with_new_column"] == 0]

    print(f"  in range      : {[p['event_date'] for p in in_range]}")
    print(f"  out of range  : {[p['event_date'] for p in out_range]}")
    print(f"  populated     : {len(backfilled_populated)}/{len(in_range)}")
    print(f"  still null    : {len(untouched_still_null)}/{len(out_range)}")

    rows_after_backfill = sum(p["rows"] for p in after_backfill["partitions"])

    print("\nfull rebuild to propagate the change everywhere")
    spark_batch("build_gold.py", ["--run-label", LABEL])
    after_full = probe()

    all_populated = [
        p for p in after_full["partitions"]
        if p["rows_with_new_column"] == p["rows_with_category"]
    ]
    rows_after_full = sum(p["rows"] for p in after_full["partitions"])

    print("\nbronze state after everything")
    bronze_after = bronze_state()

    checks = {
        "pre_change_state_had_no_new_columns": not any(
            c in before_change["columns"] for c in NEW_COLUMNS
        ),
        "new_columns_exist_after_backfill": all(
            c in after_backfill["columns"] for c in NEW_COLUMNS
        ),
        "backfilled_partitions_populated": len(backfilled_populated) == len(in_range) > 0,
        "untouched_partitions_still_null": len(untouched_still_null) == len(out_range) > 0,
        "backfill_did_not_change_row_counts": rows_after_backfill == after_backfill["expected_rows"],
        "full_rebuild_propagates_everywhere": len(all_populated) == len(after_full["partitions"]),
        "full_rebuild_preserves_row_counts": rows_after_full == rows_after_backfill,
        "bronze_never_written": bronze_before == bronze_after,
        "values_are_sane": after_full["min_category_age_ms"] is None
        or after_full["min_category_age_ms"] >= 0,
    }

    results = {
        "run_label": LABEL,
        "backfill_range": f"{RANGE_START} to {RANGE_END} (exclusive)",
        "new_columns": NEW_COLUMNS,
        "columns_before_change": before_change["columns"],
        "partitions_in_range": [p["event_date"] for p in in_range],
        "partitions_out_of_range": [p["event_date"] for p in out_range],
        "after_backfill": {
            "partitions_fully_populated": len(backfilled_populated),
            "partitions_still_null": len(untouched_still_null),
            "total_rows": rows_after_backfill,
        },
        "after_full_rebuild": {
            "partitions_fully_populated": len(all_populated),
            "total_partitions": len(after_full["partitions"]),
            "total_rows": rows_after_full,
        },
        "min_category_age_ms": after_full["min_category_age_ms"],
        "max_category_age_ms": after_full["max_category_age_ms"],
        "bronze_rows": bronze_before["rows"] if bronze_before else None,
        "bronze_unchanged": bronze_before == bronze_after,
        "checks": checks,
        "passed": all(checks.values()),
    }

    out = ROOT / "benchmarks" / "raw" / "backfill_transformation_test.json"
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
