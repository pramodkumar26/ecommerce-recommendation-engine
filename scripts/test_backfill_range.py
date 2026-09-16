"""Phase 7: Bronze can regenerate Silver for a bounded range, surgically.

Rebuilds two days of Silver from Bronze while leaving the other five untouched. Proves two
things at once: the rebuild reproduces identical content (so Silver is a pure function of
Bronze plus the SCD), and it does not disturb partitions outside the requested range.

Delta's replaceWhere is what makes the second part true. Without it, mode("overwrite") would
replace the entire table.
"""

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from _reliability_lib import env  # noqa: E402

RUN_LABEL = "silver"
REBUILD_START = "2015-05-10"
REBUILD_END = "2015-05-12"


def spark_submit(script, extra=(), master="local[2]", memory="900m"):
    e = env()
    r = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "spark-master",
            "/opt/spark/bin/spark-submit", "--master", master, "--driver-memory", memory,
            "--packages", e["SPARK_PACKAGES"], f"/opt/spark/project/{script}", *extra,
        ],
        capture_output=True, text=True, cwd=ROOT,
    )
    if r.returncode != 0:
        raise RuntimeError(r.stdout[-3000:] + r.stderr[-3000:])
    return r.stdout


def fingerprint():
    spark_submit("streaming/jobs/silver_fingerprint.py", ["--run-label", RUN_LABEL])
    return json.loads((ROOT / "data" / "delta" / "silver_fingerprint.json").read_text())


def main():
    print("fingerprinting silver before the rebuild")
    before = fingerprint()
    print(f"  version {before['delta_version']}, {before['total_rows']} rows, "
          f"{len(before['partitions'])} partitions")

    print(f"\nrebuilding only {REBUILD_START} to {REBUILD_END} from bronze")
    spark_submit(
        "batch/jobs/build_silver.py",
        ["--run-label", RUN_LABEL, "--start-date", REBUILD_START, "--end-date", REBUILD_END],
        master="spark://spark-master:7077", memory="1g",
    )

    print("fingerprinting silver after the rebuild")
    after = fingerprint()
    print(f"  version {after['delta_version']}, {after['total_rows']} rows, "
          f"{len(after['partitions'])} partitions")

    in_range = [d for d in before["partitions"] if REBUILD_START <= d < REBUILD_END]
    out_of_range = [d for d in before["partitions"] if d not in in_range]

    rebuilt_identical = [
        d for d in in_range
        if d in after["partitions"] and after["partitions"][d] == before["partitions"][d]
    ]
    untouched_identical = [
        d for d in out_of_range
        if d in after["partitions"] and after["partitions"][d] == before["partitions"][d]
    ]

    checks = {
        "rebuild_targeted_some_partitions": len(in_range) > 0,
        "partitions_outside_range_exist": len(out_of_range) > 0,
        "rebuilt_partitions_reproduce_identically": len(rebuilt_identical) == len(in_range),
        "partitions_outside_range_untouched": len(untouched_identical) == len(out_of_range),
        "total_row_count_unchanged": after["total_rows"] == before["total_rows"],
        "partition_set_unchanged": set(after["partitions"]) == set(before["partitions"]),
        "delta_version_advanced": after["delta_version"] > before["delta_version"],
    }

    results = {
        "run_label": RUN_LABEL,
        "rebuild_range": f"{REBUILD_START} to {REBUILD_END} (exclusive)",
        "partitions_in_range": in_range,
        "partitions_out_of_range": out_of_range,
        "rows_before": before["total_rows"],
        "rows_after": after["total_rows"],
        "delta_version_before": before["delta_version"],
        "delta_version_after": after["delta_version"],
        "last_operation": after["last_operation"],
        "rebuilt_partitions_identical": len(rebuilt_identical),
        "untouched_partitions_identical": len(untouched_identical),
        "checks": checks,
        "passed": all(checks.values()),
    }

    out = ROOT / "benchmarks" / "raw" / "backfill_range_test.json"
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
