"""Exercise the Silver reject path with genuinely invalid records.

Until now the Silver rejection rules had never once fired: every fixture was clean, so
`rejected_rows` was 0 on every run. Rules that have never executed are not tested rules, and a
typo in the expression would have passed silently.

This produces records that decode as valid Avro but break the business contract, then asserts:

  1 they do not reach Silver
  2 they land in silver_rejected with the right category
  3 the categories match what the simulator deliberately injected
  4 valid records are unaffected

It also verifies the documented split between the two reject surfaces. The simulator's
raw_garbage and unknown_schema_id modes never decode, so they belong to the Kafka DLQ and the
Spark `undecodable` table, not to silver_rejected.
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from _reliability_lib import clear_run, env, produce, reset_topics, run, stream  # noqa: E402

LIMIT = 30000
MALFORMED_RATE = 0.06
LABEL = "rejects"

# The simulator's invalid_field modes map onto Silver rejection reasons like this.
MODE_TO_REASON = {
    "negative_visitor": "negative_visitor_id",
    "zero_timestamp": "event_timestamp_out_of_range",
    "orphan_transaction_id": "non_transaction_with_transaction_id",
}
# These never decode, so Silver never sees them at all.
WIRE_LEVEL_MODES = {"raw_garbage", "unknown_schema_id"}


def spark_batch(script, extra=()):
    e = env()
    return run(
        [
            "docker", "compose", "exec", "-T", "spark-master",
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "--driver-memory", "1g", "--executor-memory", "2g", "--total-executor-cores", "6",
            "--packages", e["SPARK_PACKAGES"],
            f"/opt/spark/project/{script}", *extra,
        ]
    )


def read_json(name):
    return json.loads((ROOT / "data" / "delta" / name).read_text())


def main():
    print("resetting topics and producing records that break the contract")
    reset_topics()
    clear_run(LABEL)
    prod = produce(LIMIT, malformed_rate=MALFORMED_RATE, duplicate_rate=0.0)
    print(f"produced: {prod}")

    modes = prod["modes"]
    expected_rejects = {}
    for mode, count in modes.items():
        if mode.startswith("invalid_field:"):
            which = mode.split(":", 1)[1]
            reason = MODE_TO_REASON[which]
            expected_rejects[reason] = expected_rejects.get(reason, 0) + count
    expected_undecodable = sum(v for k, v in modes.items() if k in WIRE_LEVEL_MODES)
    expected_invalid_total = sum(expected_rejects.values())

    print(f"\nexpected silver rejects by reason: {expected_rejects}")
    print(f"expected undecodable (wire level):  {expected_undecodable}")

    print("\nstreaming into bronze")
    stream(LABEL, per_trigger=6000, await_seconds=1200)

    print("building scd and silver")
    spark_batch("batch/jobs/build_item_scd.py", ["--run-label", LABEL])
    spark_batch("batch/jobs/build_silver.py", ["--run-label", LABEL])
    silver_stats = read_json("silver_stats.json")

    print(f"\nsilver stats: {json.dumps(silver_stats, indent=2)}")

    got_rejects = silver_stats["rejection_reasons"]
    bronze_rows = silver_stats["bronze_rows_total"]

    # Valid records are everything produced, minus the ones that never decoded, minus the ones
    # rejected for breaking the contract.
    expected_valid = prod["emitted"] - expected_undecodable - expected_invalid_total

    checks = {
        "bronze_excludes_undecodable": bronze_rows == prod["emitted"] - expected_undecodable,
        "silver_rejects_fired_at_all": silver_stats["rejected_rows"] > 0,
        "reject_count_matches_injected": silver_stats["rejected_rows"] == expected_invalid_total,
        "reject_categories_match": got_rejects == expected_rejects,
        "all_three_invalid_modes_present": len(got_rejects) == 3,
        "valid_rows_reach_silver": silver_stats["silver_rows"] == expected_valid,
        "no_valid_record_lost": silver_stats["silver_rows"] + silver_stats["rejected_rows"]
        == bronze_rows,
    }

    results = {
        "limit": LIMIT,
        "malformed_rate": MALFORMED_RATE,
        "produced": prod,
        "injected_modes": modes,
        "expected_silver_rejects": expected_rejects,
        "actual_silver_rejects": got_rejects,
        "expected_undecodable_wire_level": expected_undecodable,
        "bronze_rows": bronze_rows,
        "silver_rows": silver_stats["silver_rows"],
        "rejected_rows": silver_stats["rejected_rows"],
        "expected_valid_rows": expected_valid,
        "checks": checks,
        "passed": all(checks.values()),
    }

    out = ROOT / "benchmarks" / "raw" / "silver_rejects_test.json"
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
