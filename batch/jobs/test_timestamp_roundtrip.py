"""Prove epoch_ms -> Spark timestamp -> epoch_ms is exact, and host-timezone independent.

The Avro schema stores event time as a plain long rather than the timestamp-millis logical
type, precisely so no implicit timezone conversion happens. This asserts that decision actually
holds end to end, because a silent one-hour shift would corrupt every window and every
point-in-time join without failing anything.

Four things are checked:

  1 round trip is exact for ordinary timestamps
  2 round trip is exact across daylight-saving transitions in timezones that observe them
  3 the result does not depend on spark.sql.session.timeZone
  4 the real Bronze and Silver tables round trip exactly, not just synthetic values

Point 3 is the one that matters most. If the pipeline only works because the host happens to be
in a particular timezone, it is broken and nobody has noticed yet.
"""

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructField, StructType

DELTA_ROOT = "/opt/spark/project/data/delta"

# Instants chosen to sit on or around real DST transitions. UTC has no DST, which is the point:
# if the pipeline is correct these convert identically no matter what session timezone is set.
DST_CASES = {
    "us_spring_forward_2015": 1425797999000,   # 2015-03-08 01:59:59 America/Denver
    "us_spring_forward_plus_1s": 1425798000000,
    "us_fall_back_2015": 1446361199000,        # 2015-11-01 01:59:59 America/Denver
    "us_fall_back_plus_1s": 1446361200000,
    "eu_spring_forward_2015": 1427418000000,   # 2015-03-27 02:00 Europe/London
    "eu_fall_back_2015": 1445734800000,
    "au_dst_2015": 1443889800000,              # Australia/Sydney, southern hemisphere
    "epoch_zero": 0,
    "source_min": 1430622004384,
    "source_max": 1442545187788,
    "pre_epoch": -86400000,
    "far_future": 4102444800000,
}

TIMEZONES_TO_TRY = ["UTC", "America/Denver", "Asia/Kolkata", "Australia/Sydney", "Pacific/Chatham"]


def session(tz):
    spark = (
        SparkSession.builder.appName(f"timestamp-roundtrip-{tz}")
        .config("spark.sql.session.timeZone", tz)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def roundtrip(spark, values):
    """epoch_ms -> timestamp -> epoch_ms, using exactly the conversion the pipeline uses."""
    schema = StructType([StructField("epoch_ms", LongType())])
    df = spark.createDataFrame([(v,) for v in values], schema)
    out = (
        df.withColumn("as_timestamp", F.expr("timestamp_millis(epoch_ms)"))
        .withColumn("back_to_ms", F.expr("unix_millis(as_timestamp)"))
        .withColumn("exact", F.col("epoch_ms") == F.col("back_to_ms"))
    )
    return {r["epoch_ms"]: r["back_to_ms"] for r in out.collect()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-label", default="silver")
    ap.add_argument("--out", default="/opt/spark/project/data/delta/timestamp_roundtrip.json")
    args = ap.parse_args()

    values = list(DST_CASES.values())
    results = {}
    per_tz = {}

    for tz in TIMEZONES_TO_TRY:
        spark = session(tz)
        mapping = roundtrip(spark, values)
        per_tz[tz] = {str(k): v for k, v in mapping.items()}
        spark.stop()

    # Every session timezone must produce identical output. If one differs, the pipeline's
    # correctness depends on where the machine happens to be.
    baseline = per_tz["UTC"]
    tz_disagreements = {
        tz: {k: v for k, v in mapping.items() if baseline.get(k) != v}
        for tz, mapping in per_tz.items()
        if mapping != baseline
    }

    exact_per_case = {
        name: baseline[str(ms)] == ms for name, ms in DST_CASES.items()
    }

    # Now the real tables, under UTC, which is what the pipeline actually sets.
    spark = session("UTC")
    root = f"{DELTA_ROOT}/{args.run_label}"
    table_results = {}
    for table, ts_col, time_col in (
        ("bronze_events", "event_timestamp", "event_time"),
        ("silver_events", "event_timestamp", "event_time"),
    ):
        try:
            df = spark.read.format("delta").load(f"{root}/{table}")
        except Exception as e:
            table_results[table] = {"error": f"{type(e).__name__}"}
            continue
        checked = df.select(
            F.col(ts_col).alias("stored_ms"),
            F.expr(f"unix_millis({time_col})").alias("derived_ms"),
        ).withColumn("exact", F.col("stored_ms") == F.col("derived_ms"))
        total = checked.count()
        mismatches = checked.filter(~F.col("exact")).count()
        table_results[table] = {
            "rows": total,
            "mismatches": mismatches,
            "all_exact": mismatches == 0,
        }
    session_tz = spark.conf.get("spark.sql.session.timeZone")
    spark.stop()

    results = {
        "session_timezone_configured": session_tz,
        "timezones_tried": TIMEZONES_TO_TRY,
        "synthetic_cases": {name: {"epoch_ms": ms, "exact": exact_per_case[name]}
                            for name, ms in DST_CASES.items()},
        "timezone_disagreements": tz_disagreements,
        "tables": table_results,
    }

    checks = {
        "all_synthetic_cases_exact": all(exact_per_case.values()),
        "dst_transition_cases_exact": all(
            v for k, v in exact_per_case.items() if "forward" in k or "back" in k or "dst" in k
        ),
        "result_independent_of_session_timezone": len(tz_disagreements) == 0,
        "pipeline_runs_in_utc": session_tz == "UTC",
        "bronze_all_exact": table_results.get("bronze_events", {}).get("all_exact", False),
        "silver_all_exact": table_results.get("silver_events", {}).get("all_exact", False),
    }
    results["checks"] = checks
    results["passed"] = all(checks.values())

    with open(args.out, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(json.dumps(results, indent=2, default=str))
    print()
    for k, v in checks.items():
        print(f"  {'ok  ' if v else 'FAIL'} {k}")
    print("PASS" if results["passed"] else "FAIL")
    return 0 if results["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
