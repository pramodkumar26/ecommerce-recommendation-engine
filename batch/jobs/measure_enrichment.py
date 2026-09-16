"""Measure point-in-time enrichment coverage, and validate the join against Phase 2.

Two different questions get measured, because conflating them caused a 9.5 point discrepancy
that looked like a bug and was not:

  ANY PROPERTY    does the item have any property record, of the 1,104 distinct names, at or
                  before the event? This is what Phase 2 measured with pandas in
                  DATASET-PITJOIN-001. It is used here purely as a CORRECTNESS CHECK: the Spark
                  implementation must reproduce the independently computed 14.266% exactly.

  CATEGORYID      does the item have a category at or before the event? This is the number that
                  actually matters, because category is what enrichment feeds to downstream
                  features. It is necessarily higher, since an item can have some property
                  recorded before it has a category.

Reporting only the first would flatter the pipeline. Reporting only the second would look like
a regression against Phase 2. Both are recorded, with the distinction stated.
"""

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW = "/opt/spark/project/data/raw"
DELTA_ROOT = "/opt/spark/project/data/delta"
PHASE2_MISS_RATE_PCT = 14.266
TOLERANCE_POINTS = 0.01


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-label", default="fulldata")
    ap.add_argument("--out", default="/opt/spark/project/data/delta/enrichment_measurement.json")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("measure-enrichment")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "24")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    root = f"{DELTA_ROOT}/{args.run_label}"

    raw_props = (
        spark.read.option("header", True)
        .csv([f"{RAW}/item_properties_part1.csv", f"{RAW}/item_properties_part2.csv"])
        .select(
            F.col("timestamp").cast("long").alias("ts"),
            F.col("itemid").cast("long").alias("item_id"),
            F.col("property").alias("property"),
        )
    )
    first_any = raw_props.groupBy("item_id").agg(F.min("ts").alias("first_any_ms"))
    first_cat = (
        raw_props.filter(F.col("property") == "categoryid")
        .groupBy("item_id")
        .agg(F.min("ts").alias("first_cat_ms"))
    )

    bronze = spark.read.format("delta").load(f"{root}/bronze_events").select(
        "event_id", "item_id", "event_timestamp"
    )
    total = bronze.count()

    j = bronze.join(first_any, "item_id", "left").join(first_cat, "item_id", "left").cache()

    def breakdown(first_col):
        no_prop = j.filter(F.col(first_col).isNull()).count()
        before = j.filter(
            F.col(first_col).isNotNull() & (F.col("event_timestamp") < F.col(first_col))
        ).count()
        joinable = total - no_prop - before
        return {
            "joinable": joinable,
            "item_has_no_such_property": no_prop,
            "event_precedes_first_occurrence": before,
            "miss_rate_pct": round(100 * (total - joinable) / total, 3),
        }

    any_prop = breakdown("first_any_ms")
    cat_prop = breakdown("first_cat_ms")

    # What Silver actually produced, as opposed to what the source says is possible.
    silver = spark.read.format("delta").load(f"{root}/silver_events")
    silver_rows = silver.count()
    silver_enriched = silver.filter(F.col("category_id").isNotNull()).count()
    silver_future_leaks = silver.filter(
        F.col("category_id").isNotNull()
        & (F.col("category_id_valid_from_ms") > F.col("event_timestamp"))
    ).count()

    phase2_delta = abs(any_prop["miss_rate_pct"] - PHASE2_MISS_RATE_PCT)

    checks = {
        "reproduces_phase2_any_property_rate": phase2_delta <= TOLERANCE_POINTS,
        "categoryid_rate_is_higher_as_expected": cat_prop["miss_rate_pct"] > any_prop["miss_rate_pct"],
        "same_items_lack_any_property": (
            any_prop["item_has_no_such_property"] == cat_prop["item_has_no_such_property"]
        ),
        "no_future_enrichment_in_silver": silver_future_leaks == 0,
        "silver_enrichment_matches_source_possibility": (
            abs(
                round(100 * (silver_rows - silver_enriched) / silver_rows, 3)
                - cat_prop["miss_rate_pct"]
            )
            <= 0.5
        ),
    }

    out = {
        "run_label": args.run_label,
        "total_events": total,
        "any_property": any_prop,
        "categoryid": cat_prop,
        "phase2_expected_any_property_pct": PHASE2_MISS_RATE_PCT,
        "difference_from_phase2_points": round(phase2_delta, 4),
        "events_with_a_property_but_no_category_yet": (
            cat_prop["event_precedes_first_occurrence"]
            - any_prop["event_precedes_first_occurrence"]
        ),
        "silver_rows": silver_rows,
        "silver_enriched": silver_enriched,
        "silver_miss_rate_pct": round(100 * (silver_rows - silver_enriched) / silver_rows, 3),
        "silver_rows_enriched_from_the_future": silver_future_leaks,
        "checks": checks,
        "passed": all(checks.values()),
    }

    with open(args.out, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str))
    print()
    for k, v in checks.items():
        print(f"  {'ok  ' if v else 'FAIL'} {k}")
    print("PASS" if out["passed"] else "FAIL")
    spark.stop()
    return 0 if out["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
