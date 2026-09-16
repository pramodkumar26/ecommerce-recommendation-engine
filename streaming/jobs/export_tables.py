"""Dump Phase 5 Delta output to JSON so an independent checker can compare it.

Kept deliberately dumb. All it does is read and write; the judging happens in
scripts/verify_streaming.py using pandas over the original CSV, so Spark is not grading itself.
"""

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DELTA_ROOT = "/opt/spark/project/data/delta"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-label", default="fixture")
    ap.add_argument("--out", default="/opt/spark/project/data/delta/export.json")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("export-tables")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    root = f"{DELTA_ROOT}/{args.run_label}"

    bronze = spark.read.format("delta").load(f"{root}/bronze_events")

    # A table may legitimately not exist yet, for example when exporting mid-stream after a
    # deliberate kill in the restart test. Missing is reported as empty, not as a crash.
    try:
        metrics = spark.read.format("delta").load(f"{root}/metrics_5min")
        metrics_missing = False
    except Exception:
        metrics = None
        metrics_missing = True
    try:
        deduped = spark.read.format("delta").load(f"{root}/deduped_events")
        deduped_rows = deduped.count()
        deduped_distinct = deduped.select("event_id").distinct().count()
    except Exception:
        deduped_rows = None
        deduped_distinct = None

    exact = (
        bronze.groupBy(F.window("event_time", "5 minutes").alias("w"))
        .agg(
            F.countDistinct("visitor_id").alias("unique_visitors_exact"),
            F.countDistinct("item_id").alias("unique_items_exact"),
        )
        .select(
            F.unix_timestamp("w.start").alias("window_start_s"),
            "unique_visitors_exact",
            "unique_items_exact",
        )
    )

    try:
        undecodable = spark.read.format("delta").load(f"{root}/undecodable").count()
    except Exception:
        undecodable = 0

    out = {
        "bronze_rows": bronze.count(),
        "bronze_distinct_event_ids": bronze.select("event_id").distinct().count(),
        "bronze_event_time_min_ms": bronze.agg(F.min("event_timestamp")).first()[0],
        "bronze_event_time_max_ms": bronze.agg(F.max("event_timestamp")).first()[0],
        "bronze_by_type": {
            r["event_type"]: r["n"]
            for r in bronze.groupBy("event_type").agg(F.count("*").alias("n")).collect()
        },
        "bronze_partitions": [
            r["event_date"].isoformat()
            for r in bronze.select("event_date").distinct().orderBy("event_date").collect()
        ],
        "undecodable_rows": undecodable,
        "deduped_rows": deduped_rows,
        "deduped_distinct_event_ids": deduped_distinct,
        "metrics_missing": metrics_missing,
        "metrics_rows": 0 if metrics_missing else metrics.count(),
        "metrics": []
        if metrics_missing
        else [
            {
                "window_start_s": int(r["window_start"].timestamp()),
                "events": r["events"],
                "views": r["views"],
                "carts": r["carts"],
                "transactions": r["transactions"],
                "unique_visitors_approx": r["unique_visitors_approx"],
                "unique_items_approx": r["unique_items_approx"],
                "view_to_cart_rate": r["view_to_cart_rate"],
            }
            for r in metrics.orderBy("window_start").collect()
        ],
        "exact_distincts": [
            {
                "window_start_s": int(r["window_start_s"]),
                "unique_visitors_exact": r["unique_visitors_exact"],
                "unique_items_exact": r["unique_items_exact"],
            }
            for r in exact.collect()
        ],
    }

    with open(args.out, "w") as f:
        json.dump(out, f, default=str)
    print(f"wrote {args.out}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
