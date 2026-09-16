"""Dump Silver enrichment results for independent checking."""

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DELTA_ROOT = "/opt/spark/project/data/delta"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-label", default="silver")
    ap.add_argument("--out", default="/opt/spark/project/data/delta/silver_export.json")
    ap.add_argument("--sample-out", default="/opt/spark/project/data/delta/silver_sample")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("export-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    root = f"{DELTA_ROOT}/{args.run_label}"
    silver = spark.read.format("delta").load(f"{root}/silver_events")

    # The leakage assertion, evaluated in Spark over every row rather than on a sample.
    leaks = silver.filter(
        F.col("category_id").isNotNull()
        & (F.col("category_id_valid_from_ms") > F.col("event_timestamp"))
    ).count()

    # Full per-event enrichment result, written out for the pandas comparison.
    (
        silver.select("event_id", "item_id", "event_timestamp", "category_id")
        .repartition(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(args.sample_out)
    )

    stats = {
        "silver_rows": silver.count(),
        "enriched_rows": silver.filter(F.col("category_id").isNotNull()).count(),
        "rows_enriched_from_the_future": leaks,
        "min_enrichment_lag_ms": silver.filter(F.col("category_id").isNotNull())
        .agg(F.min(F.col("event_timestamp") - F.col("category_id_valid_from_ms")))
        .first()[0],
        "enrichment_status_counts": {
            r["enrichment_status"]: r["n"]
            for r in silver.groupBy("enrichment_status").agg(F.count("*").alias("n")).collect()
        },
        "partitions": sorted(
            r["event_date"].isoformat()
            for r in silver.select("event_date").distinct().collect()
        ),
        "rows_with_parent_category": silver.filter(F.col("parent_category_id").isNotNull()).count(),
    }
    with open(args.out, "w") as f:
        json.dump(stats, f, indent=2, default=str)
    print(json.dumps(stats, indent=2, default=str))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
