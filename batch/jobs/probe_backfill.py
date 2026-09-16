"""Report, per partition, how far a newly added Gold column has propagated.

Used by the Phase 8 backfill test. A bounded backfill should populate the new column only
inside its range and leave every other partition NULL, which is what proves it was surgical
rather than a disguised full rebuild.
"""

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DELTA_ROOT = "/opt/spark/project/data/delta"
NEW_COLUMN = "category_known_since_ms"
AGE_COLUMN = "category_age_at_event_ms"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-label", default="silver")
    ap.add_argument("--out", default="/opt/spark/project/data/delta/backfill_probe.json")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("probe-backfill")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    root = f"{DELTA_ROOT}/{args.run_label}"

    fact = spark.read.format("delta").load(f"{root}/fact_events")
    silver = spark.read.format("delta").load(f"{root}/silver_events")
    columns = sorted(fact.columns)

    has_new = NEW_COLUMN in columns
    per_partition = (
        fact.groupBy("event_date")
        .agg(
            F.count("*").alias("rows"),
            (
                F.sum(F.when(F.col(NEW_COLUMN).isNotNull(), 1).otherwise(0))
                if has_new
                else F.lit(0)
            ).alias("rows_with_new_column"),
            # The provenance column is NULL exactly where there is no category, so the correct
            # comparison is against enriched rows, not against every row in the partition.
            F.sum(F.when(F.col("category_id").isNotNull(), 1).otherwise(0)).alias(
                "rows_with_category"
            ),
        )
        .orderBy("event_date")
        .collect()
    )

    age_stats = (
        fact.agg(F.min(AGE_COLUMN).alias("lo"), F.max(AGE_COLUMN).alias("hi")).first()
        if AGE_COLUMN in columns
        else None
    )

    out = {
        "run_label": args.run_label,
        "columns": columns,
        "expected_rows": silver.count(),
        "partitions": [
            {
                "event_date": r["event_date"].isoformat(),
                "rows": r["rows"],
                "rows_with_new_column": r["rows_with_new_column"],
                "rows_with_category": r["rows_with_category"],
            }
            for r in per_partition
        ],
        "min_category_age_ms": age_stats["lo"] if age_stats else None,
        "max_category_age_ms": age_stats["hi"] if age_stats else None,
    }
    with open(args.out, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
