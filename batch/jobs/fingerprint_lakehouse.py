"""Fingerprint every lakehouse table so restarts and rebuilds can be compared exactly.

Row counts alone would pass even if every value in every row changed. The fingerprint is a sum
over per-row hashes, which is order-independent: a rebuild that writes the same content into a
different number of files still compares equal, while any content change does not.
"""

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DELTA_ROOT = "/opt/spark/project/data/delta"

TABLES = [
    "bronze_events",
    "silver_events",
    "silver_rejected",
    "dim_items_scd",
    "dim_categories",
    "fact_events",
    "fact_transactions",
    "mart_item_funnel",
    "mart_category_performance",
    "mart_daily_item_metrics",
]

# Columns excluded from the hash because they legitimately change on a rebuild without meaning
# the data changed. Nothing about the event itself lives here.
VOLATILE = {"observed_at", "rejection_timestamp"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-label", default="silver")
    ap.add_argument("--out", default="/opt/spark/project/data/delta/lakehouse_fingerprint.json")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("fingerprint-lakehouse")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    root = f"{DELTA_ROOT}/{args.run_label}"

    out = {"run_label": args.run_label, "tables": {}}
    for table in TABLES:
        try:
            df = spark.read.format("delta").load(f"{root}/{table}")
        except Exception:
            continue
        cols = [c for c in sorted(df.columns) if c not in VOLATILE]
        hashed = df.withColumn(
            "_row_hash",
            F.xxhash64(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in cols])),
        )
        agg = hashed.agg(
            F.count("*").alias("rows"),
            F.sum(F.col("_row_hash").cast("decimal(38,0)")).alias("hash_sum"),
        ).first()
        out["tables"][table] = {
            "rows": int(agg["rows"]),
            "hash_sum": str(agg["hash_sum"]),
            "columns": cols,
        }

    with open(args.out, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps({k: {"rows": v["rows"]} for k, v in out["tables"].items()}, indent=2))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
