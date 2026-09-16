"""Fingerprint Silver per partition so a bounded rebuild can be proven surgical."""

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DELTA_ROOT = "/opt/spark/project/data/delta"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-label", default="silver")
    ap.add_argument("--out", default="/opt/spark/project/data/delta/silver_fingerprint.json")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("silver-fingerprint")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    path = f"{DELTA_ROOT}/{args.run_label}/silver_events"

    silver = spark.read.format("delta").load(path)

    # Order-independent fingerprint: xor of per-row hashes, so a rewrite that preserves content
    # produces the same value regardless of file layout.
    per_partition = (
        silver.withColumn(
            "row_hash",
            F.xxhash64(
                F.concat_ws(
                    "|",
                    F.col("event_id"),
                    F.coalesce(F.col("category_id").cast("string"), F.lit("null")),
                    F.coalesce(F.col("available").cast("string"), F.lit("null")),
                    F.col("enrichment_status"),
                )
            ),
        )
        .groupBy("event_date")
        .agg(
            F.count("*").alias("rows"),
            F.sum(F.col("row_hash").cast("decimal(38,0)")).alias("hash_sum"),
            F.sum(F.when(F.col("category_id").isNotNull(), 1).otherwise(0)).alias("enriched"),
        )
        .orderBy("event_date")
    )

    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 5").collect()

    out = {
        "total_rows": silver.count(),
        "delta_version": int(history[0]["version"]),
        "last_operation": history[0]["operation"],
        "partitions": {
            r["event_date"].isoformat(): {
                "rows": r["rows"],
                "hash_sum": str(r["hash_sum"]),
                "enriched": r["enriched"],
            }
            for r in per_partition.collect()
        },
    }
    with open(args.out, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
