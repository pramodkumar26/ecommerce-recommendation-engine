"""Turn the 18 item-property snapshots into a slowly changing dimension with validity intervals.

Phase 2 measured that item_properties is not a change log. Every one of its 20,275,902 rows
carries one of only 18 timestamps, all at 03:00:00 UTC, spaced 7 or 14 days apart. It is a
weekly snapshot export.

That makes point-in-time enrichment exact rather than approximate. Collapse consecutive
snapshots where a property did not change into a single interval, and the join becomes a range
lookup: find the row where item and property match and the event time falls inside
[valid_from, valid_to).

valid_from is the snapshot that first reported the value. valid_to is the next snapshot where it
changed, or NULL for the currently open interval. Intervals are half open so an event exactly on
a snapshot boundary belongs to the newer value, matching "latest known at or before t".

Critically, the FIRST interval for an item starts at its first snapshot, not at the beginning of
time. Events before that point have no known property and must stay unenriched. Backfilling them
from the first snapshot forward would be leakage, and Phase 2 measured that it would silently
affect 137,613 events.
"""

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

RAW = "/opt/spark/project/data/raw"
DELTA_ROOT = "/opt/spark/project/data/delta"
TRACKED_PROPERTIES = ["categoryid", "available"]


def build_session(app_name):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "24")
        .getOrCreate()
    )


def load_properties(spark):
    return (
        spark.read.option("header", True)
        .csv([f"{RAW}/item_properties_part1.csv", f"{RAW}/item_properties_part2.csv"])
        .select(
            F.col("timestamp").cast("long").alias("snapshot_ms"),
            F.col("itemid").cast("long").alias("item_id"),
            F.col("property").alias("property"),
            F.col("value").alias("value"),
        )
    )


def to_intervals(props):
    """Collapse repeated values across consecutive snapshots into half-open intervals."""
    w = Window.partitionBy("item_id", "property").orderBy("snapshot_ms")

    # A new interval starts wherever the value differs from the previous snapshot's value.
    changed = props.withColumn("prev_value", F.lag("value").over(w)).withColumn(
        "is_change",
        F.when(F.col("prev_value").isNull() | (F.col("prev_value") != F.col("value")), 1).otherwise(0),
    )
    grouped = changed.withColumn("interval_id", F.sum("is_change").over(w))

    intervals = grouped.groupBy("item_id", "property", "interval_id").agg(
        F.min("snapshot_ms").alias("valid_from_ms"),
        F.first("value").alias("value"),
    )

    nxt = Window.partitionBy("item_id", "property").orderBy("valid_from_ms")
    return (
        intervals.withColumn("valid_to_ms", F.lead("valid_from_ms").over(nxt))
        .select(
            "item_id",
            "property",
            "value",
            "valid_from_ms",
            "valid_to_ms",
            F.expr("timestamp_millis(valid_from_ms)").alias("valid_from"),
            F.expr("timestamp_millis(valid_to_ms)").alias("valid_to"),
            F.when(F.col("valid_to_ms").isNull(), True).otherwise(False).alias("is_current"),
        )
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-label", default="fixture")
    ap.add_argument("--properties", nargs="+", default=TRACKED_PROPERTIES)
    ap.add_argument("--out", default="/opt/spark/project/data/delta/item_scd_stats.json")
    args = ap.parse_args()

    spark = build_session("build-item-scd")
    spark.sparkContext.setLogLevel("ERROR")
    root = f"{DELTA_ROOT}/{args.run_label}"

    props = load_properties(spark)
    snapshots = sorted(r["snapshot_ms"] for r in props.select("snapshot_ms").distinct().collect())

    tracked = props.filter(F.col("property").isin(args.properties))
    scd = to_intervals(tracked).cache()

    scd.write.format("delta").mode("overwrite").partitionBy("property").save(f"{root}/dim_items_scd")

    # category_tree is small and static, no versioning needed
    categories = (
        spark.read.option("header", True)
        .csv(f"{RAW}/category_tree.csv")
        .select(
            F.col("categoryid").cast("long").alias("category_id"),
            F.col("parentid").cast("long").alias("parent_id"),
        )
    )
    categories.write.format("delta").mode("overwrite").save(f"{root}/dim_categories")

    by_property = {
        r["property"]: {"intervals": r["n"], "items": r["items"]}
        for r in scd.groupBy("property")
        .agg(F.count("*").alias("n"), F.countDistinct("item_id").alias("items"))
        .collect()
    }

    stats = {
        "distinct_snapshots": len(snapshots),
        "first_snapshot_ms": snapshots[0],
        "last_snapshot_ms": snapshots[-1],
        "snapshot_gaps_days": sorted(
            {round((snapshots[i + 1] - snapshots[i]) / 86_400_000, 3) for i in range(len(snapshots) - 1)}
        ),
        "property_rows_total": props.count(),
        "tracked_properties": args.properties,
        "scd_rows": scd.count(),
        "scd_by_property": by_property,
        "open_intervals": scd.filter(F.col("is_current")).count(),
        "categories": categories.count(),
        "compression_ratio": round(tracked.count() / max(scd.count(), 1), 3),
    }

    with open(args.out, "w") as f:
        json.dump(stats, f, indent=2)
    print(json.dumps(stats, indent=2))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
