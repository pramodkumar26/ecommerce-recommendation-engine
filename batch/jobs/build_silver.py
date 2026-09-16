"""Bronze -> Silver: clean, deduplicate, and enrich without leaking the future.

Silver is where the actual data cleaning happens. Bronze is immutable raw and keeps everything,
including duplicates and invalid records, because Phase 8 replays from it.

Reads Bronze rather than the streaming deduped_events table on purpose. Silver must be fully
reproducible from Bronze alone, otherwise a backfill cannot rebuild it.

The point-in-time join is the part that matters. An event at time t is enriched with the item
property that was known at or before t, looked up from dim_items_scd validity intervals. Using
the item's final value would tell a model something that had not happened yet, and that kind of
leakage happens during enrichment rather than at the train/test split, which is where people
usually look for it.

Events with no property at or before their timestamp get NULL. They are never backfilled from a
later snapshot. Phase 2 measured that doing so would silently affect 137,613 events.
"""

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DELTA_ROOT = "/opt/spark/project/data/delta"

# Business rules, identical to kafka/validation.py. Phase 2 measured every one of these on the
# source rather than assuming it.
SOURCE_TS_MIN = 1430622004384
SOURCE_TS_MAX = 1442545187788
VALID_EVENT_TYPES = ["view", "addtocart", "transaction"]


def build_session(app_name):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "24")
        .getOrCreate()
    )


def rejection_reason(df):
    """One column naming why a row is unusable, or null if it is fine."""
    return (
        F.when(
            ~F.col("event_id").rlike("^[0-9a-f]{64}$"), F.lit("event_id_not_sha256")
        )
        .when(F.col("visitor_id").isNull() | (F.col("visitor_id") < 0), F.lit("negative_visitor_id"))
        .when(F.col("item_id").isNull() | (F.col("item_id") < 0), F.lit("negative_item_id"))
        .when(
            F.col("event_timestamp").isNull()
            | (F.col("event_timestamp") < SOURCE_TS_MIN)
            | (F.col("event_timestamp") > SOURCE_TS_MAX),
            F.lit("event_timestamp_out_of_range"),
        )
        .when(
            F.col("ingestion_timestamp").isNull() | (F.col("ingestion_timestamp") <= 0),
            F.lit("ingestion_timestamp_invalid"),
        )
        .when(~F.col("event_type").isin(VALID_EVENT_TYPES), F.lit("unknown_event_type"))
        .when(
            (F.col("event_type") == "transaction") & F.col("transaction_id").isNull(),
            F.lit("transaction_without_transaction_id"),
        )
        .when(
            (F.col("event_type") != "transaction") & F.col("transaction_id").isNotNull(),
            F.lit("non_transaction_with_transaction_id"),
        )
        .otherwise(F.lit(None))
    )


def point_in_time_join(events, scd, property_name, out_column):
    """Left join on the interval containing the event time.

    Half-open [valid_from, valid_to) so an event exactly on a snapshot boundary takes the newer
    value, which is what "latest known at or before t" means. Left join so an event with no
    known property survives with NULL rather than being dropped.
    """
    prop = (
        scd.filter(F.col("property") == property_name)
        .select(
            F.col("item_id").alias("_scd_item_id"),
            F.col("value").alias(out_column),
            F.col("valid_from_ms").alias("_valid_from_ms"),
            F.col("valid_to_ms").alias("_valid_to_ms"),
        )
    )
    joined = events.join(
        prop,
        (events["item_id"] == prop["_scd_item_id"])
        & (events["event_timestamp"] >= prop["_valid_from_ms"])
        & (prop["_valid_to_ms"].isNull() | (events["event_timestamp"] < prop["_valid_to_ms"])),
        "left",
    )
    return joined.withColumnRenamed("_valid_from_ms", f"{out_column}_valid_from_ms").drop(
        "_scd_item_id", "_valid_to_ms"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-label", default="fixture")
    ap.add_argument("--start-date", default=None, help="inclusive, YYYY-MM-DD, for bounded rebuild")
    ap.add_argument("--end-date", default=None, help="exclusive, YYYY-MM-DD")
    ap.add_argument("--out", default="/opt/spark/project/data/delta/silver_stats.json")
    args = ap.parse_args()

    spark = build_session("build-silver")
    spark.sparkContext.setLogLevel("ERROR")
    root = f"{DELTA_ROOT}/{args.run_label}"

    bronze = spark.read.format("delta").load(f"{root}/bronze_events")
    bronze_total = bronze.count()

    bounded = args.start_date is not None
    if bounded:
        bronze = bronze.filter(F.col("event_date") >= F.lit(args.start_date))
        if args.end_date:
            bronze = bronze.filter(F.col("event_date") < F.lit(args.end_date))
    in_scope = bronze.count()

    # Deduplicate in batch by event id. Bronze keeps replay duplicates; Silver must not.
    deduped = bronze.dropDuplicates(["event_id"])
    deduped_count = deduped.count()

    flagged = deduped.withColumn("rejection_reason", rejection_reason(deduped))
    rejected = flagged.filter(F.col("rejection_reason").isNotNull())
    rejected_count = rejected.count()
    clean = flagged.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")

    scd = spark.read.format("delta").load(f"{root}/dim_items_scd")
    categories = spark.read.format("delta").load(f"{root}/dim_categories")

    enriched = point_in_time_join(clean, scd, "categoryid", "category_id_raw")
    enriched = point_in_time_join(enriched, scd, "available", "available_raw")

    silver = (
        enriched.withColumn("category_id", F.col("category_id_raw").cast("long"))
        .withColumn("available", F.col("available_raw").cast("int"))
        .withColumnRenamed("category_id_raw_valid_from_ms", "category_id_valid_from_ms")
        .withColumnRenamed("available_raw_valid_from_ms", "available_valid_from_ms")
        .drop("category_id_raw", "available_raw")
        .join(
            categories.select(
                F.col("category_id").alias("_cat_id"), F.col("parent_id").alias("parent_category_id")
            ),
            F.col("category_id") == F.col("_cat_id"),
            "left",
        )
        .drop("_cat_id")
        .withColumn("has_category", F.col("category_id").isNotNull())
        .withColumn(
            "enrichment_status",
            F.when(F.col("category_id").isNotNull(), F.lit("enriched")).otherwise(
                F.lit("no_property_at_or_before_event")
            ),
        )
    )

    mode = "overwrite" if not bounded else "overwrite"
    writer = silver.write.format("delta").mode(mode).partitionBy("event_date")
    if bounded:
        # replaceWhere rewrites only the requested partitions, leaving the rest of Silver intact
        condition = f"event_date >= '{args.start_date}'"
        if args.end_date:
            condition += f" AND event_date < '{args.end_date}'"
        writer = writer.option("replaceWhere", condition)
    writer.save(f"{root}/silver_events")

    silver_count = silver.count()
    enriched_count = silver.filter(F.col("has_category")).count()

    rejected.write.format("delta").mode(mode).save(f"{root}/silver_rejected")

    stats = {
        "run_label": args.run_label,
        "bounded_rebuild": bounded,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "bronze_rows_total": bronze_total,
        "bronze_rows_in_scope": in_scope,
        "after_dedup": deduped_count,
        "duplicates_removed": in_scope - deduped_count,
        "rejected_rows": rejected_count,
        "rejection_reasons": {
            r["rejection_reason"]: r["n"]
            for r in rejected.groupBy("rejection_reason").agg(F.count("*").alias("n")).collect()
        },
        "silver_rows": silver_count,
        "enriched_rows": enriched_count,
        "unenriched_rows": silver_count - enriched_count,
        "join_miss_rate_pct": round(100 * (silver_count - enriched_count) / max(silver_count, 1), 3),
        "distinct_categories_seen": silver.filter(F.col("has_category"))
        .select("category_id")
        .distinct()
        .count(),
        "partitions": silver.select("event_date").distinct().count(),
    }

    with open(args.out, "w") as f:
        json.dump(stats, f, indent=2, default=str)
    print(json.dumps(stats, indent=2, default=str))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
