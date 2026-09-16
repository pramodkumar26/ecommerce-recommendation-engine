"""Silver -> Gold: analytics marts.

Scope is deliberate. Roadmap section 15 lists user_features, item_features,
user_item_interactions, and recommendation_training_data as Gold candidates too, but those are
ML feature engineering and belong to Phase 11 where leakage rules get their own tests. This job
builds the analytics side only.

Every mart reads Silver, so every mart inherits the point-in-time enrichment and carries the
same null-category semantics rather than inventing its own.
"""

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DELTA_ROOT = "/opt/spark/project/data/delta"


def build_session(app_name):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "24")
        .getOrCreate()
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-label", default="silver")
    ap.add_argument("--out", default="/opt/spark/project/data/delta/gold_stats.json")
    args = ap.parse_args()

    spark = build_session("build-gold")
    spark.sparkContext.setLogLevel("ERROR")
    root = f"{DELTA_ROOT}/{args.run_label}"
    silver = spark.read.format("delta").load(f"{root}/silver_events").cache()

    # fact_events: the grain is one row per event, time partitioned like Silver
    fact_events = silver.select(
        "event_id", "visitor_id", "item_id", "event_type", "event_time", "event_timestamp",
        "category_id", "parent_category_id", "available", "enrichment_status", "event_date",
    )
    fact_events.write.format("delta").mode("overwrite").partitionBy("event_date").save(
        f"{root}/fact_events"
    )

    # fact_transactions: one row per transaction line. Phase 2 measured 1.27 items per
    # transaction id, so a transaction id is not unique here and must not be used as a key.
    fact_transactions = (
        silver.filter(F.col("event_type") == "transaction")
        .select(
            "event_id", "transaction_id", "visitor_id", "item_id", "event_time",
            "category_id", "parent_category_id", "event_date",
        )
    )
    fact_transactions.write.format("delta").mode("overwrite").partitionBy("event_date").save(
        f"{root}/fact_transactions"
    )

    # mart_item_funnel: view -> cart -> purchase per item, across the whole slice
    funnel = (
        silver.groupBy("item_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "addtocart", 1).otherwise(0)).alias("carts"),
            F.sum(F.when(F.col("event_type") == "transaction", 1).otherwise(0)).alias("purchases"),
            F.countDistinct("visitor_id").alias("unique_visitors"),
            F.max("category_id").alias("last_known_category_id"),
        )
        .withColumn(
            "view_to_cart_rate",
            F.when(F.col("views") > 0, F.col("carts") / F.col("views")),
        )
        .withColumn(
            "cart_to_purchase_rate",
            F.when(F.col("carts") > 0, F.col("purchases") / F.col("carts")),
        )
        .withColumn(
            "view_to_purchase_rate",
            F.when(F.col("views") > 0, F.col("purchases") / F.col("views")),
        )
    )
    funnel.write.format("delta").mode("overwrite").save(f"{root}/mart_item_funnel")

    # mart_category_performance: only enriched rows can be attributed to a category, and the
    # unattributed share is reported rather than hidden
    category_perf = (
        silver.filter(F.col("category_id").isNotNull())
        .groupBy("category_id", "parent_category_id")
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("item_id").alias("items"),
            F.countDistinct("visitor_id").alias("visitors"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "addtocart", 1).otherwise(0)).alias("carts"),
            F.sum(F.when(F.col("event_type") == "transaction", 1).otherwise(0)).alias("purchases"),
        )
    )
    category_perf.write.format("delta").mode("overwrite").save(f"{root}/mart_category_performance")

    # mart_daily_item_metrics: the grain Phase 11 will build item features from
    daily = (
        silver.groupBy("event_date", "item_id")
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("visitor_id").alias("unique_visitors"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "addtocart", 1).otherwise(0)).alias("carts"),
            F.sum(F.when(F.col("event_type") == "transaction", 1).otherwise(0)).alias("purchases"),
        )
    )
    daily.write.format("delta").mode("overwrite").partitionBy("event_date").save(
        f"{root}/mart_daily_item_metrics"
    )

    silver_rows = silver.count()
    attributed = silver.filter(F.col("category_id").isNotNull()).count()

    stats = {
        "run_label": args.run_label,
        "silver_rows": silver_rows,
        "fact_events_rows": fact_events.count(),
        "fact_transactions_rows": fact_transactions.count(),
        "distinct_transaction_ids": fact_transactions.select("transaction_id").distinct().count(),
        "mart_item_funnel_rows": funnel.count(),
        "mart_category_performance_rows": category_perf.count(),
        "mart_daily_item_metrics_rows": daily.count(),
        "events_attributable_to_a_category": attributed,
        "category_attribution_pct": round(100 * attributed / max(silver_rows, 1), 3),
        "items_with_a_purchase": funnel.filter(F.col("purchases") > 0).count(),
        "items_with_a_cart": funnel.filter(F.col("carts") > 0).count(),
    }
    with open(args.out, "w") as f:
        json.dump(stats, f, indent=2, default=str)
    print(json.dumps(stats, indent=2, default=str))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
