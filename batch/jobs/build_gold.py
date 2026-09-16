"""Silver -> Gold: analytics marts.

Scope is deliberate. Roadmap section 15 lists user_features, item_features,
user_item_interactions, and recommendation_training_data as Gold candidates too, but those are
ML feature engineering and belong to Phase 11 where leakage rules get their own tests. This job
builds the analytics side only.

Every mart reads Silver, so every mart inherits the point-in-time enrichment and carries the
same null-category semantics rather than inventing its own.

BOUNDED REBUILD, and where it stops working

Two kinds of table live here and they behave differently under a bounded backfill:

  time-grained    fact_events, fact_transactions, mart_daily_item_metrics. Partitioned by
                  event_date, so a date range can be rewritten with replaceWhere and the rest of
                  the table is untouched.

  global          mart_item_funnel, mart_category_performance. These aggregate across the whole
                  history with no date in the grain, so reprocessing three days cannot be
                  expressed as a partial rewrite. They are recomputed in full whenever the job
                  runs.

That is a real limitation, not an oversight. Making them incrementally updatable would mean
storing per-day partials and summing them, which is Phase 9 warehouse modelling work. A bounded
backfill therefore fixes the time-grained tables surgically and rebuilds the global ones, and
the job reports which it did.
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
    ap.add_argument("--start-date", default=None, help="inclusive, YYYY-MM-DD, bounded rebuild")
    ap.add_argument("--end-date", default=None, help="exclusive, YYYY-MM-DD")
    ap.add_argument(
        "--legacy-schema",
        action="store_true",
        help="build fact_events WITHOUT the Phase 8 provenance columns. Exists so the backfill "
        "demonstration can recreate the pre-change state and be re-run repeatably; not for "
        "normal use.",
    )
    ap.add_argument("--out", default="/opt/spark/project/data/delta/gold_stats.json")
    args = ap.parse_args()

    spark = build_session("build-gold")
    spark.sparkContext.setLogLevel("ERROR")
    root = f"{DELTA_ROOT}/{args.run_label}"
    all_silver = spark.read.format("delta").load(f"{root}/silver_events")

    bounded = args.start_date is not None
    if bounded:
        silver = all_silver.filter(F.col("event_date") >= F.lit(args.start_date))
        if args.end_date:
            silver = silver.filter(F.col("event_date") < F.lit(args.end_date))
        replace_where = f"event_date >= '{args.start_date}'"
        if args.end_date:
            replace_where += f" AND event_date < '{args.end_date}'"
    else:
        silver = all_silver
        replace_where = None
    silver = silver.cache()

    def write_partitioned(df, path, allow_schema_shrink=False):
        """Time-grained tables: surgical when bounded, full overwrite otherwise.

        mergeSchema is required for a backfill that also changes the schema. Delta refuses to
        combine replaceWhere with overwriteSchema, because rewriting one date range cannot
        redefine a table the rest of which is untouched. mergeSchema permits ADDITIVE columns.

        The consequence is visible and correct: partitions outside the backfill range keep NULL
        for a newly added column until they are reprocessed too. That is not a defect, it is an
        accurate record of which partitions have been migrated. A full rebuild fills them in.
        """
        w = df.write.format("delta").mode("overwrite").partitionBy("event_date")
        if allow_schema_shrink:
            # Only the legacy-schema reset path removes columns, and it always rewrites the
            # whole table, so replaceWhere is never combined with overwriteSchema.
            w = w.option("overwriteSchema", "true")
        else:
            w = w.option("mergeSchema", "true")
            if replace_where:
                w = w.option("replaceWhere", replace_where)
        w.save(path)

    # Global aggregates read ALL of Silver even on a bounded run. Computing them from the
    # bounded slice would silently discard every other day in the history.
    global_source = all_silver

    # fact_events: the grain is one row per event, time partitioned like Silver
    # Phase 8 transformation change: carry point-in-time provenance into Gold.
    #
    # Before, fact_events recorded WHAT category an item had at event time but not WHEN that
    # value became known. Downstream that is the difference between "this item was in category
    # 1338" and "this item had been in category 1338 for 3 days when the event happened".
    # Phase 11 features care about the second, and it cannot be recovered later without
    # re-joining the SCD.
    #
    # Additive: no row changes enrichment status, so ENRICH-CATEGORY-001 at 23.835% is
    # unaffected. Only the column set grows.
    fact_events = silver.select(
        "event_id", "visitor_id", "item_id", "event_type", "event_time", "event_timestamp",
        "category_id", "parent_category_id", "available", "enrichment_status", "event_date",
        F.col("category_id_valid_from_ms").alias("category_known_since_ms"),
        F.when(
            F.col("category_id_valid_from_ms").isNotNull(),
            F.col("event_timestamp") - F.col("category_id_valid_from_ms"),
        ).alias("category_age_at_event_ms"),
    )
    if args.legacy_schema:
        fact_events = fact_events.drop("category_known_since_ms", "category_age_at_event_ms")
    write_partitioned(fact_events, f"{root}/fact_events", allow_schema_shrink=args.legacy_schema)

    # fact_transactions: one row per transaction line. Phase 2 measured 1.27 items per
    # transaction id, so a transaction id is not unique here and must not be used as a key.
    fact_transactions = (
        silver.filter(F.col("event_type") == "transaction")
        .select(
            "event_id", "transaction_id", "visitor_id", "item_id", "event_time",
            "category_id", "parent_category_id", "event_date",
        )
    )
    write_partitioned(fact_transactions, f"{root}/fact_transactions")

    # mart_item_funnel: view -> cart -> purchase per item, across the whole slice
    funnel = (
        global_source.groupBy("item_id")
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
        global_source.filter(F.col("category_id").isNotNull())
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
    write_partitioned(daily, f"{root}/mart_daily_item_metrics")

    silver_rows = all_silver.count()
    silver_rows_in_scope = silver.count()
    attributed = all_silver.filter(F.col("category_id").isNotNull()).count()

    stats = {
        "run_label": args.run_label,
        "bounded_rebuild": bounded,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "replace_where": replace_where,
        "tables_rebuilt_surgically": [
            "fact_events", "fact_transactions", "mart_daily_item_metrics"
        ] if bounded else [],
        "tables_rebuilt_in_full": ["mart_item_funnel", "mart_category_performance"],
        "silver_rows": silver_rows,
        "silver_rows_in_scope": silver_rows_in_scope,
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
