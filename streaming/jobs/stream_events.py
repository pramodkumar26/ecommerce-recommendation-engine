"""Phase 5: read the three behavioral topics, compute event-time windows, write Delta and Redis.

Three queries, chained through Delta:

  raw      Kafka -> Bronze, plus undecodable bytes to a side table. Stateless foreachBatch,
           one Kafka read for the whole job.
  dedup    Bronze -> deduped_events, dropping replayed duplicates by deterministic event id.
  metrics  deduped_events -> 5 minute event-time windows, upserted into Delta and pushed to
           Redis.

Why three rather than one pipeline: Spark supports chaining multiple stateful operators only in
append mode. Deduplication and windowed aggregation are both stateful, and append mode makes a
window wait for the watermark before emitting. With the measured 24 hour watermark that means
the aggregate lags a full day of event time, so on a bounded replay almost nothing ever closes.

Splitting them gives each query at most one stateful operator. The aggregate keeps update mode
plus a MERGE sink, which Phase 5 measured as exact on all 777 windows, and deduplication runs
in its own query where append mode costs nothing because dropDuplicates emits the first
occurrence immediately rather than waiting for a window to close.

The chain also matches the medallion layering Phase 7 builds on.

Submit against the standalone cluster, not local[*]. The driver runs in the spark-master
container and executors run on spark-worker, which is where the memory is.

Both write to the LOCAL Delta volume. Writing micro-batches straight to ADLS would put WAN
latency inside the streaming benchmark, so cloud sync is a separate scheduled job in Phase 7.

Session timezone is pinned to UTC. Event timestamps are epoch millis and every window boundary
depends on that interpretation being stable.
"""

import argparse
import json
import sys
import time
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, "/opt/spark/project")

from streaming.transforms.decode import (  # noqa: E402
    decode_events,
    deduplicate,
    flatten,
    is_decoded,
    is_not_decoded,
)

SCHEMA_PATH = "/opt/spark/project/producer/schemas/clickstream_event_v1.avsc"
DELTA_ROOT = "/opt/spark/project/data/delta"
CHECKPOINT_ROOT = "/opt/spark/project/data/checkpoints"
TOPICS = "item_view,add_to_cart,transaction"


def build_session(app_name):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )


KAFKA_SOURCE_SCHEMA = StructType([
    StructField("key", BinaryType()),
    StructField("value", BinaryType()),
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("timestamp", TimestampType()),
    StructField("timestampType", IntegerType()),
])


def ensure_tables(spark, schema_json, bronze_path, deduped_path):
    """Create the chained Delta tables empty before any stream starts.

    Each query in the chain reads the previous table as a stream, and a Delta streaming read
    fails with DELTA_SCHEMA_NOT_SET until the table has been written at least once. Rather than
    sequencing query startup, derive the exact schema by running the same transform chain over
    an empty frame and write it once. Idempotent.
    """
    empty = spark.createDataFrame([], KAFKA_SOURCE_SCHEMA)
    shape = flatten(is_decoded(decode_events(empty, schema_json)))
    for path in (bronze_path, deduped_path):
        if not DeltaTable.isDeltaTable(spark, path):
            shape.write.format("delta").mode("append").partitionBy("event_date").save(path)


def read_kafka(spark, bootstrap, starting_offsets, max_per_trigger):
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", TOPICS)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
    )
    if max_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", str(max_per_trigger))
    return reader.load()


def windowed_metrics(events, window_duration, watermark=None):
    """Event-time windows in update mode, with a MERGE sink.

    Output mode went update -> append across Phases 5 and 6, and the reason is worth keeping.

    Phase 5 used append with a guessed watermark and the aggregate was badly wrong: windows
    holding roughly 120 Bronze events received 1 or 2, because replay compresses 138 days of
    event time into minutes and a too-small watermark raced ahead of records still arriving on
    other Kafka partitions. Update mode plus a MERGE sink fixed that by dropping nothing.

    Phase 6 then needed deduplication before the aggregate, and Spark only supports chaining
    multiple stateful operators in APPEND mode. In update mode the chain silently produced
    wrong results: 76% of events vanished, and the 24% that survived were exactly the records
    measured as having zero lateness.

    Append is correct now because the watermark is measured rather than guessed. At 24 hours it
    sits above the 16 hour maximum observed lateness, so nothing is dropped. The cost is that a
    window is only emitted once the watermark passes its end, so results lag by the watermark
    in event time. That is honest streaming behaviour, not a defect.

    The sink still MERGEs on window_start. Append emits each window once, so a plain append
    would also work, but MERGE makes the sink idempotent if a batch is replayed after a restart.

    Watermark default comes from measurement, not intuition. streaming/jobs/measure_lateness.py
    computes how far behind the running maximum event time each record actually arrives, which
    is exactly what Spark compares against.

    Measured on the 50,000 event fixture at 5,000 per trigger:

        p50  104 min      p95  845 min
        p90  737 min      p99  916 min      max  959 min (16.0 h)

    Nothing arrives later than 24 hours, at any batch size tried. So the default is 24 hours:
    above the measured maximum with margin, and it bounds dedup state to roughly one day of
    event ids.

    The counterintuitive part, also measured: smaller batches produce MORE lateness. At 1,000
    per trigger p50 lateness is 220 minutes, at 20,000 it is 0. With fewer, larger batches most
    records sit in the first batch and have no preceding maximum to be late against.

    Critically, this is event-time lateness created by replay compression, not network delay.
    138 days of 2015 are pushed through in minutes, so records legitimately sit hours behind the
    running maximum. A deployment ingesting live events would see lateness in seconds and would
    use a watermark of minutes. The watermark must match how the source actually delivers event
    time.
    """
    # No watermark here. The source is already deduplicated, this query has one stateful
    # operator, and update mode keeps every record. Adding a watermark would only reintroduce
    # the Phase 5 failure where late records were silently dropped from the aggregate.
    return (
        events.groupBy(F.window("event_time", window_duration).alias("w"))
        .agg(
            F.count("*").alias("events"),
            F.approx_count_distinct("visitor_id", 0.01).alias("unique_visitors_approx"),
            F.approx_count_distinct("item_id", 0.01).alias("unique_items_approx"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "addtocart", 1).otherwise(0)).alias("carts"),
            F.sum(F.when(F.col("event_type") == "transaction", 1).otherwise(0)).alias(
                "transactions"
            ),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "events",
            "unique_visitors_approx",
            "unique_items_approx",
            "views",
            "carts",
            "transactions",
            F.when(F.col("views") > 0, F.col("carts") / F.col("views"))
            .otherwise(F.lit(None))
            .alias("view_to_cart_rate"),
            F.when(F.col("carts") > 0, F.col("transactions") / F.col("carts"))
            .otherwise(F.lit(None))
            .alias("cart_to_purchase_rate"),
            F.when(F.col("views") > 0, F.col("transactions") / F.col("views"))
            .otherwise(F.lit(None))
            .alias("view_to_purchase_rate"),
        )
    )


def make_redis_writer(redis_host, redis_port, metrics_path):
    """Upsert each batch into Delta on window_start, then push a summary into Redis.

    Append mode emits each window once, finalised. MERGE rather than a plain append keeps the
    sink idempotent: if a micro-batch is reprocessed after a restart, the window is overwritten
    instead of duplicated.

    The aggregate batch is small, tens of windows, so collecting to the driver for the Redis
    write is cheaper than opening a connection per partition. If it grows this becomes a
    foreachPartition.
    """

    def write(batch_df, batch_id):
        from delta.tables import DeltaTable

        batch_df.persist()
        try:
            if batch_df.isEmpty():
                return

            spark = batch_df.sparkSession
            if DeltaTable.isDeltaTable(spark, metrics_path):
                (
                    DeltaTable.forPath(spark, metrics_path)
                    .alias("t")
                    .merge(batch_df.alias("s"), "t.window_start = s.window_start")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                batch_df.write.format("delta").mode("append").save(metrics_path)

            rows = batch_df.orderBy(F.col("window_start").desc()).limit(50).collect()
            if not rows:
                return
            import redis

            r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
            pipe = r.pipeline()
            for row in rows:
                key = f"metrics:window:{int(row['window_start'].timestamp())}"
                pipe.hset(
                    key,
                    mapping={
                        "window_start": row["window_start"].isoformat(),
                        "window_end": row["window_end"].isoformat(),
                        "events": row["events"],
                        "unique_visitors_approx": row["unique_visitors_approx"],
                        "unique_items_approx": row["unique_items_approx"],
                        "views": row["views"],
                        "carts": row["carts"],
                        "transactions": row["transactions"],
                    },
                )
                pipe.expire(key, 86400)
            latest = rows[0]
            pipe.hset(
                "metrics:latest",
                mapping={
                    "batch_id": batch_id,
                    "window_start": latest["window_start"].isoformat(),
                    "events": latest["events"],
                    "views": latest["views"],
                    "carts": latest["carts"],
                    "transactions": latest["transactions"],
                },
            )
            pipe.execute()
        finally:
            batch_df.unpersist()

    return write


def run_bounded(queries, await_seconds, idle_seconds):
    """Stop on a hard deadline, or earlier once every query has gone idle.

    awaitAnyTermination was not usable here: it only returns when a query dies, and these
    queries never die on their own, so a bounded run has to poll progress and stop explicitly.
    A job that outlives its run holds every executor core and blocks the next submit.
    """
    deadline = time.time() + await_seconds
    idle_since = None

    while time.time() < deadline:
        if not any(q.isActive for q in queries):
            break
        rows = 0
        for q in queries:
            p = q.lastProgress
            if p:
                rows += p.get("numInputRows", 0)
        if rows == 0:
            idle_since = idle_since or time.time()
            if time.time() - idle_since > idle_seconds:
                print(f"all queries idle for {idle_seconds}s, stopping")
                break
        else:
            idle_since = None
        time.sleep(3)

    for q in queries:
        progress = q.lastProgress
        print(f"{q.name}: {json.dumps(progress, default=str) if progress else 'no progress'}")
        if q.isActive:
            q.stop()
    print("all queries stopped")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="kafka:29092")
    ap.add_argument("--redis-host", default="redis")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--starting-offsets", default="earliest")
    ap.add_argument("--window", default="5 minutes")
    ap.add_argument("--watermark", default="24 hours", help="measured, see windowed_metrics")
    ap.add_argument(
        "--no-dedup",
        dest="dedup",
        action="store_false",
        help="disable dedup, used to show duplicates inflating counts",
    )
    ap.add_argument("--max-offsets-per-trigger", type=int, default=None)
    ap.add_argument("--run-label", default="dev", help="isolates delta and checkpoint paths")
    ap.add_argument("--await-seconds", type=int, default=0, help="0 means run until stopped")
    ap.add_argument(
        "--idle-seconds",
        type=int,
        default=45,
        help="stop early once every query has processed no rows for this long",
    )
    args = ap.parse_args()

    schema_json = Path(SCHEMA_PATH).read_text()
    spark = build_session(f"phase5-stream-{args.run_label}")
    spark.sparkContext.setLogLevel("WARN")

    bronze_path = f"{DELTA_ROOT}/{args.run_label}/bronze_events"
    metrics_path = f"{DELTA_ROOT}/{args.run_label}/metrics_5min"
    undecodable_path = f"{DELTA_ROOT}/{args.run_label}/undecodable"
    deduped_path = f"{DELTA_ROOT}/{args.run_label}/deduped_events"
    cp = f"{CHECKPOINT_ROOT}/{args.run_label}"

    ensure_tables(spark, schema_json, bronze_path, deduped_path)

    raw = read_kafka(spark, args.bootstrap, args.starting_offsets, args.max_offsets_per_trigger)
    decoded = decode_events(raw, schema_json)

    def write_raw(batch_df, batch_id):
        batch_df.persist()
        try:
            good = flatten(is_decoded(batch_df))
            good.write.format("delta").mode("append").partitionBy("event_date").save(bronze_path)

            bad_batch = is_not_decoded(batch_df).select(
                F.col("topic").alias("source_topic"),
                F.col("partition").alias("source_partition"),
                F.col("offset").alias("source_offset"),
                F.col("schema_id"),
                F.col("value").alias("original_payload"),
                F.lit(batch_id).alias("batch_id"),
                F.current_timestamp().alias("observed_at"),
            )
            if not bad_batch.isEmpty():
                bad_batch.write.format("delta").mode("append").save(undecodable_path)
        finally:
            batch_df.unpersist()

    raw_q = (
        decoded.writeStream.option("checkpointLocation", f"{cp}/raw")
        .queryName("raw_to_bronze")
        .foreachBatch(write_raw)
        .start()
    )

    # Bronze keeps duplicates because it is immutable raw history and Phase 8 replays from it.
    # Deduplication happens on the way out of Bronze so the live aggregate cannot double count.
    bronze_stream = spark.readStream.format("delta").load(bronze_path)
    if args.dedup:
        dedup_q = (
            deduplicate(bronze_stream, args.watermark)
            .writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{cp}/dedup")
            .partitionBy("event_date")
            .queryName("deduped_events")
            .start(deduped_path)
        )
        metrics_source = spark.readStream.format("delta").load(deduped_path)
    else:
        dedup_q = None
        metrics_source = bronze_stream

    metrics_q = (
        windowed_metrics(metrics_source, args.window)
        .writeStream.outputMode("update")
        .option("checkpointLocation", f"{cp}/metrics")
        .queryName("metrics_5min")
        .foreachBatch(make_redis_writer(args.redis_host, args.redis_port, metrics_path))
        .start()
    )

    queries = [q for q in (raw_q, dedup_q, metrics_q) if q is not None]
    print(json.dumps({"bronze": bronze_path, "deduped": deduped_path, "metrics": metrics_path, "checkpoints": cp}))

    if args.await_seconds:
        run_bounded(queries, args.await_seconds, args.idle_seconds)
    else:
        spark.streams.awaitAnyTermination()

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
