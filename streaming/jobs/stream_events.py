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
EVENT_SUBJECTS = ["item_view-value", "add_to_cart-value", "transaction-value"]


def registered_schema_ids(registry_url, subjects=EVENT_SUBJECTS):
    """Ask the registry which schema ids are legitimate for the behavioral topics.

    Spark's from_avro cannot do this itself, so the ids are fetched once at startup and used to
    reject records whose declared schema id was never registered. Resolved eagerly rather than
    per batch: the set is tiny and stable, and a registry outage should not silently widen what
    the pipeline accepts.
    """
    import urllib.request

    ids = set()
    for subject in subjects:
        versions_url = f"{registry_url}/subjects/{subject}/versions"
        try:
            versions = json.loads(urllib.request.urlopen(versions_url, timeout=10).read())
        except Exception as e:
            raise RuntimeError(f"cannot reach schema registry at {registry_url}: {e}") from e
        for v in versions:
            meta = json.loads(
                urllib.request.urlopen(f"{versions_url}/{v}", timeout=10).read()
            )
            ids.add(int(meta["id"]))
    if not ids:
        raise RuntimeError(f"no schemas registered for {subjects}, refusing to accept anything")
    return sorted(ids)
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


def read_kafka(spark, bootstrap, starting_offsets, max_per_trigger, fail_on_data_loss=True):
    """Read the behavioral topics.

    failOnDataLoss defaults to TRUE and that is deliberate. When Kafka has aged out records a
    query has not consumed yet, false makes Spark log a warning and carry on with a gap, while
    true fails the query loudly. A project whose reliability claim is "no silent loss" cannot
    have silent loss configured as its default.

    The escape hatch exists because it is occasionally the right call: after deliberately
    deleting and recreating topics, an old checkpoint references offsets that no longer exist
    and the query cannot start at all. That is a development recovery scenario, so it is opt-in
    via --allow-data-loss rather than always on.
    """
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", TOPICS)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "true" if fail_on_data_loss else "false")
    )
    if max_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", str(max_per_trigger))
    return reader.load()


def windowed_metrics(events, window_duration, watermark=None):
    """Event-time windows, update mode, no aggregation watermark, MERGE sink.

    That combination is deliberate and took three attempts to get right, so the reasoning is
    recorded here. The `watermark` argument is accepted and ignored by design; the dedup query
    upstream owns the watermark, this one must not apply a second.

    HISTORICAL REPLAY MODE, which is what this pipeline runs
        The source is a compressed replay: 138 days of 2015 pushed through in minutes. A record
        can sit hours behind the running maximum event time purely because of that compression,
        not because anything was slow. Modeled lateness on the 50,000 event fixture at 5,000 per
        trigger reached p95 845 min and a maximum of 959 min (16.0 h).

        Applying a watermark to THIS aggregate drops real data. Phase 5 measured it: windows
        holding roughly 120 Bronze events emitted 1 or 2. Update mode with no watermark keeps
        every record, and the MERGE sink collapses the repeated emissions into one row per
        window. State is bounded in practice because a replay is finite.

    LIVE MODE, the intended production behaviour, not implemented here
        Events arrive near wall-clock time, so real lateness is seconds to minutes rather than
        hours. There the aggregate SHOULD carry a watermark, sized from measured live lateness,
        so window state is evicted and an indefinitely running query stays bounded. Append mode
        becomes viable, and the MERGE sink is no longer needed for correctness.

        Phase 7 preserves replay behaviour only. Switching modes is a deliberate future change,
        not a config tweak, because it alters what happens to late data.

    Why not one query with dedup and aggregation chained: Spark supports multiple stateful
    operators only in append mode, and in update mode the chain silently produced wrong results,
    76% of events vanishing. Splitting them across queries gives each at most one stateful
    operator and restores update mode here. See the module docstring.
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

    Idle is detected from batch IDs, not from numInputRows. Summing input rows looked correct
    and truncated a run at 3 of 5 batches: with three chained queries a Delta source can
    legitimately commit a zero-row batch while the pipeline is still working, so all queries
    read zero at the same instant and the idle timer started while data was still pending.
    A query that is genuinely working always advances its batch ID, so that is the real signal.
    """
    deadline = time.time() + await_seconds
    idle_since = None
    last_batches = None

    def check_for_failures():
        """A dead query must be loud.

        This originally just stopped when no query was active, so a query that died from an
        executor OOM looked identical to a clean finish and the job still exited zero. A run
        truncated at 3 of 5 batches was reported as success. In a pipeline whose entire claim is
        "no silent loss", a silently failed query is the worst possible failure mode.
        """
        failed = [(q.name, q.exception()) for q in queries if q.exception() is not None]
        if failed:
            details = "; ".join(f"{name}: {exc}" for name, exc in failed)
            raise RuntimeError(f"streaming query failed: {details}")

    while time.time() < deadline:
        check_for_failures()
        if not any(q.isActive for q in queries):
            break
        batches = tuple(
            (q.name, q.lastProgress.get("batchId") if q.lastProgress else None) for q in queries
        )
        if batches == last_batches:
            idle_since = idle_since or time.time()
            if time.time() - idle_since > idle_seconds:
                print(f"no query advanced a batch for {idle_seconds}s, stopping")
                break
        else:
            last_batches = batches
            idle_since = None
        time.sleep(3)

    for q in queries:
        progress = q.lastProgress
        print(f"{q.name}: {json.dumps(progress, default=str) if progress else 'no progress'}")
        if q.isActive:
            q.stop()

    # Checked again after stopping: a query can fail during the final batch.
    check_for_failures()
    print("all queries stopped")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="kafka:29092")
    ap.add_argument("--redis-host", default="redis")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--schema-registry", default="http://schema-registry:8081")
    ap.add_argument("--starting-offsets", default="earliest")
    ap.add_argument(
        "--allow-data-loss",
        dest="fail_on_data_loss",
        action="store_false",
        help="set failOnDataLoss=false. Development recovery only, see read_kafka",
    )
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
    valid_ids = registered_schema_ids(args.schema_registry)
    print(json.dumps({"trusted_schema_ids": valid_ids}))
    spark = build_session(f"phase5-stream-{args.run_label}")
    spark.sparkContext.setLogLevel("WARN")

    bronze_path = f"{DELTA_ROOT}/{args.run_label}/bronze_events"
    metrics_path = f"{DELTA_ROOT}/{args.run_label}/metrics_5min"
    undecodable_path = f"{DELTA_ROOT}/{args.run_label}/undecodable"
    deduped_path = f"{DELTA_ROOT}/{args.run_label}/deduped_events"
    cp = f"{CHECKPOINT_ROOT}/{args.run_label}"

    ensure_tables(spark, schema_json, bronze_path, deduped_path)

    raw = read_kafka(
        spark,
        args.bootstrap,
        args.starting_offsets,
        args.max_offsets_per_trigger,
        args.fail_on_data_loss,
    )
    decoded = decode_events(raw, schema_json)

    def write_raw(batch_df, batch_id):
        batch_df.persist()
        try:
            good = flatten(is_decoded(batch_df, valid_ids))
            good.write.format("delta").mode("append").partitionBy("event_date").save(bronze_path)

            bad_batch = is_not_decoded(batch_df, valid_ids).select(
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
