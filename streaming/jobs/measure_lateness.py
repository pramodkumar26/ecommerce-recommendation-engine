"""Measure how late records actually arrive, so the watermark is chosen rather than guessed.

Phase 5 set the watermark three times by intuition and was wrong three times. The number that
matters is not wall-clock lateness, it is how far behind the running maximum event time a
record arrives, because that is exactly what Spark's watermark compares against.

Runs as a batch job over the Bronze table. Bronze preserves arrival order through
(source_partition, source_offset) and carries event_time, which is everything needed.
"""

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

DELTA_ROOT = "/opt/spark/project/data/delta"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-label", default="fixture")
    ap.add_argument("--batch-size", type=int, default=5000,
                    help="must match maxOffsetsPerTrigger of the run being modelled")
    ap.add_argument("--out", default="/opt/spark/project/data/delta/lateness.json")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("measure-lateness")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    bronze = spark.read.format("delta").load(f"{DELTA_ROOT}/{args.run_label}/bronze_events")

    # Reconstruct consumption order. Spark reads partitions in parallel and takes a slice from
    # each per trigger, so global arrival order is approximated by offset position within each
    # partition, interleaved across partitions.
    per_partition = Window.partitionBy("source_topic", "source_partition").orderBy("source_offset")
    ordered = bronze.withColumn("pos_in_partition", F.row_number().over(per_partition))

    # A record's batch is determined by how far into its partition it sits, scaled by that
    # partition's share of the trigger budget.
    totals = (
        bronze.groupBy("source_topic", "source_partition")
        .agg(F.count("*").alias("partition_rows"))
    )
    grand_total = bronze.count()

    ordered = (
        ordered.join(totals, ["source_topic", "source_partition"])
        .withColumn("partition_share", F.col("partition_rows") / F.lit(grand_total))
        .withColumn(
            "batch_index",
            F.floor(
                F.col("pos_in_partition")
                / F.greatest(F.col("partition_share") * F.lit(args.batch_size), F.lit(1.0))
            ).cast("int"),
        )
    )

    # Watermark as Spark computes it: the running maximum event time over batches seen so far.
    batch_max = (
        ordered.groupBy("batch_index").agg(F.max("event_timestamp").alias("batch_max_ms"))
    )
    running = Window.orderBy("batch_index").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    batch_max = batch_max.withColumn("running_max_ms", F.max("batch_max_ms").over(running))

    # Lateness of a record = how far behind the watermark-driving maximum it arrived. Spark
    # compares against the maximum from PREVIOUS batches, so shift by one.
    prev = Window.orderBy("batch_index")
    batch_max = batch_max.withColumn(
        "prev_running_max_ms", F.lag("running_max_ms", 1).over(prev)
    )

    joined = ordered.join(batch_max, "batch_index").withColumn(
        "lateness_ms",
        F.when(
            F.col("prev_running_max_ms").isNull(), F.lit(0)
        ).otherwise(F.greatest(F.col("prev_running_max_ms") - F.col("event_timestamp"), F.lit(0))),
    )

    q = joined.approxQuantile("lateness_ms", [0.5, 0.9, 0.95, 0.99, 0.999, 1.0], 0.0001)
    labels = ["p50", "p90", "p95", "p99", "p999", "max"]
    quantiles = {k: int(v) for k, v in zip(labels, q)}

    late_counts = {}
    for minutes in (1, 5, 10, 30, 60, 180, 360, 720, 1440):
        n = joined.filter(F.col("lateness_ms") > minutes * 60_000).count()
        late_counts[f"later_than_{minutes}min"] = n

    result = {
        "run_label": args.run_label,
        "batch_size_modelled": args.batch_size,
        "total_records": grand_total,
        "batches_modelled": batch_max.count(),
        "lateness_ms_quantiles": quantiles,
        "lateness_readable": {
            k: f"{v / 60000:.1f} min" if v < 86_400_000 else f"{v / 3_600_000:.1f} h"
            for k, v in quantiles.items()
        },
        "records_later_than": late_counts,
        "records_with_zero_lateness": joined.filter(F.col("lateness_ms") == 0).count(),
    }

    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
    print(f"wrote {args.out}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
