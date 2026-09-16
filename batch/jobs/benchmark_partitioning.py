"""Measure what time partitioning actually buys, rather than assuming it helps.

Writes the same Silver data twice, once partitioned by event_date and once not, then runs the
same date-filtered queries against both. Reports wall time and, more importantly, how many files
each query had to open, since partition pruning is a file-skipping optimisation and the file
count is the mechanism rather than the symptom.

Small data on a laptop will not show a dramatic time difference. The file counts show whether
pruning is happening at all, which is the thing worth being able to explain.
"""

import argparse
import json
import statistics
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DELTA_ROOT = "/opt/spark/project/data/delta"


def timed(fn, repeats):
    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - start)
    return result, round(statistics.median(times) * 1000, 1), round(min(times) * 1000, 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-label", default="silver")
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--target-date", default="2015-05-11")
    ap.add_argument("--out", default="/opt/spark/project/data/delta/partition_benchmark.json")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("benchmark-partitioning")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    root = f"{DELTA_ROOT}/{args.run_label}"

    silver = spark.read.format("delta").load(f"{root}/silver_events")

    part_path = f"{root}/bench_partitioned"
    flat_path = f"{root}/bench_unpartitioned"
    silver.write.format("delta").mode("overwrite").partitionBy("event_date").save(part_path)
    silver.write.format("delta").mode("overwrite").save(flat_path)

    partitioned = spark.read.format("delta").load(part_path)
    unpartitioned = spark.read.format("delta").load(flat_path)

    results = {}
    for name, df in (("partitioned", partitioned), ("unpartitioned", unpartitioned)):
        total_files = len(df.inputFiles())

        def single_day(d=df):
            return d.filter(F.col("event_date") == F.lit(args.target_date)).count()

        def single_day_files(d=df):
            return len(d.filter(F.col("event_date") == F.lit(args.target_date)).inputFiles())

        def aggregate_one_day(d=df):
            return (
                d.filter(F.col("event_date") == F.lit(args.target_date))
                .groupBy("event_type")
                .count()
                .collect()
            )

        count, count_median, count_min = timed(single_day, args.repeats)
        agg, agg_median, agg_min = timed(aggregate_one_day, args.repeats)

        results[name] = {
            "files_in_table": total_files,
            "files_scanned_for_one_day": single_day_files(),
            "rows_matched": count,
            "count_median_ms": count_median,
            "count_best_ms": count_min,
            "groupby_median_ms": agg_median,
            "groupby_best_ms": agg_min,
        }

    p, u = results["partitioned"], results["unpartitioned"]
    results["comparison"] = {
        "target_date": args.target_date,
        "file_skip_ratio": round(u["files_scanned_for_one_day"] / max(p["files_scanned_for_one_day"], 1), 2),
        "count_speedup": round(u["count_median_ms"] / max(p["count_median_ms"], 0.1), 2),
        "groupby_speedup": round(u["groupby_median_ms"] / max(p["groupby_median_ms"], 0.1), 2),
        "pruning_observed": p["files_scanned_for_one_day"] < u["files_scanned_for_one_day"],
        "rows_agree": p["rows_matched"] == u["rows_matched"],
    }

    with open(args.out, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(json.dumps(results, indent=2, default=str))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
