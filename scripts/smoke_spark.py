import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = SparkSession.builder.appName("phase1a-smoke").getOrCreate()
    sc = spark.sparkContext
    print(f"spark version: {spark.version}")
    print(f"master: {sc.master}")
    print(f"default parallelism: {sc.defaultParallelism}")

    rows = [(f"visitor-{i % 4}", f"item-{i % 7}", i % 3) for i in range(1000)]
    df = spark.createDataFrame(rows, "visitor_id string, item_id string, event_code int")

    total = df.count()
    by_visitor = (
        df.groupBy("visitor_id")
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("item_id").alias("distinct_items"),
        )
        .orderBy("visitor_id")
    )
    collected = by_visitor.collect()

    print(f"total rows: {total}")
    for r in collected:
        print(f"  {r['visitor_id']}: {r['events']} events, {r['distinct_items']} distinct items")

    shuffled = df.repartition(8, "visitor_id").groupBy("event_code").count().collect()
    print(f"shuffle across 8 partitions produced {len(shuffled)} event_code groups")

    spark.stop()

    ok = total == 1000 and len(collected) == 4 and len(shuffled) == 3
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
