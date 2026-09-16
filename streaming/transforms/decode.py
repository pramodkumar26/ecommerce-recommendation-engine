"""Turn Confluent-framed Avro from Kafka into typed columns.

Confluent wire format is one magic byte 0x00, four bytes of big-endian schema id, then the
Avro payload. Spark's from_avro does not understand that framing, so the first five bytes have
to be stripped before decoding. Getting this wrong produces a silent stream of nulls rather
than an error, which is why the Phase 5 checks assert a non-zero decode rate.
"""

from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

CONFLUENT_HEADER_BYTES = 5
DECODED = "decoded"


def strip_confluent_header(df, column="value"):
    """substring on binary is 1-indexed, so position 6 drops the five header bytes."""
    return df.withColumn(
        "avro_payload",
        F.expr(f"substring({column}, {CONFLUENT_HEADER_BYTES + 1}, length({column}) - {CONFLUENT_HEADER_BYTES})"),
    ).withColumn(
        "schema_id",
        F.expr(f"conv(hex(substring({column}, 2, 4)), 16, 10)").cast("long"),
    )


def decode_events(df, schema_json):
    """PERMISSIVE so a bad record becomes a null row instead of killing the query."""
    return strip_confluent_header(df).withColumn(
        DECODED,
        from_avro(F.col("avro_payload"), schema_json, {"mode": "PERMISSIVE"}),
    )


def flatten(df):
    """Project the decoded struct into flat columns plus Kafka provenance.

    event_time is built from the raw epoch millis explicitly. The schema stores a plain long
    precisely so no implicit timezone conversion happens anywhere in this path.
    """
    return df.select(
        F.col(f"{DECODED}.event_id").alias("event_id"),
        F.col(f"{DECODED}.visitor_id").alias("visitor_id"),
        F.col(f"{DECODED}.item_id").alias("item_id"),
        F.col(f"{DECODED}.event_type").alias("event_type"),
        F.col(f"{DECODED}.event_timestamp").alias("event_timestamp"),
        F.col(f"{DECODED}.ingestion_timestamp").alias("ingestion_timestamp"),
        F.col(f"{DECODED}.transaction_id").alias("transaction_id"),
        F.col(f"{DECODED}.schema_version").alias("schema_version"),
        F.col(f"{DECODED}.source_file").alias("source_file"),
        F.col(f"{DECODED}.source_row_number").alias("source_row_number"),
        F.expr(f"timestamp_millis({DECODED}.event_timestamp)").alias("event_time"),
        F.expr(f"timestamp_millis({DECODED}.ingestion_timestamp)").alias("ingestion_time"),
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
        F.col("schema_id"),
        F.to_date(F.expr(f"timestamp_millis({DECODED}.event_timestamp)")).alias("event_date"),
    )


def deduplicate(df, watermark):
    """Drop replayed duplicates by deterministic event id.

    The watermark is what makes this survivable. dropDuplicates alone would remember every id
    ever seen, so state grows without bound. With a watermark Spark evicts ids older than it,
    trading a bounded memory footprint for the guarantee that a duplicate arriving more than
    `watermark` after the original will not be caught.

    Phase 2 is what makes this work at all: the same source row always hashes to the same id,
    so a replayed record is recognisable. The 460 byte-identical rows in the source correctly
    survive deduplication, because source position is part of the hash and they are distinct
    records rather than replay duplicates.
    """
    return df.withWatermark("event_time", watermark).dropDuplicates(["event_id", "event_time"])


def is_decoded(df):
    return df.filter(F.col(DECODED).isNotNull() & F.col(f"{DECODED}.event_id").isNotNull())


def is_not_decoded(df):
    return df.filter(F.col(DECODED).isNull() | F.col(f"{DECODED}.event_id").isNull())
