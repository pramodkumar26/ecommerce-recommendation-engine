"""Decode and validate clickstream records, and describe why one failed.

Shared deliberately. Phase 4's standalone router uses it, and Phase 5's Spark job reuses the
same rules so a record judged bad by one is judged bad by the other.

Business rules come from the Phase 2 profile, not from guesses:
- every transaction event in the source has a transaction id, and no non-transaction event
  carries one (measured, 0 exceptions in 2,756,101 rows)
- visitor and item ids are non-negative
- event timestamps fall inside the measured source range
"""

import re

from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext

SOURCE_TS_MIN = 1430622004384
SOURCE_TS_MAX = 1442545187788
EVENT_ID_RE = re.compile(r"^[0-9a-f]{64}$")

DESERIALIZATION_FAILED = "deserialization_failed"
UNKNOWN_SCHEMA_ID = "unknown_schema_id"
VALIDATION_FAILED = "validation_failed"


class Invalid(Exception):
    def __init__(self, error_type, message):
        super().__init__(message)
        self.error_type = error_type
        self.message = message


def decode(deserializer, topic, payload):
    """Turn bytes into a record, or raise Invalid with the layer that rejected it."""
    if payload is None:
        raise Invalid(DESERIALIZATION_FAILED, "null payload")
    try:
        record = deserializer(payload, SerializationContext(topic, MessageField.VALUE))
    except SchemaRegistryError as e:
        raise Invalid(UNKNOWN_SCHEMA_ID, f"schema id not resolvable: {e}") from e
    except Exception as e:
        msg = str(e)
        if "schema" in msg.lower() and "not found" in msg.lower():
            raise Invalid(UNKNOWN_SCHEMA_ID, msg) from e
        raise Invalid(DESERIALIZATION_FAILED, f"{type(e).__name__}: {msg}") from e
    if record is None:
        raise Invalid(DESERIALIZATION_FAILED, "deserializer returned None")
    return record


def validate(record, expected_event_type=None):
    """Raise Invalid if the record decodes but breaks a business rule."""
    eid = record.get("event_id")
    if not isinstance(eid, str) or not EVENT_ID_RE.match(eid):
        raise Invalid(VALIDATION_FAILED, f"event_id is not a 64 character sha256 hex: {eid!r}")

    for field in ("visitor_id", "item_id"):
        value = record.get(field)
        if not isinstance(value, int) or value < 0:
            raise Invalid(VALIDATION_FAILED, f"{field} must be a non-negative integer, got {value!r}")

    ts = record.get("event_timestamp")
    if not isinstance(ts, int) or not (SOURCE_TS_MIN <= ts <= SOURCE_TS_MAX):
        raise Invalid(
            VALIDATION_FAILED,
            f"event_timestamp {ts!r} outside measured source range "
            f"[{SOURCE_TS_MIN}, {SOURCE_TS_MAX}]",
        )

    ingestion = record.get("ingestion_timestamp")
    if not isinstance(ingestion, int) or ingestion <= 0:
        raise Invalid(VALIDATION_FAILED, f"ingestion_timestamp must be positive, got {ingestion!r}")

    event_type = record.get("event_type")
    if event_type not in ("view", "addtocart", "transaction"):
        raise Invalid(VALIDATION_FAILED, f"unknown event_type {event_type!r}")

    if expected_event_type and event_type != expected_event_type:
        raise Invalid(
            VALIDATION_FAILED,
            f"event_type {event_type!r} does not match topic expecting {expected_event_type!r}",
        )

    tx = record.get("transaction_id")
    if event_type == "transaction" and not tx:
        raise Invalid(VALIDATION_FAILED, "transaction event has no transaction_id")
    if event_type != "transaction" and tx:
        raise Invalid(
            VALIDATION_FAILED, f"non-transaction event carries transaction_id {tx!r}"
        )

    return record


def dlq_record(payload, error_type, message, topic, partition, offset, key, now_ms, schema_version):
    return {
        "original_payload": payload if payload is not None else b"",
        "error_type": error_type,
        "error_message": message[:2000],
        "source_topic": topic,
        "partition": partition,
        "offset": offset,
        "message_key": key,
        "ingestion_timestamp": now_ms,
        "schema_version": schema_version,
    }
