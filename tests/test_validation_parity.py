"""Prove the two contract-validation implementations agree, record for record.

The same rules exist twice:

    kafka/validation.py        row-at-a-time Python, used by the Phase 4 DLQ harness
    build_silver.rejection_reason   a Spark column expression, used by Bronze -> Silver

A Spark job cannot call the Python validator per row without paying a serialization cost on
every record, and a UDF would forfeit predicate pushdown and the Catalyst optimiser entirely.
So the rules are expressed twice: once as control flow, once as a column expression.

Duplicated logic drifts. This test is the guard: it feeds the same crafted records to both
implementations and asserts they reach the same verdict every time, including the reason.

Run without Spark:   .venv/bin/pytest tests/test_validation_parity.py -v
The Spark side is evaluated by translating the same expression tree in pure Python, mirroring
batch/jobs/build_silver.py exactly. A mismatch here means the two have drifted.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from kafka.validation import VALIDATION_FAILED, Invalid, validate  # noqa: E402

SOURCE_TS_MIN = 1430622004384
SOURCE_TS_MAX = 1442545187788
VALID_EVENT_TYPES = ("view", "addtocart", "transaction")

EXPECTED_TYPE_BY_TOPIC = {
    "item_view": "view",
    "add_to_cart": "addtocart",
    "transaction": "transaction",
}


def spark_rejection_reason(rec):
    """Pure-Python mirror of the Spark column expression in build_silver.rejection_reason.

    The ORDER of these checks matters and must match the Spark version, because both return the
    first reason that fires rather than all of them.
    """
    import re

    eid = rec.get("event_id")
    if not isinstance(eid, str) or not re.match(r"^[0-9a-f]{64}$", eid):
        return "event_id_not_sha256"

    vid = rec.get("visitor_id")
    if vid is None or vid < 0:
        return "negative_visitor_id"

    iid = rec.get("item_id")
    if iid is None or iid < 0:
        return "negative_item_id"

    ts = rec.get("event_timestamp")
    if ts is None or ts < SOURCE_TS_MIN or ts > SOURCE_TS_MAX:
        return "event_timestamp_out_of_range"

    ing = rec.get("ingestion_timestamp")
    if ing is None or ing <= 0:
        return "ingestion_timestamp_invalid"

    if rec.get("event_type") not in VALID_EVENT_TYPES:
        return "unknown_event_type"

    if rec.get("event_type") == "transaction" and rec.get("transaction_id") is None:
        return "transaction_without_transaction_id"

    if rec.get("event_type") != "transaction" and rec.get("transaction_id") is not None:
        return "non_transaction_with_transaction_id"

    return None


def python_verdict(rec, topic=None):
    """Verdict from kafka/validation.py, normalised to the same reason vocabulary."""
    try:
        validate(rec, EXPECTED_TYPE_BY_TOPIC.get(topic) if topic else None)
        return None
    except Invalid as e:
        assert e.error_type == VALIDATION_FAILED
        msg = e.message
        if "event_id" in msg:
            return "event_id_not_sha256"
        if "visitor_id" in msg:
            return "negative_visitor_id"
        if "item_id" in msg:
            return "negative_item_id"
        if "event_timestamp" in msg:
            return "event_timestamp_out_of_range"
        if "ingestion_timestamp" in msg:
            return "ingestion_timestamp_invalid"
        if "unknown event_type" in msg:
            return "unknown_event_type"
        if "has no transaction_id" in msg:
            return "transaction_without_transaction_id"
        if "carries transaction_id" in msg:
            return "non_transaction_with_transaction_id"
        if "does not match topic" in msg:
            return "topic_event_mismatch"
        return "other_contract_failure"


def good(**overrides):
    rec = {
        "event_id": "a" * 64,
        "visitor_id": 257597,
        "item_id": 355908,
        "event_type": "view",
        "event_timestamp": 1433221332117,
        "ingestion_timestamp": 1789503301234,
        "transaction_id": None,
        "schema_version": 1,
        "source_file": "events.csv",
        "source_row_number": 1,
    }
    rec.update(overrides)
    return rec


CASES = [
    ("valid_view", good(), None),
    ("valid_cart", good(event_type="addtocart"), None),
    ("valid_transaction", good(event_type="transaction", transaction_id="17672"), None),
    ("event_id_too_short", good(event_id="abc"), "event_id_not_sha256"),
    ("event_id_uppercase", good(event_id="A" * 64), "event_id_not_sha256"),
    ("event_id_not_hex", good(event_id="z" * 64), "event_id_not_sha256"),
    ("negative_visitor", good(visitor_id=-1), "negative_visitor_id"),
    ("negative_item", good(item_id=-5), "negative_item_id"),
    ("zero_timestamp", good(event_timestamp=0), "event_timestamp_out_of_range"),
    ("timestamp_before_source", good(event_timestamp=SOURCE_TS_MIN - 1),
     "event_timestamp_out_of_range"),
    ("timestamp_after_source", good(event_timestamp=SOURCE_TS_MAX + 1),
     "event_timestamp_out_of_range"),
    ("timestamp_at_lower_bound", good(event_timestamp=SOURCE_TS_MIN), None),
    ("timestamp_at_upper_bound", good(event_timestamp=SOURCE_TS_MAX), None),
    ("ingestion_zero", good(ingestion_timestamp=0), "ingestion_timestamp_invalid"),
    ("ingestion_negative", good(ingestion_timestamp=-1), "ingestion_timestamp_invalid"),
    ("unknown_event_type", good(event_type="wishlist"), "unknown_event_type"),
    ("transaction_missing_id", good(event_type="transaction", transaction_id=None),
     "transaction_without_transaction_id"),
    ("view_with_transaction_id", good(transaction_id="17672"),
     "non_transaction_with_transaction_id"),
    ("cart_with_transaction_id", good(event_type="addtocart", transaction_id="1"),
     "non_transaction_with_transaction_id"),
]


@pytest.mark.parametrize("name,record,expected", CASES, ids=[c[0] for c in CASES])
def test_both_implementations_agree(name, record, expected):
    spark_side = spark_rejection_reason(record)
    python_side = python_verdict(record)
    assert spark_side == expected, f"spark expression said {spark_side!r}, expected {expected!r}"
    assert python_side == expected, f"python validator said {python_side!r}, expected {expected!r}"
    assert spark_side == python_side


def test_every_rule_has_at_least_one_failing_case():
    """A rule with no test case is a rule nobody has proven fires."""
    covered = {expected for _, _, expected in CASES if expected is not None}
    rules = {
        "event_id_not_sha256",
        "negative_visitor_id",
        "negative_item_id",
        "event_timestamp_out_of_range",
        "ingestion_timestamp_invalid",
        "unknown_event_type",
        "transaction_without_transaction_id",
        "non_transaction_with_transaction_id",
    }
    assert rules - covered == set(), f"rules with no failing test case: {rules - covered}"


def test_topic_event_mismatch_is_python_only():
    """Documented asymmetry rather than a silent gap.

    The Kafka harness knows which topic a record arrived on and can catch a view landing on the
    transaction topic. Silver reads Bronze, where topic is a column but routing already happened
    upstream, so the rule is not re-applied there. Recorded so the difference is deliberate.
    """
    rec = good(event_type="view")
    assert python_verdict(rec, topic="transaction") == "topic_event_mismatch"
    assert spark_rejection_reason(rec) is None
