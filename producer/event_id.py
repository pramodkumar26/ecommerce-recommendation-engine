"""Deterministic event identity.

The same source row must produce the same ID on every replay, or deduplication in Phase 6
cannot tell a replayed event from a new one.

Source file and row number are part of the hash on purpose. The dataset contains rows whose
business fields are byte-identical (460 of them). Those are distinct source records, and
collapsing them would silently drop data. Position makes them distinguishable while still
being stable across replays, because the source files never change.
"""

import hashlib

SEPARATOR = "|"


def normalize_transaction_id(value) -> str:
    """Source stores transaction ids as floats because the column is sparse. 1234.0 -> '1234'.

    Missing values arrive as None or NaN depending on the reader, and both mean absent.
    """
    if value is None:
        return ""
    if isinstance(value, float):
        if value != value:
            return ""
        if value.is_integer():
            return str(int(value))
    return str(value)


def event_id(
    source_file: str,
    source_row_number: int,
    timestamp: int,
    visitor_id: int,
    item_id: int,
    event_type: str,
    transaction_id=None,
) -> str:
    payload = SEPARATOR.join(
        (
            source_file,
            str(int(source_row_number)),
            str(int(timestamp)),
            str(int(visitor_id)),
            str(int(item_id)),
            str(event_type),
            normalize_transaction_id(transaction_id),
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
