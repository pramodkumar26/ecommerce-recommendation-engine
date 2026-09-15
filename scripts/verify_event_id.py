"""Prove the deterministic event ID holds across two independent passes over the source."""

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from producer.event_id import event_id, normalize_transaction_id  # noqa: E402

RAW = Path("data/raw")
OUT = Path("benchmarks/raw")
SOURCE = "events.csv"


def generate_ids():
    """Full pass over the source, returning every event id in source order."""
    ids = []
    row_number = 0
    for chunk in pd.read_csv(
        RAW / SOURCE,
        dtype={
            "timestamp": "int64",
            "visitorid": "int64",
            "event": "str",
            "itemid": "int64",
            "transactionid": "float64",
        },
        chunksize=500_000,
    ):
        for ts, vid, ev, iid, tx in zip(
            chunk["timestamp"].to_numpy(),
            chunk["visitorid"].to_numpy(),
            chunk["event"].to_numpy(),
            chunk["itemid"].to_numpy(),
            chunk["transactionid"].to_numpy(),
            strict=True,
        ):
            ids.append(event_id(SOURCE, row_number, ts, vid, iid, ev, tx))
            row_number += 1
    return ids


def digest(ids):
    h = hashlib.sha256()
    for i in ids:
        h.update(i.encode("ascii"))
    return h.hexdigest()


def unit_checks():
    """Small fixed cases, so a regression is obvious without reading the whole file."""
    base = event_id("events.csv", 0, 1433221332117, 257597, 355908, "view", None)
    cases = {
        "stable_across_calls": event_id(
            "events.csv", 0, 1433221332117, 257597, 355908, "view", None
        )
        == base,
        "nan_and_none_agree": event_id(
            "events.csv", 0, 1433221332117, 257597, 355908, "view", float("nan")
        )
        == base,
        "float_and_int_tx_agree": event_id("events.csv", 1, 1, 2, 3, "transaction", 1234.0)
        == event_id("events.csv", 1, 1, 2, 3, "transaction", 1234),
        "row_number_changes_id": event_id(
            "events.csv", 1, 1433221332117, 257597, 355908, "view", None
        )
        != base,
        "source_file_changes_id": event_id(
            "other.csv", 0, 1433221332117, 257597, 355908, "view", None
        )
        != base,
        "event_type_changes_id": event_id(
            "events.csv", 0, 1433221332117, 257597, 355908, "addtocart", None
        )
        != base,
        "tx_presence_changes_id": event_id(
            "events.csv", 0, 1433221332117, 257597, 355908, "view", 1
        )
        != base,
        "normalize_nan": normalize_transaction_id(float("nan")) == "",
        "normalize_float": normalize_transaction_id(1234.0) == "1234",
    }
    return cases


def main():
    OUT.mkdir(parents=True, exist_ok=True)

    print("unit checks")
    cases = unit_checks()
    for name, passed in cases.items():
        print(f"  {'ok  ' if passed else 'FAIL'} {name}")
    if not all(cases.values()):
        print("FAIL unit checks")
        return 1

    print("\npass 1 over events.csv")
    run1 = generate_ids()
    print("pass 2 over events.csv")
    run2 = generate_ids()

    d1, d2 = digest(run1), digest(run2)
    unique = len(set(run1))

    checks = {
        "same_row_count": len(run1) == len(run2),
        "identical_id_sequence": run1 == run2,
        "identical_set_digest": d1 == d2,
        "no_collisions": unique == len(run1),
    }

    report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "source_file": SOURCE,
        "rows_processed": len(run1),
        "unique_ids": unique,
        "collisions": len(run1) - unique,
        "run1_digest": d1,
        "run2_digest": d2,
        "sample_first_3": run1[:3],
        "unit_checks": cases,
        "checks": checks,
        "passed": all(checks.values()),
    }

    out = OUT / "event_id_reproducibility.json"
    out.write_text(json.dumps(report, indent=2))

    print()
    for name, passed in checks.items():
        print(f"  {'ok  ' if passed else 'FAIL'} {name}")
    print(f"\nrows: {len(run1)}, unique ids: {unique}, collisions: {len(run1) - unique}")
    print(f"run 1 digest: {d1}")
    print(f"run 2 digest: {d2}")
    print(f"\nwrote {out}")
    print("PASS" if report["passed"] else "FAIL")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
