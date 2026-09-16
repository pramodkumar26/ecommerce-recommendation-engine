"""Phase 6 test B: an injected duplicate must not change the final count.

Produces one stream carrying injected replay duplicates, then consumes it twice: once with
deduplication on and once off. With dedup on the window totals must match what the source says
they should be. With dedup off they must be visibly inflated, because a test that passes when
the feature is disabled proves nothing.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _reliability_lib import (  # noqa: E402
    ROOT,
    clear_run,
    export,
    produce,
    reset_topics,
    stream,
    window_totals,
)

LIMIT = 50000
DUPLICATE_RATE = 0.05


def expected_from_source():
    import pandas as pd

    from producer.event_id import event_id

    df = pd.read_csv(
        ROOT / "data" / "raw" / "events.csv",
        dtype={"timestamp": "int64", "visitorid": "int64", "event": "str",
               "itemid": "int64", "transactionid": "float64"},
    )
    df["source_row_number"] = range(len(df))
    df = df.sort_values(["timestamp", "source_row_number"], kind="stable").reset_index(drop=True)
    df = df.iloc[:LIMIT]
    ids = {
        event_id("events.csv", r.source_row_number, r.timestamp, r.visitorid, r.itemid,
                 r.event, r.transactionid)
        for r in df.itertuples(index=False)
    }
    df["window_start_s"] = (df["timestamp"] // (5 * 60 * 1000)) * (5 * 60 * 1000) // 1000
    per_window = {}
    for ws, g in df.groupby("window_start_s"):
        per_window[int(ws)] = {
            "events": int(len(g)),
            "views": int((g["event"] == "view").sum()),
            "carts": int((g["event"] == "addtocart").sum()),
            "transactions": int((g["event"] == "transaction").sum()),
        }
    return {
        "events": int(len(df)),
        "views": int((df["event"] == "view").sum()),
        "carts": int((df["event"] == "addtocart").sum()),
        "transactions": int((df["event"] == "transaction").sum()),
        "distinct_event_ids": len(ids),
        "per_window": per_window,
    }


def main():
    exp = expected_from_source()
    print(f"expected from source: {exp}")

    print("\nresetting topics and producing with injected duplicates")
    reset_topics()
    for label in ("dedup_on", "dedup_off"):
        clear_run(label)
    prod = produce(LIMIT, duplicate_rate=DUPLICATE_RATE)
    print(f"produced: {prod}")

    print("\nrun 1, deduplication enabled")
    stream("dedup_on")
    on = export("dedup_on")

    print("run 2, deduplication disabled")
    stream("dedup_off", extra=["--no-dedup"])
    off = export("dedup_off")

    t_on = window_totals(on)
    t_off = window_totals(off)

    # Append mode emits a window only once the watermark passes its end, so only compare
    # windows that were actually finalised. Nothing is dropped; the tail simply has not closed.
    on_windows = {m["window_start_s"]: m for m in on["metrics"]}
    off_windows = {m["window_start_s"]: m for m in off["metrics"]}
    per_window = exp["per_window"]

    on_mismatches = []
    for ws, m in on_windows.items():
        e = per_window.get(ws)
        if e is None:
            on_mismatches.append({"window_start_s": ws, "reason": "window not in source"})
            continue
        for field in ("events", "views", "carts", "transactions"):
            if m[field] != e[field]:
                on_mismatches.append(
                    {"window_start_s": ws, "field": field, "expected": e[field], "got": m[field]}
                )

    shared = set(on_windows) & set(off_windows)
    inflated = [ws for ws in shared if off_windows[ws]["events"] > on_windows[ws]["events"]]
    off_excess = sum(
        off_windows[ws]["events"] - on_windows[ws]["events"] for ws in shared
    )

    checks = {
        "bronze_keeps_duplicates": on["bronze_rows"] == prod["emitted"],
        "bronze_distinct_ids_match_source": on["bronze_distinct_event_ids"] == exp["distinct_event_ids"],
        "dedup_on_emitted_windows": len(on_windows) > 0,
        "dedup_on_every_window_matches_source": len(on_mismatches) == 0,
        "dedup_off_inflates_windows": len(inflated) > 0,
        "dedup_removed_something": off_excess > 0,
    }

    results = {
        "limit": LIMIT,
        "duplicate_rate": DUPLICATE_RATE,
        "produced": prod,
        "expected_events_from_source": exp["events"],
        "expected_windows_in_source": len(per_window),
        "bronze_rows_with_dedup_on": on["bronze_rows"],
        "bronze_distinct_event_ids": on["bronze_distinct_event_ids"],
        "dedup_on_totals": t_on,
        "dedup_off_totals": t_off,
        "windows_emitted_dedup_on": len(on_windows),
        "windows_emitted_dedup_off": len(off_windows),
        "windows_compared": len(shared),
        "windows_inflated_without_dedup": len(inflated),
        "extra_events_without_dedup": off_excess,
        "dedup_on_mismatches": on_mismatches[:10],
        "checks": checks,
        "passed": all(checks.values()),
    }

    out = ROOT / "benchmarks" / "raw" / "duplicate_test.json"
    out.write_text(json.dumps(results, indent=2))
    print()
    print(json.dumps({k: v for k, v in results.items() if k != "checks"}, indent=2))
    print()
    for k, v in checks.items():
        print(f"  {'ok  ' if v else 'FAIL'} {k}")
    print(f"\nwrote {out}")
    print("PASS" if results["passed"] else "FAIL")
    return 0 if results["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
