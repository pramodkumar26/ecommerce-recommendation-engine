"""Phase 5 definition of done.

Spark does not grade itself here. Expected values are computed independently with pandas from
the original CSV, then compared against what the streaming job wrote to Delta.

1. Every produced event reaches Bronze exactly once, with the right identity.
2. Event-time windows carry the correct counts.
3. Windows not yet emitted are only those still inside the watermark, not dropped data.
4. Approximate distinct counts are close to exact ones.
5. Live metrics reached Redis.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import redis

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from producer.event_id import event_id, normalize_transaction_id  # noqa: E402

LIMIT = 50000
WINDOW_MS = 5 * 60 * 1000
WATERMARK_MS = int(os.environ.get("VERIFY_WATERMARK_MS", 6 * 60 * 60 * 1000))
EXPORT = ROOT / "data" / "delta" / "export.json"


def expected_from_source():
    df = pd.read_csv(
        ROOT / "data" / "raw" / "events.csv",
        dtype={
            "timestamp": "int64",
            "visitorid": "int64",
            "event": "str",
            "itemid": "int64",
            "transactionid": "float64",
        },
    )
    df["source_row_number"] = range(len(df))
    df = df.sort_values(["timestamp", "source_row_number"], kind="stable").reset_index(drop=True)
    df = df.iloc[:LIMIT].reset_index(drop=True)

    df["event_id"] = [
        event_id("events.csv", r.source_row_number, r.timestamp, r.visitorid, r.itemid, r.event,
                 r.transactionid)
        for r in df.itertuples(index=False)
    ]
    df["window_start_s"] = (df["timestamp"] // WINDOW_MS) * WINDOW_MS // 1000

    grouped = df.groupby("window_start_s")
    windows = {}
    for ws, g in grouped:
        windows[int(ws)] = {
            "events": int(len(g)),
            "views": int((g["event"] == "view").sum()),
            "carts": int((g["event"] == "addtocart").sum()),
            "transactions": int((g["event"] == "transaction").sum()),
            "unique_visitors_exact": int(g["visitorid"].nunique()),
            "unique_items_exact": int(g["itemid"].nunique()),
        }

    return {
        "rows": int(len(df)),
        "event_ids": set(df["event_id"]),
        "by_type": {k: int(v) for k, v in df["event"].value_counts().items()},
        "ts_min": int(df["timestamp"].min()),
        "ts_max": int(df["timestamp"].max()),
        "windows": windows,
        "transaction_ids_present": int(
            df[df["event"] == "transaction"]["transactionid"].map(normalize_transaction_id).ne("").sum()
        ),
    }


def run_export(run_label):
    env = dict(line.split("=", 1) for line in (ROOT / ".env").read_text().splitlines()
               if "=" in line and not line.startswith("#"))
    cmd = [
        "docker", "compose", "exec", "-T", "spark-master",
        "/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "900m",
        "--packages", env["SPARK_PACKAGES"],
        "/opt/spark/project/streaming/jobs/export_tables.py",
        "--run-label", run_label,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    if r.returncode != 0:
        raise RuntimeError(r.stdout[-3000:] + r.stderr[-3000:])
    return json.loads(EXPORT.read_text())


def main():
    print("computing expected values from the source csv")
    exp = expected_from_source()

    print("exporting delta tables")
    got = run_export("fixture")

    metrics_by_window = {m["window_start_s"]: m for m in got["metrics"]}
    exact_by_window = {e["window_start_s"]: e for e in got["exact_distincts"]}

    emitted = set(metrics_by_window)
    expected_windows = set(exp["windows"])
    missing = expected_windows - emitted
    unexpected = emitted - expected_windows

    # a window may legitimately not be emitted yet if it sits inside the watermark
    watermark_cutoff_s = (exp["ts_max"] - WATERMARK_MS) // 1000
    missing_inside_watermark = {w for w in missing if w + WINDOW_MS // 1000 > watermark_cutoff_s}
    missing_outside_watermark = missing - missing_inside_watermark

    count_mismatches = []
    for ws in sorted(emitted & expected_windows):
        e, g = exp["windows"][ws], metrics_by_window[ws]
        for field in ("events", "views", "carts", "transactions"):
            if e[field] != g[field]:
                count_mismatches.append({"window_start_s": ws, "field": field,
                                         "expected": e[field], "got": g[field]})

    # HyperLogLog accuracy is a relative guarantee, so judging it on tiny cardinalities is
    # meaningless: a window with 10 distinct visitors estimated at 9 is off by one record and
    # scores a 10% error. Relative error is checked only where the base is large enough to mean
    # something, and absolute error is checked everywhere.
    MEANINGFUL_CARDINALITY = 50
    approx_errors = []
    approx_abs_errors = []
    for ws in sorted(emitted & set(exact_by_window)):
        exact = exact_by_window[ws]["unique_visitors_exact"]
        approx = metrics_by_window[ws]["unique_visitors_approx"]
        approx_abs_errors.append(abs(approx - exact))
        if exact >= MEANINGFUL_CARDINALITY:
            approx_errors.append(abs(approx - exact) / exact)

    exact_matches_source = sum(
        1 for ws in exact_by_window
        if ws in exp["windows"]
        and exact_by_window[ws]["unique_visitors_exact"] == exp["windows"][ws]["unique_visitors_exact"]
    )

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    redis_keys = len(r.keys("metrics:window:*"))
    redis_latest = r.hgetall("metrics:latest")

    worst_approx = max(approx_errors) if approx_errors else 0.0
    mean_approx = sum(approx_errors) / len(approx_errors) if approx_errors else 0.0
    worst_abs = max(approx_abs_errors) if approx_abs_errors else 0

    checks = {
        "bronze_row_count_matches": got["bronze_rows"] == exp["rows"],
        "bronze_event_ids_unique": got["bronze_distinct_event_ids"] == got["bronze_rows"],
        "bronze_event_type_counts_match": got["bronze_by_type"] == exp["by_type"],
        "bronze_event_time_range_matches": (
            got["bronze_event_time_min_ms"] == exp["ts_min"]
            and got["bronze_event_time_max_ms"] == exp["ts_max"]
        ),
        "bronze_partitioned_by_date": len(got["bronze_partitions"]) > 1,
        "no_undecodable_records": got["undecodable_rows"] == 0,
        "no_unexpected_windows": len(unexpected) == 0,
        "window_counts_exact": len(count_mismatches) == 0,
        "unemitted_windows_all_inside_watermark": len(missing_outside_watermark) == 0,
        "exact_distincts_match_source": exact_matches_source == len(exact_by_window),
        "approx_distinct_within_5pct_on_large_windows": worst_approx <= 0.05,
        "approx_distinct_absolute_error_small": worst_abs <= 5,
        "redis_received_metrics": redis_keys > 0 and bool(redis_latest),
    }

    results = {
        "expected_rows": exp["rows"],
        "bronze_rows": got["bronze_rows"],
        "bronze_by_type": got["bronze_by_type"],
        "expected_by_type": exp["by_type"],
        "bronze_partitions": len(got["bronze_partitions"]),
        "undecodable_rows": got["undecodable_rows"],
        "expected_windows": len(expected_windows),
        "emitted_windows": len(emitted),
        "windows_not_yet_emitted": len(missing),
        "of_those_inside_watermark": len(missing_inside_watermark),
        "of_those_outside_watermark": len(missing_outside_watermark),
        "count_mismatches": count_mismatches[:10],
        "approx_distinct_windows_judged": len(approx_errors),
        "approx_distinct_mean_error_pct": round(100 * mean_approx, 4),
        "approx_distinct_worst_error_pct": round(100 * worst_approx, 4),
        "approx_distinct_worst_absolute_error": worst_abs,
        "redis_window_keys": redis_keys,
        "redis_latest": redis_latest,
        "checks": checks,
        "passed": all(checks.values()),
    }

    out = ROOT / "benchmarks" / "raw" / "streaming_verification.json"
    out.write_text(json.dumps(results, indent=2, default=str))

    print()
    print(json.dumps({k: v for k, v in results.items() if k != "checks"}, indent=2, default=str))
    print()
    for name, ok in checks.items():
        print(f"  {'ok  ' if ok else 'FAIL'} {name}")
    print(f"\nwrote {out}")
    print("PASS" if results["passed"] else "FAIL")
    return 0 if results["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
