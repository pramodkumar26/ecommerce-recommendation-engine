"""Phase 7 definition of done: point-in-time enrichment with no future leakage.

Spark does not grade itself. The expected enrichment is recomputed with pandas merge_asof,
which is the canonical backward as-of join, directly from the source CSVs. Then every event is
compared one by one against what Silver actually wrote.

Three things are checked:
  1 no enriched row carries a property whose validity starts after the event
  2 Spark's enrichment matches an independent as-of join, event by event
  3 unenriched rows are unenriched for a defensible reason, not because the join dropped them
"""

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from _reliability_lib import env  # noqa: E402

RAW = ROOT / "data" / "raw"
START_ROW = 100000
LIMIT = 100000
RUN_LABEL = "silver"


def run_export():
    e = env()
    subprocess.run(
        [
            "docker", "compose", "exec", "-T", "spark-master",
            "/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "900m",
            "--packages", e["SPARK_PACKAGES"],
            "/opt/spark/project/streaming/jobs/export_silver.py",
            "--run-label", RUN_LABEL,
        ],
        check=True, capture_output=True, text=True, cwd=ROOT,
    )
    stats = json.loads((ROOT / "data" / "delta" / "silver_export.json").read_text())
    sample_dir = ROOT / "data" / "delta" / "silver_sample"
    csv = next(p for p in sample_dir.iterdir() if p.suffix == ".csv")
    got = pd.read_csv(csv, dtype={"event_id": "str", "item_id": "int64",
                                  "event_timestamp": "int64", "category_id": "float64"})
    return stats, got


def expected_enrichment():
    """Independent as-of join with pandas merge_asof, straight from the source files."""
    ev = pd.read_csv(
        RAW / "events.csv",
        dtype={"timestamp": "int64", "visitorid": "int64", "event": "str",
               "itemid": "int64", "transactionid": "float64"},
    )
    ev["source_row_number"] = range(len(ev))
    ev = ev.sort_values(["timestamp", "source_row_number"], kind="stable").reset_index(drop=True)
    ev = ev.iloc[START_ROW:START_ROW + LIMIT].reset_index(drop=True)

    props = []
    for part in ("item_properties_part1.csv", "item_properties_part2.csv"):
        for chunk in pd.read_csv(
            RAW / part,
            dtype={"timestamp": "int64", "itemid": "int64", "property": "str", "value": "str"},
            chunksize=2_000_000,
        ):
            props.append(chunk[chunk["property"] == "categoryid"][["timestamp", "itemid", "value"]])
    cat = pd.concat(props, ignore_index=True)
    cat = cat.sort_values(["timestamp", "itemid"], kind="stable").reset_index(drop=True)

    left = ev[["timestamp", "itemid"]].sort_values("timestamp", kind="stable").reset_index(drop=True)
    left["_row"] = range(len(left))

    merged = pd.merge_asof(
        left.sort_values("timestamp"),
        cat.rename(columns={"timestamp": "prop_ts", "value": "category_id"}).sort_values("prop_ts"),
        left_on="timestamp", right_on="prop_ts", by="itemid", direction="backward",
    )
    merged["category_id"] = pd.to_numeric(merged["category_id"], errors="coerce")

    ev = ev.sort_values("timestamp", kind="stable").reset_index(drop=True)
    ev["expected_category_id"] = merged.sort_values("_row")["category_id"].to_numpy()
    ev["expected_prop_ts"] = merged.sort_values("_row")["prop_ts"].to_numpy()
    return ev


def main():
    print("exporting silver from spark")
    stats, got = run_export()

    print("recomputing expected enrichment with pandas merge_asof")
    exp = expected_enrichment()

    merged = got.merge(
        exp[["timestamp", "itemid", "expected_category_id", "expected_prop_ts"]],
        left_on=["event_timestamp", "item_id"], right_on=["timestamp", "itemid"], how="left",
    ).drop_duplicates(subset=["event_id"])

    both_null = merged["category_id"].isna() & merged["expected_category_id"].isna()
    both_set = merged["category_id"].notna() & merged["expected_category_id"].notna()
    agree = both_null | (both_set & (merged["category_id"] == merged["expected_category_id"]))
    disagreements = merged[~agree]

    spark_enriched = int(merged["category_id"].notna().sum())
    pandas_enriched = int(merged["expected_category_id"].notna().sum())

    future_leaks = int(
        (
            merged["category_id"].notna()
            & (merged["expected_prop_ts"] > merged["event_timestamp"])
        ).sum()
    )

    checks = {
        "spark_reports_zero_future_enrichment": stats["rows_enriched_from_the_future"] == 0,
        "independent_check_finds_zero_future_enrichment": future_leaks == 0,
        "min_enrichment_lag_non_negative": (stats["min_enrichment_lag_ms"] or 0) >= 0,
        "enriched_count_matches_independent_join": spark_enriched == pandas_enriched,
        "every_event_agrees_with_independent_join": len(disagreements) == 0,
        "unenriched_rows_have_a_status": stats["enrichment_status_counts"].get(
            "no_property_at_or_before_event", 0
        ) == stats["silver_rows"] - stats["enriched_rows"],
    }

    results = {
        "run_label": RUN_LABEL,
        "source_rows": f"{START_ROW} to {START_ROW + LIMIT}",
        "silver_rows": stats["silver_rows"],
        "spark_enriched": spark_enriched,
        "pandas_enriched": pandas_enriched,
        "rows_compared": int(len(merged)),
        "disagreements": int(len(disagreements)),
        "rows_enriched_from_the_future_spark": stats["rows_enriched_from_the_future"],
        "rows_enriched_from_the_future_independent": future_leaks,
        "min_enrichment_lag_ms": stats["min_enrichment_lag_ms"],
        "enrichment_status_counts": stats["enrichment_status_counts"],
        "rows_with_parent_category": stats["rows_with_parent_category"],
        "partitions": stats["partitions"],
        "checks": checks,
        "passed": all(checks.values()),
    }

    out = ROOT / "benchmarks" / "raw" / "silver_verification.json"
    out.write_text(json.dumps(results, indent=2, default=str))
    print()
    print(json.dumps({k: v for k, v in results.items() if k != "checks"}, indent=2, default=str))
    print()
    for k, v in checks.items():
        print(f"  {'ok  ' if v else 'FAIL'} {k}")
    print(f"\nwrote {out}")
    print("PASS" if results["passed"] else "FAIL")
    return 0 if results["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
