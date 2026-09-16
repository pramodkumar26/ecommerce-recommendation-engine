"""Reprocess a bounded historical range from Bronze, through Silver, into Gold.

This is what Bronze being immutable was for. Every earlier phase argued that keeping raw
history makes reprocessing possible; this is the command that demonstrates it.

SHARED TRANSFORMATION LOGIC

There is exactly one definition of the Silver rules and one of the Gold marts. This command
invokes build_silver.py and build_gold.py rather than restating their logic, so a full rebuild
and a bounded backfill cannot drift apart. A second implementation "optimised for backfill" is
how a backfill quietly stops matching the pipeline it is meant to repair.

WHAT A BOUNDED RUN ACTUALLY TOUCHES

    silver_events              replaceWhere on the date range
    fact_events                replaceWhere on the date range
    fact_transactions          replaceWhere on the date range
    mart_daily_item_metrics    replaceWhere on the date range
    mart_item_funnel           FULL rebuild, no date in the grain
    mart_category_performance  FULL rebuild, no date in the grain
    bronze_events              never written, it is the source
    dim_items_scd              never written unless --rebuild-scd is passed

The two global marts cannot be partially rebuilt because they aggregate across all history with
no date in their grain. Recomputing them from only the bounded slice would silently discard
every other day, so they are rebuilt from all of Silver. Reported, not hidden.

Bronze is never modified. A backfill able to corrupt its own source would defeat the point.

Usage:

    .venv/bin/python scripts/backfill.py --run-label silver \\
        --start-date 2015-05-10 --end-date 2015-05-12
"""

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from _reliability_lib import env, run  # noqa: E402

DELTA_ROOT = ROOT / "data" / "delta"


def spark_batch(script, extra):
    e = env()
    cmd = [
        "docker", "compose", "exec", "-T", "spark-master",
        "/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077",
        "--driver-memory", "1g", "--executor-memory", "1600m", "--total-executor-cores", "6",
        "--packages", e["SPARK_PACKAGES"],
        f"/opt/spark/project/batch/jobs/{script}", *extra,
    ]
    print(f"    {script} {' '.join(extra)}", flush=True)
    return run(cmd, timeout=7200)


def main():
    ap = argparse.ArgumentParser(
        description="Reprocess a bounded date range from Bronze into Silver and Gold."
    )
    ap.add_argument("--run-label", default="silver")
    ap.add_argument("--start-date", required=True, help="inclusive, YYYY-MM-DD")
    ap.add_argument("--end-date", required=True, help="exclusive, YYYY-MM-DD")
    ap.add_argument(
        "--rebuild-scd",
        action="store_true",
        help="also rebuild dim_items_scd. Needed only when the property SOURCE changed, not "
        "when a transformation rule changed.",
    )
    ap.add_argument("--skip-gold", action="store_true", help="rebuild Silver only")
    ap.add_argument("--out", default=str(ROOT / "benchmarks" / "raw" / "backfill_report.json"))
    args = ap.parse_args()

    if args.start_date >= args.end_date:
        print(f"FAIL start-date {args.start_date} is not before end-date {args.end_date}")
        return 1

    report = {
        "run_label": args.run_label,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "rebuild_scd": args.rebuild_scd,
        "skip_gold": args.skip_gold,
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "stages": {},
    }

    def stage(name, fn):
        print(f"\n=== {name} ===", flush=True)
        t0 = time.time()
        fn()
        report["stages"][name] = round(time.time() - t0, 1)
        print(f"--- {name}: {report['stages'][name]}s", flush=True)

    if args.rebuild_scd:
        stage("rebuild_item_scd",
              lambda: spark_batch("build_item_scd.py", ["--run-label", args.run_label]))

    stage("rebuild_silver_range", lambda: spark_batch("build_silver.py", [
        "--run-label", args.run_label,
        "--start-date", args.start_date,
        "--end-date", args.end_date,
    ]))

    if not args.skip_gold:
        stage("rebuild_gold_range", lambda: spark_batch("build_gold.py", [
            "--run-label", args.run_label,
            "--start-date", args.start_date,
            "--end-date", args.end_date,
        ]))

    silver_stats = json.loads((DELTA_ROOT / "silver_stats.json").read_text())
    report["silver"] = {
        "rows_in_scope": silver_stats["bronze_rows_in_scope"],
        "rows_written": silver_stats["silver_rows"],
        "rejected": silver_stats["rejected_rows"],
        "enriched": silver_stats["enriched_rows"],
    }
    if not args.skip_gold:
        gold_stats = json.loads((DELTA_ROOT / "gold_stats.json").read_text())
        report["gold"] = {
            "surgical": gold_stats["tables_rebuilt_surgically"],
            "full_rebuild": gold_stats["tables_rebuilt_in_full"],
            "silver_rows_in_scope": gold_stats["silver_rows_in_scope"],
            "silver_rows_total": gold_stats["silver_rows"],
        }

    report["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    report["total_seconds"] = round(sum(report["stages"].values()), 1)
    Path(args.out).write_text(json.dumps(report, indent=2, default=str))

    print()
    print(json.dumps(report, indent=2, default=str))
    print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
