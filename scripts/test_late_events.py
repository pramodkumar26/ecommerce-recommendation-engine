"""Phase 6 test D: late events, on both sides of the watermark boundary.

The policy under test:

  within the watermark   the event is deduplicated and counted normally
  beyond the watermark   the event is dropped from the deduplicated stream, but Bronze still
                         holds it, so it is recoverable by backfill in Phase 8

Both sides are exercised, because running only the passing side would not show the boundary
exists. This run injects 10% additional delay ON TOP of the lateness replay already creates,
so a small tail is expected to fall outside even the 24 hour watermark. That is the tradeoff
being measured, not a failure.

The zero-loss case is proven separately: scripts/test_duplicates.py runs the same 24 hour
watermark without injected delay and the deduplicated table comes out at exactly 50,000.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _reliability_lib import ROOT, clear_run, export, produce, reset_topics, stream  # noqa: E402

LIMIT = 50000
DELAY_RATE = 0.10
GENEROUS_WATERMARK = "24 hours"
TIGHT_WATERMARK = "1 minute"


def main():
    print("resetting topics and producing with delayed events")
    reset_topics()
    for label in ("late_generous", "late_tight"):
        clear_run(label)
    prod = produce(LIMIT, delay_rate=DELAY_RATE)
    print(f"produced: {prod}")

    print(f"\nrun 1, watermark {GENEROUS_WATERMARK} (measured max lateness is 16h)")
    stream("late_generous", extra=["--watermark", GENEROUS_WATERMARK])
    generous = export("late_generous")

    print(f"run 2, watermark {TIGHT_WATERMARK} (deliberately too small)")
    stream("late_tight", extra=["--watermark", TIGHT_WATERMARK])
    tight = export("late_tight")

    g_events = sum(m["events"] for m in generous["metrics"])
    t_events = sum(m["events"] for m in tight["metrics"])

    g_dropped = LIMIT - g_events
    t_dropped = LIMIT - t_events

    checks = {
        "bronze_has_everything_generous": generous["bronze_rows"] == prod["emitted"],
        "bronze_has_everything_tight": tight["bronze_rows"] == prod["emitted"],
        "watermark_size_controls_drop_rate": g_dropped < t_dropped,
        "generous_watermark_drop_rate_under_1pct": g_dropped / LIMIT < 0.01,
        "tight_watermark_drops_events": t_dropped > 0,
        "dropped_events_still_in_bronze": tight["bronze_rows"] - t_events == t_dropped,
        "generous_dropped_events_still_in_bronze": generous["bronze_rows"] - g_events == g_dropped,
        "no_duplicates_survive_either_run": (
            generous["deduped_rows"] == generous["deduped_distinct_event_ids"]
            and tight["deduped_rows"] == tight["deduped_distinct_event_ids"]
        ),
    }

    results = {
        "limit": LIMIT,
        "delay_rate": DELAY_RATE,
        "produced": prod,
        "generous_watermark": GENEROUS_WATERMARK,
        "tight_watermark": TIGHT_WATERMARK,
        "generous_bronze_rows": generous["bronze_rows"],
        "generous_deduped_rows": generous["deduped_rows"],
        "generous_windowed_events": g_events,
        "tight_bronze_rows": tight["bronze_rows"],
        "tight_deduped_rows": tight["deduped_rows"],
        "tight_windowed_events": t_events,
        "events_dropped_by_generous_watermark": g_dropped,
        "pct_dropped_by_generous_watermark": round(100 * g_dropped / LIMIT, 3),
        "events_dropped_by_tight_watermark": t_dropped,
        "pct_dropped_by_tight_watermark": round(100 * t_dropped / LIMIT, 3),
        "checks": checks,
        "passed": all(checks.values()),
    }

    out = ROOT / "benchmarks" / "raw" / "late_event_test.json"
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
