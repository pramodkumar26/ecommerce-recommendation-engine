"""Second pass: the questions that decide the stream and model design, not just counts."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

RAW = Path("data/raw")
OUT = Path("benchmarks/raw")


def ms_to_iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def load_events():
    return pd.read_csv(
        RAW / "events.csv",
        dtype={
            "timestamp": "int64",
            "visitorid": "int64",
            "event": "category",
            "itemid": "int64",
            "transactionid": "float64",
        },
    )


def property_timeline():
    """Are property timestamps continuous, or discrete snapshots?"""
    stamps = set()
    items_with_props = set()
    first_seen = {}
    for part in ("item_properties_part1.csv", "item_properties_part2.csv"):
        for chunk in pd.read_csv(
            RAW / part,
            usecols=["timestamp", "itemid", "property"],
            dtype={"timestamp": "int64", "itemid": "int64", "property": "str"},
            chunksize=2_000_000,
        ):
            stamps.update(chunk["timestamp"].unique().tolist())
            items_with_props.update(chunk["itemid"].unique().tolist())
            mins = chunk.groupby("itemid")["timestamp"].min()
            for item, ts in mins.items():
                if item not in first_seen or ts < first_seen[item]:
                    first_seen[item] = int(ts)
    return sorted(stamps), items_with_props, first_seen


def main():
    OUT.mkdir(parents=True, exist_ok=True)

    print("loading events")
    ev = load_events()

    print("scanning property timeline")
    stamps, items_with_props, first_seen = property_timeline()

    gaps = [stamps[i + 1] - stamps[i] for i in range(len(stamps) - 1)]
    gap_days = sorted({round(g / 86_400_000, 3) for g in gaps})

    first_prop_ts = stamps[0]
    last_prop_ts = stamps[-1]

    # point in time join feasibility
    before_any_property = int((ev["timestamp"] < first_prop_ts).sum())
    after_last_property = int((ev["timestamp"] > last_prop_ts).sum())

    event_items = set(ev["itemid"].unique().tolist())
    items_missing_props = event_items - items_with_props

    ev_first = ev["itemid"].map(first_seen)
    no_prop_at_all = ev_first.isna()
    before_item_first_prop = (~no_prop_at_all) & (ev["timestamp"] < ev_first)
    joinable = int((~no_prop_at_all & ~before_item_first_prop).sum())

    # repeat interactions
    pair_counts = ev.groupby(["visitorid", "itemid"], observed=True).size()
    repeat_pairs = int((pair_counts > 1).sum())

    # eligible users for evaluation, under candidate minimum history rules
    per_visitor = ev.groupby("visitorid", observed=True).size()
    tx = ev[ev["event"] == "transaction"]
    cart = ev[ev["event"].isin(["transaction", "addtocart"])]
    tx_visitors = set(tx["visitorid"].unique().tolist())
    cart_visitors = set(cart["visitorid"].unique().tolist())

    eligibility = {}
    for min_hist in (2, 3, 5, 10, 20):
        enough = set(per_visitor[per_visitor >= min_hist].index.tolist())
        eligibility[str(min_hist)] = {
            "visitors_with_min_history": len(enough),
            "also_have_a_transaction": len(enough & tx_visitors),
            "also_have_cart_or_transaction": len(enough & cart_visitors),
        }

    report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "property_timeline": {
            "distinct_timestamps": len(stamps),
            "first": ms_to_iso(first_prop_ts),
            "last": ms_to_iso(last_prop_ts),
            "gap_sizes_days": gap_days,
            "interpretation": "discrete snapshots" if len(stamps) < 100 else "continuous",
        },
        "point_in_time_join": {
            "total_events": int(len(ev)),
            "events_before_first_property_snapshot": before_any_property,
            "events_after_last_property_snapshot": after_last_property,
            "events_whose_item_has_no_property_record": int(no_prop_at_all.sum()),
            "events_before_their_items_first_property": int(before_item_first_prop.sum()),
            "events_with_a_valid_point_in_time_property": joinable,
            "join_miss_rate_pct": round(100 * (len(ev) - joinable) / len(ev), 3),
        },
        "item_coverage": {
            "distinct_items_in_events": len(event_items),
            "distinct_items_in_properties": len(items_with_props),
            "event_items_with_no_property_record": len(items_missing_props),
            "event_items_covered_pct": round(
                100 * (len(event_items) - len(items_missing_props)) / len(event_items), 3
            ),
        },
        "repeat_interactions": {
            "distinct_visitor_item_pairs": int(len(pair_counts)),
            "pairs_seen_more_than_once": repeat_pairs,
            "repeat_pair_pct": round(100 * repeat_pairs / len(pair_counts), 3),
            "max_interactions_on_one_pair": int(pair_counts.max()),
        },
        "transaction_reality": {
            "transaction_events": int(len(tx)),
            "unique_transaction_ids": int(tx["transactionid"].nunique()),
            "items_per_transaction_mean": round(len(tx) / tx["transactionid"].nunique(), 3),
            "visitors_who_ever_transacted": len(tx_visitors),
            "pct_of_all_visitors": round(100 * len(tx_visitors) / ev["visitorid"].nunique(), 3),
        },
        "evaluation_eligibility": eligibility,
    }

    out = OUT / "dataset_joins.json"
    out.write_text(json.dumps(report, indent=2))
    print(f"\nwrote {out}\n")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
