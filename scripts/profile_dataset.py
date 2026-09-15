"""Measure the Retailrocket source files. Writes JSON evidence, prints a readable summary."""

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

RAW = Path("data/raw")
OUT = Path("benchmarks/raw")
FILES = [
    "events.csv",
    "item_properties_part1.csv",
    "item_properties_part2.csv",
    "category_tree.csv",
]
HIST_BUCKETS = [1, 2, 3, 5, 10, 20]


def sha256(path, chunk=1 << 20):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(chunk):
            h.update(block)
    return h.hexdigest()


def count_rows(path, chunk=1 << 22):
    n = 0
    with open(path, "rb") as f:
        while block := f.read(chunk):
            n += block.count(b"\n")
        f.seek(-1, 2)
        if f.read(1) != b"\n":
            n += 1
    return n - 1


def ms_to_iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def profile_files():
    rows = []
    for name in FILES:
        p = RAW / name
        rows.append(
            {
                "file": name,
                "bytes": p.stat().st_size,
                "rows": count_rows(p),
                "sha256": sha256(p),
            }
        )
    return rows


def profile_events():
    df = pd.read_csv(
        RAW / "events.csv",
        dtype={
            "timestamp": "int64",
            "visitorid": "int64",
            "event": "category",
            "itemid": "int64",
            "transactionid": "float64",
        },
    )

    counts = df["event"].value_counts().to_dict()
    tx = df[df["event"] == "transaction"]

    per_visitor = df.groupby("visitorid", observed=True).size()
    hist = {}
    for b in HIST_BUCKETS:
        if b == HIST_BUCKETS[-1]:
            hist[f"{b}+"] = int((per_visitor >= b).sum())
        else:
            hist[str(b)] = int((per_visitor == b).sum())

    at_least = {str(b): int((per_visitor >= b).sum()) for b in HIST_BUCKETS}

    span_ms = int(df["timestamp"].max() - df["timestamp"].min())

    return {
        "total_rows": int(len(df)),
        "unique_visitors": int(df["visitorid"].nunique()),
        "unique_items": int(df["itemid"].nunique()),
        "event_type_counts": {k: int(v) for k, v in counts.items()},
        "duplicate_full_rows": int(df.duplicated().sum()),
        "duplicate_business_key_rows": int(
            df.duplicated(subset=["timestamp", "visitorid", "itemid", "event"]).sum()
        ),
        "transactions_missing_transactionid": int(tx["transactionid"].isna().sum()),
        "non_transactions_with_transactionid": int(
            df[df["event"] != "transaction"]["transactionid"].notna().sum()
        ),
        "unique_transaction_ids": int(tx["transactionid"].nunique()),
        "timestamp_min_ms": int(df["timestamp"].min()),
        "timestamp_max_ms": int(df["timestamp"].max()),
        "timestamp_min_iso": ms_to_iso(df["timestamp"].min()),
        "timestamp_max_iso": ms_to_iso(df["timestamp"].max()),
        "span_days": round(span_ms / 86_400_000, 2),
        "visitors_by_interaction_count": hist,
        "visitors_with_at_least_n_interactions": at_least,
        "interactions_per_visitor_mean": round(float(per_visitor.mean()), 3),
        "interactions_per_visitor_median": int(per_visitor.median()),
        "interactions_per_visitor_p95": int(per_visitor.quantile(0.95)),
        "interactions_per_visitor_max": int(per_visitor.max()),
        "items_by_event_type_unique": {
            str(k): int(df[df["event"] == k]["itemid"].nunique()) for k in counts
        },
        "visitors_by_event_type_unique": {
            str(k): int(df[df["event"] == k]["visitorid"].nunique()) for k in counts
        },
    }


def profile_item_properties():
    total_rows = 0
    updates_per_item = pd.Series(dtype="int64")
    property_counts = pd.Series(dtype="int64")
    items = set()
    ts_min, ts_max = None, None
    category_rows = 0
    category_items = set()

    for part in ("item_properties_part1.csv", "item_properties_part2.csv"):
        for chunk in pd.read_csv(
            RAW / part,
            dtype={"timestamp": "int64", "itemid": "int64", "property": "str", "value": "str"},
            chunksize=2_000_000,
        ):
            total_rows += len(chunk)
            items.update(chunk["itemid"].unique().tolist())

            c = chunk.groupby("itemid").size()
            updates_per_item = updates_per_item.add(c, fill_value=0)

            p = chunk["property"].value_counts()
            property_counts = property_counts.add(p, fill_value=0)

            lo, hi = int(chunk["timestamp"].min()), int(chunk["timestamp"].max())
            ts_min = lo if ts_min is None else min(ts_min, lo)
            ts_max = hi if ts_max is None else max(ts_max, hi)

            cat = chunk[chunk["property"] == "categoryid"]
            category_rows += len(cat)
            category_items.update(cat["itemid"].unique().tolist())

    updates_per_item = updates_per_item.astype("int64")

    return {
        "total_rows": int(total_rows),
        "distinct_items_with_properties": len(items),
        "distinct_property_names": int(len(property_counts)),
        "updates_per_item_mean": round(float(updates_per_item.mean()), 3),
        "updates_per_item_median": int(updates_per_item.median()),
        "updates_per_item_p95": int(updates_per_item.quantile(0.95)),
        "updates_per_item_max": int(updates_per_item.max()),
        "timestamp_min_ms": ts_min,
        "timestamp_max_ms": ts_max,
        "timestamp_min_iso": ms_to_iso(ts_min),
        "timestamp_max_iso": ms_to_iso(ts_max),
        "categoryid_rows": category_rows,
        "items_with_categoryid": len(category_items),
        "top_20_properties": {
            str(k): int(v) for k, v in property_counts.sort_values(ascending=False).head(20).items()
        },
    }


def profile_categories():
    df = pd.read_csv(RAW / "category_tree.csv")
    roots = df[df["parentid"].isna()]
    children = set(df["categoryid"])
    parents = set(df["parentid"].dropna().astype("int64"))
    return {
        "total_rows": int(len(df)),
        "distinct_categories": int(df["categoryid"].nunique()),
        "root_categories": int(len(roots)),
        "parents_not_listed_as_category": int(len(parents - children)),
    }


def cross_checks(events, props, cats):
    return {
        "event_span_vs_property_span": {
            "events": [events["timestamp_min_iso"], events["timestamp_max_iso"]],
            "properties": [props["timestamp_min_iso"], props["timestamp_max_iso"]],
            "properties_start_after_events_start": props["timestamp_min_ms"]
            > events["timestamp_min_ms"],
        },
        "items_in_events_vs_properties": {
            "unique_items_in_events": events["unique_items"],
            "items_with_any_property": props["distinct_items_with_properties"],
            "items_with_categoryid": props["items_with_categoryid"],
        },
        "categories": cats["distinct_categories"],
    }


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    missing = [f for f in FILES if not (RAW / f).exists()]
    if missing:
        print(f"FAIL missing files: {missing}")
        return 1

    print("profiling files")
    files = profile_files()
    print("profiling events")
    events = profile_events()
    print("profiling item properties")
    props = profile_item_properties()
    print("profiling categories")
    cats = profile_categories()

    report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "files": files,
        "events": events,
        "item_properties": props,
        "category_tree": cats,
        "cross_checks": cross_checks(events, props, cats),
    }

    out = OUT / "dataset_profile.json"
    out.write_text(json.dumps(report, indent=2))
    print(f"\nwrote {out}\n")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
