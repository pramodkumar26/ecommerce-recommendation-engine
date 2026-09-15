# Dataset Profile

Retailrocket Recommender System Dataset. Measured facts about the local copy of the source
files, produced in Phase 2 before the stream or the recommender is designed.

Every count here must come from running code against the local files. The roadmap's figures
(roughly 2.75M events, about 1.4M visitors, 235K items, roughly 22K transactions, about 4.5
months) are expectations to verify, not values to copy in. Each confirmed count gets a
`DATASET-*` row in `docs/MEASUREMENTS.md` before it appears in the README or a resume bullet.

## Source files

| File | Contents | Use | Rows | Size | SHA256 |
|---|---|---|---|---|---|
| `events.csv` | timestamp, visitorid, event, itemid, transactionid | Kafka replay source | | | |
| `item_properties_part1.csv` | item properties over time | point-in-time enrichment | | | |
| `item_properties_part2.csv` | item properties over time | point-in-time enrichment | | | |
| `category_tree.csv` | categoryid, parentid | category hierarchy | | | |

## Event counts

```text
total rows:
unique visitors:
unique items:
view count:
add_to_cart count:
transaction count:
duplicate source rows:
missing transaction IDs:
```

## Timestamp range

```text
earliest event:
latest event:
span:
timezone / epoch format:
```

Event volume by day, week, and hour goes here once measured, along with any gaps or volume
anomalies in the source.

## Interaction histogram

Visitors bucketed by number of interactions. This drives the minimum-history rule.

| Interactions | Visitors | Percent of visitors |
|---:|---:|---:|
| 1 | | |
| 2 | | |
| 3 | | |
| 5 | | |
| 10 | | |
| 20+ | | |

## Minimum history rule

```text
chosen threshold:
visitors retained:
visitors excluded:
percent excluded:
reasoning:
```

The threshold comes from the distribution above, not from whichever value makes the metric look
best. Record the excluded count either way.

## Item properties

```text
distinct items with properties:
distinct property names:
property updates per item, median:
property updates per item, p95:
property timestamp range:
items with no property record:
```

Point-in-time join implication: for an event at time `t`, enrich with the latest property whose
timestamp is at or before `t`. Never use the end-of-dataset value. Record here how many events
fall before their item's first known property record, since those are the join misses.

## Category tree

```text
distinct categories:
root categories:
max depth:
orphan categories:
items with unknown category:
```

## Deterministic event ID

```text
SHA256(
  source_file
  + source_row_number
  + timestamp
  + visitor_id
  + item_id
  + event_type
  + transaction_id
)
```

Source file and row number are included so two legitimate rows with identical business fields
do not collapse into one ID, and so a replay of the same source produces the same IDs.

Reproducibility test:

```text
run 1 ID set hash:
run 2 ID set hash:
identical:
collisions found:
date:
```

## Interaction signal design

Purchases are sparse, so transaction-only positives may leave too few usable visitors. Starting
weights to test, not truths:

```text
view = 1
cart = 3
purchase = 5
```

Record here how many visitors survive the minimum-history filter under transaction-only
positives versus weighted interactions. That comparison decides the target design.

## Notes

Anything surprising about the source goes here: encoding issues, malformed rows, unexpected
event types, duplicated timestamps, visitors with impossible activity rates.
