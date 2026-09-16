# Lakehouse Layers

Bronze, Silver, and Gold on the local Delta volume. Built in Phase 7.

## Layer responsibilities

| Layer | Contents | Keeps duplicates | Enriched |
|---|---|---|---|
| Bronze | immutable raw decoded events | yes | no |
| Silver | cleaned, deduplicated, point-in-time enriched | no | yes |
| Gold | analytics marts and facts | no | inherits Silver |

Bronze keeps everything on purpose. It is the replay source: Phase 8 reprocesses from it, and
Phase 6 measured that events dropped by the streaming watermark are still recoverable there.
Cleaning on the way in would make that impossible.

Silver reads Bronze, not the streaming `deduped_events` table, so Silver is reproducible from
Bronze alone. That is what makes a bounded rebuild meaningful.

## Contract validation between Bronze and Silver

Bronze is decoded history and is NOT clean. Contract validation sits between Bronze and Silver,
so a record that decodes as valid Avro but violates a business rule never reaches Silver.

```text
Kafka
  |
  v
BRONZE                      immutable decoded history, keeps duplicates and invalid records
  |
  +-- undecodable ------->  wire-level failures: bad framing, unregistered schema id
  |
  v
contract validation
  |
  +-- silver_rejected --->  decoded but contract-invalid, categorised
  |
  v
SILVER                      valid, deduplicated, normalised, point-in-time enriched
```

### Why the rules exist twice

`kafka/validation.py` is row-at-a-time Python used by the Phase 4 harness.
`build_silver.rejection_reason` is a Spark column expression used by the pipeline.

A Spark job cannot call the Python validator per row without paying serialization on every
record, and a UDF would forfeit predicate pushdown and Catalyst optimisation entirely. So the
rules are expressed twice: once as control flow, once as a column expression.

Duplicated logic drifts, so `tests/test_validation_parity.py` feeds the same crafted records to
both and asserts identical verdicts, including the reason. It also asserts every rule has at
least one failing case, because a rule with no test is a rule nobody has proven fires.

One documented asymmetry: the Kafka harness can catch a topic/event-type mismatch because it
knows which topic a record arrived on. Silver reads Bronze after routing has happened, so it
does not re-apply that rule.

### Two reject surfaces, deliberately distinct

| Surface | Catches | Owner |
|---|---|---|
| `clickstream_dlq` and Spark `undecodable` | bytes that are not valid Confluent Avro framing, or carry an unregistered schema id | wire level |
| `silver_rejected` | records that decoded cleanly and then broke a business rule | contract level |

The overlap is `validation_failed`, classified from Kafka by the harness and again in Spark by
Silver. The parity test is what keeps those two classifications identical.

Measured on a 30,000 event fixture with 6% malformed injection: 1,170 wire-level records never
reached Silver at all, and 610 decodable but contract-invalid records were rejected and
categorised exactly as injected, 203 / 192 / 215.

### Schema id validation in Spark

Spark's `from_avro` takes a static schema string and has no Schema Registry integration. Left
alone it strips the 5 byte Confluent header, ignores the schema id entirely, and decodes the
payload with whatever schema it was handed. A record claiming schema 964304 would decode
"successfully" against schema 3 and look completely valid downstream.

Measured: 613 unknown-schema-id records reached Bronze before this was fixed.

The job now fetches the registered schema ids from the registry once at startup and rejects
records whose declared id is not among them. Resolved eagerly rather than per batch, because a
registry outage should not silently widen what the pipeline accepts.

## Silver cleaning steps

1. Deduplicate by `event_id`, the deterministic hash from Phase 2
2. Reject records failing the business rules below, into `silver_rejected` with a reason
3. Point-in-time join to `dim_items_scd` for `categoryid` and `available`
4. Join enriched categories to `dim_categories` for the parent
5. Partition by `event_date`

### Rejection rules

Identical to `kafka/validation.py`, so a record judged bad by the streaming DLQ router is judged
bad the same way in batch. Every rule was measured on the source in Phase 2 rather than assumed.

| Reason | Rule |
|---|---|
| `event_id_not_sha256` | must match 64 hex characters |
| `negative_visitor_id` | non-negative |
| `negative_item_id` | non-negative |
| `event_timestamp_out_of_range` | within the measured source range |
| `ingestion_timestamp_invalid` | positive |
| `unknown_event_type` | one of view, addtocart, transaction |
| `transaction_without_transaction_id` | transactions carry an id |
| `non_transaction_with_transaction_id` | others do not |

The last two are only safe to enforce because Phase 2 measured them holding with zero exceptions
across 2,756,101 rows.

## Point-in-time enrichment

The part of this phase that matters most, because leakage here is nearly invisible.

An event on 2015-06-01 must be enriched with the item's category **as it was known on or before
that date**. Using the item's final September value tells a model something that had not happened
yet. It happens during enrichment rather than at the train/test split, which is where people
usually look for leakage.

### dim_items_scd

Phase 2 measured that `item_properties` is not a change log. All 20,275,902 rows carry one of
only **18 timestamps**, all at 03:00:00 UTC, spaced 7 or 14 days apart. It is a weekly snapshot
export.

`batch/jobs/build_item_scd.py` collapses consecutive snapshots where a value did not change into
half-open validity intervals:

```text
item 12345, categoryid=1338, valid_from 2015-05-10, valid_to 2015-05-24
item 12345, categoryid=1401, valid_from 2015-05-24, valid_to NULL     (current)
```

Half-open `[valid_from, valid_to)` so an event exactly on a boundary takes the newer value, which
is what "latest known at or before t" means.

Measured: 2,291,853 tracked property rows collapse to 1,031,473 intervals, 2.22x compression.
Categories barely change: 442,672 intervals over 417,053 items, 1.06 per item. Open intervals
total 834,106, exactly 417,053 items times 2 properties, confirming every item-property pair has
precisely one current interval.

### The rule that is easy to get wrong

An item's first interval starts at its **first snapshot**, not at the beginning of time. Events
before that have no known property and stay NULL.

The tempting shortcut is to backfill them from the next snapshot forward. That is exactly the
leakage the join exists to prevent, and Phase 2 measured it would silently affect 137,613 events.

Unenriched rows carry `enrichment_status = no_property_at_or_before_event` so the gap is visible
rather than indistinguishable from a join bug. Phase 10 tracks it as a data-quality metric.

### Verification

`scripts/verify_silver.py` recomputes the expected enrichment with pandas `merge_asof`, the
canonical backward as-of join, directly from the CSVs, then compares event by event. Spark does
not grade itself.

Measured on 100,000 events straddling the first snapshot: Spark and pandas both enriched exactly
31,408 rows, zero disagreements, zero rows enriched from the future. The closest case was an
event 9,663 ms after its property snapshot, positive, so nothing leaks at the boundary.

### Two different miss rates, and why conflating them looks like a bug

Running the full 2,756,101 events produced a 23.835% miss rate against Phase 2's 14.266%, a 9.5
point gap that appeared to be a regression. It was not. The two figures answer different
questions.

| Question | Miss rate | Metric |
|---|---:|---|
| Does the item have **any** property at or before the event, across all 1,104 property names? | 14.266% | ENRICH-ANYPROP-001 |
| Does it have a **`categoryid`** at or before the event? | 23.835% | ENRICH-CATEGORY-001 |

263,730 events involve an item that already has some property recorded but does not yet have a
category. Phase 2 measured the broader question; enrichment needs the narrower one.

The broader figure is retained as the **correctness check**: reproducing it in Spark gives
14.266%, a difference of 0.000 points from the independent pandas measurement, with every
component matching exactly (255,585 items with no property, 137,613 events preceding their
item's first property). Two independent implementations agreeing to the record is what makes
the join trustworthy.

The narrower figure is the one to quote for enrichment coverage. Quoting 14.266% would overstate
coverage by 9.6 points.

Zero rows enriched from the future across all 2,756,101, and the unenrichable rows stay NULL.

Note the 68.6% seen on the 100,000-row fixture is neither of these. That slice sits early in the
timeline where most items have no snapshot yet, and is not comparable to a full-dataset figure.

## Gold marts

| Table | Grain |
|---|---|
| `fact_events` | one row per event |
| `fact_transactions` | one row per transaction line |
| `mart_item_funnel` | one row per item, view to cart to purchase |
| `mart_category_performance` | one row per category, enriched rows only |
| `mart_daily_item_metrics` | one row per item per day |

`transaction_id` is **not** a key in `fact_transactions`. Phase 2 measured 1.27 items per
transaction id, and this fixture shows 639 rows over 519 distinct ids.

`mart_category_performance` covers only enriched rows and the unattributed share is reported
alongside it rather than hidden.

User and item feature tables are Phase 11, where leakage rules get their own tests.

## Partitioning

Silver, Bronze, and the time-grained Gold tables partition by `event_date`.

Partitioning by `visitor_id` would produce 1.4 million directories, which is a high-cardinality
layout that makes metadata handling worse than the scans it saves. Kafka partitions by visitor
and Delta partitions by time; they solve different problems.

### Measured, including the part that does not flatter the design

| Measure | Partitioned | Unpartitioned |
|---|---:|---:|
| Files in table | 15 | 6 |
| Files scanned, one day | 1 | 5 |
| Rows matched | 16,178 | 16,178 |
| Count, median | 214.3 ms | 182.4 ms |
| GroupBy, median | 166.0 ms | 157.0 ms |

Pruning demonstrably works: 1 file instead of 5. But the partitioned table is **15% slower** in
wall time here, because partitioning produced 15 small files instead of 6 and per-file overhead
dominates at 100,000 rows, while Spark's fixed startup of roughly 150 ms swamps the I/O saved.

The mechanism is correct and the benefit scales with data volume. At this size it has not
arrived. Do not claim partitioning made queries faster on the basis of this run.

## Bounded rebuild

`build_silver.py --start-date --end-date` rebuilds only the requested partitions using Delta
`replaceWhere`. Without it, `mode("overwrite")` would replace the whole table.

Measured: rebuilding 2 of 7 partitions reproduced both byte-identically and left the other 5
untouched, verified with an order-independent per-row hash so a rewrite preserving content is
provably identical regardless of file layout. Total rows unchanged, Delta version advanced 0 to 1.

That is the property Phase 8 depends on: Silver is a pure function of Bronze plus the SCD.

## Replay mode versus live mode

The aggregation semantics here are tuned for compressed historical replay. They are not what a
live deployment would use, and the difference is deliberate rather than an oversight.

### Historical replay mode, what this pipeline runs

138 days of 2015 are pushed through in minutes, so a record can sit hours behind the running
maximum event time purely from compression rather than from anything being slow. Modeled replay
lateness reached p95 845 min and a maximum of 959 min.

Consequences:

- the dedup query carries a 24 hour watermark, above the modeled maximum
- the aggregate carries NO watermark, because applying one drops real data. Phase 5 measured
  windows holding roughly 120 Bronze events emitting 1 or 2
- update mode plus a MERGE sink keeps every record and collapses repeated emissions
- state is bounded in practice because a replay is finite

### Live mode, intended production behaviour, not implemented

Events would arrive near wall-clock time, so real lateness is seconds to minutes.

- the aggregate SHOULD carry a watermark, sized from measured live lateness
- window state is then evicted normally and an indefinitely running query stays bounded
- append mode becomes viable and the MERGE sink is no longer needed for correctness

Switching modes is a deliberate change, not a config tweak, because it changes what happens to
late data. Phase 7 preserves replay behaviour only.

## Data loss configuration

`failOnDataLoss` defaults to **true**. When Kafka has aged out records a query has not consumed,
false makes Spark log a warning and continue with a gap; true fails loudly. A project whose
reliability claim is "no silent loss" cannot have silent loss as its default.

The opt-out is `--allow-data-loss`, for the one case where it is genuinely right: after
deliberately deleting and recreating topics, an old checkpoint references offsets that no longer
exist and the query cannot start at all.

Related: `run_bounded` now calls `query.exception()` and raises. It previously treated a dead
query and a finished one identically, so a query killed by executor memory pressure truncated a
run at 3 of 5 batches and the job still exited zero reporting success.

## Persistence and rebuildability

Delta lives on a bind mount at `./data/delta`, not on a container filesystem, so it survives
`docker compose down`. Verified with zero containers running and 14 tables still on the host.

- Gold rebuilds entirely from Silver
- Silver rebuilds from Bronze plus the item property source
- no manual state is required anywhere

Verified by fingerprinting every table before restart, after restart, and after a full rebuild.
Fingerprints are order-independent sums over per-row hashes, so a rebuild writing identical
content into a different file layout compares equal while any content change does not. All 10
tables identical at every stage.
