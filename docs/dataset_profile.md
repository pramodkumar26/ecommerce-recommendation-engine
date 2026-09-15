# Dataset Profile

Retailrocket Recommender System Dataset. Every number here was measured from the local copy of
the source files on 2026-09-15.

Raw output: `benchmarks/raw/dataset_profile.json`, `benchmarks/raw/dataset_joins.json`,
`benchmarks/raw/event_id_reproducibility.json`. Reproduce with:

```bash
make profile
```

## Source files

| File | Rows | Bytes | SHA256 (first 16) |
|---|---:|---:|---|
| `events.csv` | 2,756,101 | 94,237,913 | `3745aa83238b1e6d` |
| `item_properties_part1.csv` | 10,999,999 | 484,315,749 | `30aad5aeca58b2dc` |
| `item_properties_part2.csv` | 9,275,903 | 408,929,907 | `d5e7d1a91dc40f52` |
| `category_tree.csv` | 1,669 | 14,454 | `94e865eb0a3d48cb` |

Full checksums are in `benchmarks/raw/dataset_profile.json`. They pin exactly which copy of the
data produced every downstream number.

## Event counts

| Measure | Value |
|---|---:|
| Total rows | 2,756,101 |
| Unique visitors | 1,407,580 |
| Unique items | 235,061 |
| Views | 2,664,312 |
| Add to cart | 69,332 |
| Transactions | 22,457 |
| Duplicate full rows | 460 |
| Transactions missing a transaction id | 0 |
| Non-transaction rows carrying a transaction id | 0 |
| Unique transaction ids | 17,672 |

The roadmap's expected figures all confirm: roughly 2.75M events, about 1.4M visitors, 235K
items, roughly 22K transactions.

Views are 96.67% of all events, add to cart 2.52%, transactions 0.81%.

The 460 duplicate rows are byte-identical on every column. They are real duplicates in the
source, and they are the reason the event ID includes source position. See the identity section.

22,457 transaction events resolve to 17,672 unique transaction ids, so a purchase averages 1.27
items. Transactions are mostly single-item.

## Timestamp range

| Measure | Value |
|---|---|
| First event | 2015-05-03T03:00:04Z |
| Last event | 2015-09-18T02:59:47Z |
| Span | 138.0 days |

Timestamps are epoch milliseconds. 138 days is consistent with the roadmap's "roughly 4.5
months".

## Interaction distribution

This is the finding that constrains the entire ML half of the project.

| Interactions | Visitors | Share of all visitors |
|---:|---:|---:|
| exactly 1 | 1,001,560 | 71.16% |
| exactly 2 | 205,992 | 14.63% |
| exactly 3 | 79,612 | 5.66% |
| exactly 5 | 22,967 | 1.63% |
| exactly 10 | 3,659 | 0.26% |
| 20 or more | 6,610 | 0.47% |

Cumulative view:

| At least N interactions | Visitors | Share |
|---:|---:|---:|
| 1 | 1,407,580 | 100% |
| 2 | 406,020 | 28.84% |
| 3 | 200,028 | 14.21% |
| 5 | 81,620 | 5.80% |
| 10 | 23,241 | 1.65% |
| 20 | 6,610 | 0.47% |

Mean interactions per visitor is 1.96, median is 1, p95 is 5, max is 7,757.

Seven out of ten visitors appear exactly once. Any recommendation metric computed over all
visitors would mostly be measuring users about whom nothing is known.

## Minimum history rule

Proposed rule: **at least 5 interactions before the held-out test event.**

| Threshold | Eligible visitors | Excluded | Excluded share |
|---:|---:|---:|---:|
| 2 | 406,020 | 1,001,560 | 71.16% |
| 3 | 200,028 | 1,207,552 | 85.79% |
| 5 | 81,620 | 1,325,960 | 94.20% |
| 10 | 23,241 | 1,384,339 | 98.35% |
| 20 | 6,610 | 1,400,970 | 99.53% |

Reasoning. A test example needs history to build a user representation from *and* a held-out
positive. At a threshold of 2 or 3, a visitor has one or two remaining events after the holdout,
which is barely more signal than a cold-start user, so the two-tower model would be evaluated on
users it cannot meaningfully represent. At 10 the eligible pool drops to 23,241 and the
evaluation starts measuring a narrow slice of unusually active visitors. Five keeps 81,620
visitors while leaving at least four events of history per user.

This excludes 94.20% of visitors, which sounds severe and is worth stating plainly rather than
hiding. It is a property of the dataset, not of the filter. The threshold is revisited in Phase
12 against the actual split, and any change gets recorded here with the new exclusion count.

## Transaction sparsity and positive signal

Only **11,719 visitors ever transacted**, which is 0.83% of all visitors.

Combining the minimum-history rule with the choice of what counts as a positive:

| Min history | Eligible visitors | Of those, have a transaction | Of those, have a cart or transaction |
|---:|---:|---:|---:|
| 2 | 406,020 | 11,649 | 36,258 |
| 3 | 200,028 | 11,064 | 29,401 |
| 5 | 81,620 | 7,610 | 18,548 |
| 10 | 23,241 | 3,790 | 8,196 |
| 20 | 6,610 | 1,624 | 3,136 |

At the proposed threshold of 5, transaction-only positives leave **7,610 evaluable visitors**.
Cart-or-transaction leaves 18,548. Any interaction leaves 81,620.

Conclusion: transaction-only training is not viable as the primary design. The roadmap flagged
this as a risk and the data confirms it. The project uses weighted interactions:

```text
view        = 1
cart        = 3
transaction = 5
```

These are experiment parameters to compare in Phase 13, not settled truths. The alternative
weightings and the transaction-only variant both get measured.

## Repeat interactions

| Measure | Value |
|---|---:|
| Distinct visitor-item pairs | 2,145,179 |
| Pairs seen more than once | 333,038 |
| Share of pairs that repeat | 15.53% |
| Most interactions on a single pair | 308 |

15.53% of pairs repeat, so the decision of whether to collapse repeated interactions or keep
them is not cosmetic. Phase 12 documents which was chosen. Keeping repeats lets interaction
weight accumulate; collapsing them prevents a single obsessive visitor-item pair from dominating.

## Item properties

| Measure | Value |
|---|---:|
| Total rows | 20,275,902 |
| Distinct items with properties | 417,053 |
| Distinct property names | 1,104 |
| Property updates per item, mean | 48.6 |
| Property updates per item, median | 42 |
| Property updates per item, p95 | 110 |
| Property updates per item, max | 468 |
| `categoryid` rows | 788,214 |
| Items with a `categoryid` | 417,053 |

Most property names are opaque numeric codes. The named ones that matter are `categoryid` and
`available`. The ten most frequent properties are `888`, `790`, `available`, `categoryid`, `6`,
`283`, `776`, `678`, `364`, and `202`.

### Properties are 18 weekly snapshots, not continuous updates

| Measure | Value |
|---|---|
| Distinct property timestamps | 18 |
| First | 2015-05-10T03:00:00Z |
| Last | 2015-09-13T03:00:00Z |
| Gaps between snapshots | 7 or 14 days |

Every property row carries one of only 18 timestamps, each at exactly 03:00:00 UTC. This is a
weekly snapshot export, not a change log.

This simplifies Phase 7 considerably. The point-in-time join is an as-of join against 18 known
dates rather than an arbitrary temporal join, which means it can be implemented as a bucketed
lookup and tested exhaustively.

## Point-in-time join feasibility

For an event at time `t`, enrichment must use the latest property known at or before `t`. Using
the final property value would leak future state into historical events.

| Measure | Events | Share |
|---|---:|---:|
| Total events | 2,756,101 | 100% |
| Events with a valid point-in-time property | 2,362,903 | 85.73% |
| **Join miss rate** | **393,198** | **14.27%** |

Causes of the misses, which overlap:

| Cause | Events |
|---|---:|
| Item has no property record at all | 255,585 |
| Event occurs before its item's first property snapshot | 137,613 |
| Event occurs before the first snapshot globally (2015-05-10) | 137,193 |
| Event occurs after the last snapshot (2015-09-13) | 78,581 |

Item coverage:

| Measure | Value |
|---|---:|
| Distinct items appearing in events | 235,061 |
| Distinct items in the property files | 417,053 |
| Event items with no property record | 49,815 |
| Event item coverage | 78.81% |

Two structural gaps worth understanding.

The property files cover more items (417,053) than appear in events (235,061), but coverage is
not a superset: 49,815 items that appear in events have no property record at all, 21.19% of
items seen in events.

Events start on 2015-05-03 while the first property snapshot is 2015-05-10, so the first week of
events has no prior property by definition. Events also run five days past the last snapshot,
which is fine because the last snapshot is still *before* those events and is a legitimate
point-in-time value.

**Policy for Phase 7:** events with no property at or before their timestamp get null
enrichment and are counted in the join miss metric. They are not backfilled from the first
snapshot that follows them. Doing so would be exactly the leakage this join exists to prevent,
and it would silently affect 137,613 events. The 14.27% miss rate becomes a tracked data-quality
metric rather than a problem to paper over.

## Category tree

| Measure | Value |
|---|---:|
| Rows | 1,669 |
| Distinct categories | 1,669 |
| Root categories | 25 |
| Parent ids not present as a category | 0 |

The tree is internally consistent, with no orphan references.

## Deterministic event ID

```text
SHA256(
  source_file | source_row_number | timestamp | visitor_id | item_id | event_type | transaction_id
)
```

Implemented in `producer/event_id.py`. Fields are joined with `|`. A missing transaction id
normalizes to the empty string, and float ids from the sparse source column normalize to
integers, so `1234.0` and `1234` produce the same ID.

Source file and row number are included deliberately. The dataset has 460 byte-identical rows.
Without position in the hash they would collapse to a single ID, and deduplication in Phase 6
would silently drop 460 real source records. Position is stable across replays because the
source files never change, so determinism is preserved.

### Reproducibility test

Two independent full passes over `events.csv`:

| Check | Result |
|---|---|
| Same row count | pass |
| Identical ID sequence | pass |
| Identical set digest | pass |
| No collisions | pass |

| Measure | Value |
|---|---:|
| Rows processed | 2,756,101 |
| Unique IDs | 2,756,101 |
| Collisions | 0 |

Both passes produced digest
`16a58583137991e1b0895a684460fea2ceab428d5a78616b07d13e2f73ad5561`.

Nine unit checks also pass, covering stability across calls, NaN and None agreeing, float and
int transaction ids agreeing, and row number, source file, event type, and transaction id
presence each changing the ID.

## Notes and surprises

The property files are weekly snapshots with only 18 distinct timestamps. Expecting a continuous
change log would have made the Phase 7 join far more complex than it needs to be.

Item property coverage is not a superset of event items. 21.19% of items seen in events have no
metadata at all, which affects both enrichment and the cold-start story in Phase 32.

Transaction ids are perfectly clean: every transaction event has one, no non-transaction event
has one. That is unusual for a public dataset and means no repair logic is needed.

The 460 duplicate rows are the reason the identity design includes source position. They were
found by measurement, not assumed.

Event volume runs 7 days before the first property snapshot and 5 days after the last, which
sets the boundaries of what can be enriched.
