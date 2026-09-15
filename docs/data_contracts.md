# Data Contracts

Topic design and event structure. Avro schemas and Schema Registry compatibility rules are
added in Phase 4; this currently describes the Phase 3 JSON payload.

## Topics

Defined in `kafka/topics/topics.yml`, created by `kafka/create_topics.py`, which is idempotent.

| Topic | Partitions | RF | Retention | Purpose |
|---|---:|---:|---:|---|
| `item_view` | 3 | 1 | 7 days | `view` events |
| `add_to_cart` | 3 | 1 | 7 days | `addtocart` events |
| `transaction` | 3 | 1 | 7 days | `transaction` events |
| `clickstream_dlq` | 3 | 1 | 30 days | malformed or incompatible records, Phase 4 |

`KAFKA_AUTO_CREATE_TOPICS_ENABLE` is false on the broker. A typo in a topic name is an error
rather than a silently created topic.

### Why three topics rather than one

Event types have different volumes and different downstream consumers. Views are 96.67% of
traffic, transactions 0.81%. Separate topics let a consumer that only cares about purchases
avoid reading 2.66 million view records, and let retention or partition counts diverge later
without a migration.

### Why 3 partitions

The broker is single node, so this is about parallelism and making partition behaviour visible,
not durability. Three allows up to three consumers in a group to work in parallel and is enough
for partition assignment to be observable in tests without adding local overhead.

Replication factor is 1 because there is one broker. Production would use 3.

### Why 7 and 30 day retention

Seven days on behavioral topics means a replay or restart test can rewind across several work
sessions rather than finding the data expired. The DLQ keeps 30 days because bad records are
evidence, and Phase 10 inspects them well after they were produced.

## Partitioning

Records are keyed by `visitor_id` as a UTF-8 string. Kafka hashes the key to select a
partition.

Kafka guarantees ordering **within** a partition, not across partitions. Keying by visitor means
every event for one visitor lands on one partition in produce order, so per-visitor session and
sequence logic can rely on it. Keying randomly or by item would scatter a visitor's history
across three partitions and lose that guarantee.

Verified in Phase 3: 11,420 distinct visitor keys across 20,000 records, zero keys spanning more
than one partition.

Kafka partitioning is not Delta partitioning. Delta partitions by time (Phase 7). Partitioning
Delta by visitor id would produce a high-cardinality layout with 1.4 million directories.

## Event payload, schema version 1

JSON in Phase 3. Phase 4 replaces this with Avro registered in Schema Registry.

```json
{
  "event_id": "sha256 hex",
  "visitor_id": 257597,
  "item_id": 355908,
  "event_type": "view",
  "event_timestamp": 1433221332117,
  "ingestion_timestamp": 1789503301234,
  "transaction_id": null,
  "schema_version": 1,
  "source_file": "events.csv",
  "source_row_number": 1203847
}
```

| Field | Type | Notes |
|---|---|---|
| `event_id` | string | SHA256, see `producer/event_id.py` |
| `visitor_id` | long | also the partition key |
| `item_id` | long | |
| `event_type` | string | `view`, `addtocart`, `transaction` |
| `event_timestamp` | long | epoch ms, original 2015 source time |
| `ingestion_timestamp` | long | epoch ms, when the producer emitted it |
| `transaction_id` | string or null | present only on `transaction` events |
| `schema_version` | int | 1 |
| `source_file` | string | provenance |
| `source_row_number` | long | provenance, and part of `event_id` |

### Why both timestamps

`event_timestamp` is when the behaviour happened, in 2015. `ingestion_timestamp` is when the
record entered the pipeline, which is now. Keeping both is what makes Phase 5's event-time
versus processing-time work possible, and what watermarks in Phase 6 operate on.

The 138 days of source time are replayed in minutes, so the gap between the two is large and
deliberate.

## Simulator behaviour

`producer/simulator.py`.

### Ordering

The source file is **not** sorted by timestamp. 1,377,377 adjacent row pairs are out of order,
roughly half the file. The simulator sorts by `(event_timestamp, source_row_number)` before
replaying.

This is deliberate. If the input arrived already jumbled, a passing watermark test could not be
distinguished from a favourably shuffled input. Sorting makes the baseline strictly ordered, so
out-of-order arrival becomes something injected at a known rate with a fixed seed. Verified:
baseline replay produces zero out-of-order pairs.

### Determinism contract

For a fixed `--seed` and a fixed bounded range, the emitted sequence is byte-identical across
runs, except `ingestion_timestamp` which is wall clock by definition.

All injection decisions are made up front from the seeded RNG, so the plan is a pure function of
the seed. Delays shift an event by a number of **positions**, not by wall-clock time, so emitted
order does not depend on how fast the machine happens to run.

### Controls

| Flag | Effect |
|---|---|
| `--limit`, `--start-row` | bounded range after sorting |
| `--rate` | target events per second |
| `--burst-rate`, `--burst-every`, `--burst-duration` | periodic bursts |
| `--seed` | fixes every injection decision |
| `--duplicate-rate` | fraction of records sent twice |
| `--malformed-rate` | fraction corrupted, for the Phase 4 DLQ |
| `--delay-rate`, `--max-delay-positions` | fraction deferred, creating out-of-order arrival |
| `--dry-run PATH` | write JSONL instead of producing to Kafka |

### Malformed record modes

Chosen from the seeded RNG:

- `missing_field`: `visitor_id` removed
- `wrong_type`: `event_timestamp` set to a string
- `truncated_json`: payload cut in half, invalid JSON

Phase 4 routes all three to `clickstream_dlq` with the error type recorded.

### Producer configuration

```text
acks=all
enable.idempotence=true
retries=5
compression.type=snappy
linger.ms=20
batch.num.messages=10000
```

Idempotence plus `acks=all` means a producer-side retry does not create a duplicate record.
That matters because the project injects duplicates on purpose and needs to know that every
duplicate observed downstream was one it created, not an artefact of retry.
