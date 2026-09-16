# Data Contracts

Topic design, event schemas, compatibility rules, and dead letter queue behaviour.

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

## Event schema, version 1

Avro, registered in Schema Registry. Source of truth is
`producer/schemas/clickstream_event_v1.avsc`.

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
| `event_type` | enum `EventType` | `view`, `addtocart`, `transaction`. An enum rather than a string so an unknown value fails at serialization |
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

## Schema Registry

Subject naming is `TopicNameStrategy`, so each topic gets `<topic>-value`. All three behavioral
topics register the same `ClickstreamEvent` schema today, but separate subjects mean one could
diverge later without touching the others.

| Subject | Schema |
|---|---|
| `item_view-value` | ClickstreamEvent |
| `add_to_cart-value` | ClickstreamEvent |
| `transaction-value` | ClickstreamEvent |
| `clickstream_dlq-value` | DlqRecord |

Registry compatibility level: `BACKWARD`. A new schema must be able to read data written with
the previous one.

### Why timestamps are plain longs and not `timestamp-millis`

Avro has a `timestamp-millis` logical type and using it would be the more idiomatic choice. It
is deliberately not used here.

fastavro decodes `timestamp-millis` into timezone-aware `datetime` objects, and Spark's Avro
reader maps it to `TimestampType` with session-timezone conversion applied. Phases 5 and 6
depend on exact event-time semantics for watermarks and windowing, and a silent one-hour shift
introduced by a timezone default would be extremely hard to detect and would quietly corrupt
every windowed aggregate.

Keeping raw epoch milliseconds means the conversion to a timestamp happens once, explicitly,
where the timezone is stated. The field docs record this.

### Schema evolution

Three versions exist. Only v1 and v2 are ever registered.

| Version | Change | Compatible under BACKWARD |
|---|---|---|
| v1 | baseline, ten fields | n/a |
| v2 | adds optional `session_id` and `device_type`, both with `null` defaults | yes |
| v3 | changes `item_id` from `long` to `string` | no, rejected by the registry |

v2 is backward compatible because a reader using v2 encounters old records with the new fields
absent and falls back to the declared defaults. Adding a field *without* a default would break
this.

v3 is rejected because `long` and `string` are not promotable in Avro schema resolution, so a
v3 reader could not read any existing v1 data. The registry refuses the registration outright.
v3 exists only to prove the guard works and is never registered.

Verified: registry returns compatible for v2, incompatible for v3.

### Forward read, v2 data through a v1 reader

Separately from registration, a consumer pinned to v1 can still read records written with v2.
Avro schema resolution drops writer fields the reader does not declare. Verified: a v2 record
carrying `session_id` and `device_type` decodes through a v1 reader into exactly the ten v1
fields, with the two extra fields silently discarded.

This is what lets producers and consumers be upgraded independently rather than in lockstep.

## Dead letter queue

`kafka/validation.py` holds the rules, `kafka/dlq_router.py` applies them. Phase 5's Spark job
reuses the same validation module, so a record judged bad by one is judged bad by the other.

### Three failure layers, three error types

| `error_type` | What failed | Example message |
|---|---|---|
| `deserialization_failed` | bytes are not a valid Confluent Avro frame | `SerializationError: Invalid magic byte` |
| `unknown_schema_id` | framing is valid, the schema id was never registered | `Schema 964304 not found (HTTP 404, SR code 40403)` |
| `validation_failed` | decodes cleanly, breaks a business rule | `non-transaction event carries transaction_id 'not-a-real-transaction'` |

They are separated because they mean different things operationally. A deserialization failure
usually means a non-Avro producer is writing to the topic. An unknown schema id usually means a
producer is ahead of the registry or pointing at the wrong one. A validation failure means the
data itself is wrong, which is a data-quality problem rather than a plumbing one.

### Validation rules

Every rule comes from the Phase 2 measurements, not from assumption.

| Rule | Basis |
|---|---|
| `event_id` is 64 hex characters | SHA256 by construction |
| `visitor_id` and `item_id` are non-negative | source contains no negatives |
| `event_timestamp` within [1430622004384, 1442545187788] | measured source range |
| `ingestion_timestamp` positive | producer sets it |
| `event_type` is one of the three known values | enum in the schema |
| topic matches event type | `item_view` carries only `view`, and so on |
| transaction events have a `transaction_id`, others do not | measured, 0 exceptions in 2,756,101 rows |

That last rule is only safe to enforce because Phase 2 confirmed it holds perfectly in the
source. Guessing it would have been wrong.

### DLQ record contents

Defined in `producer/schemas/dlq_record.avsc`:

```text
original_payload      exact bytes as they arrived, so the failure can be reproduced
error_type            one of the three above
error_message         truncated to 2000 characters
source_topic
partition
offset
message_key
ingestion_timestamp   when the router wrote the DLQ record, not when the event happened
schema_version        null when the payload could not be decoded far enough to tell
```

Keeping `original_payload` as raw bytes is what makes a DLQ record actionable. Without it you
know something failed but cannot reproduce it.

### Measured behaviour

Over 20,422 consumed records with a 5% malformed injection rate:

| Measure | Value |
|---|---:|
| Consumed | 20,422 |
| Valid, passed through | 19,462 |
| Routed to DLQ | 960 |
| DLQ rate | 4.70% |
| `deserialization_failed` | 312 |
| `unknown_schema_id` | 320 |
| `validation_failed` | 328 |

`valid + dlq == consumed` exactly, so nothing was lost or invented. The valid stream kept
flowing throughout, which is the property this phase exists to prove.

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

Chosen from the seeded RNG, one per decoding layer so each maps to a distinct DLQ error type:

| Mode | Produces | DLQ error type |
|---|---|---|
| `raw_garbage` | 24 random bytes, no Confluent magic byte | `deserialization_failed` |
| `unknown_schema_id` | valid framing, schema id in the 900000 range | `unknown_schema_id` |
| `invalid_field` | valid Avro that breaks a business rule | `validation_failed` |

`invalid_field` further picks one of `negative_visitor`, `zero_timestamp`, or
`orphan_transaction_id`.

A record can be both malformed and duplicated, in which case the same bad payload appears twice
on the wire. The simulator reports `malformed injected` (distinct records) and `malformed
records on the wire` (including duplicate copies) separately, because DLQ counts must be
compared against the second number.

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

## Streaming reliability policy

Set in Phase 6, measured rather than assumed.

### Pipeline shape

```text
Kafka  -> bronze_events     stateless, keeps everything including duplicates
Bronze -> deduped_events    dropDuplicates on event_id, 24 hour watermark
deduped -> metrics_5min     5 minute windows, update mode, MERGE on window_start
```

Three queries rather than one because Spark supports chaining multiple stateful operators only
in append mode, and append makes a window wait for the watermark before emitting. With a 24 hour
watermark that lags the aggregate by a full day of event time, so on a bounded replay almost
nothing closes. Splitting gives each query at most one stateful operator.

### Watermark

24 hours, chosen from measurement.

| Quantile | Lateness |
|---|---:|
| p50 | 104 min |
| p90 | 737 min |
| p95 | 845 min |
| p99 | 916 min |
| max | 959 min (16.0 h) |

Nothing exceeded 24 hours at any batch size tested. The roadmap's suggested 10 minute
development watermark would have dropped 73% of records here.

This is event-time lateness created by replay compression, not network delay. 138 days of 2015
are replayed in minutes, so records legitimately sit hours behind the running maximum. A live
deployment ingesting real events would see seconds and would use a watermark of minutes. The
watermark has to match how the source actually delivers event time.

Counterintuitive and measured: smaller batches produce more lateness. p50 is 220 minutes at
1,000 per trigger and 0 at 20,000, because with fewer, larger batches most records have no
preceding maximum to be late against.

### Deduplication

By `event_id`, the deterministic hash from Phase 2. The same source row always produces the same
id, so a replayed record is recognisable.

The 460 byte-identical rows in the source survive deduplication, correctly. Source position is
part of the hash, so they are distinct records rather than replay duplicates.

The watermark bounds dedup state. Without it Spark would remember every id ever seen. The
tradeoff is that a duplicate arriving more than 24 hours after the original will not be caught.

Measured: 2,389 injected duplicates removed, all 777 windows matching the source exactly. With
deduplication disabled the same run inflates by exactly 2,389 events across 626 windows.

### Late event policy

| Case | Behaviour |
|---|---|
| within the watermark | deduplicated and counted normally |
| beyond the watermark | dropped from the deduplicated stream, still present in Bronze |

Dropped events are recoverable, because Bronze is immutable raw history and Phase 8 reprocesses
from it. That asymmetry is deliberate: durable history keeps everything, the live aggregate is
bounded.

Measured with 10% additional delay injected on top of replay lateness: 0.560% dropped at a 24
hour watermark, 3.448% at 1 minute. With normal replay traffic and no injected delay, the 24
hour watermark drops nothing.

### Restart behaviour

Kafka and Spark checkpoints do different jobs, and the restart test makes the difference
concrete. Kafka retains the messages regardless of what any consumer does. The Spark checkpoint
records how far this query got and what its stateful operators held. Delete the checkpoint and
the query reprocesses from `startingOffsets`; keep it and the query resumes exactly where it
stopped.

Measured: driver killed with `pkill` at 19,980 of 50,000 rows, no clean shutdown, 30,020 events
remaining. After restart against the same checkpoint, exactly 50,000 Bronze rows, 50,000
distinct event ids, 50,000 deduplicated rows, 777 windows. Zero loss, zero duplicate inflation.
