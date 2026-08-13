<!--
Copyright 2026 laughingman7743

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ADR-0101: The Spanner Change Streams reader bounds asynchronous partition queries

- Status: Accepted
- Date: 2026-08-12; revised 2026-08-13
- Issues: [#222](https://github.com/laughingman7743/flink-connector-gcp/issues/222),
  [#536](https://github.com/laughingman7743/flink-connector-gcp/issues/536),
  [#535](https://github.com/laughingman7743/flink-connector-gcp/issues/535),
  [#551](https://github.com/laughingman7743/flink-connector-gcp/issues/551),
  [#554](https://github.com/laughingman7743/flink-connector-gcp/issues/554),
  [#581](https://github.com/laughingman7743/flink-connector-gcp/issues/581),
  [#635](https://github.com/laughingman7743/flink-connector-gcp/issues/635)
- Modules: spanner (`source`, `source.changestream.reader`)
- Current behavior: [Change Streams source](../content/docs/connectors/datastream/spanner.md#change-streams-source)

## Context

ADR-0099 fixes the coordinator-facing partition and event protocol but leaves the reader's concurrency, decoding, and checkpoint boundary open.
One Flink subtask must make progress on several Spanner partitions without letting callback threads emit into Flink's output or accumulate an unbounded queue.

The two database dialects expose the same logical records through different physical results.
GoogleSQL returns a nested `ARRAY<STRUCT>` envelope, while PostgreSQL returns the read function's JSON representation.
The type descriptor inside a data record is recursive and can contain service types or annotations that the pinned client library does not model as a current enum value.

Several records can share one commit timestamp.
A restored query can therefore resume inclusively and repeat its timestamp boundary, or advance past that boundary and risk skipping records that were not emitted before the checkpoint.

## Decision

`SpannerChangeStreamSource` is a continuous FLIP-27 source with a collector-based, zero-to-many deserialization SPI over the connector-owned `DataChangeRecord` model.
The model carries every documented data-change field.
It stores each recursive column type descriptor as normalized JSON with object members sorted recursively and keeps an absent mod value distinct from an explicit JSON `null`.
`DataChangeRecord` supplies its own Flink type information and field serializer, so `TypeInformation.of(DataChangeRecord.class)` does not fall back to reflective Kryo.
The serializer writes the connector-owned fields directly, treats the model as immutable for copies, and versions its snapshot independently of JDK implementation classes.

Each reader subtask opens at most `maxConcurrentQueriesPerSubtask` partition queries, default eight.
The reader starts assigned splits until it reaches that bound, keeps excess restored splits in a FIFO, and checkpoints both active and queued splits.
The configured job-wide capacity is source parallelism multiplied by this value; it is not presented as a Spanner quota.

Each partition query uses `executeQueryAsync` on a strong single-use read-only transaction.
The query applies the configured RPC priority and does not enable Data Boost.
Its callback hands over one record, completion, or failure and returns `PAUSE`; the mailbox calls `resume()` only after consuming that result.
This one-slot handover bounds memory and keeps all Flink output and coordinator events on the task thread.

The GoogleSQL and PostgreSQL decoders map their physical results into the same internal record variants.
Data records that pass the output filters go through the user deserializer, which may emit zero or more outputs through a Flink `Collector`.
Every output from one data record carries that record's commit timestamp as its Flink event timestamp, and returning successfully without emitting increments `recordsSkipped` once.
The reader advances split progress only after deserialization returns successfully, so a failure retains the record for at-least-once replay.
Heartbeat records advance the partition watermark reported to the coordinator.
The coordinator computes the complete-ledger minimum described by ADR-0099 and broadcasts that source-wide frontier to every reader.
Readers emit it through the main source output instead of a split output, so a scheduled or queued partition cannot disappear from Flink's watermark minimum merely because no query is running for it.
Spanner guarantees that records after heartbeat instant `H` have timestamps greater than `H`, but Flink timestamps have millisecond precision and treat events at or below watermark `W` as late.
The connector therefore emits `H.toEpochMilli() - 1`, with saturation at `Long.MIN_VALUE`, because a later nanosecond instant can truncate to the same millisecond as `H`.
It does not mark quiet partitions idle.
Child-partitions records become coordinator events, and successful query completion becomes a separate partition-finished event.
A query error fails the task without reporting completion or dropping the split.

Optional table and column filters run on the mailbox thread after dialect-specific decoding and before the user deserializer.
Each Java regular expression fully matches the Spanner-reported table name or a `table.column` identifier.
Include and exclude lists are mutually exclusive within each scope.
Column projection retains primary keys and their type descriptors, and removes every other rejected column consistently from `columnTypes` and each mod's old and new value objects.
The projection uses only the current record's metadata, preserves absent values separately from explicit JSON `null`, and never consults a historical schema.

A table-filtered record does not call the deserializer.
A record whose reported non-key values are all removed is delivered with empty projected value objects by default.
The explicit `skipMessagesWithoutChange` option skips that record instead.
Both outcomes still advance the split position and report progress to the coordinator.
Heartbeat and child-partitions records bypass output filtering entirely.

The reader checkpoints the greatest consumed record timestamp and watermark for each active split.
Restore uses that timestamp as the next query's inclusive start.
This can repeat the boundary record and gives the source at-least-once delivery; advancing past it could skip another record with the same commit timestamp.
On restore, the reader queues but does not open those splits until the coordinator has validated their positions against the effective retention.
An explicit whole-ledger fallback tells the reader to discard that queue and request the replacement null-token split.

The enumerator counts child partitions when it first accepts them and reports both scheduled-partition count and oldest scheduled-position lag.
Each reader reports successful query opens, currently active queries, queued assigned partitions, oldest queued-position lag, missed heartbeat intervals, and the wait for the latest non-heartbeat result.
It also counts table-filtered records, records skipped without a projected change, and column metadata or value occurrences removed from records passed to the deserializer.
The gauges aggregate partition state within their coordinator or reader-subtask scope and never use partition tokens as labels.
The source emits commit timestamps and the coordinator frontier through Flink's source output so the runtime supplies `numRecordsIn`, `currentEmitEventTimeLag`, `watermarkLag`, and `sourceIdleTime`; the connector does not duplicate those names.

## Evidence

Decoder fixtures cover every data field, recursive type descriptors, `TOKENLIST`, an unknown future code, absent versus explicit JSON `null`, heartbeats, and child partitions for both dialect shapes.
Reader and coordinator tests drive the concurrency bound, excess restored splits, the one-slot pause and resume, zero-, one-, and multi-output deserialization, commit timestamps, failure-before-progress, complete-ledger watermarks, child-before-finish ordering, query failure, and bounded completion.
Filter tests cover full-match identifiers, table-local column names, primary-key retention, consistent metadata and mod projection, empty-projection delivery and skipping, restored progress with changed filters, and distinct counters.
Serializer tests obtain the type through `TypeInformation.of`, round-trip every record field and projected collections, and reject an unknown serializer snapshot version without opening JDK modules.
Metric tests use a deterministic clock to cover query lifecycle, queue and heartbeat transitions, future timestamps, and overflow without waiting on wall-clock time.
The source rescaling test restores six partition queries through parallelism one, three, and one, proves an even scale-out at two slots per reader, proves a later scale-in preserves the positions of four queued splits, and observes a non-regressing source watermark after each restore.
Emulator MiniCluster tests run the production source for both dialects across a schema and value-capture change.
They also project a non-key column before deserialization in both dialects.
A separate failover test restarts each dialect and requires every record plus a repeated inclusive boundary.
The gated service run verifies GoogleSQL STRUCT and PostgreSQL JSON decoding, commit timestamps, heartbeat watermarks, bounded query metrics, intentional checkpoint recovery, and savepoint recovery in both dialects.
At 5,000 unique mutation ids, the measured GoogleSQL run repeated 500 deliveries across the intentional failure and still recovered every id; duplicate volume is evidence for that run, not a promised fixed boundary size.

## Alternatives declined

- **Run one blocking query per subtask.**
  Source parallelism would also become the query cap, and a quiet partition would prevent the subtask from serving another assigned partition.
- **Let callbacks enqueue freely.**
  The service could outrun the mailbox and make memory proportional to unread change records rather than configured query capacity.
- **Flatten the type descriptor to the client library enum.**
  That would reject or erase `TOKENLIST`, nested descriptor fields, annotations, and service types added before the client library learns them.
- **Resume after the checkpoint timestamp.**
  Commit timestamps are not unique per record, so an exclusive boundary can skip records from the same transaction or partition.
- **Treat builder filters as server-side selection.**
  The Change Streams read function still returns the complete record, so only the Change Stream DDL watch definition can prevent an excluded value from entering the source process.
- **Drop every empty projected record.**
  A record can still identify transaction activity after all reported non-key values are removed, so dropping it requires an explicit option.

## Consequences

- Increasing the concurrency option spends one callback thread, one transaction, and one active query per additional slot in each subtask.
- A restart can repeat records at the last checkpointed timestamp, so downstream consumers that need uniqueness must deduplicate.
- Changing output filters on restore does not alter checkpointed source positions; the new filters apply from those positions, including any boundary records repeated by the source's normal inclusive restore.
- Connector-side filters do not reduce Spanner processing, query concurrency, or traffic into Flink.
- The emulator proves connector wiring and both dialect adapters; service partitioning, retention, and fallback are covered separately by the gated #535 acceptance.
- The connector exposes capacity and lag signals but does not change Flink operator parallelism; deployment autoscaling remains outside the Source API.
