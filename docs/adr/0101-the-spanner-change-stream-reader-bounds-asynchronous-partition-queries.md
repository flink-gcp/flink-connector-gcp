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
- Date: 2026-08-12
- Issues: [#222](https://github.com/laughingman7743/flink-connector-gcp/issues/222),
  [#536](https://github.com/laughingman7743/flink-connector-gcp/issues/536)
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

`SpannerChangeStreamSource` is a continuous FLIP-27 source with a nullable-return deserialization SPI over the connector-owned `DataChangeRecord` model.
The model carries every documented data-change field.
It stores each recursive column type descriptor as normalized JSON with object members sorted recursively and keeps an absent mod value distinct from an explicit JSON `null`.

Each reader subtask opens at most `maxConcurrentQueriesPerSubtask` partition queries, default eight.
The reader starts assigned splits until it reaches that bound, keeps excess restored splits in a FIFO, and checkpoints both active and queued splits.
The configured job-wide capacity is source parallelism multiplied by this value; it is not presented as a Spanner quota.

Each partition query uses `executeQueryAsync` on a strong single-use read-only transaction.
The query applies the configured RPC priority and does not enable Data Boost.
Its callback hands over one record, completion, or failure and returns `PAUSE`; the mailbox calls `resume()` only after consuming that result.
This one-slot handover bounds memory and keeps all Flink output and coordinator events on the task thread.

The GoogleSQL and PostgreSQL decoders map their physical results into the same internal record variants.
Data records go through the user deserializer and carry the commit timestamp as their Flink event timestamp.
Heartbeat records advance a per-split watermark.
Child-partitions records become coordinator events, and successful query completion becomes a separate partition-finished event.
A query error fails the task without reporting completion or dropping the split.

The reader checkpoints the greatest consumed record timestamp and watermark for each active split.
Restore uses that timestamp as the next query's inclusive start.
This can repeat the boundary record and gives the source at-least-once delivery; advancing past it could skip another record with the same commit timestamp.

## Evidence

Decoder fixtures cover every data field, recursive type descriptors, `TOKENLIST`, an unknown future code, absent versus explicit JSON `null`, heartbeats, and child partitions for both dialect shapes.
Reader tests drive the concurrency bound, excess restored splits, the one-slot pause and resume, watermarks, child-before-finish ordering, nullable deserialization, query failure, and bounded completion.
Emulator MiniCluster tests run the production source for both dialects across a schema and value-capture change.
A separate failover test restarts each dialect and requires every record plus a repeated inclusive boundary.

## Alternatives declined

- **Run one blocking query per subtask.**
  Source parallelism would also become the query cap, and a quiet partition would prevent the subtask from serving another assigned partition.
- **Let callbacks enqueue freely.**
  The service could outrun the mailbox and make memory proportional to unread change records rather than configured query capacity.
- **Flatten the type descriptor to the client library enum.**
  That would reject or erase `TOKENLIST`, nested descriptor fields, annotations, and service types added before the client library learns them.
- **Resume after the checkpoint timestamp.**
  Commit timestamps are not unique per record, so an exclusive boundary can skip records from the same transaction or partition.

## Consequences

- Increasing the concurrency option spends one callback thread, one transaction, and one active query per additional slot in each subtask.
- A restart can repeat records at the last checkpointed timestamp, so downstream consumers that need uniqueness must deduplicate.
- The emulator proves connector wiring and both dialect adapters, not real-service split, merge, retention, or fallback behavior.
  That acceptance remains in #535, while operational query-capacity measurements and metrics remain in #551.
