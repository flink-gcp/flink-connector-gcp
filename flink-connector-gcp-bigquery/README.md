# flink-connector-gcp-bigquery

BigQuery sink for Apache Flink with a unified, `BigQueryIO`-style write API.

One builder dispatches to a write-method implementation at job-graph construction time:

| Write method | Semantics | Status |
|---|---|---|
| `STORAGE_API_AT_LEAST_ONCE` | Storage Write API default stream; dynamic per-record table destinations; connection multiplexing delegated to the client's connection pool | Writer implemented, incl. table auto-creation with create dispositions, error classification/routing and schema evolution (full emulator IT suite: #15) |
| `STORAGE_API_EXACTLY_ONCE` | Storage Write API buffered streams + two-phase commit | Not buildable yet — `build()` rejects (#30) |
| `FILE_LOADS` | GCS-staged files + BigQuery load jobs; batch only | Not buildable yet — `build()` rejects (#14) |

```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                .destinationResolver(
                        (e, ctx) -> TableDestination.of("my-project", "my_dataset", e.tableName()))
                .serializer(new MyEventProtoSerializer())
                .build();
```

API notes:

- `BigQueryProtoSerializer` is an abstract class exposing `getDescriptor(TableDestination)` in
  addition to `serialize`, so the sink can derive table/stream schemas *before* the first record
  of a destination (table auto-creation, write-stream and load-job schemas). Protobuf
  `Descriptor`s are not Java-serializable — obtain them statically or lazily, don't store them in
  instance fields.
- `DestinationResolver.resolve(element, context)` receives the writer context (event timestamp,
  watermark) so time-based routing such as daily tables is expressible. Resolvers run per record:
  cache and reuse `TableDestination` instances.
- `ProtoMessageSerializer.of(MyMessage.class)` is the built-in serializer for records that
  already are protobuf messages: the BigQuery schema is derived from the message descriptor
  (integers → INT64, float/double → DOUBLE, enum → STRING, `google.protobuf.Timestamp` →
  TIMESTAMP in microseconds, nested messages → STRUCT, maps → REPEATED STRUCT<key, value>), and
  `ProtoSchemaOptions` can map selected message fields to JSON columns.
- `TableDestination` is pure table identity (`equals`/`hashCode` over project/dataset/table);
  per-destination creation metadata (partitioning, clustering) is supplied through
  `TableCreateOptionsProvider` so destination identity stays stable as a cache/connection key.

## Table auto-creation

Under the default `CreateDisposition.CREATE_IF_NEEDED`, an append failing with `NOT_FOUND` is
recovered on the task thread: the destination table is created through the BigQuery REST API
(schema from the serializer's `getTableSchema`; partitioning/clustering from
`tableCreateOptions(...)` or a per-destination `tableCreateOptionsProvider(...)`), the
destination's stream writer is rebuilt, and the failed batch is re-appended with backoff while
table metadata propagates to the Storage Write API backend. Creation is idempotent across
parallel subtasks (HTTP 409 is treated as success); the credentials need
`bigquery.tables.create` on the destination dataset. Options apply only at creation time —
existing tables are never modified.

With `CreateDisposition.CREATE_NEVER`, writing to a missing table fails the job immediately.

## Schema evolution

Schema changes are handled without a job restart. Reactive handling is always on:

- **Server-pushed schema updates** — when an append response reports `updated_schema` (the
  table's schema changed, e.g. through DDL), the destination's stream writer is rebuilt with a
  fresh serializer descriptor. A raw Storage Write API `StreamWriter` never refreshes its schema
  by itself, also not under connection-pool multiplexing.
- **Serializer schema changes** — a serializer with an evolving schema overrides
  `getSchemaFingerprint(destination)` to return a cheap token that changes with its schema. The
  writer compares it per record and refreshes the destination's stream *before* appending rows
  serialized under the changed schema, so the first append after an evolution does not have to
  fail.
- **Stale-stream-writer failures** (`STREAM_FINALIZED`, `STREAM_NOT_FOUND`,
  `INVALID_STREAM_STATE`, writer-closed) are repaired by rebuilding the writer and re-appending
  within the transient retry budget instead of failing the job.

**Connector-driven table schema updates** are opt-in via `schemaUpdateOptions(...)`:

```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .destination(TableDestination.of("my-project", "my_dataset", "events"))
                .serializer(new MyEventProtoSerializer())
                .schemaUpdateOptions(
                        SchemaUpdateOptions.builder()
                                .allowNewFields()
                                .allowFieldRelaxation()
                                .build())
                .build();
```

When enabled and the serializer's schema evolves past the destination table's (detected through
the fingerprint pre-check or a `SCHEMA_MISMATCH_EXTRA_FIELDS` append failure), the sink updates
the table itself: fresh read of the live schema, union with the serializer schema, and an
etag-conditioned `tables.update`. The union is strictly widening — existing fields are never
dropped, reordered or re-typed (a type change fails the job); new fields are appended at the end
— including inside `STRUCT` columns: updates go through the REST API, which unlike SQL
`ALTER TABLE` supports adding nested fields — and forced `NULLABLE` (BigQuery cannot add
`REQUIRED` columns); `REQUIRED`→`NULLABLE` relaxation happens only under `allowFieldRelaxation`
(any mode not explicitly `REQUIRED` counts as nullable); `REPEATED` is never changed. Concurrent updates from
parallel subtasks need no coordination: updates are additive and idempotent, lost races (etag
mismatch, HTTP 409/412, `rateLimitExceeded` — the per-table quota is about five metadata updates
per ten seconds) re-read and re-union with jitter, and unions of concurrent unions converge.
The credentials need `bigquery.tables.get` and `bigquery.tables.update`.

Caveats:

- Rows already handed to the sink are retained as serialized bytes and are never re-encoded, so
  serializer schema evolution must be wire-compatible: append new fields at the end (including
  inside nested types) and relax `REQUIRED`→`NULLABLE`; never remove, reorder or re-type fields.
- A schema update propagates to the Storage Write API backend within minutes. The writer keeps
  re-appending affected batches for up to ~15 minutes (flat 30 s waits, ±25 % jitter, 30
  attempts) — a schema repair can therefore block a checkpoint longer than Flink's default
  checkpoint timeout of 10 minutes, which may need raising on jobs that enable schema updates.
- Schema unionization stays opt-in because BigQuery columns can never be dropped again: one
  malformed record shipping an unexpected field could otherwise poison a table permanently. With
  updates disabled, schema-mismatch appends fail the job (with a hint), and externally driven
  schema changes are still picked up reactively.

## Delivery guarantees and state

The `STORAGE_API_AT_LEAST_ONCE` writer is **stateless by design**: rows are appended
asynchronously as batches fill, and on **every checkpoint** Flink invokes the writer's `flush()`
(before the barrier is emitted), which appends all pending batches and awaits every in-flight
append with direct response inspection. A successful checkpoint therefore means *all* records up
to the barrier are acknowledged by BigQuery, and the writer stores nothing in Flink state —
**discarding operator state (savepoint-less redeploys, state resets) can never lose
sink-buffered data**. This is a deliberate decision: the alternative `AsyncSinkWriter`-style
model persists unflushed buffers into writer state instead of flushing at the barrier, which
silently loses those buffers whenever state is dropped.

Checkpointing must be enabled for the at-least-once guarantee in streaming jobs: without it,
Flink never calls `flush()` mid-stream, so sub-threshold buffers are lost on failure (a
time-based flush option for checkpoint-less jobs is tracked in #54). Batch execution is covered
by the end-of-input flush. End-to-end loss behavior additionally depends on the source's own
state handling.

## Error handling

Append failures are classified on the task thread and routed by class:

| Class | Examples | Behavior |
|---|---|---|
| Transient | `UNAVAILABLE`, `ABORTED`, `INTERNAL`, `CANCELLED`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `UNKNOWN` | Retried by the SDK's in-stream retries first (500 ms initial delay, ×2 up to 30 s, 5 attempts); failures that still surface are re-appended by the writer on a rebuilt stream writer with backoff (500 ms initial, doubled up to 10 s, 10 attempts). They do not fail the job unless the retry budget is exhausted |
| Stale stream writer | `STREAM_FINALIZED`, `STREAM_NOT_FOUND`, `INVALID_STREAM_STATE`, writer closed | Repaired like transient failures: the destination's stream writer is rebuilt and the batch re-appended within the retry budget |
| Schema mismatch | `SCHEMA_MISMATCH_EXTRA_FIELDS` (rows carry fields the table does not have) | With `schemaUpdateOptions(...)` enabled: the table schema is reconciled and the batch re-appended while the update propagates (see [Schema evolution](#schema-evolution)). Otherwise terminal |
| Terminal | `INVALID_ARGUMENT`, `PERMISSION_DENIED`, `NOT_FOUND` under `CREATE_NEVER`, retry-budget exhaustion, failures without a status code | Fail the ongoing write or checkpoint immediately |
| Row-level | Rows rejected with per-row error details (`AppendSerializationError`, response row errors), serialization failures, rows over the per-row size limit | Routed row by row to the configured `FailedRowHandler`; surviving rows of the batch are re-appended |

The failed-row policy is pluggable via `failedRowHandler(...)`:

```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .destination(TableDestination.of("my-project", "my_dataset", "events"))
                .serializer(new MyEventProtoSerializer())
                .failedRowHandler(FailedRowHandler.logAndDrop())
                .build();
```

- `FailedRowHandler.failJob()` (default) — every row-level failure fails the checkpoint
- `FailedRowHandler.logAndDrop()` — logs each failed row at WARN and drops it
- `FailedRowHandler.sendToDeadLetterQueue(...)` — forwards each failed row to a
  `DeadLetterQueue`, an experimental stub interface for the cross-connector DLQ
  standardization (#37). The stub has no flush/checkpoint lifecycle yet: implementations
  should write through synchronously (throwing on failure), and restarts can produce
  duplicate dead-letter entries
- Custom handlers implement `FailedRowHandler`; throwing from `handle` fails the checkpoint,
  returning drops the row. `FailedRow` carries the serialized protobuf bytes (the writer is
  stateless, so the original record object is gone by the time server-side row errors arrive),
  or `null` bytes when serialization itself failed

Retries preserve the at-least-once contract: a batch whose append outcome was lost may be
re-appended in full, so duplicates are possible (as with any retry in this write method). Worst
case, a single repair can take about a minute of SDK retries plus a minute of writer re-appends
before surfacing as terminal. The SDK retry schedule is not configurable yet — deliberately
deferred until a real-world need shows which knobs matter.

## Provenance and attribution

This module is an original implementation. Its design references the following Apache-2.0
projects; when code is adapted from them, the fact is recorded here and in the repository-level
`NOTICE` file, keeping the original license headers where applicable:

- [Apache Beam](https://github.com/apache/beam) `BigQueryIO` — the unified write-method API shape
  and the dynamic destinations concept
- [GoogleCloudDataproc/flink-bigquery-connector](https://github.com/GoogleCloudDataproc/flink-bigquery-connector)
  — reference for Storage Write API sink internals and the serializer contract
  (descriptor accessor + `ByteString` rows)
- [googleapis/java-bigquerystorage](https://github.com/googleapis/java-bigquerystorage)
  (`JsonToProtoMessage`, `BQTableSchemaToProtoDescriptor`, `BqToBqStorageSchemaConverter`) —
  reference for proto/schema conversion (`StorageSchemaConverter` and
  `BigQuerySchemaConverter` are independent implementations of the two directions)
- [Aiven-Open/bigquery-connector-for-apache-kafka](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka)
  (the maintained continuation of
  [wepay/kafka-connect-bigquery](https://github.com/wepay/kafka-connect-bigquery)) — design
  reference for the schema-evolution mechanics: the schema union rules and their gating flags,
  the update-on-error flow with a bounded jittered wait for schema propagation, and the
  coordinator-free concurrent-update retry pattern (`SchemaUnifier`,
  `BigQueryDefaultStreamWriter`'s reconciliation and `BigQueryTableAdmin`'s lost-race handling
  are independent reimplementations; the full design research is recorded on issue #12)

No source code has been copied into this module so far.
