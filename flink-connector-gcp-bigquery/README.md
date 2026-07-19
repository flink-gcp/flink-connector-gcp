# flink-connector-gcp-bigquery

BigQuery sink for Apache Flink with a unified, `BigQueryIO`-style write API.

One builder dispatches to a write-method implementation at job-graph construction time:

| Write method | Semantics | Status |
|---|---|---|
| `STORAGE_API_AT_LEAST_ONCE` | Storage Write API default stream; dynamic per-record table destinations; connection multiplexing delegated to the client's connection pool | Writer implemented, incl. table auto-creation with create dispositions and error classification/routing (full emulator IT suite: #15) |
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
  reference for proto/schema conversion (`StorageSchemaConverter` is an independent
  implementation of the reverse direction of the last)

No source code has been copied into this module so far.
