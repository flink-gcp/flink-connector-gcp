---
title: BigQuery
type: docs
weight: 10
---

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

# BigQuery Connector

BigQuery sink for Apache Flink with a unified, `BigQueryIO`-style write API, provided by the
`flink-connector-gcp-bigquery` module.

One builder dispatches to a write-method implementation at job-graph construction time:

| Write method | Semantics |
|---|---|
| `STORAGE_API_AT_LEAST_ONCE` | Storage Write API default stream; dynamic per-record table destinations; connection multiplexing delegated to the client's connection pool |
| `STORAGE_API_EXACTLY_ONCE` | Storage Write API buffered streams + two-phase commit on checkpoints; single fixed destination |
| `FILE_LOADS` | GCS-staged Avro files + BigQuery load jobs; batch and streaming (checkpoint-triggered), exactly-once |

Per-feature implementation status is tracked in the
[module README]({{< param BookRepo >}}/blob/main/flink-connector-gcp-bigquery/README.md).

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
time-based flush option for checkpoint-less jobs is tracked in [#54]({{< param BookRepo >}}/issues/54)). Batch execution is covered
by the end-of-input flush. End-to-end loss behavior additionally depends on the source's own
state handling.

**Discarded operator state.** The two Storage Write API methods differ in *when* rows become
durable relative to when the source advances its position, and that difference decides what a
state-less restart costs:

| | Rows become visible in BigQuery | Source commits offsets / acks |
|---|---|---|
| `STORAGE_API_AT_LEAST_ONCE` | Before the checkpoint barrier (in `flush()`) | After the checkpoint completes |
| `STORAGE_API_EXACTLY_ONCE` | After the checkpoint completes (`FlushRows` in the committer) | After the checkpoint completes |

At-least-once keeps the sink strictly ahead of the source: whatever the source has acked is
already visible in BigQuery, so discarding state can duplicate rows but cannot lose them.
Exactly-once puts both side effects in the same phase with no atomicity between them, so
discarding state opens a loss window of at most one checkpoint — rows appended but not yet
flushed, and committables checkpointed but not yet committed, stay invisible forever while the
source may already have acked past them. This is inherent to two-phase commit (a Kafka
exactly-once producer behaves the same way), not specific to this connector.

**The sink cannot detect this situation** — a writer restored with no state is indistinguishable
from a brand-new job — so the guard belongs in deployment tooling. Redeploy through savepoints
(`stop-with-savepoint`, then `flink run -s`); with the Flink Kubernetes Operator use
`upgradeMode: savepoint` (or `last-state`) and never `stateless`. When state has to be dropped,
rewind the source behind the last completed checkpoint so a potential loss becomes a duplicate,
and make duplicates harmless downstream (an idempotent key plus `MERGE` or
`QUALIFY ROW_NUMBER()`).

Neither method is uniformly safer — their loss paths are disjoint:

| Loss path | `STORAGE_API_AT_LEAST_ONCE` | `STORAGE_API_EXACTLY_ONCE` |
|---|---|---|
| Discarded operator state | none (duplicates only) | up to one checkpoint |
| Checkpointing disabled | buffered rows lost ([#54]({{< param BookRepo >}}/issues/54)) | impossible — rejected at graph construction |
| Committable outliving its write stream | none (holds no committer state) | possible — see [Exactly-once](#exactly-once-buffered-streams) |
| `FailedRowHandler` drop policies | by configuration | by configuration |

## Exactly-once (buffered streams)

`WriteMethod.STORAGE_API_EXACTLY_ONCE` writes through application-created Storage Write API
**BUFFERED** streams committed with a two-phase commit protocol on Flink checkpoints: writers
append rows at explicit offsets (invisible while buffered), and when a checkpoint completes the
committer makes exactly that checkpoint's rows visible with `FlushRows`.

```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                .destination(TableDestination.of("my-project", "my_dataset", "events"))
                .serializer(new MyEventProtoSerializer())
                .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                .build();

env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
env.enableCheckpointing(60_000); // EXACTLY_ONCE mode (the default)
```

Method-specific settings live in `BufferedStreamOptions` (required for this write method,
rejected for the others; all knobs are defaulted): `maxAppendRequestBytes` (512 KiB default) and
the connector-driven retry schedule (`retryInitialBackoff` 500 ms, `retryMaxBackoff` 10 s,
`retryMaxAttempts` 10) governing stream creation, transient re-appends and the restore probe.

**Stream lifecycle.** Each writer subtask owns **one buffered stream, created lazily on its first
append and reused across checkpoints** — per GCP guidance, frequent `CreateWriteStream` churn
(e.g. a new stream per checkpoint × parallelism) is not intended usage of the API; a clean run
creates exactly one stream per subtask for its whole lifetime. The stream name and next append
offset are Flink writer state. The SDK connection pool is default-stream-only, so each stream
gets a dedicated `StreamWriter` connection; backpressure comes from the SDK's bounded in-flight
window. `prepareCommit()` emits one committable per subtask naming the offset the completed
checkpoint may flush up to; `FlushRows` is naturally idempotent (re-flushing an already-flushed
offset answers `ALREADY_EXISTS` = success), so re-commits after restarts need no deterministic-id
machinery, no checkpoint stamping, and no global committer routing — the committer runs at the
sink's parallelism.

**Restore.** A restored writer probes its stream with the first replayed batch at the restored
offset, synchronously. Success reuses the stream; `OFFSET_ALREADY_EXISTS` (the pre-crash attempt
appended past the restored offset), `OFFSET_OUT_OF_RANGE`, a finalized/unknown stream, or a
failure to reopen it abandon the stream and a fresh one starts at offset zero. This cannot lose
or duplicate data: rows appended past the restored offset were never named by any committable,
so nothing ever flushes them. Abandoned streams (and streams of closing writers) are
deliberately **never finalized** — BigQuery rejects `FlushRows` on a finalized stream (verified
against the real service, and the reason batch commits happen after writer close), so finalizing
could permanently break a restored-but-uncommitted committable; an open stream's unflushed tail
stays invisible either way. Commit failures follow the FILE_LOADS model: the committer throws,
the job restarts, and the framework re-commits the restored committables idempotently.

**Stream lifetime.** BigQuery gives a buffered write stream a default TTL of
[seven days with no traffic on the stream](https://docs.cloud.google.com/bigquery/docs/write-api-streaming),
and streams cannot be deleted explicitly — they age out on that TTL, so the streams this write
method abandons need no cleanup. A running writer's own appends keep its stream alive, so the
TTL matters across downtime: a job stopped for longer than the TTL and then restored with
committables still pending references a stream that may no longer exist, and those flushes may
fail permanently. The only escape from a permanently failing commit is to start without state,
which drops those rows — the same class of hazard as an expired FILE_LOADS staging object. A
subtask that received rows and then went idle past the TTL while the job is still running hits
the same expiry; a missing stream is terminal mid-run, so the job restarts and the restore probe
starts a fresh stream. **What exactly happens at expiry — whether the flush fails, with which
error, whether the seven days is configurable, and whether unflushed buffered rows are billed as
storage — is not stated in the documentation and has not been verified here.**

**Execution modes.** The mode must be explicit (`AUTOMATIC` is rejected at graph construction —
were it to resolve to streaming without checkpointing, buffered rows would never become visible).
Streaming requires checkpointing with `CheckpointingMode.EXACTLY_ONCE` and
checkpoints-after-tasks-finish enabled (the final batch of a bounded job rides the post-finish
checkpoint); a slow flush delays the next checkpoint — that is the backpressure, and `commit()`
returning means the rows are visible. `BATCH` execution is supported: the single end-of-input
committable is committed when the job completes. There is no checkpoint-cadence quota guard:
`FlushRows` once per subtask per checkpoint is far below its quota (unlike FILE_LOADS' per-table
daily load-job limit).

**Scope (v1).** One fixed `destination(...)` per sink — the builder rejects
`destinationResolver(...)` for this write method (dynamic destinations are a planned follow-up).
The table schema is pinned when the stream is created: **mid-stream schema evolution is not
supported** — no fingerprint refresh and no connector-driven schema updates, so the builder
rejects an enabled `schemaUpdateOptions(...)` rather than accepting a setting this write method
would silently ignore, and a schema mismatch fails the job with a hint (update the table out of
band and restart). Table auto-creation under `CREATE_IF_NEEDED` *is* supported: it runs at
stream-creation time — schema from the serializer, partitioning and clustering from
`tableCreateOptions(...)` — with retries while table metadata propagates, and `CREATE_NEVER`
fails immediately.

**Error handling.** Serialization failures and oversized rows go to the `FailedRowHandler` before
any stream exists, as in the at-least-once method. Server-side **row-level rejections are also
routed to the handler** — with more machinery than the at-least-once path needs: an append
request is rejected atomically (the offset never advances), so the writer routes the failing rows
to the handler and replays the surviving rows plus every batch appended behind the rejected one
at recomputed offsets. Transient failures are re-appended at their original offset
(`OFFSET_ALREADY_EXISTS` then means the original landed). Stream-state errors mid-run
(`STREAM_FINALIZED`, `STREAM_NOT_FOUND`, `INVALID_STREAM_STATE`) are terminal — the restart +
restore protocol is the repair. Consistency guards (an acknowledged append behind a rejected one,
an offset-echo mismatch, `OFFSET_ALREADY_EXISTS` during an offset-shifting replay) fail the job
rather than risk silent divergence.

## File loads

`WriteMethod.FILE_LOADS` writes each destination table's rows to Avro files on Cloud Storage and
loads them with BigQuery load jobs — free of streaming-insert cost, always exactly-once. Batch
execution loads everything at end of input; streaming execution loads each checkpoint's files
(the checkpoint is the trigger, like Beam's streaming FILE_LOADS `triggeringFrequency` model):

```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .writeMethod(WriteMethod.FILE_LOADS)
                .destinationResolver(
                        (e, ctx) -> TableDestination.of("my-project", "my_dataset", e.tableName()))
                .serializer(new MyEventProtoSerializer())
                .fileLoadsOptions(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://my-staging-bucket/flink-loads")
                                .build())
                .build();

env.setRuntimeMode(RuntimeExecutionMode.BATCH);
// or: env.setRuntimeMode(RuntimeExecutionMode.STREAMING) with checkpointing enabled.
```

FILE_LOADS-only settings live in `FileLoadsOptions` (required for this write method, rejected for
the others): `stagingPath` (required), `writeDisposition` (`WRITE_APPEND` default,
`WRITE_TRUNCATE` for atomic batch reloads, `WRITE_EMPTY`), `tempDataset`, and the streaming guard
`minCheckpointInterval` (all described below).

**Topology.** Parallel writers encode records (serializer proto bytes → Avro `GenericRecord`) and
stream them straight to per-destination GCS objects — rows never accumulate on the heap, so memory
use is ~5 MiB per open destination regardless of data volume; in streaming the inter-checkpoint
buffer *is* GCS. Files roll at 1.5 GiB. The pre-commit topology routes every subtask's
committables to a single committer subtask (in streaming through a stage that stamps each
committable with its checkpoint id), and that committer — the actual commit — groups the staged
files by destination table and runs **one load job per table** (all jobs submitted first, then
awaited — BigQuery runs them concurrently server-side): once at end of input in batch, once per
completed checkpoint in streaming. Load jobs carry the serializer's schema explicitly
(`useAvroLogicalTypes`), plus the partitioning/clustering from `tableCreateOptions(...)` for
tables created under `CREATE_IF_NEEDED`. Loading in the committer (rather than a post-commit
topology, where the [#14]({{< param BookRepo >}}/issues/14) batch implementation originally ran it) is deliberate: committables ride
in Flink's committer state until their loads succeed, and the final batch of a streaming job is
committed during task shutdown's final-checkpoint wait — records emitted to a post-commit
topology at that point are not guaranteed to be processed before the job terminates.

**Execution modes.** The mode must be explicit: `AUTOMATIC` is rejected when the job graph is
built, because were it to resolve to streaming with checkpointing disabled, no trigger would ever
come and files would stage forever. Streaming additionally requires, also checked at graph
construction: checkpointing enabled (the checkpoint is the load trigger),
`WriteDisposition.WRITE_APPEND` (truncating/rejecting per checkpoint is meaningless), and a
checkpoint interval compatible with BigQuery's **1,500 load jobs per table per day** quota — each
checkpoint issues at least one load job per destination table:

| Checkpoint interval | Load jobs per table per day |
|---|---|
| 1 min | 1,440 — too close to the ceiling, not viable |
| 2 min | 720 |
| 5 min | 288 |

Intervals below `minCheckpointInterval` (default 2 minutes) are rejected; intervals below 5
minutes log a warning. Lowering `minCheckpointInterval(...)` is the explicit opt-in for
short-lived jobs whose daily load count stays safe (the integration tests do this). A runtime
warning also fires when observed checkpoint cadence stays under 2 minutes, catching interval
configuration the client-side guard cannot see. Streaming pipelines that need second-level
latency belong on the Storage Write API methods; checkpoint-triggered file loads trade minutes of
latency for free ingestion.

**Streaming operation.** Each completed checkpoint's committables are committed — loaded — by the
framework at that checkpoint's completion, in checkpoint order. Loads are synchronous: a slow
load delays the next checkpoint's completion, which is the backpressure mechanism (loads of a few
minutes of data typically finish in seconds to tens of seconds, well within the quota-mandated
2-5 minute intervals). Everything not yet loaded rides in Flink's committer state: on recovery
the committables are re-committed and the deterministic job ids re-attach to jobs a previous
attempt already created. A load-job failure fails the ongoing checkpoint (and the job), which
restarts from the last checkpoint with the staged files still in place. On stop-with-savepoint
without `--drain`, the final checkpoint's rows land when the savepoint is resumed.

**Exactly-once.** Load jobs reference exactly the file URIs emitted by the writers — never a
bucket prefix — so files from failed/restarted attempts (which use unique names: Flink job id,
subtask, attempt, random component) can never leak into a load. Job ids are deterministic hashes
of the destination and its sorted file list (streaming ids additionally carry a visible
`-c<checkpointId>` segment for attribution): a retry after a failure re-attaches to the
already-running/completed BigQuery job instead of loading twice. Committables carry the Flink job
id of the run that staged them, so even a restore under a *new* Flink job id (`flink run -s` on a
savepoint or retained checkpoint) reproduces the original job ids and re-attaches. Known residual
risk (shared with the Beam and Dataproc designs): if a failure destroys the persisted
committables *and* re-runs the writer stage after load jobs were already submitted, the retried
run produces new file names — and thus new job ids — while the first run's jobs keep running
server-side, which can duplicate rows under `WRITE_APPEND`.

**Per-load-job limits.** In batch, a table whose staged files exceed one load job's limits
(10,000 source URIs / 11 TiB) is loaded partition-wise into temporary tables (`WRITE_TRUNCATE`,
so retries are idempotent) and appended to the final table with one atomic copy job. Temporary
tables go to the destination's dataset by default, or to `tempDataset(...)` — a dedicated dataset
with a default table expiration is recommended so tables orphaned by hard failures are
garbage-collected. Copy jobs support no schema update options, so on this path the final table is
created/schema-unioned via the REST API before the copy (same union rules as
[Schema evolution](#schema-evolution)). In streaming there is no temporary-table path: an
oversized checkpoint × table submits multiple direct append jobs (deterministic per-partition
ids keep exactly-once; only that checkpoint's atomic visibility is lost).

**Schema evolution.** The same `schemaUpdateOptions(...)` flags map to the load jobs' native
`ALLOW_FIELD_ADDITION`/`ALLOW_FIELD_RELAXATION` options. BigQuery honors them only for
`WRITE_APPEND` loads; with `WRITE_TRUNCATE` the loaded schema replaces the table schema anyway.

**Staging cleanup.** Staged files are deleted after a successful load — best-effort; on failure
they are deliberately kept so a Flink restart retries deterministically. Point `stagingPath` at a
**dedicated bucket (separate from checkpoint/savepoint storage) with a lifecycle rule** (for
example: delete objects after 1–7 days) so orphans from hard failures expire on their own. Size
the rule's age above the longest outage you intend to recover from: staged files referenced by a
checkpoint *are* the data, and restoring a streaming job after the rule already expired them
leaves the pending loads permanently failing (the poisoned committables can then only be dropped
by starting without state).

**Errors.** `FailedRowHandler` covers serialization/Avro-conversion failures (row-level, before
staging). A load job itself is all-or-nothing: there is no per-row policy at load time, and a
failed load fails the Flink job.

**Type mapping.** `TIMESTAMP`/`DATE`/`TIME` use Avro logical types, `DATETIME` travels as a
canonical civil-time string, `NUMERIC`/`BIGNUMERIC` as Avro decimals (parameterized
precision/scale respected), `JSON`/`GEOGRAPHY` as strings, `STRUCT`/`REPEATED` nest naturally.
`INTERVAL` and `RANGE` columns are not supported by this write method.

The integration tests (`BigQueryFileLoadsITCase` for batch, `BigQueryFileLoadsStreamingITCase`
for checkpoint-triggered streaming loads) run real jobs against BigQuery and GCS and are gated on
`BQ_IT_PROJECT`, `BQ_IT_DATASET` and `BQ_IT_GCS_BUCKET` (application-default credentials); they
are skipped when the variables are unset, keeping `./mvnw verify` credential-free. For local
runs, put the variables (plus `GOOGLE_APPLICATION_CREDENTIALS` if not using the default ADC
location) in an uncommitted `.env` at the repository root — mise loads it automatically.

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
  standardization ([#37]({{< param BookRepo >}}/issues/37)). The stub has no flush/checkpoint lifecycle yet: implementations
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

## Testing

The module is tested at three levels; `./mvnw verify` runs the first two and needs no GCP
credentials.

**Unit tests** cover the builder/facade dispatch, serializers, schema converters, error
classification and the writer/committer state machines against in-memory fakes.

**Emulator integration tests** run [goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator)
in a testcontainer and exercise the Storage Write API gRPC endpoint plus the REST
table-metadata path end to end: plain at-least-once appends across checkpoint-style flushes
through the `BigQuerySink` facade (`BigQueryDefaultStreamWriterITCase`), dynamic multi-table
destinations (`BigQueryDynamicDestinationsITCase`), table auto-creation with create dispositions
(`BigQueryTableAutoCreationITCase`), schema evolution
(`BigQuerySchemaEvolutionITCase`), and a buffered-stream smoke test of the production
exactly-once client wiring (`BigQueryBufferedStreamSmokeITCase` — single flush only: the
emulator keeps no flush cursor, every `FlushRows` re-inserts all rows up to the offset, and
buffered appends neither honor the request offset nor raise `OFFSET_ALREADY_EXISTS`, so the
exactly-once semantics are verified against real BigQuery instead). The at-least-once tests
connect through a test-only plaintext appender
factory (`EmulatorAppenderFactory`) that also papers over two emulator deviations tracked by
[goccy/bigquery-emulator#342](https://github.com/goccy/bigquery-emulator/issues/342) (default-stream naming, `UNKNOWN` instead of `NOT_FOUND` for missing
tables); routing the *production* factory at the emulator via an injection seam is tracked in
[#54]({{< param BookRepo >}}/issues/54). One further deviation (same family): on a connection opened after an earlier connection to
the emulator has closed, only the first `AppendRows` request is durably applied — follow-ups are
acknowledged but never become queryable. The multi-flush scenario therefore runs in its own test
class, whose connection is guaranteed to be its container's first (one forked JVM and fresh
container per `*ITCase` class). Real BigQuery applies every acknowledged default-stream append.

**Real-GCP tests** cover what the emulator cannot faithfully reproduce, and stay out of
credential-less CI:

- quotas and rate-limit behavior (per-table metadata-update quota, `RESOURCE_EXHAUSTED`
  handling under real enforcement)
- the SDK connection pool: multiplexing, scale-up and in-stream retry behavior of the
  production `StreamWriterRowAppenderFactory`
- load jobs: goccy/bigquery-emulator supports neither `gs://` load jobs nor a Cloud Storage
  endpoint, so the whole `FILE_LOADS` path runs against real services
  (`BigQueryFileLoadsITCase` and `BigQueryFileLoadsStreamingITCase`, env-gated as described
  [above](#file-loads))
- buffered-stream exactly-once semantics: idempotent re-flush, the restore probe, and the
  [issue #30]({{< param BookRepo >}}/issues/30) acceptance criterion — a MiniCluster streaming job
  with an induced mid-run restart showing no duplicates and no gaps — plus a clean streaming run
  and batch execution
  (`BigQueryBufferedStreamExactlyOnceITCase`, gated on `BQ_IT_PROJECT`/`BQ_IT_DATASET` only;
  no bucket needed)

The remaining real-GCP coverage (MiniCluster E2E on GitHub Actions via WIF) is tracked in [#16]({{< param BookRepo >}}/issues/16)
and [#28]({{< param BookRepo >}}/issues/28).
