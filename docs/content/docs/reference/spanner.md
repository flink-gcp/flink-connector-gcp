---
title: Spanner
type: docs
weight: 50
---

<!--
Copyright 2026 The flink-gcp authors

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

# Spanner options

Every option the Spanner sink and source take. What each one is *for* is on the
[Spanner connector]({{< relref "docs/connectors/datastream/spanner" >}}) page; the three forms of
the Default column are explained [here]({{< relref "docs/reference" >}}#what-a-default-means).

Unlike the [Bigtable]({{< relref "docs/reference/bigtable" >}}) sink, this one **does** take retry
knobs, and they are not decoration: the Spanner client library does not retry the batch write RPC
at all, so the sink owns the whole retry loop. See
[Retries]({{< relref "docs/connectors/datastream/spanner" >}}#retries-belong-to-the-sink).

## Spanner Table API / SQL

The `spanner` factory requires `project`, `instance`, `database`, and `table`.
Unset sink options inherit the corresponding `SpannerWriterOptions` default documented below.
The complete DDL option table, native UUID and other schema mappings, and primary-key behavior are on the [Spanner SQL page]({{< relref "docs/connectors/table/spanner" >}}).

### Table Change Stream readable metadata

These keys are available only when `scan.mode = 'change-stream'`.
Every native metadata type is non-null because every emitted `DataChangeRecord` supplies the field.

| Metadata key | Native type | What it contains |
|---|---|---|
| `commit-timestamp` | `TIMESTAMP_LTZ(9) NOT NULL` | Spanner commit timestamp, preserving nanosecond precision |
| `sequence` | `STRING NOT NULL` | Record sequence within its partition, commit timestamp, and transaction |
| `server-transaction-id` | `STRING NOT NULL` | Spanner server transaction identifier |
| `is-last-record-in-transaction-in-partition` | `BOOLEAN NOT NULL` | Whether this is the transaction's final record in the originating partition |
| `table` | `STRING NOT NULL` | Dialect-aware native table name reported by Spanner |
| `mod-type` | `STRING NOT NULL` | `INSERT`, `UPDATE`, or `DELETE` from the original data-change record |
| `value-capture-type` | `STRING NOT NULL` | Value-capture type carried by the original data-change record |
| `number-of-records-in-transaction` | `BIGINT NOT NULL` | Number of data-change records in the transaction |
| `number-of-partitions-in-transaction` | `BIGINT NOT NULL` | Number of Change Stream partitions containing the transaction |
| `transaction-tag` | `STRING NOT NULL` | Transaction tag, or an empty string when no tag was supplied |
| `system-transaction` | `BOOLEAN NOT NULL` | Whether Spanner identifies the transaction as a system transaction |
| `mod-number` | `INT NOT NULL` | Zero-based position of the mod in the original data-change record; a full-mode update's before and after rows share it |

Use `TIMESTAMP_LTZ(3) METADATA FROM 'commit-timestamp'` with `WATERMARK FOR ... AS SOURCE_WATERMARK()` because Flink rowtime attributes support precision 0 through 3.
Use the native `TIMESTAMP_LTZ(9)` declaration without a watermark when nanosecond precision is required.

## `SpannerSink.builder()`

| Option | Default | What it does |
|---|---|---|
| `database` | **required** | The database every mutation is written to. Which *table* is not configured here — the mutation the serializer returns names its own |
| `serializer` | **required** | Turns a record into a `Mutation`, or into `null` to skip it |
| `writerOptions` | [defaults](#spannerwriteroptions) | The batch limits, the request scheduling and the retry budget |
| `failedMutationHandler` | `FailureHandler.failJob()` | What happens to a mutation the service terminally refused. See [Error handling]({{< relref "docs/connectors/datastream/spanner" >}}#error-handling) for the two statuses that reach it |
| `constraintViolationPolicy` | `FAIL_JOB` | What happens to a mutation refused for violating a constraint. `ROUTE_TO_FAILURE_HANDLER` hands it to `failedMutationHandler` instead, so that handler then decides between failing, dropping and dead-lettering. See [Error handling]({{< relref "docs/connectors/datastream/spanner" >}}#error-handling) |
| `serviceAccountKeyFile` | *unset ⇒ ADC for the real service* | Service-account JSON key-file path read by each TaskManager writer at runtime. The job graph contains the path, not the credential contents. Mutually exclusive with `emulatorEndpoint`; see [Credentials]({{< relref "docs/connectors/datastream/spanner" >}}#credentials) |
| `emulatorEndpoint` | *unset ⇒ the real service* | `host:port` of a Spanner emulator. Setting it also stops the client looking for credentials |

## `SpannerWriterOptions`

Built with `SpannerWriterOptions.builder()`, passed to `writerOptions(...)`. Every knob is
defaulted, so `SpannerWriterOptions.defaults()` is the same as not setting options at all.

### Batch limits

A request Spanner refuses is refused as a whole, so these three bound the request the writer builds.
Each has a ceiling of its own, refused at submission — but only `maxBatchBytes`' ceiling marks a
request the service is documented to refuse; the other two are argued rather than measured, and the
connector page says how. The three are **ANDed** — a batch flushes on whichever binds first — so
raising one alone often changes nothing; **`maxBatchCells` and `maxBatchBytes` are the pair that
decides how large a request grows**. The reasoning is under
[Batching]({{< relref "docs/connectors/datastream/spanner" >}}#batching).

| Option | Default | What it does |
|---|---|---|
| `maxBatchCells` | `5000` | Caps the mutation *cells* in one request, **at most `80000`**. A written column costs one cell for the table plus one for every secondary index containing it, so this is **not** a column count — raising it toward the ceiling removes the headroom that keeps an unread schema safe |
| `maxBatchMutations` | `500` | Caps the mutations in one request, **at most `80000`** — a mutation costs at least one cell, so a batch never holds more mutations than cells, and the ceiling is `maxBatchCells`' ceiling for that reason. Set above the *configured* `maxBatchCells` it cannot take effect either, and building the options **logs a warning** to wherever the job's `main` runs. Whether a lower value binds depends on what each mutation costs in cells, not on the two knobs' order |
| `maxBatchBytes` | `1048576` (1 MiB) | Caps the *estimated* size of one request, **at most `104857600`** (100 MiB). Estimated, not measured: the client library exposes no way to size a `Mutation` as it goes on the wire, so framing is ignored and it reads low — the ceiling is a guard against a misconfiguration, not a value to set. A `BYTES` value counts as its base64 length, which is what the service receives. The ceiling is the service's own figure, measured rather than inferred ([#441]({{< param BookRepo >}}/issues/441)): a larger request is refused at exactly `104857600` bytes |

### Request scheduling

| Option | Default | What it does |
|---|---|---|
| `maxCommitDelay` | *unset ⇒ the service's own handling* | How long Spanner may delay a commit to group it with others, trading latency for throughput. Between zero and 500 ms, which is what the service accepts. Not rounded to milliseconds — the client forwards seconds and nanoseconds unchanged |
| `rpcPriority` | *unset ⇒ `HIGH`* | `LOW`, `MEDIUM` or `HIGH`. Spanner treats an unspecified priority as `HIGH`, so `MEDIUM` is a step down from the default rather than a restatement of it. `LOW` is what a backfill that must not disturb serving traffic wants |

### Retry budget

Spent on transient failures only, and on the mutations that are still undecided rather than on the
whole batch.

| Option | Default | What it does |
|---|---|---|
| `retryInitialBackoff` | `500ms` | The first backoff, at least 1 ms |
| `retryMaxBackoff` | `10s` | The backoff cap, at least `retryInitialBackoff` |
| `retryMaxAttempts` | `10` | Attempts before the job fails. Exhausting the budget fails the job — a sink cannot drop what the service never refused. Note the wall-clock worst case: the client library gives a batch write a one-hour total timeout and this sink sets no deadline of its own, so a wedged request blocks the task thread — and therefore checkpointing — for up to an hour per attempt |

## `SpannerSource.builder()`

The bounded batch source. What each option is *for*, and what Spanner decides rather than the job,
is under [Source]({{< relref "docs/connectors/datastream/spanner" >}}#source).

| Option | Default | What it does |
|---|---|---|
| `database` | **required** | The database to read |
| `readOperation` | **required** | What to read: `SpannerReadOperation.query(...)`, `.read(...)` or `.readUsingIndex(...)`. A query has to be root-partitionable |
| `deserializer` | **required** | Emits zero or more non-null output records from each `Struct` through a synchronous Flink `Collector`; do not retain the collector. Emitting nothing skips the row |
| `timestampBound` | `TimestampBound.strong()` | The snapshot to read at. Only `strong()`, `ofReadTimestamp` and `ofExactStaleness` are accepted; the other two modes are single-use-only and are rejected here |
| `maxPartitions` | *unset ⇒ the service decides* | How many partitions to ask for. A hint the service may ignore, and the emulator ignores outright |
| `partitionSizeBytes` | *unset ⇒ the service decides* | How much data one partition should cover. A hint, like the one above |
| `dataBoostEnabled` | `false` | Runs the read on Data Boost's independent compute. Needs `spanner.databases.useDataBoost`, is billed separately, and has a concurrency quota of its own |
| `rpcPriority` | *unset ⇒ `HIGH`* | `LOW`, `MEDIUM` or `HIGH`, applied to the reads that move the rows. `LOW` is what a backfill that must not disturb serving traffic wants. Spanner treats an unspecified priority as `HIGH`, so `MEDIUM` is a step down from the default rather than a restatement of it |
| `serviceAccountKeyFile` | *unset ⇒ ADC for the real service* | Service-account JSON key-file path read by a fresh or restored JobManager enumerator and by every TaskManager reader. The job graph contains the path, not the credential contents. Mutually exclusive with `emulatorEndpoint`; see [Credentials]({{< relref "docs/connectors/datastream/spanner" >}}#credentials) |
| `emulatorEndpoint` | *unset ⇒ the real service* | `host:port` of a Spanner emulator. Setting it also stops the client looking for credentials |

There is no per-fetch record cap here, and no options object: the cap is a correctness floor rather
than a knob, and promoting it would need a measurement.

## `SpannerChangeStreamSource.builder()`

The unbounded Change Streams source.
Its partition lifecycle, checkpoint recovery, and delivery semantics are under [Change Streams source]({{< relref "docs/connectors/datastream/spanner" >}}#change-streams-source).

| Option | Default | What it does |
|---|---|---|
| `database` | **required** | The database containing the change stream |
| `changeStreamName` | **required** | The change stream whose generated read function each partition query calls |
| `deserializer` | **required** | Emits zero or more non-null output records from each `DataChangeRecord` through a Flink `Collector`. Emit synchronously during the call; do not retain the collector |
| `startPosition` | `StartPosition.latest()` | Where a fresh ledger begins. Absolute, latest, and relative start positions resolve once on the coordinator |
| `resumeFallback` | *unset ⇒ fail an expired restore* | Where to restart after restored partition positions fall outside retention. Setting it permits discarding the whole stale partition ledger and can lose the unavailable interval |
| `absentRetentionFallback` | `7 days` | Retention to assume when `INFORMATION_SCHEMA.CHANGE_STREAM_OPTIONS` has no explicit retention row. It must be longer than the one-minute safety margin used at the moving retention boundary |
| `heartbeatInterval` | `2 s` | Service heartbeat interval, from 1 second through 5 minutes. Heartbeats advance the coordinator's complete-ledger source watermark |
| `rpcPriority` | `HIGH` | `LOW`, `MEDIUM`, or `HIGH`, applied to every partition query |
| `maxConcurrentQueriesPerSubtask` | `8` | Maximum partition queries one source subtask opens concurrently. Source parallelism multiplied by this value is the job's configured capacity, not a published Spanner quota |
| `serviceAccountKeyFile` | *unset ⇒ ADC for the real service* | Service-account JSON key-file path read by the JobManager coordinator and every TaskManager reader when they open. The job graph contains the path, not credential contents. Mutually exclusive with `emulatorEndpoint`; see [Credentials]({{< relref "docs/connectors/datastream/spanner" >}}#credentials) |
| `tableIncludeList` | empty | Java regular expressions for table names to retain. Each expression must match the complete Spanner-reported table name. Mutually exclusive with `tableExcludeList` |
| `tableExcludeList` | empty | Java regular expressions for table names to remove before deserialization. Each expression must match the complete name. Mutually exclusive with `tableIncludeList` |
| `columnIncludeList` | empty | Java regular expressions for `table.column` identifiers to retain. Primary-key columns are always retained. Mutually exclusive with `columnExcludeList` |
| `columnExcludeList` | empty | Java regular expressions for `table.column` identifiers to remove. Primary-key columns are always retained. Mutually exclusive with `columnIncludeList` |
| `skipMessagesWithoutChange` | `false` | Skips a data-change record when column projection removes every non-key value it reported. The default delivers the record with empty projected value objects |
| `emulatorEndpoint` | *unset ⇒ the real service* | `host:port` of a Spanner emulator. Setting it also stops the client looking for credentials |
