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

# BigQuery options

Every option the BigQuery sink and source take. What each one is *for* is on the
[BigQuery connector]({{< relref "docs/connectors/datastream/bigquery" >}}) page, linked from each
section; the three forms of the Default column are explained
[here]({{< relref "docs/reference" >}}#what-a-default-means).

## `BigQuerySink.builder()`

| Option | Default | What it does |
|---|---|---|
| `writeMethod` | `STORAGE_API_AT_LEAST_ONCE` | Which write path the sink dispatches to at graph construction |
| `destination` | **required**, unless `destinationResolver` is set | Writes every record to one fixed table |
| `destinationResolver` | — | Resolves the table per record. Rejected under `STORAGE_API_EXACTLY_ONCE` |
| `serializer` | **required** | Converts each record into the protobuf row the Storage Write API accepts, or into `null` to skip it |
| `createDisposition` | `CREATE_IF_NEEDED` | Whether a missing destination table is created or fails the job. `CREATE_IF_NEEDED` also lets `STORAGE_API_EXACTLY_ONCE` wait out the post-creation propagation window at commit time, so `CREATE_NEVER` opts out of both |
| `tableCreateOptions` | plain tables | [Creation settings](#tablecreateoptions) for every table the sink creates |
| `tableCreateOptionsProvider` | — | The same, resolved per destination. Overrides `tableCreateOptions` |
| `schemaUpdateOptions` | updates disabled | [What the sink may change](#schemaupdateoptions) about a destination table's schema |
| `failedRowHandler` | `FailureHandler.failJob()` | What happens to a row that terminally fails — fail, drop, or dead-letter. The queue behind `sendToDeadLetterQueue(...)` has [options of its own]({{< relref "docs/reference/pubsub" >}}#pubsubdeadletterqueuebuilder) |
| `location` | — | The BigQuery location shared by the destination tables. Setting it avoids a per-table metadata lookup when a write connection is opened; under `FILE_LOADS` it is the location every load job runs in and is recovered under, derived from each job's destination dataset when unset — which is what a sink routing to datasets in several regions should rely on |
| `serviceAccountKeyFile` | *unset → ADC* | Uses the service account in this JSON key file for every BigQuery client and for GCS staging under `FILE_LOADS`. The file is loaded at runtime and must exist on each TaskManager; rejected with either emulator endpoint |
| `defaultStreamOptions` | [defaults](#defaultstreamoptions) | Tuning for `STORAGE_API_AT_LEAST_ONCE`; rejected for the other two |
| `bufferedStreamOptions` | **required** for `STORAGE_API_EXACTLY_ONCE` | [Tuning](#bufferedstreamoptions) for that method; rejected for the other two |
| `fileLoadsOptions` | **required** for `FILE_LOADS` | [Settings](#fileloadsoptions) for that method; rejected for the other two |
| `emulatorEndpoint` | — | Sends the Storage Write API traffic to a BigQuery emulator at `host:port`, over plaintext and without credentials. Rejected under `FILE_LOADS` |
| `emulatorRestEndpoint` | — | The same for table creation and schema updates, which go over REST — a transport BigQuery serves on a different port, so the two endpoints are separate |

The two emulator endpoints are for testing against a local emulator and nothing else: both use
plaintext and no credentials, and both are rejected under `FILE_LOADS`, which stages files to Cloud
Storage that no emulator here stands in for. A malformed `host:port` fails in the setter, on the
client, rather than as a connection error after the job is deployed.

Each write method's options object is required by exactly one method and rejected by the others, so
a misplaced one fails when the job graph is built rather than being ignored. See
[Delivery guarantees]({{< relref "docs/connectors/datastream/bigquery" >}}#delivery-guarantees-and-state)
for how the three methods differ, and
[Error handling]({{< relref "docs/connectors/datastream/bigquery" >}}#error-handling) for the
failed-row policies.

## `DefaultStreamOptions`

Tuning for `STORAGE_API_AT_LEAST_ONCE`, set through `defaultStreamOptions(...)`. Every knob is
defaulted, so an unconfigured sink uses this table. The three groups configure three distinct
layers — the reasoning, including why `maxInflightRequests` departs from the SDK's own default and
why the pool's caveats are JVM-global, is under
[Tuning]({{< relref "docs/connectors/datastream/bigquery" >}}#tuning).

**Connector batching and recovery budget.**

| Option | Default | What it does |
|---|---|---|
| `maxAppendRequestBytes` | 512 KiB | Serialized-row bytes buffered per destination before an append is issued |
| `recoveryInitialBackoff` | 500 ms | First backoff of the connector's own re-append schedule |
| `recoveryMaxBackoff` | 10 s | Cap that schedule doubles up to, before jitter |
| `recoveryMaxAttempts` | 10 | Attempt cap of that schedule |

**SDK in-stream retries**, applied to retriable append failures before they reach the writer.

| Option | Default | What it does |
|---|---|---|
| `retryInitialDelay` | 500 ms | First retry delay; `0` (gax's own default) means none |
| `retryDelayMultiplier` | 2.0 | Delay multiplier |
| `retryMaxDelay` | 30 s | Delay cap; `0` clamps every retry delay to none |
| `retryMaxAttempts` | 5 | Attempt cap |
| `maxRetryDuration` | 5 min *(the SDK's own)* | Ceiling on retrying one failure across attempts; `0` is the SDK's own value for *no* time limit |

**Connection pool (multiplexing).** JVM-global: the pool is static per (location, credentials) and
adopts whichever stream writer is built first.

| Option | Default | What it does |
|---|---|---|
| `maxInflightRequests` | 100 | In-flight append requests per pooled connection. Deliberately not the SDK's 1000 |
| `maxInflightBytes` | 100 MiB *(the SDK's own)* | In-flight append bytes per pooled connection |
| `minConnectionsPerRegion` | 2 *(the SDK's own)* | Starting connection count per pool |
| `maxConnectionsPerRegion` | 20 *(the SDK's own)* | Connection ceiling per pool |

**Writer housekeeping.**

| Option | Default | What it does |
|---|---|---|
| `destinationIdleTimeout` | 1 h | How long a destination may go without records before its stream writer is closed and dropped. Set a very large duration to never evict — up to about 292 years (`Duration.ofNanos(Long.MAX_VALUE)`), the largest the builder accepts |
| `flushInterval` | disabled | Periodic processing-time flush, for streaming jobs running without checkpointing. A mitigation, not a substitute for the guarantee |
| `perDestinationMetrics` | `false` | Register `recordsSend`/`sendErrors` counters per destination table. Off by default because Flink cannot unregister a metric — see [Metrics]({{< relref "docs/connectors/datastream/bigquery" >}}#metrics) |

## `BufferedStreamOptions`

Required by `STORAGE_API_EXACTLY_ONCE` and rejected by the other two methods; every knob is
defaulted, so `builder().build()` means "the defaults". The recovery schedule governs stream
creation, transient re-appends, the restore probe and the committer's flush retries — see
[Exactly-once]({{< relref "docs/connectors/datastream/bigquery" >}}#exactly-once-buffered-streams).

| Option | Default | What it does |
|---|---|---|
| `maxAppendRequestBytes` | 512 KiB | Serialized-row bytes per append request |
| `recoveryInitialBackoff` | 500 ms | First backoff of the connector-driven recovery schedule |
| `recoveryMaxBackoff` | 10 s | Cap that schedule doubles up to, before jitter |
| `recoveryMaxAttempts` | 10 | Attempt cap of that schedule |
| `retryInitialDelay` | 500 ms | First delay of the SDK's in-stream retries; `0` (gax's own default) means none |
| `retryDelayMultiplier` | 2.0 | Delay multiplier of that schedule |
| `retryMaxDelay` | 30 s | Delay cap of that schedule; `0` clamps every retry delay to none |
| `retryMaxAttempts` | 5 | Attempt cap of that schedule |
| `maxRetryDuration` | 5 min *(the SDK's own)* | Ceiling on retrying one failure across attempts; `0` is the SDK's own value for *no* time limit |

Unlike the default-stream path these appenders never enter the SDK's connection pool, so there is no
pool-sizing knob here.

## `FileLoadsOptions`

Required by `FILE_LOADS` and rejected by the other two methods. Only `stagingPath` has no default —
see [File loads]({{< relref "docs/connectors/datastream/bigquery" >}}#file-loads), which is also
where the load-job quota that shapes `minCheckpointInterval` is set out.

| Option | Default | What it does |
|---|---|---|
| `stagingPath` | **required** | The `gs://` prefix staged files are written to |
| `writeDisposition` | `WRITE_APPEND` | How loaded rows land in a table that already holds data. Streaming requires `WRITE_APPEND` |
| `tempDataset` | the destination's own dataset | Where temporary tables go when a table's staged files exceed one load job's limits |
| `minCheckpointInterval` | 2 min | Smallest checkpoint interval accepted in streaming; a shorter one is rejected when the graph is built |
| `maxStagingFileBytes` | 16 MiB | Size at which an open staging file is finished and the next one opened. [File loads]({{< relref "docs/connectors/datastream/bigquery" >}}#file-loads) carries the measurement it comes from and when raising it is worthwhile |
| `stagingFormat` | `AVRO` | The file format rows are staged in. `PARQUET` is opt-in and needs dependencies this connector does not ship — see [File loads]({{< relref "docs/connectors/datastream/bigquery" >}}#file-loads) before selecting it |
| `parquetCompression` | `ZSTD` | How Parquet staging files are compressed. Rejected under `AVRO`. `NONE` is the only value needing no Hadoop runtime, and stages more bytes than Avro does |
| `loadJobPollInitialBackoff` | 1 s | First backoff between polls of a submitted load or copy job |
| `loadJobPollMaxBackoff` | 30 s | Cap of that backoff, before jitter. There is deliberately no attempt cap |
| `schemaReconcileInitialBackoff` | 500 ms | First backoff after losing an etag race while reconciling a table's schema |
| `schemaReconcileMaxBackoff` | 10 s | Cap of that backoff, before jitter |
| `schemaReconcileMaxAttempts` | 10 | Attempt cap of the schema reconcile |
| `perDestinationMetrics` | `false` | Register `recordsSend`/`sendErrors` counters per destination table. Off by default because Flink cannot unregister a metric — see [Metrics]({{< relref "docs/connectors/datastream/bigquery" >}}#metrics) |

## `TableCreateOptions`

Applied by `tableCreateOptions(...)` or `tableCreateOptionsProvider(...)` to tables the sink
creates. **Creation only** — an existing table is never modified by them. See
[Table auto-creation]({{< relref "docs/connectors/datastream/bigquery" >}}#table-auto-creation).

| Option | Default | What it does |
|---|---|---|
| `timePartitioning(type)` | unpartitioned | Partitions on the ingestion time at the given granularity |
| `timePartitioning(type, field)` | unpartitioned | Partitions on the given `TIMESTAMP`, `DATE` or `DATETIME` column instead. A `DATE` column takes no `HOUR` granularity — BigQuery refuses that table at creation, and only the SQL layer checks it client-side |
| `timePartitioningExpiration` | partitions never expire | How long BigQuery keeps a partition |
| `clusteredFields` | not clustered | Clusters on the given columns in precedence order, at most four |

## `SchemaUpdateOptions`

Both flags are off by default, which is what makes connector-driven schema updates opt-in — see
[Schema evolution]({{< relref "docs/connectors/datastream/bigquery" >}}#schema-evolution) for why,
and note `STORAGE_API_EXACTLY_ONCE` rejects an enabled options object outright.

| Option | Default | What it does |
|---|---|---|
| `allowNewFields` | off | Lets the sink add columns the serializer's schema has and the table lacks |
| `allowFieldRelaxation` | off | Lets the sink relax a `REQUIRED` column to `NULLABLE` |

## `ProtoSchemaOptions`

Passed to `ProtoMessageSerializer.of(...)`. Everything configured is unioned, so a field selected
any of these ways gets the column type. See [Protobuf
messages]({{< relref "docs/connectors/datastream/bigquery" >}}#protobuf-messages),
[JSON columns]({{< relref "docs/connectors/datastream/bigquery" >}}#json-columns) and
[Geography columns]({{< relref "docs/connectors/datastream/bigquery" >}}#geography-columns) — in
particular for why an unmatched *path* is an error while an unmatched *option number* is not.

| Option | Default | What it does |
|---|---|---|
| `deriveRequiredColumns` | off, every non-repeated column `NULLABLE` | Derives `REQUIRED` from each field's presence instead |
| `jsonFieldPath` / `jsonFieldPaths` | no JSON columns | Maps the message or string field at a dotted path to a `JSON` column |
| `jsonFieldOption` | no JSON columns | Maps every field carrying the given `bool` extension, wherever it appears |
| `jsonFieldOptionNumber` | no JSON columns | The same by extension number, when the generated class is unavailable |
| `geographyFieldPath` / `geographyFieldPaths` | no geography columns | Maps the string field at a dotted path to a `GEOGRAPHY` column |
| `geographyFieldOption` | no geography columns | Maps every string field carrying the given `bool` extension |
| `geographyFieldOptionNumber` | no geography columns | The same by extension number |

Prefer the extension over the bare number: protobuf's private extension range has no registry, so
the number alone cannot tell your annotation from an unrelated one at the same number.

## `AvroSchemaOptions`

Passed to `AvroRecordSerializer.of(...)`. The two markers exist because Avro has no standard JSON
logical type and nothing that says "this string is a geometry"; there is no annotation-driven form,
because Avro has no field-option mechanism to key off. See
[Avro records]({{< relref "docs/connectors/datastream/bigquery" >}}#avro-records).

| Option | Default | What it does |
|---|---|---|
| `deriveRequiredColumns` | off, every non-repeated column `NULLABLE` | Derives `REQUIRED` for any field that is not a `["null", T]` union |
| `jsonFieldPath` / `jsonFieldPaths` | no JSON columns | Maps the string field at a dotted path to a `JSON` column |
| `geographyFieldPath` / `geographyFieldPaths` | no geography columns | Maps the string field at a dotted path to a `GEOGRAPHY` column |

## `JsonDocumentSerializerOptions`

Passed to `JsonDocumentSerializer.of(...)`. There is deliberately no nullability option here —
you supply the schema, so a `REQUIRED` column in it is your own statement. See
[JSON records]({{< relref "docs/connectors/datastream/bigquery" >}}#json-records).

| Option | Default | What it does |
|---|---|---|
| `ignoreUnknownFields` | off, an unknown field fails the record | Drops document fields the schema has no column for |

## `BigQuerySource.builder()`

The bounded source over the Storage Read API. What each option is *for*, and what BigQuery actually
does with the two stream-count knobs, is under
[Source]({{< relref "docs/connectors/datastream/bigquery" >}}#source).

| Option | Default | What it does |
|---|---|---|
| `table` | **required**, unless `query` is set | The table to read. To read a named view, combine it with `materializeViews`; the Storage Read API cannot read the view directly |
| `query` | **required**, unless `table` is set | The explicit-query route: runs this GoogleSQL query first, then reads its result. It can read a view without naming that view as `table`. Billed twice: the bytes the query scans, then the bytes the read session scans |
| `deserializer` | **required** | Converts each Avro row into a record, or into `null` to skip it |
| `parentProject` | the table's own project | The project the read session belongs to and is billed to. Set it to read a table in another project, such as a public dataset. **Required with `query`**, which names no table to default from and which is billed to it as well |
| `materializeViews` | off | Reads a `table` that turns out to be a view by materializing it: one metadata call at job start, then `SELECT … FROM the_view` and a read of its result. An ordinary table is read directly. Off by default because it costs that call, and because it bills a query nobody wrote. Spelled `viewsEnabled` in the Spark and Dataproc connectors |
| `queryLocation` | BigQuery infers it from the tables the query names | The location the query job runs in. `query` or `materializeViews` only |
| `queryResultDataset` | BigQuery's anonymous dataset | Where the query's (or the materialized view's) result lands. Unset, BigQuery writes it into a hidden dataset of its own, expires it after about a day and charges no storage for it. Set, the connector creates a table in this dataset with a one-day expiration, and storage is charged until it expires. `query` or `materializeViews` only |
| `reuseQueryResultWithin` | off — every plan runs the query | Lets a re-planned job reuse a previous attempt's query job, for attempts within this window: same Flink job name + same query configuration → one query job, so a pre-checkpoint JobManager failover stops billing the query twice. A redeploy under the same name inside the window also reuses the result — rename the job to force a fresh one. Positive, at most 24 hours; **requires `queryLocation`**, because BigQuery scopes a job to (project, location, id) and a location-less look-up sees only the US multi-region. `query` or `materializeViews` only |
| `selectedFields` | every column | The columns to read. Applied by BigQuery when the session is created, so the rest are neither transferred nor scanned — and scanned bytes are what a read is charged for |
| `rowRestriction` | no filter | A BigQuery filter expression — a `WHERE` clause without the keyword — applied before any row is sent |
| `snapshotTime` | the table's current contents | Reads the table as of an instant inside BigQuery's time-travel window, seven days by default; an older instant is rejected when the session is created |
| `maxStreamCount` | `0`, BigQuery decides | An upper bound on the read streams the session gets. A cap and never a floor: a small table is read by one stream however many are asked for |
| `preferredMinStreamCount` | `0`, no request | How many read streams to ask BigQuery for. Best effort; must not exceed `maxStreamCount` when both are set |
| `maxRecordsPerFetch` | 10000 | The most rows one fetch hands to the task thread, so a checkpoint can be taken part-way through a response block |
| `retryMaxAttempts` | 25 | How many consecutive attempts at a read stream the client library may make **without progress** before the read fails. An attempt that delivered rows resets the count; without a bound the client retries for twenty-four hours |
| `serviceAccountKeyFile` | *unset → ADC* | Uses the service account in this JSON key file for the read-session, stream-reading, query, and view-materialization clients. Loaded at runtime; the same key file must exist at this path on the JobManager and every TaskManager. Rejected with either emulator endpoint |
| `emulatorEndpoint` | — | Sends the source's read traffic to a BigQuery emulator at `host:port`, over plaintext and without credentials. The whole of it for a `table` source, which makes no REST call |
| `emulatorRestEndpoint` | — | The REST half of `emulatorEndpoint`, for the query job and the view lookup. `query` or `materializeViews` only |
