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

# BigQuery SQL Connector

The `bigquery` connector writes a table to BigQuery through the module
`flink-connector-gcp-bigquery`. It is a mapping onto the DataStream sink documented in
[BigQuery]({{< relref "docs/connectors/datastream/bigquery" >}}) — that page carries the design,
the delivery guarantees and the error handling; this one carries the DDL surface. Per-feature
status is in the module README.

`sink.parallelism` comes from Flink's own `FactoryUtil` rather than from this connector. There is
no `format` option: a BigQuery row is structured and the DDL schema *is* the schema, so the
connector supplies its own `RowData` serializer.

```sql
CREATE TABLE events (
  id STRING,
  amount BIGINT,
  event_ts TIMESTAMP_LTZ(6),
  attributes ROW<source STRING, version INT>
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'dataset' = 'analytics',
  'table' = 'events'
);

INSERT INTO events
SELECT id, amount, event_ts, ROW(source, version) FROM staged_events;
```

## Options

Every option maps onto one builder setter of the DataStream API, which stays the source of truth.
An option left out of the DDL leaves that setter uncalled, so its default is whatever the connector
or the SDK already uses — the default is never restated here. The full list of defaults is in the
[configuration reference]({{< relref "docs/reference/bigquery" >}}).

### Destination

| Option | Type | Maps to |
|---|---|---|
| `project` | String | The project part of `destination(...)`; a bare project id |
| `dataset` | String | The dataset part of `destination(...)` |
| `table` | String | The table part of `destination(...)`. One SQL table writes to one BigQuery table: per-record routing has no SQL surface and stays on the DataStream API |
| `emulator-endpoint` | String | `emulatorEndpoint(...)`, the Storage Write API's gRPC endpoint as `host:port` — parsed when the planner builds the sink, so a malformed value fails there |
| `emulator-rest-endpoint` | String | `emulatorRestEndpoint(...)`, the table-metadata REST endpoint. Separate because BigQuery serves the two transports on different ports |

### Sink

| Option | Type | Maps to |
|---|---|---|
| `sink.write-method` | Enum | `writeMethod(...)`. Only `storage-api-at-least-once` is available from SQL so far |
| `sink.create-disposition` | Enum | `createDisposition(...)` — `create-if-needed` or `create-never` |
| `sink.location` | String | `location(...)` |
| `sink.schema-update.allow-new-fields` | Boolean | `SchemaUpdateOptions.allowNewFields()` |
| `sink.schema-update.allow-field-relaxation` | Boolean | `SchemaUpdateOptions.allowFieldRelaxation()` |
| `sink.derive-required-columns` | Boolean | Derives a `REQUIRED` column from a `NOT NULL` one; off, every derived column is `NULLABLE` |
| `sink.json-field-paths` | List&lt;String&gt; | Derives the named columns as BigQuery `JSON` |
| `sink.geography-field-paths` | List&lt;String&gt; | Derives the named columns as BigQuery `GEOGRAPHY` |
| `sink.parallelism` | Integer | The sink's parallelism (Flink's own option) |

### Sink tuning — `storage-api-at-least-once`

Setting any one of these builds a `DefaultStreamOptions`; the rest stay at the connector's
defaults.

| Option | Type | Maps to |
|---|---|---|
| `sink.default-stream.max-append-request-bytes` | MemorySize | `maxAppendRequestBytes(...)` |
| `sink.default-stream.recovery.initial-backoff` | Duration | `recoveryInitialBackoff(...)` |
| `sink.default-stream.recovery.max-backoff` | Duration | `recoveryMaxBackoff(...)` |
| `sink.default-stream.recovery.max-attempts` | Integer | `recoveryMaxAttempts(...)` |
| `sink.default-stream.retry.initial-delay` | Duration | `retryInitialDelay(...)` |
| `sink.default-stream.retry.delay-multiplier` | Double | `retryDelayMultiplier(...)` |
| `sink.default-stream.retry.max-delay` | Duration | `retryMaxDelay(...)` |
| `sink.default-stream.retry.max-attempts` | Integer | `retryMaxAttempts(...)` |
| `sink.default-stream.retry.max-duration` | Duration | `maxRetryDuration(...)` |
| `sink.default-stream.max-inflight-requests` | Integer | `maxInflightRequests(...)` |
| `sink.default-stream.max-inflight-bytes` | MemorySize | `maxInflightBytes(...)` |
| `sink.default-stream.min-connections-per-region` | Integer | `minConnectionsPerRegion(...)` |
| `sink.default-stream.max-connections-per-region` | Integer | `maxConnectionsPerRegion(...)` |
| `sink.default-stream.destination-idle-timeout` | Duration | `destinationIdleTimeout(...)` |
| `sink.default-stream.flush-interval` | Duration | `flushInterval(...)` |
| `sink.default-stream.per-destination-metrics` | Boolean | `perDestinationMetrics(...)` |

## Type mapping

A column's BigQuery type is derived from its SQL type, and the derived schema is what the connector
creates a missing table with.

| Flink type | BigQuery type |
|---|---|
| `CHAR`, `VARCHAR`, `STRING` | `STRING`, or `JSON` / `GEOGRAPHY` when marked |
| `BOOLEAN` | `BOOL` |
| `BINARY`, `VARBINARY`, `BYTES` | `BYTES` |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | `INT64` |
| `FLOAT`, `DOUBLE` | `FLOAT64` |
| `DECIMAL(p, s)` | `NUMERIC` when `s <= 9` and `p - s <= 29`, otherwise `BIGNUMERIC` |
| `DATE` | `DATE` |
| `TIME(p)` | `TIME`; `p > 3` is rejected |
| `TIMESTAMP(p)` | `DATETIME`; `p > 6` is rejected |
| `TIMESTAMP_LTZ(p)` | `TIMESTAMP`; `p > 6` is rejected |
| `ROW` | `STRUCT`, recursively — or `JSON` when marked |
| `ARRAY<T>` | `REPEATED T`; nullable elements and nested arrays are rejected |
| `MAP<K, V>`, `MULTISET<T>` | `REPEATED STRUCT<key, value>` |
| `TIMESTAMP WITH TIME ZONE`, `INTERVAL`, `RAW`, `NULL`, structured and distinct types | rejected when the job graph is built |

Every rejection above happens on the client, when the job graph is built — not per record.

### `TIMESTAMP` is civil and `TIMESTAMP_LTZ` is an instant

`TIMESTAMP` is a wall-clock type, so it becomes BigQuery's `DATETIME`; `TIMESTAMP_LTZ` is an
instant, so it becomes `TIMESTAMP`. The GoogleCloudDataproc connector maps these the other way
round, which stores a wall-clock value as an instant and an instant as a wall-clock value. If you
are migrating from it, this is the row to check.

### `TIME` stops at millisecond precision

BigQuery's `TIME` holds microseconds, but Flink carries a time of day as an `int` of milliseconds,
so a column declared `TIME(6)` could only ever be filled to `TIME(3)`. Rather than derive a schema
claiming more than the values can carry, `TIME(p)` with `p > 3` is rejected.

### Marked columns

`sink.json-field-paths` and `sink.geography-field-paths` name columns by dotted path — `payload`,
`event.body`, `the_map.value`. A map's key cannot be marked.

A marked `STRING` is passed through **verbatim and unvalidated**: malformed JSON or an invalid
geometry is a row-level error BigQuery reports, exactly as on the other write paths. A marked `ROW`
is different — it is rendered as JSON text rather than expanded into a `STRUCT`, so its columns
become object members. Strings are escaped, `BYTES` becomes base64, `DECIMAL` an unquoted number,
and the temporal types ISO-8601 strings. A `MULTISET` has no JSON form and is rejected, as is a map
with non-string keys. `GEOGRAPHY` may only mark a `STRING`: no structured value means a geometry to
BigQuery.

## Delivery guarantees

At-least-once, and only with checkpointing enabled: rows are durable once a checkpoint completes,
and a restart may re-append rows the previous attempt had already written. See
[Delivery guarantees]({{< relref "docs/connectors/datastream/bigquery" >}}#delivery-guarantees-and-state)
on the DataStream page for what each write method promises.

### Inserts only

The changelog mode is insert-only. An updating query — an aggregation without a window, a
non-windowed join — is rejected when the plan is built, because BigQuery's append-only write paths
cannot express a retraction and appending the `-U` and `-D` rows as ordinary ones would corrupt the
table silently. Upserts are tracked separately.

## Design decisions

**A row that fails to convert fails the job.** The failed-row handler is not exposed from SQL: there
is no way to name a dead-letter queue in a `WITH` clause, and a dropping policy is a decision to
make against a concrete need rather than in advance.

**`PARTITIONED BY` is rejected rather than consumed.** Flink's clause models Hive-style value
partitioning, which BigQuery time partitioning is not, and ingestion-time partitioning has no column
to name at all — so the clause could never cover the whole feature. The sink does not implement
`SupportsPartitioning`, which makes a partition spec fail at plan time instead of being silently
ignored. `INSERT OVERWRITE` is refused for the same reason.

**No metadata columns.** A BigQuery row has no envelope around it, so there is nothing to expose.

**One table per SQL table.** Per-record routing and per-destination creation options stay on the
DataStream API; a SQL `INSERT INTO` names one table, and a table-name pattern is deferred until a
concrete need appears.

**Enum options carry their DDL spelling in `toString()`**, so `sink.write-method` takes
`storage-api-at-least-once` rather than the Java constant. Flink resolves an enum-valued option by
matching `toString()` case-insensitively and normalizing nothing else.

## Testing

The unit tests drive the factory without a planner and pin every row of the type mapping. The
integration tests run `CREATE TABLE` and `INSERT INTO` through the planner against the
goccy/bigquery-emulator container, with the two emulator endpoints interpolated into the `WITH`
clause — so they exercise the production factory rather than a test seam. Column types the emulator
does not implement are covered by the unit tests and by the gated real-GCP suite.
