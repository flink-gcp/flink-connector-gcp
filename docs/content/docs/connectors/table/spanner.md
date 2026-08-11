---
title: Spanner
type: docs
weight: 50
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

# Spanner SQL connector

The `spanner` connector reads bounded Table API and SQL scans, serves primary-key lookup joins, and writes rows through `flink-connector-gcp-spanner`.
It maps onto the [DataStream source and sink]({{< relref "docs/connectors/datastream/spanner" >}}), so partitioning, snapshot, batching, retry, delivery, metrics, and failure behavior remain the same.

```sql
CREATE TABLE orders (
  order_id BIGINT,
  customer STRING,
  total DECIMAL(38, 9),
  updated_at TIMESTAMP_LTZ(9),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'database' = 'orders-db',
  'table' = 'orders'
);

INSERT INTO orders SELECT order_id, customer, total, updated_at FROM staged_orders;

SELECT customer, total FROM orders;
```

The plain connector jar requires its transitive dependencies on the job classpath.
A relocated SQL uber-jar is tracked separately by [#505]({{< param BookRepo >}}/issues/505).

## Mutation behavior

A declared `PRIMARY KEY` makes the sink an upsert sink.
`INSERT` and `UPDATE_AFTER` rows use Spanner `insertOrUpdate`, and `DELETE` rows use a key built in the declared column order.
The key may be composite, but every member must map to a Spanner key type.

Without a primary key, the sink accepts insert-only input and uses Spanner `insert`.
This preserves duplicate-key errors instead of pretending that an unknown physical key can support upserts or deletes.
As with other Flink connectors, `PRIMARY KEY ... NOT ENFORCED` describes the contract to the planner; the connector does not verify uniqueness.

The delivery guarantee remains at-least-once.
Upserts and deletes are idempotent under replay, while insert-only writes can fail on a replayed primary key.

## Type mapping

| Flink SQL type | Spanner type |
|---|---|
| `BOOLEAN` | `BOOL` |
| `BIGINT` | `INT64` |
| `FLOAT` / `DOUBLE` | `FLOAT32` / `FLOAT64` |
| `DECIMAL(38, 9)` | `NUMERIC` |
| `CHAR` / `VARCHAR` / `STRING` | `STRING` |
| `BINARY` / `VARBINARY` / `BYTES` | `BYTES` |
| `DATE` | `DATE` |
| `TIMESTAMP_LTZ(0..9)` | `TIMESTAMP` |
| `ARRAY<T>` | `ARRAY<T>` |

The mapping is recursive for arrays, and nullable Flink values become null Spanner values.
`ROW` is rejected because Spanner `STRUCT` cannot be stored in a table column.
Plain `TIMESTAMP` is rejected because it has no time-zone semantics; use `TIMESTAMP_LTZ` for an instant.
Other decimal precisions and scales are rejected because mapping them to `NUMERIC` would not be lossless.

## Scan behavior

The source is bounded and reads the table through Spanner `partitionRead` at one shared snapshot.
Top-level projection is pushed into the requested Spanner column list, so unused columns do not cross the network.
Nested projection is not advertised.
If the planner requests no physical column, the connector reads the first declared column as a carrier but emits zero-field rows in the requested shape.

Partition count and size are service hints, not exact split controls.
The default timestamp bound is strong; set either a read timestamp or exact staleness, never both.
There are deliberately no column-range partition options because Spanner chooses partition boundaries from physical storage.

## Lookup behavior

The source supports temporal lookup joins when the equality key contains every column of the declared `PRIMARY KEY`.
Composite keys are encoded in the DDL declaration order even when the planner supplies the predicates in another order.
A null key or an absent Spanner row produces no joined row.

`lookup.async` chooses Spanner's synchronous `readRow` or asynchronous `readRowAsync` API.
Flink's standard `lookup.cache = NONE` and `PARTIAL` modes are supported; `FULL` is rejected because it would require a scan-backed cache with different snapshot and refresh semantics.
The standard partial-cache expiry, size, and missing-key options apply unchanged.
`lookup.max-retries` retries only `ABORTED`, `DEADLINE_EXCEEDED`, and `UNAVAILABLE` point-read failures and counts retries after the initial request.

JSON, protocol buffers, and enums share carrier types with ordinary columns, so the DDL marks them explicitly:

```sql
WITH (
  'schema.json-field-paths' = 'metadata;payloads',
  'schema.proto-type-names' = 'event:example.events.Event',
  'schema.enum-type-names' = 'status:example.events.Status'
)
```

JSON fields use `STRING`, PROTO fields use `BYTES`, and ENUM fields use `BIGINT` in the Flink schema.
Set `dialect = 'POSTGRESQL'` for PostgreSQL-dialect databases; JSON markers then produce `jsonb` values.
PROTO and ENUM markers require `GOOGLE_STANDARD_SQL` and are rejected for PostgreSQL databases.
A marker may name a nested field with dot notation or an entire array field.
Every marker must resolve to exactly one physical field and no field may have more than one marker.

## Options

| Option | Default | What it does |
|---|---|---|
| `project` | **required** | The Google Cloud project containing the instance |
| `instance` | **required** | The Spanner instance containing the database |
| `database` | **required** | The database containing the table |
| `table` | **required** | The table receiving rows |
| `emulator-endpoint` | *unset ⇒ the real service* | `host:port` of a Spanner emulator; setting it also stops credential discovery |
| `schema.json-field-paths` | empty | Semicolon-separated physical field paths whose `STRING` carriers map to Spanner JSON |
| `dialect` | `GOOGLE_STANDARD_SQL` | Database dialect; use `POSTGRESQL` for PostgreSQL `jsonb` values |
| `schema.proto-type-names` | empty | Comma-separated `field-path:fully.qualified.Type` entries whose `BYTES` carriers map to Spanner PROTO |
| `schema.enum-type-names` | empty | Comma-separated `field-path:fully.qualified.Type` entries whose `BIGINT` carriers map to Spanner ENUM |
| `scan.partition.max-partitions` | *unset* | Desired maximum partition count passed to Spanner as a hint |
| `scan.partition.size` | *unset* | Desired partition size passed to Spanner as a hint |
| `scan.data-boost-enabled` | `false` | Whether scans use Data Boost compute |
| `scan.rpc-priority` | *unset ⇒ Spanner default* | Priority of scan RPCs |
| `scan.timestamp-bound.read-timestamp` | *unset* | RFC 3339 snapshot timestamp; mutually exclusive with exact staleness |
| `scan.timestamp-bound.exact-staleness` | *unset ⇒ strong read* | Exact age of the snapshot; mutually exclusive with read timestamp |
| `scan.parallelism` | *unset ⇒ operator parallelism* | Flink's standard source parallelism override |
| `lookup.async` | `false` | Use asynchronous `readRowAsync` point reads instead of synchronous `readRow` |
| `lookup.cache` | `NONE` | Flink's standard lookup cache mode; `NONE` and `PARTIAL` are supported |
| `lookup.max-retries` | `3` | Retries after the initial point read, for transient failures only |
| `lookup.partial-cache.expire-after-access` | *unset* | Flink's standard partial-cache access expiry |
| `lookup.partial-cache.expire-after-write` | *unset* | Flink's standard partial-cache write expiry |
| `lookup.partial-cache.cache-missing-key` | `true` | Whether partial cache records lookup misses |
| `lookup.partial-cache.max-rows` | *unset* | Maximum rows retained by the partial cache |
| `sink.buffer-flush.max-cells` | `5000` | Maps to `maxBatchCells` |
| `sink.buffer-flush.max-mutations` | `500` | Maps to `maxBatchMutations` |
| `sink.buffer-flush.max-size` | `1 mb` | Maps to `maxBatchBytes` |
| `sink.buffer-flush.max-commit-delay` | *unset* | Maps to `maxCommitDelay` |
| `sink.rpc-priority` | *unset ⇒ Spanner treats it as `HIGH`* | Maps to `rpcPriority` |
| `sink.retry.initial-backoff` | `500 ms` | Maps to `retryInitialBackoff` |
| `sink.retry.max-backoff` | `10 s` | Maps to `retryMaxBackoff` |
| `sink.retry.max-attempts` | `10` | Maps to `retryMaxAttempts` |
| `sink.parallelism` | *unset ⇒ operator parallelism* | Flink's standard sink parallelism override |

The sink options map directly onto the DataStream writer options; their validation limits are listed in the [Spanner reference]({{< relref "docs/reference/spanner" >}}#spannerwriteroptions).
