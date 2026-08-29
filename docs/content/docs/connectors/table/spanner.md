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

# Spanner SQL connector

The `spanner` connector reads bounded Table API and SQL scans, emits unbounded Change Streams changelogs, serves primary-key lookup joins, and writes rows through `flink-connector-gcp-spanner`.
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

Use `flink-sql-connector-gcp-spanner`, the relocated SQL uber-jar, for SQL deployments.
Place `flink-sql-connector-gcp-spanner-<version>.jar` in Flink's `lib/` before starting the cluster,
or load it for one SQL Client session:

```sql
ADD JAR '/path/to/flink-sql-connector-gcp-spanner-0.1.0-SNAPSHOT.jar';
```

The artifact bundles the connector and its runtime dependencies while leaving Flink APIs provided
by the cluster. Its bundled packages are relocated so it can coexist with other connector jars and
with application dependencies. DataStream applications should depend on
`flink-connector-gcp-spanner` instead of the SQL uber-jar.

## Credentials

`service-account-key-file` selects one service-account JSON key for the sink, bounded scan, Change Streams scan, and synchronous or asynchronous lookup paths.
When it and `emulator-endpoint` are absent, every path uses Application Default Credentials.
The option stores only the path in the job graph and reads the file in each runtime process that creates a Spanner client.
Sink and lookup jobs need the path on applicable TaskManagers, while bounded and Change Streams scans need it on the JobManager and applicable TaskManagers.
Mount the same configured path in all of those containers, including after a restart or restore.
The option is mutually exclusive with `emulator-endpoint`; prefer an attached service account or Workload Identity when available.
See the [DataStream credential deployment contract]({{< relref "docs/connectors/datastream/spanner" >}}#credentials) for the exact process boundaries and failure behavior.

## Named schemas

Set `schema` when the destination or source table belongs to a named Spanner schema.
The connector qualifies that schema with `table` for mutations, bounded scans, and synchronous and asynchronous lookups.
When `scan.index` is set, the same schema also qualifies the index because Spanner requires a table and its index to share a schema.
Leaving `schema` unset retains the empty GoogleSQL schema or PostgreSQL `public`.

The `schema`, `table`, and `scan.index` values each name one identifier component when `schema` is set.
Do not put a dot-qualified name in any of these options.
GoogleSQL accepts an unquoted identifier or a backtick-quoted identifier, and PostgreSQL accepts an unquoted identifier or a double-quoted identifier.
PostgreSQL folds unquoted values to lower case and preserves quoted values, including their case.
GoogleSQL compares schema-object names case-insensitively.
Quoted values use one canonical spelling: PostgreSQL doubles an embedded double quote, while GoogleSQL escapes an embedded backtick or backslash with a backslash.
Numeric, octal, and Unicode escape spellings are not accepted in these options.
The connector rejects blank values, multipart values, mismatched quotes, and non-canonical quote escapes before opening a client.
It does not duplicate Spanner's identifier character, length, or keyword rules.
The connector removes SQL quoting after decoding each component because Spanner's native data APIs accept catalog names, not SQL identifier tokens.
The bounded scan resolves those decoded names through `INFORMATION_SCHEMA`, while sink mutations and lookup reads leave final name validation to the Spanner API.
Quote a reserved word when it is used as the schema, the first component of the qualified name.

```sql
CREATE TABLE sales_orders (
  order_id BIGINT,
  total DECIMAL(38, 9),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'database' = 'orders-db',
  'schema' = 'sales',
  'table' = 'orders',
  'scan.index' = 'orders_by_total'
);
```

## Mutation behavior

See [Write and key-collision semantics]({{< relref "docs/connectors/delivery-guarantees" >}}#write-and-key-collision-semantics)
for the cross-connector distinction between a planner key and a destination write operation.

A declared `PRIMARY KEY` makes the sink an upsert sink.
`INSERT` and `UPDATE_AFTER` rows use Spanner `insertOrUpdate`, and `DELETE` rows use a key built in the declared column order.
The key may be composite, but every member must map to a Spanner key type.

Without a primary key, the sink accepts insert-only input and uses Spanner `insert`.
This preserves duplicate-key errors instead of pretending that an unknown physical key can support upserts or deletes.
As with other Flink connectors, `PRIMARY KEY ... NOT ENFORCED` describes the contract to the planner; the connector does not verify uniqueness.

The delivery guarantee remains at-least-once.
Replaying one upsert or delete is idempotent, while insert-only writes can fail on a replayed primary key.
The writer submits records as separate `BatchWrite` mutation groups, whose application order is not
guaranteed, so successive updates to the same key do not provide a latest-input-value guarantee.

## Type mapping

Spanner's [GoogleSQL data types](https://cloud.google.com/spanner/docs/reference/standard-sql/data-types) and [PostgreSQL data types](https://cloud.google.com/spanner/docs/reference/postgresql/data-types) define the native column and key constraints that this mapping enforces.

| Flink SQL type | Spanner type |
|---|---|
| `BOOLEAN` | `BOOL` |
| `BIGINT` | `INT64` |
| `FLOAT` / `DOUBLE` | `FLOAT32` / `FLOAT64` |
| `DECIMAL(38, 9)` | GoogleSQL `NUMERIC` |
| `DECIMAL(p, s)` | PostgreSQL `numeric` |
| `CHAR` / `VARCHAR` / `STRING` | `STRING` |
| marked `CHAR` / `VARCHAR` / `STRING` | `UUID` |
| `BINARY` / `VARBINARY` / `BYTES` | `BYTES` |
| `DATE` | `DATE` |
| `TIMESTAMP_LTZ(0..9)` | `TIMESTAMP` |
| `ARRAY<T>` | `ARRAY<T>` |

The mapping applies to one-dimensional array elements, and nullable Flink values become null Spanner values.
Nested arrays are rejected because neither Spanner dialect permits an array whose element is another array.
`ROW` is rejected because Spanner `STRUCT` cannot be stored in a table column.
Plain `TIMESTAMP` is rejected because it has no time-zone semantics; use `TIMESTAMP_LTZ` for an instant.
GoogleSQL accepts only `DECIMAL(38, 9)`, which exactly matches its fixed `NUMERIC` precision and scale.
PostgreSQL accepts every Flink-supported `DECIMAL(p, s)`, but the physical `numeric` column is wider than Flink's maximum precision of 38 and has no DDL-level precision or scale modifier.
On reads, a PostgreSQL numeric value must fit the declared Flink precision without reducing non-zero fractional digits; trailing fractional zeros may be removed without changing the value.
Precision overflow, scale loss, and PostgreSQL numeric `NaN` fail conversion instead of rounding or turning a non-null value into null.
The error names the physical column and declared Flink shape without including the stored value.
`FLOAT` cannot be a primary-key column in either dialect, and no PostgreSQL decimal can be a primary-key column because PostgreSQL `numeric` is not a key type.

## Scan behavior

The source is bounded and reads the table through Spanner `partitionRead` at one shared snapshot.
Top-level projection is pushed into the requested Spanner column list, so unused columns do not cross the network.
Nested projection is not advertised.
If the planner requests no physical column, the connector reads the first declared column as a carrier but emits zero-field rows in the requested shape.

The source translates `=`, `<`, `<=`, `>`, `>=`, and conjunctions over consecutive key columns into Spanner `KeySet` points and lexicographic ranges.
A complete primary-key equality is an exact point read.
A leading equality prefix, optionally followed by a range on the next primary-key column, is exact and is removed from Flink's residual filter.
Predicates on later key columns, `OR`, `IN`, `<>`, computed expressions, null literals, and ordered `FLOAT64` comparisons remain with Flink.
A literal the connector cannot convert to its key column's Spanner type, such as a non-integral value compared with an `INT64` column or text that is not a canonical UUID, also remains with Flink instead of failing the job during planning.

Set `scan.index` to read a bounded scan through a named [secondary index](https://cloud.google.com/spanner/docs/secondary-indexes).
The connector resolves the live index key order, sort direction, state, null filtering, and readable columns from the [GoogleSQL](https://cloud.google.com/spanner/docs/information-schema) or [PostgreSQL](https://cloud.google.com/spanner/docs/information-schema-pg) `INFORMATION_SCHEMA` at the batch transaction's exact snapshot.
It then uses matching predicates as a best-effort index-key prefilter and leaves every such predicate with Flink to preserve SQL semantics.
The selected index must be `READ_WRITE`, belong to the configured table and schema, cover every column the scan reads, and be safe for nullable key rows.
GoogleSQL `STORING` and PostgreSQL `INCLUDE` columns are readable through the index together with its key and the base-table primary key.
A null-filtered index is accepted only when the pushed filters prove every nullable index key is not null.
An unusable configured index fails the job during partition planning instead of falling back to the base table.
The connector resolves schema, table, and index metadata together at the scan snapshot.
A missing or invisible schema is reported separately from a missing or invisible table access path.

Partition count and size are service hints, not exact split controls.
The default timestamp bound is strong; set either a read timestamp or exact staleness, never both.
There are deliberately no column-range partition options because Spanner chooses partition boundaries from physical storage.

## Change Streams scan behavior

Set `scan.mode = 'change-stream'` to replace the bounded scan with an unbounded CDC source over one Spanner Change Stream.
The default remains `bounded`, so existing DDL keeps its snapshot behavior.
Change-stream mode maps the declared `schema` and `table` to the exact dialect-aware native table name and silently advances past records for other watched tables.
It does not support lookup joins, projection or filter pushdown, bounded-scan partition and timestamp options, or lookup options.
A change-stream table is source-only.
Its declared columns are the watched table's own, so an `INSERT INTO` naming one would otherwise write into the very table being watched; it is rejected when the statement is planned instead.
A table written to may still carry the bounded-scan and lookup options it is also read with, but a `scan.change-stream.*` option on it is rejected rather than ignored.

`scan.change-stream.changelog-mode` is required.
`full` accepts only records captured with `NEW_ROW_AND_OLD_VALUES` and emits `INSERT`, adjacent `UPDATE_BEFORE` and `UPDATE_AFTER`, and full `DELETE` rows.
For an update, the complete new row is copied and each reported old value replaces its new value to reconstruct the before row.
`upsert` accepts `NEW_ROW` and `NEW_ROW_AND_OLD_VALUES`, requires a declared primary key, and emits `INSERT`, `UPDATE_AFTER`, and key-only `DELETE` rows.

```sql
CREATE TABLE order_changes (
  order_id BIGINT,
  customer STRING,
  status STRING,
  commit_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'commit-timestamp' VIRTUAL,
  record_sequence STRING METADATA FROM 'sequence' VIRTUAL,
  server_transaction_id STRING METADATA FROM 'server-transaction-id' VIRTUAL,
  mod_number INT METADATA FROM 'mod-number' VIRTUAL,
  WATERMARK FOR commit_timestamp AS SOURCE_WATERMARK(),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'database' = 'orders-db',
  'table' = 'orders',
  'scan.mode' = 'change-stream',
  'scan.change-stream.name' = 'order_changes',
  'scan.change-stream.changelog-mode' = 'upsert',
  'scan.startup.mode' = 'latest'
);
```

The source exposes the stable scalar identity, transaction, and record fields listed in the [Spanner metadata reference]({{< relref "docs/reference/spanner" >}}#table-change-stream-readable-metadata).
The vocabulary follows Debezium's Spanner source metadata where it represents the same Spanner field, with hyphenated keys matching this connector's SQL conventions.
The native `commit-timestamp` metadata type is non-null `TIMESTAMP_LTZ(9)`, preserving Spanner's nanosecond precision.
Flink permits watermark columns only through precision 3, so the example declares that metadata column as `TIMESTAMP_LTZ(3)` and lets the planner apply the compatible precision cast.
Omit the watermark declaration and use `TIMESTAMP_LTZ(9)` when downstream SQL must retain the full commit timestamp.
`SOURCE_WATERMARK()` uses the Change Streams source's existing commit-timestamped records and coordinator-owned unfinished-ledger heartbeat frontier; it does not introduce a second clock or a second out-of-orderness policy.

`mod-number` is the zero-based position of the mod in its original Spanner data-change record.
The adjacent before and after rows produced for one full-mode update carry the same `mod-number`, so metadata never splits the logical identity of that mod.

Each data-change record is validated against the DDL's physical names, native Spanner types, and, in upsert mode, primary-key column membership before any row from that record is emitted.
Extra watched columns are ignored, while a missing declared column, a type mismatch, an incompatible value-capture mode, or an absent required row value fails deserialization.
Explicit JSON null becomes SQL null, but an absent JSON member is never substituted with null when a complete row is required.
A conversion failure identifies the table, commit timestamp, transaction, record sequence, and mod index without including row JSON or credential paths.

Startup, expired-restore fallback, retention fallback, heartbeat interval, RPC priority, per-subtask query concurrency, emulator endpoint, credential path, and source parallelism map to the DataStream Change Streams builder.
The checkpoint, retention, delivery, and capacity contracts therefore remain those of the [DataStream Change Streams source]({{< relref "docs/connectors/datastream/spanner" >}}#change-streams-source).

## Lookup behavior

The source supports temporal lookup joins when the equality key contains every column of the declared `PRIMARY KEY`.
Composite keys are encoded in the DDL declaration order even when the planner supplies the predicates in another order.
A null key or an absent Spanner row produces no joined row.
Exact pushed primary-key predicates also gate synchronous and asynchronous lookups before an RPC, while Flink evaluates any residual predicate normally.
`scan.index` does not change lookup keys or lookup access paths.

`lookup.async` chooses Spanner's synchronous `readRow` or asynchronous `readRowAsync` API.
Flink's standard `lookup.cache = NONE` and `PARTIAL` modes are supported; `FULL` is rejected because it would require a scan-backed cache with different snapshot and refresh semantics.
The standard partial-cache expiry, size, and missing-key options apply unchanged.
`lookup.max-retries` retries only `ABORTED`, `DEADLINE_EXCEEDED`, and `UNAVAILABLE` point-read failures and counts retries after the initial request.
Of those, only `UNAVAILABLE` is also retried by the Spanner client, so this option is what buys a second attempt at the other two.
`RESOURCE_EXHAUSTED` is not retried here because the client already retries it whenever the server asks for a delay, and waits that delay; re-issuing the read at once would spend the budget against the wait rather than observe it.

UUID, JSON, protocol buffers, and enums share carrier types with ordinary columns, so the DDL marks them explicitly:

```sql
WITH (
  'schema.uuid-field-paths' = 'id;related_ids',
  'schema.json-field-paths' = 'metadata;payloads',
  'schema.proto-type-names' = 'event:example.events.Event',
  'schema.enum-type-names' = 'status:example.events.Status'
)
```

UUID and JSON fields use `STRING`, PROTO fields use `BYTES`, and ENUM fields use `BIGINT` in the Flink schema.
The UUID marker maps `STRING` and `ARRAY<STRING>` carriers to native UUID columns in both dialects, including primary-key columns and composite lookup keys.
UUID input must use the complete 36-character `8-4-4-4-12` hexadecimal form, with either letter case; Java's shortened accepted forms are rejected.
Reads always emit lowercase canonical UUID strings, and nullable UUID scalars, arrays, and array elements remain null.
Malformed sink or lookup input fails conversion with the physical column name but without including the input value.
Set `dialect = 'POSTGRESQL'` for PostgreSQL-dialect databases; JSON markers then produce `jsonb` values.
PROTO and ENUM markers require `GOOGLE_STANDARD_SQL` and are rejected for PostgreSQL databases.
A marker names one top-level physical field or an entire array field.
Every marker must resolve to exactly one physical field and no field may have more than one marker.

The connector does not inspect or migrate the live Spanner schema.
Changing an existing column from `STRING` to `UUID` therefore requires coordinating the Spanner DDL and this option before redeploying the Flink job, and every stored string must already be valid canonical UUID input before migration.

## Options

| Option | Default | What it does |
|---|---|---|
| `project` | **required** | The Google Cloud project containing the instance |
| `instance` | **required** | The Spanner instance containing the database |
| `database` | **required** | The database containing the table |
| `schema` | *unset ⇒ empty GoogleSQL schema or PostgreSQL `public`* | Named schema containing the table; one canonical quoted or unquoted identifier component |
| `table` | **required** | Table receiving or supplying rows; one canonical quoted or unquoted identifier component when `schema` is set |
| `emulator-endpoint` | *unset ⇒ the real service* | `host:port` of a Spanner emulator; setting it also stops credential discovery. Parsed when a statement over the table is planned — not at `CREATE TABLE`, which registers the options without calling the connector — so a malformed value fails on the client for every direction, whether the table is written to, scanned, watched as a Change Stream, or joined as a lookup dimension. The rejection names `emulator-endpoint`, the key written in the DDL |
| `service-account-key-file` | *unset ⇒ ADC for the real service* | Service-account JSON key-file path shared by the sink, bounded scan, Change Streams scan, and lookup paths; rejected with `emulator-endpoint` |
| `schema.json-field-paths` | empty | Semicolon-separated physical field paths whose `STRING` carriers map to Spanner JSON |
| `schema.uuid-field-paths` | empty | Semicolon-separated physical field paths whose `STRING` carriers map to native Spanner UUID |
| `dialect` | `GOOGLE_STANDARD_SQL` | Database dialect; use `POSTGRESQL` for PostgreSQL `jsonb` values |
| `schema.proto-type-names` | empty | Comma-separated `field-path:fully.qualified.Type` entries whose `BYTES` carriers map to Spanner PROTO |
| `schema.enum-type-names` | empty | Comma-separated `field-path:fully.qualified.Type` entries whose `BIGINT` carriers map to Spanner ENUM |
| `scan.mode` | `bounded` | `bounded` reads one snapshot; `change-stream` emits an unbounded CDC changelog and makes the table source-only, so writing to it is rejected |
| `scan.change-stream.name` | **required in change-stream mode** | Change Stream whose generated read function supplies records |
| `scan.change-stream.changelog-mode` | **required in change-stream mode** | `full` emits retract rows from `NEW_ROW_AND_OLD_VALUES`; `upsert` emits keyed upserts from `NEW_ROW` or `NEW_ROW_AND_OLD_VALUES` |
| `scan.startup.mode` | `latest` | Fresh Change Streams start: `earliest`, `latest`, or `timestamp` |
| `scan.startup.timestamp-millis` | *unset* | Unix epoch milliseconds required only when startup mode is `timestamp` |
| `scan.resume-fallback.mode` | *unset ⇒ fail an expired restore* | Position used after discarding an expired restored ledger: `earliest`, `latest`, or `timestamp` |
| `scan.resume-fallback.timestamp-millis` | *unset* | Unix epoch milliseconds required only when resume-fallback mode is `timestamp` |
| `scan.change-stream.absent-retention-fallback` | `7 d` | Retention assumed when the Change Stream has no explicit retention row |
| `scan.change-stream.heartbeat-interval` | `2 s` | Change Streams service heartbeat interval, from one second through five minutes |
| `scan.max-concurrent-queries-per-subtask` | `8` | Maximum Change Streams partition queries opened concurrently by one source subtask |
| `scan.index` | *unset ⇒ primary-key table read* | Secondary index used only by bounded scans; one canonical quoted or unquoted component in the configured schema, validated from live metadata when the job plans partitions |
| `scan.partition.max-partitions` | *unset* | Desired maximum partition count passed to Spanner as a hint |
| `scan.partition.size-bytes` | *unset* | Desired partition size passed to Spanner as a hint |
| `scan.data-boost-enabled` | `false` | Whether scans use Data Boost compute |
| `scan.rpc-priority` | *unset ⇒ bounded Spanner default; Change Streams `HIGH`* | Priority of scan or Change Streams partition-query RPCs |
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
| `sink.recovery.initial-backoff` | `500 ms` | Maps to `recoveryInitialBackoff` |
| `sink.recovery.max-backoff` | `10 s` | Maps to `recoveryMaxBackoff` |
| `sink.recovery.max-attempts` | `10` | Maps to `recoveryMaxAttempts` |
| `sink.parallelism` | *unset ⇒ operator parallelism* | Flink's standard sink parallelism override |

The sink options map directly onto the DataStream writer options; their validation limits are listed in the [Spanner reference]({{< relref "docs/reference/spanner" >}}#spannerwriteroptions).
