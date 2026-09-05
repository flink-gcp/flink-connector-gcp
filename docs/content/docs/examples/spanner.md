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

# Spanner examples

Worked cases beyond the [quickstart]({{< relref "docs/quickstart/spanner" >}}) follow the shared source-to-sink order.
The [Spanner options]({{< relref "docs/reference/spanner" >}}) page lists every option, while the
[Spanner connector]({{< relref "docs/connectors/datastream/spanner" >}}) page explains the runtime
contracts behind them.

## DataStream source

The quickstart owns the basic [bounded DataStream read]({{< relref "docs/quickstart/spanner" >}}#read-the-table-back-into-flink).
The cases below change its read shape, snapshot, compute placement, or row handling.

### Reading a key range instead of a query

A table read takes a key set and a column list, and is the cheapest shape when the rows wanted are
a contiguous range of the primary key. There is no SQL to be root-partitionable, so nothing about
the read can be refused for being undistributable.

{{< java-snippet file="SpannerExamplesKeyRange.java" tag="spanner-examples-key-range" >}}

`SpannerReadOperation.readUsingIndex("Orders", "OrdersByCustomer", keys, columns)` reads the same
way through a secondary index, with the key set interpreted in the *index's* key space.

### Reading a large table without disturbing serving traffic

Data Boost serves the read from compute that is not the instance's. The caller needs
`spanner.databases.useDataBoost` on the database, the read is billed separately, and its
concurrency has a quota of its own.

{{< java-snippet file="SpannerExamplesDataBoost.java" tag="spanner-examples-data-boost" >}}

### Reading at a fixed timestamp

Reading two tables at the same timestamp makes their contents consistent with each other, which a
job joining them usually wants.

{{< java-snippet file="SpannerExamplesFixedTimestamp.java" tag="spanner-examples-fixed-timestamp" >}}

The timestamp has to lie inside the database's `version_retention_period` — an hour by default, up
to a week — or the data behind it is gone. `TimestampBound.ofExactStaleness(...)` is the other
accepted form; `ofMaxStaleness` and `ofMinReadTimestamp` are rejected, because Spanner allows them
only on a single-use transaction.

### Skipping rows on the way in

Emitting nothing from a source deserializer skips the row.
`recordsSkipped` counts each such input row once.

{{< java-snippet file="SpannerExamplesSkippingRows.java" tag="spanner-examples-skipping-rows" >}}

A row you could not read is a different thing: throw, and the job fails rather than losing it.
Collector calls must be synchronous, must emit non-null records, and must not continue after
`deserialize` returns.

## DataStream sink

The quickstart owns the basic [DataStream write]({{< relref "docs/quickstart/spanner" >}}#write-a-stream-into-a-spanner-table).
The cases below change mutation routing, mutation shape, refusal handling, or batching.

### Writing to several tables from one sink

The [dynamic destinations guide]({{< relref "docs/examples/dynamic-destinations" >}}#spanner-tables) explains why Spanner takes its table from each mutation instead of using the resolver-based pattern.
The mutation names its table, so routing by record needs no option, only a serializer that decides.

{{< java-snippet file="SpannerExamplesSeveralTables.java" tag="spanner-examples-several-tables" >}}

At start-up, the sink reads index-aware cell weights for the database's visible tables, so both pre-existing tables in this example are weighed correctly against the batch limit.
A table created after the writer opens, or hidden from the writer's database role, is counted without its index entries.
Both tables must already exist because the sink does not create them.

### Deleting rows

A delete is a mutation like any other, so a stream of keys is a stream of deletes.

{{< java-snippet file="SpannerExamplesDeletingRows.java" tag="spanner-examples-deleting-rows" >}}

Deletes are idempotent, so replaying a record is harmless.
A delete over a key *range* is also possible, but the sink counts it as one row against `maxBatchCells` because the client cannot know how many rows the range matches.

### Skipping records

Returning `null` skips the record: it is written nowhere, is not a failure, and increments
`recordsSkipped`.

{{< java-snippet file="SpannerExamplesSkippingRecords.java" tag="spanner-examples-skipping-records" >}}

### Dropping refused mutations instead of failing the job

By default the first refused mutation fails the job.
`failedMutationHandler` changes that decision.

{{< java-snippet file="SpannerExamplesSinkOptions.java" tag="spanner-examples-dropping-refused-mutations" >}}

**Read what reaches this handler before relying on it.**
By default only `ALREADY_EXISTS` and `INVALID_ARGUMENT` do; a `NOT NULL` violation, an over-long value, a foreign key, or a `CHECK` constraint fails the job instead.
The constraint-violation policy can route those schema refusals to the same handler:

{{< java-snippet file="SpannerExamplesSinkOptions.java" tag="spanner-examples-constraint-violation-policy" >}}

Choose that policy when the stream genuinely carries occasional records the schema will not accept.
Keep the fail-job default when a refusal would mean the column mapping is wrong.
The [error table]({{< relref "docs/connectors/datastream/spanner" >}}#what-does-not-reach-the-failure-handler-and-why) lists every case and the reason for its route.

A handler using the connector's failure type can inspect the mutation, table, and error message.
This example logs the table and service-provided text without capturing a non-serializable logger:

{{< java-snippet file="SpannerExamplesSinkOptions.java" tag="spanner-examples-custom-failure-handler" >}}

Treat that text as potentially sensitive because the service may include rejected identifiers or values.
Omit or sanitize it when the log is not an approved destination for record data.

### Tuning the batch

The defaults are Apache Beam's and sit far below the request limits.
Lower them for latency, and raise them only after checking what `maxBatchCells` counts.
The [configuration reference]({{< relref "docs/reference/spanner" >}}#batch-limits) records each ceiling and its evidence.
The limits are combined, so a batch flushes when the first one binds and raising one limit alone often changes nothing.
Setting `maxBatchMutations` above `maxBatchCells` writes a warning where the job's `main` runs because the cell cap must bind first.
`batchWriteTimeout` separately bounds one complete write attempt so a stalled response stream cannot hold the task thread indefinitely.

{{< java-snippet file="SpannerExamplesSinkOptions.java" tag="spanner-examples-batch-options" >}}

`maxBatchCells` counts every written table cell plus the secondary-index entries that the write changes.
Read `bufferedCells` beside `bufferedBytes` to identify the limit that is firing.

## Table source

### Reading a bounded table with SQL

The default Table source reads one bounded snapshot through the same partitioned source as the DataStream API.
Create and seed the physical table in the quickstart's `orders-db` database first:

{{< sql-snippet file="spanner/SpannerGoogleSqlExamples.sql" tag="inventory-table-and-row" >}}

The Flink DDL defaults to `scan.mode = 'bounded'`, so no Change Stream options are needed:

{{< sql-snippet file="flink/SpannerExamples.sql" tag="bounded-table-source" >}}

Projection is pushed into the Spanner column list.
The non-key `quantity` predicate remains with Flink because bounded scan pushdown is limited to consecutive primary-key columns.

## Table sink

### Writing upserts with SQL

A declared primary key makes the Spanner sink use `insertOrUpdate` for inserts and updates.
This bounded aggregation emits one final row for each composite key.
Create its physical destination in `orders-db` first:

{{< sql-snippet file="spanner/SpannerGoogleSqlExamples.sql" tag="account-status-table" >}}

{{< sql-snippet file="flink/SpannerExamples.sql" tag="batch-upsert" >}}

Without the Flink `PRIMARY KEY` declaration, the connector accepts only insert-only input and uses Spanner `insert`.
The declaration is a planner contract; it does not create or verify the physical key.

## Lookup joins

### Joining a composite-key lookup table

A processing-time temporal join turns each facts row into a Spanner point read.
Create and seed the physical table in the quickstart's `orders-db` database first:

{{< sql-snippet file="spanner/SpannerGoogleSqlExamples.sql" tag="accounts-table-and-row" >}}

The one-row data generator derives `region-1` from account `1`, so it deterministically produces the seeded key.
Real event tables normally carry both key columns as physical fields.

{{< sql-snippet file="flink/SpannerExamples.sql" tag="lookup-join" >}}

The equality condition must cover every declared primary-key column.
The join predicates may appear in either order, but the connector encodes the lookup key in the `PRIMARY KEY (region, account)` declaration order.
That order must match the physical Spanner primary key, or the point read addresses a different key.

## Change Streams

### Comparing changelog modes and materializing changes

The Table source offers two changelog shapes over the same physical Change Stream.
Create the watched table, a stream that captures old and new values, and a separate materialization table first:

{{< sql-snippet file="spanner/SpannerGoogleSqlExamples.sql" tag="orders-change-stream-and-replica" >}}

| Mode | Required capture | Flink primary key | Row kinds | Delete row |
|---|---|---|---|---|
| `full` | `NEW_ROW_AND_OLD_VALUES` | Optional | `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE` | Complete old row |
| `upsert` | `NEW_ROW` or `NEW_ROW_AND_OLD_VALUES` | Required and equal to the record key | `INSERT`, `UPDATE_AFTER`, `DELETE` | Key only |

Both modes expose the same readable commit, transaction, sequence, mod, table, and modification-type metadata.
`mod_number` starts at zero for each original data-change record, and the before and after rows of one full-mode update share it.
Both DDLs start at `latest`, so a fresh job waits for changes committed after it starts.

The `full` DDL needs no primary key because it reconstructs complete retract rows:

{{< sql-snippet file="flink/SpannerExamples.sql" tag="change-stream-full" >}}

The sink's advertised upsert changelog excludes `UPDATE_BEFORE`, and the planner does not send those rows to it.
Use `upsert` when the source should feed a keyed Spanner sink:

{{< sql-snippet file="flink/SpannerExamples.sql" tag="change-stream-materialization" >}}

After starting either job, wait until `changeStreamQueriesStarted` is non-zero for at least one
source reader subtask, then insert a watched row from another session:

{{< sql-snippet file="spanner/SpannerGoogleSqlExamples.sql" tag="order-change-after-source-starts" >}}

The Change Streams DDL and sink DDL remain separate because `scan.mode = 'change-stream'` makes a table source-only.
The source metadata columns are virtual, while the replica stores their selected values in ordinary physical columns for observability.
The query widens the Flink `INT` mod number to `BIGINT`, the lossless mapping for a physical Spanner `INT64` column.
A key-only delete still works because the sink constructs a delete mutation from the primary key and does not write non-key fields.

This is a replica-shaped materialization pattern, not a strict replica guarantee.
The sink is at-least-once, and `BatchWrite` does not guarantee the application order of successive writes to one key, so the destination is not guaranteed to retain the latest source value.

Use `TIMESTAMP_LTZ(3)` plus `WATERMARK FOR source_commit_timestamp AS SOURCE_WATERMARK()` when event-time operations matter more than retaining nanoseconds.

### Filtering Change Streams records

Table and column filters run after the connector decodes Spanner's record and before the user deserializer runs.
Each Java regular expression matches a complete table name or `table.column` identifier.

{{< java-snippet file="SpannerExamplesChangeStreamFilters.java" tag="spanner-examples-change-stream-filters" >}}

Primary keys remain in `keys` and `columnTypes` even when a column expression matches them.
The default above delivers a record whose projected old and new values are empty.
Set `skipMessagesWithoutChange(true)` only when downstream processing does not need that transaction activity.

These filters do not change the Change Stream query or prevent excluded values from entering the source process.
Restrict the Change Stream's DDL watch definition when exclusion must happen inside Spanner.

## Local development

### Running against the emulator

```sh
docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator:1.5.57
```

The emulator has separate resources from the real service.
Point a dedicated `gcloud` configuration at its REST endpoint, then recreate the quickstart resources there:

```sh
gcloud config configurations create spanner-emulator --no-activate
gcloud config set auth/disable_credentials true --configuration=spanner-emulator
gcloud config set project my-project --configuration=spanner-emulator
gcloud config set api_endpoint_overrides/spanner http://localhost:9020/ \
    --configuration=spanner-emulator
gcloud spanner instances create my-instance --config=emulator-config \
    --description="my-instance" --nodes=1 --configuration=spanner-emulator
gcloud spanner databases create orders-db --instance=my-instance \
    --ddl='CREATE TABLE Orders (OrderId STRING(64) NOT NULL, Total INT64) PRIMARY KEY (OrderId)' \
    --configuration=spanner-emulator
```

{{< java-snippet file="SpannerExamplesSinkOptions.java" tag="spanner-examples-emulator-sink" >}}

The source takes the same option:

{{< java-snippet file="SpannerExamplesEmulatorSource.java" tag="spanner-examples-emulator-source" >}}

Setting the endpoint also stops the client looking for credentials. Pin an image at **v1.5.31 or
newer**: the emulator implements the `BatchWrite` RPC this sink writes with only from that release,
and an older one answers `UNIMPLEMENTED` to everything. The emulator has no IAM and serializes
concurrent transactions, so it is a convenience for fast feedback rather than evidence about the
service.

On the read path in particular, the emulator plans exactly two partitions whatever the data, ignores
both partition hints, and applies a partitionability check of its own that refuses query shapes the
real service accepts —
[the deviation table]({{< relref "docs/connectors/datastream/spanner" >}}#reading-against-the-emulator)
has the details.
