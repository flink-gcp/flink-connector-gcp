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

Worked cases beyond the [quickstart]({{< relref "docs/quickstart/spanner" >}}). What each option
means is on the [Spanner options]({{< relref "docs/reference/spanner" >}}) page; why the sink
behaves as it does is on the
[Spanner connector]({{< relref "docs/connectors/datastream/spanner" >}}) page.

## Joining a composite-key lookup table

A processing-time temporal join turns each facts row into a Spanner point read.
Create and seed the physical table in the quickstart's `orders-db` database first:

{{< sql-snippet file="spanner/SpannerGoogleSqlExamples.sql" tag="accounts-table-and-row" >}}

The one-row data generator below derives the demo region `region-1` from account `1`, so it
deterministically produces the seeded key. Real event tables normally carry both key columns as
physical fields.

{{< sql-snippet file="flink/SpannerExamples.sql" tag="lookup-join" >}}

The equality condition must cover every declared primary-key column.
The join predicates may appear in either order, but the connector encodes the lookup key in the `PRIMARY KEY (region, account)` declaration order.
That declaration order must match the physical Spanner table's primary-key order, or the point read addresses a different key.

## Writing upserts with SQL

A declared primary key makes the Spanner sink use `insertOrUpdate` for inserts and updates.
This bounded aggregation emits one final row for each composite key:

Create its physical destination in `orders-db` first:

{{< sql-snippet file="spanner/SpannerGoogleSqlExamples.sql" tag="account-status-table" >}}

{{< sql-snippet file="flink/SpannerExamples.sql" tag="batch-upsert" >}}

Without the Flink `PRIMARY KEY` declaration, the connector accepts only insert-only input and uses Spanner `insert` instead.
The declaration is a planner contract; it does not create or verify the physical key.

## Writing to several tables from one sink

The [dynamic destinations guide]({{< relref "docs/examples/dynamic-destinations" >}}#spanner-tables) explains why Spanner takes its table from each mutation instead of using the resolver-based pattern.
The mutation names its table, so routing by record needs no option — just a serializer that decides.

{{< java-snippet file="SpannerExamplesSeveralTables.java" tag="spanner-examples-several-tables" >}}

At start-up, the sink reads index-aware cell weights for the database's visible tables, so both pre-existing tables in this example are weighed correctly against the batch limit.
A table created after the writer opens, or hidden from the writer's database role, is counted without its index entries.
Both tables must already exist because the sink does not create them.

## Deleting rows

A delete is a mutation like any other, so a stream of keys is a stream of deletes.

{{< java-snippet file="SpannerExamplesDeletingRows.java" tag="spanner-examples-deleting-rows" >}}

Deletes are idempotent, so a replayed record is harmless. A delete over a key *range* is also
possible — note that the sink counts it as a single row against `maxBatchCells`, because nothing on
the client side can know how many rows a range matches.

## Skipping records

Returning `null` skips the record: it is written nowhere, is not a failure, and is counted by
`recordsSkipped`.

{{< java-snippet file="SpannerExamplesSkippingRecords.java" tag="spanner-examples-skipping-records" >}}

## Dropping refused mutations instead of failing the job

By default the first refused mutation fails the job. `failedMutationHandler` changes that.

{{< java-snippet file="SpannerExamplesSinkOptions.java" tag="spanner-examples-dropping-refused-mutations" >}}

**Read what actually reaches this handler before relying on it.** By default only `ALREADY_EXISTS`
and `INVALID_ARGUMENT` do — a `NOT NULL` violation, an over-long value, a foreign key or a `CHECK`
constraint all fail the job instead. That default is deliberate, and it is also an option:

{{< java-snippet file="SpannerExamplesSinkOptions.java" tag="spanner-examples-constraint-violation-policy" >}}

which sends constraint violations to the same handler, so the handler above then decides. Choose it
when your stream genuinely carries occasional records the schema will not accept; leave it alone
when a refusal would mean your column mapping is wrong. The
[table on the connector page]({{< relref "docs/connectors/datastream/spanner" >}}#what-does-not-reach-the-failure-handler-and-why)
lists every case, and says why the default is what it is.

A handler taking the connector's own type can inspect the mutation itself as well as its table and error message.
This example logs the table and service-provided error text without capturing a non-serializable logger in the handler:

{{< java-snippet file="SpannerExamplesSinkOptions.java" tag="spanner-examples-custom-failure-handler" >}}

Treat that error text as potentially sensitive because the service may include rejected identifiers or values in it; omit or sanitize it when the log is not an approved destination for record data.

## Tuning the batch

The defaults are Apache Beam's, and they sit far under every limit a batch write request has to stay
within. Lower them for latency; raise them only knowing what `maxBatchCells` counts. Each has a
ceiling, refused at submission rather than on a task manager — see the
[configuration reference]({{< relref "docs/reference/spanner" >}}#batch-limits) for what each
ceiling is and what it rests on. They are also ANDed — a batch flushes on whichever binds first — so
raising one alone often changes nothing; `maxBatchCells` and `maxBatchBytes` are the pair to reach
for. Setting `maxBatchMutations` above `maxBatchCells` writes a warning to the log of wherever the
job's `main` runs, because the cell cap is then reached first and the mutation cap can never take
effect.
`batchWriteTimeout` separately bounds a complete write attempt so that a stalled response stream
cannot hold the task thread indefinitely.

{{< java-snippet file="SpannerExamplesSinkOptions.java" tag="spanner-examples-batch-options" >}}

`maxBatchCells` counts index entries, not columns: a row of five columns in a table with three
indexes covering them costs far more than five. Watch `bufferedCells` beside `bufferedBytes` to see
which limit is the one actually firing.

## Reading a key range instead of a query

A table read takes a key set and a column list, and is the cheapest shape when the rows wanted are
a contiguous range of the primary key. There is no SQL to be root-partitionable, so nothing about
the read can be refused for being undistributable.

{{< java-snippet file="SpannerExamplesKeyRange.java" tag="spanner-examples-key-range" >}}

`SpannerReadOperation.readUsingIndex("Orders", "OrdersByCustomer", keys, columns)` reads the same
way through a secondary index, with the key set interpreted in the *index's* key space.

## Reading a large table without disturbing serving traffic

Data Boost serves the read from compute that is not the instance's. The caller needs
`spanner.databases.useDataBoost` on the database, the read is billed separately, and its
concurrency has a quota of its own.

{{< java-snippet file="SpannerExamplesDataBoost.java" tag="spanner-examples-data-boost" >}}

## Reading at a fixed timestamp

Reading two tables at the same timestamp makes their contents consistent with each other, which a
job joining them usually wants.

{{< java-snippet file="SpannerExamplesFixedTimestamp.java" tag="spanner-examples-fixed-timestamp" >}}

The timestamp has to lie inside the database's `version_retention_period` — an hour by default, up
to a week — or the data behind it is gone. `TimestampBound.ofExactStaleness(...)` is the other
accepted form; `ofMaxStaleness` and `ofMinReadTimestamp` are rejected, because Spanner allows them
only on a single-use transaction.

## Skipping rows on the way in

Emitting nothing from a source deserializer skips the row.
`recordsSkipped` counts each such input row once.

{{< java-snippet file="SpannerExamplesSkippingRows.java" tag="spanner-examples-skipping-rows" >}}

A row you could not read is a different thing: throw, and the job fails rather than losing it.
Collector calls must be synchronous, must emit non-null records, and must not continue after
`deserialize` returns.

## Grouping SQL changes by transaction and mod

Readable metadata keeps Spanner's transaction and record identity beside the relational changelog row.
The names match Debezium's Spanner source vocabulary where it exposes the same Spanner fields, which makes an existing Debezium pipeline's grouping model reusable.

{{< sql-snippet file="flink/SpannerExamples.sql" tag="change-stream-source" >}}

`mod_number` starts at zero for each original data-change record.
In `full` mode, the before and after rows for one update deliberately carry the same value.
Use `TIMESTAMP_LTZ(3)` plus `WATERMARK FOR commit_timestamp AS SOURCE_WATERMARK()` when event-time operations matter more than retaining nanoseconds.

## Filtering Change Streams records

Table and column filters run after the connector decodes Spanner's record and before the user deserializer runs.
Each Java regular expression matches a complete table name or `table.column` identifier.

{{< java-snippet file="SpannerExamplesChangeStreamFilters.java" tag="spanner-examples-change-stream-filters" >}}

Primary keys remain in `keys` and `columnTypes` even when a column expression matches them.
The default above delivers a record whose projected old and new values are empty.
Set `skipMessagesWithoutChange(true)` only when downstream processing does not need that transaction activity.

These filters do not change the Change Stream query or prevent excluded values from entering the source process.
Restrict the Change Stream's DDL watch definition when exclusion must happen inside Spanner.

## Running against the emulator

```sh
docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator:1.5.56
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
