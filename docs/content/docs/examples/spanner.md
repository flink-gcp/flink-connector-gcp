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

# Spanner examples

Worked cases beyond the [quickstart]({{< relref "docs/quickstart/spanner" >}}). What each option
means is on the [Spanner options]({{< relref "docs/reference/spanner" >}}) page; why the sink
behaves as it does is on the
[Spanner connector]({{< relref "docs/connectors/datastream/spanner" >}}) page.

## Writing to several tables from one sink

The mutation names its table, so routing by record needs no option — just a serializer that decides.

```java
SpannerSink.<Event>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "events-db"))
        .serializer(
                (event, context) ->
                        Mutation.newInsertOrUpdateBuilder(
                                        event.isAudit() ? "AuditEvents" : "Events")
                                .set("EventId").to(event.getId())
                                .set("Body").to(event.getBody())
                                .build())
        .build();
```

The cell weights the sink reads at start-up cover every table of the database, so both tables are
weighed correctly against the batch limit.

## Deleting rows

A delete is a mutation like any other, so a stream of keys is a stream of deletes.

```java
SpannerSink.<String>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
        .serializer((orderId, context) -> Mutation.delete("Orders", Key.of(orderId)))
        .build();
```

Deletes are idempotent, so a replayed record is harmless. A delete over a key *range* is also
possible — note that the sink counts it as a single row against `maxBatchCells`, because nothing on
the client side can know how many rows a range matches.

## Skipping records

Returning `null` skips the record: it is written nowhere, is not a failure, and is counted by
`recordsSkipped`.

```java
SpannerSink.<Event>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "events-db"))
        .serializer(
                (event, context) ->
                        event.isHeartbeat()
                                ? null
                                : Mutation.newInsertOrUpdateBuilder("Events")
                                        .set("EventId").to(event.getId())
                                        .build())
        .build();
```

## Dropping refused mutations instead of failing the job

By default the first refused mutation fails the job. `failedMutationHandler` changes that.

```java
SpannerSink.<Event>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "events-db"))
        .serializer(...)
        .failedMutationHandler(FailureHandler.logAndDrop())
        .build();
```

**Read what actually reaches this handler before relying on it.** By default only `ALREADY_EXISTS`
and `INVALID_ARGUMENT` do — a `NOT NULL` violation, an over-long value, a foreign key or a `CHECK`
constraint all fail the job instead. That default is deliberate, and it is also an option:

```java
.constraintViolationPolicy(ConstraintViolationPolicy.ROUTE_TO_FAILURE_HANDLER)
```

which sends constraint violations to the same handler, so the handler above then decides. Choose it
when your stream genuinely carries occasional records the schema will not accept; leave it alone
when a refusal would mean your column mapping is wrong. The
[table on the connector page]({{< relref "docs/connectors/datastream/spanner" >}}#what-does-not-reach-the-failure-handler-and-why)
lists every case, and says why the default is what it is.

A handler taking the connector's own type sees the mutation itself:

```java
.failedMutationHandler(
        (FailureHandler<FailedMutation>)
                failure -> {
                    Mutation refused = failure.getMutation();
                    LOG.warn("Dropping a mutation on {}: {}",
                            failure.getTable(), failure.getErrorMessage());
                })
```

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

```java
SpannerSink.<Event>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "events-db"))
        .serializer(...)
        .writerOptions(
                SpannerWriterOptions.builder()
                        .maxBatchMutations(100)
                        // A commit delay trades latency for throughput by letting Spanner group
                        // this commit with others. Zero to 500 ms.
                        .maxCommitDelay(Duration.ofMillis(50))
                        // A backfill that must not disturb serving traffic on the same instance.
                        .rpcPriority(SpannerRpcPriority.LOW)
                        .build())
        .build();
```

`maxBatchCells` counts index entries, not columns: a row of five columns in a table with three
indexes covering them costs far more than five. Watch `bufferedCells` beside `bufferedBytes` to see
which limit is the one actually firing.

## Reading a key range instead of a query

A table read takes a key set and a column list, and is the cheapest shape when the rows wanted are
a contiguous range of the primary key. There is no SQL to be root-partitionable, so nothing about
the read can be refused for being undistributable.

```java
SpannerSource.<Order>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
        .readOperation(
                SpannerReadOperation.read(
                        "Orders",
                        KeySet.range(KeyRange.closedOpen(Key.of(1_000L), Key.of(2_000L))),
                        Arrays.asList("OrderId", "Total")))
        .deserializer(new OrderDeserializer())
        .build();
```

`SpannerReadOperation.readUsingIndex("Orders", "OrdersByCustomer", keys, columns)` reads the same
way through a secondary index, with the key set interpreted in the *index's* key space.

## Reading a large table without disturbing serving traffic

Data Boost serves the read from compute that is not the instance's. The caller needs
`spanner.databases.useDataBoost` on the database, the read is billed separately, and its
concurrency has a quota of its own.

```java
SpannerSource.<Order>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
        .readOperation(
                SpannerReadOperation.query(
                        Statement.of("SELECT OrderId, Total FROM Orders WHERE Total > 100")))
        .deserializer(new OrderDeserializer())
        .dataBoostEnabled(true)
        // Both are hints. Asking for one partition per subtask is reasonable; getting a different
        // number is normal, and the enumerator warns when the plan is smaller than the parallelism.
        .maxPartitions(env.getParallelism())
        .build();
```

## Reading at a fixed timestamp

Reading two tables at the same timestamp makes their contents consistent with each other, which a
job joining them usually wants.

```java
Timestamp readAt = Timestamp.parseTimestamp("2026-08-10T00:00:00Z");

SpannerSource.<Order>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
        .readOperation(SpannerReadOperation.query(Statement.of("SELECT OrderId FROM Orders")))
        .deserializer(new OrderDeserializer())
        .timestampBound(TimestampBound.ofReadTimestamp(readAt))
        .build();
```

The timestamp has to lie inside the database's `version_retention_period` — an hour by default, up
to a week — or the data behind it is gone. `TimestampBound.ofExactStaleness(...)` is the other
accepted form; `ofMaxStaleness` and `ofMinReadTimestamp` are rejected, because Spanner allows them
only on a single-use transaction.

## Skipping rows on the way in

Emitting nothing from a source deserializer skips the row.
`recordsSkipped` counts each such input row once.

```java
new SpannerStructDeserializationSchema<Order>() {
    @Override
    public void deserialize(Struct row, Collector<Order> out) {
        // Rows the query could not exclude, filtered before they cost anything downstream.
        if (!row.isNull("Total")) {
            out.collect(new Order(row.getLong("OrderId"), row.getBigDecimal("Total")));
        }
    }

    @Override
    public TypeInformation<Order> getProducedType() {
        return TypeInformation.of(Order.class);
    }
};
```

A row you could not read is a different thing: throw, and the job fails rather than losing it.
Collector calls must be synchronous, must emit non-null records, and must not continue after
`deserialize` returns.

## Grouping SQL changes by transaction and mod

Readable metadata keeps Spanner's transaction and record identity beside the relational changelog row.
The names match Debezium's Spanner source vocabulary where it exposes the same Spanner fields, which makes an existing Debezium pipeline's grouping model reusable.

```sql
CREATE TABLE order_changes (
  order_id BIGINT,
  status STRING,
  commit_timestamp TIMESTAMP_LTZ(9) METADATA FROM 'commit-timestamp' VIRTUAL,
  record_sequence STRING METADATA FROM 'sequence' VIRTUAL,
  server_transaction_id STRING METADATA FROM 'server-transaction-id' VIRTUAL,
  mod_number INT METADATA FROM 'mod-number' VIRTUAL,
  records_in_transaction BIGINT
    METADATA FROM 'number-of-records-in-transaction' VIRTUAL,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'database' = 'orders-db',
  'table' = 'orders',
  'scan.mode' = 'change-stream',
  'scan.change-stream.name' = 'order_changes',
  'scan.change-stream.changelog-mode' = 'upsert'
);

SELECT server_transaction_id, record_sequence, mod_number,
       records_in_transaction, order_id, status
FROM order_changes;
```

`mod_number` starts at zero for each original data-change record.
In `full` mode, the before and after rows for one update deliberately carry the same value.
Use `TIMESTAMP_LTZ(3)` plus `WATERMARK FOR commit_timestamp AS SOURCE_WATERMARK()` when event-time operations matter more than retaining nanoseconds.

## Filtering Change Streams records

Table and column filters run after the connector decodes Spanner's record and before the user deserializer runs.
Each Java regular expression matches a complete table name or `table.column` identifier.

```java
SpannerChangeStreamSource<OrderChange> source =
        SpannerChangeStreamSource.<OrderChange>builder()
                .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
                .changeStreamName("all_changes")
                .deserializer(new OrderChangeDeserializer())
                .startPosition(StartPosition.latest())
                .tableIncludeList(List.of("orders", "order_items"))
                .columnExcludeList(
                        List.of("orders\\.internal_note", "order_items\\.debug_payload"))
                .skipMessagesWithoutChange(false)
                .build();
```

Primary keys remain in `keys` and `columnTypes` even when a column expression matches them.
The default above delivers a record whose projected old and new values are empty.
Set `skipMessagesWithoutChange(true)` only when downstream processing does not need that transaction activity.

These filters do not change the Change Stream query or prevent excluded values from entering the source process.
Restrict the Change Stream's DDL watch definition when exclusion must happen inside Spanner.

## Running against the emulator

```sh
docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator:1.5.56
```

```java
SpannerSink.<Event>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "events-db"))
        .serializer(...)
        .emulatorEndpoint("localhost:9010")
        .build();
```

The source takes the same option:

```java
SpannerSource.<Order>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
        .readOperation(SpannerReadOperation.query(Statement.of("SELECT OrderId FROM Orders")))
        .deserializer(new OrderDeserializer())
        .emulatorEndpoint("localhost:9010")
        .build();
```

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
