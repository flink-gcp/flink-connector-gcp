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

# Write a stream into a Spanner table

Assumes the artifacts and credentials from the
[Quickstart]({{< relref "docs/quickstart" >}}) index, and the imports an IDE resolves from the
[Java API reference]({{< param ApiDocsURL >}}).

**Create the instance, database and table first.** The sink creates none of them — a missing table
fails every record alike, so it is a configuration error rather than something a sink can repair.

```sh
gcloud spanner instances create my-instance \
    --config=regional-asia-northeast1 --description="my-instance" --nodes=1
gcloud spanner databases create orders-db --instance=my-instance \
    --ddl='CREATE TABLE Orders (OrderId STRING(64) NOT NULL, Total INT64) PRIMARY KEY (OrderId)'
```

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
// Not optional: the sink is at-least-once only with checkpointing, which is what makes Flink wait
// for the batch to be applied before the barrier passes.
env.enableCheckpointing(60_000);

env.fromData("a-1", "a-2")
        .sinkTo(
                SpannerSink.<String>builder()
                        .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
                        .serializer(
                                (element, context) ->
                                        // insertOrUpdate, not insert: the sink is at-least-once
                                        // and Spanner's batch write has no replay protection, so
                                        // a record can arrive twice. An upsert makes that a
                                        // no-op; an insert makes it a routed failure.
                                        Mutation.newInsertOrUpdateBuilder("Orders")
                                                .set("OrderId").to("order#" + element)
                                                .set("Total").to(element.length())
                                                .build())
                        .build());

env.execute("spanner-quickstart");
```

Read the rows back:

```sh
gcloud spanner databases execute-sql orders-db --instance=my-instance \
    --sql='SELECT OrderId, Total FROM Orders ORDER BY OrderId'
```

Two things decided in that job rather than by the sink. The **table** comes from the mutation, not
from the builder — one sink writes to as many tables of `orders-db` as the serializer names. And the
**mutation operation** is what decides whether a replay is harmless; the
[connector page]({{< relref "docs/connectors/datastream/spanner" >}}#delivery-guarantee-and-why-the-mutation-operation-is-your-decision)
has the table of which operations are idempotent.

## Read the table back into Flink

The source reads the database at one snapshot and finishes. Spanner decides how the read is divided;
the job supplies no split column and no bounds.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

Source<Long, ?, ?> source =
        SpannerSource.<Long>builder()
                .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
                .readOperation(
                        SpannerReadOperation.query(
                                Statement.of("SELECT OrderId FROM Orders")))
                .deserializer(
                        new SpannerStructDeserializationSchema<Long>() {
                            @Override
                            public void deserialize(Struct row, Collector<Long> out) {
                                out.collect(row.getLong("OrderId"));
                            }

                            @Override
                            public TypeInformation<Long> getProducedType() {
                                return TypeInformation.of(Long.class);
                            }
                        })
                .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "orders").print();
env.execute("spanner-read-quickstart");
```

The identity running the job needs `spanner.sessions.create`, `spanner.databases.select` and the
partitioned-read permissions `spanner.databases.partitionQuery` and `spanner.databases.partitionRead`
— `roles/spanner.databaseReader` carries all four. Data Boost needs one more, and a different role;
see [Serverless reads with Data Boost]({{< relref "docs/connectors/datastream/spanner" >}}#serverless-reads-with-data-boost).

Two things worth knowing before the first large read. The read holds a snapshot at one timestamp,
and the database's `version_retention_period` (an hour by default) is what bounds how long it stays
readable. And recovery re-reads whole partitions, so a job that fails part-way emits some rows twice
— see
[Splits, partitions and recovery]({{< relref "docs/connectors/datastream/spanner" >}}#splits-partitions-and-recovery).

## Next

[Spanner examples]({{< relref "docs/examples/spanner" >}}) — deletes, skipping records, dropping
refused mutations instead of failing, tuning the batch, reading a key range, and running against the
emulator.
