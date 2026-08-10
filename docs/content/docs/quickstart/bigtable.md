---
title: Bigtable
type: docs
weight: 40
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

# Write to and read from a Bigtable table

Assumes the artifacts and credentials from the
[Quickstart]({{< relref "docs/quickstart" >}}) index, and the imports an IDE resolves from the
[Java API reference]({{< param ApiDocsURL >}}).

## Write a stream of row mutations

**Create the table and its column family first.** By default the sink creates neither: a table's
schema is its column families and their garbage-collection policies, which is the part a sink
cannot guess. (Declaring that schema on the builder instead is
[table auto-creation]({{< relref "docs/connectors/datastream/bigtable" >}}#table-auto-creation);
the instance always has to exist.)

```sh
gcloud bigtable instances create my-instance \
    --display-name="my-instance" --cluster-config=id=my-cluster,zone=asia-northeast1-a,nodes=1
gcloud bigtable instances tables create orders --instance=my-instance \
    --column-families=cf
```

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
// Not optional: the sink is at-least-once only with checkpointing, which is what makes Flink wait
// for every outstanding mutation before the barrier passes.
env.enableCheckpointing(60_000);

env.fromData("a-1", "a-2")
        .sinkTo(
                BigtableSink.<String>builder()
                        .table(TableDestination.of("my-project", "my-instance", "orders"))
                        .serializer(
                                (element, context) ->
                                        RowMutationEntry.create("order#" + element)
                                                // An explicit cell timestamp, so a replayed record
                                                // overwrites this cell instead of adding a version.
                                                .setCell(
                                                        "cf",
                                                        "payload",
                                                        context.timestamp() == null
                                                                ? 0L
                                                                : context.timestamp() * 1_000,
                                                        element))
                        .build());

env.execute("bigtable-quickstart");
```

Read the rows back with the `cbt` CLI:

```sh
cbt -project my-project -instance my-instance read orders
```

Two things decided in that job rather than by the sink. The **row key** is the whole access pattern
in Bigtable — reads are by key or by key range — so `order#<id>` is a choice about how the data will
be read, not a formality. And the **cell timestamp** is what makes the replay of a record after a
failure an overwrite rather than a second version of the cell; leaving it out lets the server's
clock decide, and both versions then live until garbage collection removes one.

## Read a table back

The source reads the rows of a key range and finishes. It is bounded, which is not the same as
batch-only: this job runs in streaming mode and simply ends, which is also what lets a Bigtable
table be read and joined against an unbounded stream.

```java
public class ReadOrders {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Source<String, ?, ?> source =
                BigtableSource.<String>builder()
                        .table(TableDestination.of("my-project", "my-instance", "orders"))
                        // Zero or more records per row: this one emits the payload of each cell.
                        .deserializer(
                                new BigtableRowDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(Row row, Collector<String> out) {
                                        for (RowCell cell : row.getCells("cf", "payload")) {
                                            out.collect(
                                                    row.getKey().toStringUtf8()
                                                            + " = "
                                                            + cell.getValue().toStringUtf8());
                                        }
                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return TypeInformation.of(String.class);
                                    }
                                })
                        // Only the rows this job needs: a prefix is sugar for the range it
                        // describes, and what a filter excludes never leaves the server.
                        .prefix("order#")
                        .filter(Filters.FILTERS.family().exactMatch("cf"))
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "orders").print();
        env.execute("read-orders");
    }
}
```

The job needs `bigtable.tables.readRows` and `bigtable.tables.sampleRowKeys` — `roles/bigtable.reader`
covers both — and creates nothing.

**How many subtasks read is Bigtable's decision, not the job's.** Splits come from where the service
says the table's sections begin, so a small table is read by one subtask however high the parallelism
is set; the others finish immediately.

## The same thing in SQL

The `bigtable` table connector writes the same rows from a `CREATE TABLE`. The schema is the HBase
convention: one atomic column is the row key, and every `ROW<...>` column is a column family whose
fields are its qualifiers.

```sql
CREATE TABLE orders (
  rowkey STRING,
  cf ROW<order_id STRING, amount BIGINT>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project'   = 'my-project',
  'instance'  = 'my-instance',
  'table'     = 'orders'
);

INSERT INTO orders VALUES ('order#a-1', ROW('a-1', CAST(10 AS BIGINT)));
```

The sink is upsert-shaped, so an updating query works as it stands and a delete removes the whole
row. There is no uber-jar for the SQL client yet ([#461]({{< param BookRepo >}}/issues/461)), so a
SQL deployment needs `flink-connector-gcp-bigtable` and its runtime tree on the classpath. The full
`WITH` surface, the cell encodings and the type mapping are on the
[Bigtable SQL connector]({{< relref "docs/connectors/table/bigtable" >}}) page. Reading a table from
SQL is [#459]({{< param BookRepo >}}/issues/459).

## Next

[Bigtable examples]({{< relref "docs/examples/bigtable" >}}) — several mutations per record,
deletes, a table named per record, dropping bad rows instead of failing, reading a key range, and
running against the emulator.
