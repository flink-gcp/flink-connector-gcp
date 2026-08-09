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

# Write to and read from BigQuery

Assumes the artifacts and credentials from the
[Quickstart]({{< relref "docs/quickstart" >}}) index. The whole file, since it is the one worth
copying verbatim; the other connectors' pages show only the job.

## Write a stream to a table

```java
package example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.json.JsonDocumentSerializer;

public class BigQueryQuickstart {

    public static void main(String[] args) throws Exception {
        // JSON carries no schema, so this one is supplied rather than derived. Serializers for
        // input that does carry a schema — protobuf messages and Avro records — derive it.
        Schema schema =
                Schema.of(
                        Field.of("order_id", StandardSQLTypeName.STRING),
                        Field.of("amount", StandardSQLTypeName.INT64));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // Not optional. Every sink here is at-least-once *only* with checkpointing: the checkpoint
        // is what makes Flink flush what the Google client libraries are still holding, so without
        // it those records are lost on failure.
        env.enableCheckpointing(60_000);

        env.fromData(
                        "{\"order_id\":\"a-1\",\"amount\":10}",
                        "{\"order_id\":\"a-2\",\"amount\":20}")
                .sinkTo(
                        BigQuerySink.<String>builder()
                                .destination(
                                        TableDestination.of("my-project", "my_dataset", "orders"))
                                .serializer(JsonDocumentSerializer.of(schema))
                                .build());

        env.execute("bigquery-quickstart");
    }
}
```

The dataset must exist; the table need not, because the default create disposition is
`CREATE_IF_NEEDED` and the schema above is what it is created from. The default write method is
`STORAGE_API_AT_LEAST_ONCE`, which makes rows queryable within seconds — the other two, and when to
reach for them, are under
[exactly-once]({{< relref "docs/examples/bigquery" >}}#exactly-once).

`Schema` here is the BigQuery REST client's type, so a schema read back from the destination table
with `BigQuery.getTable(...)` can be passed straight in. The traps in the JSON conversion — a bare
number in a `TIMESTAMP` column is epoch *microseconds*, a `JSON` column takes text rather than an
object, a `BYTES` column takes an array of byte values rather than base64 — are on the
[BigQuery connector]({{< relref "docs/connectors/datastream/bigquery" >}}) page, along with the
protobuf and Avro serializers for input that is not JSON.

## Read a table

The other direction is a bounded source over the Storage Read API. It finishes when the table has
been read, so it works both as a batch input and as the dimension side of a join in a streaming job.

```java
Schema readerSchema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"Person\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"long\"},"
                + "{\"name\":\"name\",\"type\":\"string\"}]}");

Source<GenericRecord, ?, ?> source =
        BigQuerySource.<GenericRecord>builder()
                .table(TableDestination.of("my-project", "my_dataset", "people"))
                .deserializer(BigQueryRowDeserializer.genericRecord(readerSchema))
                .selectedFields("id", "name")
                .rowRestriction("id > 1000")
                .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "BigQuery")
        .map(row -> row.get("name").toString())
        .print();
```

`Schema` here is Avro's, not the REST client's: rows arrive as Avro, and the schema you pass is what
they are read *into* — naming the columns you want with their natural types is enough, since Avro's
schema resolution maps the table's schema onto it. Reading `GenericRecord`s needs `flink-avro` on
the job's classpath.

`selectedFields` and `rowRestriction` are applied by BigQuery before anything is sent, which matters
for more than speed: **a read through this API is charged for the bytes it scans**, unlike the free
`FILE_LOADS` write path, and a column you do not select is a column BigQuery does not scan.

## Next

[BigQuery examples]({{< relref "docs/examples/bigquery" >}}) — a table per day from the event
timestamp, both exactly-once write methods, and what `tableCreateOptions(...)` decides at creation
time.

Reading is covered further under
[Source]({{< relref "docs/connectors/datastream/bigquery" >}}#source) — what a checkpoint carries,
how many read streams BigQuery actually gives you, and why no recovery test may be written against
the emulator.
