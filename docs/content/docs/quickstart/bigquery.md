---
title: BigQuery
type: docs
weight: 10
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

# Write to and read from BigQuery

Assumes the artifacts and credentials from the
[Quickstart]({{< relref "docs/quickstart" >}}) index. The whole file, since it is the one worth
copying verbatim; the other connectors' pages show only the job.

## Write a stream to a table

{{< java-snippet file="BigQueryQuickstartWrite.java" tag="bigquery-quickstart-write" >}}

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

{{< java-snippet file="BigQueryQuickstartRead.java" tag="bigquery-quickstart-read" >}}

`Schema` here is Avro's, not the REST client's: rows arrive as Avro, and the schema you pass is what
they are read *into* — naming the columns you want with their natural types is enough, since Avro's
schema resolution maps the table's schema onto it. Reading `GenericRecord`s needs `flink-avro` on
the job's classpath.

`selectedFields` and `rowRestriction` are applied by BigQuery before anything is sent, which matters
for more than speed: **a read through this API is charged for the bytes it scans**, unlike the free
`FILE_LOADS` write path, and a column you do not select is a column BigQuery does not scan.

## Next

Choose the direction and API you need next:

- DataStream [source examples]({{< relref "docs/examples/bigquery" >}}#datastream-source) and
  [sink examples]({{< relref "docs/examples/bigquery" >}}#datastream-sink)
- Table [source example]({{< relref "docs/examples/bigquery" >}}#table-source) and
  [sink example]({{< relref "docs/examples/bigquery" >}}#table-sink)
- [Change data capture]({{< relref "docs/examples/bigquery" >}}#change-data-capture) and
  [local development]({{< relref "docs/examples/bigquery" >}}#local-development)
- DataStream connector reference [source]({{< relref "docs/connectors/datastream/bigquery" >}}#source)
  and [sink]({{< relref "docs/connectors/datastream/bigquery" >}}#sink) sections
- Table connector reference [source]({{< relref "docs/connectors/table/bigquery" >}}#source) and
  [sink]({{< relref "docs/connectors/table/bigquery" >}}#sink) sections
