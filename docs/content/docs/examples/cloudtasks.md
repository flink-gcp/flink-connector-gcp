---
title: Cloud Tasks
type: docs
weight: 30
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

# Cloud Tasks examples

Starting from the [Cloud Tasks quickstart]({{< relref "docs/quickstart/cloudtasks" >}}) job.

## Table API request bodies

The `cloud-tasks` Table sink passes physical columns to the selected Flink serialization format
and projects writable request metadata out before encoding.
The metadata columns in these examples therefore set `X-Trace-Id` without appearing in the body.
This projection is a connector guarantee.

The connector treats bytes from a generic format as opaque and does not select their media type.
Each table therefore sets the `Content-Type` expected by its HTTP handler.
External HTTP `POST`, `PUT` and `PATCH` requests carry the encoded body, while App Engine `POST`
and `PUT` requests do; other methods do not serialize the row or carry a body.
Those method rules are connector guarantees, while the byte representation inside the body belongs
to the selected Flink format.

Use the matching format reference for the Flink version deployed with the job:

| Format | Flink 1.20 | Flink 2.2 | Flink 2.3 |
|---|---|---|---|
| JSON | [JSON format](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/formats/json/) | [JSON format](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/formats/json/) | [JSON format](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/json/) |
| CSV | [CSV format](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/formats/csv/) | [CSV format](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/formats/csv/) | [CSV format](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/csv/) |
| raw | [raw format](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/formats/raw/) | [raw format](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/formats/raw/) | [raw format](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/raw/) |
| Avro | [Avro format](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/formats/avro/) | [Avro format](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/formats/avro/) | [Avro format](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/avro/) |

The following examples name only the options that determine their bodies.

### Nested JSON

JSON derives its object shape from the physical table schema.
This table includes a nested row, an array of rows, a map, a null value and text that requires JSON
escaping.

```sql
CREATE TABLE json_tasks (
  order_id STRING,
  customer ROW<name STRING, city STRING>,
  items ARRAY<ROW<sku STRING, quantity INT>>,
  attributes MAP<STRING, STRING>,
  note STRING,
  request_headers MAP<STRING, STRING> METADATA FROM 'headers'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'orders',
  'http.url' = 'https://api.example.com/orders',
  'http.method' = 'POST',
  'http.headers.Content-Type' = 'application/json',
  'format' = 'json',
  'json.encode.ignore-null-fields' = 'false'
);

INSERT INTO json_tasks
VALUES (
  'o-42',
  CAST(ROW('Alice "A"', '東京') AS ROW<name STRING, city STRING>),
  ARRAY[
    CAST(ROW('book', 2) AS ROW<sku STRING, quantity INT>),
    CAST(ROW('pen', 1) AS ROW<sku STRING, quantity INT>)
  ],
  MAP['priority', 'high'],
  CAST(NULL AS STRING),
  MAP['X-Trace-Id', 'trace-42']
);
```

The HTTP handler receives these UTF-8 bytes:

```json
{"order_id":"o-42","customer":{"name":"Alice \"A\"","city":"東京"},"items":[{"sku":"book","quantity":2},{"sku":"pen","quantity":1}],"attributes":{"priority":"high"},"note":null}
```

The current supported Flink lines write row members in physical schema order.
JSON object order has no semantic meaning, and Flink does not promise that map iteration or object
member order remains byte-for-byte stable across future versions.
HTTP handlers should parse the object instead of comparing its member order.

### CSV quoting and nulls

CSV derives one output record from the physical row.
This table selects a pipe delimiter, the ordinary double-quote character and an explicit null
literal.

```sql
CREATE TABLE csv_tasks (
  order_id STRING,
  note STRING,
  missing_value STRING,
  request_headers MAP<STRING, STRING> METADATA FROM 'headers'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'orders',
  'http.url' = 'https://api.example.com/import',
  'http.method' = 'POST',
  'http.headers.Content-Type' = 'text/csv; charset=UTF-8',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'csv.quote-character' = '"',
  'csv.null-literal' = 'NULL'
);

INSERT INTO csv_tasks
SELECT '42',
       U&'line 1 | "quoted"\000Aline 2',
       CAST(NULL AS STRING),
       MAP['X-Trace-Id', 'trace-42'];
```

The body contains one line break inside the quoted second field and no line separator after
`NULL`:

```text
"42"|"line 1 | ""quoted""
line 2"|NULL
```

Flink owns the delimiter, quoting, escaping and null-literal behavior.
Its CSV schema supports scalar fields and one level of `ARRAY` or `ROW` whose members are simple
types; it rejects `MAP` and deeper nesting.
Flatten structured input or use JSON or Avro when a CSV consumer needs another convention.

### A pre-serialized raw body

The raw format accepts exactly one physical column, so that column can hold a complete body
prepared by SQL or an upstream function.
Writable metadata does not count toward the one-column boundary because the connector projects it
out first.

```sql
CREATE TABLE raw_tasks (
  body STRING,
  request_headers MAP<STRING, STRING> METADATA FROM 'headers'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'orders',
  'http.url' = 'https://api.example.com/text',
  'http.method' = 'POST',
  'http.headers.Content-Type' = 'text/plain; charset=UTF-16BE',
  'format' = 'raw',
  'raw.charset' = 'UTF-16BE'
);

INSERT INTO raw_tasks
VALUES ('東京', MAP['X-Trace-Id', 'trace-42']);
```

UTF-16BE encodes the body as four bytes:

```text
67 71 4E AC
```

For a `VARBINARY` physical column, raw forwards the supplied byte sequence unchanged and
`raw.charset` has no effect.
The string charset and numeric endianness options are upstream Flink behavior rather than
connector policy.

### Binary Avro

Avro derives its writer schema from the physical table schema and physical field order.
Declare the fields `NOT NULL` when the receiving schema must not contain nullable unions.

```sql
CREATE TABLE avro_tasks (
  order_id STRING NOT NULL,
  quantity INT NOT NULL,
  gift BOOLEAN NOT NULL,
  request_headers MAP<STRING, STRING> METADATA FROM 'headers'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'orders',
  'http.url' = 'https://api.example.com/avro-orders',
  'http.method' = 'POST',
  'http.headers.Content-Type' = 'application/octet-stream',
  'format' = 'avro',
  'avro.encoding' = 'binary'
);

INSERT INTO avro_tasks
VALUES ('o-7', 3, TRUE, MAP['X-Trace-Id', 'trace-7']);
```

Flink derives this writer schema from the three physical columns:

```json
{
  "type": "record",
  "name": "record",
  "namespace": "org.apache.flink.avro.generated",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "quantity", "type": "int"},
    {"name": "gift", "type": "boolean"}
  ]
}
```

The binary datum is `06 6F 2D 37 06 01` in hexadecimal, or `Bm8tNwYB` in base64.
Decoded with the derived schema, it contains `order_id = "o-7"`, `quantity = 3` and
`gift = true`.

The body is an Avro binary datum without an object-container header, embedded schema or magic
bytes.
The handler must use the same writer schema, and changing field order, nullability or types changes
the wire representation.
The Cloud Tasks SQL uber-jar does not bundle generic Flink formats; JSON, CSV and raw are available
in the Flink SQL distribution, while Avro requires the version-matched `flink-avro` format artifact
on the SQL Client and cluster classpaths.

## Sharding across queues

The [dynamic destinations guide]({{< relref "docs/examples/dynamic-destinations" >}}#cloud-tasks-queues) places this sharding pattern in the shared resolver contract.

```java
CloudTasksSink.<OrderEvent>builder()
        .destinationResolver(
                (element, context) ->
                        QueueDestination.of(
                                "my-project",
                                "asia-northeast1",
                                "webhooks-" + Math.floorMod(element.customerId().hashCode(), 4)))
        .serializer(
                CloudTasksSerializationSchema.httpTarget("https://api.example.com/v1/orders")
                        .withBody(new OrderEventSchema()))
        .build();
```

A single Cloud Tasks client serves every queue, and the sink creates no per-queue client, stream, publisher or batcher to cache or evict.
When `CloudTasksWriterOptions.builder().perDestinationMetrics(true).build()` is supplied through `writerOptions(...)`, each queue with a recorded send or failure registers counters that remain for the task lifetime because Flink cannot unregister metrics.
That optional metric registry state is separate from service-client state.

Sharding this way is how a pipeline exceeds the per-queue throughput ceiling.
The aggregate limits, and why they rarely matter for the workload this connector exists for, are on the [Cloud Tasks connector]({{< relref "docs/connectors/datastream/cloudtasks" >}}) page.
All the queues must exist; the sink creates none of them.

## Running against the emulator

Google publishes no Cloud Tasks emulator; the one the integration tests use is
[`aertje/cloud-tasks-emulator`](https://github.com/aertje/cloud-tasks-emulator) (MIT). Queues are
declared at startup, since neither the emulator nor the sink creates one on demand:

```sh
docker run --rm -p 8123:8123 --add-host=host.docker.internal:host-gateway \
    ghcr.io/aertje/cloud-tasks-emulator:1.2.0 \
    -host 0.0.0.0 -port 8123 \
    -queue projects/my-project/locations/asia-northeast1/queues/webhooks
```

```java
CloudTasksSink.<String>builder()
        .queue(QueueDestination.of("my-project", "asia-northeast1", "webhooks"))
        .serializer(
                // Not localhost: the emulator dispatches from inside the container, where that
                // would be the container itself. --add-host above is what makes this name resolve
                // to the host on Linux; Docker Desktop provides it already.
                CloudTasksSerializationSchema.httpTarget("http://host.docker.internal:9000/orders")
                        .withBody(new SimpleStringSchema()))
        .emulatorEndpoint("localhost:8123")
        .build();
```

Unlike the Pub/Sub emulator this one dispatches over **real HTTP**, so a server on your machine
sees exactly what the tasks carry — which is the whole reason it is worth running, and also why the
target URL has to be reachable from the container's network rather than yours. (The module's own
tests solve the same problem with testcontainers' `exposeHostPorts(...)`.)

What it cannot show, per the
[rule about emulators]({{< relref "docs/examples" >}}#an-emulator-is-a-convenience-not-an-authority):
task-name garbage collection, so the deduplication *window* is untestable and only the
`ALREADY_EXISTS` response is; queue-level `uriOverride` routing; the OAuth token path, since it
implements OIDC only; failure injection; and any size limit.
