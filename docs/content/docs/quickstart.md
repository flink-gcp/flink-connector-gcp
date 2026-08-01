---
title: Quickstart
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

# Quickstart

One complete job per connector, from an empty project to rows in BigQuery, messages on a Pub/Sub
topic, or tasks on a Cloud Tasks queue. The connector pages document *what each option does*; this
page is the shortest path to a job that runs.

Everything below writes to real Google Cloud. To run without touching a project, see
[emulator-backed local runs]({{< relref "docs/examples" >}}#running-against-an-emulator).

## Before you start

- **JDK 17 or 21.** Java 11 is not supported even though Flink 2.x declares it; the build targets
  bytecode 17.
- **Apache Flink 2.2 or 2.3**, or 1.20 (the 1.x LTS) — see
  [Supported versions]({{< relref "/" >}}#supported-versions) for how the range is verified. The
  jobs below run on the embedded MiniCluster that `StreamExecutionEnvironment` starts from an IDE,
  so no cluster is needed to try them.
- **A Google Cloud project** with the APIs of the services you use enabled. BigQuery tables and
  Pub/Sub topics are created for you by default; a Pub/Sub subscription and a Cloud Tasks queue
  must already exist. Each job below says which of those it is.

## Getting the connector onto the classpath

**Nothing is published yet.** Maven Central publishing arrives with
[#39]({{< param BookRepo >}}/issues/39), so until then the artifacts come from a local build.

```sh
git clone https://github.com/laughingman7743/flink-connector-gcp.git
cd flink-connector-gcp
./mvnw install -DskipTests
```

That installs `0.1.0-SNAPSHOT` into `~/.m2`, from where an ordinary dependency resolves:

```xml
<dependency>
  <groupId>io.github.flink-gcp</groupId>
  <artifactId>flink-connector-gcp-bigquery</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>

<!-- The connectors declare their Flink dependencies as `provided`, so a job brings its own.
     `compile` scope runs the jobs below from an IDE on the embedded MiniCluster; switch both to
     `provided` for a jar submitted to a cluster, which has them already. -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java</artifactId>
  <version>2.2.1</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients</artifactId>
  <version>2.2.1</version>
</dependency>
```

The other connector artifact ids are `flink-connector-gcp-pubsub` and
`flink-connector-gcp-cloudtasks`. All three are SNAPSHOTs of an unreleased project: the coordinates
and the API behind them change without notice, and this section is rewritten when there is
something published to point at. The Flink version above is the floor the connectors are compiled
against; any version in the supported range works, since one build covers the whole of it.

**Building for Flink 1.20** means selecting the compatibility source root along with the version,
which one command does both halves of:

```sh
./mvnw install -DskipTests -Dflink.version=1.20.4 -Dflink.compat=flink1
```

**For SQL**, use `flink-sql-connector-gcp-pubsub` instead — an uber-jar that bundles the connector
with its whole runtime tree, built to be dropped into Flink's `lib/` or added with `ADD JAR`. Why
that rather than the plain jar, and what it relocates, is on the
[Pub/Sub SQL connector]({{< relref "docs/connectors/table/pubsub" >}}) page.

## Credentials

Every connector authenticates with **application default credentials** and nothing else — there is
no credentials option on any builder. Locally:

```sh
gcloud auth application-default login
gcloud config set project my-project
```

On GKE, Dataproc or Compute Engine the workload's own service account is picked up with no
configuration. Service account key files work through `GOOGLE_APPLICATION_CREDENTIALS`, but
workload identity is the better answer wherever it is available.

Two environment facts a first run trips over:

- **`GOOGLE_CLOUD_PROJECT` (or a gcloud config the client library can see) must resolve a default
  project** for BigQuery's `FILE_LOADS` write method, whose clients are built with
  `getDefaultInstance()`. Without it the load-job committer fails with *"A project ID is required
  for this service"* — the project named in the destination is not consulted for this.
- **The BigQuery sink and the Pub/Sub sink need permission to *create* their destination**, since
  that is what they do by default. `createDisposition(CREATE_NEVER)` turns both into
  write-only jobs. The Pub/Sub source creates a subscription only when given creation settings,
  and the Cloud Tasks sink never creates a queue at all.

What each connector asks for:

| Connector | Permissions |
|---|---|
| BigQuery | `bigquery.tables.create` on the dataset under the default create disposition; `bigquery.tables.get` and `bigquery.tables.update` when schema updates or `FILE_LOADS` are enabled, plus BigQuery data-editor and job-user access, and Cloud Storage read/write on the staging bucket for `FILE_LOADS` |
| Pub/Sub sink | `pubsub.topics.publish`, plus `pubsub.topics.create` (roles/pubsub.editor) when topic auto-creation may trigger |
| Pub/Sub source | `pubsub.subscriptions.get` (roles/pubsub.viewer) on every configured subscription for the startup check, plus `create` when auto-creating and `update` when seeking — roles/pubsub.editor covers all three |
| Cloud Tasks | `cloudtasks.tasks.create` ([roles/cloudtasks.enqueuer](https://cloud.google.com/tasks/docs/secure-queue-configuration)), which binds to a single queue as well as to the project. The sink never creates a queue |

## Write a stream to BigQuery

The whole file, since it is the one worth copying verbatim; the jobs after this one show only the
body.

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
`STORAGE_API_AT_LEAST_ONCE`, which makes rows queryable within seconds — the other two, and when
to reach for them, are under [exactly-once]({{< relref "docs/examples" >}}#exactly-once).

`Schema` here is the BigQuery REST client's type, so a schema read back from the destination table
with `BigQuery.getTable(...)` can be passed straight in. The traps in the JSON conversion — a bare
number in a `TIMESTAMP` column is epoch *microseconds*, a `JSON` column takes text rather than an
object, a `BYTES` column takes an array of byte values rather than base64 — are on the
[BigQuery connector]({{< relref "docs/connectors/datastream/bigquery" >}}) page.

## Publish a stream to Pub/Sub

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
env.enableCheckpointing(60_000);

env.fromData("hello", "world")
        .sinkTo(
                PubSubSink.<String>builder()
                        .topic(TopicDestination.of("my-project", "orders"))
                        .serializer(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
                        .build());

env.execute("pubsub-sink-quickstart");
```

`dataOnly(...)` wraps any Flink `SerializationSchema` for payload-only messages. Attributes and an
ordering key layer onto it with `withAttributes(...)` and `withOrderingKey(...)`; a schema that
needs full control returns a `PubsubMessage` directly.

The topic is created if it does not exist. **An auto-created topic has no subscriptions**, so
messages published before one is attached reach nobody — which is what
[topic auto-creation]({{< relref "docs/examples" >}}#topic-and-subscription-auto-creation) is about.
Create the subscription first when trying this.

## Read a stream from Pub/Sub

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
// Also not optional here, and for a sharper reason: the source acknowledges on checkpoint
// completion, so without checkpointing nothing is ever acknowledged and it stalls once the client
// library's flow control fills. It fails the job itself after 10 minutes of that rather than
// hanging quietly.
env.enableCheckpointing(60_000);

Source<String, ?, ?> source =
        PubSubSource.<String>builder()
                .subscription(SubscriptionDestination.of("my-project", "orders-sub"))
                .deserializationSchema(
                        PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "pubsub").print();

env.execute("pubsub-source-quickstart");
```

The subscription must already exist: passing it without creation settings is the statement that it
does, and the enumerator checks before assigning a split. Pub/Sub's publish time becomes each
record's event timestamp, so a `WatermarkStrategy` over it is what to use instead of
`noWatermarks()` in an event-time job.

## Dispatch a stream as Cloud Tasks

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
env.enableCheckpointing(60_000);

env.fromData("{\"order_id\":\"a-1\"}")
        .sinkTo(
                CloudTasksSink.<String>builder()
                        .queue(QueueDestination.of("my-project", "asia-northeast1", "webhooks"))
                        .serializer(
                                CloudTasksSerializationSchema
                                        .httpTarget("https://api.example.com/v1/orders")
                                        .withBody(new SimpleStringSchema())
                                        .withHeaders(
                                                element ->
                                                        Map.of(
                                                                "Content-Type",
                                                                "application/json")))
                        .build());

env.execute("cloudtasks-quickstart");
```

**The queue must exist, and the sink will not create one** — its rate limits are the entire reason
to use the service, and a queue created with defaults would silently discard them. Create it with
the pacing the target endpoint can absorb:

```sh
gcloud tasks queues create webhooks --location=asia-northeast1 \
    --max-dispatches-per-second=10 --max-concurrent-dispatches=5
```

The endpoint must be reachable from Cloud Tasks, which for an HTTP target generally means a public
IP — the exception, and how to authorize against a Cloud Run service, is on the
[Cloud Tasks connector]({{< relref "docs/connectors/datastream/cloudtasks" >}}) page.

## The same thing in SQL

Only Pub/Sub has a table connector today; BigQuery and Cloud Tasks are tracked on
[#57]({{< param BookRepo >}}/issues/57) and [#99]({{< param BookRepo >}}/issues/99). Put
`flink-sql-connector-gcp-pubsub` in Flink's `lib/`, or add it in the SQL client:

```sql
ADD JAR '/path/to/flink-sql-connector-gcp-pubsub-0.1.0-SNAPSHOT.jar';

CREATE TABLE orders (
  order_id STRING,
  amount   INT
) WITH (
  'connector' = 'pubsub',
  'project'   = 'my-project',
  'topic'     = 'orders',
  'format'    = 'json'
);

INSERT INTO orders VALUES ('a-1', 10), ('a-2', 20);
```

Reading is the same table definition with `subscription` in place of `topic`, and the parts of a
message that are not the payload — attributes, ordering key, message id, publish time — arrive as
metadata columns:

```sql
CREATE TABLE incoming_orders (
  order_id     STRING,
  amount       INT,
  publish_time TIMESTAMP_LTZ(3) METADATA FROM 'publish-time' VIRTUAL,
  WATERMARK FOR publish_time AS publish_time - INTERVAL '5' SECOND
) WITH (
  'connector'    = 'pubsub',
  'project'      = 'my-project',
  'subscription' = 'orders-sub',
  'format'       = 'json'
);

SELECT * FROM incoming_orders;
```

Checkpointing is a cluster setting here rather than a line of code — set
`execution.checkpointing.interval` in `flink-conf.yaml` or with `SET` in the SQL client. It matters
for exactly the reasons the two DataStream jobs above give.

## Next steps

- [Examples]({{< relref "docs/examples" >}}) — dynamic per-record destinations, exactly-once,
  auto-creation, and running the whole thing against an emulator
- [Connectors]({{< relref "docs/connectors" >}}) — what every option does, what each connector
  guarantees, and why
- The [Java API reference]({{< param ApiDocsURL >}}), generated from the source of every module
