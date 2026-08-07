---
title: Cloud Pub/Sub
type: docs
weight: 20
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

# Publish to and read from Cloud Pub/Sub

Assumes the artifacts and credentials from the
[Quickstart]({{< relref "docs/quickstart" >}}) index, and the imports an IDE resolves from the
[Java API reference]({{< param ApiDocsURL >}}).

## Publish a stream to a topic

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

Checkpointing is not decoration: the sink is at-least-once *only* with it, since the checkpoint is
what makes Flink flush the messages the SDK publishers are still batching.

`dataOnly(...)` wraps any Flink `SerializationSchema` for payload-only messages. Attributes and an
ordering key layer onto it with `withAttributes(...)` and `withOrderingKey(...)`; a schema that
needs full control returns a `PubsubMessage` directly.

The topic is created if it does not exist. **An auto-created topic has no subscriptions**, so
messages published before one is attached reach nobody — which is what
[topic auto-creation]({{< relref "docs/examples/pubsub" >}}#topics-on-the-sink) is about. Create
the subscription first when trying this.

## Read a stream from a subscription

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

## The same thing in SQL

Pub/Sub and BigQuery have table connectors; Cloud Tasks' is tracked on
[#99]({{< param BookRepo >}}/issues/99). Put `flink-sql-connector-gcp-pubsub` in Flink's `lib/`, or
add it in the SQL client — and `flink-sql-connector-gcp-bigquery` beside it if the job also writes
to BigQuery, since the two are built to share a classpath:

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
for exactly the reasons the two jobs above give. The full option surface is on the
[Pub/Sub SQL connector]({{< relref "docs/connectors/table/pubsub" >}}) page.

## Next

[Cloud Pub/Sub examples]({{< relref "docs/examples/pubsub" >}}) — a topic per record, topic and
subscription auto-creation, and running the whole thing against the emulator.
