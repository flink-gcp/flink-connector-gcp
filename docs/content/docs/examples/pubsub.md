---
title: Cloud Pub/Sub
type: docs
weight: 20
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

# Cloud Pub/Sub examples

Starting from the [Cloud Pub/Sub quickstart]({{< relref "docs/quickstart/pubsub" >}}) jobs.

## Publishing and consuming with SQL

Create the subscription before publishing so it retains every message from the example:

```sh
gcloud pubsub topics create orders --project=my-project
gcloud pubsub subscriptions create orders-sub \
  --topic=orders \
  --enable-message-ordering \
  --project=my-project
```

The sink table publishes JSON payloads, attributes, and an ordering key to the topic:

```sql
CREATE TABLE outgoing_orders (
  order_id STRING,
  amount INT,
  attrs MAP<STRING, STRING> METADATA FROM 'attributes',
  ordering_key STRING METADATA FROM 'ordering-key'
) WITH (
  'connector' = 'pubsub',
  'project' = 'my-project',
  'topic' = 'orders',
  'format' = 'json',
  'sink.message-ordering.enabled' = 'true'
);

INSERT INTO outgoing_orders
VALUES
  ('a-1', 10, MAP['source', 'sql'], 'customer-1'),
  ('a-2', 20, MAP['source', 'sql'], 'customer-1');
```

The source table consumes those payloads from the subscription and can expose Pub/Sub fields as virtual metadata columns:

```sql
SET 'execution.checkpointing.interval' = '10 s';

CREATE TABLE incoming_orders (
  order_id STRING,
  amount INT,
  message_id STRING METADATA FROM 'message-id' VIRTUAL,
  publish_time TIMESTAMP_LTZ(3) METADATA FROM 'publish-time' VIRTUAL,
  attrs MAP<STRING, STRING> METADATA FROM 'attributes' VIRTUAL,
  ordering_key STRING METADATA FROM 'ordering-key' VIRTUAL,
  WATERMARK FOR publish_time AS publish_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'pubsub',
  'project' = 'my-project',
  'subscription' = 'orders-sub',
  'format' = 'json',
  'scan.ordering-mode' = 'per-key'
);

SELECT order_id, amount, attrs, ordering_key, message_id, publish_time
FROM incoming_orders;
```

The source is unbounded, so the `SELECT` continues waiting for later messages.
With ordering enabled on the sink and subscription and `scan.ordering-mode` set to `per-key`, Pub/Sub and the source preserve publish order separately for each non-empty ordering key within this subscription; they do not establish one global order across keys, subscriptions, or topics.
The table sink routes one non-empty key to one writer subtask before publishing, while null and empty keys remain unordered.
The source pins this subscription to one reader subtask and preserves order only up to its output; repartition downstream by `ordering_key` when later operators must retain the same per-key order.
The connector remains at-least-once, so recovery may replay a message even though the relative order for its key is preserved.
Checkpointing acknowledges messages only after their output is durable and flushes messages still batched by the sink; configure it for both directions in a production job.
The subscription must exist unless `scan.auto-create.*` settings explicitly authorize source-side creation, and the source never creates its topic.

## A topic per record

The [dynamic destinations guide]({{< relref "docs/examples/dynamic-destinations" >}}#pubsub-topics) explains the shared resolver contract and the ordering boundary for a dynamically selected topic.

{{< java-snippet file="PubSubExamplesTopicPerRecord.java" tag="pubsub-examples-topic-per-record" >}}

A lambda is fine where the destination set is small: `TopicDestination` is pure identity, so the allocation is a few fields.
Cache it as the [BigQuery example]({{< relref "docs/examples/bigquery" >}}#a-table-per-day) does when the resolver is doing real work to produce the name.

Each distinct topic gets its own SDK publisher, owned by the writer and closed with it.
`enableMessageOrdering(true)` is required because the writer rejects a serialized ordering key while ordering is disabled.
The `keyBy` call routes one customer's records to one sink subtask, and the resolver must keep that customer on one topic.
Pub/Sub preserves a separate sequence when the same key moves to another topic or writer subtask.

## Topics, on the sink

The sink creates a missing topic reactively: a publish failing with `NOT_FOUND` parks its messages,
creates the topic, and republishes under a bounded backoff. An existing topic costs nothing — no
admin call is made unless a publish actually fails.

{{< java-snippet file="PubSubExamplesTopicsOnSink.java" tag="pubsub-examples-topics-on-sink" >}}

**An auto-created topic starts with no subscriptions**, so without `messageRetention` the messages
published before one is attached are retained for nobody. That makes auto-creation without it suit
pipelines whose consumers create their own subscriptions, or attach them promptly. With dynamic
destinations one options object applies to *every* topic the sink creates.

Supplying the options is not what authorises creation here — the disposition is, because a topic
can meaningfully be created with defaults. Combining them with `CREATE_NEVER` is rejected at graph
construction rather than silently ignored.

## Subscriptions, on the source

The source is the other way round: **passing creation settings alongside a subscription is what
authorises creating it.** There is no disposition, because there is no meaningful "create with
defaults" — a subscription without a topic is not a subscription, and only you know which topic to
bind.

{{< java-snippet file="PubSubExamplesSubscriptionsOnSource.java" tag="pubsub-examples-subscriptions-on-source" >}}

**The settings are per subscription because they carry the topic binding.** One options object
shared across several would bind them all to one topic, and Pub/Sub delivers a complete copy of a
topic's stream to every subscription of it — so the source would emit each message once per
subscription, with nothing anywhere reporting an error.

A subscription only retains messages published **after** it exists, so a job that auto-creates one
starts from an empty backlog whatever was published before.

## Running against the emulator

Google's own emulator runs on your machine rather than in a container, but it is a gcloud
component rather than part of the base install:

```sh
gcloud components install pubsub-emulator
gcloud beta emulators pubsub start --project=my-project --host-port=localhost:8085
```

Pass `--host-port` rather than taking the default, which binds the IPv6 loopback (`[::1]:8085`) —
`emulatorEndpoint("localhost:8085")` is then resolving a name the emulator may not be listening on.

{{< java-snippet file="PubSubExamplesEmulator.java" tag="pubsub-examples-emulator" >}}

`emulatorEndpoint(...)` exists on the source too, and on both it opens a **plaintext channel with
no credentials** — so it must only ever point at an emulator, never at production Pub/Sub. Note the
source deliberately does *not* honour the `PUBSUB_EMULATOR_HOST` environment variable, unlike the
Apache connector: a stray value on a task manager would silently redirect a production job.

What this emulator cannot show, and the reason the
[rule about emulators]({{< relref "docs/examples" >}}#an-emulator-is-a-convenience-not-an-authority)
matters here: ordered delivery (per-key callback serialization in the client library is gated on a
subscription property the emulator does not set, so callbacks arrive out of order with no Flink
involved), ordered seek, dead-letter forwarding, IAM, and the *effect* of every create-option it
stores and ignores — a KMS key that does not exist is accepted.
