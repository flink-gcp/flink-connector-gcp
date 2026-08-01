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

# Cloud Pub/Sub examples

Starting from the [Cloud Pub/Sub quickstart]({{< relref "docs/quickstart/pubsub" >}}) jobs.

## A topic per record

```java
env.fromSource(source, WatermarkStrategy.noWatermarks(), "orders")
        .sinkTo(
                PubSubSink.<OrderEvent>builder()
                        .destinationResolver(
                                (element, context) ->
                                        TopicDestination.of("my-project", element.region()))
                        .serializer(
                                PubSubSerializationSchema.dataOnly(new OrderEventSchema())
                                        .withOrderingKey(OrderEvent::customerId))
                        .build());
```

A lambda is fine where the destination set is small: `TopicDestination` is pure identity, so the
allocation is a few fields. Cache it as the
[BigQuery example]({{< relref "docs/examples/bigquery" >}}#a-table-per-day) does when the resolver
is doing real work to produce the name.

Each distinct topic gets its own SDK publisher, owned by the writer and closed with it. Ordering,
when enabled, is per key *within one topic* and holds per writer subtask — route same-key records
to the same subtask with `keyBy` for end-to-end order.

## Topics, on the sink

The sink creates a missing topic reactively: a publish failing with `NOT_FOUND` parks its messages,
creates the topic, and republishes under a bounded backoff. An existing topic costs nothing — no
admin call is made unless a publish actually fails.

```java
PubSubSink.<OrderEvent>builder()
        .topic(TopicDestination.of("my-project", "orders"))
        .topicCreateOptions(
                TopicCreateOptions.builder()
                        // What makes messages published before a subscription exists reachable
                        // by one created later, or by a backwards seek.
                        .messageRetention(Duration.ofDays(7))
                        .build())
        .serializer(PubSubSerializationSchema.dataOnly(new OrderEventSchema()))
        .build();
```

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

```java
PubSubSource.<OrderEvent>builder()
        .subscription(
                SubscriptionDestination.of("my-project", "orders-sub"),
                SubscriptionCreateOptions.builder()
                        .topic(TopicDestination.of("my-project", "orders"))
                        .ackDeadline(Duration.ofSeconds(60))
                        .build())
        // No options: this one must already exist, and the startup check says so if it does not.
        .subscription(SubscriptionDestination.of("my-project", "returns-sub"))
        .deserializationSchema(new OrderEventDeserializationSchema())
        .build();
```

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

```java
PubSubSink.<String>builder()
        .topic(TopicDestination.of("my-project", "orders"))
        .serializer(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
        .emulatorEndpoint("localhost:8085")
        .build();
```

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
