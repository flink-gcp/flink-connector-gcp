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

## DataStream source

The [Quickstart consume job]({{< relref "docs/quickstart/pubsub" >}}#read-a-stream-from-a-subscription)
is the canonical basic source example.
The worked case below changes how the source obtains its subscription.

### Subscriptions, on the source

On the source, **passing creation settings alongside a subscription is what authorises creating
it.** No separate disposition is needed, because there is no meaningful "create with defaults" — a
subscription without a topic is not a subscription, and only you know which topic to bind.

{{< java-snippet file="PubSubExamplesSubscriptionsOnSource.java" tag="pubsub-examples-subscriptions-on-source" >}}

**The settings are per subscription because they carry the topic binding.** One options object
shared across several would bind them all to one topic, and Pub/Sub delivers a complete copy of a
topic's stream to every subscription of it — so the source would emit each message once per
subscription, with nothing anywhere reporting an error.

This example keeps the default `continueFromSubscription()` start position, so a subscription it
auto-creates begins without a pre-creation backlog. A topic with topic-level message retention can
make older retained messages available to a new subscription when a job explicitly seeks with
`earliestRetained()` or `fromTimestamp(...)`.

## DataStream sink

The [Quickstart publish job]({{< relref "docs/quickstart/pubsub" >}}#publish-a-stream-to-a-topic)
is the canonical basic sink example.
The worked cases below change the destination and how the sink creates it.

### A topic per record

The [dynamic destinations guide]({{< relref "docs/examples/dynamic-destinations" >}}#pubsub-topics) explains the shared resolver contract and the ordering boundary for a dynamically selected topic.

{{< java-snippet file="PubSubExamplesTopicPerRecord.java" tag="pubsub-examples-topic-per-record" >}}

A lambda is fine where the destination set is small: `TopicDestination` is pure identity, so the allocation is a few fields.
Cache it as the [BigQuery example]({{< relref "docs/examples/bigquery" >}}#a-table-per-day) does when the resolver is doing real work to produce the name.

Each active topic gets its own SDK publisher, owned by the writer.
The writer can release a clean publisher after its idle timeout or as the least-recently-used entry at the active-publisher limit, and recreates it if that topic receives another record.
If bounded shutdown does not finish, the writer fails before opening a replacement; the [publisher lifecycle guide]({{< relref "docs/connectors/datastream/pubsub" >}}#publisher-lifecycle) explains this resource guard.
Publishers still active when the writer closes are closed with it.
`enableMessageOrdering(true)` is required because the writer rejects a serialized ordering key while ordering is disabled.
The `keyBy` call routes one customer's records to one sink subtask, and the resolver must keep that customer on one topic.
Pub/Sub preserves a separate sequence when the same key moves to another topic or writer subtask.

### Topics, on the sink

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

## Publishing and consuming with SQL

Create the subscription before publishing so it retains every message from the example:

```sh
gcloud pubsub topics create orders --project=my-project
gcloud pubsub subscriptions create orders-sub \
  --topic=orders \
  --enable-message-ordering \
  --project=my-project
```

Source guidance is presented before sink guidance on this page.
To run the two SQL regions as one end-to-end round trip, prepare both without submitting the source
`SELECT`, run the bounded sink `INSERT` to completion, and then submit the unbounded source `SELECT`.
Each submission creates a separate Flink job.

### Table source

Before submitting the source `SELECT`, create the ordering-enabled subscription and run the bounded
sink `INSERT` in the order described by the [round-trip setup](#publishing-and-consuming-with-sql).

The source consumes JSON payloads and exposes the fields that Pub/Sub keeps outside the payload as
virtual metadata columns:

{{< sql-snippet file="flink/PubSubExamples.sql" tag="source" >}}

The attributes map supplies `profile_key`, which can become the equality key for the
[Bigtable enrichment pipeline]({{< relref "docs/examples/bigtable"
>}}#enriching-pubsub-events-before-creating-tasks).
That example introduces asynchronous lookup, while the
[Bigtable Table connector]({{< relref "docs/connectors/table/bigtable" >}}#lookup-joins) covers the
synchronous, asynchronous, and cached modes.

The publish time drives the watermark, and the message id and subscription resource name remain
available for tracing the enriched event.
After the lookup, the [Cloud Tasks request metadata]({{< relref
"docs/connectors/table/cloudtasks" >}}#writable-metadata) can derive a URL, headers, schedule time,
or task id from the enriched row.
The [Cloud Tasks delivery section]({{< relref "docs/connectors/table/cloudtasks"
>}}#delivery-guarantees-and-task-identity) explains what happens after task creation.

With `scan.ordering-mode` set to `per-key`, the source creates one split for each subscription
and assigns each subscription to one reader subtask.
Source parallelism above the subscription count therefore adds no consuming capacity.
The source preserves a key's order only to its output.
If the enrichment plan introduces a downstream exchange, it must partition by `ordering_key`
when later operators still require that order.
The [DataStream ordering section]({{< relref "docs/connectors/datastream/pubsub"
>}}#message-ordering) gives the full boundary and cost.

The source is unbounded, so the `SELECT` continues waiting for later messages.
The connector remains at-least-once, and recovery may replay a message even when the relative order
for its key is preserved.
Checkpointing acknowledges messages only after their output is durable.
The subscription must exist unless `scan.auto-create.*` settings authorize source-side creation,
and the source never creates its topic.
The source DDL selects `continue-from-subscription`, which leaves the shared subscription position
unchanged.
The [Table source guide]({{< relref "docs/connectors/table/pubsub" >}}#source) explains the other
startup modes and the service-side seek they perform.

### Table sink

Before submitting the sink `INSERT`, create the ordering-enabled subscription shown in the
[round-trip setup](#publishing-and-consuming-with-sql) so it retains the published messages.

The sink publishes the payload, attributes, and ordering key that the source definition reads:

{{< sql-snippet file="flink/PubSubExamples.sql" tag="sink" >}}

The table sink routes one non-empty ordering key to one writer subtask before publishing.
Null and empty keys remain unordered.
This bounded `INSERT` flushes at end of input.
In a streaming sink job, checkpointing flushes messages still batched by the publishers and is
required for at-least-once delivery.
The [Table sink guide]({{< relref "docs/connectors/table/pubsub" >}}#sink) documents the writable
metadata and the shuffle introduced by an ordering-key column.

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
