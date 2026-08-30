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

# Publish to and read from Cloud Pub/Sub

Assumes the artifacts and credentials from the
[Quickstart]({{< relref "docs/quickstart" >}}) index, and the imports an IDE resolves from the
[Java API reference]({{< param ApiDocsURL >}}).

## Publish a stream to a topic

{{< java-snippet file="PubSubQuickstartPublish.java" tag="pubsub-quickstart-publish" >}}

Checkpointing is not decoration: the sink is at-least-once *only* with it, since the checkpoint is
what makes Flink flush the messages the SDK publishers are still batching.

`payload(...)` wraps any Flink `SerializationSchema` for payload-only messages. Attributes and an
ordering key layer onto it with `withAttributes(...)` and `withOrderingKey(...)`; a schema that
needs full control returns a `PubsubMessage` directly.

The topic is created if it does not exist. **An auto-created topic has no subscriptions**, so
messages published before one is attached reach nobody — which is what
[topic auto-creation]({{< relref "docs/examples/pubsub" >}}#topics-on-the-sink) is about. Create
the subscription first when trying this.

## Read a stream from a subscription

{{< java-snippet file="PubSubQuickstartRead.java" tag="pubsub-quickstart-read" >}}

The subscription must already exist: passing it without creation settings is the statement that it
does, and the enumerator checks before assigning a split. Pub/Sub's publish time becomes each
record's event timestamp, so a `WatermarkStrategy` over it is what to use instead of
`noWatermarks()` in an event-time job.

## The same thing in SQL

Pub/Sub, BigQuery and Bigtable have table connectors; Cloud Tasks' is tracked on
[#99]({{< param BookRepo >}}/issues/99). Put `flink-sql-connector-gcp-pubsub` in Flink's `lib/`, or
add it in the SQL client — and `flink-sql-connector-gcp-bigquery` beside it if the job also writes
to BigQuery, since the jars are built to share a classpath:

{{< sql-snippet file="flink/PubSubQuickstart.sql" tag="sink" >}}

Reading is the same table definition with `subscription` in place of `topic`, and the parts of a
message that are not the payload — attributes, ordering key, message id, publish time — arrive as
metadata columns:

{{< sql-snippet file="flink/PubSubQuickstart.sql" tag="source" >}}

Checkpointing is a cluster setting here rather than a line of code — set
`execution.checkpointing.interval` in `flink-conf.yaml` or with `SET` in the SQL client. It matters
for exactly the reasons the two jobs above give. The full option surface is on the
[Pub/Sub SQL connector]({{< relref "docs/connectors/table/pubsub" >}}) page.

## Next

Continue with the Pub/Sub examples by direction: [DataStream source]({{< relref
"docs/examples/pubsub" >}}#datastream-source), [DataStream sink]({{< relref
"docs/examples/pubsub" >}}#datastream-sink), [Table source]({{< relref
"docs/examples/pubsub" >}}#table-source), [Table sink]({{< relref
"docs/examples/pubsub" >}}#table-sink), or [local development]({{< relref
"docs/examples/pubsub" >}}#running-against-the-emulator).
