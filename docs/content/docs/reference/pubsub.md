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

# Cloud Pub/Sub options

Every option the Pub/Sub sink and source take. What each one is *for* is on the
[Cloud Pub/Sub connector]({{< relref "docs/connectors/datastream/pubsub" >}}) page, linked from
each section; the three forms of the Default column are explained
[here]({{< relref "docs/reference" >}}#what-a-default-means).

The `WITH` options of the `pubsub` table connector are a separate surface, documented on the
[Pub/Sub SQL connector]({{< relref "docs/connectors/table/pubsub" >}}) page.

## `PubSubSink.builder()`

| Option | Default | What it does |
|---|---|---|
| `topic` | **required**, unless `destinationResolver` is set | Publishes every record to one fixed topic |
| `destinationResolver` | — | Resolves the topic per record |
| `serializer` | **required** | Converts each record into a `PubsubMessage`. `dataOnly(...)` wraps a payload-only schema; `withAttributes(...)` and `withOrderingKey(...)` layer onto any of them |
| `createDisposition` | `CREATE_IF_NEEDED` | Whether a missing topic is created or fails the job |
| `topicCreateOptions` | Pub/Sub's own defaults | [Settings](#topiccreateoptions) for topics the sink creates. Rejected with `CREATE_NEVER` |
| `publisherOptions` | [defaults](#pubsubpublisheroptions) | Publisher and writer tuning |
| `failedMessageHandler` | `FailureHandler.failJob()` | What happens to a message that terminally fails — fail, drop, or dead-letter. Rejected with `enableMessageOrdering(true)` |
| `emulatorEndpoint` | — | Points the sink at an emulator over a plaintext channel with **no credentials**. Never production |

## `PubSubPublisherOptions`

Set through `publisherOptions(...)`. The batching and retry knobs are handed to the SDK publisher
and left unset by default, so `defaults()` is equivalent to passing no options at all; the in-flight
caps and the recovery budget are the connector's own. See
[Publisher options]({{< relref "docs/connectors/datastream/pubsub" >}}#publisher-options) for why
the SDK's flow controller is deliberately not exposed, and
[Backpressure]({{< relref "docs/connectors/datastream/pubsub" >}}#delivery-guarantees-and-state)
for how the caps are sized.

**SDK batching.**

| Option | Default | What it does |
|---|---|---|
| `batchElementCountThreshold` | *unset ⇒ SDK default (100)* | Messages batched into one publish request |
| `batchRequestByteThreshold` | *unset ⇒ SDK default (1000 B)* | Bytes batched into one publish request. Keep below `maxInFlightBytes` |
| `batchDelayThreshold` | *unset ⇒ SDK default (1 ms)* | How long a publisher waits for a batch to fill |

**SDK publish retries.**

| Option | Default | What it does |
|---|---|---|
| `retryTotalTimeout` | *unset ⇒ SDK default (600 s)* | Total budget for a publish including retries |
| `retryInitialDelay` | *unset ⇒ SDK default (100 ms)* | Delay before the first retry |
| `retryDelayMultiplier` | *unset ⇒ SDK default (×4)* | Factor the retry delay grows by |
| `retryMaxDelay` | *unset ⇒ SDK default (60 s)* | Cap on the delay between retries |
| `retryInitialRpcTimeout` | *unset ⇒ SDK default* | Timeout of the first publish RPC attempt |
| `retryRpcTimeoutMultiplier` | *unset ⇒ SDK default* | Factor the per-RPC timeout grows by |
| `retryMaxRpcTimeout` | *unset ⇒ SDK default* | Cap on a publish RPC attempt's timeout |
| `retryMaxAttempts` | *unset ⇒ SDK default* | Cap on publish attempts |

**Ordering, in-flight caps and the auto-creation recovery**, all the connector's own.

| Option | Default | What it does |
|---|---|---|
| `enableMessageOrdering` | `false` | Honours ordering keys. Without it, a message carrying one is rejected with an error naming this option |
| `maxInFlightMessages` | 1000 | Caps the writer's unacknowledged publishes; a write at the cap yields to the mailbox |
| `maxInFlightBytes` | 64 MiB | Caps their total serialized size. `Long.MAX_VALUE` bounds by count only |
| `recoveryInitialBackoff` | 500 ms | First backoff of the topic auto-creation republish |
| `recoveryMaxBackoff` | 10 s | Cap of that backoff, before ±25% jitter |
| `recoveryMaxAttempts` | 10 | Republish attempts per destination |

**Metrics.**

| Option | Default | What it does |
|---|---|---|
| `perDestinationMetrics` | `false` | Registers per-topic `recordsSend` and `sendErrors` counters beside the writer's totals. Off by default: Flink cannot unregister a metric, so with dynamic destinations every topic the job writes to keeps a row in the registry for the task's lifetime. See [Sink metrics]({{< relref "docs/connectors/datastream/pubsub" >}}#sink-metrics) |

## `TopicCreateOptions`

Applied to topics the sink creates. **Creation only** — an existing topic keeps its own settings,
and these are neither applied to it nor compared against it. One options object serves every topic
the sink creates. See
[Topic creation settings]({{< relref "docs/connectors/datastream/pubsub" >}}#topic-creation-settings).

| Option | Default | What it does |
|---|---|---|
| `messageRetention` | *unset ⇒ no topic-level retention* | Keeps messages on the topic itself, acknowledged or not — what makes a later subscription or a backwards seek able to reach them |
| `kmsKeyName` | *unset ⇒ Google-managed encryption* | Encrypts with a customer-managed Cloud KMS key |
| `allowedPersistenceRegions` | *unset ⇒ whatever org policy allows* | Restricts where messages may be persisted |
| `enforceInTransit` | `false` | Also rejects publishes travelling outside those regions. Requires the regions |

## `PubSubSource.builder()`

| Option | Default | What it does |
|---|---|---|
| `subscription` / `subscriptions` | **required**, at least one | The subscriptions to consume. The two-argument `subscription(...)` also [authorises creating it](#subscriptioncreateoptions) |
| `deserializationSchema` | **required** | Converts each `PubsubMessage` into zero or more records |
| `orderingMode` | `NONE` | `PER_KEY` preserves per-ordering-key order, at the cost of one subtask per subscription and one pull connection |
| `subscriberOptions` | [defaults](#pubsubsubscriberoptions) | Subscriber and reader tuning |
| `deserializationFailurePolicy` | `FAIL` | What happens to a message the schema cannot convert — fail, drop, or nack |
| `startPosition` | `continueFromSubscription()` | Where the source begins. Any other value issues a seek, once, at first start |
| `emulatorEndpoint` | — | Points the source at an emulator over a plaintext channel with **no credentials**. `PUBSUB_EMULATOR_HOST` is deliberately ignored |

## `PubSubSubscriberOptions`

Set through `subscriberOptions(...)`. The flow-control, connection-count and ack-extension knobs go
to the SDK subscriber and are unset by default; the last three are the source's own. Google
publishes no recommended flow-control values, so the SDK defaults stand — see
[Tuning]({{< relref "docs/connectors/datastream/pubsub" >}}#tuning) for the one sizing rule that is
specific to this source, that acknowledgement waits for a checkpoint.

| Option | Default | What it does |
|---|---|---|
| `flowControlMaxOutstandingElementCount` | *unset ⇒ SDK default (1000)* | Messages one subscriber holds outstanding before it stops pulling |
| `flowControlMaxOutstandingRequestBytes` | *unset ⇒ SDK default (100 MB)* | The same in bytes |
| `parallelPullCount` | *unset ⇒ SDK default (1)* | Streaming-pull connections per subscriber. Rejected under `orderingMode(PER_KEY)` |
| `maxAckExtensionPeriod` | *unset ⇒ SDK default (1 h)* | How long the client library keeps extending a message's deadline. Must exceed the checkpoint interval comfortably |
| `minDurationPerAckExtension` | *unset ⇒ SDK default (adaptive)* | Smallest extension requested at a time |
| `maxDurationPerAckExtension` | *unset ⇒ SDK default (adaptive)* | Largest extension requested at a time |
| `awaitAckConfirmation` | *unset ⇒ acknowledgement is fire-and-forget* | Makes each completed checkpoint wait for the server's confirmation, failing the job on timeout |
| `shutdownTimeout` | 5 s | How long a reader's close waits for its subscribers. Keep under `source.reader.close.timeout` |
| `maxRecordsPerFetch` | 1000 | How much one fetch drains from one split. Not a memory bound |
| `firstCheckpointTimeout` | 10 min | How long a reader holding unacknowledged messages waits for its first checkpoint before failing the job. `Duration.ZERO` disables the detector |

## `SubscriptionCreateOptions`

**Passing these is what authorises creating the subscription** — there is no disposition, and a
subscription added without them must already exist. They are per subscription because they carry the
topic binding. Creation only, never an update. See
[Subscription auto-creation]({{< relref "docs/connectors/datastream/pubsub" >}}#subscription-auto-creation).

| Option | Default | What it does |
|---|---|---|
| `topic` | **required** | The topic to bind the subscription to |
| `ackDeadline` | *unset ⇒ Pub/Sub default (10 s)* | How long a consumer has to acknowledge before redelivery |
| `enableMessageOrdering` | *unset ⇒ off* | Required by `orderingMode(PER_KEY)`, and fixed at creation |
| `messageRetention` | *unset ⇒ Pub/Sub default (7 days)* | How long unacknowledged messages are retained |
| `retainAckedMessages` | *unset ⇒ off* | Keeps acknowledged messages so a backwards seek can replay them |
| `expirationTtl` | *unset ⇒ Pub/Sub default (31 days)* | How long the subscription may sit inactive before deletion |
| `neverExpire` | *unset ⇒ the TTL above applies* | Creates a subscription that never expires |
| `deadLetterPolicy` | *unset ⇒ no dead lettering* | Forwards a message after N delivery attempts. Required by `deserializationFailurePolicy(NACK)` |
| `filter` | *unset ⇒ every message delivered* | Delivers only matching messages, acknowledging the rest |

`enableExactlyOnceDelivery` is deliberately absent: the source's startup check rejects such a
subscription, so offering it would only let you create one the source then refuses.
