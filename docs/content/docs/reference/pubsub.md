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

# Cloud Pub/Sub options

Every option the Pub/Sub sink and source take, and the ones the shared
[dead-letter queue](#pubsubdeadletterqueuebuilder) takes. What each one is *for* is on the
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
| `serializer` | **required** | Converts each record into a `PubsubMessage`, or into `null` to skip it. `payload(...)` wraps a payload-only schema; `withAttributes(...)` and `withOrderingKey(...)` layer onto any of them |
| `createDisposition` | `CREATE_IF_NEEDED` | Whether a missing topic is created or fails the job |
| `topicCreateOptions` | Pub/Sub's own defaults | [Settings](#topiccreateoptions) for topics the sink creates. Rejected with `CREATE_NEVER` |
| `publisherOptions` | [defaults](#pubsubpublisheroptions) | Publisher and writer tuning |
| `failedMessageHandler` | `FailureHandler.failJob()` | What happens to a message that terminally fails — fail, drop, or dead-letter. Under `enableMessageOrdering(true)`, dropping a keyed message leaves a gap in that key's stream |
| `serviceAccountKeyFile` | *unset ⇒ application-default credentials* | Reads a service-account JSON key on each TaskManager when the writer starts. Every eligible TaskManager must see the same path. Rejected beside `emulatorEndpoint`; see the [deployment note]({{< relref "docs/connectors/datastream/pubsub" >}}#credential-file-deployment) |
| `emulatorEndpoint` | — | Points the sink at an emulator over a plaintext channel with **no credentials**. Never production. Given as `host:port`, and rejected at the setter if it is not |

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
| `retryTotalTimeout` | *unset ⇒ SDK default (600 s)* | Total budget for a publish including retries; `0` is gax's own value for "bound retries by the attempt count instead" |
| `retryInitialDelay` | *unset ⇒ SDK default (100 ms)* | Delay before the first retry; `0` (gax's own default) means none |
| `retryDelayMultiplier` | *unset ⇒ SDK default (×4)* | Factor the retry delay grows by |
| `retryMaxDelay` | *unset ⇒ SDK default (60 s)* | Cap on the delay between retries; `0` clamps every delay to none |
| `retryInitialRpcTimeout` | *unset ⇒ SDK default* | Timeout of the first publish RPC attempt; `0` is gax's own value for "let the call run indefinitely" |
| `retryRpcTimeoutMultiplier` | *unset ⇒ SDK default* | Factor the per-RPC timeout grows by |
| `retryMaxRpcTimeout` | *unset ⇒ SDK default* | Cap on a publish RPC attempt's timeout; `0` lets every call run indefinitely |
| `retryMaxAttempts` | *unset ⇒ SDK default* | Cap on publish attempts |

`retryTotalTimeout` and `retryMaxAttempts` **are rejected beside `enableMessageOrdering(true)`**,
rather than silently ignored: an ordering-enabled publisher retries without limit, so neither an
attempt cap nor a total timeout can bound a publish there — for unkeyed messages too. The other six
retry knobs are unaffected and combine with ordering freely. A program that toggles ordering must
therefore set these two only on the branch that leaves it off, rather than once for both. The
mechanism is on the [Publisher lifecycle]({{< relref "docs/connectors/datastream/pubsub" >}}#publisher-lifecycle)
page, where it also explains why the shutdown budget exists.

**Ordering, in-flight caps and the republish recovery**, all the connector's own.

| Option | Default | What it does |
|---|---|---|
| `enableMessageOrdering` | `false` | Honours ordering keys. Without it, a message carrying one is rejected with an error naming this option. With it, a dropping `failedMessageHandler` leaves a gap in the dropped message's key, and `retryTotalTimeout`/`retryMaxAttempts` may not be set (see above) |
| `maxInFlightMessages` | 1000 | Caps the writer's unacknowledged publishes. A write at the cap yields to the mailbox, asks the publishers to send what they are still batching, and is bounded by `publishProgressTimeout` |
| `maxInFlightBytes` | 64 MiB | Caps their total serialized size. `Long.MAX_VALUE` bounds by count only |
| `maxActivePublishers` | 100 | Caps the publishers retained by one writer subtask. A new destination releases the least-recently-used clean publisher; if none is clean, the write drains and repairs all publishers before releasing one. The replacement opens only after bounded shutdown succeeds; an overrun fails the write rather than accumulating abandoned publisher resources |
| `destinationIdleTimeout` | 1 h | Releases a clean publisher after its destination has been unused for strictly longer than this timeout at a successful non-terminal checkpoint flush. A later record recreates it |
| `publishProgressTimeout` | 600 s | How long the sink may wait with **no** publish completing before it fails the job. The budget restarts at every completion, so a topic that keeps answering never spends it however slow it is; one that has stopped answering spends it once. Covers the admission gate and drains for checkpoints, capacity eviction, failure repair, and per-message isolation. With `enableMessageOrdering` nothing *inside the sink* ends an outage but this. See [What a running job can spend]({{< relref "docs/connectors/datastream/pubsub" >}}#what-a-running-job-can-spend-and-publishprogresstimeout) |
| `recoveryInitialBackoff` | 500 ms | First backoff of a republish — after creating a missing topic, or after resuming an ordering key |
| `recoveryMaxBackoff` | 10 s | Cap of that backoff, before ±25% jitter |
| `recoveryMaxAttempts` | 10 | Republish attempts per destination and incident. Bounds a repair making no progress, not the length of a rejected run (see [Ordering and a dropping policy]({{< relref "docs/connectors/datastream/pubsub" >}}#ordering-and-a-dropping-policy)) |
| `maxConsecutiveRejections` | 100 | Fails the job once this many confirmed rejections arrive in a row with no successful publish between them — the guardrail on a dropping policy's [isolation cost]({{< relref "docs/connectors/datastream/pubsub" >}}#ordering-and-a-dropping-policy). Any success resets the count; `-1` removes the bound |

**Shutdown.**

| Option | Default | What it does |
|---|---|---|
| `shutdownTimeout` | 30 s | How long one sink publisher release waits. Capacity eviction can spend it in `write`, idle eviction in a successful non-terminal `flush`, and final teardown in `close`; each release asks every selected publisher to stop before waiting, so it costs this once however many publishers it covers. For running eviction this is the maximum task-thread stall from one release, and an overrun fails the task before a replacement opens. Keep it under Flink's `task.cancellation.timeout` (180 s by default) for final close. It bounds the sink's own publishers — a `sendToDeadLetterQueue(...)` handler spends [a second close budget of the same shape](#pubsubdeadletterqueuebuilder) on top. See [Publisher lifecycle]({{< relref "docs/connectors/datastream/pubsub" >}}#publisher-lifecycle) |

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
| `deserializer` | **required** | Converts each `PubsubMessage` into zero or more non-null records. Emit synchronously during the call; do not retain the collector |
| `orderingMode` | `NONE` | `PER_KEY` preserves per-ordering-key order, at the cost of one subtask per subscription and one pull connection |
| `subscriberOptions` | [defaults](#pubsubsubscriberoptions) | Subscriber and reader tuning |
| `deserializationFailurePolicy` | `FAIL` | What happens to a message the schema cannot convert — fail, drop, or nack |
| `startPosition` | `continueFromSubscription()` | Where the source begins. Any other value issues a seek, once, at first start |
| `serviceAccountKeyFile` | *unset ⇒ application-default credentials* | Reads a service-account JSON key when the JobManager's enumerator or a TaskManager's reader starts. Every eligible process must see the same path. Rejected beside `emulatorEndpoint`; see the [deployment note]({{< relref "docs/connectors/datastream/pubsub" >}}#credential-file-deployment) |
| `emulatorEndpoint` | — | Points the source at an emulator over a plaintext channel with **no credentials**. `PUBSUB_EMULATOR_HOST` is deliberately ignored. Given as `host:port`, and rejected at the setter if it is not |

## `PubSubSubscriberOptions`

Set through `subscriberOptions(...)`. The flow-control, connection-count and ack-extension knobs go
to the SDK subscriber and are unset by default; the reader-wide and paused-split bounds and the
last three are the source's own. Google publishes no recommended flow-control values, so the SDK defaults stand — see
[Tuning]({{< relref "docs/connectors/datastream/pubsub" >}}#tuning) for the one sizing rule that is
specific to this source, that acknowledgement waits for a checkpoint.

| Option | Default | What it does |
|---|---|---|
| `flowControlMaxOutstandingElementCount` | *unset ⇒ SDK default (1000)* | Messages one subscriber holds outstanding before it stops pulling, for as long as it is extending their leases |
| `flowControlMaxOutstandingRequestBytes` | *unset ⇒ SDK default (100 MB)* | The same in bytes |
| `subscriberBufferMaxMessages` | 10000 | Hard aggregate cap on messages retained in all subscriber buffers of one source reader |
| `subscriberBufferMaxBytes` | 64 MiB | Hard aggregate cap on serialized bytes retained in all subscriber buffers of one source reader; whichever hard cap would be crossed first controls the response |
| `pausedSplitBufferMaxMessages` | *unset ⇒ twice the effective `flowControlMaxOutstandingElementCount` (so 2000 by default)* | Messages a split paused by watermark alignment may buffer before its subscriber is stopped and reopened on resume |
| `pausedSplitBufferMaxBytes` | *unset ⇒ twice the effective `flowControlMaxOutstandingRequestBytes` (so 200 MB by default)* | The same in bytes; whichever bound is exceeded first stops the subscriber |
| `parallelPullCount` | *unset ⇒ SDK default (1)* | Streaming-pull connections per subscriber. Rejected under `orderingMode(PER_KEY)` |
| `maxAckExtensionPeriod` | *unset ⇒ SDK default (1 h)* | How long the client library keeps extending a message's deadline. Must exceed the checkpoint interval comfortably; `0` is the client library's own value for "disable auto deadline extension" |
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

## `PubSubDeadLetterQueue.builder()`

Experimental ([#211]({{< param BookRepo >}}/issues/211)). The class is Pub/Sub's, but **one instance
serves every connector in this repository** — it is what `FailureHandler.sendToDeadLetterQueue(...)`
takes on a BigQuery, Cloud Tasks or Bigtable sink too, which is why those pages send you here. What
each option is *for*, and how to size the two budgets against a checkpoint interval, is under
[Dead-lettering to a Pub/Sub topic]({{< relref "docs/connectors/datastream/pubsub" >}}#dead-lettering-to-a-pubsub-topic).

| Option | Default | What it does |
|---|---|---|
| `topic` | **required** | Publishes every dead letter to one topic, which must already exist — this queue never creates one |
| `serviceAccountKeyFile` | *unset ⇒ application-default credentials* | Reads a service-account JSON key when each host sink writer opens the queue. Every eligible TaskManager must see the same path. Independent of the host connector's credentials and rejected beside `emulatorEndpoint`; see the [deployment note]({{< relref "docs/connectors/datastream/pubsub" >}}#credential-file-deployment) |
| `maxInFlightMessages` | `1000` | How many publishes may be in flight before an offer waits for them. `WRITE_THROUGH` (`0`) publishes each element synchronously, the narrowest loss window at one round trip per element; `UNBOUNDED` (`-1`) buffers until the flush |
| `flushTimeout` | 60 s | How long **one wait** for in-flight publishes may take — the wait in `flush()`, and the one an offer makes when the bound above is full. One deadline per wait rather than per publish, and there is no unbounded setting. Expiry throws, failing the checkpoint or the task |
| `shutdownTimeout` | 30 s | How long the queue's own close waits for its publisher. Spent *after* the sink's own `shutdownTimeout`, so keep the sum under Flink's `task.cancellation.timeout` (180 s by default) |
| `emulatorEndpoint` | — | Points the queue at an emulator over a plaintext channel with **no credentials**. Never production. Given as `host:port`, and rejected at the setter if it is not |
