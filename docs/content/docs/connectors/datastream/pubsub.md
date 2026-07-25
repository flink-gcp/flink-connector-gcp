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

# Cloud Pub/Sub Connector

Cloud Pub/Sub sink and source for Apache Flink, with dynamic per-record topic destinations on the
sink and multi-subscription consumption on the source, provided by the
`flink-connector-gcp-pubsub` module.

Per-feature implementation status is tracked in the
[module README]({{< param BookRepo >}}/blob/main/flink-connector-gcp-pubsub/README.md).

```java
Sink<MyEvent> sink =
        PubSubSink.<MyEvent>builder()
                .destinationResolver(
                        (e, ctx) -> TopicDestination.of("my-project", e.topicName()))
                .serializer(
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema())
                                .withAttributes(e -> Map.of("source", e.source()))
                                .withOrderingKey(MyEvent::deviceId))
                .publisherOptions(
                        PubSubPublisherOptions.builder()
                                .enableMessageOrdering(true)
                                .batchDelayThreshold(Duration.ofMillis(10))
                                .build())
                .build();
```

API notes:

- `PubSubSerializationSchema.serialize` returns a full `PubsubMessage`, so message attributes
  and ordering keys are expressible. `dataOnly(...)` wraps a plain Flink `SerializationSchema`
  for payload-only messages; `withAttributes(...)` and `withOrderingKey(...)` layer extracted
  attributes and an ordering key onto any schema (null/empty extractions add nothing).
- **Message ordering** is honored when `PubSubPublisherOptions.enableMessageOrdering(true)` is
  set; the writer rejects a message carrying an ordering key while ordering is disabled with an
  error naming the option (instead of the SDK's less actionable failure). Ordering is per key
  within one topic, and holds per writer subtask — route same-key records to the same subtask
  (e.g. `keyBy` on the ordering key) for end-to-end order.
- `DestinationResolver.resolve(element, context)` receives the writer context (event timestamp,
  watermark) so time-based routing is expressible. Resolvers run per record: cache and reuse
  `TopicDestination` instances.
- `TopicDestination` is pure topic identity (`equals`/`hashCode` over project/topic) and serves
  as the key of the writer's per-topic publisher map; publisher settings stay on the sink so
  identity remains stable.
- `emulatorEndpoint(host:port)` points the sink at a Pub/Sub emulator: the per-topic publishers
  and the topic auto-creation admin connect over a plaintext channel with no credentials, so it
  must only ever be used against an emulator (for example a testcontainers
  `PubSubEmulatorContainer`) — never against production Pub/Sub.

## Publisher options

`publisherOptions(PubSubPublisherOptions)` tunes the SDK publishers and the writer. Every knob
left unset keeps the SDK's (or the sink's) current default — `PubSubPublisherOptions.defaults()`
is equivalent to not setting options at all.

| Knob | Default when unset |
|---|---|
| `batchElementCountThreshold` / `batchRequestByteThreshold` / `batchDelayThreshold` | SDK batching defaults (100 / 1000 B / 1 ms) |
| `flowControlMaxOutstandingElementCount` / `flowControlMaxOutstandingRequestBytes` | no limit |
| `retryTotalTimeout`, `retryInitialDelay`, `retryDelayMultiplier`, `retryMaxDelay`, `retryInitialRpcTimeout`, `retryRpcTimeoutMultiplier`, `retryMaxRpcTimeout`, `retryMaxAttempts` | SDK publish-retry defaults (600 s total, 100 ms initial, ×4, 60 s cap, ...) |
| `enableMessageOrdering` | `false` |
| `maxInFlightMessages` (writer cap) | 1000 |
| `recoveryInitialBackoff` / `recoveryMaxBackoff` / `recoveryMaxAttempts` (topic auto-creation) | 500 ms / 10 s / 10 |

Flow-control limits use the SDK's `LimitExceededBehavior.Block`: a publish beyond a limit blocks
the task thread until in-flight publishes complete — plain backpressure (permits are released on
SDK threads, so there is no deadlock). `ThrowException` and `Ignore` are deliberately not
exposed. The writer's `maxInFlightMessages` remains the mailbox-friendly primary cap; the
flow-control byte limit is the bound the element-count cap cannot provide. Because the SDK
publisher does not cap its batch thresholds to the flow-control limits (a batch that could never
fill under them would stall until the delay alarm while holding permits), the sink caps the
thresholds itself when limits are set.

**Flow-control limits cannot be combined with message ordering** — the options builder rejects
the combination: in `google-cloud-pubsub` 1.152.0, `Publisher.publish` acquires a flow-control
permit *before* the paused-ordering-key check, and the paused-key rejection and per-key
cancellation paths never release it (verified in the SDK source). After a per-key publish
failure, leaked permits would permanently shrink — and with `Block` eventually exhaust — the
flow-control budget, hanging the task thread with no exception. The guard can be relaxed once
the SDK fixes the leak.

## Delivery guarantees and state

The sink is **at-least-once** and the writer is **stateless by design**: records are published
asynchronously through `google-cloud-pubsub` `Publisher` instances (which batch by element
count, bytes and delay), and on **every checkpoint** Flink invokes the writer's `flush()`
(before the barrier is emitted), which sends all messages still buffered inside the SDK
publishers (`publishAllOutstanding`) and blocks until every in-flight publish is acknowledged.
A successful checkpoint therefore means *all* records up to the barrier are persisted by
Pub/Sub, and the writer stores nothing in Flink state — **discarding operator state
(savepoint-less redeploys, state resets) can never lose sink-buffered data**.

**FLIP-171 `AsyncSinkBase` was evaluated and rejected** for this sink:

- The Pub/Sub `Publisher` SDK already batches and flow-controls; layering `AsyncSinkWriter`'s
  own batching/buffering on top double-buffers every record. Using AsyncSink idiomatically
  would mean bypassing `Publisher` and driving the raw publish RPC with AsyncSink owning
  batching, backpressure and retries — discarding exactly the SDK behavior #20 exposes
  (`BatchingSettings`/`FlowControlSettings`/`RetrySettings` map 1:1 onto `Publisher.Builder`).
- `AsyncSinkWriter` persists unflushed buffers into writer state instead of flushing at the
  barrier, which silently loses those buffers whenever state is dropped. This project
  deliberately chose flush-on-checkpoint statelessness (the BigQuery module records the same
  decision).
- Both models are at-least-once — Pub/Sub has no transactional publish — so rejecting AsyncSink
  forecloses no exactly-once path.

Checkpointing must be enabled for the at-least-once guarantee in streaming jobs: without it,
Flink never calls `flush()` mid-stream, so messages buffered in the SDK publishers are lost on
failure. Batch execution is covered by the end-of-input flush.

**Backpressure.** The number of unacknowledged publishes per writer is capped
(`maxInFlightMessages`, default 1,000). Publish completions are re-dispatched onto the task
mailbox, so all writer state is single-threaded; a write at the cap yields to the mailbox until
completions bring the count back down, bounding sink memory between checkpoints. This is the
mailbox model of the Apache `flink-connector-gcp-pubsub` writer (a design reference — no code is
copied from it); that writer's infinite republish of non-fatally failed messages under
`failOnError=false` is deliberately **not** adopted. SDK-side flow control is available through
the publisher options.

## Publisher lifecycle

Publishers are created lazily per destination topic, owned by the writer, and shut down in the
writer's `close()` (with a bounded 30 s termination wait). This deviates from the vendored
upstream, which caches one `Publisher` per topic JVM-wide and shuts them down only in a JVM
shutdown hook: writer ownership gives a deterministic lifecycle and no cross-job leakage in
shared TaskManagers. The tradeoff: several subtasks on one TaskManager publishing to the same
topic each hold their own `Publisher` (own batcher, own channel) instead of sharing one —
acceptable at moderate parallelism; gRPC channels are multiplexed inside the SDK.

## Topic auto-creation

Under `createDisposition(CreateDisposition.CREATE_IF_NEEDED)` — the default — publishes that
fail with `NOT_FOUND` are recovered reactively on the task thread: the failed messages are
parked per destination, the topic is created with default topic settings, and the messages are
republished under a bounded backoff budget (`recoveryInitialBackoff` doubling to
`recoveryMaxBackoff` over `recoveryMaxAttempts`; by default 500 ms → 10 s, 10 attempts,
~1 minute **per destination**) covering topic-metadata propagation. Existing topics cost nothing: no
admin call is made (and no admin client is even constructed) unless a publish actually fails
with `NOT_FOUND`; when one does, the admin client is short-lived — opened for the creation
call and closed with it.

Creation is idempotent across parallel subtasks: `ALREADY_EXISTS` is treated as success, so
subtasks racing to create the same topic need no coordination. The credentials running the job
need the `pubsub.topics.create` permission (roles/pubsub.editor) on the project when
auto-creation may trigger.

`createDisposition(CreateDisposition.CREATE_NEVER)` disables auto-creation: a `NOT_FOUND`
publish fails the job immediately with a message naming the disposition.

With message ordering enabled, the repair preserves per-key order: after a per-key failure the
SDK publisher pauses the key and cancels its queued publishes in order; those cascade
cancellations are parked behind the parked `NOT_FOUND`, each repair attempt calls
`resumePublish` for the batch's keys before republishing, and the batch is republished in
original arrival order. Cross-key and cross-topic order are unaffected.

Caveats: without ordering keys, repaired messages are republished after later writes may have
published (no guarantee regression — the sink is at-least-once); a repair inside `flush()`
extends the checkpoint duration by up to the backoff budget of each repaired destination;
auto-created topics start with **no subscriptions**, so messages published before a
subscription exists are not retained for anyone — auto-creation suits pipelines whose
consumers create their own subscriptions or attach them promptly (`CREATE_NEVER` restores
fail-fast behavior for pipelines where a missing topic signals a routing bug).

## Error handling

Any terminally failed publish fails the ongoing write or checkpoint: failures captured by
completion callbacks are rethrown on the task thread from the next `write()`/`flush()`
(capture-and-rethrow), and `flush()` awaits every in-flight publish, so a failure can never
slip past a checkpoint barrier — `flush()` also repairs any `NOT_FOUND` failure it discovers
while draining, so a completed checkpoint never leaves messages parked for topic creation.
Publish completion callbacks carry their message (one small callback object per publish) so the
`NOT_FOUND` repair can republish it; the success path still enqueues a per-destination shared
mail. Publish retries within the SDK default to its settings and are tunable through the
publisher options. A per-record failure policy (the `FailedRowHandler` analog of the BigQuery
module) and a fatal-exception classifier are deferred to #37.

## Source

```java
Source<String, ?, ?> source =
        PubSubSource.<String>builder()
                .subscriptions(
                        SubscriptionDestination.of("my-project", "orders"),
                        SubscriptionDestination.of("my-project", "returns"))
                .deserializationSchema(
                        PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                .subscriberOptions(
                        PubSubSubscriberOptions.builder()
                                .flowControlMaxOutstandingElementCount(5_000)
                                .maxAckExtensionPeriod(Duration.ofMinutes(30))
                                .build())
                .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "pubsub");
```

API notes:

- `PubSubDeserializationSchema.deserialize` receives the full `PubsubMessage` — payload,
  attributes, ordering key, message id and publish time are all available — and writes to a
  `Collector`, so one message may produce any number of records. Emitting none drops the message
  (it is still acknowledged). `dataOnly(...)` wraps a plain Flink `DeserializationSchema` for
  payload-only messages.
- The Pub/Sub publish time becomes the record's event timestamp.
- `emulatorEndpoint(host:port)` points the source at a Pub/Sub emulator over a plaintext channel
  with no credentials, so it must only ever be used against an emulator — never against production
  Pub/Sub. Unlike the vendored upstream, the source deliberately does **not** honor the
  `PUBSUB_EMULATOR_HOST` environment variable: a stray value on a task manager would silently
  redirect a production job.

### Subscriber options

`subscriberOptions(PubSubSubscriberOptions)` tunes the SDK subscribers and the reader. Every knob
left unset keeps the SDK's (or the source's) current default — `PubSubSubscriberOptions.defaults()`
is equivalent to not setting options at all.

| Knob | Default when unset |
|---|---|
| `flowControlMaxOutstandingElementCount` / `flowControlMaxOutstandingRequestBytes` | SDK subscriber defaults (1000 messages / 100 MB) |
| `parallelPullCount` | SDK default (one streaming-pull connection) |
| `maxAckExtensionPeriod` | SDK default (1 hour) |
| `minDurationPerAckExtension` / `maxDurationPerAckExtension` | SDK default (adaptive, derived from observed acknowledgement latencies) |
| `shutdownTimeout` (source-owned) | 5 s |
| `maxRecordsPerFetch` (source-owned) | 1000 |
| `awaitAckConfirmation` | unset — acknowledgement is fire-and-forget |
| `firstCheckpointTimeout` (source-owned) | 10 min; `Duration.ZERO` disables the detector |

**Flow control is the real bound on in-flight messages.** Because the source acknowledges only on
checkpoint completion, everything received since the last completed checkpoint counts against these
limits, and the client stops pulling once they are reached. `maxRecordsPerFetch` only caps how much
a single fetch drains from one split; it is not a memory bound. The limit behavior is not exposed
because the SDK subscriber does not expose it either — it forces blocking regardless of the
settings, which for a subscriber means it stops pulling rather than blocking a thread.

**The subscriber shutdown mode is fixed at `NACK_IMMEDIATELY`** and deliberately not a knob. The
SDK's `WAIT_FOR_PROCESSING` default waits for acknowledgements that only arrive at checkpoint
completion — which never happens during shutdown — so it would stall every close. Only
`shutdownTimeout` is configurable, and because a reader closes its splits' subscribers one after
another it is paid per split: keep the total under Flink's `source.reader.close.timeout` (30 s by
default), or a split whose turn never comes leaves its messages to expire instead of being nacked.

**`parallelPullCount` cannot be combined with `orderingMode(PER_KEY)`** — the source builder
rejects it, for the reason given under [Message ordering](#message-ordering): callback
serialization is per streaming-pull connection, so a second connection breaks per-key order.

#### Tuning

Google does not publish recommended flow-control values — the
[flow control documentation](https://cloud.google.com/pubsub/docs/flow-control) says to size the
limits "according to the throughput capacity of your client machines", and the defaults exist as a
safety mechanism against out-of-memory on small subscribers rather than as a throughput setting.
This connector therefore leaves every SDK knob at its SDK default and gives you the levers instead.
Where Google *is* specific:

- **One streaming-pull connection carries about 10 MB/s.** Raise `parallelPullCount` only if one
  split needs more than that, or for resilience — a single stream is a single point of failure, and
  the `subscription/open_streaming_pulls` metric shows how many are open. Note it multiplies the
  parallelism you already have: this source opens one subscriber per split, and
  `splitCount = max(|subscriptions|, parallelism)` under `NONE`, so total streams are
  `splitCount × parallelPullCount`.
- **Lower the flow-control limits if you see duplicate or expired deliveries** — that is Google's
  documented remedy for a subscriber holding more than it can acknowledge in time.

The connector-specific sizing rule is that **acknowledgement waits for a checkpoint**, so
outstanding messages accumulate for a whole checkpoint interval:

```
flowControlMaxOutstandingElementCount ≳ peak messages/s × checkpoint interval
```

Below that, the client stops pulling before each checkpoint completes and throughput is capped by
the checkpoint interval rather than by Pub/Sub. Above it, the limit stops bounding anything.
Raising the limits also raises what a failure replays and what a reader holds in memory, so the
byte limit should stay within the TaskManager's memory budget. `maxAckExtensionPeriod` is the other
half: it must exceed the checkpoint interval by a comfortable margin (see
[Delivery guarantees](#delivery-guarantees)).

### Deserialization failures

`deserializationFailurePolicy(...)` decides what happens to a message the schema cannot convert:

| Policy | Behavior |
|---|---|
| `FAIL` (default) | Fails the job. The message stays unacknowledged, so it is redelivered — a permanently bad message fails the job again after every restart until it is removed or the schema is fixed |
| `DROP` | Discards the message, acknowledging it immediately so it is not redelivered. Counted in `messagesDropped`, and logged at a decreasing rate so a bad batch cannot flood the log |

Either way the failure is counted in Flink's standard `numRecordsInErrors`. `DROP` **drops data**, and
a schema that collected records before failing keeps those — the emitted prefix has already reached
the output and cannot be recalled.

`NACK` (return the message so a subscription dead-letter policy eventually captures it) is
deliberately **not** offered yet: without a dead-letter policy it is an infinite redelivery loop, and
whether the subscription has one is only knowable from `GetSubscription`, which arrives with
subscription auto-creation (#81).

**Nack on emission failure.** If the failure comes from the output rather than from the schema, the
message is fine and the job is about to fail anyway, so it is nacked at once for immediate
redelivery instead of waiting out its acknowledgement deadline. Only *inline* downstream failures
are visible: `SourceOutput.collect` runs the chained operators synchronously, so their exceptions
propagate back into the source — but a failure past a shuffle boundary happens on another task and
cannot be seen. Those messages are covered by the nack the reader performs when it closes.

### Metrics

Registered on the reader and enumerator metric groups:

| Metric | Type | Meaning |
|---|---|---|
| `messagesReceived` | counter | messages handed over by the client library |
| `messagesAcked` | counter | acknowledgements **requested** (see below) |
| `messagesNacked` | counter | messages returned for redelivery |
| `messagesDropped` | counter | messages discarded by `DROP` |
| `pendingAcks` | gauge | messages received or emitted but not yet acknowledged |
| `checkpointsPendingAck` | gauge | checkpoints taken but not yet completed |
| `assignedSplits` / `unassignedReaders` | gauge (enumerator) | splits handed out; readers that got none |
| `numRecordsInErrors` | counter (Flink standard) | deserialization failures |

**`messagesAcked` counts acknowledgements requested, not confirmed.** On an ordinary subscription
the client library sends them asynchronously and does not retry a failure — it logs a warning and
stops. No data is lost (the lease expires and Pub/Sub redelivers, which is the at-least-once
contract), but a *persistent* failure such as a revoked permission becomes a silent reprocessing
loop that this counter will not show. Two ways to see it: set
`awaitAckConfirmation(Duration)` to make each completed checkpoint wait for the server's
confirmation and fail the job on timeout, or watch Cloud Monitoring's
`subscription/oldest_unacked_message_age`, which grows monotonically when acknowledgements stop
landing.

`awaitAckConfirmation` costs latency — the wait happens on the task thread at checkpoint completion
— and **the timeout is the only detector**: on a subscription without exactly-once delivery the
acknowledgement future completes with `SUCCESSFUL` on success and never completes at all on
failure, so there is no error to observe, only the absence of a confirmation.

`pendingRecordsGauge` is deliberately **not** set. Pub/Sub exposes no backlog through the data
plane, and a wrong lag number is worse than none.

### Delivery guarantees

The source is **at-least-once**, and holds no message data in Flink state — delivery state lives on
the Pub/Sub server. A received message passes through four states: *pending* (received, not yet
emitted), *staged* (emitted downstream), *bound to a checkpoint* (that checkpoint is being taken),
and *acknowledged* (that checkpoint completed). Only the last step tells Pub/Sub the message is
done, so a failure at any earlier point leaves it unacknowledged and Pub/Sub redelivers it.
Acknowledging sweeps every checkpoint at or below the completed id, so an aborted checkpoint or a
lost completion notification is healed by the next successful one. Because checkpoints carry no
message data, a restore needs no retained checkpoint.

**Checkpointing must be enabled** in streaming jobs: without it `notifyCheckpointComplete` never
fires, nothing is ever acknowledged, and the source stalls once the client library's flow control
fills. The reader enforces this itself — if no checkpoint has been taken within
`firstCheckpointTimeout` (10 min by default) while messages wait to be acknowledged, it fails the
job with a message naming `execution.checkpointing.interval`. It has to observe the outcome rather
than read the configuration: a reader is handed the *TaskManager* configuration, while
`env.enableCheckpointing(...)` writes into the *job* configuration, so the interval is usually
invisible from inside the source and its absence proves nothing. The check runs from the fetch
loop, not the record path, because the stalled state is precisely the state with no records — once
flow control fills the client stops delivering and nothing would poll again. Raise
`firstCheckpointTimeout(...)` for a job that legitimately checkpoints less often, or set it to
`Duration.ZERO` to switch the detector off.

The checkpoint interval must also stay well under the client library's maximum
acknowledgement-deadline extension (`maxAckExtensionPeriod`, 1 hour by default), or leases expire
and everything is redelivered. The source warns when twice the interval exceeds that budget — but
only when the interval happens to be set at cluster level, for the same visibility reason.

**Nack.** Messages that are pending, staged, or bound to an incomplete checkpoint are **nacked when
the reader closes**, so Pub/Sub redelivers them immediately instead of after the acknowledgement
deadline expires — this is what makes failover recover quickly. The SDK subscriber is additionally
configured with `NACK_IMMEDIATELY` shutdown, which releases messages the client buffered but never
handed to the source; the SDK's `WAIT_FOR_PROCESSING` default would instead wait for
acknowledgements that only arrive at checkpoint completion. Acknowledgement state is scoped per
split, so a split that goes away releases only its own messages. A redelivery arriving before its
predecessor was settled nacks the superseded handle, which is what releases that delivery's
flow-control permit inside the client library.

### Why streaming pull rather than synchronous pull

The source consumes through the client library's high-level `Subscriber`, which uses
StreamingPull. The Apache `flink-connector-gcp-pubsub` instead drives `SubscriberGrpc`'s blocking
stub and issues unary `Pull` calls — a deliberate choice made during its review
([FLINK-9311](https://github.com/apache/flink/pull/6594)), where an earlier `Subscriber`-based
implementation was replaced. The reasons given were that `sourceContext.collect()` blocking under
backpressure naturally stops the pull loop, that users then need not tune flow-control parameters,
and that dropping the intermediate queue lowers both memory footprint and latency.

Two of those reasons are specific to the `SourceFunction` model that connector is built on, where
`run()` is a pull loop and an asynchronous client has to be bridged into it with a hand-built queue
and lock coordination. Under FLIP-27 that bridge is the framework's job: `SplitReader.fetch()` is
already a pull loop, and `SourceReaderBase` already owns the element queue and the backpressure
between the fetcher and the task thread. The remaining reason — that flow control becomes a knob the
user can get wrong, and that it rather than Flink bounds how much is buffered — does apply here, and
is why the subscriber's flow-control settings are exposed rather than hidden.

Two things decided it the other way:

- **Lease extension.** Unary `Pull` hands back acknowledgement ids and nothing else; the Apache
  connector never calls `ModifyAckDeadline`, so every message must be acknowledged within the
  subscription's acknowledgement deadline (600 s at most) and its documentation accordingly
  requires a checkpoint interval well below that deadline. The client library extends leases
  automatically, up to an hour by default, which suits a source that acknowledges on checkpoint
  completion far better.
- **Ordering.** Per-ordering-key sequential dispatch exists only in the high-level client. Building
  `PER_KEY` on unary pull would mean reimplementing it.

The trade-off is real in the other direction too: StreamingPull costs more CPU in gRPC, and the
review thread above measured the synchronous design as checkpoint-frequency bound (3,000 msg/s at a
1 s checkpoint interval, 20,000 msg/s at 50 ms).

### Subscriptions, splits and parallelism

A split is one streaming-pull connection to one subscription, and carries no progress state —
Pub/Sub has no offset to resume from. The split universe is a pure function of the subscription
list, the ordering mode and the source parallelism:

```
splitCount = (orderingMode == PER_KEY) ? |subscriptions| : max(|subscriptions|, parallelism)
split i    -> subscription[i % |subscriptions|],  owner(i) -> i % parallelism
```

Every subscription is therefore consumed by someone, and under `NONE` no subtask sits idle. Because
the assignment is deterministic, a returned split needs no bookkeeping (the restarted subtask is
handed exactly the same splits again) and a restore recomputes the plan from the current
parallelism — so changing parallelism across a restore is safe.

### Message ordering

**The default, `OrderingMode.NONE`, makes no ordering guarantee and is tuned for throughput.** The
split plan gives every subtask at least one split — opening several subscriber clients on the same
subscription when parallelism exceeds the subscription count — and Pub/Sub balances messages across
them. Nothing constrains how many subtasks share a subscription, and no message waits on another.

`orderingMode(OrderingMode.PER_KEY)` preserves per-ordering-key delivery order. It requires
subscriptions created with `enableMessageOrdering`, and it constrains the source in two ways: each
subscription is assigned to exactly one subtask, and its subscriber uses a single streaming-pull
connection.

Both constraints are load-bearing. A subscription consumed by two subtasks is two subscriber
clients, and Pub/Sub's per-key client affinity shifts on reconnect or rebalance. Within one client,
each streaming-pull connection has its **own** message dispatcher, and per-key callback
serialization is per dispatcher — so a second connection would let two messages of one key be
delivered concurrently.

What the source guarantees is **in-order emission per ordering key per subscription**. Preserving
that across the rest of the job requires partitioning by the ordering key, for example
`keyBy(orderingKey)`; a rebalancing shuffle discards it.

#### The cost of ordering

Ordering is off by default because it is expensive, and **most of the cost is Pub/Sub's rather than
this connector's**. Google's [ordering
documentation](https://cloud.google.com/pubsub/docs/ordering) states it directly:

- *"Compared with unordered delivery, ordered delivery decreases publish availability and increases
  end-to-end message delivery latency."*
- Publish throughput is capped at **1 MB/s per ordering key** (a topic can still reach multiple
  GB/s across many keys).
- For pull subscriptions, *"only one batch of messages can be outstanding for an ordering key at a
  time"* — so a key's next batch waits for the current one to be acknowledged.
- *"Unacknowledged messages for a given ordering key can potentially delay delivery of messages for
  other ordering keys"*, so the cost is not confined to the busy key.
- A redelivery re-delivers every subsequent message for that key, acknowledged or not.

Google's mitigation is to *"use the most granular keys that you can"* — throughput per key is
bounded, throughput across keys is not.

On top of that, this source adds two costs of its own:

- **Parallelism is effectively capped at the subscription count.** Surplus subtasks receive no
  splits, are told there are no more, and finish — so they do not hold the watermark back, but they
  do no work either.
- **Acknowledgement waits for a checkpoint.** Combined with one-batch-outstanding, per-key
  throughput is bounded by roughly one batch per checkpoint interval. Ordered jobs therefore want
  short checkpoint intervals — and because unacknowledged messages for one key can delay *other*
  keys, a long interval slows the whole subscription rather than only its hottest key.

Acknowledgement is *not* what gates ordered **dispatch**, though: the client library runs the next
callback for a key once the previous callback **returns**, and this source's callback only appends
to an in-memory buffer. Deferring acknowledgement costs throughput at the service, not a stall in
the client.

## Testing

Unit tests cover the builder/facade, destination identity, the serialization adapters
(data-only, attributes/ordering-key composition), the publisher options (defaults, validation,
SDK settings mapping with a drift guard pinned to the SDK's own retry defaults) and the writer
(fan-out to per-topic publishers, publisher reuse, checkpoint flush draining, async error
capture, backpressure at the in-flight cap, close semantics, the topic auto-creation repair
paths, and the ordering-cascade park/resume/republish paths) against in-memory fakes. Emulator
integration tests (testcontainers `PubSubEmulatorContainer`) run the production publisher
factory and topic admin in their emulator-endpoint mode and cover topic auto-creation
end-to-end, attributes and per-key ordered delivery (including ordering across the auto-creation
repair), publishing under overridden batching settings, dynamic destinations fanning out to
several topics (including auto-creating them), and the checkpoint flush (batching thresholds
set so high that only `flush` can drive delivery, which must also drain the in-flight count —
across repeated write/flush cycles on one writer). A MiniCluster streaming integration test
drives the sink exclusively through the public builder with `emulatorEndpoint(...)`, dynamic
destinations and topic auto-creation under real 1-second checkpoints, asserting complete
delivery. All integration tests run in PR CI without cloud credentials.

On the source side, unit tests cover the split-assignment plan (table-driven over subscription ×
parallelism combinations: every subscription covered, every subtask busy under `NONE`, exactly one
split per subscription under `PER_KEY`, determinism across invocations), the acknowledgement
lifecycle (staging only after emission, the checkpoint sweep healing aborted checkpoints, per-split
nack isolation, superseded redeliveries), the enumerator against a hand-written context, the split
reader against a fake client (multi-split drain, per-fetch caps, split removal, pausing, wake-up,
close aggregating failures), the record emitter, the subscriber options (defaults, validation, SDK
settings mapping with a drift guard pinned to the SDK's own maximum ack-extension default, and the
single-connection forcing that the ordering guarantee rests on) and the missing-checkpoint detector
against a hand-moved clock, the deserialization failure policy (both values, including the records
a partially failing schema already emitted), the nack-on-emission-failure path, the acknowledgement
confirmation (confirmed, rejected and timed out) and the reader's checkpoint/acknowledgement wiring
against a fake reader context, with the enumerator gauges asserted through Flink's own metric
listener. Emulator integration tests run the production
subscriber factory against the emulator and cover the acknowledgement round trip, nack-on-close
producing immediate redelivery, and one reader consuming several subscriptions; a MiniCluster test
drives the source through the public builder over two subscriptions under real checkpoints.

**The emulator cannot verify ordered delivery.** Per-key callback serialization in the client
library is gated on `subscriptionProperties.messageOrderingEnabled` in the streaming-pull response,
which the emulator does not set — probing the client library directly against it shows callbacks
arriving out of order with no Flink involved. The emulator test therefore asserts only that ordered
mode consumes the subscription from a single subtask without stalling on idle ones; end-to-end
per-key order is covered by the real-GCP suite (#82).

