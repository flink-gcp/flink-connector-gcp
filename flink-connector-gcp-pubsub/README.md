# flink-connector-gcp-pubsub

Cloud Pub/Sub sink and source for Apache Flink, with dynamic per-record topic destinations on the
sink and multi-subscription consumption on the source.

| Sink feature | Status |
|---|---|
| SinkV2 at-least-once sink; per-record topic resolution; per-topic publishers; checkpoint flush | Implemented (#18) |
| Topic auto-creation | Implemented (#19) |
| Attributes/ordering-key conveniences; message ordering; batching/flow-control/retry options; recovery and in-flight knobs | Implemented (#20) |
| Per-record failure policy; fatal-exception classifier | Planned (#37) |
| Emulator integration tests | Implemented (#21) |

| Source feature | Status |
|---|---|
| FLIP-27 at-least-once source; multi-subscription splits; checkpoint-bound acknowledgement; nack on close | Implemented (#79) |
| Ordering mode preserving per-ordering-key order | Implemented (#79) |
| Subscriber tuning options; deserialization failure policy; metrics | Planned (#80) |
| Subscription auto-creation; start position (seek) | Planned (#81) |
| Acceptance and real-GCP integration tests | Planned (#82) |

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
fills. The checkpoint interval must also stay well under the client library's maximum
acknowledgement-deadline extension (1 hour by default), or leases expire and everything is
redelivered.

**Nack.** Messages that are pending, staged, or bound to an incomplete checkpoint are **nacked when
the reader closes**, so Pub/Sub redelivers them immediately instead of after the acknowledgement
deadline expires — this is what makes failover recover quickly. The SDK subscriber is additionally
configured with `NACK_IMMEDIATELY` shutdown, which releases messages the client buffered but never
handed to the source; the SDK's `WAIT_FOR_PROCESSING` default would instead wait for
acknowledgements that only arrive at checkpoint completion. Acknowledgement state is scoped per
split, so a split that goes away releases only its own messages. A redelivery arriving before its
predecessor was settled nacks the superseded handle, which is what releases that delivery's
flow-control permit inside the client library.

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

`orderingMode(OrderingMode.PER_KEY)` preserves per-ordering-key delivery order, and is **off by
default** because it is not free. It requires subscriptions created with `enableMessageOrdering`,
and it constrains the source in two ways: each subscription is assigned to exactly one subtask, and
its subscriber uses a single streaming-pull connection.

Both constraints are load-bearing. A subscription consumed by two subtasks is two subscriber
clients, and Pub/Sub's per-key client affinity shifts on reconnect or rebalance. Within one client,
each streaming-pull connection has its **own** message dispatcher, and per-key callback
serialization is per dispatcher — so a second connection would let two messages of one key be
delivered concurrently.

What the source guarantees is **in-order emission per ordering key per subscription**. Preserving
that across the rest of the job requires partitioning by the ordering key, for example
`keyBy(orderingKey)`; a rebalancing shuffle discards it. Two costs to plan for: source parallelism
is effectively capped at the subscription count (surplus subtasks receive no splits, are told there
are no more, and finish, so they do not hold the watermark back), and because Pub/Sub keeps only one
batch outstanding per ordering key while this source defers acknowledgement to checkpoint
completion, per-key throughput is bounded by roughly one batch per checkpoint interval — ordered
jobs want short checkpoint intervals.

Acknowledgement is *not* what gates ordered dispatch: the client library runs the next callback for
a key once the previous callback **returns**, and this source's callback only appends to an
in-memory buffer, so deferring acknowledgements does not stall the key.

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
close aggregating failures) and the record emitter. Emulator integration tests run the production
subscriber factory against the emulator and cover the acknowledgement round trip, nack-on-close
producing immediate redelivery, and one reader consuming several subscriptions; a MiniCluster test
drives the source through the public builder over two subscriptions under real checkpoints.

**The emulator cannot verify ordered delivery.** Per-key callback serialization in the client
library is gated on `subscriptionProperties.messageOrderingEnabled` in the streaming-pull response,
which the emulator does not set — probing the client library directly against it shows callbacks
arriving out of order with no Flink involved. The emulator test therefore asserts only that ordered
mode consumes the subscription from a single subtask without stalling on idle ones; end-to-end
per-key order is covered by the real-GCP suite (#82).

## Provenance and attribution

This module contains code adapted from the Flink connector in
[GoogleCloudPlatform/pubsub](https://github.com/GoogleCloudPlatform/pubsub) (`flink-connector/`,
Apache-2.0, Copyright 2023 Google LLC), as decided on issue #17; the adaptation is also
recorded in the repository-level `NOTICE`. Files retaining substantial upstream code carry the
upstream license header:

- `PubSubSerializationSchema` / `DataOnlySerializationSchema` — the serialization-schema
  contract producing a full `PubsubMessage` (attributes/ordering keys expressible) and the
  data-only adapter, from upstream `PubSubSerializationSchema`
- `PubSubWriter` — the publish/flush core (async publish, `publishAllOutstanding`-then-await
  checkpoint flush), from upstream `PubSubSinkWriter` and `PubSubFlushablePublisher`
- `PubSubDeserializationSchema` / `DataOnlyDeserializationSchema` — the deserialization contract
  and its payload-only adapter
- `AckTracker` / `PubSubAckTracker` — the pending → staged → checkpoint-bound → acknowledged
  lifecycle
- `NotifyingPullSubscriber` / `PubSubNotifyingPullSubscriber` — the streaming-pull-to-buffer bridge
- `PubSubSplitReader`, `PubSubRecordEmitter`, `PubSubSourceReader`, `SubscriptionSplit` — the reader
  stack and the split type

Deviations from upstream, sink side: dynamic per-record topic destinations with a writer-owned
per-topic publisher map (upstream: single fixed topic with a JVM-wide static publisher cache),
mailbox-based backpressure with an in-flight cap and async error capture (upstream: unbounded
future list), and a hand-written builder following this repository's conventions (upstream:
AutoValue).

Deviations from upstream, source side: multi-subscription enumeration with a deterministic split
plan (upstream: one hard-coded subscription, one split per registered subtask — which supports
neither several subscriptions nor ordering, since every subtask then opens its own streaming pull
against the same subscription); an ordering mode; per-split acknowledgement scoping (upstream's
tracker is reader-wide, so closing one subscriber nacks every split's messages); a `Collector`-based
deserialization contract (upstream returns one nullable record and then collects it without a null
check); hand-written serializers (upstream: protobuf code generation); an explicit emulator endpoint
(upstream: `PUBSUB_EMULATOR_HOST` sniffing plus an `emulator:///` URI prefix, where the environment
variable silently overrides an explicitly configured endpoint); and a hand-written builder. The
vendored reader stack also fixes several upstream defects: a `null` user-code class loader passed to
the deserialization schema, a fresh `Configuration` in place of the job's (which made the source
reader options unreachable), draining at most one message per split per fetch, rejecting split
removal, not overriding `pauseOrResumeSplits` (breaking watermark alignment), a missing `return` in
the wake-up branch, and `shutdown()` mutating lock-guarded state without holding the lock.

[apache/flink-connector-gcp-pubsub](https://github.com/apache/flink-connector-gcp-pubsub) is a
**design reference only** — the mailbox-based backpressure model and the idea of a
fatal-exception classifier (#37) — no code has been copied from it.
