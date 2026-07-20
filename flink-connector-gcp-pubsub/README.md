# flink-connector-gcp-pubsub

Cloud Pub/Sub sink for Apache Flink with dynamic per-record topic destinations.

| Feature | Status |
|---|---|
| SinkV2 at-least-once sink; per-record topic resolution; per-topic publishers; checkpoint flush | Implemented (#18) |
| Topic auto-creation | Implemented (#19) |
| Attributes/ordering-key conveniences; message ordering; batching/flow-control/retry options; recovery and in-flight knobs | Implemented (#20) |
| Per-record failure policy; fatal-exception classifier | Planned (#37) |
| Emulator integration tests | Planned (#21) |
| Pub/Sub source | Planned for v0.2.0 (#31) |

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
flow-control byte limit is the bound the element-count cap cannot provide.

**Caution — flow-control limits with message ordering**: in `google-cloud-pubsub` 1.152.0,
`Publisher.publish` acquires a flow-control permit *before* the paused-ordering-key check, and
the paused-key rejection and per-key cancellation paths never release it (verified in the SDK
source). After a per-key publish failure, leaked permits can therefore permanently shrink — and
with `Block` eventually exhaust — the flow-control budget, hanging the task thread. Avoid
combining flow-control limits with `enableMessageOrdering` until the SDK fixes the leak.

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

## Testing

Unit tests cover the builder/facade, destination identity, the serialization adapters
(data-only, attributes/ordering-key composition), the publisher options (defaults, validation,
SDK settings mapping with a drift guard pinned to the SDK's own retry defaults) and the writer
(fan-out to per-topic publishers, publisher reuse, checkpoint flush draining, async error
capture, backpressure at the in-flight cap, close semantics, the topic auto-creation repair
paths, and the ordering-cascade park/resume/republish paths) against in-memory fakes. Emulator
integration tests (testcontainers `PubSubEmulatorContainer`) cover topic auto-creation
end-to-end, attributes and per-key ordered delivery (including ordering across the auto-creation
repair), and publishing under overridden batching settings; the broader emulator IT matrix is
tracked in #21.

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

Deviations from upstream: dynamic per-record topic destinations with a writer-owned per-topic
publisher map (upstream: single fixed topic with a JVM-wide static publisher cache),
mailbox-based backpressure with an in-flight cap and async error capture (upstream: unbounded
future list), and a hand-written builder following this repository's conventions (upstream:
AutoValue).

[apache/flink-connector-gcp-pubsub](https://github.com/apache/flink-connector-gcp-pubsub) is a
**design reference only** — the mailbox-based backpressure model and the idea of a
fatal-exception classifier (#20) — no code has been copied from it.
