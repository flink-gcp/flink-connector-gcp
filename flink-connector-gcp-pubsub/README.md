# flink-connector-gcp-pubsub

Cloud Pub/Sub sink for Apache Flink with dynamic per-record topic destinations.

| Feature | Status |
|---|---|
| SinkV2 at-least-once sink; per-record topic resolution; per-topic publishers; checkpoint flush | Implemented (#18) |
| Topic auto-creation | Planned (#19) |
| Attributes/ordering-key conveniences; `BatchingSettings`/`FlowControlSettings`/`RetrySettings`; bounded retry policy | Planned (#20) |
| Emulator integration tests | Planned (#21) |
| Pub/Sub source | Planned for v0.2.0 (#31) |

```java
Sink<MyEvent> sink =
        PubSubSink.<MyEvent>builder()
                .destinationResolver(
                        (e, ctx) -> TopicDestination.of("my-project", e.topicName()))
                .serializer(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
                .build();
```

API notes:

- `PubSubSerializationSchema.serialize` returns a full `PubsubMessage`, so message attributes
  and ordering keys are expressible today. Note that ordering keys are **not honored yet**: the
  publisher-level `enableMessageOrdering` flag arrives with the publisher settings in #20 —
  until then, publishing a message that carries an ordering key fails in the client SDK.
  `PubSubSerializationSchema.dataOnly(...)` wraps a plain Flink `SerializationSchema` for
  payload-only messages.
- `DestinationResolver.resolve(element, context)` receives the writer context (event timestamp,
  watermark) so time-based routing is expressible. Resolvers run per record: cache and reuse
  `TopicDestination` instances.
- `TopicDestination` is pure topic identity (`equals`/`hashCode` over project/topic) and serves
  as the key of the writer's per-topic publisher map; publisher settings stay on the sink so
  identity remains stable.

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

**Backpressure.** The number of unacknowledged publishes per writer is capped (1,000). Publish
completions are re-dispatched onto the task mailbox, so all writer state is single-threaded; a
write at the cap yields to the mailbox until completions bring the count back down, bounding
sink memory between checkpoints. This is the mailbox model of the Apache
`flink-connector-gcp-pubsub` writer (a design reference — no code is copied from it); that
writer's infinite republish of non-fatally failed messages under `failOnError=false` is
deliberately **not** adopted. Exposing the cap, and SDK-side flow control, is deferred to #20.

## Publisher lifecycle

Publishers are created lazily per destination topic, owned by the writer, and shut down in the
writer's `close()` (with a bounded 30 s termination wait). This deviates from the vendored
upstream, which caches one `Publisher` per topic JVM-wide and shuts them down only in a JVM
shutdown hook: writer ownership gives a deterministic lifecycle and no cross-job leakage in
shared TaskManagers. The tradeoff: several subtasks on one TaskManager publishing to the same
topic each hold their own `Publisher` (own batcher, own channel) instead of sharing one —
acceptable at moderate parallelism; gRPC channels are multiplexed inside the SDK.

## Error handling

Any terminally failed publish fails the ongoing write or checkpoint: failures captured by
completion callbacks are rethrown on the task thread from the next `write()`/`flush()`
(capture-and-rethrow), and `flush()` awaits every in-flight publish, so a failure can never
slip past a checkpoint barrier. Publish retries within the SDK are its defaults. A per-record
failure policy (the `FailedRowHandler` analog of the BigQuery module), a fatal-exception
classifier and a bounded retry policy are deferred to #20/#37.

## Testing

Unit tests cover the builder/facade, destination identity, the serialization adapter and the
writer (fan-out to per-topic publishers, publisher reuse, checkpoint flush draining, async
error capture, backpressure at the in-flight cap, close semantics) against in-memory fakes.
Emulator integration tests (testcontainers `PubSubEmulatorContainer`) are tracked in #21.

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
