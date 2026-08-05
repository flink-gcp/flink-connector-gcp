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
- Returning `null` **skips** the record — it is written nowhere, is not a failure, and never
  reaches the failed-message handler — which is how a filter that depends on the message being
  built belongs in the serializer rather than upstream of the sink. Every serializer in this
  connector family reads `null` that way, and a `null` travels unchanged through
  `withAttributes(...)` and `withOrderingKey(...)`, whose extractors are not called for it.
  A skip is counted by [`recordsSkipped`](#sink-metrics), the only thing that reports it: a
  serializer skipping every record would otherwise leave an empty topic under a green job.
  `dataOnly(...)` cannot skip — Flink's `SerializationSchema` contract has no `null` in it, so a
  `null` payload is reported as a serialization failure instead. The destination is resolved
  *before* the serializer runs, so a record the serializer would skip still needs a resolvable
  topic: a resolver returning `null` for it fails the job.
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
  `PubSubEmulatorContainer`) — never against production Pub/Sub. The value is parsed by the
  setter, so a malformed `host:port` is rejected at `build()` on the client rather than
  surfacing as a connection failure on a task manager
  ([#235]({{< param BookRepo >}}/issues/235)).

## Publisher options

`publisherOptions(PubSubPublisherOptions)` tunes the SDK publishers and the writer. Every knob
left unset keeps the SDK's (or the sink's) current default — `PubSubPublisherOptions.defaults()`
is equivalent to not setting options at all. Every knob and its default is in the
[configuration reference]({{< relref "docs/reference/pubsub" >}}#pubsubpublisheroptions); this
section is why they are what they are.

**The SDK publisher's flow controller is deliberately not exposed** ([#85]({{< param BookRepo >}}/issues/85), revising [#20]({{< param BookRepo >}}/issues/20)). In-flight
publishes are bounded by the writer instead, along both dimensions — see **Backpressure** below.
Two properties made the SDK's version unusable as the sink's byte bound:

- It blocks the task thread rather than yielding to the mailbox, which is what the writer's own
  cap exists to avoid.
- It **cannot be combined with message ordering.** In `google-cloud-pubsub` 1.152.0,
  `Publisher.publish` acquires a flow-control permit *before* the paused-ordering-key check, and
  neither the paused-key rejection nor the per-key cancellation path releases it (verified in the
  SDK source). After a per-key publish failure, leaked permits permanently shrink — and under
  `Block` eventually exhaust — the budget, hanging the task thread with no exception. That left
  ordered sinks, where a paused key holds its whole cascade, with no byte bound at all.

One interaction to size around: a `batchRequestByteThreshold` above `maxInFlightBytes` means a
batch can never fill under the writer cap, so batches leave only on the delay threshold (or
`flush()`). That is latency, not deadlock — the delay alarm always fires — but keep the batch
threshold below the in-flight cap.

## Delivery guarantees and state

The sink is **at-least-once** and the writer is **stateless by design**: records are published
asynchronously through `google-cloud-pubsub` `Publisher` instances (which batch by element
count, bytes and delay), and on **every checkpoint** Flink invokes the writer's `flush()`
(before the barrier is emitted), which sends all messages still buffered inside the SDK
publishers (`publishAllOutstanding`) and blocks until every in-flight publish is acknowledged.
A successful checkpoint therefore means *all* records up to the barrier are persisted by
Pub/Sub — other than those the serializer skipped by returning `null`, which are written nowhere
by design — and the writer stores nothing in Flink state — **discarding operator state
(savepoint-less redeploys, state resets) can never lose sink-buffered data**.

That guarantee assumes the default `FailureHandler.failJob()` policy. Under `logAndDrop()` or
`sendToDeadLetterQueue(...)` a successful checkpoint means every record up to the barrier was
either persisted by Pub/Sub, skipped by the serializer, or handed to the
[failed-message policy](#failed-message-policy), which says which failures reach it.

**FLIP-171 `AsyncSinkBase` was evaluated and rejected** for this sink:

- The Pub/Sub `Publisher` SDK already batches; layering `AsyncSinkWriter`'s
  own batching/buffering on top double-buffers every record. Using AsyncSink idiomatically
  would mean bypassing `Publisher` and driving the raw publish RPC with AsyncSink owning
  batching, backpressure and retries — discarding exactly the SDK behavior [#20]({{< param BookRepo >}}/issues/20) exposes
  (`BatchingSettings`/`RetrySettings` map 1:1 onto `Publisher.Builder`).
- `AsyncSinkWriter` persists unflushed buffers into writer state instead of flushing at the
  barrier, which silently loses those buffers whenever state is dropped. This project
  deliberately chose flush-on-checkpoint statelessness (the BigQuery module records the same
  decision).
- Both models are at-least-once — Pub/Sub has no transactional publish — so rejecting AsyncSink
  forecloses no exactly-once path.

Checkpointing must be enabled for the at-least-once guarantee in streaming jobs: without it,
Flink never calls `flush()` mid-stream, so messages buffered in the SDK publishers are lost on
failure. Batch execution is covered by the end-of-input flush.

**Backpressure.** Unacknowledged publishes per writer subtask are capped along both dimensions
that bound memory: their number (`maxInFlightMessages`, default 1,000) and their serialized size
(`maxInFlightBytes`, default 64 MiB, measured as `PubsubMessage.getSerializedSize()`). Publish
completions are re-dispatched onto the task mailbox, so all writer state is single-threaded; a
write at either cap yields to the mailbox until completions bring the counters back down. This is
the mailbox model of the Apache `flink-connector-gcp-pubsub` writer (a design reference — no code
is copied from it); that writer's infinite republish of non-fatally failed messages under
`failOnError=false` is deliberately **not** adopted.

The byte cap exists because the message count bounds no memory on its own: Pub/Sub allows 10 MiB
per message, so 1,000 in flight is up to ~10 GiB per subtask in the pathological case, and 256 KiB
payloads already reach 256 MiB ([#85]({{< param BookRepo >}}/issues/85)). Publish retries hold a message for up to the 600 s total
retry timeout, so a Pub/Sub partial outage is exactly when the peak is reached.

**Sizing rule:** `maxInFlightBytes` × the sink subtasks sharing a TaskManager must fit that
TaskManager's heap budget, alongside everything else in the job. The 64 MiB default is per subtask,
deliberately below Google's own 100 MB Java-subscriber default, which is per *client*. With the
1,000-message cap, the byte cap binds only above ~64 KiB per message — below that the message cap
still trips first, so small-message pipelines are unaffected by it.

Two bounded ways the byte cap is exceeded, both deliberate:

- Admission is checked *before* a publish, not against the message's own size, so a message larger
  than the cap is published anyway when the writer is empty and overshoots until it completes. A
  "does it fit" predicate would never admit such a message: yielding blocks until a mail arrives,
  and with nothing in flight no mail can arrive, so it would hang the task rather than backpressure
  it.
- A repair republishes its parked batch without re-checking either cap (see below).

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
parked per destination, the topic is created with the configured `TopicCreateOptions` (every
field at its service default when none are set), and the messages are
republished under a bounded backoff budget (`recoveryInitialBackoff` doubling to
`recoveryMaxBackoff` over `recoveryMaxAttempts`; by default 500 ms → 10 s, 10 attempts,
~1 minute **per destination**) covering topic-metadata propagation. Each backoff carries ±25%
jitter — every subtask that parked publishes for the same missing topic resumes against the same
freshly created topic, so unjittered they would republish in lockstep. Existing topics cost
nothing: no admin call is made (and no admin client is even constructed) unless a publish
actually fails with `NOT_FOUND`; when one does, the admin client is short-lived — opened for the
creation call and closed with it.

Creation is idempotent across parallel subtasks: `ALREADY_EXISTS` is treated as success, so
subtasks racing to create the same topic need no coordination. The credentials running the job
need the `pubsub.topics.create` permission (roles/pubsub.editor) on the project when
auto-creation may trigger.

`createDisposition(CreateDisposition.CREATE_NEVER)` disables auto-creation: a `NOT_FOUND`
publish fails the job immediately with a message naming the disposition.

### Topic creation settings

`topicCreateOptions(...)` configures the topics the sink creates ([#153]({{< param BookRepo >}}/issues/153)). Unlike the source's
subscription-creation settings, supplying them is *not* what authorises creation — the disposition
is, because a topic (unlike a subscription) can meaningfully be created with defaults. The settings
are purely additive, and combining them with `CREATE_NEVER` is rejected at graph construction,
since they would configure a topic the sink never creates.

```java
PubSubSink.<String>builder()
        .topic(TopicDestination.of("my-project", "orders-topic"))
        .topicCreateOptions(
                TopicCreateOptions.builder()
                        .messageRetention(Duration.ofDays(7))
                        .kmsKeyName("projects/p/locations/l/keyRings/r/cryptoKeys/k")
                        .allowedPersistenceRegions(List.of("europe-west1", "europe-west4"))
                        .enforceInTransit(true)
                        .build())
        .serializer(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
        .build();
```

Knobs: `messageRetention` (topic-level retention, acknowledged or not — what lets a subscription
created *later*, or a backwards seek, reach messages published before it existed),
`kmsKeyName` (customer-managed encryption; the key must exist and the Pub/Sub service account
needs encrypt/decrypt on it, or publishes to the created topic fail) and the message storage
policy — `allowedPersistenceRegions` plus `enforceInTransit`, which requires the regions. Every
knob is optional, and unset leaves Pub/Sub's own default. **One options object applies to every
topic the sink creates**: with dynamic destinations each missing topic is created with the same
settings, because unlike a subscription's topic binding, nothing in them ties them to one topic.
As on the source side, creation is not an update — an existing topic keeps its own settings, and
these are neither applied to it nor compared against it.

Considered and declined: `schemaSettings` — topic-side validation would re-check what this sink
itself serialized, the schema resource is provisioned out of band anyway, and it changes nothing
for a subscriber (validation happens at publish time only; subscribers just see the
`googclient_schema*` attributes). Its payoff accrues to GCP-managed consumers (a BigQuery export
subscription deriving columns from the topic schema, say), not to the Flink pipeline — and
supporting it would not end at creation, because Pub/Sub's schema evolution is its own constrained
machinery (single-file Avro or Protocol Buffer definitions, a bounded revision range per topic
managed through topic updates). Also declined: `labels`/`tags`, unexposed on the subscription side
too. All are additive if a need with a real consumer appears.

The emulator stores all four knobs verbatim and returns them on `GetTopic`, so the emulator ITs
verify the settings reach the created topic — but it validates nothing (a KMS key that does not
exist is accepted) and cannot show the settings' *effect*: actual CMEK encryption, residency
enforcement, and retention-driven replay belong to the real-GCP suite
([#82]({{< param BookRepo >}}/issues/82)).

With message ordering enabled, the repair preserves per-key order: after a per-key failure the
SDK publisher pauses the key and cancels its queued publishes; those cascade cancellations are
parked alongside the failure that caused them — a `NOT_FOUND`, or a message the
[failed-message policy](#failed-message-policy) dropped — each repair attempt calls `resumePublish`
for the batch's keys before republishing, and the batch is republished in **publish order**.
Cross-key and cross-topic order are unaffected.

Publish order is recovered by sorting the parked batch on a per-writer publish sequence, not by
the order the failures are observed in. The SDK cancels queued publishes from its own thread, so a
cascade can be reported *before* the failure that caused it — anything derived from that
observation order, including deciding whether to park a cascade based on whether something is
parked already, is a race ([#78]({{< param BookRepo >}}/issues/78)). One consequence is worth knowing: since a cancellation is never
itself a root cause, one is always parked for repair — whatever the create disposition — rather
than failing the job. Only a parked `NOT_FOUND` makes the repair create a topic, so `CREATE_NEVER`
still never creates one; the disposition decides that directly rather than by refusing to park.

That separation is what lets one repair serve a second root cause: a message the
[failed-message policy](#failed-message-policy) dropped. The SDK pauses an ordering key on *any*
non-retryable failure without inspecting it, and never resumes one by itself, so a dropped keyed
message hands its key to the next repair — which resumes it and republishes the messages queued
behind it, creating no topic.

The repair republishes its parked batch **without re-checking the in-flight caps**, so both
counters can transiently exceed them by one destination's batch size. This is bounded — the parked
messages were themselves admitted under the caps, and the repair drains the writer to empty before
republishing — and it is the cheaper of the two options: yielding between the batch's publishes
would run failure mails in the middle of a key's republish and reorder it, reintroducing the
hazard the publish-sequence sorting exists to close. Parked messages are counted by neither cap
while parked; their failure mail released them, and they are the same objects the repair
republishes, so nothing is hidden from the accounting.

Caveats: without ordering keys, repaired messages are republished after later writes may have
published (no guarantee regression — the sink is at-least-once); a repair inside `flush()`
extends the checkpoint duration by up to the backoff budget of each repaired destination;
auto-created topics start with **no subscriptions**, so messages published before a
subscription exists are not retained for anyone — unless the creation settings set
`messageRetention`, which makes the topic itself keep them — so auto-creation without it suits
pipelines whose consumers create their own subscriptions or attach them promptly
(`CREATE_NEVER` restores fail-fast behavior for pipelines where a missing topic signals a
routing bug).

## Error handling

Any terminally failed publish fails the ongoing write or checkpoint: failures captured by
completion callbacks are rethrown on the task thread from the next `write()`/`flush()`
(capture-and-rethrow), and `flush()` awaits every in-flight publish, so a failure can never
slip past a checkpoint barrier — `flush()` also repairs anything it discovers while draining, so a
completed checkpoint never leaves messages parked for republish, nor an ordering key paused by a
dropped message.
Publish completion callbacks carry their message (one small callback object per publish) so the
`NOT_FOUND` repair can republish it, plus its serialized size so both in-flight counters can be
released; the callback *is* the success mail, so the success path allocates nothing beyond it.
Publish retries within the SDK default to its settings and are tunable through the
publisher options.

Failed publishes are classified on the task thread and routed by class:

| Class | Examples | Behavior |
|---|---|---|
| Message-level | `INVALID_ARGUMENT` — the message is over the size limit, its attributes break a limit, its ordering key is unusable | A candidate verdict, confirmed before it is acted on: `Publish` is a batch RPC that rejects all-or-nothing, so the status reaches every co-batched message. The sink republishes the failed batch one message per request and routes only the messages rejected individually to the configured [failed-message handler](#failed-message-policy) — co-batched neighbours are published, not dropped. With ordering on, the messages queued behind a rejected one on its key are cancelled by the SDK and republished by the same repair (see [Ordering and a dropping policy](#ordering-and-a-dropping-policy)) |
| Topic not found | `NOT_FOUND` | Under `CREATE_IF_NEEDED` the topic is created and the message republished (see [Topic auto-creation](#topic-auto-creation)). Under `CREATE_NEVER` the job fails |
| Cancellation | The SDK cancelling an ordering key's queued publishes after an earlier failure for that key | Never a root cause. With ordering enabled it is parked alongside the failure that caused it — a `NOT_FOUND`, or a message the handler dropped — and republished; with ordering disabled it fails the job |
| Terminal | An outage the SDK's own retries gave up on (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, …), `PERMISSION_DENIED`, failures carrying no status at all | Fail the ongoing write or checkpoint |

### Failed-message policy

Two data-shaped failures are pluggable: a record the serializer rejects, and a message-level
publish rejection. A record the serializer *skips* by returning `null` is neither: it is not a
failure, so it never reaches the handler and is counted by
[`recordsSkipped`](#sink-metrics) rather than `numRecordsSendErrors`. The policy is
`failedMessageHandler(...)`, taking the shared
`FailureHandler<FailedMessage>` SPI from `flink-connector-gcp-base`
([#37]({{< param BookRepo >}}/issues/37) standardizes it across the connectors in this
repository):

```java
Sink<String> sink =
        PubSubSink.<String>builder()
                .topic(TopicDestination.of("my-project", "events"))
                .serializer(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
                .failedMessageHandler(FailureHandler.logAndDrop())
                .build();
```

- `FailureHandler.failJob()` (default) — every per-message failure fails the checkpoint, which is
  the sink's behavior when nothing is configured
- `FailureHandler.logAndDrop()` — logs each failed message at WARN and drops it
- `FailureHandler.sendToDeadLetterQueue(...)` — forwards each failed message to a `DeadLetterQueue`
  (experimental), whose implementation the sink drives through a lifecycle: `open(context)` once
  when the writer is created (the context carries the subtask index and the writer's metric group),
  `offer(element)` per failed message — buffering is allowed — `flush()` at every checkpoint
  barrier and at end of input, always after the sink's own write path has drained (on return
  everything offered must be durable, throwing fails the checkpoint), and `close()` when the writer
  closes, which must not be relied on for persistence
- Custom handlers implement `FailureHandler<FailedMessage>` — or `FailureHandler<FailedElement>`,
  which `failedMessageHandler(...)` accepts as-is (the parameter is contravariant), so one handler
  written against the shared contract serves every connector in this repository. Throwing from
  `handle` fails the checkpoint, returning drops the message. `FailedMessage` carries the
  `PubsubMessage` the serializer produced, or `null` when serialization itself failed; under the
  shared `FailedElement` contract it reports `getConnector()` (`"pubsub"`),
  `describeDestination()` (`projects/<p>/topics/<t>`) and `getPayloadBytes()` — the **whole**
  serialized message, so a consumer recovers the attributes and the ordering key with
  `PubsubMessage.parseFrom(bytes)`

**Only those two failures are routed, deliberately.** An outage must not reach a dropping handler,
or a service incident would bleed the stream one message at a time instead of backpressuring and
restarting; that is why the message-level class is `INVALID_ARGUMENT` alone and is widened only
with evidence that a status code identifies one message rather than a condition. Configuration
failures stay fatal for the mirror-image reason: a destination resolver returning `null`, and a
message carrying an ordering key without `enableMessageOrdering(true)`, fail every record alike, so
dropping them would leave an empty topic under a green job.

That reasoning does not extend to a serializer that produces an *invalid message* for every
record — a bug that puts a malformed attribute or an over-long ordering key on all of them.
Pub/Sub rejects each one individually, the sink cannot tell a systematic rejection from a
per-message one (the classification is the response's status code, not a judgement about the
whole stream), and a dropping policy discards the lot silently. Watch
[`numRecordsSendErrors`]({{< relref "docs/connectors/datastream/pubsub" >}}#sink-metrics) rather
than the job status when running anything other than `failJob()`: it counts every message the
handler received, so a systematic rejection shows up as a rate rather than as a failure.

One thing the classification alone cannot see, and how the sink closes it: `Publish` is a
**batch** RPC, so an `INVALID_ARGUMENT` is a *request*-level status that the SDK reports against
every message in the batch — measured on real Pub/Sub
([#264]({{< param BookRepo >}}/issues/264)): the rejection is all-or-nothing, every co-batched
future carries the same throwable, and nothing in the error names the offending message. The sink
therefore treats such a report as a candidate verdict only. The failed batch is parked and
republished **one message per request**, each message earns its own verdict, and only the
individually rejected ones reach the handler — so `numRecordsSendErrors` counts true rejections,
not batch fan-out. The cost is one request per message of a failed batch, on the failure path only
— and the confirming republish is strictly serial, one round trip at a time, so a stream whose
every message is invalid degrades to one round trip per message while it lasts. (An *oversized* message under the default batching settings never needed this: the SDK
sends an element exceeding `batchRequestByteThreshold` as its own request, so only messages under
that threshold — attribute violations and the like — ever share a rejection.)

#### Ordering and a dropping policy

A dropping policy and `enableMessageOrdering(true)` work together, and what a drop means on an
ordered stream is worth being precise about.

**The survivors of the key keep their relative order.** When Pub/Sub rejects a keyed message the
SDK publisher pauses that ordering key and cancels every publish queued behind it — on *any*
non-retryable failure, without inspecting it, and it never resumes a key by itself. So a drop
leaves work behind, and the sink does it: the key is handed to the same repair the missing-topic
path uses, which resumes it and republishes the cancelled messages in publish order. `flush()`
repairs until nothing is pending, so **no checkpoint completes with an ordering key left paused**.

**The dropped message leaves a gap that a consumer cannot distinguish from a lost message.** That
is inherent to dropping — the sink cannot fill a hole in a sequence it does not retain — and it
matters more here than on an unordered topic, because a consumer of an ordered stream is more
likely to be a state machine a gap corrupts. The dead-letter record is the only place the gap is
written down, and it is enough to close it: `getPayloadBytes()` is the whole serialized message, so
a consumer recovers the ordering key with `PubsubMessage.parseFrom(bytes)` and can replay what was
dropped.

If that trade is not acceptable for a given topic, `failJob()` — the default — is the policy that
never leaves a gap.

**The recovery budget bounds unproductive retrying, not the length of a poisoned key.** Draining
a key whose messages are rejected one after another happens through the same repair and the same
`recoveryMaxAttempts` budget as a topic-creation republish — but the one-message-per-request
republish gives every parked message its own verdict within a single attempt, and the key a drop
pauses is handed back before its next message, so a run of consecutively invalid messages drains
in one attempt however long it is ([#269]({{< param BookRepo >}}/issues/269)). What the budget
still bounds is a repair making no progress: topic metadata that never propagates, or a key whose
republishes keep failing without a verdict. When it runs out, the failure message says which
happened — `kept failing` (a republish that never got through, `after creating the topic` when
the repair created one) or `could not drain its parked messages within the recovery budget`, with
the number of messages that were handed to the failure handler during the repair and, when both
facts hold, the creation too.

Dead-letter output is **at-least-once, for failures that recur on replay**: messages are offered
before the checkpoint covering their originating records completes, so a restart replays those
records and a deterministic failure (an oversized message, a record the serializer cannot convert)
is offered again — consume the dead-letter destination idempotently or deduplicate by key. A
failure that does *not* recur on replay is preserved only if a completed checkpoint already flushed
it. Exactly-once dead-letter output is deliberately not offered: it would require the dead-letter
write to join the sink's own commit protocol, which no external destination can be enrolled in.

**This is not Pub/Sub's own dead-lettering.** The handler above is a *sink-side* policy: this
connector decides that a message it is publishing has terminally failed, and hands it to your
handler. Pub/Sub's dead-letter topics are a *service-side* feature on the subscribe side, used by
this connector's source under
[`deserializationFailurePolicy(NACK)`](#deserialization-failures) — and they trigger on
**delivery count, not cause**, so a redelivery after an unrelated job restart raises the same
counter as one after a nack. The two are configured separately, route to different places, and
neither substitutes for the other.

### Dead-lettering to a Pub/Sub topic

`PubSubDeadLetterQueue` is this repository's one shipped `DeadLetterQueue` implementation
(experimental, [#211]({{< param BookRepo >}}/issues/211)). It publishes each failed element to a
Pub/Sub topic, and it sees failures through the shared `FailedElement` contract — so **one instance
serves every connector here**, not only this one:

```java
PubSubSink.<String>builder()
        .topic(TopicDestination.of("my-project", "events"))
        .serializer(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
        .failedMessageHandler(
                FailureHandler.sendToDeadLetterQueue(
                        PubSubDeadLetterQueue.builder()
                                .topic(TopicDestination.of("my-project", "dead-letters"))
                                .build()))
        .build();
```

| Attribute | Value |
|---|---|
| `dlq-connector` | `bigquery`, `pubsub` or `cloudtasks` |
| `dlq-destination` | the resource the element was bound for |
| `dlq-error` | the failure description, truncated to Pub/Sub's 1024-byte attribute-value limit and marked with `...` |
| `dlq-timestamp` | when the element was offered, ISO-8601 |
| `dlq-subtask` | the offering sink subtask's index |

The message **data** is the element's payload bytes — empty when serialization itself failed, which
is how a consumer tells the two apart. The failure's cause chain is not in the envelope (it has no
bounded string form); enable `DEBUG` logging on `PubSubDeadLetterQueue` to see untruncated errors in
the job logs.

Publishes are batched and awaited in `flush()`, so a rare failure costs no round trip of its own.
`maxOutstandingMessages` bounds what one checkpoint interval can accumulate when *every* record
fails — the default is 1000, `0` publishes each element synchronously (the narrowest loss window,
one round trip per element) and `-1` buffers until the flush. The topic must already exist: this
queue never creates one, because a dead-letter destination created on the fly is one nothing is
consuming.

## Sink metrics

Registered on the sink writer's metric group, one set per subtask:

| Metric | Type | Meaning |
|---|---|---|
| `numRecordsSend` | counter (Flink standard) | records handed to the client library for publishing |
| `numBytesSend` | counter (Flink standard) | their serialized size |
| `numRecordsSendErrors` | counter (Flink standard) | records routed to the failed-message handler |
| `recordsSkipped` | counter | records the serializer skipped by returning `null` — neither sent nor failed, and not broken down per topic |
| `inFlightMessages` | gauge | publishes not yet acknowledged |
| `inFlightBytes` | gauge | their serialized size, against `maxInFlightBytes` |
| `parkedMessages` | gauge | messages held for a destination's next republish — after a missing topic, after an ordering key was paused by a dropped message, or a batch awaiting the one-message-per-request republish that confirms a rejection |
| `topicsCreated` | counter | completed topic-creation repairs under `CREATE_IF_NEEDED` (see below) |
| `errorClass.CODE.errors` | counter | failed publishes by status code, `CODE` being a gRPC status name or `UNCLASSIFIED` |
| `destination.TOPIC.recordsSend`, `destination.TOPIC.sendErrors` | counter | the same two counts per topic, **only** with `perDestinationMetrics(true)` |

**`numRecordsSend` counts records, not publish attempts.** A message a repair republishes is counted
once, when the client first accepted it, so a job recovering from a missing topic — or from an
ordering key paused by a dropped message — does not report itself as a busier one. Every connector
in this repository counts the same way, whether its retries live in the sink or inside the SDK, so
the number is comparable across them. The consequence to know: `numBytesSend` is payload volume
rather than wire volume — a record republished three times moved three times its size across the
network. Retry volume is what `errorClass.CODE.errors` measures, and it measures it per status code.

**`numRecordsSendErrors` is the counter to watch when the handler is not `failJob()`.** It counts
exactly what reached `failedMessageHandler(...)` — a record the serializer rejected, and a publish
the service answered `INVALID_ARGUMENT` on its own single-message request — whether the handler then
dropped the message or failed the job. A serializer bug that makes *every* message invalid is dropped one at a time under a dropping
policy, and this counter is what shows it while the job stays green.

**`topicsCreated` counts repairs, not distinct topics.** A creation that answers `ALREADY_EXISTS`
— a parallel subtask got there first — is a success, so one new topic is counted once by every
subtask that had to repair for it. It answers "how often did a missing topic stall this subtask",
which is what a reader of this sink's auto-creation behaviour wants; it is not an inventory. Only a
repair that saw a `NOT_FOUND` is counted: a repair triggered by a dropped message's ordering key
creates nothing and increments nothing.

`errorClass` counts **root** failures only. With message ordering enabled the SDK cancels an ordering
key's queued publishes after that key's first failure; those cascades are not counted, since they
carry no status of their own and would multiply one incident by the length of the key's queue. The
failure that caused them is counted. A request-level `INVALID_ARGUMENT` reported against a
co-batched message is excluded for the same multiplication — the SDK sets the one status on every
future of the batch — so what is counted is the solo rejection the isolation republish confirms,
one per genuinely invalid message.

**`perDestinationMetrics` is off by default, and should stay off for dynamic destinations.** Flink
cannot unregister a metric, so every topic the job has ever written to keeps its counters for the
lifetime of the task — and, for the same reason, a topic whose writer state was evicted and later
rebuilt resumes its old counters rather than restarting at zero. Switch it on when the topic set is
small and known; the option is in
[`PubSubPublisherOptions`]({{< relref "docs/reference/pubsub" >}}#pubsubpublisheroptions).

`currentSendTime` is deliberately **not** set. The SDK batches publishes and completes their futures
asynchronously, so any latency this writer could report would measure its own bookkeeping rather
than the service's response time — a missing number beats a wrong one. There is no committer here
either (the sink is single-phase), so Flink's committer metrics do not apply.

**The SDK contributes nothing to these numbers**, measured against `google-cloud-pubsub` 1.152.0
(libraries-bom 26.85.1): `Publisher` exposes no metric or statistics accessor, and its only telemetry
surface is `setEnableOpenTelemetryTracing`/`setOpenTelemetry`, which emits **spans** — not meters —
into an OpenTelemetry instance a Flink job need not have configured. A Kafka-style passthrough of
client-native metrics therefore has nothing to read. The connector leaves that tracing switch alone;
a job that wants publish spans configures OpenTelemetry itself.

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
  redirect a production job. As on the sink, the endpoint is parsed by the setter and a malformed
  `host:port` is rejected at `build()` ([#235]({{< param BookRepo >}}/issues/235)).

### Subscriber options

`subscriberOptions(PubSubSubscriberOptions)` tunes the SDK subscribers and the reader. Every knob
left unset keeps the SDK's (or the source's) current default — `PubSubSubscriberOptions.defaults()`
is equivalent to not setting options at all. Every knob and its default is in the
[configuration reference]({{< relref "docs/reference/pubsub" >}}#pubsubsubscriberoptions); this
section is why they are what they are.

**Flow control is the real bound on in-flight messages.** Because the source acknowledges only on
checkpoint completion, everything received since the last completed checkpoint counts against these
limits, and the client stops pulling once they are reached. `maxRecordsPerFetch` only caps how much
a single fetch drains from one split; it is not a memory bound. The limit behavior is not exposed
because the SDK subscriber does not expose it either — it forces blocking regardless of the
settings, which for a subscriber means it stops pulling rather than blocking a thread.

**The subscriber shutdown mode is fixed at `NACK_IMMEDIATELY`** and deliberately not a knob. The
SDK's `WAIT_FOR_PROCESSING` default waits for acknowledgements that only arrive at checkpoint
completion — which never happens during shutdown — so it would stall every close. Only
`shutdownTimeout` is configurable, and it bounds a reader's whole close rather than each split's:
the reader nacks every split's messages and asks every client to stop before it waits on any, so
the waits overlap however many splits it owns. Keep it under Flink's `source.reader.close.timeout`
(30 s by default).

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

```text
flowControlMaxOutstandingElementCount ≳ peak messages/s × checkpoint interval
```

Below that, the client stops pulling before each checkpoint completes and throughput is capped by
the checkpoint interval rather than by Pub/Sub. Above it, the limit stops bounding anything.
Raising the limits also raises what a failure replays and what a reader holds in memory, so the
byte limit should stay within the TaskManager's memory budget. `maxAckExtensionPeriod` is the other
half: it must exceed the checkpoint interval by a comfortable margin (see
[Delivery guarantees](#delivery-guarantees)).

### Startup check

Before it assigns a single split, the enumerator describes every configured subscription and refuses
to start on one the source cannot consume:

- **`orderingMode(PER_KEY)` against a subscription without message ordering.** The setting is fixed
  at creation, and Pub/Sub only preserves ordering-key order on ordering-enabled subscriptions —
  without this check the job would run and quietly deliver unordered messages.
- **Exactly-once delivery.** Its acknowledgement ids are invalidated on redelivery and expire with
  the acknowledgement deadline, while this source holds them for a whole checkpoint interval.
- **`deserializationFailurePolicy(NACK)` on a subscription with no dead-letter policy** (see
  [Deserialization failures](#deserialization-failures)).

Nothing is verified and then acted on in one pass: every subscription is resolved and checked before
any of them is sought, so a rejection cannot leave an earlier subscription already rewound.

The check runs asynchronously, so it never blocks the coordinator thread; readers that register while
it is in flight wait for it to finish. That fence is also what keeps a subscriber from attaching to a
subscription mid-seek. These are start-time snapshots, not invariants: flipping a subscription's
settings under a running job is not noticed.

The job manager's credentials need `pubsub.subscriptions.get` on every configured subscription
(roles/pubsub.viewer), plus `create` when auto-creating and `update` when seeking —
roles/pubsub.editor covers all three.

### Subscription auto-creation

Passing creation settings alongside a subscription is what authorises creating it; a subscription
added without them must already exist, and the job fails at startup naming the option if it does not.
There is no separate disposition enum because there is no meaningful "create with defaults": a
subscription without a topic is not a subscription, and only you know which topic to bind.

```java
PubSubSource.<String>builder()
        .subscription(
                SubscriptionDestination.of("my-project", "orders"),
                SubscriptionCreateOptions.builder()
                        .topic(TopicDestination.of("my-project", "orders-topic"))
                        .ackDeadline(Duration.ofSeconds(60))
                        .retainAckedMessages(true)
                        .build())
        // No options: this one must already exist.
        .subscription(SubscriptionDestination.of("my-project", "returns"))
        .deserializationSchema(PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
        .build();
```

**Settings are per subscription because they carry the topic binding.** One options object shared by
several subscriptions would bind them all to the same topic, and Pub/Sub delivers a complete copy of
a topic's stream to every subscription of it — so the source would emit each message once per
subscription, with nothing anywhere reporting an error.

Knobs: `topic` (required), `ackDeadline`, `enableMessageOrdering`, `messageRetention`,
`retainAckedMessages`, `expirationTtl` / `neverExpire`, `deadLetterPolicy` and `filter`. Every one
but the topic is optional, and unset leaves Pub/Sub's own default. `enableExactlyOnceDelivery` is
deliberately absent — the startup check rejects it, so offering it would only let you create a
subscription the source then refuses. The builder likewise rejects, at graph construction, creation
settings that the check would reject once used: ordering left off under `orderingMode(PER_KEY)`, and
no dead-letter policy under `deserializationFailurePolicy(NACK)`. Both are fixed at creation, so
catching them here is what stops the source creating a subscription it then refuses to consume.

Creation is idempotent: `ALREADY_EXISTS` counts as success, so two jobs racing to create the same
subscription need no coordination. It is **not** an update — an existing subscription keeps its own
settings, and these are neither applied to it nor compared against it. Existing subscriptions cost
one `GetSubscription` each at startup and nothing after.

Caveat: a subscription only retains messages published **after** it exists, so a job that
auto-creates one starts from an empty backlog no matter what was published before.

### Start position

`startPosition(...)` decides where the source begins:

| Position | Behavior |
|---|---|
| `continueFromSubscription()` (default) | Starts wherever the subscription already is. The only position that issues no seek |
| `earliestRetained()` | Replays the whole retained backlog |
| `latest()` | Discards the existing backlog, starting from messages published after the job starts |
| `fromTimestamp(Instant)` | Marks everything published before the instant acknowledged, everything after unacknowledged |

How far back a backwards position reaches is a property of the subscription, not of this setting:
already-acknowledged messages are replayable only if the subscription has `retainAckedMessages` or
its topic retains messages. Against a subscription with neither, a backwards seek recovers only what
was never acknowledged — the startup check warns when it sees that combination. Pub/Sub also applies
a seek asynchronously; deliveries already in flight can take up to a minute to reflect it.

**A seek rewrites state shared by every consumer of the subscription, including other jobs.** Any
non-default start position wants a subscription the job owns.

**The seek runs once, at the first start of a job, and never on a restore** — the enumerator records
that it ran in its checkpointed state, so a failover resumes instead of rewinding. Two consequences
worth planning around:

- **A redeploy without a savepoint seeks again**, because the state that remembered it is gone. So
  does a job that crash-loops before its first checkpoint completes.
- **`latest()` is the one position that is not reproducible.** It resolves against the clock at the
  moment the seek runs, so a failover before any split is assigned resolves it again, to a later
  instant, discarding whatever was published in between. Nothing already emitted is affected — the
  enumerator assigns no split until the check completes — but use `fromTimestamp(...)` when the
  boundary has to be exact.

### Deserialization failures

`deserializationFailurePolicy(...)` decides what happens to a message the schema cannot convert:

| Policy | Behavior |
|---|---|
| `FAIL` (default) | Fails the job. The message stays unacknowledged, so it is redelivered — a permanently bad message fails the job again after every restart until it is removed or the schema is fixed |
| `DROP` | Discards the message, acknowledging it immediately so it is not redelivered. Counted in `messagesDropped`, and logged at a decreasing rate so a bad batch cannot flood the log |
| `NACK` | Returns the message for redelivery and carries on, leaving it to the subscription's dead-letter policy. Counted in `messagesNacked`, logged at the same decreasing rate |

Whichever is chosen, the failure is counted in Flink's standard `numRecordsInErrors`. `DROP` **drops
data**, and a schema that collected records before failing keeps those under both `DROP` and `NACK` —
the emitted prefix has already reached the output and cannot be recalled, so a `NACK`ed message is
both partially emitted and redelivered in full.

**`NACK` requires a dead-letter policy on every subscription**, which the startup check enforces:
nacking does not fail the job, so without one a message the schema can never convert is redelivered
forever, invisibly. Note that Pub/Sub dead-letters on **delivery count, not cause** — a redelivery
after an unrelated job restart raises the same counter — so set the subscription's delivery-attempt
limit high enough that ordinary failovers do not dead-letter healthy messages. Pub/Sub also needs its
own service account granted publish on the dead-letter topic and subscribe on the subscription;
without those grants it silently keeps redelivering.

For anything richer than these three, deserialize permissively instead: the schema receives the whole
`PubsubMessage` and writes to a `Collector`, so it can emit a bad-record variant rather than throwing
(and emit nothing to drop). Splitting that downstream with a side output puts the dead-letter write
inside the pipeline, where it is checkpointed and rescalable — which a handler doing its own I/O on
the task thread would not be. A source-side failure-handler SPI was considered and rejected for that
reason; cross-connector dead-lettering is [#37]({{< param BookRepo >}}/issues/37).

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
| `pendingCheckpoints` | gauge | checkpoints taken but not yet completed |
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
flow control fills the client stops delivering and nothing would poll again. The budget is measured
from the reader's first split assignment, not from when the reader is created: a reader that has
been given no subscription yet has nothing to checkpoint, so counting that time against it would
report a missing checkpoint on a job that is checkpointing normally. Raise
`firstCheckpointTimeout(...)` for a job that legitimately checkpoints less often, or set it to
`Duration.ZERO` to switch the detector off.

The budget is spent only once, and only against a job's *first* checkpoint. The reader reports every
checkpoint barrier — one that carries no data, or that reaches a reader owning no subscription,
counts just as much — so a job that checkpoints at all retires the detector on its first barrier and
is never measured again. Combined with the outstanding-message condition, that leaves two ways to
see this failure: checkpointing really is off, or the first checkpoint takes longer than the budget
while messages are already in flight.

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

```text
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
capture, backpressure at both in-flight caps — including that the byte cap trips with the message
count far below its own, that an oversized message is still admitted rather than hanging, that a
zero-byte message still counts as in flight for the drain, and that the repair is exempt — close
semantics, the topic auto-creation repair paths, and the
ordering-cascade park/resume/republish paths) against in-memory fakes. The two writer test classes
carry a class-level timeout: the fake mailbox blocks on an empty queue exactly as the real one
does, so a broken in-flight predicate hangs rather than fails, and the timeout turns that back
into a test failure. Emulator
integration tests (testcontainers `PubSubEmulatorContainer`) run the production publisher
factory and topic admin in their emulator-endpoint mode and cover topic auto-creation
end-to-end (a fully-populated `TopicCreateOptions` reading back field for field off the created
topic — from the SQL DDL too — with the option-to-protobuf translation also unit-tested on its
own), attributes and per-key ordered delivery (including ordering across the auto-creation
repair), publishing under overridden batching settings, dynamic destinations fanning out to
several topics (including auto-creating them), and the checkpoint flush (batching thresholds
set so high that only `flush` can drive delivery, which must also drain the in-flight count —
across repeated write/flush cycles on one writer). A MiniCluster streaming integration test
drives the sink exclusively through the public builder with `emulatorEndpoint(...)`, dynamic
destinations and topic auto-creation under real 1-second checkpoints, asserting complete
delivery. All of these integration tests run in PR CI without cloud credentials; the sink's one
credential-gated class is part of the real-GCP suite described below.

On the source side, unit tests cover the split-assignment plan (table-driven over subscription ×
parallelism combinations: every subscription covered, every subtask busy under `NONE`, exactly one
split per subscription under `PER_KEY`, determinism across invocations), the acknowledgement
lifecycle (staging only after emission, the checkpoint sweep healing aborted checkpoints, per-split
nack isolation, superseded redeliveries), the enumerator against a hand-written context, the split
reader against a fake client (multi-split drain, per-fetch caps, split removal, pausing, wake-up,
close aggregating failures), the record emitter, the subscriber options (defaults, validation, SDK
settings mapping with a drift guard pinned to the SDK's own maximum ack-extension default, and the
single-connection forcing that the ordering guarantee rests on) and the missing-checkpoint detector
against a hand-moved clock, the deserialization failure policy (all three values, including the
records a partially failing schema already emitted), the nack-on-emission-failure path, the
acknowledgement confirmation (confirmed, rejected and timed out) and the reader's
checkpoint/acknowledgement wiring against a fake reader context, with the enumerator gauges asserted
through Flink's own metric listener. The startup check is covered against a fake admin and a fake
context that records the asynchronous call instead of running it, so the fence is exercised
deterministically: readers parked before the check completes and assigned when it does, a reader that
leaves or re-registers while parked, each rejection (ordering mismatch, exactly-once delivery, `NACK`
without a dead-letter policy, a missing subscription with no creation settings), each subscription
created with its own settings, no seek issued when a rejection is coming, and the start position
running exactly once and never on a restore. The subscription-create options, the start position and
the option-to-protobuf translation are unit-tested on their own. Emulator integration tests run the
production subscriber factory against the emulator and cover the acknowledgement round trip,
nack-on-close (the nacks counted on the reader's own metric, and the messages redelivered rather
than lost — redelivery *promptness* is a service-timing property the emulator does not specify, so
the emulator settles for non-loss and the real-GCP suite below asserts promptness; see
[#118]({{< param BookRepo >}}/issues/118)), and one reader consuming several subscriptions; they
also drive the production subscription admin (creation with settings read back, `ALREADY_EXISTS`
leaving an existing subscription alone, and seek-to-timestamp replaying acknowledged messages).
MiniCluster tests drive the source through the public builder over two subscriptions under real
checkpoints, and through the startup check end-to-end: auto-creating a missing subscription and then
consuming it, failing the job when creation is not authorised, rejecting an unordered subscription
under ordered consumption, and replaying a backlog under `earliestRetained()`. Recovery has its own
MiniCluster tests: a failure injected after a completed checkpoint restarts and restores without
losing messages, and a savepoint taken at one parallelism restores at another in both directions —
the split plan is recomputed by the enumerator on every start, so the rescale reassigns cleanly.

A **real-GCP gated suite** covers what the emulator cannot, gated on `PUBSUB_IT_PROJECT`
(application-default credentials; skipped when unset, keeping `./mvnw verify` credential-free, and
never selected by an ordinary build even when it is set, because of the `@Tag("gated")` these
classes carry — running the suite is opt-in per command, through `just e2e`,
[#245]({{< param BookRepo >}}/issues/245)):
end-to-end per-key ordering through `orderingMode(PER_KEY)` (the only coverage of the ordering
guarantee anywhere — see below), dead-letter forwarding under the `NACK` policy (the forwarding is
performed by the Pub/Sub service agent, whose project-level grants the repository's opentofu
provisions), seek-to-timestamp on an ordering-enabled subscription, the create-option knobs
(retention, expiration, filter) persisting on the service, prompt redelivery after a nack-on-close
(an observed-behaviour bound, deliberately not a contract), the subscription admin's
permission-denied messages, exercised by impersonating a deliberately unauthorized service account
(`e2e-no-pubsub`, provisioned with no Pub/Sub role), and — on the sink side — the batch-rejection
outcome the confirmed-solo routing above rests on ([#303]({{< param BookRepo >}}/issues/303)):
a valid message co-batched with an invalid one is published rather than dropped, exactly the
invalid one reaches a dropping handler, and the flush completes. Topics and subscriptions are
created under
per-run UUID-suffixed names and deleted afterwards. For local runs, put `PUBSUB_IT_PROJECT` in the
uncommitted `.env` at the repository root (in a git worktree, run `just worktree-env` once to make it
reachable there) — and note the IAM tests impersonate `e2e-no-pubsub`,
which needs a one-off `roles/iam.serviceAccountTokenCreator` binding for your own account
(`gcloud iam service-accounts add-iam-policy-binding e2e-no-pubsub@<project>.iam.gserviceaccount.com
--member=user:<you> --role=roles/iam.serviceAccountTokenCreator`); the grant is deliberately not in
opentofu, which keeps personal identifiers out of source. `just e2e` runs every gated ITCase across
the BigQuery and Pub/Sub modules and fails loudly if a variable is missing or a gated class did not
actually execute; the same recipe runs weekly in the E2E workflow, authenticating via Workload
Identity Federation ([#28]({{< param BookRepo >}}/issues/28)).

**The emulator cannot verify ordered delivery.** Per-key callback serialization in the client
library is gated on `subscriptionProperties.messageOrderingEnabled` in the streaming-pull response,
which the emulator does not set — probing the client library directly against it shows callbacks
arriving out of order with no Flink involved. The emulator test therefore asserts only that ordered
mode consumes the subscription from a single subtask without stalling on idle ones; end-to-end
per-key order is covered by the real-GCP suite above ([#82]({{< param BookRepo >}}/issues/82)).
