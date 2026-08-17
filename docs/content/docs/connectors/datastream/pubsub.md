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

# Cloud Pub/Sub Connector

Cloud Pub/Sub sink and source for Apache Flink, with dynamic per-record topic destinations on the
sink and multi-subscription consumption on the source, provided by the
`flink-connector-gcp-pubsub` module.

Per-feature implementation status is tracked in the
[module README]({{< param BookRepo >}}/blob/main/flink-connector-gcp-pubsub/README.md).

{{< java-snippet file="PubSubConnectorOverview.java" tag="pubsub-connector-overview" >}}

API notes:

- `PubSubSerializationSchema.serialize` returns a full `PubsubMessage`, so message attributes
  and ordering keys are expressible. `payload(...)` wraps a plain Flink `SerializationSchema`
  for payload-only messages; `withAttributes(...)` and `withOrderingKey(...)` layer extracted
  attributes and an ordering key onto any schema (null/empty extractions add nothing).
- Returning `null` **skips** the record — it is written nowhere, is not a failure, and never
  reaches the failed-message handler — which is how a filter that depends on the message being
  built belongs in the serializer rather than upstream of the sink. Every serializer in this
  connector family reads `null` that way, and a `null` travels unchanged through
  `withAttributes(...)` and `withOrderingKey(...)`, whose extractors are not called for it.
  A skip is counted by [`recordsSkipped`](#sink-metrics), the only thing that reports it: a
  serializer skipping every record would otherwise leave an empty topic under a green job.
  `payload(...)` cannot skip — Flink's `SerializationSchema` contract has no `null` in it, so a
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
- `serviceAccountKeyFile(path)` authenticates every publisher and the topic auto-creation admin
  with the service-account JSON key at `path`.
  The file is read when each writer starts, so the same path must be readable on every TaskManager
  that can run the sink.
  See [Credential file deployment](#credential-file-deployment) for the Kubernetes, session-cluster and rotation requirements.
  When the setter is absent, application-default credentials remain in effect, including
  `GOOGLE_APPLICATION_CREDENTIALS`.
  Service-account keys are long-lived secrets, so prefer an attached service account or Workload
  Identity where the deployment supports one.
  The setter accepts a file path only, not raw or Base64-encoded JSON, access tokens, or custom
  credential-provider classes.
  A read or parse failure reports neither the path nor credential material.
  It is rejected beside `emulatorEndpoint(...)`, whose channel carries no credentials.
- `emulatorEndpoint(host:port)` points the sink at a Pub/Sub emulator: the per-topic publishers
  and the topic auto-creation admin connect over a plaintext channel with no credentials, so it
  must only ever be used against an emulator (for example a testcontainers
  `PubSubEmulatorContainer`) — never against production Pub/Sub. The value is parsed by the
  setter, so a malformed `host:port` is rejected by that call on the client rather than
  surfacing as a connection failure on a task manager
  ([#235]({{< param BookRepo >}}/issues/235)).

## Credential file deployment

> **Authentication recommendation.** Google recommends [avoiding service-account keys whenever possible](https://cloud.google.com/iam/docs/best-practices-service-accounts#choose-when-to-use).
> Prefer keyless application-default credentials from an attached service account or Workload Identity over a service-account key file.
> Use `serviceAccountKeyFile(path)` only when the job must select an explicit service account that the process environment cannot provide.
>
> On Kubernetes, store the JSON key in a `Secret` and mount it as a read-only volume at the same absolute container path in every pod that may load it.
> A sink needs the path on every eligible TaskManager; a source also needs it on the JobManager.
> This path is inside the container, not a path that merely exists on the Kubernetes node.
> Do not store credential material in a `ConfigMap`, SQL DDL or a connector option.
> Mount the Secret directory rather than one file through `subPath` when in-place rotation is expected, because Kubernetes does not update a Secret mounted with `subPath`.
>
> On a session cluster, the same path must remain readable by every eligible JobManager and TaskManager process, including replacement or newly allocated TaskManagers.
> Each writer, reader or enumerator reads the file once when that runtime component starts.
> Replacing or rotating the mounted file does not hot-reload credentials.
> Wait until a normally projected Secret has updated in every eligible pod before restarting the affected job; with a `subPath` mount, recreate the affected pods or cluster first.
> Replace the key in every workload that uses it and validate those workloads before disabling the replaced key.
> Monitor them after disabling it, then delete it after confirming that they still work, following Google's [service-account key rotation guidance](https://cloud.google.com/iam/docs/key-rotation#process).
>
> Mounting several job-specific keys into one shared session cluster weakens isolation because co-located jobs share the cluster environment.
> Prefer an application/per-job cluster with Workload Identity when jobs require separate identities.

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

**`retryTotalTimeout` and `retryMaxAttempts` cannot be combined with message ordering either**, and
`PubSubPublisherOptions.build()` rejects the pair rather than accepting settings the SDK will
overwrite. An ordering-enabled publisher retries without limit — see
[Publisher lifecycle](#publisher-lifecycle) for the mechanism and for what it costs the close — so
neither an attempt cap nor a total timeout can bound a publish there, for unkeyed messages too. The
six other retry knobs are unaffected. A program that toggles ordering has to set these two on the
unordered branch only, rather than once for both.

One interaction to size around: a `batchRequestByteThreshold` above `maxInFlightBytes` means a
batch can never fill under the writer cap, so batches leave only on the delay threshold (or
`flush()`). That is latency, not deadlock — the delay alarm always fires — but keep the batch
threshold below the in-flight cap.

## Delivery guarantees and state

See [Delivery guarantees]({{< relref "docs/connectors/delivery-guarantees" >}}) for the terms and
cross-connector comparison.

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
completions are re-dispatched onto the task mailbox, so every write to the writer's state happens
on the task thread — your `DestinationResolver`, serializer and `FailureHandler` are called from
there and need no synchronization of their own. Reads are not all on it: the metric reporter runs
on a thread of its own, which is why the gauges below read plain counters rather than walking the
writer's per-destination maps. A
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
writer's `close()`. This deviates from the vendored
upstream, which caches one `Publisher` per topic JVM-wide and shuts them down only in a JVM
shutdown hook: writer ownership gives a deterministic lifecycle and no cross-job leakage in
shared TaskManagers. The tradeoff: several subtasks on one TaskManager publishing to the same
topic each hold their own `Publisher` (own batcher, own channel) instead of sharing one —
acceptable at moderate parallelism; gRPC channels are multiplexed inside the SDK.

The close runs in two phases. Every publisher is asked to shut down, and only then is any of them
waited on, so the waits overlap: a close costs one `shutdownTimeout` (30 s by default) however many
topics the writer wrote to, rather than one per topic. That matters with dynamic destinations,
where seven sequential 30 s waits would exceed Flink's `task.cancellation.timeout` (180 s) and make
a cancelling task a fatal TaskManager error. On a task failure or a clean shutdown that watchdog
does not run, and an over-long close merely delays the task.

The timeout is a real bound rather than a formality, because the SDK's own shutdown is not
guaranteed to return. `Publisher.shutdown()` waits on a counter of accepted publishes,
uninterruptibly and with no timeout, until it is exactly zero — and two independent things stop it
getting there:

- **With `enableMessageOrdering`, the SDK replaces the publisher's retry settings with
  `maxAttempts = Integer.MAX_VALUE` and an effectively infinite total timeout** — for unkeyed
  messages too, as its own `TODO` notes. During a Pub/Sub outage the in-flight publishes retry
  forever, so the counter never drains and the close would never return. Nothing is defective here;
  it is what ordered publishing costs. The same override is why `retryTotalTimeout` and
  `retryMaxAttempts` are rejected beside `enableMessageOrdering(true)` — they would reach nothing —
  while the six other retry knobs still apply.
- A failing ordering key can leave the counter permanently above zero: the failure callback cancels
  the messages still accumulating in that key's un-flushed batch and drops the batch, but returns
  only the in-flight batch's count. That one is an SDK defect —
  [#265]({{< param BookRepo >}}/issues/265) records the analysis, closed with this sink's bounded
  teardown as the mitigation; the fix is upstream in
  [googleapis/google-cloud-java#14002](https://github.com/googleapis/google-cloud-java/pull/14002)
  but not yet in a released client, and
  [#309]({{< param BookRepo >}}/issues/309) tracks its arrival through the BOM.

So the sink runs the whole SDK teardown on a separate daemon thread and gives up on it at the
deadline, releasing the channel either way. Anything that teardown throws is rethrown from the
writer's close with its own type, rather than being left to the JVM's uncaught-exception handler.

Awaiting the client's resources runs on that thread too, and deliberately: gax hands its *full*
timeout to each background resource in turn rather than sharing one deadline across them, so
awaiting on the task thread would cost a multiple of `shutdownTimeout` instead of `shutdownTimeout`.

The residue is honest, logged and counted: a publisher whose shutdown never returns leaves that
thread and the client's executors behind until the JVM exits, and the job continues. The thread is
named after both the topic and the task thread that created it (`… for Sink: Writer (2/4)#1`), so a
thread dump says which subtask left it. On a job that restarts repeatedly against a Pub/Sub outage
this residue accumulates once per attempt, which is the cost of not hanging the task instead — and
the [`publisherShutdownsAbandoned`](#sink-metrics) counter is what makes the overruns visible
without reading logs. What a teardown still in flight holds is worth knowing when reading that
number:
the publisher's own scheduled executor (`5 × availableProcessors` threads, no core timeout), its
gRPC stub and channel pool, and the shutdown thread itself.

Those warnings are logged by `io.github.flink.gcp.connector.base.lifecycle.BoundedShutdown`, not by
a `…connector.pubsub` class — a log configuration scoped to the connector's own package will not
match them.

The same teardown is what a `sendToDeadLetterQueue(...)` handler uses. It owns a publisher of its
own and is closed after the sink's, so it spends a second budget of the same shape —
`PubSubDeadLetterQueue.builder().shutdownTimeout(...)`, 30 s by default. `shutdownTimeout` therefore
bounds the sink's publishers and that one bounds the queue's; keep the **sum** under
`task.cancellation.timeout`.

Both of those budgets are spent at *close*. The waits a running job makes for the same queue — at
each checkpoint barrier, and whenever the queue's outstanding bound fills — have a budget of their
own, `flushTimeout`, described under
[Dead-lettering to a Pub/Sub topic](#dead-lettering-to-a-pubsub-topic).

### What a running job can spend, and `publishProgressTimeout`

The sink itself makes two waits on the task thread, and both are on the same thing — a publish
completing. `write` waits when the in-flight caps are full, which is ordinary backpressure; `flush`
waits at every checkpoint barrier until *nothing* is in flight, which is the checkpoint's sync
phase. Neither is bounded by the caps: those bound how many publishes are outstanding, not how long
one takes.

What bounds them is `publishProgressTimeout` (600 s by default), and what it bounds is a **stall,
not a slow topic**. The budget restarts at every completion, so a publisher that keeps answering
never spends it however long the wait lasts in total; one that has stopped answering entirely
spends it once and fails the job. Nothing is dropped — the sink is at-least-once and holds no Flink
state, so the records behind the unresolved publishes are replayed from the last completed
checkpoint.

Without `enableMessageOrdering` this rarely fires, because a publish already gives up at
`retryTotalTimeout` (600 s by default) and that failure fails the job by itself — measured
2026-08-07, one run: 591 s against an unreachable endpoint. **With ordering, nothing inside the sink
ends an outage but this.** The SDK replaces the publisher's retry settings with "retry forever" (see
above), so no publish ever resolves and no failure is ever produced; the same measurement left an
ordered flush still waiting at 700 s, past the budget the unordered one had already died of.

Outside the sink, Flink's `execution.checkpointing.timeout` still ends such a job at its default —
and since that default is also 600 s, expect it to be what you see first, with the less specific
`Checkpoint expired`. The budget is chosen to match what the unordered path already spends rather
than to beat that clock; if you want a Pub/Sub-named failure ahead of it, set this below your
checkpoint timeout, as the dead-letter queue's `flushTimeout` does by defaulting to a tenth of it.
Where the two stop being interchangeable is `execution.checkpointing.tolerable-failed-checkpoints`:
raise it above `0` and the checkpoint timeout stops failing anything, leaving this budget alone.

Which of the two waits a stalled sink is parked in is not something an operator gets to choose — it
depends only on whether the in-flight caps fill before the checkpoint barrier arrives. Measured on a
job at 5000 records/s with a 1 s checkpoint interval: at the default `maxInFlightMessages` of 1000
the task thread was parked in `write`, and only with a cap large enough for the barrier to arrive
first was it parked in `flush`. That is why one budget covers both.

One interaction it deliberately does not leave to you: a message counts against the in-flight caps
from the moment the publisher accepts it, which is before it goes anywhere, so at the cap every
in-flight message can still be sitting in an SDK batch. Blocking at the cap therefore sends what the
batcher is holding, once — otherwise `batchDelayThreshold` would be a term *inside* this budget, and
a batch delay configured longer than it would fail a job on a perfectly reachable topic. The two
knobs are independent as a result, and no configuration of one is rejected because of the other.

The budget does **not** bound the total time a checkpoint can spend. A `flush` that keeps making
progress can still cost several publish budgets in sequence: the republish recovery adds a drain per
attempt (up to `recoveryMaxAttempts`, 10 by default) and an isolation pass one per parked message —
and a parked batch can hold about twice `maxInFlightMessages`, so that second multiplier is the
larger of the two. Those drains are sequential. Size `publishProgressTimeout` against how long a
publish takes when the topic is healthy, which for most jobs is milliseconds; the 600 s default is
not that number, it is the retry budget an unhealthy one spends before the client gives up.

It also bounds the sink's *own* waits and nothing else on the task thread. A `DestinationResolver`,
a serializer, a `FailureHandler` or a `DeadLetterQueue` you supply runs on that thread too — inside
the wait, in the handler's case — and this budget cannot bound code it is executing. The built-in
`PubSubDeadLetterQueue` bounds itself with `flushTimeout`; a handler of your own must not block
indefinitely. Topic auto-creation's `createTopic` call is outside it too, bounded by the client's
own settings.

**A wait that has stopped making progress says so** — a `WARN` naming Pub/Sub, the wait it is in and
how many publishes are outstanding, once a tenth of the budget has passed with nothing completing
and at most once per that interval thereafter. At the default that is a line at 60 s, and at most
one a minute, rather than a job that dies silently at 600 s; the line beats both clocks that can end
the job, including the checkpoint timeout whose message names nothing about Pub/Sub.

The counters are not where this shows up, which is why the line exists: a publish that never answers
is never counted as a failure, so `errorClass.*.errors` and `numRecordsSendErrors` need not move at
all while the sink is stalled — they will sit at whatever the repairs before it left them. What is
reliable is `numRecordsSend` going flat. Alert on that and on the warning.

The expiry itself does not tell you which retryable status is behind it, because the client retries
them all the same way. `RESOURCE_EXHAUSTED` — a publish quota — reaches the same message as an
unreachable topic, and raising the budget is the wrong answer for it; read the cause on the
exception.

If you leave it at its default, note what happens without it: an ordered sink whose topic became
unreachable does not fail. With Flink's own defaults the *checkpoint* timeout eventually fails the
job — 600 s of a stalled job, ending in `Checkpoint expired before completing`, which names nothing
about Pub/Sub. With `execution.checkpointing.tolerable-failed-checkpoints` raised above `0`, nothing
fails it at all: measured, the job stayed `RUNNING` with its record count frozen while checkpoint
after checkpoint expired underneath it.

The cost of the bound is the honest other half: a disturbance longer than the budget now fails the
job where the SDK's retries used to absorb it, and a persistent one becomes a restart loop. That is
the trade the default is chosen for — 600 s is what the unordered path already spends before failing
of its own accord, so an ordered sink is being given the same self-termination rather than a
stricter one.

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

{{< java-snippet file="PubSubConnectorTopicCreationSettings.java" tag="pubsub-connector-topic-creation-settings" >}}

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

{{< java-snippet file="PubSubConnectorFailedMessagePolicy.java" tag="pubsub-connector-failed-message-policy" >}}

- `FailureHandler.failJob()` (default) — every per-message failure fails the ongoing write or
  checkpoint, depending on where the failure surfaces; this is the sink's behavior when nothing is
  configured
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
  `handle` propagates from the current write or checkpoint operation; returning drops the message.
  `FailedMessage` carries the `PubsubMessage` the serializer produced, or `null` when serialization
  itself failed; under the shared `FailedElement` contract it reports `getConnector()` (`"pubsub"`),
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
Pub/Sub rejects each one individually, and every rejection the sink confirms counts toward
[`maxConsecutiveRejections`]({{< relref "docs/reference/pubsub" >}}#pubsubpublisheroptions), so a
stream the service refuses wholesale fails the job at that bound rather than draining silently —
the bound is described under [Ordering and a dropping
policy](#ordering-and-a-dropping-policy) but applies with or without ordering. Watch
[`numRecordsSendErrors`]({{< relref "docs/connectors/datastream/pubsub" >}}#sink-metrics) rather
than the job status for everything below the bound: it counts every message the handler
received, so a rejection run the bound has not ended shows up as a rate.

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
every message is invalid degrades to one round trip per message until `maxConsecutiveRejections`
ends it. (An *oversized* message under the default batching settings never needed this: the SDK
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
in one attempt ([#269]({{< param BookRepo >}}/issues/269)). What the budget
still bounds is a repair making no progress: topic metadata that never propagates, or a key whose
republishes keep failing without a verdict. When it runs out, the failure message says which
happened — `kept failing` (a republish that never got through, `after creating the topic` when
the repair created one) or `could not drain its parked messages within the recovery budget`, with
the number of messages that were handed to the failure handler during the repair and, when both
facts hold, the creation too.

**How long a run the sink will drain at all is bounded by
[`maxConsecutiveRejections`]({{< relref "docs/reference/pubsub" >}}#pubsubpublisheroptions)**
([#361]({{< param BookRepo >}}/issues/361)): a dropping policy is a decision to keep running
through *anomalous* records, and a stream being refused wholesale is not that — it is broken data
degraded to one publish per message under a green job — so once that many confirmed rejections
arrive in a row, with not one successfully published message between them, the job fails with a
message naming the option, the count and the last rejection's status. Every message rejected up to
that point — the tripping one included — was routed to the handler first; what a handler had
durably delivered by then follows the `FailureHandler` contract's own checkpoint-contingent
guarantee. Any successful publish, to any topic of the writer, resets the count — an occasional
bad record can never accumulate into a failure — and only rejections the isolation republish has
*confirmed* count: records the serializer rejects say nothing about the service's view of the
stream. The `-1` sentinel removes the bound for a pipeline that really does want to trickle
through arbitrarily bad data.

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
serves every connector here**, not only this one. Every knob and its default is in the
[configuration reference]({{< relref "docs/reference/pubsub" >}}#pubsubdeadletterqueuebuilder);
this section is why they are what they are.

{{< java-snippet file="PubSubConnectorDeadLettering.java" tag="pubsub-connector-dead-lettering" >}}

`PubSubDeadLetterQueue.builder().serviceAccountKeyFile(path)` selects credentials for the dead-letter
publisher independently of this Pub/Sub sink's `serviceAccountKeyFile(path)` setting.
Each sink writer reads the DLQ file when it opens the queue, so the path must be readable on every
TaskManager that can run the sink.
If the DLQ setting is absent, the queue uses application-default credentials even when the host
sink uses an explicit key file.
The [credential file deployment](#credential-file-deployment) note covers Kubernetes Secret
mounts, session clusters and rotation.

| Attribute | Value |
|---|---|
| `dlq-connector` | `bigquery`, `bigtable`, `cloudtasks`, `pubsub` or `spanner` |
| `dlq-destination` | the resource the element was bound for, or a connector-defined sentinel such as `unresolved` |
| `dlq-error` | the failure description, truncated to Pub/Sub's 1024-byte attribute-value limit and marked with `...` |
| `dlq-timestamp` | when the element was offered, ISO-8601 |
| `dlq-subtask` | the offering sink subtask's index |

The message **data** is the element's payload bytes, or empty when the failure has no payload.
A concrete failure may also supply intentionally empty bytes, so data length alone does not
classify the failure.
Use the attributes for that distinction.
The failure's cause chain is not in the envelope (it has no bounded string form); enable `DEBUG`
logging on `PubSubDeadLetterQueue` to see untruncated errors in the job logs.

Publishes are batched and awaited in `flush()`, so a rare failure costs no round trip of its own.
`maxOutstandingMessages` bounds what one checkpoint interval can accumulate when *every* record
fails — the default is 1000, `0` publishes each element synchronously (the narrowest loss window,
one round trip per element) and `-1` buffers until the flush. The topic must already exist: this
queue never creates one, because a dead-letter destination created on the fly is one nothing is
consuming.

`flushTimeout` (60 s by default) bounds each wait a running job makes for those publishes — the one
in `flush()`, which runs at each checkpoint barrier and at any sink-triggered flush such as a
periodic one, and the one `maxOutstandingMessages` triggers inside an offer. It is **one deadline per
wait, covering all of that wait's publishes** rather than each of them. Without it a wait lasts as
long as the SDK keeps retrying, 600 s by default — which is also Flink's default
`execution.checkpointing.timeout`, so a dead-letter outage could spend a checkpoint's whole budget on
its own. (The queue's *close* waits for the same publishes under `shutdownTimeout` instead, below.)

**It bounds one wait, not what a checkpoint interval spends.** How many waits an interval makes is
`maxOutstandingMessages`: one at `-1`, one per 1000 dead letters at the default, and one per dead
letter at `0`. A topic that is slow but working therefore spends several budgets in an interval
without any of them expiring — 100 dead letters at `0`, each taking 25 s, is 2500 s of task-thread
time and no timeout. Size the budget against the interval, not against one wait, when the queue is
configured to drain often.

On expiry the wait throws. From `flush()` that fails the ongoing checkpoint and thereby the job; from
an offer there is no checkpoint in progress, so it fails the task where the record was being
processed. Either way the job restarts from the last completed checkpoint and the records behind the
unpublished dead letters are replayed. **The queue itself drops nothing**, and because the publishes
are not cancelled the SDK may still deliver them — a duplicate, which is what the at-least-once
guarantee already asks a dead-letter consumer to expect. What that guarantee does *not* promise is
unchanged here: a failure that does not recur on replay is preserved only if a completed checkpoint
already flushed it, so an expiry can still cost the dead-letter *entry* for such a failure even
though the record is replayed.

The cost of choosing a budget at all, stated plainly: a Pub/Sub disturbance longer than it now
**fails the job** where the SDK's 600 s retry would have absorbed it, and if the disturbance outlasts
the restart the job will restart repeatedly — accumulating duplicates in the dead-letter topic and
the abandoned-shutdown residue described under
[Publisher lifecycle](#publisher-lifecycle). That is the trade the bound buys: a failure inside the
checkpoint budget instead of one that consumes it. Raise `flushTimeout` if a job should ride out a
longer disturbance, and keep it against the interval budget above rather than against
`execution.checkpointing.timeout` directly. There is no unbounded setting: a `Duration` longer than
any disturbance you mean to survive says the same thing without making waiting forever a mode.

`shutdownTimeout` (30 s by default) bounds the queue's own close, through the same two-phase
teardown the sink's publishers get — see [Publisher lifecycle](#publisher-lifecycle) for why an SDK
publisher's shutdown needs bounding at all. It is spent *after* the sink's, so budget for the sum.

#### Dead-letter metrics

The queue registers these on **the metric group of whichever sink is dead-lettering**, which is the
only group it is given: a BigQuery or Cloud Tasks job dead-lettering to a topic reports them beside
that sink's own names, one set per subtask.

| Metric | Type | Meaning |
|---|---|---|
| `deadLettersPublished` | counter | dead letters the service **confirmed**, counted as each publish resolves rather than when it was handed over |
| `outstandingDeadLetters` | gauge | dead letters handed to the client library and not yet confirmed, which `maxOutstandingMessages` bounds |
| `deadLetterFlushMillis` | gauge | how long the most recent wait for those publishes took — the number to read against `flushTimeout`. A flush with nothing buffered is not a wait and leaves it alone |
| `longestDeadLetterFlushMillis` | gauge | the longest such wait **this task attempt** has seen. It never falls, and a restart starts it over |
| `deadLetterPublisherShutdownsAbandoned` | counter | the queue's publisher closes that overran `shutdownTimeout`, process-wide in the sense [Publisher lifecycle](#publisher-lifecycle) describes |

**How many were dead-lettered is already `numRecordsSendErrors`**, which every sink in this
repository increments immediately before calling its failure handler — so under
`sendToDeadLetterQueue(...)` that standard counter reports exactly what this queue was offered, on
this same group. Read the three as a chain: `numRecordsSendErrors` offered, `outstandingDeadLetters`
in flight, `deadLettersPublished` confirmed. A gap between the first and the last that the next
checkpoint does not close is a queue falling behind; it cannot persist quietly, because the flush
that fails to close it throws.

**`deadLetterFlushMillis` is what a `flushTimeout` expiry has no time to tell you.** The expiry
fails the job, and a metric group is torn down with its task, so the value to act on is the series
*before* the failure: waits climbing towards the budget over several checkpoints are the warning
that raising `flushTimeout` — or lowering `maxOutstandingMessages` so each wait carries less — is
due. The wait it reports is whichever ran last, the one in `flush()` or the one an offer triggered
at the outstanding bound; both spend the same budget. A `flush()` that finds nothing buffered — on
a job that dead-letters occasionally, that is almost every checkpoint — does not touch it, so the
value stays that of the last wait there actually was rather than being zeroed a barrier later.

**Read `longestDeadLetterFlushMillis` for the spike the other one cannot keep.** Waits happen as
often as the queue drains, which under `maxOutstandingMessages(0)` is once per *element* — so a
publish that nearly spent the budget is overwritten thousands of times before a reporter runs, and
alerting on the last-wait gauge alone would miss exactly the warning it exists to give. The maximum
is the one to alert on (`longestDeadLetterFlushMillis` approaching `flushTimeout` means the next
disturbance fails the job); the last wait is the one to read for what is happening now. Because it
never falls, a high value means "this attempt saw a wait that long", not "waits are long now" —
those two questions are why there are two gauges, and a restart clears it along with the rest of
the writer's state.

**`deadLetterPublisherShutdownsAbandoned` is separate from the sink's
`publisherShutdownsAbandoned`** on purpose. They count different publishers, and a Pub/Sub sink that
also dead-letters to Pub/Sub carries both names on one group — which one name could not do, since
Flink resolves a duplicate registration by keeping the metric registered first and dropping the
other. Everything the sink counter's notes below say about scope, what "abandoned" means and how to
aggregate across TaskManagers holds for this one too, with its own name substituted in the PromQL.

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
| `publisherShutdownsAbandoned` | counter | **sink** publisher closes that overran their shutdown budget. **Not this subtask's, and not this attempt's** — see below. A dead-letter queue's are [counted apart](#dead-letter-metrics) |
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

**`publisherShutdownsAbandoned` reports a whole JVM, not the subtask reading it.** Its value is a
process-wide total, so every subtask sharing a TaskManager returns the same number. That is
deliberate: the quantity is what accumulates *across* restart attempts, and a per-attempt figure
could never be observed — a writer's metric group is unregistered as its task is cleaned up, in the
same instant the close that abandoned the teardown ran. (Measured, not assumed: a probe with a
reporter at 10 ms, a thousand times Flink's 10 s default, scraped ~90 times per run and never saw a
close-time counter above zero.) So a teardown this attempt gives up on is reported by the *next*
attempt's writers, and the value grows while the job keeps restarting against an outage.

**Aggregating it.** Never sum the raw series — that multiplies one JVM's count by the subtasks on
it. De-duplicate within a TaskManager first, then sum across them; in PromQL,
`sum(max by (tm_id) (flink_taskmanager_job_task_operator_publisherShutdownsAbandoned))`. A plain
maximum is wrong in the other direction: it reports the worst single TaskManager as though it were
the cluster.

**What it does and does not tell you.** It counts closes that *overran their budget*. It does not
count teardowns still in flight: once the close gives up, the background thread exits as soon as the
client's own shutdown returns, so a close that overran by a second leaves nothing behind and still
increments this. Read a rising value as "closes are timing out", then use
[Publisher lifecycle](#publisher-lifecycle) and a thread dump to see whether anything is still
stranded.

**Its scope depends on how the connector was deployed**, which is the part most easily got wrong.
The count lives in whichever class loader loaded the connector:

- **The job's own jar** (a DataStream job, or `ADD JAR` in the SQL client) — Flink's per-job class
  loader, so the count is that job's, and two jobs on one TaskManager cannot see each other's.
- **The SQL uber-jar in Flink's `lib/`**, the placement the
  [SQL connector page]({{< relref "docs/connectors/table/pubsub" >}}#getting-the-connector-onto-the-classpath)
  recommends — the *system* class loader, so **one count is shared by every job on that
  TaskManager**, and it never resets while the TaskManager lives.

The second case is the one to think about on a session cluster or in application mode running
several pipelines side by side. Nothing is corrupted — the increments are ordinary and the number is
exact — but it becomes a property of the **TaskManager**, not of the job whose dashboard displays
it. That is arguably the honest scope, because the threads and channels being counted are in the
JVM regardless of which pipeline left them there; a job's metric group is simply the only vehicle
Flink gives us to report a JVM-level quantity. Read it that way and it is useful; read it as
"my pipeline's residue" and it is not.

Two consequences of that worth stating outright, because they surprise:

- A pipeline with **no Pub/Sub sink** still contributes to a count no Pub/Sub sink reads.
  `PubSub → BigQuery` with `sendToDeadLetterQueue(PubSubDeadLetterQueue…)` owns a Pub/Sub
  publisher, and its abandoned teardowns land in the class loader's dead-letter residue — which
  that job *does* report, as
  [`deadLetterPublisherShutdownsAbandoned`](#dead-letter-metrics) on its BigQuery sink's group,
  but every other pipeline sharing the loader reports the same total.
- Conversely a job cancelled and resubmitted from its own jar gets a fresh class loader and a count
  of zero while any stranded threads remain: **zero does not mean clean**.

It counts every bounded teardown of a **sink** publisher the class loader has served. A dead-letter
queue's teardowns are counted apart, under
[`deadLetterPublisherShutdownsAbandoned`](#dead-letter-metrics), which is what lets a job with no
Pub/Sub sink report them in a metric at all rather than only as a `WARN` in its logs: that queue
registers on whichever sink hosts it, and one name for both residues would collide on the group of
a Pub/Sub sink that also dead-letters. Sum the two when what you want is "every publisher this
class loader gave up on"; read them apart when what you want is which publisher is stalling.

**`numRecordsSendErrors` is the counter to watch when the handler is not `failJob()`.** It counts
exactly what reached `failedMessageHandler(...)` — a record the serializer rejected, and a publish
the service answered `INVALID_ARGUMENT` on its own single-message request — whether the handler then
dropped the message or failed the job. A rejection run under a dropping policy is dropped one message at a time and shows here as a
rate, until it reaches
[`maxConsecutiveRejections`]({{< relref "docs/reference/pubsub" >}}#pubsubpublisheroptions) and
fails the job. A record the *serializer* rejects counts here but never toward that bound — it
says nothing about the service's view of the stream.

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
lifetime of the task.
The writer also retains each topic's publisher until it closes; there is no sink-side destination eviction or rebuild.
Switch the metrics on when the topic set is small and known; the option is in
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

{{< java-snippet file="PubSubConnectorSource.java" tag="pubsub-connector-source" >}}

API notes:

- `PubSubDeserializationSchema.deserialize` receives the full `PubsubMessage` — payload,
  attributes, ordering key, message id and publish time are all available — and writes to a
  `Collector`, so one message may produce any number of records. Emitting none drops the message
  (it is still acknowledged). `payload(...)` wraps a plain Flink `DeserializationSchema` for
  payload-only messages.
  Every collected record must be non-null and emitted synchronously during that call; do not retain
  the collector or use it from another thread.
- The Pub/Sub publish time becomes the record's event timestamp.
- `serviceAccountKeyFile(path)` authenticates the subscription admin and every subscriber with the
  service-account JSON key at `path`.
  The file is read on the JobManager for subscription administration and on each TaskManager that
  creates a reader, so the same path must be readable in every eligible process.
  See [Credential file deployment](#credential-file-deployment) for the Kubernetes, session-cluster and rotation requirements.
  When the setter is absent, application-default credentials remain in effect, including
  `GOOGLE_APPLICATION_CREDENTIALS`.
  Service-account keys are long-lived secrets, so prefer an attached service account or Workload
  Identity where the deployment supports one.
  The setter accepts a file path only, not raw or Base64-encoded JSON, access tokens, or custom
  credential-provider classes.
  A read or parse failure reports neither the path nor credential material.
  It is rejected beside `emulatorEndpoint(...)`, whose channel carries no credentials.
- `emulatorEndpoint(host:port)` points the source at a Pub/Sub emulator over a plaintext channel
  with no credentials, so it must only ever be used against an emulator — never against production
  Pub/Sub. Unlike the vendored upstream, the source deliberately does **not** honor the
  `PUBSUB_EMULATOR_HOST` environment variable: a stray value on a task manager would silently
  redirect a production job. As on the sink, the endpoint is parsed by the setter, so a malformed
  `host:port` is rejected by that call ([#235]({{< param BookRepo >}}/issues/235)).

### Subscriber options

`subscriberOptions(PubSubSubscriberOptions)` tunes the SDK subscribers and the reader. Every knob
left unset keeps the SDK's (or the source's) current default — `PubSubSubscriberOptions.defaults()`
is equivalent to not setting options at all. Every knob and its default is in the
[configuration reference]({{< relref "docs/reference/pubsub" >}}#pubsubsubscriberoptions); this
section is why they are what they are.

**Flow control is the bound on in-flight messages, for as long as the client is extending their
leases.** Because the source acknowledges only on checkpoint completion, everything received since
the last completed checkpoint counts against these limits, and the client stops pulling once they
are reached — until `maxAckExtensionPeriod` passes for a message the job has not emitted, after
which the client releases its permit while the connector still holds it. **What decides whether that
accumulates is how fast the split is being drained**, and the break-even is
`flowControlMaxOutstandingElementCount / (maxAckExtensionPeriod − one lease extension)` — about
**0.28 messages a second at the defaults**, since an acknowledgement only ever covers a message the
job already consumed, leaving expiry as the only source of permits that does not track the drain.
The subtracted term is the client library's *own* extension length, which starts at ten seconds and
adapts to how long messages are taking; it is not the subscription's `ackDeadlineSeconds`, and it
is why the rate is an order rather than a constant.
Any job making real progress stays above it. The two cases that do not are a split paused by
watermark alignment, which is not drained at all and is bounded by `pausedSplitBufferMaxMessages` /
`pausedSplitBufferMaxBytes` (see [Watermark alignment](#watermark-alignment)), and a downstream that
has stopped consuming altogether, which nothing bounds — watch `bufferedMessages` and
`bufferedBytes` ([#377]({{< param BookRepo >}}/issues/377)).

Once a split is in either state, **most of what it is handed stops being new data**. A lapsed lease
is redelivered, and the copy is buffered *beside* the one the reader is still holding, so the buffer
fills with duplicates and the same record is emitted twice into a running pipeline — within
at-least-once, but not at a restart. Measured against the service at 215 and 338 such redeliveries
out of 369 and 462 deliveries over 90 s.

`maxRecordsPerFetch` only caps how much
a single fetch drains from one split; it is not a memory bound. The limit behavior is not exposed
because the SDK subscriber does not expose it either — it forces blocking regardless of the
settings, which for a subscriber means it stops pulling rather than blocking a thread.

**The subscriber shutdown mode is fixed at `NACK_IMMEDIATELY`** and deliberately not a knob. The
SDK's `WAIT_FOR_PROCESSING` default waits for acknowledgements that only arrive at checkpoint
completion — which never happens during shutdown — so it would stall every close. Only
`shutdownTimeout` is configurable, and it bounds a reader's whole close rather than each split's:
the reader nacks every split's messages and asks every client to stop before it waits on any, so
the waits overlap however many splits it owns. Keep it under Flink's `source.reader.close.timeout`
(30 s by default). Whether the value is right for a deployment is what
[`subscriberShutdownsAbandoned`](#metrics) answers.

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

The job manager's credentials need `pubsub.subscriptions.get` on every configured subscription for
the startup check.
Every non-default start position also makes the job manager seek by timestamp, which needs
`pubsub.subscriptions.consume`.
Auto-creation on the job manager additionally needs `pubsub.subscriptions.create` on the containing
project and `pubsub.topics.attachSubscription` on the requested topic.
Each task manager reader needs `pubsub.subscriptions.consume` to pull, acknowledge and modify
acknowledgement deadlines.
`roles/pubsub.viewer` plus `roles/pubsub.subscriber` cover an existing subscription;
`roles/pubsub.editor` covers the full create and consume path.

### Subscription auto-creation

Passing creation settings alongside a subscription is what authorises creating it; a subscription
added without them must already exist, and the job fails at startup naming the option if it does not.
There is no separate disposition enum because there is no meaningful "create with defaults": a
subscription without a topic is not a subscription, and only you know which topic to bind.

{{< java-snippet file="PubSubConnectorSubscriptionAutoCreation.java" tag="pubsub-connector-subscription-auto-creation" >}}

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
| `recordsSkipped` | counter | messages whose deserializer returned successfully without emitting output |
| `pendingAcks` | gauge | messages received or emitted but not yet acknowledged |
| `pendingCheckpoints` | gauge | checkpoints taken but not yet completed |
| `bufferedMessages` | gauge | messages this subtask's subscribers hold that the fetch loop has not taken yet — see below |
| `bufferedBytes` | gauge | the same in bytes; either dimension can be the one that fills a TaskManager first |
| `parkedSplits` | gauge | paused splits whose subscriber has been stopped, awaiting a resume |
| `splitsParked` | counter | times a paused split outgrew its buffer bound and its subscriber was stopped |
| `subscriberShutdownsAbandoned` | counter | subscriber teardowns whose wait for termination expired. **Not this subtask's, and not this attempt's** — see below |
| `subscriberFailuresUnreported` | counter | failures a subscriber's teardown was the only report of, so no job failure is coming for them. Process-wide in the same sense as the row above |
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

**`bufferedMessages` and `bufferedBytes` are what a reader is holding and has not handed to the
pipeline yet**, summed over the subtask's splits. They are the pair to watch for the failure mode
[Watermark alignment](#watermark-alignment) describes, and for the one under sustained
backpressure — in both, a reader accumulates messages the client library keeps delivering because
`maxAckExtensionPeriod` has released their flow-control permits. `pendingAcks` cannot stand in for
them: it counts messages received *or emitted* and not yet acknowledged, so it climbs for a slow
checkpoint just as readily.

They are read by the metric reporter's own thread, which matters here: the fetch loop that
evaluates the paused-split bound stops running altogether when the downstream stops consuming
([#377]({{< param BookRepo >}}/issues/377)), and these gauges do not.

What they do **not** cover is everything already pulled out of the subscriber: Flink's element
queue, the fetch the reader is working through and the batch the fetcher cannot hand over, each
holding one drain of every assigned split. That is up to
`(source.reader.element.queue.capacity + 2)` × `maxRecordsPerFetch` × splits messages — measured at
3999 with a capacity of 2, a 1000-message fetch and one split. Nothing reports it, and the bound in
[Watermark alignment](#watermark-alignment) does not see it either.

`pendingRecordsGauge` is deliberately **not** set. Pub/Sub exposes no backlog through the data
plane, and a wrong lag number is worse than none.

**The two subscriber-teardown counters report a whole JVM, not the subtask reading them.** Both are
process-wide totals held for the lifetime of the class loader, for the same reason the sink's
[`publisherShutdownsAbandoned`](#sink-metrics) is: a count written during a reader's `close()` is
never scraped, the metric group being unregistered in the same instant. Everything that counter's
notes say about scope, about how the deployment decides it, and about aggregating across TaskManagers
holds for these two, with their own names substituted in the PromQL. A reader therefore reports what
*every* subscriber in its class loader left behind, this attempt's and earlier attempts' alike —
which is the point, since a teardown giving up is a thing to see across restarts.

One class of increment does not need that scope, and it does not change the answer: parking a paused
split tears its subscriber down while the job keeps running, so those increments would be scraped
from an ordinary per-subtask counter too. A metric name has one storage, and the increments that
would otherwise be invisible are the ones that decide which.

**`subscriberShutdownsAbandoned` counts subscriber teardowns, not reader closes.** A reader owns one
subscriber per split, so one close can increment it several times; and parking a paused split closes
that split's subscriber on its own, so a park whose wait expires counts too, with no reader closing at
all. Read a rising value as `shutdownTimeout` being too low for this deployment — the alternative is
noticing that failovers have become slow — and keep the raise under Flink's
`source.reader.close.timeout`.

**`subscriberFailuresUnreported` is the one to alert on, because it is the only report there is.** It
counts a failure that reached a teardown having never been handed to the reader: raised by the
teardown itself, or arriving after the last fetch. Either way no job failure is coming for it, so
without this counter the sole trace is a `WARN`. Nothing is lost — the split's messages were nacked
before the wait — but the shutdown is what returns them to Pub/Sub, so redelivery may wait out their
acknowledgement deadline instead of being immediate.

The clearest case is a park. The job is not shutting down at all: a paused split's subscriber is torn
down, the failure it raises on the way out is absorbed by design (a park closes, and `close()`
absorbs), and a fresh subscriber opens on resume. So the pipeline runs on, healthy by every other
measure, having swallowed a failure — and this counter is the only thing that says it happened.

**The teardown's other two outcomes are counted by nothing, deliberately.** A client repeating at
teardown a failure the reader already has accompanies a job failure already under way over that very
failure, and a release that follows a *failed start* accompanies the `IOException` that fails the job
there. Both would be series whose every increment coincides with a louder report; both still log,
and [Testing](#testing) has the four messages side by side.

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
deadline expires — this is what makes failover recover quickly. A reader close is not the only
occasion: parking a paused split whose buffer outgrew its bound nacks the same three states, on a
running job with no failure and no restart, so those records are redelivered and emitted again when
the split resumes (see [Watermark alignment](#watermark-alignment)). The SDK subscriber is additionally
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
is why the subscriber's flow-control settings are exposed rather than hidden. It is also why the
connector has a bound of its own for the case flow control stops covering, a paused split (see
[Watermark alignment](#watermark-alignment)), and gauges for the case nothing bounds, a downstream
that has stopped consuming (see [Metrics](#metrics)).

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

#### Watermark alignment

`WatermarkStrategy.withWatermarkAlignment(...)` works, and pauses splits the way it does for any
other source: a **split** whose watermark runs beyond the aligned group's by more than the drift
limit stops being consumed until the rest catch up. Splits, not subscriptions — under the default
`OrderingMode.NONE` a subscription has `max(|subscriptions|, parallelism) / |subscriptions|` of them,
and Pub/Sub balances its stream across whichever are still pulling, so that subscription only stops
being consumed once all of its splits are paused. Alignment is therefore most predictable when
subscriptions are at least as many as the parallelism, and exact under `PER_KEY`, where the mapping
is one split per subscription. Nothing is paused at all if
`pipeline.watermark-alignment.allow-unaligned-source-splits` is `true`: Flink then aligns whole
subtasks and never asks a source to pause a split.

A streaming-pull connection cannot itself be paused, so a paused split is simply not drained.

**Flow control bounds a pause only for `maxAckExtensionPeriod`, and the connector bounds it after
that.** The client library's flow control holds the pause at first — it stops pulling once its
outstanding limit fills — but its lease-extension budget is measured from when a message was
*received*, not from when the job emits it. Once `maxAckExtensionPeriod` (1 h by default) passes for
a buffered message, the client library stops extending its lease, Pub/Sub redelivers it, and the
client **releases that message's flow-control permit** — while the connector is still holding the
message. Permits therefore free up in a wave, pulling resumes, and the buffer grows again by about
a whole flow-control window each time. The emulator measurement behind
[#357]({{< param BookRepo >}}/issues/357) saw two such waves and then stopped; the service
measurement behind [#377]({{< param BookRepo >}}/issues/377) kept going, so the two-wave ceiling was
an emulator artifact and there is no ceiling in the mechanism either. The wave lands one *lease
extension* before the period elapses, not after it, because the client drops a message it can no
longer extend past the next one — and that extension is the client's own adaptive value, which
starts at ten seconds, not the subscription's acknowledgement deadline.

**So past its bound, a paused split's subscriber is stopped, and a fresh one opens when the split
resumes.** The bound is [`pausedSplitBufferMaxMessages` and
`pausedSplitBufferMaxBytes`]({{< relref "docs/reference/pubsub" >}}#pubsubsubscriberoptions), and either
being exceeded is enough — which of the two binds depends on message size. Both default to
**twice** the flow-control limit they shadow: one lease-expiry wave is worth a whole window, so the
lapse crosses that bound and ordinary skew does not. (A healthy buffer can sit a little above the
flow-control limit — a message larger than the byte limit is admitted anyway, a dead-letter
subscription's delivery-attempt attribute is added after the client reserves, and a redelivery is
held beside the copy it supersedes — so a bound at the limit itself would park healthy splits.)

Stopping the client hands every lease back, so **nothing is lost**: Pub/Sub redelivers what was
buffered and the split consumes it after the resume. It is not free of duplication, and the
duplication is the point to plan for. The nack covers what the split had *emitted* since the last
completed checkpoint as well as what it was holding, so those records are emitted a second time on
resume — within the at-least-once contract, but on a running job rather than at a restart. Each
nacked message also spends one attempt against a dead-letter policy's `maxDeliveryAttempts`;
`messagesNacked` is where that shows up. Under `orderingMode(PER_KEY)` nothing else pulls those
messages meanwhile, and a key is replayed in order rather than reordered.

"At once" is the intent rather than a guarantee: if the client does not terminate within
`shutdownTimeout` the reader gives up on it with a `WARN`, and its messages then wait out their
acknowledgement deadline instead. A park is a teardown like any other here, so it increments
[`subscriberShutdownsAbandoned`](#metrics) when it does.

The bound is evaluated once per fetch, so it caps what a split holds between checks rather than what
its buffer can momentarily reach: a burst delivered between two fetches overshoots it, bounded in
turn by what the client library delivers at once (measured at 104 and 121 buffered against a bound of
60 over two runs, one wave of a 50-message window each time).

**An indefinite pause is still a problem, just no longer a memory one.** An aligned group holds its
slowest member's watermark, so a subscription that goes quiet holds every other split paused forever
unless the strategy carries `withIdleness(...)` — and a split that stays parked consumes nothing
while its subscription's backlog grows, until Pub/Sub's message retention begins dropping it.
`parkedSplits` is the signature to alert on, and it is zero on a healthy job for a reason worth
stating: a park takes about a `maxAckExtensionPeriod` of *continuous* pause to reach, which a
strategy carrying `withIdleness(...)` does not produce. So a split that is parked means alignment
is holding it indefinitely, and the fix is the strategy or the laggard — add `withIdleness(...)`,
bring the slow member forward, or widen the drift limit. Raising the bound holds more memory for a
split nobody is consuming, which is worth doing only for a pause you know is bounded and have the
heap for. `splitsParked` counts the parks themselves, which is what a park and its resume falling
between two scrapes would otherwise hide.

**A paused split is still watched.** If its subscriber fails permanently while paused, the job
fails, exactly as it would for a split that was being consumed
([#348]({{< param BookRepo >}}/issues/348)) — worth stating because the opposite is the natural
reading of "not drained".

That guarantee stops at the park, and deliberately: **a parked split has no client**, so a
subscription deleted or its access revoked *during* the pause goes unnoticed until the split
resumes, where reopening its subscriber fails the job instead. A failure recorded before the park is
still reported. The alternative is the unbounded buffer this replaces — detection that costs a
TaskManager.

Only a *permanent* failure fails a paused split's job: the client library reports one just for a
status it will not retry, so a `PERMISSION_DENIED` or a deleted subscription, never a blip. Note what that means for a
subscription being decommissioned deliberately — deleting it, or revoking the job's access to it,
fails the job, and a restart then fails in the startup check rather than recovering. Removing a
subscription from a running pipeline means removing it from `subscriptions(...)` and redeploying.

**How promptly the job fails is Flink's to decide, not the connector's.** The reader reports the
failure the next time its fetch loop runs, but a source operator only turns that into a job failure
when the mailbox next polls it — and an operator whose *subtask* is being held back by alignment
(`WAITING_FOR_ALIGNMENT`) does not poll at all, waiting on the alignment future instead of the
reader's. So on a job where this subtask is ahead of its aligned group, the failure is recorded
immediately and surfaced when the group catches up and the subtask is released. It is a delay rather
than a loss — the subtask emits nothing further, so it does not hold the group's minimum back — and
it is not specific to this connector: it is how a fetcher-thread error reaches the job for any
FLIP-27 source under alignment.

Downstream backpressure produces the same shape from the other end, and the measurement behind
[#377]({{< param BookRepo >}}/issues/377) is worth stating because the natural reading is worse than
the truth. Flink's fetcher holds the batch it could not hand over and does not call `fetch()` again
until the element queue has room, so a *stalled* downstream stops the fetch loop entirely, and with
it every check the reader makes there. But a downstream that is merely slow frees a queue slot for
every batch it takes, and each slot lets exactly one more `fetch()` run — so the checks are delayed
by one drain interval, not skipped. And where the loop does stop, so does the mailbox: the reader
would have nowhere to report a failure to either, which is why the connector adds no check on a
thread of its own. What it adds instead is `bufferedMessages` and `bufferedBytes`, which the metric
reporter reads whatever the fetch loop is doing.

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
(payload-only, attributes/ordering-key composition), the publisher options (defaults, validation,
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
the option-to-protobuf translation are unit-tested on their own. The subscriber that wraps the client
library is unit-tested too ([#325]({{< param BookRepo >}}/issues/325),
[#350]({{< param BookRepo >}}/issues/350), [#351]({{< param BookRepo >}}/issues/351)), on the paths a
working client never takes: one that fails to start, which is asked to stop again before the failure
is reported (an `Error` from a first classload taking the same path as an exception); one whose nack
throws, which must still leave the client asked to stop and waited out; one that does not terminate
within the shutdown timeout; one that reports at teardown the very failure the reader has already
been given; and one that fails *during* the teardown, which nothing else reports. All of these are
absorbed rather than raised, and a `WARN` is the whole of their record — but each says which it is,
because they mean different things to whoever reads the log:

| What the `WARN` says | Counted by | What it means |
|---|---|---|
| `… did not finish shutting down within …` | `subscriberShutdownsAbandoned` | The client outlasted `shutdownTimeout`. This split's messages were nacked before the wait, so nothing is lost; the client may keep its channel and threads until the JVM exits — the shutdown it was asked for is still running, and may yet finish. The one line here with an action behind it: raise `shutdownTimeout(...)`, keeping it under `source.reader.close.timeout` |
| `… reported at shutdown the failure it had already reported to the reader` | nothing | Expected on a failing teardown. The reader has that failure and the job is failing on it; this line is not a second problem |
| `… failed while shutting down, and this is the only report of it` | `subscriberFailuresUnreported` | A failure that arrived after the reader stopped pulling, so no job failure is coming. Messages are not lost, but the shutdown is what returns them to Pub/Sub, so redelivery may wait out the acknowledgement deadline instead of being immediate |
| `… did not shut down cleanly after failing to start` | nothing | The client never ran, so it received nothing and nothing was nacked. The start failure itself is reported separately — and *after* this line, so read on rather than back |

**Two of the four are counted** ([#358]({{< param BookRepo >}}/issues/358)), because a `WARN` is
invisible to a log pipeline filtering below `ERROR` and to every dashboard, while a crash-looping job
emits one of these per subscriber per restart. The two counters are described under
[Metrics](#metrics) — including why their values are process-wide, which is what a count written
during a reader's `close()` has to be to be scraped at all. The other two are counted by nothing on
purpose: each accompanies a louder report of the same incident, the job failure the reader is already
raising and the start failure that follows the release. For those, grep remains the detector.

One more integration test measures the *client library* rather than this connector
([#349]({{< param BookRepo >}}/issues/349)): a subscriber whose streaming pull fails permanently
comes back with the executors it was given shut down, over repeated attempts. Those are the
observable for the client's own release sequence, which the SDK sources show also closes the
transport stub — so a crash-looping job does not accumulate a channel and its threads per restart
attempt. The stub half is read rather than seen, and the connector's javadoc says which is
which.

Emulator integration tests run the
production subscriber factory against the emulator and cover the acknowledgement round trip,
nack-on-close (the nacks counted on the reader's own metric, and the messages redelivered rather
than lost — redelivery *promptness* is a service-timing property the emulator does not specify, so
the emulator settles for non-loss and the real-GCP suite below asserts promptness; see
[#118]({{< param BookRepo >}}/issues/118)), and one reader consuming several subscriptions; they
also drive the production subscription admin (creation with settings read back, `ALREADY_EXISTS`
leaving an existing subscription alone, and seek-to-timestamp replaying acknowledged messages).

Two harness rules hold across all of these, both measured
([#150]({{< param BookRepo >}}/issues/150), [#151]({{< param BookRepo >}}/issues/151)): every
drain of a running job's output is deadline-bounded, so a shortfall fails at the deadline naming
what did arrive instead of blocking in `hasNext()` until the build is killed; and a negative
("no further rows arrive") is never asserted by over-requesting rows or by pulling an empty
subscription — a synchronous `Pull` against an empty subscription long-polls for about a minute
before returning nothing — but is made deterministic another way, such as acknowledging the
whole backlog outside Flink first.

MiniCluster tests drive the source through the public builder over two subscriptions under real
checkpoints, and through the startup check end-to-end: auto-creating a missing subscription and then
consuming it, failing the job when creation is not authorised, rejecting an unordered subscription
under ordered consumption, and replaying a backlog under `earliestRetained()`. Recovery has its own
MiniCluster tests: a failure injected after a completed checkpoint restarts and restores without
losing messages, and a savepoint taken at one parallelism restores at another in both directions —
the split plan is recomputed by the enumerator on every start, so the rescale reassigns cleanly.
Watermark alignment has one too, and it is there to measure rather than to cover: one reader owning
two subscriptions under `withWatermarkAlignment(...)`, with the ahead subscription's split observed
to stop advancing — sampled until the count is unchanged three times running — while the other is
drained whole, and then its subscription deleted under the running job, which must fail the job rather than
leave it green
([#348]({{< param BookRepo >}}/issues/348)). Nothing else here configures alignment, so without it
"a paused split is a state jobs actually reach" would be an argument from reading Flink's source
rather than a measurement.

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
