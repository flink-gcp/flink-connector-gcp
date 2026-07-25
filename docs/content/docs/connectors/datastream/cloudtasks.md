---
title: Cloud Tasks
type: docs
weight: 30
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

# Cloud Tasks Connector

Cloud Tasks sink for Apache Flink, provided by the `flink-connector-gcp-cloudtasks` module.

The sink ships in #24; its emulator integration tests are #25. This page doubles as the design
record: it explains not only what the connector does but why each decision was taken, so the
reasoning is settled once rather than re-argued per pull request.

## What this connector is for

Cloud Tasks is best understood as **an HTTP request dispatch queue that executes later while
respecting a rate limit**. A task is not data at rest; it is a request to call an endpoint, held by
the service until the queue's pacing allows it through.

That makes the sink useful in a specific shape: a Flink pipeline reads from somewhere fast (Kafka,
Pub/Sub) and needs to call an endpoint that cannot absorb the stream at source speed — most often a
third-party API with a documented rate limit. The queue is configured to whatever the API allows,
and Cloud Tasks paces the delivery, retries failures with backoff, and can schedule a task up to 30
days ahead. Doing the same inside Flink means writing a stateful throttle; doing it with a plain
HTTP sink means not doing it at all.

**The pacing lives on the queue, not in this sink.** `maxDispatchesPerSecond`,
`maxConcurrentDispatches` and the retry policy are queue configuration, applied by whoever creates
the queue. The sink writes tasks; the queue decides how fast they execute. This is the opposite of
the usual connector experience, where throughput knobs sit in the sink's own options, and it is
worth internalising before reading the rest of this page.

```java
Sink<OrderEvent> sink =
        CloudTasksSink.<OrderEvent>builder()
                .queue(QueueDestination.of("my-project", "asia-northeast1", "webhooks"))
                .serializer(
                        CloudTasksSerializationSchema
                                .httpTarget("https://api.example.com/v1/orders")
                                .withBody(new MyEventJsonSerializationSchema())
                                .withHeaders(e -> Map.of("Content-Type", "application/json"))
                                .withOidcToken("dispatcher@my-project.iam.gserviceaccount.com"))
                .build();
```

## API notes

- `CloudTasksSerializationSchema.serialize` returns a full `Task`, so every per-record field of a
  task — URL, HTTP method, headers, body, schedule time, dispatch deadline, authorization — is
  expressible. Two bounds worth knowing when using them: `schedule_time` may be at most 30 days
  ahead, and an HTTP target's `dispatch_deadline` must be between 15 seconds and 30 minutes. The
  task **name** is the exception: it is composed by the sink (see task naming below), so the
  returned `Task` must carry none — the writer rejects a named one rather than letting it through.
  The `httpTarget(...)` entry point plus the `with*` layering above is a convenience over that
  contract, the same relationship `PubSubSerializationSchema.dataOnly(...)` has to a full
  `PubsubMessage`. Returning the proto rather than a narrow record type is also what keeps the
  Table API layer (#99) cheap: a `RowData` implementation slots in without reworking the sink.
- `httpTarget(url)` returns a **non-generic stage** whose only method is
  `withBody(SerializationSchema<T>)`. That is what binds the record type, so everything chained
  after it — `withMethod`, `withUrl`, `withHeaders`, `withOidcToken`, `withOAuthToken` — infers `T`
  from the body schema and needs no type witness. Each `with*` returns a new immutable schema and
  the schema *is* what the builder takes, so there is no terminal `build()` call. `withUrl(...)`
  resolves the target URL per record, which is the routing the queue-level `uriOverride` warning
  below is about; the fixed URL the chain started from is the default.
- `QueueDestination` is pure queue identity (`equals`/`hashCode` over project, location and queue)
  and can be resolved per record through `destinationResolver(...)`, exactly as the BigQuery and
  Pub/Sub sinks resolve tables and topics. Unlike those two this costs nothing: Cloud Tasks has no
  per-destination connection or stream, so one `CloudTasksClient` serves every queue and dynamic
  destinations need no per-queue state. Resolvers run per record — cache and reuse
  `QueueDestination` instances.
- The location is part of the destination because queues are regional and a project may hold
  queues in several regions.

## Targets

**Only HTTP targets are supported.** A task carries a `oneof` of `http_request` or
`app_engine_http_request`; the App Engine form is deliberately out of scope. Google itself
describes explicit App Engine targets as "less common", and they cannot be distributed across
regions — a queue targeting App Engine must live in the region of the project's App Engine
application, which cannot be changed once set. Their overload signalling also differs: App Engine
returns `503` when instances are overloaded, and Cloud Tasks reads that as *slow down* rather than
as a plain failure, so `503` is unusable as an ordinary retry signal from an App Engine handler.
(Both target types throttle on backoff errors — for external targets those are `429` and `5xx` —
so this is a difference in which code carries the signal, not an inversion.) Supporting a second
target type would double the serializer surface for a shrinking audience; it can be added later
without breaking the HTTP path.

The endpoint must be reachable from Cloud Tasks. Google's documentation opens with it: handlers
"can be run on any HTTP endpoint with an **external IP address** such as GKE, Compute Engine, or
even an on-premises web server". No Cloud Tasks page documents a VPC connector, Private Service
Connect or serverless VPC access for HTTP targets, so plan for a publicly routable endpoint —
though absence of documentation is not the same as a documented prohibition, and this is worth
re-checking before it is relied on.

One case does have a first-class integration, and it is a useful one: a **Cloud Run service set to
`Internal` ingress**, unreachable from the internet, still accepts Cloud Tasks requests. Cloud Run
names Cloud Tasks explicitly among the products whose requests "stay within the Google network"
when they are in the same project or VPC Service Controls perimeter and use the default `run.app`
URL.

Authorization follows what is being called:

| Target | Token |
|---|---|
| Cloud Run, Cloud Run functions, anything else on Google Cloud behind IAM | OIDC (`withOidcToken(serviceAccount[, audience])`) |
| Google APIs on `*.googleapis.com` | OAuth access token (`withOAuthToken(serviceAccount[, scope])`) |
| A third-party endpoint that validates the Google-issued token itself | OIDC — the proto sanctions "endpoints where you intend to validate the token yourself" |
| Anything else | Neither — carry the credential in a header |

OAuth is the narrow one: Google documents it as "generally only" for `*.googleapis.com`. The
builder does not present the two as interchangeable knobs; setting both is rejected, since the
underlying field is a `oneof`.

**Queue-level routing can silently override the task's URL — and v2 cannot see it.** A queue may
carry an `httpTarget.uriOverride` whose `uriOverrideEnforceMode` defaults to `ALWAYS`, documented
as "queue-level configuration overrides all task-level configuration". A pipeline resolving URLs
per record against such a queue will see every task go to the queue's URL instead, with no error
anywhere.

Detecting it is harder than it looks: `httpTarget` exists in the **REST** `Queue` resource and in
`v2beta3`, but **not in the v2 proto** — `com.google.cloud.tasks.v2.Queue` has no `getHttpTarget`,
so a `GetQueue` through the client this sink uses returns an object that cannot carry the field at
all. A preflight check would mean pulling in the v2beta3 client or calling REST directly. v1
therefore documents the interaction rather than guarding it, and the cost of guarding it later is
recorded here so it is not rediscovered as "just one extra call".

## Task naming and deduplication

**The default is unnamed tasks.** Cloud Tasks assigns the name, task creation runs at full speed,
and a task that Flink replays after a failure is created twice.

Naming is opt-in through the **sink builder**, not the serializer:

```java
CloudTasksSink.<OrderEvent>builder()
        .queue(queue)
        .serializer(serializer)
        .taskIdExtractor(OrderEvent::orderId)   // opt in to deduplication
```

It belongs there because a `Task` has no task-id field — only `name`, the full
`projects/P/locations/L/queues/Q/tasks/ID` path. Composing that name requires the resolved queue,
which the destination resolver produces and the serializer never sees. Keeping the extractor on the
builder also separates *what to send* (the serializer) from *how to deduplicate* (a sink policy),
and means the serializer's `Task` is always returned without a name.

With an extractor supplied, a repeated create for an id Cloud Tasks has already seen fails with
`ALREADY_EXISTS`, **which the sink treats as success**. A replayed record therefore does not
produce a second call to the endpoint, which is as close to exactly-once as this service reaches.

The window is bounded, but by how much is **contradicted between Google's own sources**: the REST
reference says an id takes "up to 24 hours" to be released, while the v2 proto comment for the same
field says "~1 hour" (both agree on ~9 days for queues created from a `queue.yaml` or `queue.xml`).
Design against the shorter one. The window also starts when the task is **deleted or executed**,
not when it is created, so a task scheduled far ahead holds its id for its whole lifetime plus the
window. A replay after that duplicates.

This is off by default because it is expensive, and the cost is Google's rather than this
connector's. From the `tasks.create` reference: *"Because there is an extra lookup cost to identify
duplicate task names, these `tasks.create` calls have significantly increased latency."* No
official number is published for how much — the "1 QPS" figure that circulates is not in Google's
documentation, and this connector will not repeat it.

**The sink hashes the extracted id.** The same page continues: *"Using hashed strings for the task
id or for the prefix of the task id is recommended. Choosing task ids that are sequential or have
sequential prefixes, for example using a timestamp, causes an increase in latency and error rates
in all task commands. The infrastructure relies on an approximately uniform distribution of task
ids to store and serve tasks efficiently."* Sequential ids are exactly what a user reaches for
first — an event id, an offset, a timestamp — so `taskIdExtractor(...)` takes any string and the
sink derives the actual task id as its SHA-256 digest. The footgun is closed off rather than
warned about, deduplication is unaffected (the same key always hashes the same way), and the digest
is 64 characters from `[0-9a-f]`, well inside the 500-character limit for the `[A-Za-z0-9_-]` id.

Because the serializer never sets a name, there is no second path around the hashing — the
extractor is the only way to name a task, and every name the sink writes is a digest.

The consequence to know: task names are not human-meaningful, so a task cannot be located in the
console from its business key. Passing a caller-chosen name through unhashed — the only thing that
would allow deduplication against tasks created by another system — is deferred until someone needs
it.

## Delivery guarantees and state

The sink is **at-least-once**, and its writer is **stateless by design**. Tasks are created
asynchronously through `createTaskCallable().futureCall(...)`, and on every checkpoint Flink calls
the writer's `flush()`, which waits for every outstanding create to complete — including those
waiting out a retry backoff. A successful checkpoint therefore means Cloud Tasks has durably
accepted every record up to the barrier — the service returns `OK` only once the task "has been
successfully written to Cloud Tasks storage" — and the writer keeps nothing in Flink state, so
discarding operator state can never lose buffered records. This is the same model the Pub/Sub and
BigQuery sinks use, and the reasoning against `AsyncSinkBase` recorded there applies unchanged.

Checkpointing must be enabled in streaming jobs; without it `flush()` is never called mid-stream
and outstanding creates are lost on failure. Batch execution is covered by the end-of-input flush.

**Retries are this sink's responsibility, unlike every other connector here.** The generated client
gives `CreateTask` an *empty* set of retryable status codes and a 20-second total timeout — verified
in `CloudTasksStubSettings` for `google-cloud-tasks` 2.94.0, where `getTask`, `listTasks` and
`deleteTask` all retry on `DEADLINE_EXCEEDED`/`UNAVAILABLE` and `createTask` alone does not. The
same empty-retry configuration covers every mutating method (`createQueue`, `updateQueue`,
`purgeQueue`, `pauseQueue`, `resumeQueue`, `runTask`), so it reads as a blanket "do not retry
mutations" rather than a judgement about `CreateTask` specifically — but the consequence is the
same either way, and it is compounded by the fact that an unnamed create is not idempotent. The
sink therefore implements its own bounded retry with exponential backoff, configurable through
`CloudTasksWriterOptions` (the nested-options pattern the other modules use, every knob defaulted),
over the codes worth retrying:

| Status | Treatment |
|---|---|
| `UNAVAILABLE`, `DEADLINE_EXCEEDED` | Retry. Under unnamed tasks a `DEADLINE_EXCEEDED` is ambiguous — the task may already exist — so the retry may duplicate. At-least-once prefers that to loss; naming removes the ambiguity |
| `RESOURCE_EXHAUSTED` | Retry with backoff — the queue is over its limits and backing off is the only useful response |
| `NOT_FOUND` | Retry on a **separate, short budget**. A queue idle for 30 days takes "a few minutes to re-activate" and "some method calls may return `NOT_FOUND`" meanwhile, so this is not proof of a misconfigured queue — but a mistyped queue name must not burn the full retry budget on every record before failing |
| `ALREADY_EXISTS` | Success when the task carried a name, which is exactly when an extractor is configured. Terminal otherwise, since it should be unreachable |
| Everything else | Terminal — fails the job |

Mechanically, a retryable failure **parks** its `CreateTaskRequest` with a due time instead of
blocking anything, and the next `write()` or `flush()` re-dispatches whatever has come due. The
request is re-sent unchanged, so a named task keeps its id across attempts and a second attempt that
the first one already completed comes back as `ALREADY_EXISTS`, which is success. Delegating this to
the client's own `createTaskSettings` was considered and rejected: gax has one retryable-code set
and one schedule per method, so the separate `NOT_FOUND` budget could not be expressed, and a
sink-owned loop is testable against a fake client without simulating gax.

Two consequences worth stating. Parked creates **count against the in-flight cap** — they are
records Cloud Tasks has not accepted yet, so they have to bound memory the same way in-flight ones
do — and a `write()` at the cap with everything parked waits out the earliest backoff rather than
spinning. And parked creates are **dropped when the writer closes**: they are not covered by a
completed checkpoint, so the restart replays their records.

### Tuning

`CloudTasksWriterOptions` (nested-options pattern, every knob defaulted, set through
`writerOptions(...)`) is the whole surface. There are deliberately no rate knobs here — that is the
queue's job:

| Option | Default | What it does |
|---|---|---|
| `maxInFlightTasks` | 1000 | Caps outstanding creates, in flight plus parked. At the cap `write()` yields to the task mailbox until completions bring the count down |
| `retryInitialBackoff` | 100 ms | First backoff for `UNAVAILABLE` / `DEADLINE_EXCEEDED` / `RESOURCE_EXHAUSTED` |
| `retryMaxBackoff` | 10 s | Cap the backoff doubles up to |
| `retryMaxAttempts` | 8 | Total attempts, the first create included; exhausting the budget fails the job |
| `notFoundInitialBackoff` | 500 ms | First backoff of the `NOT_FOUND` budget |
| `notFoundMaxBackoff` | 2 s | Cap of the `NOT_FOUND` backoff |
| `notFoundMaxAttempts` | 3 | `NOT_FOUND` attempts. Short on purpose: long enough to ride out a blip, short enough that a mistyped queue fails quickly. A queue taking minutes to re-activate outlives this budget by design — recovering from that is the job's restart strategy, not the writer's |

The transient backoff carries ±20% jitter, so parallel subtasks backing off against the same queue
at the same instant do not retry in lockstep. It is not exposed: the value only has to be non-zero
to do its job. The `NOT_FOUND` schedule has no jitter, its budget being short enough that spreading
it out would mostly eat the budget.

## Queues, rate limits and sink concurrency

**There is no batch create available to a JVM sink.** `BatchCreateTasks` is documented in v2beta3
(long-running, explicitly non-atomic, 100 tasks maximum) and `BufferTask` is a GA v2 method — but
**neither exists in `google-cloud-tasks` 2.94.0**, not even on its v2beta3 surface: both are
REST-only, so dropping to a beta client would not buy batching either. The Java client also
configures no method with batching — there is no `BatchingSettings` in `CloudTasksSettings` or
`CloudTasksStubSettings` (the `BatchingCallSettings` references in the generated callable factories
are unwired boilerplate). So the sink owns batching, backpressure and concurrency outright, and
creation costs one RPC per record.

What the sink provides is the same mailbox-based bound the Pub/Sub sink uses: a cap on outstanding
creates (`maxInFlightTasks`, defaulting to 1,000 as the Pub/Sub sink's equivalent does), with
completions re-dispatched onto the task mailbox so all writer state stays single-threaded, and a
write at the cap yielding until completions bring the count down.
Create throughput is then sink parallelism × the in-flight cap, against a per-RPC latency that
naming increases.

What the sink does **not** provide is pacing. Two numbers bound the queue instead:

- **500 dispatches per second per queue** — a hard limit; `maxDispatchesPerSecond` cannot exceed it.
- **~1000 TPS per queue, creates plus dispatches** — Google "doesn't recommend" more, "as it will
  produce higher delivery latency than normal". Ramping past 500 TPS should follow the documented
  500/50/5 rule: increase by no more than 50% every 5 minutes.

Neither is a limit this connector can raise, and neither matters to the pipeline this connector
exists for — one that is throttling *down* to a third-party limit never approaches them. A pipeline
that does need more aggregate throughput shards across queues, which is what `destinationResolver`
is for.

**Queues are not created by the sink.** Unlike Pub/Sub topics and BigQuery tables, there is no
`CreateDisposition` here, for two reasons. An auto-created queue would carry Cloud Tasks' default
rate limits, silently discarding the pacing that is the entire reason to use the service — the sink
would be helpfully creating exactly the wrong thing. And queue creation is a one-way door: a deleted
queue name cannot be reused for 3 days, so a mistake is expensive to undo. The queue is a piece of
infrastructure the pipeline points at, like a Kafka topic with a retention policy.

Two queue states to be aware of, because neither produces an error at the sink: a **paused** queue
"will stop delivering tasks from it, but more tasks can still be added to it", and a **disabled**
queue behaves the same way. A pipeline writing to either sees a healthy sink while the backlog
grows. `DISABLED` is the rarer of the two — a queue cannot be disabled directly, only by uploading
a `queue.yaml`/`queue.xml` that omits it.

## Error handling

Terminal failures fail the job. Failures captured by completion callbacks are rethrown on the task
thread from the next `write()`/`flush()`, and `flush()` awaits every outstanding create, so a
failure cannot slip past a checkpoint barrier. Only the first terminal failure is kept: once one is
captured, later failures are not retried either, since the job is going to fail regardless. A
per-record failure policy — the `FailedRowHandler` analogue — is deferred to #37 along with the
other connectors'.

A failure that carries no gRPC status at all — neither a gax `ApiException` nor a raw
`StatusRuntimeException` — is treated as terminal rather than retried, on the grounds that an
unclassifiable failure is not evidence that retrying would help.

One limit is worth stating because the documentation contradicts itself: the maximum task size is
given as **100 KB** by the `CreateTask` API reference and the proto, and as **1 MiB** by the quotas
page. The sink does not validate against either; an oversized task is rejected by the service.
Bodies should be sized against the smaller number until this is verified empirically.

## Scope

| | v1 (#24) |
|---|---|
| Targets | HTTP only; App Engine targets deferred |
| Authorization | OIDC and OAuth tokens, per-record |
| Destinations | Fixed queue or per-record resolver |
| Deduplication | Opt-in named tasks, id hashed by the sink |
| Queue management | None — the queue must exist and be configured |
| Pacing | None in the sink; owned by the queue |
| Delivery | At-least-once, flush on checkpoint, stateless writer |
| Failure policy | Fail the job; per-record policy in #37 |
| Table API / SQL | Deferred to #99 |

## Testing

Unit tests ship with the sink (#24) and cover the builder, destination identity, the writer options,
the serialization schema and the writer itself against an in-memory fake `TaskCreator`, in the shape
the Pub/Sub module already uses. The retry paths run on an injected time source rather than real
sleeps, so backoff behaviour is asserted exactly instead of being waited out.

Integration tests are #25. They run against [`aertje/cloud-tasks-emulator`](https://github.com/aertje/cloud-tasks-emulator)
(MIT, published as `ghcr.io/aertje/cloud-tasks-emulator`) driven by testcontainers as a
`GenericContainer` — testcontainers' GCloud module has no Cloud Tasks support, and Google publishes
no official emulator. The emulator implements the v2 API, real HTTP dispatch, retries, rate limits
and named-task deduplication, so the interesting paths are reachable in CI without cloud
credentials. The builder option they hook into — `emulatorEndpoint("host:port")`, a plaintext
channel with no credentials, mirroring the Pub/Sub sink's — ships with #24 so that #25 is only
tests.

Three gaps the emulator leaves, to be covered by the real-GCP suite or not at all:

- It never garbage-collects task names — the dedup window cannot be exercised, only the
  `ALREADY_EXISTS` response. Tests reset with `--hard-reset-on-purge-queue` between scenarios.
- It does not implement `UpdateQueue`, so queue-level `httpTarget` routing — the override that can
  silently redirect per-record URLs — is not testable there. (Nor is it reachable through the v2
  client at all, as the targets section explains.)
- It authenticates **OIDC only** — its task dispatch has a single `GetOidcToken` branch — so the
  OAuth path in the v1 scope has no emulator coverage and needs real GCP or a hand-written fake.

## Provenance and attribution

This module is an original implementation. There is no Flink Cloud Tasks connector in Apache Flink,
in `GoogleCloudPlatform/pubsub` or elsewhere in open source to adapt, so unlike the Pub/Sub module
nothing is vendored. Its design references the Pub/Sub sink in this repository (the mailbox-based
in-flight cap, the flush-on-checkpoint stateless writer, the serialization-schema shape) and
Google's own Cloud Tasks documentation. No source code has been copied into this module.
