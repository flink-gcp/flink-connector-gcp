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

The sink ships in [#24]({{< param BookRepo >}}/issues/24); its emulator integration tests are [#25]({{< param BookRepo >}}/issues/25). This page doubles as the design
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
  Table API layer ([#99]({{< param BookRepo >}}/issues/99)) cheap: a `RowData` implementation slots in without reworking the sink.
- Returning `null` **skips** the record — it is written nowhere, is not a failure, and never
  reaches the failed-task handler — which is how a filter that depends on the task being built
  belongs in the serializer rather than upstream of the sink. Every serializer in this connector
  family reads `null` that way. A skip is counted by [`recordsSkipped`](#metrics), the only
  thing that reports it: a serializer skipping every record would otherwise leave an empty queue
  under a green job. The `httpTarget(...)` convenience cannot skip — Flink's `SerializationSchema`
  contract has no `null` in it, so a `null` body is reported as a serialization failure instead.
  The destination is resolved *before* the serializer runs, so a record the serializer would skip
  still needs a resolvable queue: a resolver returning `null` for it fails the job.
- `httpTarget(url)` returns a **non-generic stage** whose only method is
  `withBody(SerializationSchema<T>)`. That is what binds the record type, so everything chained
  after it — `withMethod`, `withUrl`, `withHeaders`, `withOidcToken`, `withOAuthToken` — infers `T`
  from the body schema and needs no type witness. Each `with*` returns a new immutable schema and
  the schema *is* what the builder takes, so there is no terminal `build()` call. `withUrl(...)`
  resolves the target URL per record, which is the routing the queue-level `uriOverride` warning
  below is about; the fixed URL the chain started from is the default.
- Cloud Tasks accepts a request **body only on `POST`, `PUT` and `PATCH`** — "it is an error to set
  body on a task with an incompatible `HttpMethod`". Since `withBody(...)` is what binds the record
  type, it cannot be omitted, so under any other method the schema sends no body and leaves the
  body schema unused. The alternative, rejecting those methods outright, would leave no way to
  dispatch a `GET` task short of implementing the SPI by hand.
- `QueueDestination` is pure queue identity (`equals`/`hashCode` over project, location and queue)
  and can be resolved per record through `destinationResolver(...)`, exactly as the BigQuery and
  Pub/Sub sinks resolve tables and topics. Unlike those two this costs nothing: Cloud Tasks has no
  per-destination connection or stream, so one `CloudTasksClient` serves every queue and dynamic
  destinations need no per-queue state. Resolvers run per record — cache and reuse
  `QueueDestination` instances.
- The location is part of the destination because queues are regional and a project may hold
  queues in several regions.
- `serviceAccountKeyFile(path)` selects a service-account JSON key when application-default
  credentials cannot select the required identity.
  The writer reads and scopes the file when it starts, so the path itself, rather than parsed
  credentials, is the only credential setting serialized in the job graph.
  The setter accepts a file path only, not raw or Base64-encoded JSON, access tokens, user
  credentials or custom provider classes.
  A read or parse failure reports neither the path, key material nor the parser cause.
  It is rejected beside `emulatorEndpoint(...)`, whose channel carries no credentials.
- `emulatorEndpoint("host:port")` points the sink at a Cloud Tasks emulator over a plaintext
  channel with no credentials, so it must only ever be used against an emulator — never against
  production Cloud Tasks. The setter parses it, so a malformed value is rejected at `build()` on
  the client rather than surfacing as a connection failure on a task manager
  ([#235]({{< param BookRepo >}}/issues/235)).

The service account used to create a task is separate from any OIDC or OAuth identity configured
on that task.
The first authenticates the Flink writer to the Cloud Tasks API; the second is a token that Cloud
Tasks attaches when it later calls the task's HTTP target.

## Credential file deployment

> **Authentication recommendation.** Google recommends [avoiding service-account keys whenever possible](https://cloud.google.com/iam/docs/best-practices-service-accounts#choose-when-to-use).
> Prefer keyless application-default credentials from an attached service account or Workload Identity over a service-account key file.
> Use `serviceAccountKeyFile(path)` only when the job must select an explicit service account that the process environment cannot provide.
>
> On Kubernetes, store the JSON key in a `Secret` and mount it as a read-only volume at the same absolute container path in every pod that may run the sink writer.
> This path is inside the container, not a path that merely exists on the Kubernetes node.
> Do not store credential material in a `ConfigMap` or a connector option.
> Mount the Secret directory rather than one file through `subPath` when in-place rotation is expected, because Kubernetes does not update a Secret mounted with `subPath`.
>
> On a session cluster, the same path must remain readable by every eligible TaskManager process, including replacement or newly allocated TaskManagers.
> Each writer reads the file once when it starts.
> Replacing or rotating the mounted file does not hot-reload credentials.
> Wait until a normally projected Secret has updated in every eligible pod before restarting the affected job; with a `subPath` mount, recreate the affected pods or cluster first.
> Replace the key in every workload that uses it and validate those workloads before disabling the replaced key.
> Monitor them after disabling it, then delete it after confirming that they still work, following Google's [service-account key rotation guidance](https://cloud.google.com/iam/docs/key-rotation#process).
>
> Mounting several job-specific keys into one shared session cluster weakens isolation because co-located jobs share the cluster environment.
> Prefer an application/per-job cluster with Workload Identity when jobs require separate identities.

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
accepted every record up to the barrier, other than those the serializer skipped by returning
`null` — the service returns `OK` only once the task "has been successfully written to Cloud Tasks
storage" — and the writer keeps nothing in Flink state, so discarding operator state can never lose
buffered records. This is the same model the Pub/Sub and BigQuery sinks use, and the reasoning against `AsyncSinkBase` recorded there applies unchanged.

That guarantee assumes the default `FailureHandler.failJob()` policy. Under `logAndDrop()` or
`sendToDeadLetterQueue(...)` a successful checkpoint means every record up to the barrier was
either durably accepted, [skipped by the serializer](#api-notes), or handed to the
[failed-task policy](#failed-task-policy), which says which failures reach it.

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
`writerOptions(...)`) is the whole surface, and every knob with its default is in the
[configuration reference]({{< relref "docs/reference/cloudtasks" >}}#cloudtaskswriteroptions).
There are deliberately no rate knobs among them — that is the queue's job.

One shape in that table needs the reasoning that used to sit in its own row. `NOT_FOUND` has a
**separate, short budget** because a queue idle for 30 days takes "a few minutes to re-activate" and "some method calls may
return `NOT_FOUND`" meanwhile, so it is not proof of a misconfigured queue — but a mistyped queue
name must not burn the full retry budget on every record before failing. A queue taking minutes to
re-activate outlives that budget by design: recovering from it is the job's restart strategy, not
the writer's.

Both backoffs carry ±25% jitter, so parallel subtasks backing off against the same queue at the
same instant do not retry in lockstep. The ratio is not exposed: the jitter is mean-preserving —
the backoff is multiplied by a factor in `[0.75, 1.25]`, so the expected delay is the configured
one — which is why even the short `NOT_FOUND` budget carries it.

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
captured, later failures are not retried either, since the job is going to fail regardless.

A failure that carries no gRPC status at all — neither a gax `ApiException` nor a raw
`StatusRuntimeException` — is treated as terminal rather than retried, on the grounds that an
unclassifiable failure is not evidence that retrying would help.

| Class | What it covers | What happens |
|---|---|---|
| Transient | `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED` | Parked and re-dispatched within the retry budget; exhausting it fails the job |
| Missing queue | `NOT_FOUND` | The same, on a [shorter budget of its own](#tuning); exhausting it fails the job |
| Deduplicated | `ALREADY_EXISTS` on a named task | Success — this is what `taskIdExtractor(...)` asked for |
| Task-level | `INVALID_ARGUMENT` (a malformed target URL, an oversized body, a header the service refuses) | Handed to the failed-task handler |
| Record-level | The serializer throws, or the task id extractor throws | Handed to the failed-task handler, before anything is sent |
| Terminal | `PERMISSION_DENIED`, failures carrying no status at all | Fail the ongoing write or checkpoint |

### Failed-task policy

Three data-shaped failures are pluggable: a record the serializer rejects, a task id extractor that
throws, and a creation the service rejects with `INVALID_ARGUMENT`. A record the serializer *skips*
by returning `null` is none of them: it is not a failure, so it never reaches the handler and is
counted by [`recordsSkipped`](#metrics) rather than `numRecordsSendErrors`. The policy is
`failedTaskHandler(...)`, taking the shared `FailureHandler<FailedTask>` SPI from
`flink-connector-gcp-base` ([#37]({{< param BookRepo >}}/issues/37) standardizes it across the
connectors in this repository):

```java
Sink<OrderEvent> sink =
        CloudTasksSink.<OrderEvent>builder()
                .queue(QueueDestination.of("my-project", "asia-northeast1", "webhooks"))
                .serializer(serializer)
                .failedTaskHandler(FailureHandler.logAndDrop())
                .build();
```

- `FailureHandler.failJob()` (default) — every per-task failure fails the checkpoint, which is the
  sink's behavior when nothing is configured
- `FailureHandler.logAndDrop()` — logs each failed task at WARN and drops it
- `FailureHandler.sendToDeadLetterQueue(...)` — forwards each failed task to a `DeadLetterQueue`
  (experimental), whose implementation the sink drives through a lifecycle: `open(context)` once
  when the writer is created (the context carries the subtask index and the writer's metric group),
  `offer(element)` per failed task — buffering is allowed — `flush()` at every checkpoint barrier
  and at end of input, always after the sink's own write path has drained (on return everything
  offered must be durable, throwing fails the checkpoint), and `close()` when the writer closes,
  which must not be relied on for persistence
- Custom handlers implement `FailureHandler<FailedTask>` — or `FailureHandler<FailedElement>`,
  which `failedTaskHandler(...)` accepts as-is (the parameter is contravariant), so one handler
  written against the shared contract serves every connector in this repository. Throwing from
  `handle` fails the checkpoint, returning drops the task. `FailedTask` carries the `Task` the
  serializer produced, or `null` when serialization itself failed; under the shared `FailedElement`
  contract it reports `getConnector()` (`"cloudtasks"`), `describeDestination()`
  (`projects/<p>/locations/<l>/queues/<q>`) and `getPayloadBytes()` — the **whole** serialized task,
  so a consumer recovers the target URL, the method, the headers and the authorization with
  `Task.parseFrom(bytes)`

**Classification is a precedence over the whole cause chain, not the first status found.** A
failure carrying a transient status *anywhere* in its chain is transient even if it also carries an
`INVALID_ARGUMENT`, so service instability can never produce a dead letter — the worst an unstable
Cloud Tasks does is fail the job. That is a property of the code rather than of the client library's
behavior: no gax failure carries two different statuses today, and the guarantee does not rest on
that staying true. `INVALID_ARGUMENT` is the one routed status because it is the one gRPC defines as
*"arguments that are problematic regardless of the state of the system"* — explicitly unlike
`FAILED_PRECONDITION` and `OUT_OF_RANGE`, whose problems *"may be fixed if the system state
changes"*, and both of which fail the job here. It is read from the chain's *first* classifiable
status, not searched for: an `INVALID_ARGUMENT` buried under an `INTERNAL` or an `UNKNOWN` describes
the inner call, and dropping a record over a server-side failure would be the mirror image of
dropping one over an outage.

**Only those three failures are routed, deliberately.** An outage must not reach a dropping handler,
or a service incident would bleed the stream one record at a time instead of backpressuring and
restarting; that is why an exhausted retry budget, an exhausted `NOT_FOUND` budget and
`PERMISSION_DENIED` all stay job failures. Configuration failures stay fatal for the mirror-image
reason: a destination resolver returning `null`, a serializer returning an already-named task, and a
task id extractor returning `null` or an empty key fail every record alike, so dropping them would
leave an empty queue under a green job — an extractor that *throws*, by contrast, is per-record, and
is routed.

That reasoning does not extend to a serializer that produces an *invalid task* for every record — a
bug that puts a malformed URL or an over-long header on all of them. Cloud Tasks rejects each one
individually, the sink cannot tell a systematic rejection from a per-record one (the classification
is the response's status code, not a judgement about the whole stream), and a dropping policy
discards the lot silently. Watch
[`numRecordsSendErrors`]({{< relref "docs/connectors/datastream/cloudtasks" >}}#metrics) rather than
the job status when running anything other than `failJob()`: it counts every task the handler
received, so a systematic rejection shows up as a rate rather than as a failure.

Dead-letter output is **at-least-once, for failures that recur on replay**: tasks are offered before
the checkpoint covering their originating records completes, so a restart replays those records and
a deterministic failure (an oversized body, a record the serializer cannot convert) is offered
again — consume the dead-letter destination idempotently or deduplicate by key. A failure that does
*not* recur on replay is preserved only if a completed checkpoint already flushed it. Exactly-once
dead-letter output is deliberately not offered: it would require the dead-letter write to join the
sink's own commit protocol, which no external destination can be enrolled in.

Note what a dropped task means here, and that it is not what Cloud Tasks calls a failure: this is a
task the service never accepted, so it never entered the queue and the queue's own retry
configuration never applies to it. A task that *is* created and whose HTTP target then fails is
retried by Cloud Tasks under the queue's `retryConfig`, entirely outside this sink's view.

### Dead-lettering to a Pub/Sub topic

`PubSubDeadLetterQueue` is this repository's one shipped `DeadLetterQueue` implementation
(experimental, [#211]({{< param BookRepo >}}/issues/211)). It publishes each failed element to a
Pub/Sub topic, and it sees failures through the shared `FailedElement` contract, so **one instance
serves every connector here**. It lives in the Pub/Sub module, so a Cloud Tasks job dead-lettering
this way adds `flink-connector-gcp-pubsub` as a dependency:

```java
CloudTasksSink.<OrderEvent>builder()
        .queue(QueueDestination.of("my-project", "asia-northeast1", "webhooks"))
        .serializer(serializer)
        .failedTaskHandler(
                FailureHandler.sendToDeadLetterQueue(
                        PubSubDeadLetterQueue.builder()
                                .topic(TopicDestination.of("my-project", "dead-letters"))
                                .build()))
        .build();
```

| Attribute | Value |
|---|---|
| `dlq-connector` | `cloudtasks` here |
| `dlq-destination` | the queue the task was bound for |
| `dlq-error` | the failure description, truncated to Pub/Sub's 1024-byte attribute-value limit and marked with `...` |
| `dlq-timestamp` | when the element was offered, ISO-8601 |
| `dlq-subtask` | the offering sink subtask's index |

The message **data** is the whole serialized `Task` — empty when serialization itself failed, which
is how a consumer tells the two apart — so a consumer recovers the target URL, the method, the
headers and the authorization with `Task.parseFrom(data)`. The failure's cause chain is not in the
envelope (it has no bounded string form); enable `DEBUG` logging on `PubSubDeadLetterQueue` to see
untruncated errors in the job logs.

Publishes are batched and awaited in `flush()`, so a rare failure costs no round trip of its own.
`maxOutstandingMessages` bounds what one checkpoint interval can accumulate when *every* record
fails — the default is 1000, `0` publishes each element synchronously (the narrowest loss window,
one round trip per element) and `-1` buffers until the flush. The topic must already exist: this
queue never creates one, because a dead-letter destination created on the fly is one nothing is
consuming. `flushTimeout` (60 s by default) bounds each wait a running job makes for those publishes
— at a checkpoint barrier, and whenever the outstanding bound fills — as one deadline covering all of
that wait's publishes. It bounds one wait, not what an interval spends. On expiry the wait throws and
the job fails, dropping nothing; the records behind the unpublished dead letters are replayed from
the last completed checkpoint. A Pub/Sub disturbance longer than the budget therefore fails the job
where the SDK's 600 s retry would have absorbed it, which is the trade the bound buys. `shutdownTimeout` (30 s by default) bounds the queue's own close, and
it is spent *after* the sink's own teardown — so a sink that dead-letters should budget for the sum
against Flink's `task.cancellation.timeout`. Full description on the
[Pub/Sub page]({{< relref "docs/connectors/datastream/pubsub" >}}#dead-lettering-to-a-pubsub-topic).

The queue reports what it published, what it still holds and how long its waits take, on
**this sink's** writer group — documented once, with the queue, under
[Dead-letter metrics]({{< relref "docs/connectors/datastream/pubsub" >}}#dead-letter-metrics). How
many tasks were dead-lettered in the first place is [`numRecordsSendErrors`](#metrics) here.

One limit is worth stating because the documentation contradicts itself: the maximum task size is
given as **100 KB** by the `CreateTask` API reference and the proto, and as **1 MiB** by the quotas
page. The sink does not validate against either; an oversized task is rejected by the service.
Bodies should be sized against the smaller number until this is verified empirically.

## Metrics

Registered on the sink writer's metric group, one set per subtask:

| Metric | Type | Meaning |
|---|---|---|
| `numRecordsSend` | counter (Flink standard) | records handed to the client library for creation |
| `numBytesSend` | counter (Flink standard) | their serialized size |
| `numRecordsSendErrors` | counter (Flink standard) | records routed to the failed-task handler |
| `recordsSkipped` | counter | records the serializer skipped by returning `null` — neither sent nor failed, and not broken down per queue |
| `inFlightTasks` | gauge | creations the service has not answered |
| `parkedTasks` | gauge | creations waiting out a retry backoff |
| `tasksDeduplicated` | counter | named tasks Cloud Tasks already held |
| `errorClass.CODE.errors` | counter | failed creation attempts by status code, `CODE` being a gRPC status name or `UNCLASSIFIED` |
| `destination.QUEUE.recordsSend`, `destination.QUEUE.sendErrors` | counter | the same two counts per queue, **only** with `perDestinationMetrics(true)` |

**`numRecordsSend` counts records, not creation attempts.** This sink owns its retries — a failed
creation is parked and re-dispatched — and the record is counted once, when the client first
accepted it, so a job working through an outage does not report itself as a busier one. Every
connector in this repository counts the same way, whether its retries live in the sink or inside the SDK, which
is what makes the number comparable across them. The consequence: `numBytesSend` is payload volume
rather than wire volume.

**`errorClass` counts every attempt, retryable ones included** — that is the difference from
`numRecordsSend`, and it is deliberate: the sum over `UNAVAILABLE`, `DEADLINE_EXCEEDED` and
`RESOURCE_EXHAUSTED` *is* the retry volume, which is why there is no separate retries counter. A
`NOT_FOUND` run is visible the same way, under its own name.

`tasksDeduplicated` counts the `ALREADY_EXISTS` answers that named tasks exist to produce. They are
successes, not failures: they appear in neither `numRecordsSendErrors` nor `errorClass`, so a job
whose replay is being deduplicated as intended shows a clean error picture. Compare it against
`numRecordsSend` to see how much of a replay the service absorbed.

**`numRecordsSendErrors` is the counter to watch when the handler is not `failJob()`.** It counts
exactly what reached `failedTaskHandler(...)` — a record the serializer rejected, a task id
extractor that threw, and a creation the service answered `INVALID_ARGUMENT` — whether the handler
then dropped the task or failed the job. A serializer bug that makes *every* task invalid is dropped
one at a time under a dropping policy, and this counter is what shows it while the job stays green.

**`perDestinationMetrics` is off by default**, and should stay off with a per-record
`destinationResolver`: Flink cannot unregister a metric, so every queue the job has written to keeps
its counters for the lifetime of the task. A fixed `queue(...)` is the case to switch it on for. For
the same reason the counters survive eviction — a queue seen again resumes its own totals rather
than restarting at zero.

`currentSendTime` is deliberately **not** set: a creation may sit parked through several backoffs,
so the interval this writer could measure would describe its own retry budget rather than the
service's response time. There is no committer either (the sink is single-phase), so Flink's
committer metrics do not apply.

## Scope

| | v1 ([#24]({{< param BookRepo >}}/issues/24)) |
|---|---|
| Targets | HTTP only; App Engine targets deferred |
| Authorization | OIDC and OAuth tokens, per-record |
| Destinations | Fixed queue or per-record resolver |
| Deduplication | Opt-in named tasks, id hashed by the sink |
| Queue management | None — the queue must exist and be configured |
| Pacing | None in the sink; owned by the queue |
| Delivery | At-least-once, flush on checkpoint, stateless writer |
| Failure policy | Job failure by default; pluggable per-task handler ([#207]({{< param BookRepo >}}/issues/207)) |
| Table API / SQL | Deferred to [#99]({{< param BookRepo >}}/issues/99) |

## Testing

Unit tests ship with the sink ([#24]({{< param BookRepo >}}/issues/24)) and cover the builder, destination identity, the writer options,
the serialization schema and the writer itself against an in-memory fake `TaskCreator`, in the shape
the Pub/Sub module already uses. The retry paths run on an injected time source rather than real
sleeps, so backoff behaviour is asserted exactly instead of being waited out.

Integration tests ([#25]({{< param BookRepo >}}/issues/25)) run against [`aertje/cloud-tasks-emulator`](https://github.com/aertje/cloud-tasks-emulator)
(MIT, published as `ghcr.io/aertje/cloud-tasks-emulator`) driven by testcontainers as a
`GenericContainer` — testcontainers' GCloud module has no Cloud Tasks support, and Google publishes
no official emulator. They need no cloud credentials, so CI runs them with no cloud setup — on
every pull request whose changes select this module
([#243]({{< param BookRepo >}}/issues/243)). They
reach the emulator the way a user would — through the production client factory in the mode
`emulatorEndpoint("host:port")` selects, a plaintext channel with no credentials mirroring the
Pub/Sub sink's — rather than through a test seam; the job tests additionally build the sink through
the public builder, so they are what covers the serializer's `open(...)` and the writer's
construction by the runtime. Queues are created by the tests, since the sink never creates one.

- **What the target receives.** The emulator dispatches over real HTTP, so tasks are asserted where
  they land: an HTTP server inside the test JVM, published to the container network with
  `Testcontainers.exposeHostPorts(...)` and addressed as `host.testcontainers.internal`. It records
  the method, path, body and headers of every dispatch, which is what separates a task the service
  accepted from a task that arrives as intended — the POST body and its headers, the empty body
  under `GET`, per-record URLs, and an OIDC token arriving as a `Bearer` JWT whose claims carry the
  configured service account and audience.
- **What the service stores.** Task creation is asserted against **paused** queues, which accept
  tasks without dispatching them; a running queue drops a task as soon as it completes, which would
  race every assertion about the task itself. Unnamed tasks are created one per record and a replay
  creates a second; `taskIdExtractor(...)` names them with the SHA-256 digest of the key, and a key
  replayed in a later flush cycle creates the task once — `ALREADY_EXISTS` classified as success by
  the gax client the sink ships with rather than by a synthesized exception. The same key routed to
  two queues stays two tasks, since the name is composed from the queue as well. A returned `flush`
  means every task is already there, with nothing left in flight or parked.
- **End to end.** MiniCluster jobs built through the public builder, fed by a rate-limited source:
  streaming with checkpointing, so the checkpoint flush runs while records are still arriving, and
  batch, where everything rides the end-of-input flush. Delivery is asserted at the target, so a
  lost flush shows up as a missing record — though neither job can tell a mid-stream flush from the
  final one, since the writer creates each task as it is written rather than buffering until the
  flush.

What the emulator leaves uncovered, for the real-GCP suite or for nothing at all:

- It never garbage-collects task names — the dedup window cannot be exercised, only the
  `ALREADY_EXISTS` response. (Its uniqueness check is also a non-atomic check-then-act, so a
  deduplication test has to sequence its writes into separate flush cycles rather than rely on two
  concurrent creations of one name colliding. Task names are keyed by their full path, so a queue
  per test is already a namespace per test and no `--hard-reset-on-purge-queue` is needed.)
- It does not implement `UpdateQueue`, so queue-level `httpTarget` routing — the override that can
  silently redirect per-record URLs — is not testable there. (Nor is it reachable through the v2
  client at all, as the targets section explains.)
- It authenticates **OIDC only** — its task dispatch has a single `GetOidcToken` branch — so the
  OAuth path in the v1 scope has no emulator coverage and needs real GCP or a hand-written fake.
- It offers no failure injection, so the transient retry budget stays a unit test against the fake
  creator. `NOT_FOUND` is the exception — a queue that was never created produces it, so one
  integration test spends that short budget end to end; it is also the only test that drives the
  park-and-re-dispatch loop on the real clock rather than an injected time source.
- Its `ListTasks` and `GetTask` ignore `response_view` and always return the full task, where Cloud
  Tasks omits the body and headers under the default `BASIC` view. The tests ask for `FULL`
  explicitly, so their assertions describe the service and not the emulator's leniency.
- It enforces no task-size limit, so a test of the limits above would assert the emulator's
  leniency rather than the service's behaviour. Scheduling semantics (`scheduleTime`,
  `dispatchDeadline`, which a custom serialization schema may set) and App Engine targets are
  likewise left to real GCP.

## Provenance and attribution

This module is an original implementation. There is no Flink Cloud Tasks connector in Apache Flink,
in `GoogleCloudPlatform/pubsub` or elsewhere in open source to adapt, so unlike the Pub/Sub module
nothing is vendored. Its design references the Pub/Sub sink in this repository (the mailbox-based
in-flight cap, the flush-on-checkpoint stateless writer, the serialization-schema shape) and
Google's own Cloud Tasks documentation. No source code has been copied into this module.
