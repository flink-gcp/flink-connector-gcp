---
title: Cloud Tasks
type: docs
weight: 30
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

# Cloud Tasks Connector

Start with the [Quickstart]({{< relref "docs/quickstart/cloudtasks" >}}) for the basic dispatch job,
or use the [Cloud Tasks examples]({{< relref "docs/examples/cloudtasks" >}}) for dynamic queues,
Table sink requests, cross-connector pipelines, and local development.
The [Table connector]({{< relref "docs/connectors/table/cloudtasks" >}}) owns DDL, formats,
writable metadata, and planner restrictions.

## Overview and setup

Cloud Tasks sink for Apache Flink, provided by the `flink-connector-gcp-cloudtasks` module.

The sink ships in [#24]({{< param BookRepo >}}/issues/24), and App Engine targets join it in
[#628]({{< param BookRepo >}}/issues/628).
Its emulator integration tests are [#25]({{< param BookRepo >}}/issues/25).
This page doubles as the design record: it explains what the connector does and why each decision
was taken, so the reasoning is settled once rather than re-argued per pull request.

### What this connector is for

Cloud Tasks is best understood as **an HTTP request dispatch queue that executes later while
respecting a rate limit**. A task is not data at rest; it is a request to call an endpoint, held by
the service until the queue's pacing allows it through.

That makes the sink useful in a specific shape: a Flink pipeline reads from somewhere fast (Kafka,
Pub/Sub) and needs to call an endpoint that cannot absorb the stream at source speed — most often a
third-party API with a documented rate limit. The queue is configured to whatever the API allows,
and Cloud Tasks paces the delivery, retries failures with backoff, and can schedule a task up to 30
days ahead. Doing the same inside Flink means writing a stateful throttle; doing it with a plain
HTTP sink means not doing it at all.

**The pacing lives on the queue, not in this sink.** The sink writes tasks; the queue decides how
fast they execute and when a failed dispatch retries.
The [queue and sink-concurrency section](#queues-rate-limits-and-sink-concurrency) separates those
service settings from the writer options.

{{< java-snippet file="CloudTasksConnectorOverview.java" tag="cloud-tasks-connector-overview" >}}

### API notes

- `CloudTasksSerializationSchema.serialize` returns a full `Task`, so every per-record field of a
  task — HTTP URL or App Engine relative URI/routing, method, headers, body, schedule time,
  dispatch deadline, authorization — is expressible. Two bounds worth knowing when using them:
  `schedule_time` may be at most 30 days ahead, and an HTTP target's `dispatch_deadline` must be
  between 15 seconds and 30 minutes. The task **name** is the exception: it is composed by the sink
  (see task naming below), so the
  returned `Task` must carry none — the writer rejects a named one rather than letting it through.
  The `httpTarget(...)` and `appEngineTarget(...)` entry points are conveniences over that
  contract, the same relationship `PubSubSerializationSchema.payload(...)` has to a full
  `PubsubMessage`. Returning the proto rather than a narrow record type is also what lets the Table
  API layer use the same sink without reworking the writer.
- Returning `null` **skips** the record — it is written nowhere, is not a failure, and never
  reaches the failed-task handler — which is how a filter that depends on the task being built
  belongs in the serializer rather than upstream of the sink. Every serializer in this connector
  family reads `null` that way. A skip is counted by [`recordsSkipped`](#metrics), the only
  thing that reports it: a serializer skipping every record would otherwise leave an empty queue
  under a green job. The `httpTarget(...)` and `appEngineTarget(...)` conveniences cannot skip —
  Flink's `SerializationSchema` contract has no `null` in it, so a `null` body is reported as a
  serialization failure instead.
  The destination is resolved *before* the serializer runs, so a record the serializer would skip
  still needs a resolvable queue: a resolver returning `null` for it fails the job.
- `httpTarget(url)` keeps its two-stage schema API: `withBody(SerializationSchema<T>)` binds the
  record type, and each later `with*` returns a new immutable schema without a terminal `build()`.
  `appEngineTarget(relativeUri)` instead returns a mutable `AppEngineTargetBuilder`. Its
  `withBody(...)` binds `T`, its optional `with*` methods update that builder, and terminal
  `build()` snapshots the settings into an immutable serializer. The serializer itself has no
  configuration methods. `withUrl(...)` resolves an HTTP URL per record; `withRelativeUri(...)`
  and `withRouting(...)` do the same for App Engine requests.
- The allowed body methods differ between the target protos. HTTP requests carry a body under
  `POST`, `PUT` and `PATCH`; App Engine requests carry one under `POST` and `PUT`. Since
  `withBody(...)` binds the record type, it cannot be omitted, so a bodyless method leaves the
  serializer unused. Rejecting those methods would leave no convenience path for `GET` tasks.
- `QueueDestination` is pure queue identity (`equals`/`hashCode` over project, location and queue)
  and can be resolved per record through `destinationResolver(...)`, exactly as the BigQuery and
  Pub/Sub sinks resolve tables and topics.
  Dynamic routing creates no per-queue service-client state: one `CloudTasksClient` serves every queue.
  Optional per-destination metrics are separate and keep counters for each queue with a recorded send or failure for the task lifetime.
  Resolvers run per record, so cache and reuse `QueueDestination` instances.
- The location is part of the destination because queues are regional and a project may hold
  queues in several regions.
- `serviceAccountKeyFile(path)` selects a service-account JSON key when application-default
  credentials cannot select the required identity.
  The eager writer or staged committer reads and scopes the file when it starts, so the path itself, rather than parsed
  credentials, is the only credential setting serialized in the job graph.
  The setter accepts a file path only, not raw or Base64-encoded JSON, access tokens, user
  credentials or custom provider classes.
  A read or parse failure reports neither the path, key material nor the parser cause.
  It is rejected beside `emulatorEndpoint(...)`, whose channel carries no credentials.
- `emulatorEndpoint("host:port")` points the sink at a Cloud Tasks emulator over a plaintext
  channel with no credentials, so it must only ever be used against an emulator — never against
  production Cloud Tasks. The setter parses it, so a malformed value is rejected by that call on
  the client rather than surfacing as a connection failure on a task manager
  ([#235]({{< param BookRepo >}}/issues/235)).

The service account used to create a task is separate from any OIDC or OAuth identity configured
on that task.
The first authenticates the Flink writer or committer to the Cloud Tasks API; the second is a token that Cloud
Tasks attaches when it later calls the task's HTTP target.

### Credential file deployment

> **Authentication recommendation.** Google recommends [avoiding service-account keys whenever possible](https://cloud.google.com/iam/docs/best-practices-service-accounts#choose-when-to-use).
> Prefer keyless application-default credentials from an attached service account or Workload Identity over a service-account key file.
> Use `serviceAccountKeyFile(path)` only when the job must select an explicit service account that the process environment cannot provide.
>
> On Kubernetes, store the JSON key in a `Secret` and mount it as a read-only volume at the same absolute container path in every pod that may run the sink writer or committer.
> This path is inside the container, not a path that merely exists on the Kubernetes node.
> Do not store credential material in a `ConfigMap` or a connector option.
> Mount the Secret directory rather than one file through `subPath` when in-place rotation is expected, because Kubernetes does not update a Secret mounted with `subPath`.
>
> On a session cluster, the same path must remain readable by every eligible TaskManager process, including replacement or newly allocated TaskManagers.
> The eager writer loads credentials when it starts; the staged committer loads them for retention readback and task creation when it starts.
> Replacing or rotating the mounted file does not hot-reload credentials.
> Wait until a normally projected Secret has updated in every eligible pod before restarting the affected job; with a `subPath` mount, recreate the affected pods or cluster first.
> Replace the key in every workload that uses it and validate those workloads before disabling the replaced key.
> Monitor them after disabling it, then delete it after confirming that they still work, following Google's [service-account key rotation guidance](https://cloud.google.com/iam/docs/key-rotation#process).
>
> Mounting several job-specific keys into one shared session cluster weakens isolation because co-located jobs share the cluster environment.
> Prefer an application/per-job cluster with Workload Identity when jobs require separate identities.

## DataStream sink

### Targets

Cloud Tasks stores the request target as a `oneof`, and the serialization API exposes both arms:
an external `HttpRequest` and an `AppEngineHttpRequest`.
The two target schemas cannot produce both on one task because each constructs its own protobuf
field directly.
The next two sibling sections keep both target arms visible in the page outline.

### App Engine targets

An App Engine task is delivered to the application in the **same project as its queue**.
The queue location must correspond to the application's permanent region, so the serializer does
not accept a second project or location that could disagree with the queue destination.
Transport is encrypted, stays inside Google's datacenters, and has no caller-selected protocol;
the handler still observes an HTTP request.

{{< java-snippet file="CloudTasksConnectorAppEngineTargets.java" tag="cloud-tasks-connector-app-engine-targets" >}}

The relative URI is empty for the root path, or begins with `/` and contains only a path and an
optional query string.
It contains no spaces or fragment and is at most 2083 characters.
`Host`, `Content-Length`, `X-Google-*` and `X-AppEngine-*` headers are owned by Cloud Tasks and are
rejected before task creation.

`AppEngineRouting` selects a service, version and instance.
Empty routing leaves all three choices to App Engine; routing extractors may return `null` for the
same result.
The proto's `host` is output-only and the typed schema rejects it.
A specific instance is usable only with manual scaling in App Engine Standard, which the gated
acceptance fixture in [#632]({{< param BookRepo >}}/issues/632) provides.

Queue configuration wins over the record: if `appEngineRoutingOverride` is present, Cloud Tasks
uses it for every task regardless of the task-level routing above.
The v2 client can read this queue field, unlike HTTP `uriOverride`, which requires REST or v2beta3.
The sink does not validate routing overrides; the staged mode's queue read checks name retention only.

App Engine handlers may be secure, unsecure, or restricted to `login: admin`; tasks do not run as
a user and therefore cannot reach `login: required` handlers.
Dispatch does not follow redirects.
Any 2xx response succeeds and other responses retry under the queue policy, with one congestion
distinction: App Engine `503` responses throttle queue dispatch, while handler-produced `429`
responses do not invoke congestion control.

### HTTP targets

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
so a v2 `GetQueue` returns an object that cannot carry the field at all.
The staged mode uses v2beta3 for retention readback, but neither mode validates `httpTarget` routing overrides.
Configure those independently; successful retention verification does not establish which URL receives a task.

### Task naming and deduplication

See [Write and key-collision semantics]({{< relref "docs/connectors/delivery-guarantees" >}}#write-and-key-collision-semantics)
for the Table and DataStream API comparison.

**The default at-least-once mode uses unnamed tasks.** Cloud Tasks assigns the name, task creation runs at full speed,
and a task that Flink replays after a failure is created twice.

Stable-key naming is opt-in through the **sink builder**, not the serializer:

This intentionally abbreviated chain assumes concrete `queue` and `serializer` values and omits
the final `build()` call.

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

With an extractor supplied, a repeated create for an id Cloud Tasks still remembers fails with
`ALREADY_EXISTS`, **which the sink treats as success**.
A replayed record still sends a create request; name retention suppresses another physical task creation.
It does not suppress duplicate execution of the task handler.

Task creation is not an upsert.
Cloud Tasks cannot update a task after creation, and the sink accepts `ALREADY_EXISTS` without
comparing the existing task's payload or schedule with the replayed record.
The extracted value must therefore identify an immutable logical task.
Include a content or schedule version in that value when a changed record must create another task.

The window follows Google's published service specification.
The [v2 task-creation reference](https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create) describes name collisions and reuse, including the distinction for queues created from `queue.yaml` or `queue.xml`.
The [v2beta3 Queue reference](https://docs.cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues) documents `tombstoneTtl`: after deletion or execution, the name remains protected for the configured duration.
The sink creates tasks through v2; `tombstoneTtl` is configured through the v2beta3 queue-administration API and is not a sink option or a field on the v2 Queue resource.
The live task also occupies its name.
A replay after the name is released can create another task.
Queue retention is administered outside the sink and is never updated by it.
The default at-least-once mode does not read retention or enforce a bounded recovery protocol.
The [checkpointed mode](#checkpointed-task-creation) checks retention by default and enforces its recovery deadline.
The connector's [support boundary]({{< relref "docs/connectors/delivery-guarantees" >}}#support-boundary) is the published specification for the API and configuration in use.
Refer to the official references above for service limits.

Naming is off in the default at-least-once mode because it is expensive, and the cost is Google's rather than this
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

An extracted key always becomes a digest; the serializer cannot supply a name that bypasses hashing.
The checkpointed mode also names tasks without an extractor, using persisted random identities instead.

The consequence to know: task names are not human-meaningful, so a task cannot be located in the
console from its business key. Passing a caller-chosen name through unhashed — the only thing that
would allow deduplication against tasks created by another system — is deferred until someone needs
it.

### Lineage

The sink returned by `CloudTasksSink.builder().build()` implements Flink's `LineageVertexProvider`.
A fixed `QueueDestination` contributes one dataset with namespace `cloudtasks://{project}/{location}` and name `{queue}`.
This is the project's naming convention.
Its `gcp` physical-resource facet has kind `cloudtasks-queue` and retains the configured `project`, `location` and `queue`.
HTTP and App Engine targets, named and unnamed tasks, and writer concurrency or retry settings use the same queue identity.

Extraction inspects the final configured resolver without invoking it.
A `FixedDestinationResolver` supplies its queue; a user-defined resolver returns a non-null vertex with an empty dataset list, even if the resolver would always choose one queue.
The last call to `queue(...)` or `destinationResolver(...)` determines which case applies.
Extraction does not open a client, resolve credentials, issue RPCs, invoke serialization or extract a task ID.
Individual tasks, HTTP URLs, App Engine handler routes, authentication subjects and payloads are not additional datasets.
Queue lineage does not prove task dispatch, handler execution or downstream business effects.

Flink 2.x extracts the sink metadata through its native lineage path.
Flink 1.20 supports the same direct metadata inspection, but has no automatic listener delivery.
See [Lineage]({{< relref "docs/connectors/lineage" >}}) for listener integration, the version contract and the limits of the custom `gcp` facet.

## Queues, rate limits and sink concurrency

This queue and runtime behavior applies to both DataStream and Table jobs.

`maxDispatchesPerSecond`, `maxConcurrentDispatches` and the dispatch retry policy are queue
configuration, applied by whoever creates the queue.
The sink options do not change how quickly Cloud Tasks delivers requests or retries a failed
handler invocation.

**This sink does not use a batch create, and creation costs one RPC per record.** `BufferTask` is a
GA v2 method that does not exist in the Java client at all. `BatchCreateTasks` does, but only on the
**v2beta3** surface — long-running, explicitly non-atomic, 100 tasks maximum — while this
connector targets v2. It was evaluated against a real queue and declined in
[#937]({{< param BookRepo >}}/issues/937)
([ADR-0129]({{< param BookRepo >}}/blob/main/docs/adr/0129-the-cloud-tasks-sink-keeps-one-create-rpc-per-record-and-declines-v2beta3-batchcreatetasks.md)
holds the measurements): batching was no faster than the sink's existing concurrent creates, and
a batch containing already-existing named tasks is rejected wholesale with a single
`ALREADY_EXISTS` — no per-task report, its non-duplicate half still silently created — which no
sink reporting per-task outcomes can reconcile. The Java client also
configures no method with gax batching — there is no `BatchingSettings` in `CloudTasksSettings` or
`CloudTasksStubSettings` (the `BatchingCallSettings` references in the generated callable factories
are unwired boilerplate). So the sink owns batching, backpressure and concurrency outright.

The at-least-once writer provides the same mailbox-based bound the Pub/Sub sink uses: a cap on outstanding
creates (`maxInFlightTasks`, defaulting to 1,000 as the Pub/Sub sink's equivalent does), with
completions re-dispatched onto the task mailbox so all writer state stays single-threaded, and a
write at the cap yielding until completions bring the count down.
The staged committer uses the same in-flight cap in a blocking loop; it has no mailbox context.
Create throughput is then bounded by sink parallelism × min(the in-flight cap, `channelPoolSize`
× ~100) concurrent creates, against a per-RPC latency that naming increases. The transport term is
the one the [#937]({{< param BookRepo >}}/issues/937) measurement surfaced: a single gRPC channel
carries ~100 concurrent streams, so once the cap sits at or above ~100, a subtask at the default
single channel tops out around ~210 creates/s however much higher the cap is raised. [#1015]({{< param BookRepo >}}/issues/1015) routed that ceiling
into the `channelPoolSize` knob under [Tuning](#tuning), whose default keeps the single channel.

What the sink does **not** provide is pacing. Two numbers bound the queue instead:

- **500 dispatches per second per queue** — a hard limit; `maxDispatchesPerSecond` cannot exceed it.
- **~1000 TPS per queue, creates plus dispatches** — Google "doesn't recommend" more, "as it will
  produce higher delivery latency than normal". Ramping past 500 TPS should follow the documented
  500/50/5 rule: increase by no more than 50% every 5 minutes.

Neither is a limit this connector can raise, and neither matters to the pipeline this connector
exists for — one that is throttling *down* to a third-party limit never approaches them.
A DataStream pipeline in at-least-once mode that needs more aggregate throughput can shard across queues with
`destinationResolver`.
A Table sink has one fixed queue, so a Table job must route rows explicitly to separately declared
sink tables to shard across queues.

**Queues are not created by the sink.** Unlike Pub/Sub topics and BigQuery tables, there is no
`CreateDisposition` here, for two reasons. An auto-created queue would carry Cloud Tasks' default
rate limits, silently discarding the pacing that is the entire reason to use the service — the sink
would be helpfully creating exactly the wrong thing.
A deleted queue name can also remain temporarily unavailable for reuse.
The [DeleteQueue reference](https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues/delete)
requires confirming an apparently successful recreation with `GetQueue`.
The queue is infrastructure the pipeline points at, like a Kafka topic with a retention policy.

Two queue states to be aware of, because neither produces an error at the sink: a **paused** queue
"will stop delivering tasks from it, but more tasks can still be added to it", and a **disabled**
queue behaves the same way. A pipeline writing to either sees a healthy sink while the backlog
grows. `DISABLED` is the rarer of the two — a queue cannot be disabled directly, only by uploading
a `queue.yaml`/`queue.xml` that omits it.

## Delivery guarantees and state

The default is `AT_LEAST_ONCE`, with eager task creation.
The opt-in DataStream `EXACTLY_ONCE` mode stages named tasks and commits them after checkpoint completion, as described below.
Both modes leave handler execution at-least-once.
The [Table API]({{< relref "docs/connectors/table/cloudtasks" >}}#checkpointed-task-creation) exposes both modes through `sink.delivery-guarantee` and maps `sink.staged.*` keys to the same runtime.

### At-least-once mode

See [Delivery guarantees]({{< relref "docs/connectors/delivery-guarantees" >}}) for the terms and
cross-connector comparison.

The sink is **at-least-once**, and its writer is **stateless by design**. Tasks are created
asynchronously through `createTaskCallable().futureCall(...)`, and on every checkpoint Flink calls
the writer's `flush()`, which waits for every outstanding create to complete — including those
waiting out a retry backoff. A successful checkpoint therefore means Cloud Tasks has durably
accepted every record up to the barrier, other than those the serializer skipped by returning
`null` — the service returns `OK` only once the task "has been successfully written to Cloud Tasks
storage" — and the writer keeps nothing in Flink state, so discarding operator state can never lose
buffered records. This is the same model the Pub/Sub and BigQuery sinks use, and the reasoning against
`AsyncSinkBase` recorded there applies unchanged.

That guarantee assumes the default `FailureHandler.failJob()` policy. Under `logAndDrop()` or
`sendToDeadLetterQueue(...)` a successful checkpoint means every record up to the barrier was
either durably accepted, [skipped by the serializer](#api-notes), or handed to the
[failed-task policy](#failed-task-policy), which says which failures reach it.

Checkpointing must be enabled in streaming jobs; without it `flush()` is never called mid-stream
and outstanding creates are lost on failure. Batch execution is covered by the end-of-input flush.

**Retries are this sink's responsibility, unlike every other connector here.** The generated client
gives `CreateTask` an *empty* set of retryable status codes and a 20-second total timeout — it is
`CloudTasksStubSettings` that says so, where `getTask`, `listTasks` and `deleteTask` all retry on
`DEADLINE_EXCEEDED`/`UNAVAILABLE` and `createTask` alone does not. The
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

### Checkpointed task creation

Select `CloudTasksDeliveryGuarantee.EXACTLY_ONCE` for **exactly-once task creation per staged envelope within the documented scope and recovery window**.
The writer creates no tasks: it serializes each accepted record into immutable named Task bytes, and Flink checkpoints those envelopes in its committer operator.
After the owning checkpoint completes, the committer creates one task per envelope with bounded concurrency.
Tasks become visible incrementally during that commit; a completed checkpoint is not an atomic visibility transaction across tasks or other sinks.

{{< java-snippet file="CloudTasksCheckpointedCreation.java" tag="cloud-tasks-checkpointed-creation" >}}

A configured `taskIdExtractor(...)` retains the existing SHA-256 naming and collapse behavior: the winning task is neither compared nor updated.
Without an extractor, each accepted record receives a fresh persisted 128-bit random identity, so identical payloads can be distinct tasks.
Retries, restore and rescaling reuse the original queue, name, bytes and staging origin without calling the serializer, resolver or extractor again.
A serializer returning `null` still skips and increments `recordsSkipped`.
The original schedule time is preserved; when it is already past after checkpoint and commit latency, the task is immediately eligible for dispatch under the queue's pacing and state.

The committer treats `ALREADY_EXISTS` as collision success.
It owns the same transient and separate `NOT_FOUND` retry budgets as the eager writer; the GAPIC client's `CreateTask` retries remain disabled.
An exhausted retry budget or terminal failure throws, leaving the checkpoint's envelopes available for recovery, including tasks whose creation already succeeded.
The next incarnation replays those names and receives collision success for the tasks the service remembers.

Four exclusions constrain this guarantee:

- **Handler execution:** Cloud Tasks may dispatch a task more than once; the handler needs its own idempotency or durable deduplication protocol.
- **Logical duplicates without a stable key:** two separately accepted records receive two identities even if they describe one event.
- **Unbounded service effects and administrative history:** client cancellation and deadlines do not exclude late service-side effects, including requests sent by an old process; purge, queue deletion/recreation and shortened name retention can remove replay protection.
- **Stop-with-savepoint without FINISHED:** a synchronous savepoint can create tasks before an unrelated operator or the commit itself fails; the job fails without automatic recovery; an external restart from an older checkpoint can stage those records under fresh random names.

The DataStream and Table implementations have [#1245]({{< param BookRepo >}}/issues/1245)'s adopted real-service recovery evidence; release of the mode still requires [#1246]({{< param BookRepo >}}/issues/1246)'s final performance verdict.
The earlier primitive measurements remain inconclusive; implementing this mode does not change that result.

### Recovery window and prerequisites

The send authorization rule is strict:

```text
deadline = origin + nameRetention - clockSkewAllowance - requestTimeout
send only while now < min(persistedDeadline, deadlineWithCurrentOptions)
```

Every first attempt and retry checks this rule immediately before sending, including work that waited for a slot or was restored.
The absolute gRPC request deadline is computed at that check, so a local delay before dispatch cannot restart the client budget.
The client deadline bounds client waiting, not the service's final effect.
Increasing retention or reducing the safety allowances on restore never extends a persisted envelope's deadline.
The defaults leave 54 minutes 40 seconds from staging, and time spent awaiting the checkpoint consumes that same window.

Configure an explicit STREAMING runtime, enabled exactly-once checkpoints and checkpoints after tasks finish; graph construction rejects other settings.
Bounded input in streaming mode is supported, with the final batch committed after a completed checkpoint.
Use one pre-provisioned fixed `queue(...)` and the built-in `FailureHandler.failJob()`; dynamic destination resolvers and dropping/dead-lettering handlers are rejected in this mode.

By default the committer reads the queue's v2beta3 `tombstoneTtl` and requires it to cover `nameRetention`.
An absent field uses the published one-hour default, so a larger assumption requires an explicit sufficient readback.
This requires `cloudtasks.queues.get` in addition to task-creation permissions and uses the same credentials as creation.
Readback RPC failures report the status code, or `UNCLASSIFIED` when none is available, without retaining vendor messages or exception causes.
An emulator endpoint skips this check because the emulator implements v2 only.
Setting `verifyQueueRetention(false)` leaves validation to the deployment operator; it does not remove the retention prerequisite.
The connector never creates queues or changes queue policy, and a current readback cannot prove historical retention.

The following are deployment requirements, not runtime guarantees a sink can enforce:

- Retain externalized checkpoints on failure and cancellation and use a restart strategy with a finite attempt limit.
- Recover from the latest completed checkpoint, preserve the sink UID and all mapped state, and never fork concurrent jobs from one checkpoint.
- Do not purge the queue, delete/recreate it or shorten retention while any staged or checkpoint-owned envelope can still be replayed; maintain clocks within the configured relative error allowance.

An at-least-once checkpoint can initialize the staged sink with no pending envelopes.
A downgrade with committer state fails state assignment; `allowNonRestoredState` bypasses that failure by losing the owned pending records and is outside the guarantee.
Restoring an envelope against a different fixed queue fails instead of redirecting its tasks.

### Heap and checkpoint sizing

The writer caps cover one batch: named Task bytes plus 256 bytes per envelope, up to 100,000 records or 64 MiB by default.
A full batch fails with a diagnostic recommending a shorter checkpoint interval, larger caps and heap, or less parallelism per host.
It never blocks `write()` waiting for a barrier that cannot pass that write.
The complete named task is capped at 100,000 wire bytes before staging.

Flink's committer holds one batch for every writer barrier since the last notified completion, including ordinary savepoints and failed checkpoints.
Neither checkpoint concurrency nor tolerable failures gives this batch count a ceiling.
Size and alert from each committer subtask's peak `pendingCommittables`, per-task size and representation copies, with headroom for state-backend buffers and parsed in-flight requests.
The existing four-representation probe observed up to 7x accounted bytes; this is an observation, not a runtime heap bound.

The blocking commit delays the committer's next barrier.
For each pending batch, budget all waves of `maxInFlightTasks`, all request attempts and the worst-case backoffs including jitter; sum across every batch released by the notification and add scheduling/checkpoint overhead.
A task can spend both retry budgets, so a conservative mixed-status attempt bound is `recoveryMaxAttempts + notFoundRecoveryMaxAttempts - 1`.
At the defaults that is ten 20-second requests plus up to 17.75 seconds of combined backoff, about 218 seconds per full worst-case wave.
Set `execution.checkpointing.timeout` above the resulting residence time while keeping recovery inside the authorization window.
If the workload and retry budget cannot fit both limits, reduce the staged workload/budgets or increase actual queue retention and the configured assumption together.
A timeout/restart loop is not a way to extend the durable authorization deadline.

### Recovery runbook

On terminal failure or expiry, stop bounded automatic retries and locate the latest retained externalized checkpoint.
Verify that its metadata and referenced state remain readable before recovery; keep an untouched copy of the recovery point and the original job settings.
The expiry error names the queue, staging origin, effective deadline, observed clock and `expiredEnvelopePolicy`.
Corrupt state, unknown deadlines, failed retention verification and incompatible destinations fail independently of that policy.

Within the window, repair the queue permissions/configuration or other failure cause, then restore the latest checkpoint under the same sink UID and queue.
A poison envelope is retried unchanged: changing the serializer cannot repair its already checkpointed bytes.
Do not edit checkpoint bytes or use an expiry policy as a poison-state handler; retain the state and repair the service-side rejection, or make an explicit out-of-guarantee recovery decision using the application's records.

After expiry, an operator can choose one of these explicit policies in `CloudTasksStagedOptions`, or the matching kebab-case value of the Table `sink.staged.expired-envelope-policy` key:

| Policy | Effect and operator decision |
|---|---|
| `FAIL` | Sends nothing for the expired envelope and fails; keep the checkpoint for investigation |
| `ASSUME_COMMITTED` | Completes expired envelopes without sending; justified for resuming a stop-with-savepoint that demonstrably reached FINISHED |
| `CREATE_ANYWAY` | Sends the original named bytes despite expiry, accepting possible duplicate tasks |
| `DROP` | Discards expired envelopes without sending, accepting possible lost tasks or relying on handler records proving completion |

These overrides are explicit loss/duplicate-risk decisions outside the guarantee and apply to expired envelopes, not arbitrary failures.
Monitor the [expiry override counters](#checkpointed-creation-metrics) to see which decisions actually run.
Return to `FAIL` after the chosen recovery operation; leaving an override configured also affects later expired work.

A successful stop-with-savepoint reaches FINISHED only after commit completes, yet its saved committer state still contains the pre-commit requests.
Resume it normally within the window; after the window, `ASSUME_COMMITTED` is the documented override for this completed-stop case.
If a savepoint was created but the stop never reaches FINISHED, Flink 1.20.4 and 2.2.1 fail the job with a non-recoverable `StopWithSavepointStoppingException`, even with a restart strategy.
Find the savepoint path in that exception and resume from it rather than the older completed checkpoint.
Prevent deployment automation from restarting the job from an older checkpoint; if it has already done so, cancel that replacement before its next checkpoint completes.
The savepoint replays the same names, including its partially completed creates; letting a job restored from the older checkpoint commit can create duplicate tasks under new random names.
Stable application keys avoid that fresh-name hazard but do not make handler execution exactly once.

## Error handling

The policy and callback descriptions in this section apply to the default at-least-once writer.
Checkpointed creation requires `failJob()` and throws from the committer on every terminal creation failure; use the [recovery runbook](#recovery-runbook).

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
| Task-level | `INVALID_ARGUMENT` (a malformed request target, an oversized body, a header the service refuses) | Handed to the failed-task handler |
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

{{< java-snippet file="CloudTasksConnectorFailedTaskPolicy.java" tag="cloud-tasks-connector-failed-task-policy" >}}

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
  so a consumer recovers the request target, the method, the headers and any authorization with
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
bug that puts a malformed request target or an over-long header on all of them. Cloud Tasks rejects each one
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
configuration never applies to it. A task that *is* created and whose request target then fails is
retried by Cloud Tasks under the queue's `retryConfig`, entirely outside this sink's view.

### Dead-lettering to a Pub/Sub topic

`PubSubDeadLetterQueue` is this repository's one shipped `DeadLetterQueue` implementation
(experimental, [#211]({{< param BookRepo >}}/issues/211)). It publishes each failed element to a
Pub/Sub topic, and it sees failures through the shared `FailedElement` contract, so **one instance
serves every connector here**. It lives in the Pub/Sub module, so a Cloud Tasks job dead-lettering
this way adds `flink-connector-gcp-pubsub` as a dependency:

{{< java-snippet file="CloudTasksConnectorDeadLettering.java" tag="cloud-tasks-connector-dead-lettering" >}}

`PubSubDeadLetterQueue.builder().serviceAccountKeyFile(path)` selects credentials for the dead-letter
publisher independently of this Cloud Tasks sink's credentials.
Each sink writer reads the file when it opens the queue, so the path must be readable on every
TaskManager that can run the sink.
If the setting is absent, the queue uses application-default credentials.
The Pub/Sub [credential file deployment]({{< relref "docs/connectors/datastream/pubsub" >}}#credential-file-deployment)
note covers Kubernetes Secret mounts, session clusters and rotation.

| Attribute | Value |
|---|---|
| `dlq-connector` | `cloudtasks` here |
| `dlq-destination` | the queue the task was bound for |
| `dlq-error` | the failure description, truncated to Pub/Sub's 1024-byte attribute-value limit and marked with `...` |
| `dlq-timestamp` | when the element was offered, ISO-8601 |
| `dlq-subtask` | the offering sink subtask's index |

The message **data** is the whole serialized `Task` — empty when serialization itself failed, which
is how a consumer tells the two apart — so a consumer recovers the request target, the method, the
headers and any authorization with `Task.parseFrom(data)`. The failure's cause chain is not in the
envelope (it has no bounded string form); enable `DEBUG` logging on `PubSubDeadLetterQueue` to see
untruncated errors in the job logs.

Publishes are batched and awaited in `flush()`, so a rare failure costs no round trip of its own.
`maxInFlightMessages` bounds what one checkpoint interval can accumulate when *every* record
fails — the default is 1000, `0` publishes each element synchronously (the narrowest loss window,
one round trip per element) and `-1` buffers until the flush. The topic must already exist: this
queue never creates one, because a dead-letter destination created on the fly is one nothing is
consuming. `flushTimeout` (60 s by default) bounds each wait a running job makes for those publishes
— at a checkpoint barrier, and whenever the in-flight bound fills — as one deadline covering all of
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
page. The at-least-once writer leaves this validation to the service; the staged writer caps the complete named Task at 100,000 wire bytes before checkpointing.
Bodies should be sized against the smaller number until this is verified empirically.

## Metrics

Registered on the current at-least-once sink writer's metric group, one set per subtask:

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

**`errorClass` counts every failed attempt, retryable ones included** — that is the difference from
`numRecordsSend`, and it is deliberate. It is not an exact retry counter: first failures count, and
the metric uses the outermost status while retry routing scans the whole exception chain, so a retry
selected by a nested transient status appears under the outer status instead. There is no separate
exact retries counter. A `NOT_FOUND` run is visible the same way, under its own name.

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
its counters for the lifetime of the task.
A fixed `queue(...)` is the case to switch it on for.
Because the registry entries cannot be removed, a queue seen again resumes its own totals rather than restarting at zero.

`currentSendTime` is deliberately **not** set: a creation may sit parked through several backoffs,
so the interval this writer could measure would describe its own retry budget rather than the
service's response time. The at-least-once writer has no committer, so Flink committer metrics apply only to the staged mode below.

### Checkpointed creation metrics

The `EXACTLY_ONCE` writer registers the gauges below and retains `recordsSkipped`.
Its `numRecordsSendErrors` counts staging serialization failures and extractor exceptions, which fail the job, while it never increments send or sent-byte counters.

| Metric | Type | Meaning |
|---|---|---|
| `stagedTasks` | gauge | tasks still owned by the writer before `prepareCommit()` |
| `stagedBytes` | gauge | their named Task wire bytes plus 256 bytes per task |
| `oldestStagedTaskAgeMillis` | gauge | oldest writer-owned envelope age in milliseconds, or -1 when empty |
| `stagedReplayBudgetMillis` | gauge | shortest remaining writer-owned authorization budget in milliseconds, or -1 when empty |

The count and bytes gauges reset to zero and the time gauges to -1 when the writer transfers its batch and exclude committables already held by Flink's collector.
They therefore describe one writer batch, not the total pending checkpoint backlog or actual live heap.

The committer exposes the current invocation's time window and expiry decisions:

| Metric | Type | Meaning |
|---|---|---|
| `currentCommitOldestTaskAgeMillis` | gauge | oldest envelope age across the entire executing commit invocation, or -1 when idle |
| `currentCommitReplayBudgetMillis` | gauge | shortest effective authorization budget across that invocation in milliseconds, or -1 when idle |
| `expiredEnvelopesFailed` | counter | expired envelopes rejected by the FAIL policy before authorizing a create |

Time gauges read the wall clock when sampled, clamp elapsed age and remaining budget at zero and saturate an overflowing positive difference.
The committer uses the earlier of each persisted deadline and the deadline under current settings, so a relaxed configuration never extends its observation of an old envelope's window.
Its invocation minima include queued tasks and tasks that completed earlier in the same invocation, making them conservative until the invocation exits on success or failure.
They contain no payload copies and are cleared before operator shutdown, not measured only at close.

The time gauges cannot observe envelopes parked in Flink's collector before a commit invocation begins.
Use Flink's full pending count for that backlog and alert on stalled checkpoints and pending growth as well as dwindling observed replay budgets.
An idle value of -1 does not establish that the collector is empty.
An invocation stops at its first expired `FAIL` rejection, so `expiredEnvelopesFailed` does not count the entire expired backlog.
Counter increments immediately followed by job failure might not be scraped; retain exception and job-status diagnostics for terminal expiry.
These definitions apply equally to DataStream and Table jobs.

Explicit overrides are counted separately:

| Metric | Type | Meaning |
|---|---|---|
| `expiredEnvelopesAssumedCommitted` | counter | expired envelopes completed by `ASSUME_COMMITTED` without sending |
| `expiredEnvelopesDropped` | counter | expired envelopes discarded by `DROP` without sending |
| `expiredEnvelopeCreatesAuthorized` | counter | expired create attempts authorized by `CREATE_ANYWAY`, including retries |

These counters report decisions in the current committer incarnation, not unique physical tasks.
Restore can count an envelope again, and an authorized create may still fail or never reach the service.
Flink's successful-committables counter also includes `DROP`, so use `expiredEnvelopesDropped` to distinguish that outcome.

The committer registers `tasksDeduplicated` for actual `ALREADY_EXISTS` responses.
Its `errorClass.CODE.errors` counts failed creation attempts, including client timeouts, excluding collisions and stale callbacks.
With `perDestinationMetrics(true)`, its `destination.QUEUE.recordsSend` counts envelopes first submitted during each commit invocation and `destination.QUEUE.sendErrors` counts terminal RPC failures.
Restore can count an envelope again; those counters are not unique task counts, and expiry before sending increments neither.
Flink supplies `pendingCommittables`, `totalCommittables`, `successfulCommittables`, `alreadyCommittedCommittables`, `failedCommittables` and `retriedCommittables` per committer subtask.
`signalAlreadyCommitted()` feeds `alreadyCommittedCommittables`, including the explicit `ASSUME_COMMITTED` override; the connector collision counter only counts service collision responses.
The connector throws on terminal failure and owns retrying internally, so Flink's failed/retried committable counters do not measure the RPC failures or attempts.
Use the exception and job status to diagnose terminal failures and alert on pending committable growth.
See [heap and checkpoint sizing](#heap-and-checkpoint-sizing) for the backlog these gauges do not cover.

## Tuning

`CloudTasksWriterOptions` (nested-options pattern, every knob defaulted, set through
`writerOptions(...)`) tunes both creation paths; `stagedOptions(...)` adds the checkpointed mode's staging and recovery limits. Every knob with its default is in the
[configuration reference]({{< relref "docs/reference/cloudtasks" >}}#cloudtaskswriteroptions).
There are deliberately no rate knobs among them — that is the queue's job.

`channelPoolSize` is the one transport knob, and it is not a rate knob either: it sizes how much
of the in-flight cap the transport can actually carry. The client's default transport opens a
single gRPC channel, and one HTTP/2 channel carries ~100 concurrent streams, so at the default one
subtask runs about 100 concurrent creates no matter how high `maxInFlightTasks` is set — measured
in [#937]({{< param BookRepo >}}/issues/937) as ~210 creates/s per subtask at the default against
1,271/s with an 8-channel pool. Throughput is concurrency divided by a per-RPC latency that itself
grows with load — queueing took the measured p50 from ~50 ms at low concurrency to ~280 ms at
100-way — which is why eight channels bought ~6× rather than 8×; expect the pool to scale
sub-linearly. The default deliberately stays at the client's single channel
([ADR-0134]({{< param BookRepo >}}/blob/main/docs/adr/0134-the-cloud-tasks-channel-pool-is-an-explicit-knob-defaulting-to-the-clients-single-channel.md)):
a pool sized from the cap would have silently pushed jobs toward the queue's [recommended
~1,000 TPS ceiling](#queues-rate-limits-and-sink-concurrency), which that 8-channel figure already
exceeds. Raising it is therefore a deliberate act — size the pool at about one channel per 100
concurrent creates you actually want, and mind the 500/50/5 ramp rule above. Beside
`emulatorEndpoint` the knob is rejected at `build()`: the emulator always uses one plaintext
channel, so a configured pool would otherwise be silently ignored.

The retry-status table in [Delivery guarantees and state](#delivery-guarantees-and-state) gives
`NOT_FOUND` a **separate, short budget** because a queue idle for 30 days takes "a few minutes to re-activate" and
"some method calls may return `NOT_FOUND`" meanwhile, so it is not proof of a misconfigured queue —
but a mistyped queue name must not burn the full retry budget on every record before failing. A
queue taking minutes to re-activate outlives that budget by design: recovering from it is the job's
restart strategy, not the writer's.

Both backoffs carry ±25% jitter, so parallel subtasks backing off against the same queue at the
same instant do not retry in lockstep. The ratio is not exposed: the jitter is mean-preserving —
the backoff is multiplied by a factor in `[0.75, 1.25]`, so the expected delay is the configured
one — which is why even the short `NOT_FOUND` budget carries it.

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
  configured service account and audience. The assertions parse the payload as JSON and accept
  a single audience encoded as a string or a one-element array, as allowed by
  [RFC 7519 section 4.1.3](https://www.rfc-editor.org/rfc/rfc7519#section-4.1.3).
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

The gated real-GCP suite establishes App Engine routing and dispatch behavior against the service.
It runs through the production writer and creates one uniquely named, paused queue per case.
`FULL` task reads assert fixed and per-record relative URIs, bodies, headers and exact
service/version/instance routing before dispatch can delete a successful task.
For a record whose routing extractor returns `null`, the service leaves those three selectors
empty but populates the output-only routing host; the suite checks the selectors rather than
mistaking that canonical response for task-level routing.
The suite also reads a queue-level `appEngineRoutingOverride`, asserts that a `204` response removes
the task, and observes failed attempts for the fixture's `503` and `302` handlers.
Each queue is deleted after its case and again after the class as a fallback.

The manually scaled App Engine fixture is started only for this class.
The lifecycle wrapper waits for exactly one serving instance, exports that instance together with
the fixture service and version, and stops the version on normal exit and handled `INT`/`TERM`
before checking that it is `STOPPED` with zero instances.
It preserves a test or signal exit status if cleanup also fails, while still surfacing a cleanup
failure after a successful test.
The scheduled sweep restores the stopped state after a hard cancellation that cannot run shell cleanup.
The remaining gated suites run after the fixture has returned to its idle state.

The checkpointed-creation recovery acceptance has a separate, manually approved
[protocol]({{< param BookRepo >}}/blob/main/docs/adr/evidence/0158-cloudtasks-recovery-1245.md).
It captures real creation responses before injecting response loss and verifies original
creation generations through task readback.
After two partial runs stopped on a list-reservation failure and a direct-path
checkpoint-observation timeout, a third approved run passed all 56 recovery cases on each of
Flink 1.20.4 and 2.2.1, including the direct production paths.
All twelve names returned `ALREADY_EXISTS` at both positive observations, but none could be
recreated within the registered negative-control period, despite 105 attempts per name.
All three runs verified cleanup.
The owner's [acceptance clarification]({{< param BookRepo >}}/blob/main/docs/adr/evidence/0158-cloudtasks-recovery-1245.md#acceptance-clarification-on-2026-09-10)
adopts this recovery evidence for [#1245]({{< param BookRepo >}}/issues/1245), with actual
post-tombstone recreation treated as optional additional calibration.
Longer name protection for the same logical task does not create a duplicate; the connector
still stops expired envelopes under their original deadline.
The slow test remains failed under its original criterion, and actual service-side recreation
is unobserved. The production recovery window and service assumptions are unchanged.
Local model results do not establish service retention, and ten-day automatic task expiry is
not measured by this experiment.

Limits of the existing eager-mode coverage:

- The deduplication tests assert `ALREADY_EXISTS` using separate flush cycles and a queue per
  test. They do not establish the service's task-name reservation window.
- The emulator does not implement `UpdateQueue`, so queue-level `httpTarget` routing — the override that can
  silently redirect per-record URLs — is not testable there. (Nor is it reachable through the v2
  client at all, as the targets section explains.)
- App Engine request protobufs, validation and routing precedence are covered by deterministic
  tests. The gated real-GCP suite establishes queue-level override and handler behavior; local
  emulator dispatch cannot establish the service's routing or authentication behavior.
- The emulator's HTTP dispatch authenticates **OIDC only**, so the
  OAuth path in the v1 scope has no emulator coverage and needs real GCP or a hand-written fake.
- The emulator offers no failure injection, so the transient retry budget stays a unit test against the fake
  creator. `NOT_FOUND` is the exception — a queue that was never created produces it, so one
  integration test spends that short budget end to end; it is also the only test that drives the
  park-and-re-dispatch loop on the real clock rather than an injected time source.
- The task-inspection tests request `FULL` explicitly so they can inspect the body. They do not
  assert the default `BASIC` response view.
- Task-size boundaries and scheduling semantics (`scheduleTime` and `dispatchDeadline`, which a
  custom serialization schema may set) remain real-service measurements. Emulator results do not
  establish those service limits.

## Scope

| | Current |
|---|---|
| Targets | HTTP and App Engine; fixed and per-record request routing |
| Authorization | HTTP: OIDC and OAuth tokens; App Engine: internal dispatch identity |
| Destinations | Fixed queue; per-record resolvers in at-least-once mode |
| Deduplication | Stable-key hashes when configured; checkpointed mode otherwise persists random names |
| Queue management | None — the queue must exist and be configured |
| Pacing | None in the sink; owned by the queue |
| Delivery | At-least-once by default; DataStream checkpointed creation within its documented scope and recovery window |
| Failure policy | Job failure by default and required for checkpointed creation; at-least-once supports pluggable per-task handlers ([#207]({{< param BookRepo >}}/issues/207)) |
| Table API / SQL | HTTP implemented in [#605]({{< param BookRepo >}}/issues/605); App Engine implemented in [#634]({{< param BookRepo >}}/issues/634) |

## Provenance and attribution

This module is an original implementation. No source code has been copied into it.

The mailbox-based in-flight cap, flush-on-checkpoint stateless writer, and serialization-schema
boundary were designed by comparison with this repository's Pub/Sub sink; issue
[#23]({{< param BookRepo >}}/issues/23) records that internal design lineage.
Cloud Tasks-specific request and queue behavior follows Google's Cloud Tasks documentation.
[ADR-0048]({{< param BookRepo >}}/blob/main/docs/adr/0048-the-cloud-tasks-sink-owns-its-retry-loop-and-never-creates-queues.md) records retry and delivery decisions where generated-client and protocol evidence resolves gaps or conflicts in that documentation.
