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

> **This page is a design note.** Nothing described here is implemented yet — the sink is built in
> #24 and its integration tests in #25. It records the decisions those issues implement, so they
> are settled once rather than re-argued per pull request. Statements in the present tense describe
> the intended behaviour, not shipped behaviour.

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
  expressible. The `httpTarget(...)` entry point plus the `with*` layering above is a convenience
  over that contract, the same relationship `PubSubSerializationSchema.dataOnly(...)` has to a full
  `PubsubMessage`. Returning the proto rather than a narrow record type is also what keeps the
  Table API layer (#99) cheap: a `RowData` implementation slots in without reworking the sink.
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
describes explicit App Engine targets as "less common", they cannot be distributed across regions
(a project has exactly one App Engine application, and its queues must live in that application's
region), and they invert the HTTP target's overload behaviour — a `503` throttles the queue while a
`429` does not. Supporting a second target type would double the serializer surface for a shrinking
audience; it can be added later without breaking the HTTP path.

The endpoint must be reachable from Cloud Tasks. Google's documentation is explicit: handlers "can
be run on any HTTP endpoint with an **external IP address** such as GKE, Compute Engine, or even an
on-premises web server". There is no VPC connector, Private Service Connect or serverless VPC
access for HTTP targets, so an endpoint that only has an internal IP cannot be a target at all.
Cloud Run and Cloud Run functions are ordinary HTTP targets — there is no special integration.

Authorization is chosen by **what is being called**, not by preference:

| Target | Token |
|---|---|
| Cloud Run, Cloud Run functions, anything else running on Google Cloud behind IAM | OIDC (`withOidcToken(serviceAccount[, audience])`) |
| Google APIs on `*.googleapis.com` | OAuth access token (`withOauthToken(serviceAccount[, scope])`) |
| A third-party endpoint with its own scheme | Neither — carry the credential in a header |

The builder therefore does not present the two as interchangeable knobs; setting both is rejected,
since the underlying field is a `oneof`.

**Queue-level routing can silently override the task's URL.** A queue may carry an
`httpTarget.uriOverride`, and its `uriOverrideEnforceMode` defaults to `ALWAYS`, which Google
documents as "queue-level configuration overrides all task-level configuration". A pipeline that
resolves URLs per record against such a queue will see every task go to the queue's URL instead,
with no error anywhere. The sink cannot detect this without an extra `GetQueue` call and the
permission to make it, so v1 documents the interaction rather than guarding it; a preflight warning
is a candidate follow-up if it bites in practice.

## Task naming and deduplication

**The default is unnamed tasks.** Cloud Tasks assigns the name, task creation runs at full speed,
and a task that Flink replays after a failure is created twice.

Naming is opt-in through the serializer:

```java
CloudTasksSerializationSchema.httpTarget(url)
        .withBody(schema)
        .withTaskId(OrderEvent::orderId)   // opt in to deduplication
```

With a task id supplied, a repeated create for an id Cloud Tasks has already seen fails with
`ALREADY_EXISTS`, **which the sink treats as success**. A replayed record therefore does not
produce a second call to the endpoint, which is as close to exactly-once as this service reaches.
The window is bounded: Google documents "up to 24 hours" for an id to be released after the task is
created, deleted or executed (9 days for queues created from a `queue.yaml`). A replay later than
that duplicates.

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
first — an event id, an offset, a timestamp — so `withTaskId(...)` takes any string and the sink
derives the actual task id as its SHA-256 digest. The footgun is removed by construction rather
than by a warning in a document nobody reads, deduplication is unaffected (the same key always
hashes the same way), and the digest fits comfortably inside the 500-character limit for the
`[A-Za-z0-9_-]` id.

The consequence to know: task names are not human-meaningful, so a task cannot be located in the
console from its business key. Passing a caller-chosen name through unhashed — the only thing that
would allow deduplication against tasks created by another system — is deferred until someone needs
it.

## Delivery guarantees and state

The sink is **at-least-once**, and its writer is **stateless by design**. Tasks are created
asynchronously through `createTaskCallable().futureCall(...)`, and on every checkpoint Flink calls
the writer's `flush()`, which waits for every in-flight create to complete. A successful checkpoint
therefore means Cloud Tasks has durably accepted every record up to the barrier — the service
returns `OK` only once the task "has been successfully written to Cloud Tasks storage" — and the
writer keeps nothing in Flink state, so discarding operator state can never lose buffered records.
This is the same model the Pub/Sub and BigQuery sinks use, and the reasoning against
`AsyncSinkBase` recorded there applies unchanged.

Checkpointing must be enabled in streaming jobs; without it `flush()` is never called mid-stream
and in-flight creates are lost on failure. Batch execution is covered by the end-of-input flush.

**Retries are this sink's responsibility, unlike every other connector here.** The generated client
gives `CreateTask` an *empty* set of retryable status codes and a 20-second total timeout — verified
in `CloudTasksStubSettings` for `google-cloud-tasks` 2.94.0, where `getTask`, `listTasks` and
`deleteTask` all retry on `DEADLINE_EXCEEDED`/`UNAVAILABLE` and `createTask` alone does not. That is
deliberate on Google's side: an unnamed create is not idempotent, so a blind retry duplicates the
task. The sink therefore implements its own bounded retry with exponential backoff, configurable
through `CloudTasksOptions` with defaults, over the codes worth retrying:

| Status | Treatment |
|---|---|
| `UNAVAILABLE`, `DEADLINE_EXCEEDED` | Retry. Under unnamed tasks a `DEADLINE_EXCEEDED` is ambiguous — the task may already exist — so the retry may duplicate. At-least-once prefers that to loss; naming removes the ambiguity |
| `RESOURCE_EXHAUSTED` | Retry with backoff. The queue is overloaded; Google's guidance is to throttle the caller adaptively |
| `NOT_FOUND` | Retry, briefly. A queue idle for 30 days takes "a few minutes to re-activate" and "some method calls may return `NOT_FOUND`" meanwhile, so this is not proof of a misconfigured queue. It becomes terminal once the retry budget is spent |
| `ALREADY_EXISTS` | Success, when a task id was supplied (see above). Terminal otherwise, since it should be unreachable |
| Everything else | Terminal — fails the job |

## Queues, rate limits and sink concurrency

**There is no batch create.** GA v2 offers `CreateTask` and nothing else: `BatchCreateTasks` exists
only in v2beta3 preview (long-running, explicitly non-atomic, 100 tasks maximum) and `BufferTask` is
REST-only and absent from the Java client. The Java client also has no client-side batching or flow
control — every method is a plain `UnaryCallSettings`, with no `BatchingSettings` anywhere in
`CloudTasksSettings`. So the sink owns batching, backpressure and concurrency outright, and creation
costs one RPC per record.

What the sink provides is the same mailbox-based bound the Pub/Sub sink uses: a cap on unacknowledged
creates (`maxInFlightTasks`), with completions re-dispatched onto the task mailbox so all writer
state stays single-threaded, and a write at the cap yielding until completions bring the count down.
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
still accepts task creation and simply stops dispatching, and a **disabled** queue does the same. A
pipeline writing to either sees a healthy sink while the backlog grows.

## Error handling

Terminal failures fail the job. Failures captured by completion callbacks are rethrown on the task
thread from the next `write()`/`flush()`, and `flush()` awaits every in-flight create, so a failure
cannot slip past a checkpoint barrier. A per-record failure policy — the `FailedRowHandler`
analogue — is deferred to #37 along with the other connectors'.

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

Planned in #25. Unit tests cover the builder, destination identity, the serialization adapters and
the writer against an in-memory fake, in the shape the Pub/Sub module already uses. Integration
tests run against [`aertje/cloud-tasks-emulator`](https://github.com/aertje/cloud-tasks-emulator)
(MIT, published as `ghcr.io/aertje/cloud-tasks-emulator`) driven by testcontainers as a
`GenericContainer` — testcontainers' GCloud module has no Cloud Tasks support, and Google publishes
no official emulator. The emulator implements the v2 API, real HTTP dispatch, retries, rate limits
and named-task deduplication, so the interesting paths are reachable in CI without cloud
credentials.

Two gaps the emulator leaves, to be covered by the real-GCP suite or not at all:

- It never garbage-collects task names — the dedup window cannot be exercised, only the
  `ALREADY_EXISTS` response. Tests reset with `--hard-reset-on-purge-queue` between scenarios.
- It does not implement `UpdateQueue`, so queue-level `httpTarget` routing — the override that can
  silently redirect per-record URLs — is not testable there.

## Provenance and attribution

This module is an original implementation. There is no Flink Cloud Tasks connector in Apache Flink,
in `GoogleCloudPlatform/pubsub` or elsewhere in open source to adapt, so unlike the Pub/Sub module
nothing is vendored. Its design references the Pub/Sub sink in this repository (the mailbox-based
in-flight cap, the flush-on-checkpoint stateless writer, the serialization-schema shape) and
Google's own Cloud Tasks documentation. No source code has been copied into this module.
