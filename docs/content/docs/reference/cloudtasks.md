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

# Cloud Tasks options

Every option the Cloud Tasks sink and App Engine target builder take. What each one is *for* is on the
[Cloud Tasks connector]({{< relref "docs/connectors/datastream/cloudtasks" >}}) page; the three
forms of the Default column are explained
[here]({{< relref "docs/reference" >}}#what-a-default-means).

This is the shortest reference of the three, and deliberately so: **there are no rate knobs here.**
`maxDispatchesPerSecond`, `maxConcurrentDispatches` and the retry policy are *queue* configuration,
applied by whoever creates the queue — the sink writes tasks and the queue decides how fast they
execute. That inversion is the connector's whole reason for existing, and it is set out under
[What this connector is for]({{< relref "docs/connectors/datastream/cloudtasks" >}}#what-this-connector-is-for).

## `CloudTasksSink.builder()`

| Option | Default | What it does |
|---|---|---|
| `queue` | **required**, unless `destinationResolver` is set | Writes every task to one fixed queue |
| `destinationResolver` | — | Resolves the queue per record. One client serves every queue, so routing allocates no per-queue service-client state |
| `serializer` | **required** | Builds the `Task` — HTTP URL or App Engine relative URI/routing, method, headers, body, schedule, authorization — or returns `null` to skip the record. It must carry no name |
| `taskIdExtractor` | — | Opts into named tasks, deduplicating by the extracted key. The sink hashes it with SHA-256 |
| `writerOptions` | [defaults](#cloudtaskswriteroptions) | The in-flight cap, the transport channel pool and the two retry budgets |
| `failedTaskHandler` | `FailureHandler.failJob()` | What happens to a task that terminally fails — fail, drop, or dead-letter. The queue behind `sendToDeadLetterQueue(...)` has [options of its own]({{< relref "docs/reference/pubsub" >}}#pubsubdeadletterqueuebuilder) |
| `serviceAccountKeyFile` | *unset ⇒ application-default credentials* | Reads a service-account JSON key on each TaskManager when the writer starts. Every eligible TaskManager must see the same path. Rejected beside `emulatorEndpoint`; see the [deployment note]({{< relref "docs/connectors/datastream/cloudtasks" >}}#credential-file-deployment) |
| `emulatorEndpoint` | — | Points the sink at an emulator over a plaintext channel with **no credentials**. Never production. Given as `host:port`, and rejected at the setter if it is not |

**The task itself is configured outside the sink builder.** `httpTarget(url)` starts the immutable
HTTP schema chain (`withBody`, `withMethod`, `withUrl`, `withHeaders`, `withOidcToken`,
`withOAuthToken`). `appEngineTarget(relativeUri)` starts `AppEngineTargetBuilder` (`withBody`,
`withMethod`, `withRelativeUri`, `withHeaders`, `withRouting`, then `build`). Each API composes the
`Task` a record becomes, and the sink builder takes the resulting schema as its single `serializer`
option above. The APIs are described under
[API notes]({{< relref "docs/connectors/datastream/cloudtasks" >}}#api-notes) and typed in the
[Java API reference]({{< param ApiDocsURL >}}).

## `AppEngineTargetBuilder`

`appEngineTarget(relativeUri)` supplies the fixed relative URI, `withBody(...)` binds the record
type and required body serializer, and `build()` snapshots the current settings into the immutable
serialization schema.

| Option | Default | What it does |
|---|---|---|
| `withMethod` | `POST` | Sets the App Engine request method. Only `POST` and `PUT` carry the serialized body |
| `withRelativeUri` | fixed URI passed to `appEngineTarget(...)` | Resolves the relative URI per record instead |
| `withHeaders` | — | Resolves request headers per record; reserved App Engine and transport headers are rejected |
| `withRouting` | *unset ⇒ App Engine default service and version* | Sets fixed routing, or resolves service/version/instance routing per record. Queue-level `appEngineRoutingOverride` takes precedence |

Naming is off by default because Google documents deduplication as costing *"significantly increased
latency"*, and the id is hashed because sequential ids raise latency and error rates across the
whole queue — see
[Task naming and deduplication]({{< relref "docs/connectors/datastream/cloudtasks" >}}#task-naming-and-deduplication).

**The sink never creates a queue**, so there is no create disposition and no creation-options object,
unlike the BigQuery and Pub/Sub sinks. An auto-created queue would carry Cloud Tasks' default rate
limits, silently discarding the pacing that is the reason to use the service, and queue creation is
a one-way door — a deleted queue name cannot be reused for three days.

## `CloudTasksWriterOptions`

Set through `writerOptions(...)`; every knob is defaulted. Retries are this sink's own
responsibility, unlike every other connector here, because the generated client gives `CreateTask`
an *empty* set of retryable status codes — the reasoning, and which status lands in which budget,
is under [Tuning]({{< relref "docs/connectors/datastream/cloudtasks" >}}#tuning).

| Option | Default | What it does |
|---|---|---|
| `maxInFlightTasks` | 1000 | Caps outstanding creates, in flight plus parked. At the cap `write()` yields to the task mailbox |
| `channelPoolSize` | *unset ⇒ the client's single channel* | Sizes the client's gRPC channel pool, which bounds how much of the in-flight cap the transport actually carries; the sizing rule and ramp caution are under [Tuning]({{< relref "docs/connectors/datastream/cloudtasks" >}}#tuning). Rejected beside `emulatorEndpoint` |
| `retryInitialBackoff` | 100 ms | First backoff for `UNAVAILABLE` / `DEADLINE_EXCEEDED` / `RESOURCE_EXHAUSTED` |
| `retryMaxBackoff` | 10 s | Cap that backoff doubles up to, before ±25% jitter |
| `retryMaxAttempts` | 8 | Total attempts, the first create included. Exhausting the budget fails the job |
| `notFoundInitialBackoff` | 500 ms | First backoff of the separate `NOT_FOUND` budget |
| `notFoundMaxBackoff` | 2 s | Cap of that backoff, before jitter |
| `notFoundMaxAttempts` | 3 | `NOT_FOUND` attempts. Short on purpose, so a mistyped queue name fails quickly |
| `perDestinationMetrics` | `false` | Registers per-queue `recordsSend` and `sendErrors` counters beside the writer's totals. Off by default: Flink cannot unregister a metric, so with a per-record `destinationResolver` every queue the job writes to keeps a row in the registry for the task's lifetime. See [Metrics]({{< relref "docs/connectors/datastream/cloudtasks" >}}#metrics) |

`NOT_FOUND` has its own short budget because a queue idle for 30 days takes a few minutes to
reactivate and may answer `NOT_FOUND` meanwhile — so it is not proof of a misconfigured queue, but a
mistyped one must not burn the full budget per record. A queue taking minutes to reactivate outlives
this budget by design; recovering from that is the job's restart strategy, not the writer's.
