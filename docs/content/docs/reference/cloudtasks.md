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

# Cloud Tasks options

Every option the Cloud Tasks sink takes. What each one is *for* is on the
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
| `destinationResolver` | — | Resolves the queue per record. Costs nothing here: one client serves every queue |
| `serializer` | **required** | Builds the `Task` — URL, method, headers, body, schedule, authorization. It must carry no name |
| `taskIdExtractor` | — | Opts into named tasks, deduplicating by the extracted key. The sink hashes it with SHA-256 |
| `writerOptions` | [defaults](#cloudtaskswriteroptions) | The in-flight cap and the two retry budgets |
| `failedTaskHandler` | `FailureHandler.failJob()` | What happens to a task that terminally fails — fail, drop, or dead-letter |
| `emulatorEndpoint` | — | Points the sink at an emulator over a plaintext channel with **no credentials**. Never production. Given as `host:port`, and rejected at `build()` if it is not |

**The task itself is configured on the serialization schema, not here.** `httpTarget(url)` starts a
fluent chain — `withBody`, `withMethod`, `withUrl`, `withHeaders`, `withOidcToken`, `withOAuthToken`
— which composes the `Task` each record becomes, and the builder takes the result as the single
`serializer` option above. The chain is described under
[API notes]({{< relref "docs/connectors/datastream/cloudtasks" >}}#api-notes) and typed in the
[Java API reference]({{< param ApiDocsURL >}}).

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
