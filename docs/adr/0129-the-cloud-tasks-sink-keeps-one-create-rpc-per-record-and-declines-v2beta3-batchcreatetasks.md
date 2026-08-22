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

# ADR-0129: The Cloud Tasks sink keeps one create RPC per record and declines v2beta3 BatchCreateTasks

- Status: Accepted
- Date: 2026-08-22 ([#937])
- Issues: [#937], [#1015]
- Modules: cloudtasks
- Current behavior: `docs/content/docs/connectors/datastream/cloudtasks.md` § Queues, rate limits
  and sink concurrency

## Context

ADR-0048 built the sink on the fact that no batch method was callable: at `google-cloud-tasks`
2.94.0, `BatchCreateTasks` and `BufferTask` were REST-only and absent from the Java client.
2.95.0 (the [#903] `libraries-bom` 26.86.0 resolution) overturned half of that: `BatchCreateTasks`
appeared on the **v2beta3** client — and only there — as
`OperationFuture<BatchCreateTasksResponse, BatchCreateTasksMetadata> batchCreateTasksAsync(...)`,
a long-running, explicitly non-atomic call of at most 100 same-queue `CreateTaskRequest`s whose
per-request failures are reported only on the operation *metadata* (`failed_requests`, a
`map<int32 index, google.rpc.Status>`). `BufferTask` remains absent everywhere, and no method is
configured with gax batching. The v2beta3 `Task`/`CreateTaskRequest` protos are not
wire-compatible with the v2 types this sink's public API exposes (the request-target `oneof`
fields sit at different numbers), so adopting the method means either a structural v2-to-v2beta3
translation on the hot path or a public-API migration to a beta surface.

[#937] asked whether the sink should use it, and required the answer to be measured against this
sink's shapes rather than argued. The measurement ran 2026-08-22 against a real paused queue
(project-side execution, zero dispatches), `google-cloud-tasks` 2.95.0, comparing
`batchCreateTasksAsync` with the writer's actual shape — concurrent v2
`createTaskCallable().futureCall(...)` under the default 1,000-task in-flight cap. Predictions
and raw results are on [#937].

## Decision

The sink keeps one `CreateTask` RPC per record. `BatchCreateTasks` is declined on three measured
grounds, any one of which would have sufficed:

- **No latency win.** The initial `BatchCreateTasks` RPC always returned the operation already
  `done=true` (63/63 trials), so the generated 5 s first-poll floor never applies — but only
  because the RPC itself blocks until the batch has executed. A 100-task batch completed in
  p50 1.07 s / p95 2.9 s against p50 0.97 s / p95 1.4 s for 100 concurrent single creates: the
  batch is no faster at the median and ~2× worse at the tail, with polling configuration
  changing nothing.
- **The throughput win belongs to the transport, not the method.** At an equal 1,000-task
  in-flight budget, batches sustained 995 tasks/s against 210 tasks/s for singles — but the
  singles number was the default single gRPC channel's ~100-concurrent-stream ceiling, not the
  service's. The same singles arm with an 8-channel pool sustained 1,271 tasks/s, beating the
  batch path outright on the GA v2 surface, and both figures already sit at the queue's
  recommended ~1,000 TPS ceiling, which counts *tasks*, not RPCs — as does the API request
  quota, whose documentation states each task in a batch counts as one request. Everything
  batching buys is available cheaper; the channel observation is routed separately as [#1015].
- **The failure surface breaks the sink's per-record contracts.** A mixed batch behaves
  (`PARTIALLY_SUCCEEDED`, response carries the created tasks, `failed_requests` names every
  failed index), but an all-failed batch resolves as a gax `UnknownException` ("succeeded, but
  encountered a problem unpacking it"), and a batch containing already-existing named tasks
  (measured with 50 duplicates among 100) **is rejected wholesale with a single top-level
  `ALREADY_EXISTS`** — no operation, no metadata, no per-index report — while `GetTask` probes
  confirmed its non-duplicate half was silently created anyway. The sink's dedup contract
  (`ALREADY_EXISTS` on a named task is success, ADR-0048/0049) would turn a re-delivery into an
  unobservable partial write that only a per-record `GetTask` sweep could reconcile.

Costs that would have ridden along even with favorable numbers, recorded as supporting rather
than deciding: a beta API surface under a `@Public` sink ([ADR-0124]'s japicmp gate), the
non-wire-compatible proto translation, and the loss of all emulator coverage — the
`aertje/cloud-tasks-emulator` image the ITCases run implements v2 only, so a batch path would be
testable only against hand-written fakes and billed gated suites.

## Alternatives declined

The adoption shape, for whoever reopens this: an opt-in writer-options knob keeping the public
API on v2 types, a batch-creator seam beside `TaskCreator` — the "second family arrives" event
whose package cost ADR-0055 prices — per-queue buffering flushed at batch size or checkpoint,
and a `failed_requests` demultiplexer feeding the existing park/retry loop.

## Consequences

- The sink's writer is unchanged; ADR-0048's one-RPC-per-record conclusion stands on these
  measurements rather than on the method's absence.
- **The verdict is bound to what was measured**: `google-cloud-tasks` 2.95.0 and the service's
  behavior on 2026-08-22. The deciding facts sit on both sides of the wire and each can move
  independently — the wholesale `ALREADY_EXISTS` rejection and the batch's execution latency are
  service behavior that can change under an unchanged client, while the gax "problem unpacking
  it" surface and the polling defaults are client code a release can rewrite. Only the
  metadata-side failure reporting is structural to the API's shape.
- Re-evaluate when a batch create reaches the v2 surface, `ALREADY_EXISTS` in a batch becomes a
  per-index report, or a `google-cloud-tasks` release notes batch-create changes. The
  `libraries-bom` bump that moves this client is where the re-check happens — the same event
  ([#903]) that surfaced the method and falsified ADR-0048's absence claim — not a runtime
  guard. A throughput motive alone is answered by [#1015] instead, where the measured transport
  ceiling (a single channel's ~100 concurrent streams) is routed.

[#903]: https://github.com/flink-gcp/flink-connector-gcp/issues/903
[#937]: https://github.com/flink-gcp/flink-connector-gcp/issues/937
[#1015]: https://github.com/flink-gcp/flink-connector-gcp/issues/1015
[ADR-0124]: 0124-the-stability-boundary-at-1-0-0-is-a-promoted-public-entry-surface-checked-by-japicmp.md
