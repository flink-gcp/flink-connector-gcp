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

# ADR-0049: Exactly three Cloud Tasks failures are routed, and the `INVALID_ARGUMENT` half never scans

- Status: Accepted
- Date: 2026-08-02 ([#207]); metrics 2026-08-03 ([#209]); revised by [#1051] and
  [#1058] (2026-08-23)
- Issues: [#207], [#209], [#1051], [#1058] (the [#37] series)
- Modules: cloudtasks
- Current behavior: `docs/content/docs/connectors/datastream/cloudtasks.md` § Failed-task
  policy, § Metrics

## Decision

`failedTaskHandler(...)` takes the shared `FailureHandler<FailedTask>`, defaulting to
`failJob()`. `FailedTask` sits at the `sink` root (a one-class `sink.failure` fails the [#119] layer test) and carries the **whole serialized `Task`** as
`getPayloadBytes()`, so a dead-letter consumer recovers the target, method, headers and
authorization with `parseFrom`; `describeDestination()` is the queue resource path.

- **Exactly three failures are routed, and the boundary is the decision**: a record the
  serializer rejects, a task id extractor that *throws*, and a creation rejected
  `INVALID_ARGUMENT`. Not routed, in two directions: outage-shaped failures (an exhausted
  transient or `NOT_FOUND` budget, `PERMISSION_DENIED`) must never reach a dropping handler; and
  configuration-shaped ones (a resolver returning null, a serializer returning an already-named
  task, an extractor returning null or an empty key) fail *every* record alike, so dropping them
  would leave an empty queue under a green job. The extractor is the pair worth not
  re-litigating — a throw is per-record and routed, a missing key is per-stream and fatal. A
  serializer returning **null** is in neither class (ADR-0001). `ALREADY_EXISTS` on a named task
  stays success.
- **Classification is a precedence over the whole cause chain — but only the transient half**:
  `CloudTasksErrorClassifier` (`sink.writer`) owns the two code predicates. Routing takes
  `transientCode == null && code == INVALID_ARGUMENT`. Here `code` is the chain's first
  classifiable status; `transientCode` scans the whole chain, so "an unstable service can never
  produce a dead letter" is a property of the code. The
  `INVALID_ARGUMENT` half deliberately does **not** scan: searching for it anywhere would drop a
  task whose outermost status is `INTERNAL` or `UNKNOWN` — the mirror image of the mistake the
  transient scan prevents. **That asymmetry was found by mutation testing** — the scanning
  variant's mutant survived, and inspecting why showed the mutant was the better code.
- The `INVALID_ARGUMENT` branch sits **before** the `asyncError` early-return, deliberately: the
  job is failing either way, but a dead-letter destination missing the task is worse than one
  holding a duplicate the replay will produce again. A handler failure inside `onCreateFailed`
  is captured into `asyncError`. `flush()` runs after the drain loop, which exits only with
  nothing in flight *and* nothing parked, so a re-dispatched retry cannot land after the handler
  flushed. Coverage is unit tests only: the emulator's `INVALID_ARGUMENT` surface is not
  evidence about the service's.

**Metrics** ([#209]): plain counters (completions are mailbox mails).

- **`numRecordsSend` is counted inside `dispatch(...)`, guarded by `pending == null`** — that
  argument already means "first attempt", so retry-safety is carried by the method's own
  signature. Counting in `write()` instead — where it sat until PR
  [#242](https://github.com/laughingman7743/flink-connector-gcp/pull/242)'s second review round
  — was wrong for the mirror reason: a `TaskCreator` throwing synchronously registers no
  callback, so that record reached Cloud Tasks not at all.
- **Error classes count every failed attempt, retryable ones included** — the deliberate asymmetry
  with `numRecordsSend`. They are not an exact retry counter: first failures count, and the metric
  uses the outermost status while retry routing scans the whole exception chain, so a retry
  selected by a nested transient status appears under the outer status instead. No separate exact
  retries counter is registered.
- **`ALREADY_EXISTS` on a named task is `tasksDeduplicated`, not an error** — the success naming
  exists to produce; `metrics.createFailure(code)` sits *after* that branch's early return for
  exactly that reason.
- **The per-queue counters are looked up per record**, unlike the Pub/Sub sink's cached handle:
  this writer keeps no per-destination state at all (one client serves every queue), so there is
  nowhere to cache one.

[#37]: https://github.com/laughingman7743/flink-connector-gcp/issues/37
[#119]: https://github.com/laughingman7743/flink-connector-gcp/issues/119
[#207]: https://github.com/laughingman7743/flink-connector-gcp/issues/207
[#209]: https://github.com/laughingman7743/flink-connector-gcp/issues/209
[#1051]: https://github.com/flink-gcp/flink-connector-gcp/issues/1051
[#1058]: https://github.com/flink-gcp/flink-connector-gcp/issues/1058
