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

# ADR-0034: BigQuery sink metrics are three writer classes and aggregate committer metrics

- Status: Accepted
- Date: 2026-08-03; copy hierarchy wording revised by [#598] (2026-08-13); retry-volume
  wording corrected by [#1051] (2026-08-23); commit concurrency metrics added by [#1129]
  (2026-08-29)
- Issues: [#210] (the [#37] series' last metrics sub-issue), [#76], [#77], [#598], [#1051], [#1129]
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Metrics, § Committer
  metrics

## Decision

`DefaultStreamWriterMetrics`, `BufferedStreamWriterMetrics` (both `sink.storage.writer`) and
`FileLoadsWriterMetrics` (`sink.fileloads.writer`) over the shared `base.metrics` helpers, plus
aggregate FILE_LOADS committer metrics.
Three classes rather than one conditionally-registering class keep each write method's metrics
aligned with the operations it reports.
After [#76], the buffered path may have dynamic destinations but deliberately retains aggregate
counters plus its aggregate `inFlightAppends` gauge: checkpoint state records the exact active set,
`destinationIdleTimeout` bounds it, and this change does not add cardinality-bearing metrics.
After [#77], it registers the aggregate `schemaReconciliations` counter because it can apply the
same table updates as the default-stream writer.
FILE_LOADS makes no per-record request and therefore has no error-class dimension at all.

- **`numRecordsSend` is counted where the batch is first handed to the client**: `appendPending`
  on the default-stream path (the repair path re-appends from `retryBatches`, a different call
  site, so no flag is needed) and a `firstAttempt` parameter on `syncAppend` for the buffered
  path (whose probe, resend and replay all share that one call). Both count *after* the client
  call returns, so a synchronous rejection is not reported as sent.
- **`errorClass.CODE.errors` counts every failed append the task thread classifies**, not just
  the first of a repair episode (widened with the user, 2026-08-03): `collectFailedSiblings`,
  the `retryBatches` failure branch, and the buffered path's drain/resend/replay/probe sites
  count too. `appendRetries` counts connector-issued re-appends directly; no status-filtered error
  sum is its substitute because the error classes also include first and terminal failures, and a
  repair can start from a non-transient status. Cloud Tasks also reports failed attempts rather
  than exact retry volume: first failures count, and its metric classifies the outer status while
  retry routing scans the whole exception chain. **Nothing is ever counted from a gRPC callback
  thread** (the counters are plain), which is why the one failure a callback owns
  outright — a terminal one, removed from `inFlight` by `park()` — is counted in
  `checkAsyncError()` instead, behind an `asyncErrorCounted` flag. Two failures are deliberately
  **uncounted**: `OFFSET_ALREADY_EXISTS` outside a replay is a success, and the appends stranded
  behind a rejected offset in `recoverRowLevel` are cascades of a failure that is itself counted
  — the Pub/Sub cascade-exclusion rule in this writer's shape. The gax code comes from a new
  `AppendErrorClassifier.statusCode`, mirroring `PubSubErrorClassifier.statusCode`, leaving the
  classifier's own `io.grpc.Status.Code` routing untouched ([#61]'s do-not-converge decision).
- **Every gauge's backing collection is cleared in `close()`** — `inFlight` on both storage
  writers and `destinations` on FILE_LOADS: a reporter can sample a gauge between the writer's
  teardown and its metric group's, and on the failure path those collections are never drained,
  so without this a dead writer goes on reporting appends nobody will wait for. Safe because
  nothing re-adds an entry afterwards.
- **`appendRetries` counts re-issued appends, `tablesCreated` counts creations and
  `schemaReconciliations` counts applied schema updates only** — `reconcileSchema`'s
  table-had-vanished branch is a creation and is counted as one.
- `perDestinationMetrics` is on `DefaultStreamOptions` and `FileLoadsOptions`, and its handle is
  **looked up per batch rather than cached on `DestinationState`**, unlike the Pub/Sub sink's:
  this writer counts per batch, and its state is rebuilt by every repair, so caching would buy
  one map read per append at the cost of threading the handle through the rebuild path.
  `DestinationMetrics.Counters.recordsSent(long)` was added to the base helper for the same
  reason — a batching connector counts n records in one call.
- **The FILE_LOADS committer's `loadJobsSubmitted` counts load jobs only**, not the overflow
  path's final or intermediate copy jobs (the name is the contract), threaded into
  `LoadJobOrchestrator` as a `Counter` because that type is constructed per commit while the
  metric is registered once per committer. It is the whole job's rate (`prepared.global()` means
  one committer subtask). The framework's own committer metrics are **documented, not built**,
  under the names a reporter sees.
- **Destination commit concurrency is reported without destination labels** ([#1129]).
  `queuedCommitDestinations` and `activeCommitDestinations` are live gauges for the current phase's
  destination actions in a non-empty attempt.
  The coordinator clears both when the attempt ends.
  Each worker update carries its attempt generation, so a worker that starts or finishes after an
  abandoned drain cannot change that ended attempt's gauges or a later attempt's live state.
  `currentCommitDurationMillis` is zero while idle, and `lastCommitDurationMillis` retains the
  elapsed time of the most recently completed or failed non-empty attempt.
  The gauges are registered once per committer and use atomic state because worker threads update
  them while reporters may sample them.
  No gauge names a table, so a dynamic-destination job cannot grow this metric set.
- Coverage is one `*MetricsTest` per writer, `FileLoadsCommitterMetricsTest` for committer metric
  registration, duration and late-worker semantics, and the destination-executor test for live
  queue and active counts, asserting **by registered name** through the matching test metric group.

[#37]: https://github.com/flink-gcp/flink-connector-gcp/issues/37
[#61]: https://github.com/flink-gcp/flink-connector-gcp/issues/61
[#210]: https://github.com/flink-gcp/flink-connector-gcp/issues/210
[#76]: https://github.com/flink-gcp/flink-connector-gcp/issues/76
[#77]: https://github.com/flink-gcp/flink-connector-gcp/issues/77
[#598]: https://github.com/flink-gcp/flink-connector-gcp/issues/598
[#1051]: https://github.com/flink-gcp/flink-connector-gcp/issues/1051
[#1129]: https://github.com/flink-gcp/flink-connector-gcp/issues/1129
