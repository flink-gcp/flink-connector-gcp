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

# ADR-0034: BigQuery sink metrics are three writer classes and one committer counter

- Status: Accepted
- Date: 2026-08-03
- Issues: [#210] (the [#37] series' last metrics sub-issue), [#76]
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Metrics, § Committer
  metrics

## Decision

`DefaultStreamWriterMetrics`, `BufferedStreamWriterMetrics` (both `sink.storage.writer`) and
`FileLoadsWriterMetrics` (`sink.fileloads.writer`) over the shared `base.metrics` helpers, plus
one committer counter.
Three classes rather than one conditionally-registering class keep each write method's metrics
aligned with the operations it reports.
After [#76], the buffered path may have dynamic destinations but deliberately retains aggregate
counters plus its aggregate `inFlightAppends` gauge: checkpoint state records the exact active set,
`destinationIdleTimeout` bounds it, and this change does not add cardinality-bearing metrics.
It still has no schema reconciliation, while FILE_LOADS makes no per-record request and therefore
has no error-class dimension at all.

- **`numRecordsSend` is counted where the batch is first handed to the client**: `appendPending`
  on the default-stream path (the repair path re-appends from `retryBatches`, a different call
  site, so no flag is needed) and a `firstAttempt` parameter on `syncAppend` for the buffered
  path (whose probe, resend and replay all share that one call). Both count *after* the client
  call returns, so a synchronous rejection is not reported as sent.
- **`errorClass.CODE.errors` counts every failed append the task thread classifies**, not just
  the first of a repair episode (widened with the user, 2026-08-03): `collectFailedSiblings`,
  the `retryBatches` failure branch, and the buffered path's drain/resend/replay/probe sites
  count too, which makes the sum over the transient codes the retry volume — the same claim the
  Cloud Tasks page makes, so one dashboard reads both. **Nothing is ever counted from a gRPC
  callback thread** (the counters are plain), which is why the one failure a callback owns
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
  path's copy job (a different quota, and the name is the contract), threaded into
  `LoadJobOrchestrator` as a `Counter` because that type is constructed per commit while the
  metric is registered once per committer. It is the whole job's rate (`prepared.global()` means
  one committer subtask). The framework's own committer metrics are **documented, not built**,
  under the names a reporter sees.
- Coverage is one `*MetricsTest` per writer, asserting **by registered name** through
  `TestSinkWriterMetricGroup`; the buffered and FILE_LOADS ones ride their behavioural tests'
  fakes, while the default-stream one carries its own.

[#37]: https://github.com/laughingman7743/flink-connector-gcp/issues/37
[#61]: https://github.com/laughingman7743/flink-connector-gcp/issues/61
[#210]: https://github.com/laughingman7743/flink-connector-gcp/issues/210
[#76]: https://github.com/laughingman7743/flink-connector-gcp/issues/76
