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

# ADR-0017: BigQuery error handling routes row verdicts only, and repairs dead writers in place

- Status: Accepted
- Date: 2026-07-19 ([#13]); SPI extracted 2026-08-02 ([#205]); watchdog handling 2026-07-27
  ([#163]); the transient-over-row-details filter on [#213]'s round-2 review
- Issues: [#13], [#37], [#205], [#213], [#163]
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Error handling

## Decision

- The row-level failure policy is the shared `FailureHandler<FailedRow>` from `base.failure`
  ([#37]/[#205]) — fail-job (default), log-and-drop, DLQ routing — and the base module's record
  owns the lifecycle contract. This module keeps `FailedRow` (implements `FailedElement`;
  `getConnector()` = "bigquery", `describeDestination()` = the `p.d.t` string) and the builder
  setter keeps its `failedRowHandler` name — domain vocabulary at the surface users touch. The
  old `FailedRowHandler`/`FailedRowHandlers`/module-local `DeadLetterQueue` stub were deleted
  outright, not aliased (nothing published). `FailedRow` carries serialized protobuf bytes, not
  the original record (the writer is stateless).
- The three sinks open the handler in their production `createWriter`/`restoreWriter` via
  `DefaultFailureHandlerContext.of(context)`. The default-stream sink's `@VisibleForTesting
  createWriter(appenderFactory, tableAdmin)` deliberately does not open, so fake-injected writer
  tests need no `WriterInitContext` — but the buffered sink's `@VisibleForTesting` 3-arg
  `restoreWriter` is the production delegate and **does** open. The three writers call the
  handler's `flush()` after their drains, and their `close()` uses `Closers.closeAll`
  (`base.lifecycle`, never Flink's `IOUtils.closeAll` — [#276], whose record is the base
  module's) so the
  handler is closed even when closing an appender or service, or aborting a staged file, throws.
  On the FILE_LOADS path that promise is testable in exactly one shape — `StagedFileWriter
  .abort()` swallows an `IOException` or a `RuntimeException` by design, so an `Error` is the
  only failure that list can carry, which is what
  `closeStillClosesTheHandlerWhenAbortingAStagedFileThrowsAnError` drives.
- **`findRowLevel` rejects a row-detailed error whose own status code is transient** ([#213]
  round-2 review): the SDK copies the response's status code verbatim onto
  `AppendSerializationError` after its in-stream retries, so row details under `UNAVAILABLE` &c.
  are an availability verdict, not a data verdict — retrying the whole batch is always safe (a
  failed append wrote nothing), while routing on it could dead-letter rows a later attempt would
  write. This makes "outage-shaped failures never reach the handler" a property of the code, not
  of the service's conventions.
- **`replayBatches` carries the same no-progress guard as `retryBatches`**: row errors naming no
  row in the batch drop nothing, and re-appending the identical batch (with the attempt counter
  reset and no backoff) would loop for as long as the server repeats the verdict — the buffered
  writer lacked the guard the default writer had, found by trying to refute the classification
  claims rather than by reading the diff.
- **The SDK's callback-wait watchdog timeout is a client-side dead-writer verdict** ([#163]):
  `MaximumRequestCallbackWaitTimeExceededException` (no response for the SDK's hardcoded 5
  minutes) is a plain `RuntimeException` with no gRPC status, and only the **first** future of a
  dead-connection storm carries it raw (siblings get `StreamWriterClosedException`), so
  status-code classification alone would fail the job on exactly the failure that names the root
  cause. `AppendErrorClassifier.isWriterClosed` matches both exceptions and
  `requiresWriterRefresh` delegates to it; both storage writers repair in place through their
  existing paths.

## Alternatives declined

A bounded `future.get(timeout)` as further defense was considered and declined on [#163]: the
watchdog's coverage was verified against SDK 3.30.0 (requeued-on-reconnect requests are
re-timestamped and resent in the same append-loop iteration, so "never sent, never checked" is
not a real window), the residual SDK-bug hang is already bounded by Flink's checkpoint timeout →
failover, and a second timeout would race the SDK's hardcoded 5 minutes and could tear down
slow-but-progressing appends.

[#13]: https://github.com/laughingman7743/flink-connector-gcp/issues/13
[#37]: https://github.com/laughingman7743/flink-connector-gcp/issues/37
[#276]: https://github.com/laughingman7743/flink-connector-gcp/issues/276
[#163]: https://github.com/laughingman7743/flink-connector-gcp/issues/163
[#205]: https://github.com/laughingman7743/flink-connector-gcp/issues/205
[#213]: https://github.com/laughingman7743/flink-connector-gcp/issues/213
