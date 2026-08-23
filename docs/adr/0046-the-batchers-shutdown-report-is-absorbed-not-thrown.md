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

# ADR-0046: The batcher's shutdown report is absorbed, not thrown

- Status: Accepted
- Date: 2026-08-06
- Issues: [#238] (the cross-connector contract is ADR-0003)
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` (teardown)

## Context

gax's `BatcherImpl.close()` ends with `batcherStats.asException()`, an accumulator of every
entry failure of the batcher's *lifetime* that consuming an entry's future does not clear — so a
`logAndDrop` job that dropped one mutation still failed at task close, and the dropping policies
did not survive the end of a job.

## Decision

`DefaultMutationBatcherFactory.shutDownAbsorbingTheLifetimeFailureReport` catches that one type
(`BatchingException`) and logs it at WARN; `InterruptedException` and gax's own
`IllegalStateException("unexpected error closing the batcher")` still propagate, and
`MutationBatcher.close()`'s contract carries the rule the writer relies on.

**Both alternatives the issue floated were eliminated by measurement, not judgement** (Flink
2.2.1 and gax 2.82.0 sources, 2026-08-06):

- Narrowing on gax's side is not reachable — `BatcherStats` is package-private with no reset and
  no accessor, and `close(Duration)` rebuilds the exception as
  `new BatchingException(cause.getMessage())`, discarding the cause chain.
- Draining the writer's own in-flight set first would **hang the task**: `StreamTask
  .afterInvoke()` calls `prepareClose()` before `closeAllOperators()` and a quiesced mailbox
  rejects `put` while `take` still blocks, so `drainInFlight()` would park forever on a mail
  nothing can enqueue. (Corollary, repository-wide: **no `SinkWriter.close()` may `yield()`**.)
- A third option, not in the issue — swallow only when nothing is in flight, making the report
  provably a duplicate — fails because `drainInFlight()` short-circuits on `asyncError`, leaving
  that count non-zero after exactly the failures this is about.

That same quiescing is what makes the log line worth writing rather than a formality: a failure
of a batch the shutdown itself sent reaches neither the handler nor `asyncError`, so the
absorbed report is its only record.

## Consequences

- Pinned twice — `DefaultMutationBatcherFactoryTest` over the seam, with the exception built
  reflectively because gax keeps its constructor package-private, and both failure ITCases
  closing their writers plainly. Only a `finally` whose case actually provoked a rejection
  asserts anything (the emulator *accepts* an empty row key, so that batcher accumulates
  nothing). The log line itself is covered by `LogCapture` ([#323]), this call site being one of
  its two motivating cases.
- [#325] then measured whether the SPI contract is a property of the pattern or of gax, across
  all nine client-wrapping SPIs: neither purely — a second connector has the shape by an
  unrelated mechanism, so the absorb stays per-connector (ADR-0003). The duplicate here is the
  **severe** one of the two: it lands after the `FailureHandler` may have deliberately dropped
  those entry failures, so it fails a job the policy kept running.

[#238]: https://github.com/flink-gcp/flink-connector-gcp/issues/238
[#323]: https://github.com/flink-gcp/flink-connector-gcp/issues/323
[#325]: https://github.com/flink-gcp/flink-connector-gcp/issues/325
