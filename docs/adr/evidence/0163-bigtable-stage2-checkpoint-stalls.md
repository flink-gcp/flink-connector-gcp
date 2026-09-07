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

# Bigtable Stage 2 checkpoint-stall diagnostics

This is a local follow-up to the [bounded service experiment](0163-bigtable-stage2-experiment-harness.md) under [ADR-0163](../0163-bigtable-checkpointed-writes-stage-immutable-mutations-and-retain-row-markers.md).
The 2026-09-07 service run lost a checkpoint to its 60-second timeout; its failed repetition's detailed samples were unavailable.
The work below reproduces that failure class without Bigtable and adds the observations needed to distinguish an active commit invocation from its bounded set of outstanding RPCs.
It does not establish the historical service failure's exact cause, change the timeout, or pass the performance gate.

## Instrument and deterministic checks

`LocalCommitter.commit()` synchronously drives an invocation's complete collection with a bounded deque of original futures.
The local job chains the source, writer and committer into one task.
A long commit can therefore prevent a later checkpoint from completing even when each individual RPC returns well within its deadline.
The coordinator's timeout does not require a hung RPC or an overflowing writer.

`Stage2CommitProgress` retains one small entry for each active commit invocation and scalar totals for completed invocations.
The entry contains its initial collection size and monotonic start time; it retains neither requests nor completed invocation history.
A commit invocation is not necessarily one checkpoint interval: Flink can deliver earlier pending work together.
The new fields are local diagnostic JSON, not production connector metrics or a global committable-heap bound.

The existing one-second sample now includes admitted and acknowledged input totals plus `commitProgress`:

| Field | Meaning |
| --- | --- |
| `activeBatches` | Commit invocations that have entered but not left their cleanup path. |
| `activeBatchEntries` | Sum of those invocations' initial collection sizes, including entries already acknowledged within them; not the remaining backlog. |
| `oldestBatchNanos` | Maximum sampled invocation age, clamped at zero; use `activeBatches` to determine activity. |
| `largestBatchEntries` | Largest initial collection size observed in this JVM. |
| `startedBatches` / `finishedBatches` | Entered invocations and invocations whose cleanup reached the completion hook. Finished includes failed invocations. |
| `failedBatches` | Finished invocations that did not successfully drain their collection. |
| `maxFinishedBatchNanos` | Longest duration of a finished invocation, including failed cleanup. |

These fields and the input/RPC counters are sampled separately; they are not an atomic snapshot of the entire job.
A last pre-failure sample can still show an active invocation and zero failed invocations, because cancellation happens afterwards.
The failure log and process result establish terminal failure; an earlier REST sample does not override it.
The sample cap now counts the complete UTF-8 JSONL record, including the new fields and newline, before writing it.
The cap remains 8 MiB, and existing success/failure evidence preservation and exact-directory cleanup remain in place.

`Stage2CommitProgressTest` checks overlapping invocations, oldest-age changes, cumulative totals, successful completion and progress cleanup after interruption.
The interruption case also checks that `Stage2Harness` leaves the finite harness's original-future trace empty.
`Stage2JobITCase.heldCommitExpiresTheNextCheckpointAndReleasesItsProgressOnCancellation` holds four original fake replies after a completed checkpoint, then triggers another checkpoint.
With a test-local five-second timeout and restart disabled, that checkpoint expires; task cancellation during job failure cancels all four original futures, clears the active RPC count and records a failed invocation with no active entry left.
This shorter timeout is confined to the deterministic test; the standalone Stage 2 runner still uses 60 seconds.

## Local observations on 2026-09-08

First, the preserved service-run code snapshot `4a66790e30a29740f2b64ea5588b7412aafa6c38` reproduced checkpoint expiration in local fake mode with 180 ms replies, four in-flight entries and a 4,000-input cap.
It exited with status 1 after 63.901 seconds and removed its owned work directory.
This establishes that the failure class can occur without a service call; the old snapshot still lacks the newer failure-sample preservation.

The instrumented observations used frozen local commit `08ed08d580b27ada83f7591b163a5b5ae88f59b2` and Bigtable test-source tree `1fe2101fb72f36cff056daec45342ef965d9c4f3`.
Each observation used a fresh JDK 17 JVM, Flink 2.2.1, `-Xmx2g`, `-XX:ActiveProcessorCount=4`, and the `java.base/java.lang` and `java.base/java.util` opens shown in the [runner instructions](0163-bigtable-stage2-experiment-harness.md#local-execution).
All used one subtask, 1 KiB payloads, even keys, a one-second checkpoint interval, ten-second warm-up and thirty-second admission window.
The request timeout, checkpoint timeout, staging limits and drain deadline were unchanged.
The fake reply delay is an instrument; it does not model Bigtable's latency distribution, startup, retry behavior or physical storage.

| Fake delay | In flight | Input cap | Result | Largest invocation | Maximum sampled invocation age | Process lifetime |
| --- | --- | --- | --- | --- | --- | --- |
| 0 ms | 4 | 200,000 | Censored by input cap; drained | 14,043 | 0.787 s | 38.054 s |
| 20 ms | 4 | 200,000 | Observation; drained | 5,017 | 29.406 s | 66.417 s |
| 180 ms | 4 | 4,000 | Checkpoint timeout; failed | 4,000 | 60.074 s | 63.454 s |
| 180 ms | 16 | 4,000 | Cap exhausted in warm-up; drained | 4,000 | 45.410 s | 49.449 s |

The first two runs reported respectively 6,008.512 and 121.909 distinct measured inputs/s including drain, with visibility p95 of 1.516 and 30.876 seconds.
The slower reply is visible to the instrument, but the zero-delay run is censored; these single observations do not establish sustained service capacity or repeatability.
The 20 ms run acknowledged 6,207 measured inputs, with 20.915 seconds of measured drain.

In the failing 180 ms run, the first invocation contained all 4,000 inputs.
At the last retained sample, 1,304 inputs were acknowledged, four requests were active, and the invocation had run for 60.074 seconds.
REST showed two completed checkpoints and checkpoint 3 in progress; the subsequent log reports that checkpoint's expiration and `STAGE2_TERMINAL FAILED`.
The process exited with status 1, without a valid performance row or final readback.
Its 59 periodic samples plus the pre-admission baseline were preserved in 238,276 bytes before the owned directory was removed.

The 16-in-flight diagnostic control drained the same capped input count without changing the checkpoint timeout.
For orientation only, 4,000 requests at a fixed 180 ms divided by four or sixteen concurrent requests is 180 or 45 seconds before overhead; that arithmetic is not a service prediction.
The 16-in-flight run admitted no measured inputs because it exhausted its cap during warm-up, so it produced no valid measured throughput or visibility percentile.
It does not establish that sixteen in-flight requests fix the service workload or validate a new service configuration.

## Consequences and next service boundary

The missing observation is now available: a bounded number of outstanding requests can coexist with a much larger, long-running commit invocation.
A retry of the previous service setup without accounting for invocation size and completion time would not resolve the checkpoint question.
A future concrete service plan must size the checkpoint budget from accumulated commit work, obtain a non-censored serialized control and retain failed-run samples.
It must still state its own cells, repetition order, lifetime, operation/storage caps and remaining aggregate cost budget before creating resources.
No service configuration, accepted threshold or full-matrix requirement is amended by this local diagnostic.

All five local JVM observations used unique owned directories and verified their absence after exit, including both timeout failures.
Their bounded logs and samples remain in ignored local evidence storage outside those directories.
No GCP resources were created or reused, and this follow-up incurred no GCP usage charges.
The previous lease's cost estimate and unconfirmed actual billing remain unchanged.
The full 108-cell service matrix, settled marker storage, sustained hot-row growth and production-path acceptance remain pending.
