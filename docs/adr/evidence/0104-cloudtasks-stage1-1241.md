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

# Cloud Tasks Stage 1 repeat protocol and result

This records the preregistration and 2026-09-06 result of [#1241](https://github.com/flink-gcp/flink-connector-gcp/issues/1241), supporting [ADR-0104](../0104-exactly-once-modes-use-service-native-replay-protection-and-pass-a-performance-gate.md).
The approved run stopped during the first replay warm-up when two concurrent submissions returned successful responses with the same task name.
All four comparisons are inconclusive; the [result below](#result-on-2026-09-06) preserves the completed observations and verified cleanup.
The protocol sections describe the plan frozen before execution, not a claim that every planned cell ran.

## Question and scope

Compare unnamed v2 `CreateTask` with the two name shapes selected by [ADR-0158](../0158-cloud-tasks-checkpointed-creation-stages-named-tasks-and-commits-after-the-checkpoint.md): a SHA-256 hex digest of a distinct event key and 128 random bits rendered as 32 lowercase hex characters.
Measure one-channel and eight-channel clients separately, always using the same transport configuration for the baseline and its candidates.
These are service-client observations for HTTP tasks on a paused queue, not Flink checkpoint, recovery, handler-execution, or App Engine performance measurements.
Same-name collision outcomes do not measure tombstone retention after execution, deletion, purge, or expiry.

No connector writer, committer, public option, default, or supported delivery mode changes here.
One submitted record still issues one `CreateTask`; [ADR-0129](../0129-the-cloud-tasks-sink-keeps-one-create-rpc-per-record-and-declines-v2beta3-batchcreatetasks.md)'s batch-create decision remains in force.
A passing applicable result is only the primitive gate for the already accepted ADR-0158 design; end-to-end acceptance remains [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246).

## Fixed inputs

| Input | Value |
|---|---|
| Dependency pin | `libraries-bom` 26.87.0; `google-cloud-tasks` and v2/v2beta3 task protos 2.96.0 |
| Runtime | Java 17, `-Xms512m -Xmx2g`; one child JVM at a time |
| Transport | Default single channel, or the service's default gRPC provider builder with `ChannelPoolSettings.staticallySized(8)` |
| Request | v2 `createTaskCallable().futureCall`, `BASIC` response view, HTTP POST, 1,024 zero body bytes |
| Naming | SHA-256 of a run/cell/phase salt and logical ordinal, or one `SecureRandom` 128-bit value per distinct record |
| Task scheduling | Twelve hours after the experiment starts; the queue remains paused |
| In-flight bound | 1,000 calls; 1 for the serialized control |
| Retry and timeout | Verify the pinned `CreateTask` defaults: no retryable codes and a 20-second total timeout; no harness retry |
| Region | `us-central1`, with a separate fresh queue for each channel configuration |

Generate each phase's requests before starting its timer.
The same name and complete task bytes are reused for an intentional replay; distinct records with identical bodies have distinct names.
Check name uniqueness before sending.
The offline distribution control examines 100,000 submissions per naming scheme, of which 90,000 are distinct, and requires every first-byte bucket to contain between 200 and 500 distinct names.
The experiment excludes identity generation and Flink state costs from RPC latency; those costs belong in end-to-end measurement.

Record the resolved dependency tree, Java version, operating-system architecture, source checksums, queue readback, and the exact cell order before accepting a run.
The task URL uses `example.invalid`; no HTTP or App Engine service is provisioned.

## Cells and order

Each channel configuration has five main arms: unnamed, hash-named, random-named, hash-named with 10% replay, and random-named with 10% replay.
Each arm has three measured repetitions.
Each repetition starts a fresh JVM, performs 5,000 warm-up submissions, drains them, and measures 50,000 submissions with fresh identities.
The replay rule replaces every tenth submission with the previous submission, giving 45,000 distinct tasks and 5,000 collisions in each measured replay cell.
The two racing calls need not receive their responses in submission order; the oracle counts one `OK` per distinct identity and the expected number of `ALREADY_EXISTS` responses for that identity.
A replay can overlap its original request; this arm does not isolate the cost of retrying only after a successful response has already arrived.
A collision in a non-replay arm is an error, not a successful record.
Within a replay arm, a collision for an identity submitted once, or a second collision for a pair, stops further admission when its callback is observed.

The driver processes one channel configuration completely before the other.
Within each repetition it shuffles the five arms with Python's `random.Random(1241)`; the generated manifest lists every cell in its final order.
After the main cells, each channel configuration runs three random-named serialized controls, each with 10 warm-up and 100 measured submissions.
Wait 30 seconds before each main/control JVM.
No second application workload, Maven build, or stress test runs concurrently with a measured JVM.

Before the measured cells, condition each fresh queue for five minutes at at most 500 requests/s, then five minutes at at most 750 requests/s.
These are duration-bounded, unnamed, unscored ramp cells; a slow client does not extend a ramp until a fixed count is reached.
The main and control cells use a non-bursting admission ceiling of 950 requests/s.
This follows the [500/50/5 guidance](https://cloud.google.com/tasks/docs/manage-cloud-task-scaling) and stays below its recommended 1,000 creates-plus-dispatches per second.
Report time spent waiting for the admission limiter: a capped result describes this offered-load condition and is not an uncapped capacity estimate.
The gate is conservative about that distinction: any limiter sleep after the first 1,000 submissions of a measured main arm makes its applicable comparison inconclusive.
The first 1,000 submissions form the initial in-flight window; their paced startup still enters elapsed time and total limiter waiting, but is excluded from this additional indicator.
The indicator counts actual sleep, not the clock reads around a limiter check that did not wait.
This can reject bursty as well as steadily capped observations; no small waiting fraction is silently treated as uncapped capacity.

## Measurements and decision rule

Persist one raw CSV row per completed call with its ordinal, logical identity ordinal, start offset, latency, status, returned task ID and dispatch count.
The per-cell summary records completed submissions, newly created tasks, deduplications, elapsed time, throughput, p95, total and post-window admission-limiter waiting time and the observed in-flight peak.
Throughput divides completed submissions by the interval from timer start to the final callback, including drain time.
The reported RPC latency starts at the harness timestamp before dispatch bookkeeping and request construction and ends at the callback timestamp; time waiting for the harness's rate limiter precedes that interval.
The p95 is the nearest-rank value at `ceil(0.95 * count)`.
Warm-ups and ramps do not enter measured means.
Python independently reconstructs elapsed time, throughput, p95 and outcome totals from the raw measured CSVs.

For every named phase, read back the first ten submitted names with the `FULL` view and require the original body and zero dispatches.
Every successful create response must also carry the expected name, where named, and zero dispatches.
Before and after each phase, use a separate v2beta3 client to read `GetQueue` with the explicit `stats` mask and record `QueueStats.tasks_count` outside the timer.
This is a service estimate, not an exact task inventory; v2 exposes no corresponding field in the pinned API.
The queue accumulates tasks across repetitions: the planned maximum is 1,167,330 distinct tasks per queue, including ramps and controls and subtracting intentional replay collisions.
Publish these estimates with the cell order and observed creation totals; repetition and increasing queue depth are not independently controlled in this two-queue design.
Use arithmetic means of the three throughputs and of the three per-repetition p95 values; do not pool samples into a different p95 definition.
Throughput variation is `(maximum - minimum) / mean` within one arm and channel configuration.

For each naming scheme and channel count, apply the following rules in order:

1. Missing repetitions, unexpected RPC errors, an identity/readback mismatch, or unverified cleanup make the applicable result inconclusive.
2. A throughput range greater than 10% in the baseline, candidate, or its replay arm makes that candidate inconclusive.
3. Missing pacing observations or any post-window limiter sleep in the measured baseline, candidate, or replay arm make that candidate inconclusive; a capped comparison cannot establish a capacity gate pass.
4. The serialized control's mean throughput must be below 25% of that candidate's mean; otherwise the sensitivity check is inconclusive.
5. Apply ADR-0104's unchanged thresholds: general support at at least 70% of baseline throughput and at most 2x baseline mean p95; constrained opt-in at at least 25% and at most 4x; otherwise decline.

Publish all four naming-scheme/channel-count decisions.
Do not average across channel counts or use a passing naming scheme to conceal a failing one.
A constrained result names the measured HTTP payload, distribution, in-flight limit, admission ceiling and channel count as its workload scope.
A stopped eight-channel run does not erase completed, valid one-channel observations, but missing cells never qualify.
No attempt is added, replaced, or selectively omitted to obtain a pass.

## Resource and stop plan

Use the existing test project selected by `GOOGLE_CLOUD_PROJECT`.
The manifest fixes two queue IDs, `flink-ct-s1-1241-<run-id>-c1` and `flink-ct-s1-1241-<run-id>-c8`, and their complete resource names are presented for approval before any GCP call.
The queues are created sequentially; the first is deleted and verified absent before the second is created.
A pre-existing name is a collision and is never reused or deleted.
Create each queue empty, configure dispatch concurrency/rate to 1 and `maxAttempts` to 1, pause it, and confirm `PAUSED` before submitting any task.
Check its paused state at cell entry and exit; the future schedule is an additional guard while the queue is alive.
The dispatch budget is zero.

| Budget | Preregistered limit |
|---|---:|
| Main warm-up and measured submissions | 1,650,000 |
| Two five-minute ramp stages on each queue | At most 750,000 |
| Serialized controls, including warm-ups | 660 |
| Total CreateTask authorization | At most 2,450,000; unused allowance is not a repeat budget |
| Auxiliary API-call reservation | At most 10,000 |
| Conservative billable-operation estimate, including one allowance per task removed at cleanup | At most 5,000,000 32-KB units |
| Cost ceiling | USD 3 including network allowance; no free-tier credit assumed |
| Admission lifetime | 150 minutes from experiment start |
| Total lifetime, including cleanup | 180 minutes |

The [published price](https://cloud.google.com/tasks/pricing) is USD 0.40 per million operations beyond the free tier, so the operation estimate reserves USD 2 and leaves USD 1 for network use.
Tasks and their basic responses remain below one billing chunk; the cleanup allowance is deliberately conservative, not a claim that deleting a queue bills one operation per task.
Reserve a child's entire possible submission count before launching it.
Each phase has a 15-minute deadline and each child a 1,000-second supervisor timeout; start a child only when that full window fits after any required idle period.
The global monotonic supervisor budget includes JVM startup, shutdown, pauses, and ramp-up.
Stop admissions on the first unexpected RPC error, identity or dispatch failure, queue-state failure, exhausted budget, or deadline.
Preserve partial raw output and report which cells did not finish.

On success, failure, cancellation, or a lost create response, attempt cleanup of only the exact queue whose creation this run attempted after an absence check.
An explicit `ALREADY_EXISTS` response to `CreateQueue`, including a collision after the absence check, establishes non-ownership and suppresses deletion; a failure before the create attempt also suppresses deletion.
Stop the child process before cleanup, delete the queue, and require a separate `GetQueue` result of `NOT_FOUND`.
Cleanup may retry this deletion/absence sequence at most three times within the reserved lifetime.
A failed cleanup is reported as unresolved and is never marked verified merely because the delete call returned.
A configuration that was never created is reported as unrun with no owned resource requiring cleanup, separately from failed cleanup of an attempted creation.

## Reproduction and completion evidence

The standalone Maven harness, Python driver, tests, source checksums and exact manifest are retained outside the repository; public source and result records belong on [#1241](https://github.com/flink-gcp/flink-connector-gcp/issues/1241).
The driver records a clean-build stamp and refuses changed sources, compiled classes, ordered dependency jars or Java runtime before preparing or accepting a manifest.
It also rejects a changed protocol and requires the manifest's exact SHA-256 as the execution argument after the operator grants resource approval.
That argument prevents accidental execution of a changed manifest; it does not grant approval itself.
Credential material, environment files, local account identities and private host paths are excluded from public evidence.

Before a real run, execute the service-free Java and Python tests, including boundary arithmetic, reordered replay responses, failed sibling cancellation, wrong creation counts, incomplete cells, manifest changes, operation caps and exact-target cleanup.
The historical Cloud Tasks raw numbers are also a regression fixture for the inconclusive classification.
After the approved run, append the manifest/source identifiers, all raw repetitions, means, variation, replay outcomes, control results, limits reached and cleanup observations to this record, then update the delivery-guarantees page.
The PR stays draft until those observations and the review/CI evidence are complete.

## Result on 2026-09-06

Run `20260906b` stopped under the preregistered error rule during the one-channel hash-replay warm-up.
The operator approved its exact resources, limits and publication before execution; the [preregistration](https://github.com/flink-gcp/flink-connector-gcp/issues/1241#issuecomment-5558530090), [Java source and tests](https://github.com/flink-gcp/flink-connector-gcp/issues/1241#issuecomment-5558528619), and [Python source and tests](https://github.com/flink-gcp/flink-connector-gcp/issues/1241#issuecomment-5558529642) were published before any GCP request.
The [full result](https://github.com/flink-gcp/flink-connector-gcp/issues/1241#issuecomment-5558678088) includes full-precision phase summaries, all 223 terminal rows of the failed warm-up, and the execution artifact checksums.
No repeat was attempted.

The manifest SHA-256 is `13fd2d2c406631f752a52ee2055723b63c31091d18c1f424b3a87faad758c330`.
It binds the unchanged source, 13 compiled classes, 50 ordered dependency jars and Java runtime.
The runtime was Temurin 17.0.20+8 on macOS/aarch64, with the pinned BOM 26.87.0 and Cloud Tasks 2.96.0.
The frozen result inventory SHA-256 is `7eb9f6a57ef7f4dc3cc090e2d88c56417feebb7556ee334fdb59cd60d6aff0cb`.

### Completed observations

Only repetition 1 of the one-channel random, hash and unnamed arms completed its 50,000-submission measured phase, after a separate 5,000-submission warm-up.
Each reported 50,000 successful callbacks, no `ALREADY_EXISTS`, and zero dispatches; the named phases also completed their body/dispatch readbacks.
All three observed post-window limiter waiting.
These single observations do not supply the three-repetition means, variation or sensitivity comparisons required by the gate; those values remain unavailable.

| Arm, in execution order | Throughput (submissions/s) | p95 (ms) | Elapsed (s) | Post-window limiter wait (s) | Estimated tasks before / after |
|---|---:|---:|---:|---:|---|
| Random, repetition 1 | 350.259577 | 4,293.213208 | 142.751271 | 107.628290 | 202,740 / 252,740 |
| Hash, repetition 1 | 358.454406 | 4,433.048917 | 139.487754 | 102.284105 | 257,740 / 307,740 |
| Unnamed, repetition 1 | 335.828987 | 4,154.804000 | 148.885302 | 90.974992 | 312,740 / 362,740 |

The unscored five-minute ramps completed 101,445 and 96,295 submissions, with estimated depths of 101,445 and 197,740 afterward.
The three main warm-ups each completed 5,000 submissions.
Their full timing and depth observations are in the linked result and do not enter the table's measured values.
The queue accumulated tasks across these cells, so repetition and queue depth were not independently controlled.

### Concurrent replay observation

The hash-replay warm-up admitted 223 submissions before stopping.
Its terminal rows contain 53 successful `Task` callbacks, one `ALREADY_EXISTS` and 169 local cancellations.
The 53 successful responses name 52 distinct task IDs; they are not a count of distinct creations.
Cancellation also does not establish whether a request had a service-side effect.

Zero-based attempts 88 and 89 both targeted logical identity 88 and returned the expected task ID `c88d3990cd0ed0685ec22a390c3ffb586d9bbeaa1d58396f68eea89b4f5238a6`.
It is SHA-256 of `20260906b:c1-r1-hash-replay:warmup:88`, as the frozen source specifies.
The second submission began before the first submission's success callback recorded its completion time.

| Attempt | Start offset (ns) | Latency (ns) | Callback outcome | Reported dispatch count |
|---|---:|---:|---|---:|
| 88 | 173,585,416 | 245,056,459 | Successful `Task` | 0 |
| 89 | 175,302,041 | 244,563,875 | Successful `Task` | 0 |

The pair violated the preregistered oracle of one successful response and one `ALREADY_EXISTS`.
The callback validator raised `IllegalStateException: Missing/repeated task name in successful response`; the harness stopped admission and the supervisor cleaned up.
The raw CSV records `OK` for a successful `Task` callback even when the validator subsequently rejects that response.
Every admitted ordinal from 0 through 222 has one terminal row.
The raw rows do not identify which callback failed validation, because the completion timestamp is captured before validation and the CSV records both successful callbacks as `OK`.

Another overlapping pair in the same warm-up produced the expected outcomes: attempt 58 returned a successful `Task`, and attempt 59 returned `ALREADY_EXISTS` for logical identity 58.
Attempt 59 started at offset 118,254,416 ns, before attempt 58's callback timestamp of 361,935,250 ns (start 116,223,708 ns plus latency 245,711,542 ns).
Concurrent submissions therefore did not produce two successful responses for every observed pair.

Attempts 88 and 89 establish two successful responses with the same name under concurrent submissions.
These responses do not establish two persisted tasks, a lost task, a tombstone-retention bound or Flink recovery behavior.
No post-failure inventory or task readback ran before cleanup; the last depth observation was 362,740 before this warm-up.
The replay warm-up produced no valid phase summary, and its measured phase never started.

### Classification and cleanup

| Naming scheme | Channels | Outcome | Reason |
|---|---:|---|---|
| SHA-256 | 1 | Inconclusive | Replay warm-up stopped; repetitions and controls are missing |
| Random | 1 | Inconclusive | Repetitions, replay measurement and controls are missing |
| SHA-256 | 8 | Inconclusive | Configuration was not reached or created |
| Random | 8 | Inconclusive | Configuration was not reached or created |

Missing observations are sufficient for these classifications.
The post-window waiting in each completed measured arm independently excludes a capacity gate pass under the preregistered rule.
No throughput or p95 ratio is promoted to a gate result, and no three-repetition mean, range or serialized-control result is filled from a single sample.

The run ended after 1,280.467810709 seconds, including cleanup.
The nine raw phase files contain 362,963 submissions: 362,793 successful callbacks, one `ALREADY_EXISTS` and 169 local cancellations.
The conservative reservation was 595,000 `CreateTask` calls and 480 auxiliary calls, below the approved limits of 2,450,000 and 10,000; the USD 3 ceiling was unchanged.
These counts are observed submissions and reserved operations, not a billing statement.

The one-channel queue was paused at the recorded checks and all successful `Task` responses reported zero dispatches.
Deletion returned `DELETED`; a separate `GetQueue` then returned `NOT_FOUND`, recorded as `ABSENT`.
The eight-channel queue was never created and owned no resource requiring cleanup.
This is verified queue cleanup, not a complete dispatch audit of canceled requests.

A separate Python arithmetic pass checked all nine raw files after execution: unique contiguous admission ordinals, terminal status totals, nonnegative latency, dispatch counts on successful callbacks, and exact elapsed time, nearest-rank p95 and throughput for all eight completed phases.
It also checked the concurrent pair against the name-generation formula and verified the cleanup markers.
The linked result identifies every raw CSV and process/admin log by SHA-256; the complete per-call archive remains retained outside the repository.

This repeat supplies no applicable performance pass for the checkpointed-creation runtime under [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238).
Interpreting the concurrent response observation and designing another measurement require a separately scoped follow-up decision; this run does not change the accepted design or the replay oracle after seeing its outcome.
End-to-end acceptance remains [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246).
