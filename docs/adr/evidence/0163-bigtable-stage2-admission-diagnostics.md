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

# Bigtable Stage 2 admission diagnostics

This follow-up to the [checkpoint-stall investigation](0163-bigtable-stage2-checkpoint-stalls.md) adds a local diagnostic input control under Accepted [ADR-0163](../0163-bigtable-checkpointed-writes-stage-immutable-mutations-and-retain-row-markers.md).
The owner selected an explicit outstanding-input limit for diagnostic comparisons while retaining the original source as a control.
The [formal Stage 2 protocol](0163-bigtable-staged-performance-protocol.md), its thresholds and its delivery order are unchanged.
No production API or real-service performance acceptance is delivered here; [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211) remains open.

## Input ownership and observation

The optional final argument of `BigtableStage2Probe local` is `maxPendingInputsPerSubtask`.
Omitting it, or specifying zero, retains the existing unrestricted source.
A positive value enables diagnostic credits only for a local timed run; the service command and finite recovery harness do not enable this control.
The source records each sequence's owning reader before handing it downstream and releases its credit only after the ledger records that input's first acknowledgement.
This ordering covers synchronous acknowledgements, and a repeated acknowledgement cannot release another input's credit.
Only outstanding sequence-to-reader entries and per-reader counters/futures are retained.

A reader with no credit returns `NOTHING_AVAILABLE` and an availability future, allowing its task to process checkpoint messages.
Availability requires both a source split and a credit, or the observation ending or the reader closing.
The observation timer wakes a waiting reader even if no acknowledgement arrives.
Source snapshots still use the sequence reader's state; this single-process diagnostic control deliberately rejects replacement readers for the same subtask rather than claiming restart or rescaling support.
The existing finite recovery and service correctness paths retain their previous behavior.
Diagnostic configuration is fixed during construction, and invalid settings are rejected before the inventory is created.
Successful harness teardown terminates the receiver before discarding credits; an acknowledgement already running during close can finish during shutdown.

The bound includes warm-up, measured and tail inputs, and is independent of the input inventory capacity, the writer's per-interval staging limits and the number of outstanding RPCs.
It changes admission, so a successful row is labelled `DIAGNOSTIC_OBSERVATION` and cannot enter the formal gate.
Input-cap exhaustion remains `CENSORED`, including a warm-up-only exhaustion with no measured result.
At observation end, the source also checks whether the complete input inventory was admitted: a last input that consumes the final credit must not hide exhaustion until the delegate's next poll.
The sample baseline identifies the admission mode and limit before input starts.
Periodic and final observations report phase counts, distinct unacknowledged input counts, first/last measured admission offsets, credit peaks and availability waits.
The ledger's input progress is internally consistent; it is sampled separately from credits, RPCs, commit progress and REST state.
Neither a credit count nor an invocation size measures all heap retained by Flink.

## Synchronous checkpoint work

The pinned Flink 2.2.1 and 1.20.4 `CommitterOperator` synchronously drain all eligible checkpoint managers before returning from one completion notification.
Within each manager, `retryLater()` requests are retried in the same call up to the configured retry count; it does not yield the mailbox between chunks.
`Stage2CommitProgressTest.oneCompletionNotificationSynchronouslyDrainsAllEarlierCheckpointCollections` supplies collections of two, three and one requests, aborts the first checkpoint and notifies completion of the third.
With two outstanding requests allowed, the notification remains incomplete while each manually held original future is released, and returns only after all six acknowledgements and all three invocations finish.
The test uses controlled completions rather than an elapsed-time assertion.

For fixed fake delay `d`, concurrency `c` and nonempty collection sizes `N_j`, the idealized service-time term is `sum(ceil(N_j / c) * d)`.
The example above needs four request waves, rather than the three obtained by dividing the combined count by concurrency.
Actual notification occupancy additionally includes request construction, bookkeeping, transport scheduling and cleanup; a subsequent checkpoint also needs its own scheduling and snapshot time.
Service latency percentiles are not worst-case bounds and cannot certify that this total fits a 60-second checkpoint timeout.
The diagnostic input cap limits admitted work but does not itself bound the number of pending checkpoint managers or establish a global checkpoint/heap guarantee.

The standalone checkpoint timeout remains 60 seconds, request handling is unchanged and measured throughput still includes drain.
Visibility latency still includes staging and checkpoint waiting.
The held-commit MiniCluster regression remains a failure control; no larger timeout is selected to conceal it.

## Local execution protocol

The local matrix was fixed before execution at local source snapshot `f4a6f45527d1a80081fe180e2a407e46162f1c24`, whose Bigtable test-source tree is `b8e520710953490c660d8a734b36d826c9748add`.
Review subsequently added the end-of-observation full-inventory check and its regression for an input capacity and credit limit of one; it does not alter admission, acknowledgement or commit processing during the timed window.
The later constructor-validation and receiver-shutdown repairs affect setup and teardown, outside that timed window.
Every completed matrix run admitted fewer than its 1,000,000-input capacity, so that additional check does not change those rows' classification.
A separate fresh-JVM boundary check used capacity one, credit one, 500 ms measured admission and a 60-second checkpoint interval to keep the final credit occupied until observation end.
With zero warm-up the original CLI emitted `DIAGNOSTIC_OBSERVATION`, while the repaired CLI emitted `CENSORED`; with one-second warm-up the repaired CLI emitted `STAGE2_CENSORED no measured inputs`.
All three boundary JVMs drained and removed their owned directories.
Each matrix invocation uses a fresh JDK 17 JVM with Flink 2.2.1, `-Xmx2g`, `-XX:ActiveProcessorCount=4`, the existing two module opens, one subtask, 1 KiB even-key inputs, one-second checkpoints, ten-second warm-up and thirty-second admission.
The input capacity is explicitly raised from the historical 200,000-input controls to 1,000,000 within the existing 64 MB inventory budget; the hard ledger limit is unchanged.
The checkpoint storage stop threshold remains 2 GiB and the complete UTF-8 JSONL cap remains 8 MiB.
The runner's drain allowance remains 120 seconds; the external driver allows 240 seconds per JVM and terminates a process that exceeds it before inspecting its exact owned directory.

The unrestricted controls use concurrency four and delays zero and 20 ms, once per bulk/staged arm, for four observations.
The diagnostic matrix uses concurrency one/four/sixteen, credit limits 32/128/512 respectively, and delays 20/180 ms.
Each diagnostic condition has three fresh-JVM repetitions per arm, ordered bulk/staged, staged/bulk, bulk/staged, for 36 observations.
The two arms of a condition use the same credit limit.
These fake delays calibrate the instrument; they do not model a service latency distribution.
No Maven build or second calibration JVM runs alongside a timed observation.

For reproduction, obtain the test classpath as described in the [timed harness instructions](0163-bigtable-stage2-experiment-harness.md#local-execution), then append the credit limit to the existing local command:

```bash
java -Xmx2g -XX:ActiveProcessorCount=4 \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  -cp "$stage2_cp" io.github.flink.gcp.connector.bigtable.sink.BigtableStage2Probe \
  local /tmp/stage2-owned-admission-control staged 1024 1 4 1000 10000 30000 1000000 180 even 128
```

Use a new directory for every invocation and retain stdout/stderr outside it.
Preserve current compiled classes with their exact classpath and hashes before any later Maven clean; historical archived classpaths are not portable runtimes.
The driver records every exit code, censor/failure result, process lifetime and cleanup outcome rather than dropping unsuccessful cells.

## Results

All 40 preregistered invocations ran: 39 exited successfully, and the unrestricted staged 20 ms control failed at the unchanged checkpoint timeout.
All 36 credit-controlled invocations produced `DIAGNOSTIC_OBSERVATION`, admitted measured inputs without exhausting capacity, and finished with zero pending inputs and zero active/failed commit invocations.
Every JVM removed its exact owned work directory without driver cleanup; no process reached the 240-second external limit.

### Unrestricted controls

| Arm | Delay | Measured inputs | Inputs/s including drain | Visibility p95 | Drain |
|---|---:|---:|---:|---:|---:|
| Bulk | 0 ms | 196,417 | 6547.106 | 1.309 ms | 0.001 s |
| Staged | 0 ms | 219,303 | 7228.218 | 1,581.243 ms | 0.340 s |
| Bulk | 20 ms | 4,850 | 161.417 | 49.178 ms | 0.046 s |
| Staged | 20 ms | No result: checkpoint failure | — | — | — |

The failed unrestricted staged control retained 60 JSONL records (205,696 bytes), including its before-admission baseline.
At the final periodic sample, the first invocation contained 10,439 requests and had occupied the committer for 59.834 seconds; four RPCs were active.
The ledger showed 12,347 admitted inputs, all in warm-up, 9,952 acknowledged and 2,395 pending; 1,908 inputs were still staged in the writer.
REST still showed one completed checkpoint and one in progress at that sample; the subsequent job failure reported checkpoint expiry, so the preterminal zero failed-checkpoint count is not a successful outcome.
There is no completed throughput row for this run.
Even the idealized service-time term for that first collection is `ceil(10,439 / 4) * 20 ms = 52.2 s`, before observed scheduling and bookkeeping overhead.
This local retained failure supports the workload-budget mechanism; it does not recover the missing samples or establish the exact cause of the historical service repetition.

### Diagnostic repetitions

Slash-separated values are repetitions 1/2/3, not confidence intervals.
Commit duration is the maximum completed **single invocation** across the three runs; it is not a measurement of the whole completion notification when several checkpoint collections are eligible.

| Concurrency / credits | Delay | Arm | Measured inputs (r1/r2/r3) | Inputs/s (r1/r2/r3) | Visibility p95, seconds (r1/r2/r3) | Maximum drain | Maximum invocation |
|---|---:|---|---|---|---|---:|---:|
| 1 / 32 | 20 ms | Bulk | 1246 / 1249 / 1234 | 41.502 / 41.595 / 41.099 | 0.050 / 0.050 / 0.052 | 0.028 s | — |
| 1 / 32 | 20 ms | Staged | 960 / 960 / 961 | 31.470 / 31.199 / 31.094 | 0.966 / 0.969 / 0.965 | 0.906 s | 0.941 s |
| 1 / 32 | 180 ms | Bulk | 158 / 158 / 163 | 5.231 / 5.248 / 5.388 | 0.381 / 0.381 / 0.371 | 0.253 s | — |
| 1 / 32 | 180 ms | Staged | 165 / 165 / 165 | 4.787 / 4.837 / 4.929 | 6.035 / 6.058 / 5.913 | 4.470 s | 6.056 s |
| 4 / 128 | 20 ms | Bulk | 4083 / 4027 / 3997 | 135.865 / 134.083 / 133.039 | 0.058 / 0.059 / 0.059 | 0.052 s | — |
| 4 / 128 | 20 ms | Staged | 3084 / 3716 / 3459 | 97.845 / 120.472 / 114.233 | 1.711 / 1.045 / 1.474 | 1.519 s | 1.897 s |
| 4 / 128 | 180 ms | Bulk | 563 / 562 / 558 | 18.546 / 18.574 / 18.469 | 0.369 / 0.369 / 0.369 | 0.356 s | — |
| 4 / 128 | 180 ms | Staged | 645 / 645 / 645 | 19.199 / 19.129 / 19.420 | 5.956 / 5.923 / 5.934 | 3.719 s | 5.914 s |
| 16 / 512 | 20 ms | Bulk | 19246 / 19117 / 19323 | 640.863 / 636.723 / 643.234 | 0.041 / 0.042 / 0.043 | 0.040 s | — |
| 16 / 512 | 20 ms | Staged | 15360 / 15360 / 15360 | 498.477 / 508.273 / 498.644 | 0.934 / 0.947 / 0.941 | 0.814 s | 0.817 s |
| 16 / 512 | 180 ms | Bulk | 2150 / 2133 / 2134 | 71.501 / 70.618 / 70.565 | 0.362 / 0.362 / 0.358 | 0.241 s | — |
| 16 / 512 | 180 ms | Staged | 2259 / 2323 / 2515 | 69.723 / 69.244 / 75.000 | 5.551 / 4.811 / 5.850 | 3.548 s | 5.971 s |

Across diagnostic runs, total admission ranged from 212 to 25,766 inputs, with no tail inputs.
First measured admission occurred between 0.0001 and 3.177 seconds into the 30-second measurement window, and last measured admission between 27.284 and 29.999 seconds.
Thus the population is uncensored but admission is not continuous: the staged source waits for credits while commits run.
Staged per-reader credit peaks were 32/128/512 and largest observed invocation sizes were at most the corresponding limits.
Bulk peaked at 2/5/17 outstanding credits and never waited for a credit; equal configured limits did not make the arms' actual admission processes identical.
All 40 sample files parsed as JSONL with a before-admission baseline; the largest was 293,694 bytes, below 8 MiB.

Final checkpoint failure counts were not uniformly zero.
Eleven diagnostic baselines already contained one failed checkpoint before admission, and five runs accumulated seven additional failure-count increments by shutdown.
Those five runs were staged concurrency one/180 ms repetitions 1–3, staged concurrency four/20 ms repetition 1, and bulk concurrency four/180 ms repetition 3.
Their retained failed-checkpoint histories report tasks closing or the coordinator suspending; the two suspending cases each log two abort paths for the same checkpoint and an operator-coordinator gateway error during shutdown.
The runs still reached successful job completion and drained every input, but these shutdown diagnostics are retained rather than described as zero-failure checkpoint runs.
No diagnostic run failed at the checkpoint timeout; this observation does not establish a general completion-notification bound.

### Local lease-read contribution

After the matrix, a separate local probe called the pinned `Stage2Lease.requireLive()` against a new 785-byte synthetic properties file, without creating clients or contacting GCP.
Each fresh JDK 17 JVM used the same heap/processor settings, 5,000 warm-up reads and 20,000 timed observations, with one or two reads per observation.
The three paired orders were one/two, two/one, one/two; the file was removed and absence checked after each JVM.

| Reads per observation | Mean, microseconds (r1/r2/r3) | p95, microseconds (r1/r2/r3) |
|---|---|---|
| One | 66.990 / 68.912 / 74.101 | 101.208 / 110.834 / 115.583 |
| Two | 136.012 / 126.884 / 142.265 | 206.084 / 178.000 / 218.959 |

This measures repeated reads of a warm local file, not a cold filesystem, concurrent lease publication or end-to-end RPC cost.
The staged send path reads the manifest both through target preflight and attempt accounting, while bulk accounts at the transmitted-request boundary; the one/two-read rows isolate a component, not equivalent units of either transport.
Retain that asymmetry when interpreting future service throughput; do not subtract these medians or means from latency samples or attribute the whole arm difference to the service.

## Next service experiment

The next experiment asks whether the bounded diagnostic source admits measured inputs with real conditional RPC latency, how long a complete synchronous notification occupies the task, and how that changes visibility latency relative to bulk writes.
The fake delay cannot answer those service questions, nor establish server CPU, actual billing or settled physical marker storage.
It does establish a local comparison with explicit input ownership, an uncensored measured population and retained failure observations.
Local cancellation, snapshot/restore behavior and checkpoint-manager iteration do not need another service lease to be established.

### Prerequisites before charging

The current service command deliberately refuses diagnostic credits.
Before execution, implement and review a separately identified diagnostic lease profile that fixes the seven cells below, their credit limits and reservations in a new manifest, preserves the existing safety checks, and labels every result `DIAGNOSTIC_OBSERVATION` or `CENSORED`.
Do not repurpose a historical manifest or silently change the unrestricted formal protocol.
Exercise that profile's reservation, stop, deletion and failed-evidence paths locally first, and freeze the reviewed source, classpath, exact resource IDs and command arguments before creating anything.

The existing service paths reload the local lease manifest per request; the staged path also checks its target during preflight.
That local I/O differs between arms and is absent from these fake runs.
The component measurement above establishes its local cost for this fixture; recheck the actual new manifest shape and record any instrumentation change before the lease.

### Targets and workload

Use the local JDK 17 host and the same heap, processor, Flink and module-open settings as the local matrix, with one worker JVM at a time.
Create one newly owned instance in project `flink-gcp`, zone `us-central1-b`, named `flink-s2-<epoch-seconds>-<six-hex>`, with a fresh 32-hex `stage2-owner` token, one SSD node, cluster `stage2-c1`, and transactional single-cluster profile `single-cluster`.
The manifest must contain the resolved names, rather than these placeholders, before creation.
Tables are `serialized`, `bulk-r1`, `staged-r1`, `staged-r2`, `bulk-r2`, `bulk-r3` and `staged-r3`; each has `cf` and raw no-GC `flink_commit`.
No GCE worker, GCS bucket, IAM change, custom metric, autoscaling or shared resource is required.

Run `serialized` first: staged, concurrency one, 32 credits per source reader.
Then run the six comparisons at concurrency four and 128 credits per reader, ordered bulk/staged, staged/bulk, bulk/staged.
All seven cells use 1 KiB even-key inputs, parallelism one, one-second checkpoints, ten-second warm-up, thirty-second measured admission and a fresh JVM/table.
Each has an 8,000-input safety inventory.
That capacity provides headroom over the local 20 ms/concurrency-four diagnostic totals; it is not a prediction that the service cannot exhaust it.
Exhaustion, zero measured inputs or an incomplete observation stops the experiment without increasing capacity or repeating a cell in that lease.
The serialized cell remains a Flink visibility measurement, not a raw RPC benchmark.

Keep the 60-second checkpoint timeout, 120-second drain allowance, 8 MiB sample cap and 2 GiB observed local storage stop threshold.
Bound each worker process to 240 seconds, including startup, and retain its exit code, full configuration, phase counts, commit/checkpoint progress and failed samples.
Require successful cleanup as well as an uncensored row before including a result in the diagnostic comparison.
Run metadata preflight on the actual transactional profile and marker family before the first conditional client.
Do not repeat the already recorded invalid-profile and SUM recovery experiments merely to enlarge this latency comparison.

### Cost, limits and stopping

Reserve at most 250,000 attempted writes, 1 GiB declared write bytes and 1 GiB combined readback/monitoring bytes in the locked manifest, without refunds.
For seven 8,000-input cells and a conservative four-attempt reservation per input, the write reservation is `7 * 8,000 * 4 = 224,000` attempts.
Using a declared payload-plus-overhead bound of 2,048 bytes per attempt gives 458,752,000 bytes, below 1 GiB; the implementation must verify its actual request envelope against that reservation.
One bounded readback allowance of 2,048 bytes per input gives 114,688,000 bytes across the seven cells, leaving the remainder of the 1 GiB limit for monitoring and protocol overhead.
These are reservation ceilings, not instructions to generate retries or claims about compressed physical storage.
SDK retries continue to consume the shared wire budget and must fail closed at the limit.

At the official us-central1 prices checked for this plan, Enterprise compute is $0.65/node-hour and Enterprise Plus is $0.85/node-hour.
Reserve two node-hours for a lease lasting at most one hour across an hourly boundary: $1.30 or $1.70 respectively, without in-memory capacity.
SSD storage is $0.000232877/GiB-hour, and internet readback from North America to Japan is $0.12/GiB in the first volume tier.
A $2 incremental lease reservation leaves $0.18 beyond the more conservative $1.70 compute and $0.12 egress reservations for storage and other metered overhead.
Recheck edition, rates and allowances immediately before creation; see [Bigtable pricing](https://cloud.google.com/bigtable/pricing).

The aggregate authorization is $20 including past usage.
The prior lease's less-than-$2 estimate is not confirmed billing, so the currently available remainder is not established by this record.
Before creation, reconcile prior billed and still-unbilled usage into a conservative ledger and require `prior usage + outstanding reservations + $2 <= $20`.
If that cannot be established, stop before charging and report the concrete missing budget evidence.
This local continuation creates no GCP resources and adds no GCP usage charge.

Start the independently supervised lease before creation, admit no new cell after 45 minutes, begin cleanup at 50 minutes and reserve the final ten minutes within a 60-minute lease for deletion and verification.
Stop immediately on worker failure, censoring, resource disappearance, ownership mismatch, safety-limit exhaustion or unavailable required evidence.
Signal the lease stop file, terminate only its verified worker PID/start instant, preserve logs and samples, and proceed to cleanup.
There is no automatic retry, instance recreation, capacity increase or lease extension.
The local host must remain running; supervision is not a server-side TTL.

### Cleanup and interpretation

Resolve the recorded creation attempt, check the exact instance's owner label, delete only that owned instance and verify both direct lookup and list absence.
Its tables, profile and cluster are removed with it.
For an externally deleted instance, establish the prior creation outcome before accepting absence; permission errors or unresolved creation are cleanup failures.
Remove only the manifest's owned local work directory, verify absence and retain the manifest, worker logs and success/failure samples.
An interrupted cleanup resumes against that same manifest without creating a replacement resource.
Capture the existing bounded monitoring snapshot once after deletion and publication delay; preserve partial output and record missing metrics rather than extending the lease.

Compare all three pairs with their measured counts, admission offsets, drain, full-notification work and visibility distribution; show failures and censoring beside successes.
An improvement would establish only that these seven diagnostic cells can be observed under this source control.
The original 108-cell Stage 2 acceptance matrix, thresholds, hot-row/storage questions and ADR delivery order remain outstanding; passing this small comparison does not authorize production implementation or a formal-gate claim.
