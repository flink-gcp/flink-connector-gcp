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

# Stage 2 experiment harness and bounded service lease

This test-only instrument implements the timed path of the [Stage 2 protocol](0163-bigtable-staged-performance-protocol.md).
It follows Accepted [ADR-0163](../0163-bigtable-checkpointed-writes-stage-immutable-mutations-and-retain-row-markers.md).
Neither the local calibration nor the bounded service observations below constitute the formal performance gate.
[#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211) remains open; there is no production checkpointed-write API.

## Instrument and limits

`BigtableStage2Probe` runs the existing production bulk writer or the experimental staged writer/committer in an owned MiniCluster.
The finite [local harness](0163-bigtable-local-staged-harness.md) retains its literal-loopback endpoint restriction.
Only an explicit, separately planned `Stage2Lease` enables the service factories.

A continuous source admits through Flink backpressure until a wall-clock window ends.
Its deterministic sequence is also bounded by a safety input capacity.
Every distinct admission and successful acknowledgement occupies one fixed 64-byte disk slot; completed requests, payloads and futures are not retained by the timed receiver.
The ledger bounds storage to 64 MB and one million inputs.
Post-run percentile sorting and readback allocate arrays/maps bounded by that capacity, outside the admission window.
This is a single-process experiment: ledger timestamps and the static run registry do not support migration to another JVM.
Fresh JVMs isolate repetitions; correctness recovery restarts tasks and MiniClusters within a repetition.

Warm-up, measured and tail inputs have separate row namespaces.
The even-key prefix is a fixed SplitMix64-style hash; the hot arm sends sequence numbers not divisible by ten to its phase's hot row.
A 10,000-input histogram checks the fixed generator before each timed run.
There is no intentional rate limiter.
Disk-ledger access, allocation, serialization, REST sampling and the chained source itself can limit throughput; no-service calibration exercises these costs.
Service lease checks additionally reload the manifest twice per staged send and once per bulk wire request; the local calibration does not cover that asymmetric cost.
Its effect on the service comparison was not measured separately.

The numerator counts distinct measured inputs from the ledger.
The denominator ends at the last measured acknowledgement, including its checkpoint wait and drain.
Visibility percentiles use exact admission-to-acknowledgement durations.
Client p95 uses a fixed logarithmic histogram and reports its bucket upper bound (less than 1.6% quantization for durations above 64 ns).
Warm-up completions are excluded from that client histogram.
Source polling time includes synchronous downstream work in the chained operator; it is not a pure source-generation CPU measurement.
Committer wait time includes processing the awaited result.
Neither counter establishes a global heap bound on Flink's retained committables.

The source marks an exhausted input capacity `CENSORED`.
If exhaustion occurs in warm-up, the runner reports no measured inputs instead of inventing a throughput or latency.
A staging, operation or observed-storage overflow terminates as `CENSORED_WORKLOAD_LIMIT`; an undrained or otherwise failed run has no valid performance result.
Storage is sampled, so the 2 GiB checkpoint limit is an observed stop threshold, not an atomic filesystem quota.
No cap is raised automatically.

The sampler records checkpoint REST responses, staged entries/bytes, active conditional requests, and the available pending-committable/busy/backpressure/idle metrics once per second.
The [checkpoint-stall follow-up](0163-bigtable-stage2-checkpoint-stalls.md) adds input totals and active commit-invocation sizes/ages without retaining completed request history.
An initial checkpoint response is written before admission opens, so even an immediate workload failure retains that baseline.
It rediscovers metric names because committer registration may lag task registration.
Missing metrics remain missing.
An existing Flink duplicate-name warning can prevent a pending-committable metric from representing every chained committer; the raw response is evidence, not proof of an empty global backlog.
Heap is process-wide in this MiniCluster experiment.
Peak heap is sampled, while heap after an explicit collection request and GC time come from JVM management beans.
Allocation measurements cover serializer/deserializer calls where the JVM supports thread allocation accounting; `-1` means unavailable.

Successful timed runs preserve bounded `*-samples.jsonl` files beside the owned work directory (beside the manifest for service runs).
Failed runs now preserve an available bounded `*-failed-samples.jsonl` before cleanup; a failed evidence copy is attached to the original failure.
The output's `STAGE2` CSV fields, in order, are:

1. Arm, observation/censor status, distinct measured count, throughput in inputs/second.
2. Visibility p50/p95/p99, measured drain, client p95 bucket upper bound (all nanoseconds).
3. Peak writer entries, peak writer bytes, peak tracked active requests.
4. Heap after collection request, sampled peak heap (bytes), accumulated GC time (milliseconds).
5. Serialization allocation, restore allocation (bytes), source poll time, committer wait time (nanoseconds), source starvation count.

Readback compares acknowledged inputs with generated data-cell values and cardinalities, and compares every retained marker with its frozen envelope identity.
A hot-row winner must be one of the submitted values for that cell version; overwrites do not count individual applications.
The separate fixed-timestamp INT64 SUM oracle checks distinct contributions after response loss and persisted-envelope replay.
Readback reports marker count, maximum markers on one row, and logical marker key/family/qualifier/value/timestamp bytes.
Logical marker bytes omit physical encoding, replication and compression overhead; they are not billable table storage.

Preflight reads the actual app profile and marker family before creating the conditional data client.
A single-cluster transactional profile and a raw marker family without an effective GC rule are required.
The protobuf default GC value is an empty rule, which the pinned SDK also emits; an absent raw value type is distinct from an explicitly typed Bytes family.
Restored envelopes are validated before transport, and each new data-client factory revalidates metadata even after an earlier successful preflight.
Local tests inject denied metadata access and assert that no data client/send occurs.
The bounded service run checks real invalid routing and family metadata without changing IAM policies.

## Local execution

Use JDK 17 and the repository-pinned dependencies.
Compile tests with Maven, and obtain the test runtime classpath from a Surefire report's `surefire.test.class.path` property.
When switching Flink lines, clean reactor outputs first and preserve each line's compiled classpath in a separate ignored directory.
Do not rely on project SNAPSHOT jars already installed in the local Maven cache.

The entry point is `io.github.flink.gcp.connector.bigtable.sink.BigtableStage2Probe`.
For example, with `stage2_cp` set to that classpath and a new, owned absolute directory:

```bash
java -Xmx2g -XX:ActiveProcessorCount=4 \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  -cp "$stage2_cp" io.github.flink.gcp.connector.bigtable.sink.BigtableStage2Probe \
  local /tmp/stage2-owned-calibration staged 1024 1 4 100 10000 30000 200000 20 even
```

Arguments after the directory are arm, payload bytes, parallelism, in-flight limit, checkpoint milliseconds, warm-up milliseconds, measured milliseconds, input capacity, fake response delay milliseconds and key distribution.
Local mode performs no GCP calls and removes its exact checkpoint/inventory directory in `finally`.
Use a unique directory name for each invocation; retained evidence files are never overwritten.
Keep stdout/stderr outside that directory.
The emulator and fake correctness tests run through the scoped Maven test suite; service commands are standalone and never run in ordinary CI.

## Authorized small service experiment

The execution approved for this continuation uses project `flink-gcp`, zone `us-central1-b`, one SSD node, and a unique instance `flink-s2-<epoch-seconds>-<six-hex>`.
The manifest fixes a random 32-hex owner token checked against the instance's `stage2-owner` label before deletion.
There is one cluster, `stage2-c1`, and one transactional single-cluster profile, `single-cluster`.
Negative metadata profiles are `no-tx` and `multi-cluster`.
Tables are exactly:

- `bulk-r1`, `staged-r1`, `staged-r2`, `bulk-r2`, `bulk-r3`, `staged-r3`.
- `hot`, `serialized`, `recovery-flink1`, `recovery-flink2`.
- `marker-missing`, `marker-gc`, `marker-typed`.

Each table has `cf`; ordinary tables also have the raw no-GC `flink_commit` marker family.
Recovery tables additionally have `agg` INT64 SUM.
The three invalid-family fixtures intentionally violate marker preflight.
Creating/deleting the instance also creates/deletes its exact tables, profiles and cluster; there is no shared-instance table deletion.

The six comparison repetitions use 1 KiB, even keys, parallelism 1, in-flight 4, checkpoint 1 second, warm-up 10 seconds and admission 30 seconds.
Order is bulk/staged, staged/bulk, bulk/staged, with a fresh JVM/table each time.
Each repetition has a 4,000-input safety capacity.
The separate hot-row phase uses at most five minutes of admission and 35,000 inputs.
The serialized conditional control uses in-flight 1 and 256 inputs, with ten seconds each of warm-up and admission; it still includes Flink staging/checkpoint waiting.
Recovery uses 128 inputs on each Flink line, discards a successful response after partial acknowledgement, restores from the completed checkpoint, then replays at parallelism 1 and 3.
The existing local lifecycle suite covers interruption before checkpoint completion and successful/failed stop-with-savepoint semantics.
The service lease does not repeat every locally settled lifecycle permutation.

Run JVMs serially on the local host with the flags above.
No GCE, GCS checkpoint bucket, BigQuery export, IAM policy change or custom monitoring metric is needed.
Aggregate limits are 250,000 attempted writes, 1 GiB declared write bytes, 1 GiB readback/monitoring bytes and 2 GiB observed local checkpoint/inventory storage.
Bulk wire interception counts each transmitted entry again on an SDK retry, without changing the SDK retry policy.
The conditional production client already disables transparent retries.
Budgets are reserved conservatively in the locked manifest before each worker, survive JVM replacement, and are not refunded.
An instance disappearing during execution fails the worker; it is never recreated automatically.

The approved total spending ceiling is $20.
This lease targets less than $2: at the official us-central1 Enterprise price of $0.65 per node-hour, reserve two node-hours ($1.30) for hourly-boundary/minimum-hour billing, plus a storage and egress margin.
Even at the Enterprise Plus rate of $0.85 per node-hour, that compute reservation is $1.70; no in-memory capacity multiplier is requested.
The official SSD rate is $0.000232877/GiB-hour; readback to Japan is bounded to 1 GiB with a $0.12/GiB inter-region destination rate.
See [Bigtable pricing](https://cloud.google.com/bigtable/pricing); actual billing can arrive later and is not established by a deleted-instance check.

Before creation, run `plan <manifest>`, inspect the exact manifest, and start `supervise <manifest>` in a separate local JVM.
Then run `create <manifest>`, `preflight <manifest>`, and the named `service <manifest> <table>` workers.
A driver must stop on worker failure and signal the manifest's sibling `stop` file, then collect supervisor output.
Creation collisions leave the lease planned and do not reuse/delete the existing instance.
No new workload begins after 45 minutes from the creation attempt; supervision begins cleanup at 50 minutes, reserving ten minutes within the 60-minute lease.
No automatic repetition, capacity change or lease extension is authorized.

The supervisor checks the worker PID and start instant before termination, avoiding a recycled PID.
Cleanup checks the exact owner, resolves creation, deletes the instance, then verifies both direct lookup and instance-list absence.
If the initial creation response was not journaled, it reconciles a matching-owner READY instance or a listed CreateInstance operation with matching project, instance and owner token.
The service defines [READY as successfully created](https://docs.cloud.google.com/bigtable/docs/reference/admin/rest/v2/projects.instances#State); unknown or still-running creation remains a cleanup failure.
It removes only the manifest's owned `work` child and verifies that child is absent.
A prior external deletion is a valid absent result only after creation is resolved.
Permission errors, pending creation and ownership mismatch are failures, never absence.
Run `cleanup <manifest>` to resume after an interruption; preserve the manifest until absence is verified.
Local supervision requires the host to remain running; the manifest is a recovery record, not a server-side TTL.
Publication forces both the file and its parent directory before resource creation or further work proceeds; this does not guarantee recovery from disk loss.

Run `monitor <manifest>` once after deletion and metric publication delay.
An existing output is refused before reserving another read budget, and an unstarted lease reports that creation never started.
A failed attempt can leave empty or partial output; retain it with the failure log and do not retry within the same lease.
The read-only capture filters the exact instance for cluster/hottest-node CPU, server latency, request counts and table/disk storage.
Service metrics may be delayed or absent, and a short run may never establish settled physical storage or a long-term hot-row growth rate.
Record those as unmeasured instead of delaying deletion or equating logical bytes with physical storage.

## Observations

Local calibration on 2026-09-07 at source `e4ce21b042f9f0e0d5385ac66d81b1ef25122257`, JDK 17, Flink 2.2.1, 2 GiB heap and four reported processors, ran four fresh JVMs.
With 1 KiB, even keys, parallelism 1, in-flight 4, 100 ms checkpoints, 10 seconds warm-up and 30 seconds admission:

| Arm | Receiver delay | Status | Measured inputs | Inputs/s including drain | Visibility p95 |
| --- | --- | --- | --- | --- | --- |
| Bulk | 0 ms | Censored at 200,000 total inputs | 130,176 | 7,091.472 | 0.254 ms |
| Bulk | 20 ms | Observation | 4,123 | 137.410 | 48.463 ms |
| Staged | 0 ms | Censored at 200,000 total inputs | 142,742 | 6,332.670 | 1,166.060 ms |
| Staged | 20 ms | Observation | 5,701 | 158.241 | 19,368.801 ms |

Both slower controls were detected.
The staged slower control drained for 6.027 seconds; its denominator includes that drain.
These rates include local instrumentation cost and are not estimates of service capacity.
An earlier calibration failed while sampling a checkpoint file concurrently deleted by Flink; the directory sampler now ignores only that disappearance, and the failed run's owned directory was removed.
The later review repairs address failed initialization, restored metadata checks, creation reconciliation and the local SUM oracle; they do not change the calibrated normal fake-receiver timing path.

The bounded service lease ran at source `4a66790e30a29740f2b64ea5588b7412aafa6c38` with the same JDK/heap/processor flags, Flink 2.2.1 for timing, and the separately compiled Flink 1.20.4 runtime for its correctness arm.
Instance `flink-s2-1788745005-4b0b46` was attempted at 02:16:58.455 UTC on 2026-09-07 and verified absent at 02:26:49.421 UTC, less than ten minutes later.
The manifest reserved 57,216 write attempts and 117,178,368 write bytes across the workers that started; these conservative reservations are not observed service usage.
No second instance, automatic repetition, cap increase or lease extension followed the failure below.
The less-than-$2 estimate remains an estimate; actual billing was not retrieved.

The real metadata preflight accepted the valid fixture and rejected both invalid profiles and all three invalid marker-family fixtures.
Each Flink line then passed response-loss recovery followed by completed-checkpoint replay at parallelism 1 and 3.
At all three readbacks per line, 128 distinct contributions matched the per-row SUM values and 128 retained markers, including 115 markers on the hot row.
The final duplicate detections were 268 on Flink 2.2.1 and 266 on Flink 1.20.4; these count replay observations, not extra SUM applications.
The local negative control without marker protection detects a doubled contribution.
This service run uses the experimental lifecycle harness, not a delivered production checkpointed-write factory or Table API.

The serialized control completed and read back 256 inputs and markers, but exhausted its input cap during warm-up.
It therefore produced no valid measured throughput or latency control.
The first comparison pair completed and passed generated-data/marker readback:

| Arm | Measured inputs | Inputs/s including drain | Visibility p50 / p95 / p99 | Measured drain | Client p95 upper bound |
| --- | --- | --- | --- | --- | --- |
| Bulk r1 | 524 | 17.298 | 180.132 / 364.409 / 381.690 ms | 0.292 s | 192.938 ms |
| Staged r1 | 518 | 13.285 | 15.114 / 23.348 / 24.227 s | 8.990 s | 178.258 ms |

Readback included 525 total bulk inputs and 780 total staged inputs; only the measured subsets enter the table above.
The staged readback found 780 markers, one per row, occupying 58,390 logical bytes.
The source safety cap did not censor this pair, but a single completed repetition cannot establish repeatability or service saturation on this local host.
Bulk admitted only one warm-up input; the configured ten-second warm-up does not establish that its client had reached steady state.
The observed staged/bulk throughput ratio is about 76.8%, while its visibility p95 is about 64.1 times bulk; the smaller client p95 does not remove checkpoint waiting.
These observations provide no performance-gate pass or production workload acceptance.

For bulk r1, completed-checkpoint duration ranged from 200 to 10,784 ms and checkpointed size was 265 bytes.
For staged r1, those ranges were 42 to 14,038 ms and 536 to 337,932 bytes.
The final REST counts were respectively 38/4/1 and 6/29/1 completed/failed/in-progress checkpoints.
The logs include declines while tasks were closing; the raw final in-progress count is not an assertion that an acknowledged input remained unwritten.
Tracked staged writer peaks were 262 entries and 399,180 bytes; both arms tracked a peak of four active requests.
Heap after collection request was 43,237,808 bytes for bulk and 44,017,880 for staged; sampled peak heap was 77,638,144 and 76,824,128 bytes.
Accumulated GC time was 28 and 54 ms.
Staged serialization allocation was 13,379,112 bytes and restore allocation was zero in this non-recovery repetition.

Staged r2 failed after a checkpoint exceeded the configured 60-second timeout; Flink reported its checkpoint-failure threshold and disabled restart policy.
It emitted `STAGE2_TERMINAL FAILED`, with no valid throughput, visibility percentile or final readback.
The supervisor terminated that worker during failure cleanup; its driver exit status was 143.
The remaining bulk r2/r3, staged r3 and hot-row phase never ran.
No three-repetition variability verdict, sustained hot-row growth rate or formal matrix result can be calculated.
A future service plan must address the censored control and observed checkpoint limit explicitly; this record does not raise them retroactively.
The [local diagnostic follow-up](0163-bigtable-stage2-checkpoint-stalls.md) reproduces checkpoint expiration with delayed fake replies; it does not identify this historical repetition's exact cause or repeat the service run.

The supervisor checked ownership, deleted the exact instance, verified direct and list absence, and removed its owned checkpoint/inventory directory.
An independent `gcloud` describe returned `NOT_FOUND`, and an unfiltered instance list also contained no matching instance.
The manifest remains `ABSENT`; bounded logs and successful-run sample files remain outside the removed work directory in ignored local evidence storage.
At the measured revision the failure path did not preserve staged-r2 samples before cleanup; that trace is unavailable.
The later local repair preserves failed-run samples for future executions; the service run was not repeated.
Read-only Monitoring capture queried through 02:29:31.868 UTC and returned 167,441 response bytes across all six requested metric types.
The nine cluster CPU samples ranged from 0.0100 to 0.0171; the nine hottest-node samples ranged from 0.00999 to 0.01607.
Published server-latency distributions contained 163 bulk-r1 `MutateRows` requests with a count-weighted mean of 3.656 ms and 780 staged-r1 `CheckAndMutateRow` requests with a mean of 2.702 ms.
These are whole-table published request distributions, including warm-up, not the admission-window client percentile or a count of applied inputs.
The metric's unit and sampling delay follow the [Bigtable metrics reference](https://docs.cloud.google.com/bigtable/docs/metrics).
Request-count series were also captured, but publication delay means this snapshot is not an authoritative final operation ledger.
All returned table/disk storage samples were zero despite successful data readback; this short lease did not establish settled physical storage, so physical marker cost remains unmeasured.
No sustained hot-row growth measurement was obtained.

Default scoped verification passed 1,438 Bigtable tests; after isolating the wire test's telemetry and restoring the deliberate mutations, all 34 related invocations passed.
The clean scoped Flink 1.20.4 full verification also passed; its five existing emulator-deviation skips remain explicit.
All seventeen valid deliberate mutations were killed by named test failures, including the actual SDK wire-budget installation, failed-sample preservation, read-budget collision, reused-worker identity and unavailable-allocation sentinel.
Two initially non-compiling mutation variants were excluded and replaced with executable mutations; compilation failure was not counted as detection.
The wire-budget integration test builds the pinned SDK client against a local gRPC server with credentials and telemetry disabled.
The worker-stop seam tests graceful termination, forced escalation and a reused PID without killing any real unrelated process.
A local staging overflow verifies failed-sample preservation before owned-directory removal.
These later repairs were verified locally and did not repeat or retroactively change the service observations.
Formatting and Markdown lint passed; CI and the review records identify the final delivery revision.
