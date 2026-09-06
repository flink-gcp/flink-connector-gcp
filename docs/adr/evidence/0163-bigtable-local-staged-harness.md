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

# Bigtable staged local harness

This is local preparation for [ADR-0163](../0163-bigtable-checkpointed-writes-stage-immutable-mutations-and-retain-row-markers.md), not a Stage 2 service verdict.
The test application uses an embedded Flink MiniCluster and either a handwritten receiver or the Bigtable emulator.
It creates no GCP instances and does not load the real-GCP fixture or its stale-instance sweeper.
The standalone probe has no real-service execution mode.

## What the tests establish

On 2026-09-07, the repaired suite completed on both Flink 2.2.1 and 1.20.4: six unit tests, eight MiniCluster invocations (including both rescale targets), and two emulator tests per line.
Each line ran all 16 invocations with zero failures, errors or skips.

`BigtableLocalStagedJobITCase` exercises the experimental writer and committer through a real coordinator, including its checkpoint store and job restart.
The assertions cover these paths:

- Inputs stay staged until a notifying checkpoint or synchronous stop-with-savepoint completes.
  An ordinary savepoint persists the pending work but does not authorize a commit by itself.
- A response lost after a partial commit causes checkpoint recovery to submit the same requests again.
  The fake SUM oracle counts one contribution per distinct persisted envelope.
- Successful stop-with-savepoint followed by 2-to-1 or 2-to-3 rescaling replays the saved envelopes without calling the writer again.
- Cancellation before any completed snapshot leaves no applied effects; a fresh source replay can then stage the inputs.
- A writer-close failure after a stop savepoint has been created leaves its effects visible.
  Explicitly resuming that savepoint preserves the contributions; explicitly selecting the preceding retained checkpoint re-stages the inputs with fresh identities and doubles them.
  This test does not assert automatic fallback to the earlier checkpoint.
- Staging overflow fails the job instead of waiting on a writer thread that must process the next barrier.

`LocalStagedHarnessTest` checks graph-time execution/checkpoint requirements, destination/profile preservation, cancellation of every original pending future, and a SUM negative control that detects duplicate contributions when the marker predicate no longer protects them.
Its endpoint checks require a literal loopback emulator address.
`BigtableLocalStagedEmulatorITCase` uses the production single-row client factory with explicit emulator settings, compares each reconstructed SDK request with its saved protobuf, and checks marker identities and data readback after stop and restore.
The bulk arm uses the existing `BigtableMutateRowsSink` writer through its existing injectable batcher factory seam.
Both arms share payload generation, row distribution and fixed cell timestamps.

The fake models only the exercised SetCell/AddToCell shapes and assumes row atomicity.
It does not implement Bigtable's complete timestamp, filtering, aggregate or concurrency semantics.
The emulator checks transport and retained SetCell markers, not real-service SUM or IAM behavior.

## Reproduce locally

Compile and run the focused suite from the repository root:

```bash
mise x java -- ./mvnw -ntp -pl flink-connector-gcp-bigtable -am \
  -Dtest=LocalStagedHarnessTest,BigtableLocalStagedJobITCase,BigtableLocalStagedEmulatorITCase \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Docker is required for the emulator class; the other two classes do not open a Bigtable client.
No `.env` link, GCP project or credentials are required.
Clean before selecting the other supported Flink line with `-Dflink.version=1.20.4 -Dflink.compat=flink1`.

`BigtableLocalStagedProbe` is a standalone main class in the Bigtable test sources.
After test compilation, launch a fresh JDK 17 JVM using the `surefire.test.class.path` property in that module's Surefire XML report as the classpath.
Use `-Xmx768m`, `--add-opens=java.base/java.lang=ALL-UNNAMED`, and `--add-opens=java.base/java.util=ALL-UNNAMED`.
Its positional arguments are arm (`staged` or `bulk`), input count, payload bytes, parallelism, per-subtask in-flight cap, checkpoint interval in milliseconds, fake reply delay in milliseconds, and keys (`even` or `hot`).
For example, the arguments `staged 128 65536 4 4 1000 0 even` exercise a fixed burst of 128 inputs.
Both arms reject a combined input payload (`count * payloadBytes`) above 32 MiB before starting a MiniCluster or creating its checkpoint directory.
This conservative calibration limit accounts for retained request tracing; it is not a production heap bound.
A boundary check on 2026-09-07 completed 512 inputs of 64 KiB per arm in the documented 768 MiB JVM and removed both checkpoint directories; 513 inputs were rejected before directory creation in both arms.
That check used parallelism one, in-flight cap four, a 1000 ms checkpoint interval, no fake reply delay and even keys.
The hot distribution sends nine of every ten sequence numbers to one row; an incomplete group of ten changes the exact percentage.

Each `LOCAL` CSV row contains the arm, count, payload, parallelism, in-flight cap, checkpoint interval, fake delay, key distribution, acknowledged inputs per second, admission-to-acknowledgement p50/p95/p99 in nanoseconds, peak entries and charged bytes in one writer, peak tracked requests across the job, heap after collection in bytes, GC milliseconds during the job, and job setup-to-completion duration in nanoseconds.
The duration and GC interval stop immediately after job completion is observed, before REST statistics, sorting or output.
The staged arm releases a tracked request when the committer consumes its result; the bulk arm releases it in the completion callback.
Throughput divides the distinct acknowledged input count by the time from the first writer admission to the last successful response observation, including drain.
`CLIENT_COMPLETION` reports the successful client-future count and p50/p95/p99 nanoseconds separately from checkpoint waiting; the bulk value includes batch waiting and is not pure wire-RPC latency.
`CHECKPOINT_STATS` carries the local coordinator's checkpoint statistics as JSON.
`LOCAL_DIRECTORY` records the owned temporary path before the job starts.
`LOCAL_CLEANUP absent` is emitted only after the owned MiniCluster, receiver and temporary checkpoint tree have been closed and the directory is absent.

These are fixed-input, fresh-JVM observations without Stage 2 warm-up or repetitions.
The source runs without a rate limiter, but its finite input and the in-process fake cannot measure service capacity.
The bulk fake acknowledges entries without applying them; the staged fake also copies row maps and evaluates its marker model.
Their local ratio therefore cannot replace the real-service comparison.
The heap observation includes the MiniCluster, input inventory, retained fake rows and request tracing, and is not a production TaskManager sizing bound.

## Calibration observation (2026-09-07)

Twenty fresh JDK 17 JVMs completed on macOS with Flink 2.2.1 and the repository-pinned Bigtable SDK 2.82.0.
Each acknowledged all 128 inputs and reported `LOCAL_CLEANUP absent`.
All used evenly distributed keys; these single observations establish instrument operation, not repeatability or the Stage 2 variability gate.
The 16 matrix cells use an in-flight cap of four per subtask and no artificial reply delay.

| Arm | Payload (KiB) | Parallelism | Checkpoint interval (ms) | Inputs/s | Admission-to-ack p95 (ms) | Client-completion p95 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| bulk | 1 | 1 | 100 | 2787.532 | 0.072 | 0.026 |
| staged | 1 | 1 | 100 | 225.211 | 464.523 | 0.152 |
| bulk | 1 | 1 | 1000 | 3701.834 | 0.100 | 0.057 |
| staged | 1 | 1 | 1000 | 1127.615 | 71.168 | 0.065 |
| bulk | 1 | 4 | 100 | 3031.864 | 0.416 | 0.275 |
| staged | 1 | 4 | 100 | 516.767 | 209.308 | 5.233 |
| bulk | 1 | 4 | 1000 | 3213.873 | 0.270 | 0.074 |
| staged | 1 | 4 | 1000 | 991.501 | 101.602 | 0.262 |
| bulk | 64 | 1 | 100 | 1591.798 | 0.490 | 0.057 |
| staged | 64 | 1 | 100 | 498.785 | 180.911 | 0.079 |
| bulk | 64 | 1 | 1000 | 748.364 | 1.259 | 0.065 |
| staged | 64 | 1 | 1000 | 534.594 | 180.893 | 0.111 |
| bulk | 64 | 4 | 100 | 1581.435 | 4.930 | 0.236 |
| staged | 64 | 4 | 100 | 657.271 | 157.187 | 0.227 |
| bulk | 64 | 4 | 1000 | 1038.544 | 10.155 | 5.082 |
| staged | 64 | 4 | 1000 | 679.413 | 154.074 | 0.361 |

The slower-reply control fixes payload at 1 KiB, parallelism and in-flight cap at one, and checkpoint interval at 100 ms.
Adding a 20 ms scheduler delay reduced throughput and raised both latency observations in each arm:

| Arm | Fake reply delay (ms) | Inputs/s | Admission-to-ack p95 (ms) | Client-completion p95 (ms) |
| --- | ---: | ---: | ---: | ---: |
| bulk | 0 | 2354.039 | 1.575 | 0.052 |
| bulk | 20 | 35.282 | 60.387 | 30.070 |
| staged | 0 | 973.904 | 98.373 | 0.055 |
| staged | 20 | 36.343 | 3321.566 | 30.140 |

The finite source also triggers an end-of-input checkpoint, so the configured interval does not determine every observation's waiting time.
The small burst, JVM start-up and scheduler timing prevent attributing the matrix differences to checkpoint interval or parallelism alone.
No real-service request or GCP resource was needed for these observations.

## Remaining service evidence

Real profile/IAM validation, marker-family metadata enforcement against the service, SUM under real conditional writes, service CPU and latency, physical marker storage and sustained hot-row growth remain unmeasured by this harness.
The [preregistered Stage 2 protocol](0163-bigtable-staged-performance-protocol.md) retains its thresholds and full workload matrix.
A local success does not authorize a general or constrained production mode.
A future service run should name the unresolved question and first use a local client where that suffices; compute inside GCP is needed only if the execution host or network prevents the required measurement.

Future service fixtures must own their exact resource names and cleanup, and account for the repository's periodic real-GCP instance deletion.
The local harness never invokes that deletion or reuses an existing real instance.
JUnit owns test checkpoint directories, the standalone probe removes its own temporary directory on success and failure, and Testcontainers owns the emulator container.
A hard process or host termination can prevent Java cleanup; use the recorded local directory or container identity to inspect that run's leftovers rather than sweeping unrelated resources.
