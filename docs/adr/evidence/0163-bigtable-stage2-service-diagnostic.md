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

# Retired seven-cell diagnostic and retained notification instrumentation

The owner retired the optional seven-cell service diagnostic on 2026-09-11 while retaining the unrestricted formal evaluation for [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211).
The merged admission-credit implementation and its dedicated tests are removed.
The service profile, freeze entry point and their dedicated tests are withdrawn from the unmerged change; those additions never reached main, and no service execution occurred for that profile.
The earlier local observations remain in the [admission evidence](0163-bigtable-stage2-admission-diagnostics.md).
The original reviewed implementation is preserved at [commit 4a123d6](https://github.com/flink-gcp/flink-connector-gcp/tree/4a123d60414d8f59bfe456a387fbac476c49ad41).

Complete-notification instrumentation below remains active for the unrestricted timed harness.
It distinguishes an entire synchronous completion notification from one commit invocation, including multiple older collections and exceptional exits.
The shared input-phase and receiver-shutdown tests remain with `Stage2HarnessTest`; the MiniCluster wiring check uses an empty checkpoint across two committer subtasks, without admission credits.
The held six-request operator test checks nonempty notification work across three commit invocations.

The [formal Stage 2 protocol](0163-bigtable-staged-performance-protocol.md), its unrestricted source, 108 cells and acceptance thresholds remain unchanged.
Ownership, reservations, supervision, stop conditions and exact-owned deletion/absence checks remain in the shared harness.
The archived manifest discriminator is exactly `profile=diagnostic-seven-cell-v1`, as written by [the retired profile implementation](https://github.com/flink-gcp/flink-connector-gcp/blob/4a123d60414d8f59bfe456a387fbac476c49ad41/flink-connector-gcp-bigtable/src/test/java/io/github/flink/gcp/connector/bigtable/sink/Stage2Diagnostic.java#L26).
A manifest with that discriminator is rejected by creation and worker claim; its cleanup journal remains readable.
The guard does not disable shared preflight, monitoring or supervision helpers; preflight can still make read-only metadata RPCs.
Neither these tests nor the historical diagnostics pass the formal gate or close #1211.

The remainder preserves the withdrawn preparation record.
Only the complete-notification instrumentation section describes a retained mechanism; the seven-cell commands and verification below are historical and must not be executed against the current tree.

## Manifest and reservations

`BigtableStage2Probe plan-diagnostic <manifest>` creates `diagnostic-seven-cell-v1` in a new parent directory.
It refuses an existing directory so a new lease cannot inherit an earlier stop file, reservation or evidence.
The manifest records the exact project, zone, generated instance and owner token, seven ordered tables, profile/families, workload, credits, reservations and deadlines.
The existing `plan` command and historical cleanup remain available for the earlier unrestricted profile.
A historical manifest is never upgraded into diagnostic authorization.

| Order | Table | Arm | RPC concurrency | Credits per reader |
| --- | --- | --- | --- | --- |
| 1 | `serialized` | Staged | 1 | 32 |
| 2 | `bulk-r1` | Bulk | 4 | 128 |
| 3 | `staged-r1` | Staged | 4 | 128 |
| 4 | `staged-r2` | Staged | 4 | 128 |
| 5 | `bulk-r2` | Bulk | 4 | 128 |
| 6 | `bulk-r3` | Bulk | 4 | 128 |
| 7 | `staged-r3` | Staged | 4 | 128 |

Each cell uses 1 KiB even-key inputs, parallelism one, one-second checkpoints, ten-second warm-up, thirty-second measured admission and an 8,000-input inventory.
The worker uses local JDK 17, Flink 2.2.1, `-Xmx2g`, `-XX:ActiveProcessorCount=4` and the two module opens in the [harness instructions](0163-bigtable-stage2-experiment-harness.md#local-execution).
Each table is used once by a fresh worker JVM; only the next cell after an observed predecessor may be claimed.
No build or second worker runs during an observation.

A claim durably reserves 32,000 attempted writes, 65,536,000 declared write bytes and 16,384,000 readback bytes in the same locked update that records its worker identity.
Seven claims therefore reserve 224,000 attempts, 458,752,000 write bytes and 114,688,000 readback bytes.
Neither unused inventory nor failed workers refund a reservation.
The aggregate ceilings remain 250,000 attempts, 1 GiB write bytes and 1 GiB combined readback/monitoring bytes; Monitoring reserves its existing separate 8 MiB allowance.
Conditional requests and transmitted bulk batches must fit the declared 2,048 bytes per attempted input, including their request envelope.
Bulk SDK retries consume the same worker reservation again at the transmitted-request boundary.
The conditional client retains its no-transparent-retry policy.

## Complete notification instrumentation

The timed staged MiniCluster decorates the actual Flink committer operator's `notifyCheckpointComplete` call.
The delegate still owns collection iteration, retries, state, lifecycle, metrics and processing-time services; the instrument does not copy that implementation.
The decorator exposes the delegate's operator interfaces and records entry, successful return and exceptional exit around the complete synchronous call.
Reflective dispatch affects every call on that staged committer, including element and lifecycle calls; bulk has no corresponding committer decorator.
This additional staged-only instrumentation cost remains part of the observed arm difference and must not be attributed entirely to the service.
Finite lifecycle/recovery jobs keep their existing topology.
Notification tests cover a held six-request notification containing three commit invocations, exceptional delegate exit, and empty notifications from both committer subtasks in a MiniCluster on Flink 1.20.4 and 2.2.1.

`notificationProgress` is separate from the existing per-invocation `commitProgress`.
Its active entries contain checkpoint ID, elapsed nanoseconds, invocation count and initial entry counts accumulated across those invocations.
Completed notifications retain only started/finished/failed totals and maximum duration, invocation count and entry count.
An invocation occurring outside a completion notification, such as restored work during initialization, increments a separate counter.
Retries can make invocation entry totals count an envelope more than once; these totals are work observations, not distinct input counts or a Flink heap bound.
The duration includes work between invocations and exception cleanup up to the callback's exit, but excludes mailbox waiting before callback entry and later checkpoint snapshot work.
Input, notification, invocation and REST samples remain separately synchronized observations.

## Prerequisites and execution

Complete local tests and all three review rounds before freezing a service runtime.
Archive the clean reviewed source SHA, compiled runtime and classpath outside directories that subsequent builds can overwrite.
All standalone commands run from that clean source worktree with the exact JVM flags above.

`freeze-diagnostic <manifest> <prerequisites.properties>` checks the clean source against `reviewedSourceSha`, hashes the actual runtime classpath and records exact commands for creation, preflight, supervision, each cell, cleanup and monitoring.
It is single-use while the manifest is `PLANNED`.
Large classpath and command strings live in a separate `frozen-runtime.properties` file whose hash is pinned in the manifest, avoiding their repeated parsing at each RPC's lease check.
Creation and worker startup recheck the source, classpath and pinned prerequisite files; a mismatch refuses execution.
Cleanup remains possible without those execution prerequisites, including when artifacts have changed after a failure.

The prerequisite properties contain:

- `reviewedSourceSha`: the full reviewed commit.
- `budgetEvidence`, `priceEvidence`, `reviewEvidence`: absolute paths to nonempty local evidence files; their contents and the prerequisite file are hashed.
- `checkedAt`: UTC instant of the billing/reservation and current-price reconciliation, at most 24 hours old when executing.
- `priorBilledUsd`, `priorUnbilledUpperUsd`, `outstandingReservationsUsd`: nonnegative amounts covering the existing aggregate authorization.
- `nodeHourUsd`, `egressGiBUsd`, `storageAndOtherUpperUsd`: current conservative rates/allowance for two node-hours, one GiB readback and the remaining metered cost.

The guard requires `priorBilledUsd + priorUnbilledUpperUsd + outstandingReservationsUsd + 2 <= 20` and `2 * nodeHourUsd + egressGiBUsd + storageAndOtherUpperUsd <= 2`.
These checks validate arithmetic and preserve reviewed evidence; they do not query billing or prove a supplied amount correct.
The operator must reconcile billed and conservatively bounded unbilled usage, unresolved reservations and the requested edition against those files before using the freeze command.
Do not insert zero for an unavailable amount or treat the earlier less-than-$2 estimate as settled billing.
If the evidence is unavailable, finish the local change and leave the service lease uncreated.

The provisioned resources remain one newly owned SSD node in `flink-gcp`, `us-central1-b`, cluster `stage2-c1`, transactional single-cluster profile `single-cluster` and the seven tables above, each with `cf` and raw no-GC `flink_commit`.
There are no negative-profile, invalid-family or SUM recovery fixtures in this lease.
Explicit preflight validates every actual table/profile before workload admission, and client factories retain their per-target preflight.

Start `supervise <manifest>` in a separate JVM and verify its recorded PID/start instant before `create <manifest>`.
Then run `preflight <manifest>` and the seven `service <manifest> <table>` commands serially, waiting for each process exit.
An external driver must retain stdout/stderr, enforce 240 seconds per worker including startup, inspect exit and terminal result, and signal the sibling `stop` file on any failure, censoring, empty or incomplete observation.
The supervisor additionally enforces the claimed worker's process-start deadline and detects its disappearance using PID/start identity.
Keep the execution host running: this remains a local supervisor, not a server-side TTL.
If the supervisor process disappears, stop admission and invoke the frozen `cleanup` command under the external driver's remaining lease deadline.
Do not restart supervision or create a replacement instance.
If the driver also disappears, the operator must run that same exact-manifest cleanup command; a stop file alone does not delete resources without a live process polling it.

The checkpoint timeout remains 60 seconds, the drain allowance 120 seconds, the JSONL cap 8 MiB and the observed local checkpoint/inventory stop threshold 2 GiB.
No new cell begins after minute 45; cleanup starts by minute 50 and reserves the last ten minutes of the 60-minute lease.
Resource disappearance, ownership mismatch and missing required evidence stop the lease without recreation, repetition, capacity increase or extension.

## Evidence and cleanup

Diagnostic JSONL is written directly beside the manifest, outside the owned `work` directory, using create-new semantics and flushed samples.
This keeps already written samples through a worker kill even when its Java failure handler cannot run.
The live `<table>-samples.jsonl` path holds raw evidence for every outcome, including failure or a hard kill; its filename does not certify success.
The caught-failure path also preserves a failed-samples copy when available; the process log and manifest establish its terminal state.
Keep the full worker configuration, phase counts, admission offsets, invocation/notification progress, exit codes and all failed or censored observations.

An `OBSERVED` cell has completed its runner, readback and worker teardown, but its printed row remains provisional until cleanup succeeds.
After the last cell, or any stop, resolve the creation attempt, check the exact instance's owner, delete only that instance and verify both direct-lookup and list absence.
Remove only the manifest's owned local work child and verify absence, preserving the manifest, logs and samples.
For an externally deleted instance, unresolved creation and permission errors remain cleanup failures.
Resume interrupted cleanup against the same manifest without allocating a replacement instance.
Capture the existing bounded monitoring snapshot once after deletion and publication delay, preserving partial output and recording missing metrics without extending the lease.

Compare all three pairs only after successful absence verification, with failed, censored and unmeasured cells shown beside successes.
The serialized cell is a Flink visibility observation, not a raw RPC benchmark.
The formal gate, hot-row growth, settled physical storage and production-path acceptance remain outstanding.

## Local verification and remaining service boundary

The local continuation exercised the fixed reservations, failed predecessor and repeat refusal, independent-supervisor requirement, changed runtime/evidence pins, stale or insufficient budget arithmetic, request-envelope limits, empty/censored rejection and sample retention through work-directory removal.
Existing fake-admin tests retain the exact-owner, unresolved-creation, permission-error and direct/list absence oracles.
An isolated local JVM exercises the complete freeze with synthetic budget evidence, checks that twelve command pins are generated, and rejects changed evidence, invalid worker flags, repeated freezing and dirty source with more than a pipe buffer of status output.
That fixture never obtains ADC or creates a service resource.
The supervisor rechecks a suspected lost worker under the lease publication lock before signaling stop, so a just-published successful exit does not stop the next cell.
The completed 40-run local matrix was not repeated.

The new manifest's local read contribution was measured separately with the compiled `a6c640d387f7b0e0044093baf3e0c33b61662717` instrument, before the supervisor-only repair.
Six fresh JDK 17 JVMs used the stated heap/processor flags, 5,000 warm-up observations and 20,000 timed observations, in one/two, two/one, one/two order.
The 3,310-byte synthetic diagnostic manifest included the fixed seven-cell configuration, runtime hash and seven worker/evidence receipts; no client was created and no GCP request was issued.
Each synthetic directory was deleted and its absence checked after its JVM.

| Reads per observation | Mean, microseconds (r1/r2/r3) | p95, microseconds (r1/r2/r3) |
| --- | --- | --- |
| One | 124.844 / 123.585 / 118.745 | 163.209 / 164.792 / 161.958 |
| Two | 253.730 / 239.421 / 311.113 | 352.834 / 299.208 / 601.250 |

These observations include fixed-configuration validation as well as warm local file parsing; they do not measure concurrent publication, cold I/O or end-to-end RPC cost.
The existing two-read staged path and one-read bulk accounting remain asymmetric.
Do not subtract this component from visibility samples or attribute an entire arm difference to the service.

Service execution has not occurred.
Read-only inventory found no remaining instance with the earlier Stage 2 prefix, but that is not financial reconciliation.
The available evidence does not establish prior billed charges, a conservative unbilled upper bound or unresolved financial reservations under the aggregate USD 20 authorization.
Consequently the USD 2 reservation remains unproven, the new manifest stays `PLANNED` and unfrozen, and no diagnostic GCP resource has been created.
A later execution must first supply that evidence, finish all reviews, archive the exact runtime and review the external driver's commands and stop/cleanup behavior before freezing it.
