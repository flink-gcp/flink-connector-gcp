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

# Production Bigtable Stage 2 preparation

This record tracks the production instrument and execution preparation for [#1327](https://github.com/flink-gcp/flink-connector-gcp/issues/1327).
The [formal protocol](0163-bigtable-staged-performance-protocol.md) retains all 108 cells, three repetitions per arm, observation periods, thresholds and variability rules.
This preparation records no service performance verdict and does not close the assessment.
Production recovery acceptance through both API entry points and both Flink lines was recorded under [#1319](https://github.com/flink-gcp/flink-connector-gcp/issues/1319) on 2026-09-14.

## Production instrumentation

The staged jobs constructed from `Stage2Harness` now use `Stage2ProductionSink`, which delegates writer creation, committer creation, checkpoint serialization and pre-commit topology validation to the production `BigtableStagedSink` selected by the builder.
The internal `@VisibleForTesting createCommitter(context, tableAdmin, factories)` overload shares construction with the ordinary runtime entry point.
The instrument observes the actual writer gauges and forwards their registration to Flink.
It records original marker identities during `prepareCommit` and measures each successful conditional response before publishing the observed future as complete.
Cancellation reaches the original client future; observation failures fail the observed operation.
The bulk arm uses the production bulk writer and the same completion-publication boundary.
An inventory acknowledgement means the first successfully observed write response for an input, including an already-present marker; it does not mean that the complete Flink commit invocation or checkpoint notification has returned.
The historical committer also used the response timestamp for latency, but updated the inventory only when it consumed that response in submission order.
The production observer can therefore retain acknowledgements for later successful responses even when another request fails the invocation.
Formal timed runs disable automatic restart, and a failed invocation cannot become a successful assessment row.

Flink still owns checkpoint collection, notification and retry behavior.
The generic notification decorator observes the complete `notifyCheckpointComplete` call, separately from each production committer invocation.
Serialization and deserialization allocation counters wrap the production serializer; unavailable allocation measurements remain unavailable.
The existing per-input disk inventory continues to distinguish admission, acknowledgement and retained markers, including the SUM recovery oracle.

Service transports read metadata through `BigtableStagedTableAdmin` and use the default SDK single-row factory.
The lease guard rejects an expired or foreign target before metadata reads and checks the lease again before data submission.
An exhausted attempt or byte budget throws a synchronous `UncheckedIOException` before the SDK request is issued.
Production committer failure and destination-attempt counters can consequently include a rejected local submission; those counters do not establish billable RPC counts.
Fake and emulator calibration supply local metadata doubles; emulator results establish data transport and readback, not actual-service routing or metadata acceptance.
The duplicated `Stage2Preflight` validator and the experimental recovery command have been removed.
Recovery service runs use `BigtableProductionRecoveryProbe`.
The historical state-sizing and failed-stop probes retain their original test sink and evidence.

## Offline matrix and local calibration

After compiling the Bigtable test sources, use the test runtime classpath and the JVM opens already documented in the [historical harness record](0163-bigtable-stage2-experiment-harness.md).
The following argument forms belong to `io.github.flink.gcp.connector.bigtable.sink.BigtableStage2Probe`:

```text
plan-formal /absolute/path/matrix.csv
local-formal /absolute/path/new-work-directory b1024-even-p1-i1-c1-r1-bulk /absolute/path/limits.properties
```

`plan-formal` writes 648 rows in a predeclared order and refuses to overwrite an existing file.
Repetition order is bulk/staged, staged/bulk, then bulk/staged, with distinct table names for every arm and repetition.
The total warm-up and admission time is 69,120 seconds (19.2 hours), excluding drain, JVM startup, setup and cleanup.
The CSV is an execution inventory, not a resource authorization or a service runner.

`local-formal` selects a table from that inventory and runs a no-service calibration using the corresponding workload and unchanged warm-up/measurement periods.
It accepts exactly these seven capacity/deadline fields:

```properties
inventoryEntries=1000000
inventoryBytes=67108864
stagedEntries=100000
stagedBytes=67108864
workBytes=2147483648
checkpointTimeoutMillis=60000
drainMillis=120000
```

These values reproduce the old capacity defaults; they are not a sizing recommendation for the full matrix.
The inventory requires 64 bytes per entry.
The percentile summary additionally allocates a `long[inventoryEntries]` array, requiring at least eight heap bytes per configured entry before sorting overhead and other live state.
Neither the inventory file budget nor the work-directory budget bounds that heap allocation; calibrate the complete run, including summary generation, with the frozen JVM heap limit.
`stagedEntries` and `stagedBytes` bound each writer interval; they do not bound total heap or pending checkpoint collections.
`workBytes` bounds the entire working directory, including inventory and checkpoint files.
The sample-file cap remains 8 MiB.
Every run prints its capacities, deadlines and JVM flags with `instrument=production-v1`.

Adjust capacities from calibration before freezing a service execution, without changing the source rate or observation periods.
An overflow, exhausted inventory or undrained observation remains failed or censored; raising a limit after observing a service result does not repair that result.
The existing `local` argument form permits short calibration and a delayed no-service receiver, for comparison with a known slower control on the eventual execution host.
No local calibration establishes Bigtable capacity.

## Cost and resource preparation

The owner selected an additional USD 20 ceiling for #1327, separate from the earlier #1319 allowance, with minimum spending and the original assessment conditions preserved.
The preferred service configuration is one Bigtable free trial instance and a regular GCE execution host in `us-central1`.
The full conservative reservation must include setup, calibration, drain, idle storage, monitoring, transfer and cleanup.
If the full assessment cannot fit that ceiling, finish local preparation and leave the service assessment unmeasured.

As checked on 2026-09-14, [Bigtable free trials](https://docs.cloud.google.com/bigtable/docs/free-trial-instance) are available to existing customers and provide one SSD node, up to 500 GB and at most ten tables.
The trial is available once per project; deleting it does not allow another trial.
On 2026-09-14, the owner confirmed that the `flink-gcp` Console offers a free trial in `us-central1`.
That confirms the offered creation path; the created instance and its expiry still require a separate Console record before adoption.
The [documented creation procedure](https://docs.cloud.google.com/bigtable/docs/create-free-trial-instance) uses the console; the ordinary instance-creation API is not evidence of free-trial pricing.
Create the trial only after the instrument, execution package and supervision are ready, and do not upgrade it to a paid instance automatically.

A retained trial needs an owner journal for the complete campaign and separate bounded worker leases.
Retain the one instance across leases, use fresh measurement tables, and collect readback and physical-storage evidence before deleting those tables.
Count the automatically created sample table against the ten-table limit and do not import the optional sample dataset.
At completion, failure or abandonment, terminate workers, preserve evidence, delete exactly owned resources and independently verify absence.
The current one-instance-per-lease `Stage2Lease` cannot represent this retained-instance lifecycle; its create/cleanup commands must not be repurposed for the trial.
Its bounded-experiment provisioner still declares thirteen legacy tables, including the two retired experimental recovery fixtures, and exceeds the trial's ten-table limit.
The execution package must implement and validate that lifecycle before service admission.

For illustration, a regular `e2-standard-4` host costs about USD 2.68 for 20 hours of compute at the [published Iowa rate](https://cloud.google.com/products/compute/pricing/general-purpose), before disk, networking or monitoring.
This is not a full-run estimate or proof that 16 GiB is sufficient.
The existing GKE execution lifecycle is still being prepared, and [Spot Pods can be evicted](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/autopilot-spot-pods); the small example compute saving does not establish a lower cost for a completed assessment.
Calibrate the required host size and reserve the full bounded execution before provisioning it.

## Offline campaign lease planning

`plan-campaign inputs.properties limits.properties new-directory` groups the unchanged matrix into 108 proposed leases, one cell and six fresh-JVM runs per lease, retaining the declared arm order.
It creates five local files: the two input snapshots, `matrix.csv`, `leases.csv` and a final `campaign.properties` manifest with SHA-256 digests of the other four files.
An existing output directory is refused; invalid inputs, an oversized lease or an excessive reservation fail before the directory is created.
An interrupted file write can leave a partial directory, which must not be treated as a completed plan.
The properties manifest includes its generation timestamp, so compare its fields and recorded file digests rather than expecting identical manifest bytes across generations.
The command makes no service calls.

The campaign inputs contain exactly these fields; all numeric fields are positive integers.
A micro-USD is one millionth of a US dollar.
The seven fields of `limits.properties` are the local calibration limits documented above.

| Fields | Meaning |
| --- | --- |
| `project`, `instance` | Exact proposed owner project and retained trial instance. |
| `gceZone`, `gceMachineType`, `jvmFlags` | Co-located `us-central1` compute and complete proposed JVM flags, selected from host calibration. |
| `sourceSha`, `runtimeSha256` | Full lowercase source commit and runtime artifact digest. The planner checks their shape; source review, merge ancestry and artifact contents require separate verification. |
| `leaseLimitSeconds` | Maximum permitted duration of an individual proposed matrix lease. The output gives each lease its own smaller or equal host-time bound. |
| `runOverheadSeconds` | Bound per fresh JVM for startup, final checkpoint, drain, readback and teardown. It must cover at least the configured checkpoint timeout plus drain limit, rounded up to seconds. |
| `leaseOverheadSeconds` | Bound per lease for resource setup, physical-storage metric settlement, evidence collection, exact-table cleanup and worker termination confirmation. |
| `campaignOverheadSeconds` | Additional host-time bound for creation, on-host calibration, the serialized control, sustained hot-row work, pauses while the host remains billable and final cleanup. Their exact commands and separate operation budgets remain part of service admission. |
| `hostMicrousdPerHour` | Conservative hourly price bound for the selected regular GCE host, obtained from current official prices before admission. |
| `otherCostMicrousd` | Positive aggregate reservation for disk, transfer, monitoring and all other costs, including costs while workers are stopped. |
| `costCeilingMicrousd` | Total ceiling, at most `20000000` under the owner's separate allowance. |
| `maxWriteAttemptsPerRun`, `maxWriteBytesPerRun`, `maxReadBytesPerRun` | Proposed per-run service limits, sized from calibration. These values are recorded, not enforced by this offline command. |
| `maxPhysicalStorageBytes` | Proposed physical Bigtable storage limit; it is separate from inventory and checkpoint-file limits and requires service monitoring. |

For each matrix lease, the host-time bound is the sum of its six unchanged warm-up/admission periods, six per-run overheads and one lease overhead.
The planner rounds each lease and the campaign overhead to at least 60 seconds, rounds each host reservation up to a whole micro-USD, then adds the other-cost reservation.
Arithmetic overflow and duplicate campaign fields are rejected.
The limits snapshot retains the existing Java properties interpretation, including the last value winning for a duplicate key; it is interpreted identically by the local calibration command.
The reservation assumes a verified free Bigtable trial; the output says `PREPARATION_ONLY` and `VERIFIED_BIGTABLE_FREE_TRIAL_REQUIRED`, even when its arithmetic fits the ceiling.
Input prices and time bounds must be justified independently; the planner neither queries prices nor establishes sufficient capacity or trial eligibility.
Its host-time accounting requires the future supervisor to enforce the emitted bounds, including billable idle time.

The proposed table lifecycle keeps six new measurement tables for one cell until readback and physical-storage evidence are retained, then deletes and verifies all six before admitting the next cell.
Together with the trial's sample table this requires at most seven tables, conditional on a preflight rejecting any other table inventory.
The trial instance survives matrix-lease cleanup and is deleted only when the entire campaign completes, or when the owner decides so after a stop; the tracked `Stage2CampaignSupervisor.cleanup` path still deletes it on any stop and is not used by the lean controller recorded in [ADR-0166](../0166-bigtable-implementation-precedes-final-stage2-acceptance.md#lean-execution-and-recorded-failures-2026-09-19).
These are the lifecycle requirements recorded by the plan; resource ownership checks, durable admission and outcome journaling, worker dispatch, supervision and cleanup adapters are still required before service execution.

## Remaining service admission conditions

The production instrument must be reviewed and merged before final measurement; the #1319 service work and its resource cleanup finished on 2026-09-14, before this campaign starts.
Freeze source SHA, Linux runtime classpath or image digest, resource identities, exact commands, table/family names, worker leases, operations/storage limits and the cost reservation together.
Prepare independent supervision and loss-of-resource handling, and validate the retained-instance lifecycle before using the one-time trial.
A failed, empty or censored row is recorded and the next preregistered run proceeds; an interruption, lost supervision, ownership loss or an outcome that cannot be recorded stops execution. Nothing is retried, recreated, extended or given more capacity.

The retained result must include the full matrix and sustained hot-row phase, physical Bigtable storage after metrics settle, checkpoint storage, heap/GC, serialization/restore allocation, service metrics and complete notification occupancy.
Logical marker bytes do not establish physical storage or billing.
Reuse matching #1319 and #1316 evidence with its pinned configuration and oracle limits; record every unmeasured or failed workload explicitly.
Record the final verdict and update user-facing support guidance only from the retained acceptance evidence.

## Retained campaign runtime

`Stage2CampaignJournal` adds a durable state machine for the 648 ordered matrix workers.
Activation revalidates the complete offline reservation, records the supervisor PID and start instant and the calling controller JVM's actual PID and start instant, and counts the host deadline from the supplied host creation time, including preparation already spent.
The controller calls activation and waits for it to return before supervisor polling begins; activation rejects the calling JVM as its own supervisor before publishing the one-shot marker.
Only that controller can prepare a cell, publish table readiness or perform cell checkpoint cleanup.
A supervisor heartbeat also rejects a disappeared controller.
The execution package must independently verify that timestamp and the actual host's configuration and automatic deletion deadline; this local journal does not query Compute Engine or enforce its billing.
The journal atomically publishes its initial state after a durable one-shot activation marker, then replaces a forced state file under a publication lock.
An interrupted initial publication cannot admit workers or restart activation; terminal cleanup archives any partial initial snapshot and proceeds through ownership-checked deletion.
A remaining `state.next` after an interrupted publication prevents further admission instead of silently overwriting the interrupted transition.
Terminal cleanup archives that interrupted file before publishing stop, so a failed publication does not prevent resource deletion.
Completed run snapshots (`run-N.properties`) and cell evidence digests (`cell-N.properties`) are retained separately from the fixed-size active state used by measured liveness checks.
The supervisor checks the frozen manifest digest on each heartbeat; reopening workers validate and parse the same input bytes and bind that manifest to the activated state.

Before adoption, the runtime rejects per-run reservations that cannot cover the inventory's four-attempt write allowance and full readback for the largest matrix payload, and a work cap too small for six inventory files.
These are minimum structural checks, not host-capacity calibration or a guarantee that a service observation will fit.
A cell is claimed before its tables are created, and each worker is claimed once in the preregistered order.
Each worker reserves its write attempts, declared write bytes and readback bytes from fixed per-run bounds before submitting data.
Failed reservations do not partially consume one of the paired write budgets, and reopening the journal does not restore spent allowances.
A different target, expired worker or cell, stopped journal, missing supervisor process, or heartbeat at least twenty seconds old refuses further admission.
The journal permits the next cell only after all six observations finish, the previous worker exits, and the controller records a retained-evidence digest after exact-table deletion and absence verification.
The measurement loop checks `workBytes` across the campaign's complete `work` root, including prior workers' files, and checks it again after drain.
After the controller preserves the cell's required evidence and verifies table absence, the journal removes that cell's six work directories before admitting the next cell; outcome snapshots and the evidence digest remain outside that work root.
Checkpoint-tree deletion runs outside the publication lock so heartbeats can continue; completion rechecks the same retained cell and live state before publishing its evidence digest and next-cell admission.
A deletion failure publishes neither completion nor a digest, and a stop during deletion cannot be overwritten by completion.
These controller callbacks are trusted orchestration boundaries: the journal does not establish the physical-metric contents or transferred evidence from a digest alone.

`service-formal campaign-directory table` runs one fresh process through the same production instrument as local formal calibration, using the cell's original periods and the exact recorded JVM flags.
It requires an already activated and supervised journal; an offline plan alone cannot invoke service clients.
The retained-campaign guard and a legacy-lease adapter share the existing measurement path without changing the legacy instance lifecycle.
The worker publishes an observation only after drain, readback and sample preservation succeed and the observation is nonempty and uncensored.
A failed, empty or censored worker is recorded as `FAILED` in its `run-N.properties` and the campaign proceeds to the next preregistered run; the failed run is never repeated, and only an interruption or a failure to record the outcome stops the campaign; a late failure is recordable after the worker deadline until the next heartbeat observes the expired worker, so `runOverheadSeconds` must cover the failure path ([owner decision of 2026-09-19](../0166-bigtable-implementation-precedes-final-stage2-acceptance.md#lean-execution-and-recorded-failures-2026-09-19)).
An observation remains a measurement rather than a throughput/latency acceptance verdict, and a cell with a failed repetition cannot pass.

`Stage2TrialResources` provides the control-plane adapter, with a bounded response body and HTTP request completion deadline.
Credential acquisition and refresh occur outside that HTTP timeout; the execution package must also bound the control-plane process through independent supervision.
Its operator-supplied `trial.properties` has exactly `owner`, `instanceCreateTime`, `freeTrialConfirmedAt` and `freeTrialExpiresAt`.
These values are a Console attestation, not an API-derived proof of free pricing; retain the actual creation record with the execution package.
Adoption requires the recorded instance creation timestamp, a complete cluster inventory containing one ready SSD node in the planned compute zone, only the automatically created `weather-data` table, and no nondefault application profile or existing owner label.
It adds only the owner label and the transactional single-cluster application profile; it has no instance-create, upgrade or resize operation.
Each cell gets its six exact fresh tables with a data family and a raw marker family without GC, keeping seven tables including the sample.
Final deletion checks both the creation timestamp and owner label again, then verifies absence by exact GET and an independent instance list; permission failures and incomplete inventory are not absence.

`Stage2CampaignSupervisor` supplies the independent supervision step and cleanup order.
Polling begins only after activation returns; failed adoption or setup invokes terminal cleanup explicitly.
A premature poll before activation is rejected without deleting the adopted trial.
Once active, an ownership-verification error, expired supervision, or a backwards wall-clock step stops the campaign; even a transient control-plane failure can therefore end the one-shot trial.
The runtime does not retry failed observations or extend deadlines to recover from these conditions.
It publishes stop before terminating the recorded controller and then the recorded worker, preserving reused PIDs and requiring their termination checks to succeed before cloud deletion.
Stopping the controller quiesces its in-progress cell checkpoint cleanup before terminal cleanup removes the parent work root.
A controller that reaches cell cleanup after stop is refused before deletion, and one stopped during deletion cannot publish cell completion.
An activated journal without its controller identity cannot proceed to resource deletion; interrupted initial publication remains eligible for terminal cleanup because no cell could have been admitted.
`ABSENT` is recorded only after resource absence and owned checkpoint-directory removal.
A failed deletion leaves the journal stopped and preserves remaining local work for investigation.
The control-plane adapter and supervision step are exercised with hand-written fakes, including foreign ownership, interrupted adoption, missing workers, incomplete deletion and response-size exhaustion.
Separate child-JVM tests hold an actual checkpoint tree before, during and after cell cleanup, including a controller with a blocked shutdown hook that requires forced termination.
They verify controller exit before cloud cleanup, work-root absence, stop preservation and the absence of premature cell evidence.

This runtime is not yet the complete campaign execution package.
The external controller still needs to bind the frozen Linux runtime and source to the actual host, launch and monitor these processes, capture and validate settled physical-storage evidence before calling cell cleanup, and execute the separately budgeted serialized control and sustained hot-row phase.
The package also needs its concrete host provisioning, automatic termination, retained evidence transfer and final host/disk absence verification.
No trial or Compute Engine resource was created by this implementation, and no service measurement or supported-workload verdict is recorded here.

## Separately reserved auxiliary observations

`auxiliary.properties` adds two optional primitive reservations to the retained journal: `serialized`, then `sustained`, both after matrix cell 107 and its retained evidence.
A complete execution package must require this file and freeze its digest with the campaign; a matrix-only journal remains useful for focused preparation tests and does not establish full protocol completion.
Activation records the auxiliary digest, reopening binds the same bytes, and heartbeats detect a changed or removed snapshot.
The complete matrix CSV, six-run cell order, observation periods, acceptance thresholds and variability rule are unchanged.

Both auxiliary phases use 1 KiB payloads, one subtask, one-second checkpoints and ten seconds of warm-up through the production staged instrument.
The serialized control uses evenly distributed keys and one conditional request in flight; it includes staging and checkpoint waiting and is not a primitive-RPC performance verdict.
The sustained phase uses four conditional requests in flight and the existing 90% hot-row distribution with distinct envelope identities.
The exact observation duration and capacity inputs are preregistered separately; none of the test fixtures constitutes actual-host calibration.

The auxiliary snapshot has exactly these numeric fields, all positive:

| Field | Meaning |
| --- | --- |
| `serializedSeconds` | Serialized control admission duration, from 30 through 300 seconds. |
| `sustainedSeconds` | Sustained hot-row admission duration, from 600 through 3,600 seconds. |
| `runOverheadSeconds` | Per-phase worker startup, final checkpoint, drain, readback and teardown allowance; at least each phase's checkpoint timeout plus drain limit. |
| `storageAndCleanupSeconds` | Per-phase settled physical-storage collection, evidence retention and exact-table cleanup allowance. |
| `otherPreparationSeconds` | Remaining creation, calibration, idle and terminal cleanup allowance within the existing campaign overhead. |
| `<phase>.inventoryEntries`, `<phase>.inventoryBytes`, `<phase>.stagedEntries`, `<phase>.stagedBytes`, `<phase>.workBytes`, `<phase>.checkpointTimeoutMillis`, `<phase>.drainMillis` | The seven existing local limits, separately calibrated for each phase. Replace `<phase>` with `serialized` or `sustained`. |
| `<phase>.writeAttempts`, `<phase>.writeBytes`, `<phase>.readBytes` | Separately journalled operation and byte ceilings, sufficient for the phase inventory and four-attempt allowance. |

The sum of both complete phase bounds and `otherPreparationSeconds` must fit `campaignOverheadSeconds`; adding this file does not enlarge the offline cost reservation or host deadline.
The work limits must cover inventory plus the phase's sample reservation.
Ordinary and matrix observations retain their five-minute maximum and 8 MiB sample cap.
The sustained entry point permits at most one hour and reserves at most 64 MiB for samples; its complete observation-plus-drain window must be expressible in nanoseconds before work is created.
This upper bound is a capability, not the selected service duration or a completed sustained measurement.

`service-auxiliary campaign-directory serialized|sustained` claims one fresh, separately budgeted worker with the exact frozen JVM flags.
The controller and supervisor cannot claim that worker identity.
The existing supervisor sees its PID/start instant and deadline in the same active state, so terminal cleanup retains controller-before-worker termination and ownership-checked instance deletion.
A failed, empty or censored auxiliary observation is recorded as `FAILED`, retained and cleaned up like a successful one so the next phase remains admissible; it cannot be retried or substituted by a matrix outcome.

Each phase uses one fresh table (`stage2-serialized` or `stage2-sustained`) plus `weather-data`.
The resource adapter verifies exact inventory before creation and deletion and uses the same data and raw marker families as the matrix.
After successful drain/readback, the controller retains physical-storage and observation evidence, verifies table absence, then removes the phase work directory and records its evidence digest before admitting the next phase.
Deletion remains outside the publication lock and rechecks stop and controller identity before publishing completion.
With auxiliary reservations present, the last cleaned matrix cell enters `AUXILIARY_READY` instead of terminal `MATRIX_COMPLETE`.
The two phase outcomes and evidence records are separate from `run-N.properties`; `nextRun` stays at 648 throughout these observations.
The last retained auxiliary phase enters `CAMPAIGN_COMPLETE`, after which terminal cleanup applies.

These additions remain execution primitives.
The external package must still enforce actual host/runtime binding, calibrated capacities, bounded process/log handling, settled physical metrics and evidence transfer before invoking the trusted completion callbacks.
No service result or supported-workload verdict is established by the auxiliary unit fixtures or short no-service wiring test.
