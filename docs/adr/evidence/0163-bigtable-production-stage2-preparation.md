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
Production recovery acceptance through both API entry points and both Flink lines remains owned by [#1319](https://github.com/flink-gcp/flink-connector-gcp/issues/1319).

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
Eligibility and availability in `us-central1` remain unverified for this execution.
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

## Remaining service admission conditions

The production instrument must be reviewed and merged before final measurement, and the #1319 session must finish its service work and resource cleanup before this campaign starts.
Freeze source SHA, Linux runtime classpath or image digest, resource identities, exact commands, table/family names, worker leases, operations/storage limits and the cost reservation together.
Prepare independent supervision and loss-of-resource handling, and validate the retained-instance lifecycle before using the one-time trial.
A failure, empty or censored row, interruption or exhausted reservation stops execution without automatic retries, recreation, extension or capacity increases.

The retained result must include the full matrix and sustained hot-row phase, physical Bigtable storage after metrics settle, checkpoint storage, heap/GC, serialization/restore allocation, service metrics and complete notification occupancy.
Logical marker bytes do not establish physical storage or billing.
Reuse matching #1319 and #1316 evidence with its pinned configuration and oracle limits; record every unmeasured or failed workload explicitly.
Record the final verdict and update user-facing support guidance only from the retained acceptance evidence.
