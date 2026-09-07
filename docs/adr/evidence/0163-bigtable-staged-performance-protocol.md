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

# Bigtable staged-write Stage 2 protocol

This is the preregistered experimental design for [ADR-0163](../0163-bigtable-checkpointed-writes-stage-immutable-mutations-and-retain-row-markers.md) and [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211).
It does not authorize a service run or record a result.
The local sizing probe is separate; it neither runs this benchmark nor measures Bigtable capacity.
The [local MiniCluster harness](0163-bigtable-local-staged-harness.md) adds fake and emulator preparation without creating service resources; its fixed-input observations are not this protocol's service result.

## Question and controlled conditions

Measure the complete staged path's sustainable throughput, record visibility latency, checkpoint and recovery cost against the existing at-least-once bulk path.
Use a Flink test application with the proposed writer/committer protocol and real single-row clients; the production API remains unavailable until the applicable gate passes.
Run both arms with the repository-pinned BOM and Flink versions, identical node resources, source data, serializer and TaskManager resources.
Freeze the source SHA, complete runtime flags, checkpoint storage, heap and client-channel configuration before execution.
Generate distinct deterministic payloads and fixed millisecond timestamps; one record writes one data cell plus, in the candidate, its replay marker.

The full factorial matrix is 1 KiB/64 KiB payloads, evenly distributed/hot-row keys, parallelism 1/4/16, per-subtask admitted in-flight entries 1/4/16, and checkpoint intervals 1/10/60 seconds: 108 cells.
Hash the row prefix for the even arm.
For the hot arm, route 90% of inputs to one row and 10% to evenly distributed rows; retain distinct envelope IDs even on the hot row.
Fix the distribution seed and validate its histogram before each run.
The bulk bound counts entries awaiting acknowledgements; the staged bound counts conditional requests in the commit stage.
Do not equate those bounds with the number of checkpoint-owned envelopes.

Each cell has three repetitions per arm, each in a fresh JVM.
Alternate arm order by repetition, using a predeclared order, and start each repetition with new tables so prior marker accumulation cannot favor one repetition.
Warm up for `max(10 seconds, checkpoint interval)` and measure admitted inputs for `max(30 seconds, 3 × checkpoint interval)`.
Finish a checkpoint and drain every measured input before ending the observation; report the drain duration separately.
Warm-up and measured row namespaces are distinct and their metrics are separated.
With these durations the full matrix needs at least 19.2 hours of warm-up and admission time, before draining, JVM teardown, setup and pauses.
Split it into separately approved resource leases; do not compress it into the one-hour Stage 1 budget.

The source drives a capacity experiment through backpressure, without an intentional fixed-rate limiter.
Record source starvation and writer/committer admission waits to identify a harness bottleneck.
If a run uses a safety operation cap or cannot sustain input, report the limit and classify its capacity result as censored rather than asserting service saturation.
Before any service run, calibrate on the execution host with a no-service receiver and a known slower control; the control must register a lower throughput or larger latency in the reported metric.
Run a serialized conditional control against the service in its own approved operation budget.

## Metrics and verdict

Throughput is the number of distinct measured inputs acknowledged as visible divided by elapsed time from the start of the admission window through its final measured acknowledgement.
Include the final drain in this denominator for both arms.
Record p50/p95/p99 from input admission to successful write acknowledgement, including staging, checkpoint waiting and commit, as the visibility-latency proxy.
Also report RPC latency separately; a faster RPC does not remove checkpoint visibility delay.
Count acknowledged inputs from the harness's per-input inventory, independently of final stored-cell counts.
Read back the measured namespaces after completion and compare row/cell cardinalities and values with the generated workload.
Repeated hot-row writes to the same cell version overwrite one another, so readback cannot count their individual applications or require a winner across concurrent writers.
Accept only a value submitted to that cell version; use the separate SUM correctness arms below to count contributions and detect replayed effects.
For the staged arm, also compare retained marker identities with the acknowledged-envelope inventory.

Apply ADR-0104's thresholds to total staged throughput and total visibility p95 against bulk: general support at at least 70% and at most 2x, constrained opt-in at at least 25% and at most 4x, otherwise decline.
Checkpoint waiting alone can fail that latency criterion; do not subtract it after observing the result.
A change to the latency criterion or acceptance of a narrower workload needs an explicit decision and an amended ADR, rather than a retroactive pass.
For each arm and cell, a throughput range above 10% of its three-repetition mean is inconclusive.
Do not combine passing cells into a claim about failing or unmeasured cells, or use a primitive-RPC ratio as the mode's verdict.

Collect checkpoint duration and serialized size, completed/aborted/pending checkpoints, Flink pending committables, staged bytes/entries, TaskManager heap after collection and peak heap, GC time, serialization/restore allocations, client concurrency and queueing, cluster CPU/hottest-node CPU, server latency and request counts.
Record warm-up, admission, completion, drain, recovery and cleanup timestamps.
Measure marker cells per row and their logical bytes, table storage after service metrics settle, and same-row growth across a separately identified sustained hot-row phase.
Report checkpoint storage and Bigtable storage separately.
Do not infer physical storage billing from the sum of key/qualifier/value bytes alone.

## Recovery and correctness arms

Use INT64 SUM at a fixed bucket timestamp so duplicate applications change an independently read result.
Submit distinct contributions, replay a known subset of the same persisted envelopes, and compare sums and marker counts with the distinct-envelope inventory.
Use a client seam that can discard an acknowledgement after a successful service response, then terminate and restore the job from its completed checkpoint.
Also interrupt a partially completed commit and verify all remaining envelopes eventually apply once.
These arms validate the staged protocol, not Bigtable's service guarantee beyond its published contract.

Test rescaling 2-to-1 and 2-to-3, interruption before checkpoint completion, a successfully finished stop-with-savepoint and its resume, and the documented failed-stop limitation.
Exercise both supported Flink lines for correctness; performance cells use the pinned default line and do not imply equivalent throughput on the other line.
Validate rejected multi-cluster and transaction-disabled profiles, missing metadata permission and an invalid marker family's GC/type before admitting target writes.
Record a negative control with marker protection disabled that doubles a SUM contribution; a replay test unable to detect it fails calibration.

For state sizing, provision enough heap and staged capacity for the requested cell from local calibration and record both settings before the run.
If the necessary resources exceed the approved budget, leave that cell unmeasured and identify the bound rather than silently lowering its load or checkpoint interval.
An overflowing writer or growing collector backlog is a workload limit, not a reason to discard a repetition.

## Resource authorization and cleanup

Each execution request must name the project, unique ephemeral instance, region/zone, SSD node count, single-cluster transactional profile, table names/families, Flink compute location/resources, checkpoint storage and monitoring scope.
The marker family is raw with no GC rule; recovery tables also have an INT64 SUM family.
Use the same node configuration for both arms of a cell.
Authorize a hard lease duration, maximum operations and storage, and cost ceiling using current official prices before creating anything.
The authorization must state the permitted cells/repetitions and include creation, inspection, execution and cleanup of those exact resources.

The harness first verifies every target is absent; a collision fails without reusing or deleting it.
Cleanup runs on success, failure and budget exhaustion, deletes the exact created resources and checkpoint objects, and independently verifies absence.
Record any unremoved resource immediately and stop further work on that lease.
Changing node count, region, cell coverage, experiment timing, repetition count, or budget requires a new authorization before the change is run.
No resources have been created under this protocol.
