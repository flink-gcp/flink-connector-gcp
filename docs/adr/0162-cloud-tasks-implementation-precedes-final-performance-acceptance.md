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

# ADR-0162: Cloud Tasks implementation precedes final performance acceptance

- Status: Accepted
- Date: 2026-09-06
- Updated: 2026-09-19 (calibration preregistration and evidence pipeline for #1246)
- Updated: 2026-09-21 (split execution: performance on a lean single VM, correctness on the cluster)
- Issues: [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238), [#1241](https://github.com/flink-gcp/flink-connector-gcp/issues/1241), [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246)
- Evidence: [calibration preregistration](evidence/0162-cloudtasks-assessment-1246.md)
- Supersedes: only the Cloud Tasks requirement in [ADR-0104](0104-exactly-once-modes-use-service-native-replay-protection-and-pass-a-performance-gate.md) and [ADR-0158](0158-cloud-tasks-checkpointed-creation-stages-named-tasks-and-commits-after-the-checkpoint.md) for a separate primitive performance pass before implementation, connector-level evaluation or release
- Modules: cloudtasks
- Current behavior: `docs/content/docs/connectors/delivery-guarantees.md` § Performance decision rule

## Context

ADR-0158 defines checkpointed task creation, including random identities when the application cannot provide a stable key.
Its implementation-start condition required a primitive performance pass that the [#1241 repeat](evidence/0104-cloudtasks-stage1-1241.md#result-on-2026-09-06) did not supply.

## Evidence

The repeat's three completed one-channel measurements on 2026-09-06 observed no material named-task throughput decrease in the executed, capped cells: random names achieved 350.26 calls/s, hash names 358.45 calls/s, and unnamed tasks 335.83 calls/s, with 50,000 calls in each measured arm.
They ran sequentially in that order on an accumulating queue, at estimated starting depths of 202,740, 257,740 and 312,740; queue depth and arm order were not independently controlled.
The experiment nevertheless remains inconclusive under its preregistered rules: required repetitions and controls were missing, and the admission limiter waited after the initial in-flight window.
That verdict is not a measured failure of the throughput threshold.

The [post-run calibration](evidence/0104-cloudtasks-stage1-1241.md#post-run-local-calibration-2026-09-06) isolated a local pacing limitation and checked a repaired waiting implementation without a service client or network requests.
It does not establish the cause of the real run's multi-second response latency or estimate uncapped service capacity.
The concurrent replay's two successful same-name responses remain documented; the local validator's stop is not a service error code or proof of two persisted tasks.

## Decision

The owner [accepted implementation with these measurement limits](https://github.com/flink-gcp/flink-connector-gcp/issues/1238#issuecomment-5559502662).
Another standalone primitive repeat has less immediate decision value than implementing the accepted protocol and evaluating its complete cost.
This is a change to when performance acceptance is required, with the risk of implementation effort that the final assessment may decline.

The Cloud Tasks implementation may proceed from the accepted ADR-0158 protocol without a separate Stage 1 pass.
[#1241](https://github.com/flink-gcp/flink-connector-gcp/issues/1241) is complete as an inconclusive investigation; neither implementation nor release requires reopening it or relabeling its result.
The delivery order is:

| Work | Entry condition and responsibility |
|---|---|
| [#1242](https://github.com/flink-gcp/flink-connector-gcp/issues/1242) | Implement the accepted writer, immutable envelope and versioned committable format, with their deterministic tests. |
| [#1243](https://github.com/flink-gcp/flink-connector-gcp/issues/1243), then [#1244](https://github.com/flink-gcp/flink-connector-gcp/issues/1244) | Follow the merged predecessor with the committer/DataStream entry point, then Table integration and operational guidance. |
| [#1245](https://github.com/flink-gcp/flink-connector-gcp/issues/1245) | After integration, validate real-service recovery within ADR-0158's scope and the published service assumptions of ADR-0154. |
| [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246) | After the implementation tests and #1245 acceptance, evaluate the implemented mode and decide its supported workloads and release disposition. |

Implementation PRs may be reviewed and merged in that order before final performance acceptance.
The mode's release and any claim that it is supported require the correctness tests on both supported Flink lines, #1245 acceptance, and #1246's final assessment.
Development documentation that accompanies an API entry point states this remaining release condition.

The final assessment compares unnamed at-least-once, random/hash-named at-least-once, and staged creation with equivalent name distributions.
It reports both the total cost against the unnamed path and the incremental staging/commit cost against the named path, so accepting implementation does not discard the task-identity question.
The support thresholds apply to total staged cost against the unnamed path; the named baseline isolates the additional staging/commit cost.
It retains ADR-0104's general thresholds (at least 70% throughput and at most 2x p95), constrained thresholds (at least 25% and at most 4x), three measured repetitions after warm-up, and greater-than-10% variability rule.
The full benchmark design in #1246 remains binding, including equal conditions across arms and retention/recovery-budget sizing; the coverage below summarizes it.
The assessment covers 1 KiB and 64 KiB tasks, evenly distributed names and meaningful destination/workload skew, concurrency and Flink parallelism 1/4/16, and checkpoint intervals of 1/10/60 seconds.
Duplicate-task replay is a separate workload; a repeated ID must not represent distinct accepted records merely to manufacture a hot key.
Measure throughput, task visibility latency, checkpoint duration/size, live TaskManager heap, serialization/restore copies, pending checkpoints, backlog, recovery time and remaining replay budget, as specified in #1246.
An inconclusive final assessment supplies no support or release approval; a changed threshold or supported workload requires an explicit decision rather than retrospective reinterpretation.

Before any later service measurement, preregister its offered-load or capacity question and the evidence needed to answer it, calibrate the harness on the execution host, and include a sensitivity control that can detect a known regression.
The local wait repair alone does not validate a future capacity experiment: a deliberate pacing cap still limits what its throughput can establish.
Each real-GCP run retains separate approval of concrete resources, location, lifetime, operation/dispatch caps and cost, followed by verified cleanup.
The [calibration preregistration](evidence/0162-cloudtasks-assessment-1246.md) fixes the execution conditions, the calibration cells and their numeric ceilings, the instrument's acceptance criteria and the protocol interpretations the owner confirms before a session; the pinned protocol, the observation and reconciliation code and the offline analyzer are part of the reviewed supervisor bundle.
That paragraph describes the cluster, and the [2026-09-21 refinement](#split-execution-after-the-third-calibration-2026-09-21) moves the performance half off it: for that half the supervisor bundle, the session ledger and the receipt contract have no counterpart, and the preregistration's execution conditions, numeric ceilings, acceptance items and scale estimate are owed a revision that names the rig each covers.

## Split execution after the third calibration (2026-09-21)

The owner decided on 2026-09-21 to take the assessment's performance half on a lean single-VM rig and to leave its correctness half on the Autopilot cluster.
The quantities the assessment measures, its arms, cells, repetitions, periods, thresholds and variability rule are unchanged, and so is what it must answer. What changes is where each half executes and, following from that, the evidence contract and the instrument acceptance items of the performance half, which are revised rather than reinterpreted. That is a refinement of how the evidence is produced, not of what counts as passing.

The reason is the third calibration rather than a preference. Run `cal1246c-221-1` met every rate it offered but its own delay control's — 100.0 % at 10, 25, 100, 1,000 and 2,000 records per second — and still failed four of six acceptance items, every one of them on the instrument or the environment: a delay control invalid by its own arithmetic, two cells whose JobManager Pod was replaced mid-measurement with no reason recorded, and a metric listing frozen empty on all ten cells.
Kubernetes is where Flink really runs; it is not a measurement instrument, and a campaign of 420 preregistered cells and 260 probes across at least 48 sessions would measure the environment's variance at that loss rate.

- The performance half — throughput, task visibility latency, the capacity search, and the staged-against-unnamed and staged-against-named cost comparisons — runs on one `e2-standard-8` in `us-central1`, with a Compute Engine maximum run duration and `DELETE` termination action as the host deadline, on the operating model [ADR-0166](0166-bigtable-implementation-precedes-final-stage2-acceptance.md)'s refinement records: a single host under that deadline, a controller outside tracked source, and a campaign of preregistered runs it sequences. How the frozen classpath reaches that host, and the embedded cluster and fork-per-run isolation the runs use, are the rig's own to record when it exists; this refinement does not borrow them from another ADR that does not carry them.
  The size is measured rather than copied. At 2,000 records per second and parallelism 16, `k06` of that run filled 7.51 GB of the 7.78 GB heap Flink derived inside a 16 GiB, four-vCPU TaskManager, while its JVM process-load samples peaked at 0.55 of those four vCPUs and usually sat below 0.30; memory is what binds. #1327 measured the same constraint from the other side on a 16 GB `e2-standard-4`: heap exhaustion in calibration forced its staged capacity down to 192 MiB per subtask, the most its parallelism-16 cells admitted inside a 10 GiB heap, and at that cap 48 of the 51 staged runs attempted at 64 KiB were censored for capacity against 33 of 162 at 1 KiB.
  Two limits of that evidence are worth stating: the load figure is the maximum of 33 samples about 19 seconds apart, so it bounds no instant between them, and `k06` is one of the cells this document counts as restarted — which makes it unusable as a throughput measurement but leaves its heap and load readings as what the shape actually asked of a machine.
- The correctness half — staged commit across a JobManager failover, recovery from TaskManager loss, and replay deduplication after a restart — stays on the cluster, because only a real one produces those failures. That half already holds: the same run's interrupt control recovered within 25 seconds, and `k05` deduplicated 615,143 rows to 601,000 distinct across a genuine JobManager replacement.
- The VM rig's evidence contract follows #1327's: local per-run files and a SHA-256 manifest of the frozen inputs, collected to the reviewing host. The GCS receipt reconciliation answers a failure mode a single VM does not have — a Spot Pod vanishing mid-write — so the acceptance items of the [calibration preregistration](evidence/0162-cloudtasks-assessment-1246.md) that name that contract owe a revision for the rig that runs them rather than being carried onto it. That revision is not made here, and the preregistration's own rule — a failure is repaired and the document revised before any capacity claim — governs when it must be.
- The rig's controller is temporary automation kept outside tracked source on ADR-0166's terms, with its source attached to the assessment record; the measurement runtime it drives stays reviewed source in this repository.
- The two bullets above name the headline quantities, not all of them. Checkpoint duration and size, live TaskManager heap, serialization and restore copies, pending checkpoints, backlog and remaining replay budget are measured by whichever half observes the shape that produces them, and the revision above is where each is assigned; recovery time belongs to the correctness half by construction.
- The VM rig owes its own host calibration and its own stop conditions, on the terms the Decision above already sets for any execution host. ADR-0166's campaign states both; this refinement records the obligation rather than inventing the rules before the rig exists.
- Each half keeps its own resource and cost approval, and no ceiling for the performance half exists yet. Neither the split nor this refinement authorizes a session.

## Consequences

ADR-0158's protocol, correctness tests, scope, deadlines and exclusions remain the implementation contract.
The current at-least-once defaults remain in force, and the staged mode is not made available by this documentation change.
Other connectors keep ADR-0104's gate order.
The original Cloud Tasks measurements and their inconclusive verdict remain archived; the performance decision moves to #1246 rather than being inferred from #1241.

## Alternatives declined

- Require another primitive pass before writing the staged runtime: no material throughput decrease was observed in the capped, sequential cells above, and the final assessment can measure identity and staging costs together against the implemented protocol.
- Reclassify #1241 as passing or conclude that task IDs have no performance cost: its missing repetitions, controls and pacing limitation remain, and the observations cover only the executed conditions.
- Remove performance acceptance from release: the complete mode adds staging, checkpoint, heap and recovery costs that the primitive measurements did not evaluate.
