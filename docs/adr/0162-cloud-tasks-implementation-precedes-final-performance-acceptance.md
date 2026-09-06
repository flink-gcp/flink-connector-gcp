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
- Issues: [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238), [#1241](https://github.com/flink-gcp/flink-connector-gcp/issues/1241)
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

## Consequences

ADR-0158's protocol, correctness tests, scope, deadlines and exclusions remain the implementation contract.
The current at-least-once defaults remain in force, and the staged mode is not made available by this documentation change.
Other connectors keep ADR-0104's gate order.
The original Cloud Tasks measurements and their inconclusive verdict remain archived; the performance decision moves to #1246 rather than being inferred from #1241.

## Alternatives declined

- Require another primitive pass before writing the staged runtime: no material throughput decrease was observed in the capped, sequential cells above, and the final assessment can measure identity and staging costs together against the implemented protocol.
- Reclassify #1241 as passing or conclude that task IDs have no performance cost: its missing repetitions, controls and pacing limitation remain, and the observations cover only the executed conditions.
- Remove performance acceptance from release: the complete mode adds staging, checkpoint, heap and recovery costs that the primitive measurements did not evaluate.
