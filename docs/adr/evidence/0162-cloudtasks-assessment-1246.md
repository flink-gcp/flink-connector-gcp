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

# Cloud Tasks performance assessment: calibration preregistration

This is the preregistration for the host calibration that precedes the [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246) assessment, within [ADR-0162](../0162-cloud-tasks-implementation-precedes-final-performance-acceptance.md) and [ADR-0104](../0104-exactly-once-modes-use-service-native-replay-protection-and-pass-a-performance-gate.md).
It fixes the execution conditions, the calibration cells, the numeric ceilings of the calibration session, the acceptance criteria for the instrument, and the protocol interpretations the owner must confirm before any paid session.
Nothing here authorizes a dispatch: a session runs only after the owner approves this document's numbers, an image has been published for each Flink line, and the workflow input carries the approval phrase.
The main assessment has its own later approval; the scale estimate at the end is context for that decision, not a request.

## Fixed execution conditions

| Condition | Value |
| --- | --- |
| Project, region, cluster | `flink-gcp`, `us-central1`, the existing `flink-tier3` Autopilot cluster |
| Namespaces | `tier3-cloudtasks` for the cells, `tier3-system` for the Operator and supervisor |
| Queue | `projects/flink-gcp/locations/us-central1/queues/ct1246-RUN_ID`, created with the paused-queue configuration in `flink_tier3/cloudtasks.py`, paused and read back before the first cell, re-read on every poll, deleted at cleanup; one queue per session, never reused |
| Target | `https://ct1246.invalid/task`; the paused queue dispatches nothing, and the supervisor stops on any nonzero dispatch count |
| Flink lines and images | `2.2.1` from package `cloudtasks-measurement`, `1.20.4` from `cloudtasks-measurement-flink120`; the digests are dispatch inputs verified live against Artifact Registry and are **pending publication** |
| Pod shapes | JobManager 1 CPU / 2 GiB; TaskManager 1 CPU / 4 GiB for parallelism 1, 2 CPU / 8 GiB for 4, 4 CPU / 16 GiB for 16; one TaskManager holds every slot; Flink Pods on Spot, supervisor and Operator on normal capacity |
| Restart strategy | fixed delay, three attempts, ten seconds; checkpoint state retained on cancellation under the cell's benchmark prefix |
| Reviewed revision | the `main` commit named at dispatch, which also fixes `runtime_sha256` and the CUE manifests; **pending** until the preparation pull requests merge |
| Protocol pin | `flink_tier3/protocol_1246.toml`, converted field for field from the private draft with SHA-256 `32122ff01527f4e17a1eac7377c0f11430d71fabba2bce65b4c011afb5b07354`; the pin is part of the supervisor source bundle, so `runtime_sha256` covers it |
| Pricing basis | the reviewed Autopilot rates in `policy.toml` and USD 0.40 per million Cloud Tasks operations; both are re-checked at approval, and admission refuses a pricing review older than 30 days |

The staged arms keep the application's fixed options: one-hour name retention, one-minute clock-skew allowance, a 20-second request timeout, `recoveryMaxAttempts` 3 and `notFoundRecoveryMaxAttempts` 1.
These are experiment inputs, not recommended production values.

## Calibration cells

The reviewed session file is [`kubernetes/lifecycle/sessions/calibration-1246.toml`](../../../kubernetes/lifecycle/sessions/calibration-1246.toml); Flink 2.2.1, 1 KiB bodies, channel pool 1, `UNNAMED` unless stated.
Warm-up and observation windows follow the protocol rule, max(60 s, two checkpoints) and max(180 s, six checkpoints).

| Cell | Purpose | Parallelism, concurrency, checkpoint | Offered rate | Records |
| --- | --- | --- | ---: | ---: |
| `k01-pace-10` | pacing floor and target acceptance | 1, 1, 1 s | 10 | 2,430 |
| `k02-pace-25` | pacing near the single-subtask ceiling | 1, 1, 1 s | 25 | 6,075 |
| `k03-delay-control` | the 100 ms serialization delay the analysis must detect | 1, 1, 1 s | 20 | 4,860 |
| `k04-pace-100` | pacing with CSV rows on | 4, 4, 10 s | 100 | 26,100 |
| `k05-pace-1000` | pacing at 1,000 records per second | 16, 16, 60 s | 1,000 | 601,000 |
| `k06-pace-2000` | pacing at 2,000 records per second | 16, 16, 60 s | 2,000 | 1,202,000 |
| `k07-counts-only` | the `k04` pair without CSV output | 4, 4, 10 s | 100 | 26,100 |
| `k09-staged-gauges` | staged arm gauges and 64-hex names (`STAGED_HASH`) | 4, 4, 10 s | 100 | 26,100 |
| `k10-random-control` | 32-hex name control (`NAMED_RANDOM_CONTROL`) | 4, 4, 10 s | 100 | 26,100 |
| `interrupt-control-k11` | TaskManager killed 60 s after warm-up; proves the incomplete-evidence path | 1, 1, 1 s | 10 | 2,430 |

A second reviewed file, [`calibration-1246-flink120.toml`](../../../kubernetes/lifecycle/sessions/calibration-1246-flink120.toml), repeats `k01` to `k04` on Flink 1.20.4 in a separate session and campaign under its own approval.
There is no record-count-mode pair: record-count mode prints its rows to standard output, which the session cannot capture, and receipt writes happen at most four times per creator incarnation outside the observation window, so their cost is bounded by construction and is read from the observed busy time rather than measured separately.
Higher offered rates are not calibrated in advance: the capacity search of the main assessment ramps each shape from a low rate and observes the instrument at every step.

## Numeric ceilings of the calibration session

The session policy in `policy.toml` is the hard bound; the values below are what this session is expected to need under it, computed from the session file by `flink_tier3.model.validate_cells` and `estimated_session_cost`.

| Quantity | Calibration session | Policy ceiling |
| --- | ---: | ---: |
| Cells | 10 | 20 |
| Records the sources emit | 1,923,195 | derived |
| Task creations, planning bound (attempt limit × subtasks × four incarnations of one JobMaster) | 11,539,316 | 12,000,000 |
| Queue administration writes | create, pause, delete | 6 per actor |
| Metered reads | about 8,000 (735 polls, at most nine REST reads and one queue read each) | 60,000 per actor |
| Dispatches | 0 | 0 |
| Pods | 4 | 4 |
| Session plan | 11,018 s; approved window 11,918 s to 12,518 s | 18,000 s |
| Durable evidence | rows about 80 to 120 MB compressed, receipts and observations under 60 MB | 4 GiB session, 256 MiB supervisor receipts |
| Incremental cost | USD 8.31 at the planning bound; about USD 4.47 at the expected creation count | USD 10.00 |

The Flink 1.20.4 repeat needs 4,110 s of plan, 236,800 planning-bound creations and about USD 1.29.
The planning bound assumes one JobMaster's restart budget; a JobManager failover resets it, so the cell deadline and the paused queue, not the bound, cap what a misbehaving cell can spend.

## Acceptance criteria for the instrument

Calibration passes when every item holds; a failure is repaired and this document is revised before any capacity claim.

- `k03` is detected: its unique-record throughput is at most 12 records per second and its per-repetition p95 is at least 100 ms, while `k02` achieves at least 95 % of its 25 records per second.
- `k01`, `k02`, `k04`, `k05` and `k06` achieve at least 95 % of their offered rates with no sustained backlog growth across checkpoint boundaries.
- The `k04` against `k07` pair reports the CSV output overhead in busy time, heap and achieved rate; the numbers parameterize the main approval and carry no threshold here.
- The connector gauges are discovered on `k09` and absent on `k04`, and `k10` produces 32-hex task names.
- Every cell except `interrupt-control-k11` reconciles `complete`: rows decoded from the parts equal `rows_exported` and the terminal counts, no unexplained duplicate, and the exported objects verify against their recorded hashes after download.
- `interrupt-control-k11` reconciles `incomplete`, is exported, and the analyzer refuses it as a steady-state result while still reporting its recovery timing.
- The session ends with the queue deleted, the benchmark prefixes released, the idle receipt and three empty refreshed plans.

## Protocol interpretations the owner must confirm

The preregistered protocol leaves these points open; each is a decision, not a measurement, and the analysis code implements the reading below.

1. The capacity search runs on the `UNNAMED` arm of each shape, body and line, and the frozen rate applies to all five arms of that group.
2. A cell whose evidence is incomplete or whose Spot Pods were interrupted is recorded and repeated at most once, with the suffix `-x2`; a second failure leaves the arm short and the shape inconclusive.
3. The observation window of a cell runs from the first completed ordinary checkpoint after warm-up to the last completed ordinary checkpoint before end of input and must hold at least six such checkpoints; a cell with a restart is never a steady-state result.
4. Per-repetition p95 uses rows with status `OK` and a nonnegative latency; throughput counts distinct sequences whose eventual status is `OK` and whose origin lies in the window.
5. No averaging across lines or bodies; the repetition order is the pinned order.
6. One queue per session; its depth is bounded by the session's creation ceiling and it is deleted at cleanup.
7. `ALREADY_EXISTS` on a named arm is explained only by an earlier attempt of the same sequence with the same name; otherwise the cell is invalid.
8. `https://ct1246.invalid/task` is the target; `k01` proves the service accepts it before any larger cell runs.
9. Calibration runs on Flink 2.2.1 first; the 1.20.4 repeat covers `k01` to `k04`.

## Main assessment scale, for context only

The pinned protocol holds 420 cells whose warm-up and observation sum to 40 hours.
With startup and teardown per cell and about 224 capacity probes (28 shape, body and line groups at about eight probes each), the campaign needs about 115 hours of cell time, or about 24 sessions of at most five hours.
Creations depend on the accepted rates: at planning capacities of 25, 400, 500 and 3,000 records per second for the four shape families, expected creations are about 316 million (USD 126 at USD 0.40 per million) and compute about USD 80 at the policy rates; if the two largest shapes accept 1,000 records per second, creations fall to about 100 million (USD 40).
The planning bound for those sessions is several times the expected count, so the per-session creation ceiling will bind long before the cost ceiling does.
Compressed rows would occupy 12 to 19 GB of evidence.
Those sessions need a revised `[cloudtasks_ceilings]` for creations and evidence, which is a separate reviewed change and approval.
