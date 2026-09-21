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
| Queue | `projects/flink-gcp/locations/us-central1/queues/ct1246-RUN_ID`, created with the paused-queue configuration in `flink_tier3/cloudtasks.py`, paused and read back before the first cell once the service reports it, re-read on every poll, deleted at cleanup; one queue per session, never reused |
| Target | `https://ct1246.invalid/task`; the paused queue dispatches nothing, and the supervisor stops on any nonzero dispatch count |
| Flink lines and images | `2.2.1` from package `cloudtasks-measurement` at `sha256:62496ae01b1e32979434184d6ccf5be0920f54fd1ae988f8f5127a0ad741af80`, `1.20.4` from `cloudtasks-measurement-flink120` at `sha256:075029e13fd92ec43145fd68a98004357ef413258ee5a6cb30e86c3c62120296`; both are dispatch inputs verified live against Artifact Registry rather than pinned. They were published on 2026-09-19 UTC and become deletion-eligible seven days later, and the lifecycle demands a further 24-hour margin, so a session whose expiry reaches 2026-09-25T22:56Z is refused and needs a republication first |
| Pod shapes | supervisor 1 CPU / 2 GiB, sized so the scheduler cannot fit it into the remains of a busy node; JobManager 1 CPU / 2 GiB; TaskManager 1 CPU / 4 GiB for parallelism 1, 2 CPU / 8 GiB for 4, 4 CPU / 16 GiB for 16; one TaskManager holds every slot; TaskManagers on Spot, JobManager, supervisor and Operator on normal capacity |
| Restart strategy | fixed delay, three attempts, ten seconds; checkpoint state retained on cancellation under the cell's benchmark prefix |
| Reviewed revision | the `main` commit named at dispatch. The images above were built from `339d0a90675dffee74305cebe021093c6a1f9b4b`, whose supervisor source bundle hashes to `runtime_sha256` `fdf3a4d046fe9d1c7c07c5db5508bf87ec77416cd6df4bf2702e0c54470ba1f1`; a dispatch from a later commit that leaves `flink_tier3/` untouched carries the same bundle hash |
| Protocol pin | `flink_tier3/protocol_1246.toml`, converted field for field from the private draft, whose own SHA-256 is `32122ff01527f4e17a1eac7377c0f11430d71fabba2bce65b4c011afb5b07354`; the pin is part of the package source bundle, so `runtime_sha256` covers it; it is not part of the smaller set the supervisor mounts and `delivery_sha256` pins |
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

### First attempt, 2026-09-19

Run `cal1246-221-20260920g` executed `k01` to `k05` and was stopped during `k06`.
It measured the instrument rather than the service: every cell reconciled `invalid` on `receipt-fields` and `missing-source-start`, because the application declares its offered rate as a double and writes `10.0`, while the reader required an integer.
The measurement itself was intact, `k01` recording 2,430 rows against 2,430 observations, 2,430 exported rows and 2,430 distinct successful creations, and the session's 114 `observation` receipts each record the queue read back `PAUSED` with no dispatch reported.
The reader now accepts a whole number of either JSON type, and the receipts of that run are kept as the reconciler's fixtures so the contract has a test with the writer's own output in it.
Each attempt therefore takes the next campaign name: the ledger refuses a completed cell, which is what keeps a spent attempt from being quietly overwritten, so a repeat cannot reuse the ledger a previous one wrote.

### Second attempt, 2026-09-20

Run `cal1246b-221-4`, campaign `calibration-1246b`, executed `k01` to `k03` and stopped during `k04`.
The reader's repair held: `k01` and `k02` reconciled `complete`, which is the first time the evidence pipeline agreed with the service's own output end to end.
The supervisor kept the node Autopilot provisioned for it and was never preempted, which is what the larger supervisor request was for.
The preemption moved to the Operator Pod, which shares `tier3-system` and had been scheduled onto that same node: it was preempted at 12:39:56 UTC while `k03` was running, so `k03` reconciled `restarted` and is not a steady-state measurement; four replacements were refused by the namespace quota before one landed; and at 12:54:05 UTC the supervisor read the retired Pod's log, took the 404 for a deviation and began cleanup, which settled `k04` interrupted.
The environment reached verified idle with no lock and no queue, retaining only the interrupted cell's own benchmark evidence as the design intends, and the quota now admits the Operator's replacement while the supervisor tolerates a Pod that goes away under a log read.

### Third attempt, 2026-09-20

Run `cal1246c-221-1`, campaign `calibration-1246c`, executed all ten cells, and the environment returned to verified idle with no lock, no queue and nothing retained.
Every offered rate but the delay control's was met: achieved over offered rounds to 100.0 % at 10, 25, 100, 1,000 and 2,000 records per second, `k01` the only one under and the other four fractionally over, `k09` and `k10` produced 26,100 rows each with the 64-hex and 32-hex task names their arms require, and the interrupt control recovered with RESTARTING at +0.17 s, RUNNING at +10.5 s and a new creator at +24.8 s.
Two acceptance items hold and four fail, and none of the four fails on the service: `k03` is not a valid measurement of its own control, `k05` and `k06` lost their JobManager mid-cell, and no cell's sink metrics were discovered at all.
`k03` offers 20 records per second into a pipeline that its own 100 ms serialization delay caps at a measured 5.48, so its backlog grows without bound and each completed checkpoint takes about twice as long as the one before until one exceeds the 120-second timeout and the job restarts, twice; the series restarts with the job, running 7.3, 15.9, 33.2 and 69.0 seconds before the first expiry and 5.7, 12.5, 26.0, 53.6 and 109.3 before the second; the cell's one-second checkpoint interval is not the cause, and raising it would not repair the cell, because what exceeds the capacity is the offered rate.
`k05` and `k06` had their JobManager Pod replaced 3 min 04 s and 6 min 53 s after the previous cell's JobManager Deployment was deleted, their TaskManager surviving both times and no eviction, preemption or termination reason recorded for either, which [#1413](https://github.com/flink-gcp/flink-connector-gcp/pull/1413) addresses by keeping the JobManager off Spot and out of the autoscaler's way.
The sink metric identifiers were listed once per cell, on the first poll that reached the job, and all ten cells froze an empty list, so the staged gauges of `k09` and the `k04` against `k07` overhead figures have no evidence in this run at all; [#1440](https://github.com/flink-gcp/flink-connector-gcp/issues/1440) repairs the instrument, and the cell that carries the proof of that repair is the next attempt's `k09`.

The campaign of this file is therefore `calibration-1246c`, and it is now spent: the name advances with the cells the next cluster session will actually run, not ahead of them.

There is no record-count-mode pair: record-count mode prints its rows to standard output, which the session cannot capture, and receipt writes happen at most four times per creator incarnation outside the observation window, so their cost is bounded by construction and is read from the observed busy time rather than measured separately.
Higher offered rates are not calibrated in advance: the capacity search of the main assessment ramps each shape from a low rate and observes the instrument at every step.

## Numeric ceilings of the calibration session

The session policy in `policy.toml` is the hard bound; the values below are what this session is expected to need under it.
The cell count, records, creations and plan come from `flink_tier3.model.validate_cells` over the session file; the cost comes from `estimated_session_cost` at the window's lower bound of 11,918 seconds, and reaches USD 8.85 at its upper bound of 12,518 seconds.
The read estimate, the queue writes, the dispatch count and the evidence sizes are derived by hand from the poll interval, the queue protocol and the row size.

| Quantity | Calibration session | Policy ceiling |
| --- | ---: | ---: |
| Cells | 10 | 20 |
| Records the sources emit | 1,923,195 | derived |
| Task creations, planning bound (attempt limit × subtasks × four incarnations of one JobMaster) | 11,539,316 | 12,000,000 |
| Queue administration writes | create, pause, delete | 6 per actor |
| Metered reads | about 8,000 (735 polls, nine REST reads and one queue read each; a discovery or restart poll adds one) | 60,000 per actor |
| Dispatches | 0 | 0 |
| Pods | 4 running, 5 admitted | 5 |
| Replacement slot | `tier3-system` only; the Spot namespaces keep exact quotas, because a replacement TaskManager of the largest approved class would nearly double what the session quota enforces, and the restart accompanying one already invalidates the cell it was measuring. A Pod stuck `Terminating` can hold the slot for as long as its node stays unreachable, which the cost model does not charge: USD 0.141 per hour, taking the session from USD 8.66 to USD 9.13 at the planning bound if it were held throughout | 5 Pods |
| Session plan | 11,018 s; approved window 11,918 s to 12,518 s | 18,000 s |
| Durable evidence | rows about 80 to 120 MB compressed, receipts and observations under 60 MB | 4 GiB session, 256 MiB supervisor receipts |
| Incremental cost | USD 8.66 at the planning bound; about USD 4.81 at the expected creation count | USD 10.00 |

The Flink 1.20.4 repeat needs 4,110 s of plan, 236,800 planning-bound creations and USD 1.44 at its own window's lower bound of 5,010 seconds, USD 1.57 at the upper bound.
The planning bound assumes one JobMaster's restart budget; a JobManager failover resets it, so the cell deadline and the paused queue, not the bound, cap what a misbehaving cell can spend.

## Acceptance criteria for the instrument

Calibration passes when every item holds; a failure is repaired and this document is revised before any capacity claim.

- `k03` is detected: its unique-record throughput is at most 12 records per second and its per-repetition p95 is at least 100 ms, while `k02` achieves at least 95 % of its 25 records per second.
- `k01`, `k02`, `k04`, `k05` and `k06` achieve at least 95 % of their offered rates with no sustained backlog growth across checkpoint boundaries.
- The `k04` against `k07` pair reports the CSV output overhead in busy time, heap and achieved rate; the numbers parameterize the main approval and carry no threshold here.
- The connector gauges are discovered on `k09` and absent on `k04`, and `k10` produces 32-hex task names.
- Every cell except `interrupt-control-k11` is a usable steady-state result and reconciles `complete`: its job completed, it did not restart, its observation window holds at least six ordinary checkpoints, rows decoded from the parts equal `rows_exported` and the terminal counts, no unexplained duplicate, and the exported objects verify against their recorded hashes after download.
  The gauge and task-name items ask only that their cell's evidence was exported and verified, because neither reads a rate or a window.
- `interrupt-control-k11` reconciles `restarted`, because its own interruption makes the source register a second incarnation, with `missing-terminal` among its reasons; it is exported, and the analyzer refuses it as a steady-state result while still reporting its recovery timing.
- The session ends with the queue deleted, the benchmark prefixes released, the idle receipt and three empty refreshed plans; this item is the run's own receipts, not an analyzer check.

## Protocol interpretations the owner must confirm

The preregistered protocol leaves these points open; each is a decision, not a measurement, and the analysis code implements the reading below.

1. The capacity search runs on the `UNNAMED` arm of each shape, body and line, and the frozen rate applies to all five arms of that group.
   Only evidence that the sink fell behind rejects a rate: achieving less than 95 % of the offered rate, or sustained backlog growth across checkpoint boundaries.
   An interrupted, restarted or unexported probe, and one that met its offered rate over a window holding too few checkpoint boundaries to evaluate the backlog, measured nothing about saturation and is repeated under a fresh `-qN` index rather than counted as a rejection.
   A rate offered twice without a measurement stalls the search for an operator instead of buying a third probe, and a search that accepts the protocol's highest offered rate of 10,000 records per second reports a ceiling at that rate.
   A group whose contributing arms did not all run at one offered rate is inconclusive, as is a cell whose executed arm, line, body, shape, window or channel pool differs from the entry its ID names.
2. A repetition whose evidence is incomplete or whose Spot Pods were interrupted is recorded and repeated at most once, with the suffix `-x2`; a second failure leaves the arm short and the shape inconclusive.
   A probe is repeated the same number of times, but under a fresh `-qN` index, because the probe index is the end of the identifier.
3. The observation window of a cell runs from the first completed ordinary checkpoint after warm-up to the last completed ordinary checkpoint before end of input and must hold at least six such checkpoints; a cell with a restart is never a steady-state result.
4. Per-repetition p95 uses rows with status `OK` and a nonnegative latency. Throughput counts distinct sequences whose origin lies in the window and whose eventual status is `OK`, or `ALREADY_EXISTS` on a named or staged arm, where the service deduplicated an attempt whose outcome the client never saw; the unnamed baseline has no such answer, so an `ALREADY_EXISTS` there is a defect rather than a record.
5. No averaging across lines or bodies; the repetition order is the pinned order.
6. One queue per session; its depth is bounded by the session's creation ceiling and it is deleted at cleanup.
7. `ALREADY_EXISTS` on a named arm is explained only by an earlier attempt of the same sequence with the same name; otherwise the cell is invalid.
8. `https://ct1246.invalid/task` is the target; `k01` proves the service accepts it before any larger cell runs.
9. Calibration runs on Flink 2.2.1 first; the 1.20.4 repeat covers `k01` to `k04`.

## Main assessment scale, for context only

The pinned protocol holds 420 cells whose warm-up and observation sum to exactly 40 hours.
Adding the ten minutes of startup and three minutes of teardown each cell is planned with brings the 420 cells to 138 hours.
The capacity search adds one cell per probe: simulating the pinned search from an initial 25 records per second reaches the planning capacities below in six probes for the single-subtask shape, nine for the three shapes planned at 500 records per second, ten for the sixteen-subtask single-concurrency shape and eleven for the two sixteen-by-sixteen shapes, so the 28 groups of shape, body and line need 260 probe cells, or 86.6 hours.
The campaign is therefore about 224 hours of cell time, 137.9 for the pinned cells and 86.6 for the probes, and a probe that has to be repeated adds to it.
The per-session cell ceiling of 20 bounds 680 cells at 34 sessions, and the five-hour window, of which cleanup reserves the last 15 minutes, bounds the same time at 48, so the campaign needs at least 48 sessions as the ceilings stand.
Compute at the policy rates is about USD 204 for that cell time, with the largest parallelism class accounting for 108 of the 224 hours.
Creations depend on the accepted rates: at planning capacities of 25 records per second for the single-subtask shape, 400 for the sixteen-subtask single-concurrency shape, 500 for the four-subtask and one-subtask sixteen-concurrency shapes and 3,000 for the two sixteen-by-sixteen shapes, the sources would emit about 256 million records (USD 103 at USD 0.40 per million); halving the two largest shapes to 1,000 records per second brings that to about 112 million (USD 45).
The 260 probe cells add about 88 million records of their own at those capacities (USD 35), because the search spends most of its probes near the accepted rate.
The planning bound over those cells is several times the expected count, so the per-session creation ceiling binds long before the cost ceiling does.
Compressed rows would occupy 10 to 15 GB of evidence at 40 to 60 bytes a row.
Those sessions need a revised `[cloudtasks_ceilings]` for cells, creations and evidence, and the session count is itself a reason to revisit the five-hour window; each is a separate reviewed change and approval.
