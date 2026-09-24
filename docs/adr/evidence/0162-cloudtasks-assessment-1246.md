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

# Cloud Tasks performance assessment: preregistration

This is the preregistration for the [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246) assessment, within [ADR-0162](../0162-cloud-tasks-implementation-precedes-final-performance-acceptance.md) and [ADR-0104](../0104-exactly-once-modes-use-service-native-replay-protection-and-pass-a-performance-gate.md).
The assessment runs on a single-VM rig, as ADR-0162's 2026-09-21 refinement decided.
Each section below names the rig it covers: the VM rig for everything that is still to run, and the cluster for the three calibration attempts already recorded, kept as they ran.
Nothing here authorizes execution: the host, its deadline, each campaign's resources and cost are approved separately before anything billable runs, and the main assessment has its own later approval.

## Fixed execution conditions on the VM rig

| Condition | Value |
| --- | --- |
| Project and region | `flink-gcp`, `us-central1`; the zone is named in the approval |
| Host | one `e2-standard-8` (8 vCPU, 32 GB) on standard provisioning, not Spot, with a Compute Engine maximum run duration and `DELETE` termination action as its deadline |
| JVM | Java 17, `-Xmx24g`, leaving 8 GB to the operating system, direct memory and the controller; every run reports its peak heap |
| Flink line | `2.2.1`, from a classpath built from the reviewed source and frozen with its launcher among the campaign's inputs |
| Embedded cluster | the lean probe's in-JVM cluster, one TaskManager holding every slot; hashmap state, filesystem checkpoint storage inside the run's directory, the cell's checkpoint interval, a 120-second checkpoint timeout, one concurrent checkpoint, two retained, and a fixed-delay restart of three attempts ten seconds apart. These are the cluster's values, pinned in the probe by [#1473](https://github.com/flink-gcp/flink-connector-gcp/issues/1473) |
| Instrument | the lean probe ([#1446](https://github.com/flink-gcp/flink-connector-gcp/issues/1446)) reading the sink's gauges in process and, from [#1473](https://github.com/flink-gcp/flink-connector-gcp/issues/1473), writing each checkpoint completion; rows and receipts come from the measurement job |
| Queue | `projects/flink-gcp/locations/us-central1/queues/ct1246-<campaign>-<order>`, one per run, created with the paused-queue configuration in `flink_tier3/cloudtasks.py` and read back paused before the run, deleted after it; never reused |
| Target | `https://ct1246.invalid/task`; the paused queue dispatches nothing |
| Sequencing | the campaign journal (`flink_tier3.campaign`) and controller (`flink_tier3.vmcampaign`); each run reserves its nominal span plus 600 seconds |
| Evidence | each run's receipts, rows, samples and logs inside its own run directory, under an evidence root the controller names, with the per-file SHA-256 digests the controller records with its outcome; campaign directories are collected to the reviewing host with a SHA-256 manifest of the collected tree beside the frozen inputs' own digest |
| Analysis | `flink_tier3.vmanalyze`, offline, reading the cell set from `flink_tier3/protocol_1246.toml` and applying the decision rule to the measured arms; the calibration's three items are read from the same rows and the probe's output |
| Pricing basis | USD 0.268 per hour for the host and USD 0.40 per million Cloud Tasks operations, both re-checked when the owner approves an estimate |

The staged arms keep the application's fixed options: one-hour name retention, one-minute clock-skew allowance, a 20-second request timeout, `recoveryMaxAttempts` 3 and `notFoundRecoveryMaxAttempts` 1.
These are experiment inputs, not recommended production values.

## The reduced assessment

The owner reduced the assessment on 2026-09-23 to what answers its question — whether staged creation keeps up with the unnamed path at realistic rates — with measurements that fit in an afternoon.
It replaces the 420-cell campaign and its capacity search, and what it supports is limited to the shapes and rates it measures.

It measures 27 of the 420 pinned cells: Flink 2.2.1, 1 KiB bodies, the arms `UNNAMED`, `NAMED_HASH` and `STAGED_HASH`, three repetitions each, on three shapes at fixed offered rates.

| Shape | Parallelism, concurrency, checkpoint | Offered rate | Records per cell |
| --- | --- | ---: | ---: |
| `s1` | 1, 1, 1 s | 25 | 6,075 |
| `s2` | 4, 4, 10 s | 100 | 26,100 |
| `s3` | 16, 16, 60 s | 1,000 | 601,000 |

Each rate is one the third cluster attempt achieved at 100 % on the unnamed path at that shape (`k02`, `k04` and `k05`).
Warm-up and observation follow the protocol rule; the cells run in the pinned protocol's order.
The decision rule is ADR-0104's, unchanged: `STAGED_HASH` against `UNNAMED` per shape, at least 70 % of the throughput and at most twice the p95 for general support, at least 25 % and at most four times for constrained opt-in, and a throughput range above 10 % of the mean inconclusive.
At a fixed rate the throughput ratio says whether staged creation keeps up at that rate, not what its capacity is.
`NAMED_HASH` gives the incremental cost of staging against the same names.

## Calibration before the measurement

Three runs precede the 27, on the same host and in the same session:

| Cell | Purpose | Parallelism, concurrency, checkpoint | Offered rate | Passes when |
| --- | --- | --- | ---: | --- |
| `k01-pace-10` | pacing and target acceptance | 1, 1, 1 s | 10 | it achieves at least 95 % of its rate, and its window holds at least six checkpoints |
| `k03a-delay-latency` | the 100 ms serialization delay the analysis must detect, at a rate the delayed pipeline sustains | 1, 1, 1 s | 2 | it achieves at least 95 % of its rate and its per-repetition p95 is at least 100 ms |
| `k09-staged-gauges` | the staged gauges read in process (`STAGED_HASH`) | 4, 4, 10 s | 100 | the probe reports every staged gauge registered |

A failed calibration run stops the session before the measurement; the failure is recorded here and repaired before another attempt.

## Scale and ceiling

The 27 cells take 9,945 seconds of nominal span, about 2.8 hours, plus the calibration's 747 seconds and up to a few minutes per run for start and teardown.
They emit 5,698,575 records, about USD 2.30 of Cloud Tasks operations, and the host costs about USD 1 for four hours.
The planning bound — every subtask exhausting its attempt limit in each of four incarnations — is 34.2 million creations, USD 13.68; the paused queue and the per-run deadline keep a misbehaving run far below it.
The ceiling is USD 20 for the host and operations together.

## Result on the VM rig, 2026-09-24

The host `ct1246-vm-1` ran from 2026-09-23T14:40Z for about three and a half hours and was deleted, with no instance, disk or `ct1246-*` queue left.
The probe classpath was built from `32f346232d42578e713b27ae9907a724905c7624`.

Calibration passed every item.
`k01-pace-10` achieved 10.003 records per second with a p95 of 37 ms over a window of 182 checkpoints.
`k03a-delay-latency` achieved 2.0006 records per second with a p95 of 153 ms, about 115 ms above `k01`'s, so the delay is detected.
`k09-staged-gauges` reported all six staged gauges.
Each reconciled `complete`.

All 27 cells of the measurement were usable: observed, reconciled `complete`, no restart, and each window held at least six checkpoints.

| Shape | Offered rate | Arm | Mean throughput | Range over mean | Mean p95 |
| --- | ---: | --- | ---: | ---: | ---: |
| `s1` (1, 1, 1 s) | 25 | `UNNAMED` | 25.004 | 0.01 % | 60.9 ms |
| | | `NAMED_HASH` | 25.003 | 0.02 % | 51.5 ms |
| | | `STAGED_HASH` | 25.002 | 0.06 % | 783.9 ms |
| `s2` (4, 4, 10 s) | 100 | `UNNAMED` | 100.005 | 0.04 % | 27.7 ms |
| | | `NAMED_HASH` | 99.995 | 0.06 % | 33.0 ms |
| | | `STAGED_HASH` | 99.991 | 0.03 % | 9,022.4 ms |
| `s3` (16, 16, 60 s) | 1,000 | `UNNAMED` | 999.983 | 0.00 % | 31.5 ms |
| | | `NAMED_HASH` | 1,000.035 | 0.01 % | 33.1 ms |
| | | `STAGED_HASH` | 1,000.063 | 0.01 % | 53,721.1 ms |

Against `UNNAMED`, `STAGED_HASH`'s throughput ratio is 1.000 on every shape and its p95 ratio is 12.9, 326 and 1,706, so ADR-0104's rule, applied as written, labels every shape `decline` on p95 alone.
The staged p95 was 78 %, 90 % and 90 % of each shape's checkpoint interval, and it follows the interval by construction: a task is created only after the checkpoint that owns its envelope completes, so its visibility waits for most of an interval.
ADR-0162 records what the owner decided about that criterion.
At these rates staging kept up fully; capacity above them was not measured.

The run created 5,698,575 distinct tasks in the measurement, about USD 2.30 of operations, and the host cost about USD 1.
The campaign directories, analysis report and logs are retained by the owner with a per-file SHA-256 manifest; the collected archive's SHA-256 is `541d378092a68a416daaff2c8e8918264992ce9bc9ccd506915d837688f0e80d`, which matched on the host before deletion.

## Protocol interpretations the owner must confirm

The preregistered protocol leaves these points open; each is a decision, not a measurement, and the analysis code implements the reading below.

1. The capacity search runs on the `UNNAMED` arm of each shape, body and line, and the frozen rate applies to all five arms of that group.
   The reduced assessment above runs no capacity search, so this interpretation applies only if one is run later.
   Only evidence that the sink fell behind rejects a rate: achieving less than 95 % of the offered rate, or sustained backlog growth across checkpoint boundaries.
   A probe's achieved rate is the distinct creations completing between the end of warm-up and the source's last mapped record, divided by that span, not the steady-state window of interpretation 3: a saturated probe's checkpoints space out with its backlog, so it may hold too few of them for that window and still have fallen behind.
   An interrupted, restarted or unexported probe, and one that met its offered rate over a window holding too few checkpoint boundaries to evaluate the backlog, measured nothing about saturation and is repeated under a fresh `-qN` index rather than counted as a rejection.
   A rate offered twice without a measurement stalls the search for an operator instead of buying a third probe, and a search that accepts the protocol's highest offered rate of 10,000 records per second reports a ceiling at that rate.
   A group whose contributing arms did not all run at one offered rate is inconclusive, as is a cell whose executed arm, line, body, shape, window or channel pool differs from the entry its ID names.
2. A repetition whose evidence was lost is recorded and repeated at most once, with the suffix `-x2`; a second failure leaves the arm short and the shape inconclusive.
   On the VM rig evidence is lost when the run was lost with the campaign that held it, outlived its reservation, failed before its source started, or left evidence that reconciles incomplete; the repeat counts only when it was handed out after that loss.
   A restart, a failure after the source started and a window too short are the job's own behaviour on this host, so none of them is repeated.
   On the cluster the same rule named incomplete evidence and interrupted Spot Pods.
   A probe is repeated the same number of times, but under a fresh `-qN` index, because the probe index is the end of the identifier.
3. The observation window of a cell runs from the first completed ordinary checkpoint after warm-up to the last completed ordinary checkpoint before end of input and must hold at least six such checkpoints; a cell with a restart is never a steady-state result.
   On the VM rig the completions are the ones the probe records in its own wall clock, the clock of the source receipts that bound the window ([#1473](https://github.com/flink-gcp/flink-connector-gcp/issues/1473)); a cell without them has no window and no verdict, and no other window replaces it.
4. Per-repetition p95 uses rows with status `OK` and a nonnegative latency. Throughput counts distinct sequences whose origin lies in the window and whose eventual status is `OK`, or `ALREADY_EXISTS` on a named or staged arm, where the service deduplicated an attempt whose outcome the client never saw; the unnamed baseline has no such answer, so an `ALREADY_EXISTS` there is a defect rather than a record.
5. No averaging across lines or bodies; the repetition order is the pinned order.
6. One queue per run on the VM rig, named from the campaign and the run's order, created paused before the run and deleted after it; a name is never reused.
   Its depth is bounded by the run's attempt limit.
   The cluster sessions used one queue per session.
7. `ALREADY_EXISTS` on a named arm is explained only by an earlier attempt of the same sequence with the same name; otherwise the cell is invalid.
8. `https://ct1246.invalid/task` is the target; `k01` proves the service accepts it before any larger cell runs.
9. Calibration and the measurement run on Flink 2.2.1 only.
10. Recovery time and the replay budget a recovery consumes are not measured by the reduced design; [#1362](https://github.com/flink-gcp/flink-connector-gcp/issues/1362)'s deployed evidence is cited for failure behaviour, and #1245 settled recovery correctness.

## Cluster calibration, 2026-09-19 to 2026-09-20

The first three calibration attempts ran on the `flink-tier3` Autopilot cluster under the conditions below, and are recorded as they ran.
Their session ceilings, acceptance items and cost basis are the cluster's, and nothing in this section applies to the VM rig.

### Cluster execution conditions

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
| Pricing basis | the reviewed Autopilot rates in `policy.toml` and USD 0.40 per million Cloud Tasks operations; both are re-checked when the owner approves the estimate; since 2026-09-23 admission no longer refuses an older review (ADR-0165, spend is approved before dispatch) |

### Cluster calibration cells

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

The campaign of this file is therefore `calibration-1246c`, and it is now spent. The calibration that follows runs on the VM rig, under the sections above.

There is no record-count-mode pair: record-count mode prints its rows to standard output, which the session cannot capture, and receipt writes happen at most four times per creator incarnation outside the observation window, so their cost is bounded by construction and is read from the observed busy time rather than measured separately.
Higher offered rates are not calibrated in advance: the capacity search of the main assessment ramps each shape from a low rate and observes the instrument at every step.

### Cluster session ceilings

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
| Incremental cost | USD 8.66 at the planning bound; about USD 4.81 at the expected creation count | approved from the estimate; USD 10.00 until 2026-09-23 |

The Flink 1.20.4 repeat needs 4,110 s of plan, 236,800 planning-bound creations and USD 1.44 at its own window's lower bound of 5,010 seconds, USD 1.57 at the upper bound.
The planning bound assumes one JobMaster's restart budget; a JobManager failover resets it, so the cell deadline and the paused queue, not the bound, cap what a misbehaving cell can spend.

### Cluster acceptance criteria

Calibration passes when every item holds; a failure is repaired and this document is revised before any capacity claim.

- `k03` is detected: its unique-record throughput is at most 12 records per second and its per-repetition p95 is at least 100 ms, while `k02` achieves at least 95 % of its 25 records per second.
- `k01`, `k02`, `k04`, `k05` and `k06` achieve at least 95 % of their offered rates with no sustained backlog growth across checkpoint boundaries.
- The `k04` against `k07` pair reports the CSV output overhead in busy time, heap and achieved rate; the numbers parameterize the main approval and carry no threshold here.
- The connector gauges are discovered on `k09` and absent on `k04`, and `k10` produces 32-hex task names.
- Every cell except `interrupt-control-k11` is a usable steady-state result and reconciles `complete`: its job completed, it did not restart, its observation window holds at least six ordinary checkpoints, rows decoded from the parts equal `rows_exported` and the terminal counts, no unexplained duplicate, and the exported objects verify against their recorded hashes after download.
  The gauge and task-name items ask only that their cell's evidence was exported and verified, because neither reads a rate or a window.
- `interrupt-control-k11` reconciles `restarted`, because its own interruption makes the source register a second incarnation, with `missing-terminal` among its reasons; it is exported, and the analyzer refuses it as a steady-state result while still reporting its recovery timing.
- The session ends with the queue deleted, the benchmark prefixes released, the idle receipt and three empty refreshed plans; this item is the run's own receipts, not an analyzer check.
