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

# BigQuery TaskManager replacement and rescale trials: preregistration

This is the preregistration for the deployed BigQuery trials of [#1779](https://github.com/flink-gcp/flink-connector-gcp/issues/1779), which run the exactly-once Storage Write sink through a TaskManager replacement, a rescale out and a rescale in, within [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md).
It fixes the campaign, its estimate, the acceptance criteria, the stop conditions and the cleanup checks.
Nothing here authorizes a dispatch: the owner approves the campaign's estimate before the first trial, and each dispatch is approved on its own.

## Campaign

| Order | `trial` input | Upgrade | Failover | Writers and TaskManagers, initial then upgrade | Run ID |
| ---: | --- | --- | --- | --- | --- |
| 1 | `eo-10-tm-replacement` | savepoint upgrade | the TaskManager that reported the latest input | 2, 2 | `bq1779-eo-10-tm-replacement-a1` |
| 2 | `eo-10-rescale-out` | savepoint rescale out | JobManager | 1, 2 | `bq1779-eo-10-rescale-out-a1` |
| 3 | `eo-10-rescale-in` | savepoint rescale in | JobManager | 2, 1 | `bq1779-eo-10-rescale-in-a1` |

Each trial runs the `eo-10` cell, EO at 10 destinations with the scenario's 1,843,200 rows of 1,024 bytes over 30 minutes, with one of the shared exercise's fault kinds in place of one of its two recoveries, as the [BigQuery runbook](../../../kubernetes/apps/bigquery/README.md#offline-execution-proposal) lists them.
Together with the savepoint upgrade and JobManager failover that `bq1312-eo-10-a1` ran, as its [findings](0165-bigquery-trial-findings-1312.md) record, they give the exactly-once sink the five faults the Tier-3 measurement set [aligned on 2026-10-13](https://github.com/flink-gcp/flink-connector-gcp/issues/1360#issuecomment-6133736842) asks of every connector.
The question they answer is whether the writers' pending buffered streams and the committer's committables are restored correctly when the TaskManager holding them is lost and when a savepoint restores them into more or fewer writers: no deployed trial has exercised either.

The owner decided on 2026-10-13 that the code offers the faults for the `eo-10` and `fl-10` cells and that this preregistration chooses which run, and on 2026-10-14 that only the three `eo-10` trials run, so this campaign makes no claim about FILE_LOADS under a TaskManager replacement or a rescale.

The first trial is the pilot.
The others run only once it has reached a `usable` verdict and verified idle, because a first deployed run of a new fault kind is where the application, the rendered manifests and the exercise disagree if they are going to.
The order puts the replacement first because it changes no manifest size, and the two rescales after it.
The run IDs follow `bq1779-<trial>-a<N>`, and a repeat takes the next attempt suffix, under the rules of the [Storage Write preregistration](0165-bigquery-trial-preregistration-1312.md) for burned run IDs, final receipts and mispaired dispatches.
Every admitted attempt is listed in the findings, finished or not.

## Estimate

Each trial is estimated at USD 2.35 by `bigquery_plan.estimate`, the figure every rendered proposal carries, and the three come to USD 7.05.
It charges the five Pods a run keeps for the whole 90-minute window, the Operator, the supervisor, one JobManager and two TaskManagers (about USD 1.06); all twelve 4 GiB query reservations at USD 6.25/TiB (about USD 0.29); and USD 1.00 of other incremental cost.
A rescale runs one TaskManager in one of its phases and is still charged two, so it is an estimate, not a billing cap; existing cluster standing charges are outside it.
A repeat adds USD 2.35 and is approved before its dispatch like any other.

## Campaign window

Every image a trial pulls (`operator`, `lifecycle-tools` for the supervisor and `bigquery-recovery`) comes from the [fault trial publication](../../../kubernetes/images/README.md#bigquery-fault-trial-publication), which built main `25260e678` after [#1782](https://github.com/flink-gcp/flink-connector-gcp/issues/1782) merged.
Its `bigquery-recovery` image is the first that accepts the `--parallelism` argument every BigQuery delivery renders since #1782, so an earlier image would fail at job start.
The oldest of the three images, the `operator` mirror, was created at 2026-10-14T12:17:04Z (UTC; `gcloud` prints local time unless asked), and dispatch refuses an image unless the run's window ends a day before its seven-day deletion eligibility, so every attempt's window, which ends 90 minutes after admission, must end before 2026-10-20T12:17:04Z.
The three trials take about six hours of serial dispatch; the [#1778](https://github.com/flink-gcp/flink-connector-gcp/issues/1778) Cloud Tasks trials take the same environment lock, so the two campaigns interleave rather than overlap.

## Before dispatch

Each attempt is dispatched only after these are read, in this order, immediately before it:

- `pins.cue` at the dispatched `main` commit names `lifecycle-tools@sha256:4f5ec406125a5eccddf8e95bd5f1a6cc355d7609f7322fdb4539434e5a003c2c`.
- The `bigquery-recovery` digest named as `application_digest` is the one the [fault trial publication](../../../kubernetes/images/README.md#bigquery-fault-trial-publication) lists, and it is still in the registry.
- The environment lock `_control/environment.json` and every `_control/runs/` record are absent, and every earlier `approval.json` has its `result.json`.
- The window, admission plus 90 minutes, ends before the deadline under [Campaign window](#campaign-window).

## Acceptance criteria

Each trial is accepted as a measurement when three reads agree:

- Its final receipt and `just tier3-analyze` over its exported evidence agree on a `usable` verdict: both recoveries proved, the query oracle passed with zero duplicate, missing and invalid rows, and the families the [Storage Write preregistration](0165-bigquery-trial-preregistration-1312.md#acceptance-criteria-for-the-instrument) names read in the baseline window and again after the second recovery.
- The verdict does not check which fault ran, so the fault is read from the evidence: the receipt's `bigquery_trial.fault` is the trial's, and the analyzer's timing table names that fault for the recovery it replaced and `savepoint-upgrade` or `jm-replacement` for the other. A mispaired dispatch that ran a trial without a fault is `usable` and fails this read.
- The fault's proof, as the [shared recovery exercise](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#shared-recovery-exercise-fault-kinds) defines it, is in the completion record's `outcomes`. A rescale's upgrade proof records the checkpointed subtasks before and after, which for this graph are 2 and 3 out and 3 and 2 in: the input vertex and one subtask per writer. A replacement's failover proof records the deleted TaskManager, and the run's `inventory` events show the other TaskManager with the same UID before and after the failover, so only the one approved was lost.

The issue asks for more than the verdict reads:

- Each recovery's duration from Flink's restore and checkpoint timestamps, which `just tier3-analyze` reports for every proved recovery as [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#recovery-timing-from-flinks-timestamps) defines it, published beside the earlier trials' re-read figures.
- The measurement families in the baseline and after the second recovery, published per trial.
  The rescale-out trial's baseline runs one writer and its post-recovery window two, and the rescale-in trial the reverse, so a figure is compared within its trial, and against `bq1312-eo-10-a1` only where the writers match.
- The [Cleanup checks](#cleanup-checks) passing.

Measured values, durations included, are findings to publish, not thresholds.

## Stop conditions

The verdict is `usable` or `inconclusive`, and an attempt that stops early is `inconclusive` too, so the cause decides what follows.
It is read from the `cleanup-start` reason in the run's events, the recovery records, the receipt, the `inventory` and `scheduling-events` events, and the JobManager's exported `pod-log`.
Two causes have no event of their own: a TaskManager Pod the quota refused never exists, so its trace is the JobManager log's `Failed requesting worker` warning, which Flink 2.2.1 logs before it retries; and a Spot node's reclamation may leave no Kubernetes event, so it is read from Compute Engine's `compute.instances.preempted` audit entry for the node.
A container restart stops the run on the poll that sees it, before that poll's `inventory` is exported, so its cause is not in the evidence: it is read from the Pod's container and node records in Cloud Logging, and a restart whose cause cannot be read there is treated as the application's, not the platform's.

The conditions are taken in this order, and the first that matches decides what happens to the trial, pilot or not; the environment conditions after them apply in addition.

- The supervisor is preempted before application admission: the attempt spent no BigQuery query and burned only its run ID; redispatch it as the next attempt once approved.
- The oracle reports a routing violation, a duplicate, or rows still missing once every query slot or the visibility phase is spent: a connector finding, recorded rather than repeated.
- A recovery deadline expires, or the input finishes before the post-recovery window, and the last `inventory` before `cleanup-start` shows a TaskManager Pod not Running, or the JobManager log shows a refused worker request in that recovery: the TaskManager waited for a node or for the quota a terminating Pod still held, since the namespace has no replacement slot and a rescale out schedules one more Pod than the initial phase freed. Repeat once; a second such result leaves that trial inconclusive, and the owner decides whether the quota or the recovery deadline changes.
- A proof fails with "JobManager was replaced during the TaskManager replacement", or any stage ends with "Unplanned workload interruption; trial is inconclusive", and the reads above attribute it to the platform (a node drained or reclaimed): repeat once; a second such result leaves that trial inconclusive.
- A rescale's proof fails with "Rescale did not move the checkpointed subtasks as approved": the restored job contradicted the approved size; stop and read the evidence before another attempt.
- The pilot is not `usable` for any other reason: stop the campaign, repair what it exposed in its own change, revise this document and approve again before any other trial.
- Any other `inconclusive` attempt: stop, repair what it exposed in its own change, and approve again before another attempt.

The environment conditions, in addition:

- The attempt does not reach verified idle, a refreshed plan is not empty, or the environment lock is not released: stop, and recover the environment before any further dispatch.
- `just tier3-analyze` gives an attempt whose exercise recorded a stage no verdict (counted `unreadable`; the Storage Write record calls this `unexported`): download the run's whole evidence prefix again and re-analyze. A `missing-result.json` problem that persists means finalization never ran, so recover the environment; any other problem that persists is an instrument defect, so stop. Repeating the attempt repairs neither.
  An attempt that stopped before its exercise recorded any stage, such as one whose supervisor was preempted before admission, has no recovery record, so the analyzer reports `missing-recovery-complete` however often it is downloaded; the conditions above decide it.
- An image reaches its retention margin: stop; the remaining trials need a republication first.

## Cleanup checks

The rig refuses to finish until verified idle, as the [Storage Write preregistration](0165-bigquery-trial-preregistration-1312.md#cleanup-checks) describes.
After each trial, and before the next is dispatched, these are read directly:

- No table named for the attempt's run ID (`bq_<run_id>_d<n>`, hyphens as underscores) exists in `flink_gcp_tier3_bigquery`.
- No object remains under `gs://flink-gcp-tier3-bigquery/runs/<run-id>/` or `gs://flink-gcp-tier3-bigquery/.inprogress/flink-gcp-tier3-bigquery/runs/<run-id>/`, the two prefixes the rig cleans.
- No object of the run remains in `tier3-system`, and the `_control/runs/<run-id>.json` record and `_control/environment.json` lock are gone.

## Interpretations the owner must confirm

1. Each of the three trials runs at one attempt, the first a pilot; variance across repetitions is not claimed.
2. Instrument acceptance is the `usable` verdict together with the fault and proof reads under [Acceptance criteria](#acceptance-criteria); the recovery durations, the measurement families and the cleanup checks are published beside it, and a measured value is never a pass or fail criterion.
3. A duplicate is a defect, as for every EO trial.
4. A deadline or input shortfall while a TaskManager Pod was not Running, or while a worker request was refused, is an environment cause; one with every Pod Running is not, and is read as the job's own failure to recover.
5. The findings, including every inconclusive or failed attempt, are published in a record beside this one, `0165-bigquery-fault-trials-findings-1779.md`.
