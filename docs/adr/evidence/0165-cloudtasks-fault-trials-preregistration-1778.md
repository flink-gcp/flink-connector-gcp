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

# Cloud Tasks TaskManager replacement and rescale trials: preregistration

This is the preregistration for the deployed Cloud Tasks trials of [#1778](https://github.com/flink-gcp/flink-connector-gcp/issues/1778), which run the staged `EXACTLY_ONCE` Cloud Tasks sink through a TaskManager replacement, a rescale out and a rescale in, within [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md).
It fixes the campaign, its estimate, the acceptance criteria, the stop conditions and the cleanup checks.
Nothing here authorizes a dispatch: the owner approves the campaign's estimate before the first trial, and each dispatch is approved on its own.

## Campaign

| Order | `cloudtasks_trial` input | Upgrade | Pod deleted | Sink subtasks and TaskManagers, initial then upgrade | Run ID |
| ---: | --- | --- | --- | --- | --- |
| 1 | `tm-replacement` | savepoint upgrade | the TaskManager that reported the latest input | 2, 2 | `ct1778-tm-replacement-a1` |
| 2 | `rescale-out` | savepoint rescale out | JobManager | 1, 2 | `ct1778-rescale-out-a1` |
| 3 | `rescale-in` | savepoint rescale in | JobManager | 2, 1 | `ct1778-rescale-in-a1` |

Each trial has the input of `ct-1362-a1`, 180,000 records at 100 records per second, which is 30 minutes of input in a 90-minute window, with checkpoints every 30 seconds and the sink's default staged options, and takes one of the shared exercise's fault kinds in place of one of its two recoveries, as the [Cloud Tasks runbook](../../../kubernetes/apps/cloudtasks/README.md#trial-and-estimate) lists them.
The queue is created and paused for the run, so no task is dispatched and no handler runs, and the oracle maps each listed task id back to its input sequence through the SHA-256 of `<run-id>/<sequence>`.
Together with the savepoint upgrade and JobManager failover that `ct-1362-a1` ran, as its [findings](0165-cloudtasks-recovery-findings-1362.md) record, the three trials give the staged sink the five faults the Tier-3 measurement set [aligned on 2026-10-13](https://github.com/flink-gcp/flink-connector-gcp/issues/1360#issuecomment-6133736842) asks of every connector.
The question they answer is whether the writers' staged envelopes and the committer's pending commits are restored correctly when the TaskManager holding them is lost, and when a savepoint restores them into more or fewer sink subtasks.
The rescale in is the restore [ADR-0162](../0162-cloud-tasks-implementation-precedes-final-performance-acceptance.md) names, one that funnels two subtasks' envelopes through one commit loop.
No deployed trial has exercised any of the three.

The oracle answers that question only for envelopes whose commit had not finished when the fault struck.
A savepoint upgrade is a stop-with-savepoint, and the old job reaches `FINISHED` only after the committer has created the savepoint's tasks, as [ADR-0158](../0158-cloud-tasks-checkpointed-creation-stages-named-tasks-and-commits-after-the-checkpoint.md) records, so every envelope a rescale restores already exists as a task, and the listing would be complete even if the restore dropped some of them.
For a rescale, the restore is therefore read from the deduplication counter in the `upgraded` window, set against the `cloudtasks-snapshot` lines the application logs for the savepoint, as a finding, not part of the verdict.
The counter counts every create the service refused as existing, a retry after a lost response included, and carries no checkpoint identity, so a count matching the savepoint's records is consistent with a whole restore without proving it, and a shortfall is the reading that says something.
The JobManager failover each rescale trial runs afterwards restores a checkpoint at the new sink parallelism, whose commits may still have been in flight, and that the oracle does test.
The TaskManager replacement tests restored pending commits only if the deletion lands before the restored checkpoint's commit has finished, so the findings say whether the evidence shows that it did: the deduplication counter after the failover, set against the restored checkpoint's records, falls short either because some restored envelopes were still uncreated or because the restore dropped some, which the oracle then catches only in the first case, and for the reason above a matching count leaves the question open; the findings name every reading the evidence leaves.

The first trial is the pilot.
The others run only once it has reached a `usable` verdict, passed the fault and proof reads under [Acceptance criteria](#acceptance-criteria) and verified idle, because a first deployed run of a new fault kind is where the application, the rendered manifests and the exercise disagree if they are going to.
The order puts the replacement first because it changes no manifest size, and the two rescales after it, as the [BigQuery fault trials](0165-bigquery-fault-trials-preregistration-1779.md) did.
The run IDs follow `ct1778-<trial>-a<N>`, and a repeat takes the next attempt suffix, as the [runbook's dispatch rules](../../../kubernetes/apps/cloudtasks/README.md#dispatch) require; the run's queue is `ct1362-<run-id>`.
Every admitted attempt is listed in the findings, finished or not.

## Estimate

Each trial is estimated at USD 2.13 (`2.1307355`) by `cloudtasks.plan.estimate`, the figure every rendered proposal carries, and the three come to USD 6.39.
Rendering the three proposals at main `3546b6ed8` gave the same figure for each kind, made up of:

- USD 1.06 for the Operator, the supervisor and the three application Pods over the whole 90-minute window at the reviewed Autopilot rates.
- USD 0.07 for 186,370 task operations at USD 0.40 per million, without the monthly free tier: 180,000 creates, 6,000 records re-created for the two recoveries (one checkpoint's records each, refused as existing), 360 listing pages for two whole listings, and 10 queue calls.
- USD 1.00 reserve for state, evidence and telemetry.

A rescale runs one TaskManager in one of its phases and is still charged two, so it is an estimate, not a billing cap; existing cluster standing charges are outside it.
A repeat adds USD 2.13 and is approved before its dispatch like any other.

## Campaign window

The trials pull `cloudtasks-recovery` from the [Cloud Tasks fault trial publication](../../../kubernetes/images/README.md#cloud-tasks-fault-trial-publication), which built main `3546b6ed8` after [#1777](https://github.com/flink-gcp/flink-connector-gcp/issues/1777) and [#1788](https://github.com/flink-gcp/flink-connector-gcp/issues/1788) merged, and the `operator` mirror the Operator runs and `lifecycle-tools` for the supervisor from the [BigQuery fault trial publication](../../../kubernetes/images/README.md#bigquery-fault-trial-publication).
The new `cloudtasks-recovery` image, `sha256:0d6c75675aedf383eb8c0056f44bbf87f0e58bc7305c8285b5a1d3daa9d56f9e`, which every dispatch names as `application_digest`, is the first that accepts the `--sink-parallelism` argument every Cloud Tasks delivery renders since #1777, so an earlier image would fail at job start.
The `lifecycle-tools` pin stays, for the reason the publication records, and the rig's own code reaches the supervisor in the run's delivery, not in the image.
The oldest of the three images, the `operator` mirror, was created at 2026-10-14T12:17:04Z (UTC; `gcloud` prints local time unless asked), and the Cloud Tasks publication did not renew it, because the registry still held that digest.
Dispatch refuses an image unless the run's window ends a day before its seven-day deletion eligibility, so every attempt's window, which ends 90 minutes after admission, must end before 2026-10-20T12:17:04Z.
The three trials take about six hours of serial dispatch, under the environment lock every Tier-3 trial shares.

## Before dispatch

Each attempt is dispatched only after these are read, in this order, immediately before it:

- `pins.cue` at the dispatched `main` commit names `lifecycle-tools@sha256:4f5ec406125a5eccddf8e95bd5f1a6cc355d7609f7322fdb4539434e5a003c2c`, the [BigQuery fault trial publication](../../../kubernetes/images/README.md#bigquery-fault-trial-publication)'s, which is still in the registry.
- The `cloudtasks-recovery` digest named as `application_digest` is the one the [Cloud Tasks fault trial publication](../../../kubernetes/images/README.md#cloud-tasks-fault-trial-publication) lists, and it is still in the registry.
- The environment lock `_control/environment.json` and every `_control/runs/` record are absent from `gs://flink-gcp-tier3-evidence`, and every earlier `approval.json` has its `result.json`, and no other `tier3-run` workflow run is queued or in progress.
- `runs/<run-id>/` is absent from the evidence bucket, and no queue named `ct1362-<run-id>` exists in `flink-gcp/us-central1`.
- The custom roles read back as [#1768](https://github.com/flink-gcp/flink-connector-gcp/issues/1768) applied them, each bound only to its service account: `tier3CloudTasksRecoveryWorkload` (`cloudtasks.queues.get`, `cloudtasks.tasks.create`) to `cloudtasks-benchmark`, `tier3CloudTasksRecoveryRunner` (`cloudtasks.queues.create`, `cloudtasks.queues.get`, `cloudtasks.queues.pause`) to `tier3-runner`, and `tier3CloudTasksRecoverySupervisor` (`cloudtasks.queues.delete`, `cloudtasks.queues.get`, `cloudtasks.tasks.list`) to `tier3-supervisor`.
- The window, admission plus 90 minutes, ends before the deadline under [Campaign window](#campaign-window).

## Acceptance criteria

Each trial is accepted as a measurement when three reads agree:

- Its final receipt and `just tier3-analyze` over its exported evidence agree on a `usable` verdict: both recoveries proved, the queue read back `PAUSED` before the listing, the oracle passed over a whole listing with all 180,000 sequences listed, no foreign task and no dispatched task, and each of the three measurement windows read every family the [runbook](../../../kubernetes/apps/cloudtasks/README.md#exercise-and-oracle) names.
- The verdict does not check which fault ran, so the fault is read from the evidence: the receipt's `cloudtasks_trial.kind` is the trial's, and the analyzer's timing table names that fault for the recovery it replaced and `savepoint-upgrade` or `jm-replacement` for the other. A mispaired dispatch is `usable` under another kind and fails this read.
- The fault's proof, as the [shared recovery exercise](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#shared-recovery-exercise-fault-kinds) defines it, is in the completion record's `outcomes`. A rescale's upgrade proof records the checkpointed subtasks before and after, which for this graph are 2 and 3 out and 3 and 2 in: the input vertex and one subtask per sink subtask, as `ct-1362-a1`'s checkpoints reported 3 at sink parallelism two. A replacement's failover proof records the deleted TaskManager, and the run's `inventory` events show the other TaskManager with the same UID before and after the failover, so only the one approved was lost; the rig itself does not check this.

The issue asks for more than the verdict reads:

- Each recovery's duration from Flink's restore and checkpoint timestamps, and the window in which the committer's replay of the restored snapshot began, both of which `just tier3-analyze` reports for every proved recovery as [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#cloud-tasks-fault-kinds-and-measurement-windows) defines them, published beside `ct-1362-a1`'s.
- The measurement families in the three windows, published per trial, with the deduplication counter read after each recovery, set against the restored savepoint's records for a rescale, and any replay-budget gauge a sample caught.
  The rescale-out trial's baseline runs one sink subtask and its later windows two, and the rescale-in trial the reverse, so a figure is compared within its trial, and against another trial only where the subtasks match.
- The listing attempts, their page counts and durations, and the queue readbacks.
- The [Cleanup checks](#cleanup-checks) passing.

Measured values, durations included, are findings to publish, not thresholds.

## Stop conditions

The verdict is `usable` or `inconclusive`, an attempt that stops early is `inconclusive` too, and one that ends on a listing or state-read error stores no completion, which the analyzer reads as `inconclusive`, so the cause decides what follows.
It is read from the `cleanup-start` reason in the run's events, the recovery records, the `cloudtasks-*` events, the receipt, the `inventory` and `scheduling-events` events, and the JobManager's exported `pod-log`.
Two causes have no event of their own: a TaskManager Pod the quota refused never exists, so its trace is the JobManager log's `Failed requesting worker` warning, which Flink 2.2.1 logs before it retries; and a Spot node's reclamation may leave no Kubernetes event, so it is read from Compute Engine's `compute.instances.preempted` audit entry for the node.
A container restart stops the run on the poll that sees it, before that poll's `inventory` is exported, so its cause is not in the evidence: it is read from the Pod's container and node records in Cloud Logging, and a restart whose cause cannot be read there is treated as the application's, not the platform's.

The conditions are taken in this order, and the first that matches decides what happens to the trial, pilot or not; the environment conditions after them apply in addition.

- The supervisor is preempted before the queue is created: the attempt burned only its run ID; redispatch it as the next attempt once approved.
- The queue's name is not free: the run fails before it records a creation intent, deleting nothing. A create answered with 409 after the name read free clears the creation intent, and the run fails, deleting nothing; the 409 can be a deleted name's tombstone. Either way, redispatch under the next run ID once approved.
- A queue create did not return: the run keeps its call marker and neither the release nor cleanup proceeds, since the create could still land. Stop; the owner decides how the queue and the environment are recovered.
- The queue never reads back `PAUSED` with the approved configuration (`cloudtasks-queue-deviation`), or the oracle reports a foreign task, a dispatched task or the queue not `PAUSED`: a rig or platform finding, recorded; stop before another attempt.
- The oracle reports a sequence listed twice within one listing: the service refuses a second task of the same name, so this is a listing artifact, a rig finding; record it and stop before another attempt. A sequence missing from the same listing is attributed only after its pages have been read, since a listing that repeats one task may omit another.
- The oracle, over a whole listing with no sequence listed twice, reports a missing sequence: a connector finding, recorded rather than repeated, even if a later listing would differ.
- The input finishes before the post-recovery window, read from the stage's own message, from "Finished without both recovery proofs and the complete input", or from `cloudtasks-progress` lines reporting all 180,000 records processed before the `finishing` window had run its five minutes, or before that window opened at all: the input pauses while the job recovers, so time spent waiting for a Pod does not consume it, and this is a timing or rig defect whatever the Pods show; stop and repair it before another attempt. It comes before the deadline condition below because an upgraded job that finishes during the upgrade stage releases its TaskManagers and leaves that stage to its deadline.
- The `upgrade` or `failover` stage ends with "Recovery exercise deadline expired" while the input was still incomplete, and the last `inventory` before `cleanup-start` shows a TaskManager Pod not Running, or fewer TaskManager Pods than the phase runs: the TaskManager was still waiting for a node or for the quota when the deadline passed, since the namespace has no replacement slot and a rescale out schedules one more Pod than the initial phase freed. A worker request the JobManager log shows refused attributes nothing on its own, since a TaskManager replacement meets one while the deleted Pod terminates, and a later request can succeed. Repeat once; a second such result leaves that trial inconclusive, and the owner decides whether the quota or the recovery deadline changes.
- A proof fails with "JobManager was replaced during the TaskManager replacement", or any stage ends with "Unplanned workload interruption; trial is inconclusive", "Unplanned workload replacement; trial is inconclusive" or "Unexpected fresh start or unplanned initial recovery", and the reads above attribute it to the platform (a node drained or reclaimed): repeat once; a second such result leaves that trial inconclusive.
- The supervisor is preempted after the queue is created: recover the environment, then redispatch under the next run ID once approved.
- A rescale's proof fails with "Rescale did not move the checkpointed subtasks as approved": the restored job contradicted the approved size; stop and read the evidence before another attempt.
- The attempt is `usable` but fails the fault or proof reads under [Acceptance criteria](#acceptance-criteria): it is not accepted. A kind that differs from the run ID's is a dispatch error, so redispatch under the next run ID once approved; a subtask count other than the approved one, or a second TaskManager replaced, is a finding, so stop and read the evidence.
- The pilot is not `usable` for any other reason: stop the campaign, repair what it exposed in its own change, revise this document and approve again before any other trial.
- Any other `inconclusive` attempt, including one that ends on a listing or state-read error: stop, repair what it exposed in its own change, and approve again before another attempt.

The environment conditions, in addition:

- The attempt does not reach verified idle, a refreshed plan is not empty, or the environment lock is not released: stop, and recover the environment before any further dispatch.
- `just tier3-analyze` gives an attempt whose exercise recorded a stage no verdict, for example because a page or reading is missing from the export: download the run's whole evidence prefix again and re-analyze. A `missing-result.json` problem that persists means finalization never ran, so recover the environment; any other problem that persists is an instrument defect, so stop. Repeating the attempt repairs neither.
  An attempt that stopped before its exercise recorded any stage, such as one whose supervisor was preempted before admission, has no recovery record, so the analyzer reports `missing-recovery-complete` however often it is downloaded; the conditions above decide it.
- An image reaches its retention margin: stop; the remaining trials need a republication first.

## Cleanup checks

The rig refuses to finish until verified idle; for this scenario the supervisor deletes the queue only after the runner's release and an empty namespace, reads it back until it is gone, and shared cleanup removes the run's state.
After each trial, and before the next is dispatched, these are read directly:

- `gcloud tasks queues describe ct1362-<run-id> --location us-central1` reports the queue not found, and no `ct1362-` queue is listed.
- No object remains under `gs://flink-gcp-cloudtasks-benchmark/runs/<run-id>/` or under `.inprogress/flink-gcp-cloudtasks-benchmark/runs/<run-id>/` in the same bucket.
- The `tier3-cloudtasks` namespace holds no Pod and no FlinkDeployment, and its `tier3-idle` quota is back to its idle values.
- No object of the run remains in `tier3-system`, and the `_control/runs/<run-id>.json` record and `_control/environment.json` lock are gone.
- The `flink-gcp`, `tier3-bootstrap` and `tier3-operator` plans refreshed after the attempt are empty.

## Interpretations the owner must confirm

1. Each of the three trials runs at one attempt, the first a pilot; variance across repetitions is not claimed.
2. Instrument acceptance is the `usable` verdict together with the fault and proof reads under [Acceptance criteria](#acceptance-criteria); the recovery durations, the replay windows, the measurement families and the cleanup checks are published beside it, and a measured value is never a pass or fail criterion.
3. A missing task over a whole listing is a connector defect; a replayed create the service refuses as existing is not, and a task listed twice is the listing's artifact, since the service holds one task per name.
4. A recovery deadline that expires, with the input incomplete, while a TaskManager Pod is not Running or missing is an environment cause; one with every Pod Running is not, and is read as the job's own failure to recover. Input that finishes before the last window is never an environment cause.
5. The findings, including every inconclusive or failed attempt, are published in a record beside this one, `0165-cloudtasks-fault-trials-findings-1778.md`, which extends the [coverage of #1362](0165-cloudtasks-recovery-findings-1362.md#coverage-of-1362) with these trials.

## Revisions after the pilot's dispatch

This record was first fixed at commit `8cd2f0dac` and posted on [#1362](https://github.com/flink-gcp/flink-connector-gcp/issues/1362#issuecomment-6195790585) before the pilot was dispatched at 2026-10-15T15:29Z.
The first four revisions came from the review of this record while the pilot ran, and changed only how results are read, not what any trial runs, which main and the dispatch inputs fix.
The rest record the owner's decisions after later attempts stopped, and those change the attempts, the image and the order:

- The oracle's reach is stated: a rescale's restored envelopes are already committed by the stop-with-savepoint, so their restore is read from the deduplication counter as a finding, whose matching count proves nothing on its own.
- A sequence listed twice is a listing artifact rather than a connector defect, since the service holds one task per name, and it is classified before a missing sequence.
- A refused worker request no longer counts as an environment cause on its own, only a TaskManager still missing or not Running at the deadline, input that finishes early is a timing or rig defect and is classified before a deadline, and the deadline condition names the `upgrade` and `failover` stages.
- A `usable` attempt that fails the fault or proof reads, a supervisor preempted after the queue exists, and "Unplanned workload replacement" each have a condition, and the pilot gate requires the fault and proof reads.
- After `ct1778-rescale-out-a1` stopped with "Unexpected fresh start or unplanned initial recovery", because Compute Engine reclaimed its TaskManager's Spot node, sending the TaskManager SIGTERM about a second before the upgrade patch, and the job restored a checkpoint in its initial phase, that message joined the platform condition, which had not named it, so the attempt was repeated once as `ct1778-rescale-out-a2` with the owner's approval.
- `ct1778-rescale-out-a2` proved the rescale out and the JobManager failover, then stopped with "Unplanned workload interruption; trial is inconclusive" when a `kube-system` Pod of system priority, `konnectivity-agent`, preempted its JobManager a little over four minutes into the `finishing` window. That was the trial's second platform result, which this record leaves inconclusive; the owner approved a third attempt, `ct1778-rescale-out-a3`, as a deviation from this record. Its supervisor was preempted before the queue was created, so `ct1778-rescale-out-a4` was its redispatch under the first stop condition.
- `ct1778-rescale-out-a4` stopped with "Unexpected fresh start or unplanned initial recovery" because a single `INTERNAL` from `CreateTask` failed the staged commit, a connector cause, and the pilot had met the same failure after its last window. The owner paused the campaign for [#1794](https://github.com/flink-gcp/flink-connector-gcp/issues/1794), which made `INTERNAL` transient, and on 2026-10-16 approved running `ct1778-rescale-out-a5` and then `ct1778-rescale-in-a1` with the [Cloud Tasks INTERNAL retry publication](../../../kubernetes/images/README.md#cloud-tasks-internal-retry-publication)'s `cloudtasks-recovery` image, `sha256:e71790df17d5fe9383b1000cac6cf0e4fd007c17f7bef349ae4911a3a42ebfc0`, built from main `3a5832b88`; the pilot stays the result of the earlier image. Those two dispatches name that digest as `application_digest`, and the before-dispatch read checks it against that publication instead.
- `ct1778-rescale-out-a5` proved the rescale out, then stopped with "Unplanned workload replacement; trial is inconclusive" when Compute Engine preempted the Spot node both TaskManagers ran on during the `upgraded` window. The owner chose on 2026-10-16 to run the rescale in first and then one more `rescale-out` attempt, both as deviations recorded here. `ct1778-rescale-in-a1`'s supervisor was preempted before the queue was created, and `ct1778-rescale-in-a2` was its redispatch under the first stop condition, which the owner approved in advance for the rest of the campaign.
- `ct1778-rescale-out-a6` proved the rescale out; then a single `CANCELLED` from `CreateTask` restarted the job inside the `upgraded` window, which the exercise did not detect, and the run ended during the JobManager failover on a Kubernetes read refused with HTTP 429, a state-read error this record says to repair before another attempt. On 2026-10-17 the owner closed the rescale out without a verdict instead, and filed the causes as [#1796](https://github.com/flink-gcp/flink-connector-gcp/issues/1796), [#1797](https://github.com/flink-gcp/flink-connector-gcp/issues/1797) and [#1798](https://github.com/flink-gcp/flink-connector-gcp/issues/1798), with [#1799](https://github.com/flink-gcp/flink-connector-gcp/issues/1799) to rerun it.
