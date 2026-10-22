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

# Bigtable recovery trials: preregistration

This is the preregistration for the deployed Bigtable trials of [#1855](https://github.com/flink-gcp/flink-connector-gcp/issues/1855), which serve [#1316](https://github.com/flink-gcp/flink-connector-gcp/issues/1316) and [#1317](https://github.com/flink-gcp/flink-connector-gcp/issues/1317) within [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#bigtable-recovery-trial).
One job carries the `EXACTLY_ONCE` sink into `eo`, the `AT_LEAST_ONCE` sink into `alo` and the Change Streams relay from `eo` into `relay`, so each trial recovers all three components, as the [scope](https://github.com/flink-gcp/flink-connector-gcp/issues/1316#issuecomment-6247061185) decided.
This record fixes the campaign, its estimate, the acceptance criteria, the stop conditions and the cleanup checks.
The owner approved the campaign's spend on 2026-10-19.

## Campaign

| Order | `bigtable_trial` input | Upgrade | Pod deleted | Subtasks of every vertex but the input, initial then upgrade | Run ID |
| ---: | --- | --- | --- | --- | --- |
| 1 | `jm-replacement` | savepoint upgrade | JobManager | 2, 2 | `bt1855-jm-a1` |
| 2 | `tm-replacement` | savepoint upgrade | the TaskManager that reported the latest input | 2, 2 | `bt1855-tm-a1` |
| 3 | `rescale-out` | savepoint rescale out | JobManager | 1, 2 | `bt1855-ro-a1` |
| 4 | `rescale-in` | savepoint rescale in | JobManager | 2, 1 | `bt1855-ri-a1` |

Every trial has the same input, 180,000 records at 100 records per second with checkpoints every 30 seconds, and its change stream ends 3,900 seconds into its 90-minute window, as the [runbook](../../../kubernetes/apps/bigtable/README.md#trial-and-estimate) describes.
Each trial runs a savepoint upgrade and then deletes a Pod, so the four together give each component the five faults of the Tier-3 measurement set: a savepoint upgrade, a JobManager replacement, a TaskManager replacement, a rescale out and a rescale in.

The oracles answer different questions, and each reaches only part of a fault:

- **`eo`** shows that each record's contribution was applied once with one marker. A savepoint upgrade is a stop-with-savepoint, after which the staged committer has committed every envelope, so it tests a restored pending commit only for a fault that lands while a checkpoint's commit is still in flight, as the [Cloud Tasks fault trials](0165-cloudtasks-fault-trials-preregistration-1778.md#campaign) recorded for their staged sink.
- **`alo`** shows that every record arrived; its extra contributions count what the recoveries replayed and the client retried, without telling the two apart.
- **`relay`** shows that every change `eo` applied reached the relay at least once; its extra deliveries count what the source replayed after each restore and within a task instance.

The first trial is the pilot.
The others run only once it has reached a `usable` verdict and verified idle, because the pilot is the first deployed run of the application, the instance lifecycle and the oracles.
It also measures the two premises the [Bigtable recovery foundation](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#bigtable-recovery-foundation) leaves unmeasured: that the unconditional creator role lets the runner create a `bt1316-` instance, and that the instance-name conditions admit the workload's and the supervisor's data-plane calls.
The third premise there, the permission that polling the create's operation needs, no longer applies, since the runner polls the instance instead.
A repeat takes the next attempt suffix, and every admitted attempt is listed in the findings, finished or not.

## Estimate

Each trial is estimated at USD 3.36 (`3.3561875`) by `bigtable.plan.estimate`, the figure every rendered proposal carries, and the four come to USD 13.42:

- USD 1.06 for the Operator, the supervisor and the three application Pods over the whole window at the reviewed Autopilot rates, as for Cloud Tasks.
- USD 1.30 for two node-hours of the one-node SSD instance at USD 0.65, since the 90-minute window is billed as two hours.
- USD 1.00 reserve for storage, state, evidence and telemetry.

A rescale runs one TaskManager in one of its phases and is still charged two, so this is an estimate, not a billing cap.
A repeat adds USD 3.36 and is approved before its dispatch, except a redispatch after a supervisor preempted before the instance was created, which the owner approves in advance here, since that attempt created nothing.

## Campaign window

The trials pull `bigtable-recovery` from the [publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39202375380) of main `ed422edd4`, the first after the scenario merged: `sha256:f041f5486adbbe505d98ed112a7bf270f506863c5ad1cafefbcfbfac6cadf2bf`, created at 2026-10-19T05:19Z, which every dispatch names as `application_digest`.
The Operator runs the `operator` mirror and the supervisor runs the `lifecycle-tools` image that [pins.cue](../../../kubernetes/images/pins.cue) names, both created at 2026-10-14T12:17Z; the publication does not renew them, because the registry still holds those digests.
Dispatch refuses an image unless the run's window ends a day before its seven-day deletion eligibility, so every attempt's window must end before 2026-10-20T12:17Z.
Four trials take about eight hours of serial dispatch under the shared environment lock.

## Before dispatch

Each attempt is dispatched only after these are read, in this order, immediately before it:

- The `application_digest` is `sha256:f041f5486adbbe505d98ed112a7bf270f506863c5ad1cafefbcfbfac6cadf2bf`, and it is in the registry.
- `_control/environment.json` and every `_control/runs/` record are absent from `gs://flink-gcp-tier3-evidence`, every earlier `approval.json` has its `result.json`, and no other `tier3-run` workflow run is queued or in progress.
- `runs/<run-id>/` is absent from the evidence bucket, and no instance `bt1316-<run-id>` exists in `flink-gcp`.
- The window, admission plus 90 minutes, ends before the deadline under [Campaign window](#campaign-window).

## Acceptance criteria

Each trial is accepted as a measurement when its final receipt and `just tier3-analyze` over its exported evidence agree on a `usable` verdict: both recoveries proved, all three oracles passed, the relay's judged, and each measurement window read every family the [runbook](../../../kubernetes/apps/bigtable/README.md#exercise-and-oracles) names.
The fault is read from the evidence too, since the verdict does not check which ran: the receipt's `bigtable_trial.kind` is the trial's, the analyzer's timing table names it, and a rescale's upgrade proof records the checkpointed subtasks moving between 4 and 7, the input and three vertices at one or two subtasks.

The findings also publish, as findings rather than thresholds:

- each recovery's duration from Flink's restore and checkpoint timestamps, which `just tier3-analyze` reports;
- the measurement families per component and window;
- `alo`'s extra contributions and `relay`'s extra deliveries and replayed rows;
- the instance lifecycle's events and the [Cleanup checks](#cleanup-checks).

## Stop conditions

The conditions of the [Cloud Tasks fault trials](0165-cloudtasks-fault-trials-preregistration-1778.md#stop-conditions) apply with the instance in place of the queue, read from the same evidence, with these Bigtable-specific ones first:

- The runner's create is refused with 403, or the workload's or the supervisor's data-plane calls are refused: a grant finding. Stop the campaign; the grants are repaired in their own change before another attempt.
- The instance's name is not free, or the create is refused although it read free: the run deletes nothing; redispatch under the next run ID once approved.
- A create that did not return, or whose instance never read back before admission stopped, keeps its marker: stop; the owner decides how the instance and the environment are recovered.
- The table or app profile readback deviates (`bigtable-table-deviation`, `bigtable-app-profile-deviation`): a rig finding; stop before another attempt.
- `eo` reports a wrong SUM, a missing or second marker, or a missing row over a whole read, or `relay` misses a row `eo` applied: a connector finding, recorded rather than repeated.
- The relay is unjudged, because the input finished less than 120 seconds before the change stream's end: a timing finding; stop and read the evidence before another attempt.
- The job does not finish before the cleanup start, for example because a partition the connector was still reconciling held it past the end: a finding about the source's end condition; stop and read the evidence before another attempt.
- After cleanup, an instance `bt1316-<run-id>` still exists: stop the campaign and delete it by hand before anything else.

## Cleanup checks

After each trial, and before the next is dispatched, these are read directly:

- `gcloud bigtable instances describe bt1316-<run-id>` reports the instance not found.
- No object remains under `runs/<run-id>/` or `.inprogress/flink-gcp-tier3-bigtable/runs/<run-id>/` in `gs://flink-gcp-tier3-bigtable`.
- The `tier3-bigtable` namespace holds no Pod and no FlinkDeployment, and its `tier3-idle` quota is back to its idle values.
- The `_control/runs/<run-id>.json` record and `_control/environment.json` are gone, and the run workflow's refreshed plans are empty.

## Interpretations the owner must confirm

1. Each kind runs at one attempt, the first a pilot; variance across repetitions is not claimed.
2. Acceptance is the `usable` verdict with the fault read above; recovery durations, families and replay counts are published beside it, never pass or fail criteria.
3. A missing or doubled `eo` contribution or a missing relay delivery is a connector defect; `alo` extra contributions and relay replays are expected and counted.
4. The findings, including every inconclusive or failed attempt, are published beside this record as `0165-bigtable-recovery-findings-1855.md`, and that change closes #1316 and #1317.

## Revisions after the pilot's dispatch

This record was first fixed at commit `22e7e6ca1` and posted on [#1316](https://github.com/flink-gcp/flink-connector-gcp/issues/1316#issuecomment-6260274313) before the pilot was dispatched at 2026-10-19T05:21Z.
The pilot, `bt1855-jm-a1`, reached `usable`.
The revisions record the owner's decisions after later attempts stopped; they change the rig, the dispatch and the image deadline, not the application image, the input or what any trial runs:

- `bt1855-tm-a1` and `bt1855-tm-a2` stopped before the instance was created, because a `kube-system` Pod of system priority preempted their supervisor; each was redispatched under the advance approval in [Estimate](#estimate).
- `bt1855-tm-a3` stopped at admission when a table create got no response in time, and the run read nothing back. [#1867](https://github.com/flink-gcp/flink-connector-gcp/issues/1867) made the runner read back a create that did not answer or answered 409, and added a campaign dispatch: one dispatch runs the remaining kinds in order, repeats an attempt that the platform stopped before the application was created or interrupted afterwards, up to a number of repeats the dispatch names, and ends at any other stop. `bt1855-tm-a4`, `bt1855-tm-a5` and `bt1855-tm-a6` were each dispatched by hand as a campaign of the remaining kinds with two repeats, up to five attempts and USD 16.80, which the owner approved before each.
- `bt1855-tm-a4` stopped when an evidence write to the evidence bucket got no response in time; [#1869](https://github.com/flink-gcp/flink-connector-gcp/issues/1869) resolves an unanswered control or evidence write by reading it back.
- `bt1855-tm-a5` stopped with "Unplanned job restart; trial is inconclusive" during its planned failover: the lost TaskManager held two of the job's three failover regions, the sinks' and one of the relay's, and Flink restored the same checkpoint once for each. [#1871](https://github.com/flink-gcp/flink-connector-gcp/issues/1871) counts those restores as one recovery.
- The campaign dispatched on 2026-10-20 ran `bt1855-tm-a6`, `bt1855-ro-a1` and `bt1855-ri-a1`. The first two reached `usable`. `bt1855-ri-a1` stopped with "Recovery exercise deadline expired: upgrade", 300 seconds after its rescale in began, before a checkpoint after its restore completed, and the campaign ended there, since its dispatch repeats only a platform stop.
- Its last `inventory` before cleanup showed its one TaskManager Running and no platform cause fell within the stage, so the deadline condition does not apply, and it falls to "any other inconclusive attempt": stop, repair and approve again. The scheduling events show the cause the inventory could not: the new TaskManager was first refused at 02:55:58Z with two nodes unschedulable, which by the events are the two its predecessors had vacated, waited for a scale-up, was scheduled at 02:56:58Z, and had its tasks running at 02:59:13Z, 20.5 seconds before the deadline.
- [#1882](https://github.com/flink-gcp/flink-connector-gcp/issues/1882) raised the Bigtable recovery budget from 300 to 600 seconds, as Pub/Sub's already was. The budget is a ceiling, so it changes no usable trial's result. A slow recovery instead spends the margin between the input's end and the change stream's end, 1,250 to 1,485 seconds in the usable trials, and a trial past it leaves the relay unjudged, which the stop conditions already cover.
- By then the window deadline in [Campaign window](#campaign-window) had passed. On 2026-10-20 the owner approved deleting the `operator` and `flink` versions by hand, and the [Bigtable rescale-in rerun publication](../../../kubernetes/images/README.md#bigtable-rescale-in-rerun-publication) of main `5b505786e` recreated them; [#1884](https://github.com/flink-gcp/flink-connector-gcp/issues/1884) adopted its `lifecycle-tools` digest. The rerun keeps `application_digest` `sha256:f041f5486adbbe505d98ed112a7bf270f506863c5ad1cafefbcfbfac6cadf2bf`, so all four trials run one application image, and its window must end before 2026-10-25T05:19:20Z, a day before that image's eligibility.
- The owner approved `bt1855-ri-a2` on 2026-10-20, dispatched as a one-kind campaign with one repeat, so that with `bt1855-tm-a6`, `bt1855-ro-a1` and `bt1855-ri-a1` it stays within the five attempts of the campaign `bt1855-tm-a6` began; the before-dispatch reads apply with that deadline.
- `bt1855-ri-a2` proved the rescale in and the JobManager failover, then stopped with "Unplanned workload replacement; trial is inconclusive" 31 seconds after its change stream's end, before the oracles read. Compute Engine had reclaimed the Spot node its TaskManager ran on (`compute.instances.preempted`), and the node's graceful shutdown sent the TaskManager SIGTERM 27 seconds after the end. Flink's resource manager deleted that Pod and requested another within a second, before the supervisor's next poll could read the deleted Pod's status, and the node's `Preempted` and `NodeNotReady` events are outside the namespace the exercise reads, so the campaign could not attribute the stop to the platform and did not spend its repeat. The reads above attribute it to the platform, so under the replacement condition of the Cloud Tasks record the owner approved on 2026-10-22 its one repeat, `bt1855-ri-a3`, the fifth and last attempt of the campaign `bt1855-tm-a6` began.
