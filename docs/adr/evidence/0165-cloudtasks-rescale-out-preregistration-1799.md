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

# Cloud Tasks rescale out rerun: preregistration

This is the preregistration for the rerun of the Cloud Tasks `rescale-out` trial under [#1799](https://github.com/flink-gcp/flink-connector-gcp/issues/1799), which takes the staged `EXACTLY_ONCE` Cloud Tasks sink through a savepoint rescale out and a JobManager failover, within [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md).
It fixes the trial, its estimate, the acceptance criteria, the stop conditions and the cleanup checks, in the shape of the [#1778 fault trial preregistration](0165-cloudtasks-fault-trials-preregistration-1778.md).
It states its own rules in full and takes from that record only what it links to by section; that record's pilot gate and campaign order have no counterpart in a single trial.
Nothing here authorizes a dispatch: the owner approves the estimate before the first attempt, and each dispatch is approved on its own.

## Why the trial is rerun

The [#1778 findings](0165-cloudtasks-fault-trials-findings-1778.md) record six `rescale-out` attempts without a listing.
Three stopped on the platform: Spot reclamations of a TaskManager's node in `ct1778-rescale-out-a1` and `-a5`, and a `konnectivity-agent` Pod of system priority preempting the JobManager in `-a2`.
The supervisor of `-a3` was preempted before its queue was created.
The other two stopped on causes since repaired:

- In `-a4`, a single `INTERNAL` from `CreateTask` failed the staged commit; [#1794](https://github.com/flink-gcp/flink-connector-gcp/issues/1794) made `INTERNAL` transient.
- In `-a6`, a single `CANCELLED` failed the staged commit and restarted the job on the same Pods, which the exercise did not detect, and the run then ended on a Kubernetes read refused with HTTP 429. [#1796](https://github.com/flink-gcp/flink-connector-gcp/issues/1796) made `CANCELLED` transient, [#1797](https://github.com/flink-gcp/flink-connector-gcp/issues/1797) made the rig retry a 429 read, and [#1798](https://github.com/flink-gcp/flink-connector-gcp/issues/1798) made the exercise stop on a job restart it did not plan.

None of the three changes addresses the platform causes, so the attempt budget below allows for them.

## Trial

| `cloudtasks_trial` input | Upgrade | Pod deleted | Sink subtasks and TaskManagers, initial then upgrade | Run ID |
| --- | --- | --- | --- | --- |
| `rescale-out` | savepoint rescale out | JobManager | 1, 2 | `ct1799-rescale-out-a1` |

The trial has the input of `ct-1362-a1` and of the #1778 trials, 180,000 records at 100 records per second, which is 30 minutes of input in a 90-minute window, with checkpoints every 30 seconds and the sink's default staged options, as the [Cloud Tasks runbook](../../../kubernetes/apps/cloudtasks/README.md#trial-and-estimate) lists it.
The queue is created and paused for the run, so no task is dispatched and no handler runs, and the oracle maps each listed task id back to its input sequence through the SHA-256 of `<run-id>/<sequence>`.
With the TaskManager replacement and the rescale in that #1778 brought to `usable`, and the savepoint upgrade and JobManager failover of `ct-1362-a1`, a `usable` rescale out completes the five faults the Tier-3 measurement set [aligned on 2026-10-13](https://github.com/flink-gcp/flink-connector-gcp/issues/1360#issuecomment-6133736842) asks of every connector.

The question is the one #1778 asked of a rescale: whether the writer's staged envelopes and the committer's pending commits are restored correctly when a savepoint restores them into more sink subtasks.
The oracle answers it only for envelopes whose commit had not finished when a fault struck.
A savepoint upgrade is a stop-with-savepoint, and the old job reaches `FINISHED` only after the committer has created the savepoint's tasks, so every envelope the rescale restores already exists as a task, and the listing would be complete even if the restore dropped some of them.
The rescale's restore is therefore read from the deduplication counter, `tasksDeduplicated`, in the `upgraded` window, set against the records the savepoint held beyond the checkpoint before it.
The savepoint's own `cloudtasks-snapshot` line is not in the exported Pod logs, so that count is read, as the [#1778 findings](0165-cloudtasks-fault-trials-findings-1778.md#deduplication-counter) read it, from the upgraded job's first progress line.
The counter counts every create the service refused as existing, a retry after a lost response included, and carries no checkpoint identity, so a matching count is consistent with a whole restore without proving it, and only a shortfall says something.
The JobManager failover afterwards restores a checkpoint at two sink subtasks, whose commits may still have been in flight, and that the oracle does test.

Run IDs follow `ct1799-rescale-out-a<N>`, and a repeat takes the next suffix, as the [runbook's dispatch rules](../../../kubernetes/apps/cloudtasks/README.md#dispatch) require; the run's queue is `ct1362-<run-id>`.
Every dispatched attempt is listed in the findings, finished or not.

## Estimate and attempt budget

Each attempt is estimated at USD 2.13 (`2.1307355`) by `cloudtasks.plan.estimate`, the figure the rendered proposal carries; rendering the `rescale-out` proposal at main `12a12db4e` gave the same figure and the same 186,370 task operations as the [#1778 estimate](0165-cloudtasks-fault-trials-preregistration-1778.md#estimate), whose breakdown stands.

The trial takes at most four **counted attempts**: those whose exported events include `cloudtasks-queue-admitted`, which the runner emits once the queue reads back `PAUSED` and before it creates the FlinkDeployment, so an attempt that never reached it cannot have run the application.
Four counted attempts come to USD 8.52 on the estimate's basis.
An attempt that stops before that event, for example because its supervisor was preempted, is not counted; it holds only the supervisor and the Operator for minutes, but the #1778 findings charged each such attempt the full estimate, so on that basis the total exceeds USD 8.52 by USD 2.13 for each.
The four allow for the platform: in #1778, three of the five `rescale-out` attempts that admitted a queue stopped on it.
If that share held, about one rerun in eight would still end without a `usable` attempt after four counted attempts, and the owner accepted that on 2026-10-17 rather than a larger budget.
Each repeat is approved before its dispatch, except a redispatch after a supervisor preempted before the queue was created, which the owner approved in advance on 2026-10-17 for this rerun as for #1778.

## Image and window

The trial pulls a `cloudtasks-recovery` image that carries #1796, which the images of the [#1778 publications](../../../kubernetes/images/README.md#cloud-tasks-internal-retry-publication) do not.
It is published from `main` after this record is fixed, and its digest is recorded as the [Cloud Tasks CANCELLED retry publication](../../../kubernetes/images/README.md#cloud-tasks-cancelled-retry-publication) before the first dispatch, which names it as `application_digest`.
The `operator` mirror the Operator runs and the `lifecycle-tools` image for the supervisor stay those of the [BigQuery fault trial publication](../../../kubernetes/images/README.md#bigquery-fault-trial-publication), because the lifecycle image's inputs are unchanged, as a read before each dispatch confirms; the rig's own code, including the changes of #1797 and #1798, reaches the supervisor in the run's delivery rather than in the image.

The `operator` mirror was created at 2026-10-14T12:17:04Z, and a publication copies it without renewing its creation time while the registry still holds that digest.
Dispatch refuses an image unless the run's window ends a day before its seven-day deletion eligibility, so every attempt's window, which ends 90 minutes after admission, must end before 2026-10-20T12:17:04Z.
The owner chose on 2026-10-17 to keep the mirror rather than delete it by hand for a fresh window.
If the trial has no `usable` attempt when no window can end before that time, it stops; another attempt needs a republication and a revision of this record that names the new images and deadline and says how many counted attempts remain, approved by the owner before any dispatch.

## Before dispatch

Each attempt is dispatched only after these are read, in this order, immediately before it:

- The `main` commit the workflow will run contains #1797's and #1798's merges, `fd39079a4` and `4644db4ec`, and, from the second attempt on, #1828's, `917dac357`; its `pins.cue` names `lifecycle-tools@sha256:4f5ec406125a5eccddf8e95bd5f1a6cc355d7609f7322fdb4539434e5a003c2c`, which is still in the registry; and `git diff 25260e678 <commit> -- kubernetes/images/lifecycle uv.lock pyproject.toml tools/tier3/pyproject.toml` is empty, so that image still matches the rig's dependencies.
- The `cloudtasks-recovery` digest named as `application_digest` is the one the [Cloud Tasks CANCELLED retry publication](../../../kubernetes/images/README.md#cloud-tasks-cancelled-retry-publication) lists, built from a commit that contains #1796's merge, `d042b096c`, and it is still in the registry.
- The environment lock `_control/environment.json` and every `_control/runs/` record are absent from `gs://flink-gcp-tier3-evidence`, every earlier `approval.json` has its `result.json`, and no other `tier3-run` workflow run is queued or in progress.
- `runs/<run-id>/` is absent from the evidence bucket, and no queue named `ct1362-<run-id>` exists in `flink-gcp/us-central1`.
- The custom roles read back as [#1768](https://github.com/flink-gcp/flink-connector-gcp/issues/1768) applied them, each bound only to its service account, as the [#1778 preregistration lists them](0165-cloudtasks-fault-trials-preregistration-1778.md#before-dispatch).
- The window, admission plus 90 minutes, ends before the deadline under [Image and window](#image-and-window).

## Acceptance criteria

The trial is accepted as a measurement when three reads of one counted attempt agree:

- Its final receipt and `just tier3-analyze` over its exported evidence agree on a `usable` verdict: both recoveries proved, the queue read back `PAUSED` before the listing, the oracle passed over a whole listing with all 180,000 sequences listed, no foreign task and no dispatched task, and each of the three measurement windows read every family the [runbook](../../../kubernetes/apps/cloudtasks/README.md#exercise-and-oracle) names.
- The receipt's `cloudtasks_trial.kind` is `rescale-out`, and the analyzer's timing table names `rescale-out` and `jm-replacement`.
- The completion record's `outcomes` hold the upgrade's rescale proof with the checkpointed subtasks moving from 2 to 3, the input vertex and one subtask per sink subtask.

The issue asks for more than the verdict reads, as #1778 did:

- Each recovery's duration from Flink's restore and checkpoint timestamps, published beside the #1778 trials', and the window in which the committer's replay of the restored snapshot began, which `just tier3-analyze` reports where an exported `checkpoints` poll saw the restored snapshot and the restored job's first checkpoint; it saw neither for any upgrade in #1778, so an unreadable upgrade window is reported as such.
- The measurement families in the three windows, with the deduplication counter read after each recovery, set against the savepoint's records for the rescale as above and the restored checkpoint's for the failover, and any replay-budget gauge a sample caught. The baseline runs one sink subtask and the later windows two, so a figure is compared within the trial, and against another trial only where the subtasks match.
- The listing attempts, their page counts and durations, and the queue readbacks.
- The [Cleanup checks](#cleanup-checks) passing.

Measured values, durations included, are findings to publish, not thresholds.
A deduplication counter that falls short of the savepoint's records is the exception: it is the one reading that could show the rescale dropped an envelope, so it is reported as a connector finding, and the owner decides what follows, although the verdict stands, since the listing could not have shown the loss.

## Stop conditions

The verdict is `usable` or `inconclusive`, an attempt that stops early is `inconclusive` too, and one that ends on a listing or state-read error stores no completion, which the analyzer reads as `inconclusive`, so the cause decides what follows.
The cause is read from the `cleanup-start` reason in the run's events, the recovery records, the `cloudtasks-*`, `inventory`, `scheduling-events` and `unplanned-restore` events, the receipt, and the JobManager's exported `pod-log`.
Three kinds of cause may leave no trace in the evidence and are read outside it: a Spot node's reclamation from Compute Engine's `compute.instances.preempted` audit entry for the node, a Pod's preemptor from the Kubernetes audit log, and a container restart from the Pod's container and node records in Cloud Logging.
A **platform cause** is one of those reads, or a Kubernetes event in the evidence, showing a node carrying the job's Pods reclaimed or drained, or one of the job's Pods preempted by a Pod of higher priority, at or before the failure it explains; a cause that cannot be read is treated as the application's, not the platform's.

The conditions are taken in this order, and the first that matches decides what happens; the environment conditions after them apply in addition.

- The supervisor is preempted before the queue is created: the attempt is not counted; redispatch it as the next attempt.
- The queue's name is not free, or a create is answered with 409 after the name read free: the run fails, deleting nothing, and is not counted; redispatch under the next run ID once approved.
- The run ends before the queue is created for another reason, such as "Cloud Tasks startup deadline expired" while the Operator or the supervisor was not yet ready: the attempt is not counted; redispatch under the next run ID once approved, unless the cause needs a repair first.
- A queue create did not return, or was answered with a status that leaves its call marker (a 5xx, 408 or 429): neither the release nor cleanup proceeds, since the create could still land. Stop; the owner decides how the queue and the environment are recovered.
- The queue never reads back `PAUSED` with the approved configuration (`cloudtasks-queue-deviation`), or the listing the completion record names reports a foreign task, a dispatched task or the queue not `PAUSED`: a rig or platform finding, recorded; stop.
- The listing the completion record names reports a sequence listed twice: a listing artifact, a rig finding; record it and stop. A missing sequence in the same listing is attributed only after its pages have been read.
- The listing the completion record names, a whole listing with no sequence listed twice, reports a missing sequence: a connector finding, recorded rather than repeated. Earlier listing attempts, which the rig lists again while the service's listing may still lag, are published as findings and decide nothing.
- The input finishes before the `finishing` window has run its five minutes, or before that window opened, read from the stage's own message, from "Finished without both recovery proofs and the complete input", or from `cloudtasks-progress` lines reporting all 180,000 records processed: the input pauses while the job recovers, so this is a timing or rig defect whatever the Pods show; stop and repair it before another attempt.
- "Recovery exercise deadline expired" ends the run while the input was still incomplete, and either the last `inventory` before `cleanup-start` shows a Pod of the job not Running, or fewer TaskManager Pods than the phase runs, or a platform cause falls within the stage that expired: the job was still waiting for a node or for the quota, since the namespace has no replacement slot and the rescale out schedules one more TaskManager than the initial phase freed, or the platform took a Pod it needed. An exported `inventory` describes the Pods as the poll read them, which can be earlier than the deadline, since a poll may cross the deadline after that read, so a platform cause anywhere in the stage counts, not only one after the inventory, and a Pod the inventory shows not Running counts even if it started before the deadline. Repeat once within the budget; a second such result stops the trial, and the owner decides whether the quota or the deadline changes. With every Pod Running in that inventory and no platform cause in the stage, the deadline is the job's own failure to recover, and takes the last condition.
- Any stage ends with "Unplanned workload interruption; trial is inconclusive", "Unplanned workload replacement; trial is inconclusive", "Unexpected fresh start or unplanned initial recovery" or "Unplanned job restart; trial is inconclusive": with a platform cause, repeat within the budget. Otherwise, if the JobManager log names a failure the Cloud Tasks sink raised, such as a commit that failed on a status the sink does not retry, it is a connector finding: record it and stop, and the owner decides whether it is repaired before another attempt. Any other cause takes the last condition.
- The supervisor is preempted after the queue is created: recover the environment, then redispatch under the next run ID once approved, within the budget.
- The upgrade's proof fails with "Rescale did not move the checkpointed subtasks as approved": the restored job contradicted the approved size; stop and read the evidence.
- The receipt says `usable` but the attempt fails a read under [Acceptance criteria](#acceptance-criteria): it is not accepted. A kind other than `rescale-out` is a dispatch error, so redispatch under the next run ID once approved, within the budget. A subtask count other than 2 and 3 is a finding, and an analyzer that recomputes another verdict or a timing table without the two fault names is an instrument finding; either way, stop and read the evidence.
- Any other `inconclusive` attempt, including one that ends on a listing or state-read error after the rig's retries: stop, repair what it exposed in its own change, and approve again before another attempt.

A repeat the conditions allow is dispatched only while the budget has a counted attempt left; when the fourth counted attempt is not `usable`, the rescale out is inconclusive, and the owner decides what follows.

The environment conditions, in addition:

- The attempt does not reach verified idle, a refreshed plan is not empty, or the environment lock is not released: stop, and recover the environment before any further dispatch.
- `just tier3-analyze` gives an attempt whose exercise recorded a stage no verdict, for example because a page or reading is missing from the export: download the run's whole evidence prefix again and re-analyze. A `missing-result.json` problem that persists means finalization never ran, so recover the environment; any other problem that persists is an instrument defect, so stop. Repeating the attempt repairs neither. An attempt that stopped before its exercise recorded any stage, such as one whose supervisor was preempted before admission, has no recovery record, so the analyzer reports `missing-recovery-complete` however often it is downloaded; the conditions above decide it.
- An image reaches its retention margin: stop, as [Image and window](#image-and-window) says.

## Cleanup checks

The rig refuses to finish until verified idle; the supervisor deletes the queue only after the runner's release and an empty namespace, reads it back until it is gone, and shared cleanup removes the run's state.
After each attempt, and before the next is dispatched, these are read directly:

- `gcloud tasks queues describe ct1362-<run-id> --location us-central1` reports the queue not found, and no `ct1362-` queue is listed.
- No object remains under `gs://flink-gcp-cloudtasks-benchmark/runs/<run-id>/` or under `.inprogress/flink-gcp-cloudtasks-benchmark/runs/<run-id>/` in the same bucket.
- The `tier3-cloudtasks` namespace holds no Pod and no FlinkDeployment, and its `tier3-idle` quota is back to its idle values.
- No object of the run remains in `tier3-system`, and the `_control/runs/<run-id>.json` record and `_control/environment.json` lock are gone.
- The `flink-gcp`, `tier3-bootstrap` and `tier3-operator` plans the receipt records as refreshed after the attempt are empty.

## Interpretations the owner must confirm

1. The trial reaches its verdict on one `usable` attempt within four counted attempts; variance across repetitions is not claimed.
2. Instrument acceptance is the `usable` verdict together with the kind and proof reads under [Acceptance criteria](#acceptance-criteria); the recovery durations, the replay windows, the measurement families and the cleanup checks are published beside it, and a measured value is never a pass or fail criterion, except that a deduplication shortfall is reported as a connector finding.
3. A missing task in the listing the completion record names is a connector defect; a replayed create the service refuses as existing is not, and a task listed twice is the listing's artifact, since the service holds one task per name.
4. A recovery deadline that expires while a Pod of the job is not Running or missing, as the last exported `inventory` shows, or after a platform cause in the stage that expired, is an environment cause; one with every Pod Running and no platform cause in that stage is not, and is read as the job's own failure. Input that finishes before the last window is never an environment cause.
5. A job failure the sink raised is a connector finding even when the attempt is otherwise healthy, since #1794 and #1796 were found that way; one with a platform cause is an environment cause, and one whose cause cannot be read is the application's.
6. The findings, including every inconclusive attempt, are published in a record beside this one, `0165-cloudtasks-rescale-out-findings-1799.md`, which extends the [#1778 findings](0165-cloudtasks-fault-trials-findings-1778.md#coverage-of-1362) for the rescale out.

## Revisions after the first dispatch

This record was fixed at commit `6431a1aed` and posted on [#1362](https://github.com/flink-gcp/flink-connector-gcp/issues/1362#issuecomment-6232675119) before `ct1799-rescale-out-a1` was dispatched at 2026-10-17T10:18Z.

- `ct1799-rescale-out-a1`, the first counted attempt, proved the rescale out and deleted its JobManager, then stopped at 10:40:50Z when one read of its control record timed out (`GET storage request failed: ReadTimeout`) and the supervisor recorded an evidence failure: a state-read error, which the last stop condition says to repair before another attempt. [#1827](https://github.com/flink-gcp/flink-connector-gcp/issues/1827) made the rig repeat a GCS read that meets a transient failure, and its merge, `917dac357`, joined the before-dispatch read. The rig reaches the supervisor in the run's delivery, so the images stay those of [Image and window](#image-and-window). Three counted attempts remain.
- At the owner's request, the deadline correction this record took from its independent review before it was fixed, which counts a platform cause anywhere in the expired stage because the inventory is read before a poll may cross the deadline, was applied to the [#1778 preregistration](0165-cloudtasks-fault-trials-preregistration-1778.md#revisions-after-the-pilots-dispatch) as well; this record's conditions are unchanged by it.
