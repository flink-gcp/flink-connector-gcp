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

# Cloud Tasks deployed recovery trial: findings

This records the deployed Cloud Tasks trial of [#1770](https://github.com/flink-gcp/flink-connector-gcp/issues/1770), part of [#1362](https://github.com/flink-gcp/flink-connector-gcp/issues/1362), run against its [preregistration](https://github.com/flink-gcp/flink-connector-gcp/issues/1362#issuecomment-6127161854) within [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md).
The one preregistered trial reached a `usable` verdict on its first attempt on 2026-10-12: the staged `EXACTLY_ONCE` sink, deployed as a FlinkDeployment, turned each of 180,000 input records into exactly one task in a paused queue through a savepoint upgrade and a JobManager failover.
Times are UTC.

## Result

| Trial | Attempt | Run | Rig commit | Verdict | Records | Listed | Missing | Repeated | Foreign | Dispatched |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `cloudtasks-recovery` | `ct-1362-a1` | [38269699659](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38269699659) | `6670ef72d` | `usable` | 180,000 | 180,000 | 0 | 0 | 0 | 0 |

At 15:20, immediately before the dispatch at 15:21:59, no environment lock and no run record existed, no evidence existed for the run ID, no queue existed in `flink-gcp/us-central1`, the `operator`, `lifecycle-tools` and `cloudtasks-recovery` images were in GAR at their recorded digests, and no other run of the workflow was queued or in progress.
The three custom roles had read back, shortly before the preregistration was posted at 15:17, with the permissions [#1768](https://github.com/flink-gcp/flink-connector-gcp/issues/1768) applied, each bound only to its service account.
The final receipt records the `usable` verdict with no reasons, the queue released and cleaned, verified idle, and empty plans for `flink-gcp`, `tier3-bootstrap` and `tier3-operator`, read at 16:08:09.
`just tier3-analyze` over the exported evidence, 966 events, recomputed `usable` from the exported listing pages, with the same report and no reasons or problems.
The receipt and the analyzer agree on `usable` and every cleanup check below passed, so the attempt is accepted under the preregistration.

The approval bound the application, operator and supervisor images; the application image `cloudtasks-recovery@sha256:cc66ba84…` of the [Cloud Tasks recovery trial publication](../../../kubernetes/images/README.md#cloud-tasks-recovery-trial-publication), created on 2026-10-12, runs Flink 2.2.1, as the JobManager log reports.
The window, from 15:22:42 to 16:52:42, ended inside the 2026-10-15T16:22:01Z deadline; the run finished at 16:08:35, 46 minutes into the window and 47 minutes after dispatch.

### Timeline

| Time | Event |
| --- | --- |
| 15:21:59 | Dispatch |
| 15:22:42 | The window opens |
| 15:26:21 | Queue `ct1362-ct-1362-a1` reads back `PAUSED` with the approved configuration on the first read |
| 15:26:29 | The FlinkDeployment starts |
| 15:30:50 | Checkpoint 1 completes, its source snapshot at 920 records |
| 15:31:14 | Savepoint upgrade starts, with the input at 3,000 records |
| 15:31:57 | The upgraded job restores savepoint 2 |
| 15:32:45 | Checkpoint 3 completes, its source snapshot at 5,990 records, and the input reaches 6,000; the upgrade is proved |
| 15:33:05 | JobManager failover starts |
| 15:33:31 | The replacement JobManager restores checkpoint 3 |
| 15:34:26 | Checkpoint 5 completes, its source snapshot at 11,369 records |
| 15:34:31 | The input reaches 12,000; the failover is proved, recorded at 15:34:37 |
| 16:02:31 | The input reports all 180,000 records processed, and the job reaches `FINISHED` at 16:02:33 |
| 16:02:48 | The listing starts |
| 16:03:20 | The listing's last page is exported; the run completes with `usable` at 16:03:21 |
| 16:03:53 | The supervisor's cleanup reports the run's state clean |
| 16:04:58 | The runner's settlement cleanup, from 16:04:07, reports it clean too |
| 16:05:35 | Verified idle |

### Recovery

Both recoveries stayed on the input lineage the first progress line reported; all 64 progress lines carry it.
Each was proved by a restored savepoint or checkpoint, input advanced past the restore and a new completed checkpoint, as the shared exercise requires.
After the failover the job resumed at sequence 5,990, the position checkpoint 3 held.
During the upgrade the supervisor recorded one `recovery-rest-unavailable`, an HTTP 404 reading the REST Service while the job was redeployed, which the exercise tolerates while a recovery runs, and one `retired-pod-log-unavailable` for the initial job's TaskManager, which the supervisor tolerates for a Pod already gone; neither recurred.
That TaskManager's log after 15:31:10 is not in the evidence, so the savepoint's own snapshot line is missing; the upgraded job's first progress line, `restored=true` at sequence 3,472, implies the savepoint held 3,472 records, since the input logs the first record it maps after a restore and accepts only the sequence its state names.

### Task oracle

| Listing attempt | Pages | Started | Last page | Queue state | Listed | Matched | Missing | Repeated | Foreign | Dispatched |
| ---: | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 181 | 16:02:48 | 16:03:20 | `PAUSED` | 180,000 | 180,000 | 0 | 0 | 0 | 0 |

The first listing passed, so no second attempt was made and the 600-second listing window was not approached.
Its 181 pages are 180 of 1,000 tasks and a final empty page, which ends the listing.
Every listed task id is the SHA-256 of `<run-id>/<sequence>` for one input sequence, and no task reported a dispatch attempt.

The trial does not count how many creates the recoveries replayed, or how many of them the service refused as existing: the sink's metrics are not read, and the exported logs do not report them.
A create the service refused could not appear in the listing twice, so the oracle reads the outcome that matters to the trial's question, that every record became exactly one task.

### Cleanup

The receipt records the queue cleaned; the supervisor deletes it and reads it back until it is gone before state cleanup, and no event records those calls.
Read with the owner's credentials after the run:

- `gcloud tasks queues describe ct1362-ct-1362-a1` reports `NOT_FOUND`, and no queue is listed in `flink-gcp/us-central1`.
- No object remains under `gs://flink-gcp-cloudtasks-benchmark/runs/ct-1362-a1/` or under `.inprogress/flink-gcp-cloudtasks-benchmark/runs/ct-1362-a1/`.
- The `tier3-cloudtasks` namespace holds no Pod and no FlinkDeployment, and its `tier3-idle` quota is back to zero Pods and zero claims.
- `tier3-system` holds no object of the run, the Operator deployment is at zero replicas, and neither `_control/runs/ct-1362-a1.json` nor `_control/environment.json` exists.
- A local refreshed `flink-gcp` plan found no differences.

## Every admitted attempt

`ct-1362-a1` is the only attempt; no other was admitted, and no dispatch was refused before admission.

## Deviations from the preregistration

- The preregistration asks for the role read-back immediately before the dispatch; it was taken shortly before 15:17, with the preregistration's own reads, and not repeated at 15:20. No change to IAM was applied in between.
- The preregistration's cleanup checks ask for the three plans refreshed after the attempt: `flink-gcp` was refreshed locally, and the `tier3-bootstrap` and `tier3-operator` plans are the receipt's, read by the rig at 16:08:09.
- The estimate was USD 2.13 for the 90-minute window; total charges were not read from billing.

## Coverage of #1362

[#1362](https://github.com/flink-gcp/flink-connector-gcp/issues/1362) asks for a map linking the reused evidence of [#1245](https://github.com/flink-gcp/flink-connector-gcp/issues/1245) and [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246) and this deployed result.
The [scope decided on 2026-10-12](https://github.com/flink-gcp/flink-connector-gcp/issues/1362#issuecomment-6120349111) narrowed the issue to this trial and set the rest aside, each addable later as its own trial.
The evidence reused is #1245's [recovery protocol](0158-cloudtasks-recovery-1245.md) and #1246's [assessment](0162-cloudtasks-assessment-1246.md).

| #1362 acceptance item | Where it stands |
| --- | --- |
| Reuse the completed smoke, the Cloud Tasks KSA and GSA, CUE delivery, image publication, external state, admission and cleanup, without another lease framework | This trial: it ran on the shared lifecycle the smoke proved, under the environment lock; [#1767](https://github.com/flink-gcp/flink-connector-gcp/issues/1767) to [#1769](https://github.com/flink-gcp/flink-connector-gcp/issues/1769) added only the application and its image publication, per-run queue grants, the delivery and the scenario |
| Freeze source and image digests, Flink and API coverage | This trial: the receipt binds the rig commit and the approval the image digests; the application image runs Flink 2.2.1, and the [publication record](../../../kubernetes/images/README.md#cloud-tasks-recovery-trial-publication) names its source commit |
| DataStream entry point with native workload credentials and checkpoint-coordinated named-task creation | This trial |
| Table entry point | Set aside on the rig; #1245's recovery protocol covers Table-mode recovery with controlled oracles on Flink 1.20.4 and 2.2.1 |
| Eager at-least-once baseline; persisted random names | Set aside on the rig; #1245's recovery protocol covers staged recovery with persisted random names on both lines, and #1246's assessment measured the staged sink's throughput and latency against the unnamed and named at-least-once arms on a single VM, without recovery, so nothing covers eager duplicates under recovery |
| A completed external checkpoint, then a JobManager replacement, with recovery and full input reconciliation | This trial: checkpoints 3 and 5 under the run's external checkpoint path, and all 180,000 sequences listed once |
| TaskManager replacement | Set aside |
| Rescale in and out | Set aside on the rig; #1245's partial-commit cases restore committer parallelism 2 to 1 and 2 to 3 under controlled oracles on both lines |
| A successful savepoint upgrade with stable sink UIDs, the same queue, retained state and bounded restarts | This trial: the upgrade restored savepoint 2 into the same queue under the application's UIDs `ct1362-source-v1`, `ct1362-input-v1` and `ct1362-sink-v1`; the exercise treats any container restart or disruption, and any Pod replacement outside a recovery, as inconclusive and a failed job as a failure; a second replacement during a recovery that no disruption marks is not detected, and nothing suggests one here |
| No simultaneous jobs replaying the same state | The environment lock and the owner's single dispatch, as the scope records |
| Staged creation keeps task names across recovery, read back in a paused queue | This trial: every task name derives from its sequence, and the paused queue lists each once and none dispatched |
| The latest authoritative checkpoint or savepoint preserved; no silent restart from an older checkpoint | This trial: the upgrade must restore the savepoint it took, the failover a checkpoint no older than the last before the fault, and the input refuses missing or foreign state |
| Payload, authorization deadline and original creation generation preserved across recovery | #1245's recovery protocol: its lost-response cases restore identical named task bytes, its old-incarnation case completes under the original absolute deadline, and its creation oracle verifies original creation generations through task readback; this trial lists names in the basic view only and claims none of these |
| The post-savepoint drain failure of the recovery runbook | Set aside |
| Queue pause state and retention revalidated before admission and during the run | This trial, in part: the runner read the queue back `PAUSED` with a one-hour tombstone retention before admission, and the supervisor read it `PAUSED` again before listing; the pause state was not read while the job ran, and zero dispatch counts are the evidence it held. Each sink committer reads the retention back when it is created (the default `verifyQueueRetention`), and no committer creation failed; no event records those reads |
| Deliberate replay kept distinct from fresh creation; eager duplicates reported separately; no handler claim | Replay and generation: #1245's recovery protocol; eager mode set aside; no handler ran, since a paused queue dispatches nothing |
| Run, Pod, job, checkpoint and savepoint identities, fault boundaries, reconciliation and recovery metrics exported, with external timestamps; incomplete evidence withholds a verdict | This trial, in part: the receipt and 966 evidence events carry the identities, fault boundaries, reconciliation and Flink's checkpoint and restore statistics, timed by Pod log lines and the REST API rather than JVM clocks, and the analyzer withholds a verdict for a missing listing page; the connector's recovery metrics, including the remaining replay budget, are not read |
| Preregistered and approved resource, attempt, byte, duration and cost ceilings, restart bounds and stop conditions; paused queues; ownership before cleanup; absence, Operator zero replicas, idle quotas and empty plans verified | This trial: the preregistration and the owner's approval fix the attempt, the 90-minute window and the stop conditions, and its estimate is not a ceiling; no byte ceiling or restart bound was set beyond the exercise's own. The cleanup checks above verify the rest |

## What this does not establish

The trial shows one run at one input rate on Flink 2.2.1; it claims no variance across repetitions, no throughput limit and nothing about Flink 1.20.
It shows no handler behavior, because the queue never dispatched.
It does not measure recovery time or the replay budget a recovery consumes, which #1246's assessment did not measure either and [ADR-0162](../0162-cloud-tasks-implementation-precedes-final-performance-acceptance.md) cites deployed evidence for; the timeline gives restore times, not a measurement.
The faults it injected are the two the shared exercise runs, each at a point the exercise chose after a completed checkpoint; a TaskManager loss, a rescale, the Table entry point, the eager at-least-once mode and a drain failure after a stop-with-savepoint remain unexercised on the rig, and the controlled oracles of [#1245](https://github.com/flink-gcp/flink-connector-gcp/issues/1245) remain the evidence for lost create responses and creation generations.
