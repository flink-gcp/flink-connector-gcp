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

# Cloud Tasks TaskManager replacement and rescale trials: findings

These are the findings of the deployed Cloud Tasks trials of [#1778](https://github.com/flink-gcp/flink-connector-gcp/issues/1778), run under their [preregistration](0165-cloudtasks-fault-trials-preregistration-1778.md) between 2026-10-15 and 2026-10-17.
The staged `EXACTLY_ONCE` sink took a TaskManager replacement and a savepoint rescale in, each to a `usable` verdict on the deployed rig.
The rescale out reached no verdict in six attempts, and the owner ended its attempts in this campaign on 2026-10-17; [#1799](https://github.com/flink-gcp/flink-connector-gcp/issues/1799) reruns it.

## Result

| Trial | Run ID | Image | Verdict | Listed, missing, repeated, foreign, dispatched |
| --- | --- | --- | --- | --- |
| `tm-replacement` (pilot) | `ct1778-tm-replacement-a1` | [fault trial publication](../../../kubernetes/images/README.md#cloud-tasks-fault-trial-publication) (`3546b6ed8`) | `usable` | 180,000, 0, 0, 0, 0 |
| `rescale-in` | `ct1778-rescale-in-a2` | [INTERNAL retry publication](../../../kubernetes/images/README.md#cloud-tasks-internal-retry-publication) (`3a5832b88`) | `usable` | 180,000, 0, 0, 0, 0 |
| `rescale-out` | `ct1778-rescale-out-a1` to `-a6` | `3546b6ed8` for a1 to a4, `3a5832b88` for a5 and a6 | `inconclusive` six times | no listing |

For both `usable` runs the final receipt and `just tier3-analyze` agree, and the fault and proof reads under the preregistration's acceptance criteria pass:

- `ct1778-tm-replacement-a1`: the receipt's `cloudtasks_trial.kind` is `tm-replacement`, and the timing table names `savepoint-upgrade` and `tm-replacement`. The failover proof records the deleted `taskmanager-1-2`, whose Pod UID (`6d1b7466…`) is the one the latest upgraded progress line came from, and the `inventory` events show the other TaskManager, `taskmanager-1-1`, with UID `003a19a9…` before and after the failover, under one JobManager.
- `ct1778-rescale-in-a2`: the kind is `rescale-in`, and the timing table names `rescale-in` and `jm-replacement`. The upgrade proof records the checkpointed subtasks moving from 3 to 2, the input vertex and one per sink subtask.

Each read every family in its `baseline`, `upgraded` and `finishing` windows, four readings each.
Each read the queue back `PAUSED` and listed it in one attempt of 181 pages: from 16:10:27Z to 16:11:03Z for the pilot and from 23:25:50Z to 23:26:21Z for the rescale in, the listing stage's start to the completion record.

The pilot ran the image built before [#1794](https://github.com/flink-gcp/flink-connector-gcp/issues/1794) made `INTERNAL` transient, and the rescale in the image built after it; the preregistration's [revisions](0165-cloudtasks-fault-trials-preregistration-1778.md#revisions-after-the-pilots-dispatch) record why, and the owner chose on 2026-10-16 to keep the pilot as the earlier image's result.

### Recovery

Each recovery is timed from the supervisor's record of its stage, as [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#recovery-timing-from-flinks-timestamps) defines it.
`Restored after` is Flink's restore of the state, which precedes the tasks' deployment, so it is a lower bound; `Acknowledged after` is the last acknowledgement of the checkpoint that proved the restored job, which follows its first input past the fault, so it is an upper bound.
`ct-1362-a1`'s rows are its archived evidence re-read with the current analyzer.

| Run | Recovery | Fault | Restored from | Restored after | Acknowledged after |
| --- | --- | --- | --- | ---: | ---: |
| `ct-1362-a1` | upgrade | `savepoint-upgrade` | savepoint 2 | 43.5 s | 91.7 s |
| `ct-1362-a1` | failover | `jm-replacement` | checkpoint 3 | 25.9 s | 81.1 s |
| `ct1778-tm-replacement-a1` | upgrade | `savepoint-upgrade` | savepoint 15 | 42.8 s | 73.3 s |
| `ct1778-tm-replacement-a1` | failover | `tm-replacement` | checkpoint 27 | 10.8 s | 73.8 s |
| `ct1778-rescale-in-a2` | upgrade | `rescale-in` | savepoint 15 | 34.0 s | 78.1 s |
| `ct1778-rescale-in-a2` | failover | `jm-replacement` | checkpoint 27 | 25.0 s | 63.7 s |
| `ct1778-rescale-out-a2` | upgrade | `rescale-out` | savepoint 15 | 34.0 s | 88.3 s |
| `ct1778-rescale-out-a2` | failover | `jm-replacement` | checkpoint 26 | 34.4 s | 56.4 s |
| `ct1778-rescale-out-a5` | upgrade | `rescale-out` | savepoint 15 | 34.8 s | 102.5 s |
| `ct1778-rescale-out-a6` | upgrade | `rescale-out` | savepoint 15 | 41.5 s | 95.7 s |

The `rescale-out` rows are recoveries the exercise proved in attempts that later stopped, so they have no verdict; each upgrade proof records the checkpointed subtasks moving from 2 to 3.

The committer's replay of a restored checkpoint began within these windows after its last acknowledgement: 16.8 to 48.5 s for the pilot's failover, 30.7 to 68.4 s for the rescale in's, 54.8 to 76.2 s for `ct1778-rescale-out-a2`'s, and 45.2 to 69.5 s for `ct-1362-a1`'s.
The analyzer reports every upgrade's replay window unreadable, which it does when no exported `checkpoints` poll saw the restored snapshot or the restored job's first checkpoint.

#### TaskManager replacement

The supervisor deleted `taskmanager-1-2` at 15:52:32Z.
Within 15:52:33Z the `tier3-idle` quota refused the JobManager's Pods for `taskmanager-1-3`, `-1-4` and `-1-5`, while the deleted Pod still held its slot, and the JobManager logged `Failed requesting worker`; `taskmanager-1-6` was Running by the next poll, 17 seconds after the deletion, and the job was `RUNNING` again at 15:52:43Z.
These trials attribute no delay to the refusals, as the [BigQuery TaskManager replacement](0165-bigquery-fault-trials-findings-1779.md#taskmanager-replacement) did not.

#### Deduplication counter

The preregistration reads the restore from the deduplication counter, `tasksDeduplicated`, as a finding, where only a shortfall says something.

- Each upgrade's counter matched the records the savepoint held beyond the checkpoint before it.
  The savepoint's own `cloudtasks-snapshot` line is not in the exported Pod logs, so the count it held is read, as for `ct-1362-a1`, from the upgraded job's first progress line, which the input logs for the first record it maps after a restore: 43,677 in the pilot and 43,603 in the rescale in, against 41,711 and 42,257 at checkpoint 14.
  The 1,966 and 1,346 records that leaves are what the counter read after each upgrade, the latter on the one sink subtask the rescale in restored into.
  So the rescale in shows no shortfall: no evidence that its restore into one committer dropped an envelope, which the listing could not have shown.
- Each failover's counter matched the records of the checkpoint it restored: checkpoint 27's `cloudtasks-snapshot` lines account for 2,999 records in the pilot and 2,995 in the rescale in, and the counter read 2,999 and 2,995.
  A matching count is consistent with every restored envelope having already been created when the fault struck, and does not prove it; the export holds no record of when the commit of checkpoint 27 finished, so whether either failover restored a pending commit stays open.

Samples also caught the committer's `currentCommit` gauges during ordinary commits, as the runbook expects: a remaining replay budget of 3,246,722 to 3,248,344 ms and an oldest task age of 31.7 to 33.3 s.

### Measurement families

The largest value any reading in the window returned; they describe these runs at their fixed rate, and none is a threshold:

| Run | Window | Sink records in/s (sum) | Busy ms/s | Back-pressured ms/s | Sink bytes in/s (sum) | Buffer pool usage, in and out | Staged tasks | TaskManager heap MiB | Sink checkpoint end-to-end ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `ct1778-tm-replacement-a1` | `baseline` | 100.0 | 56 | 0 | 2,103 | 0, 0 | 1,308 | 216 | 870 |
| `ct1778-tm-replacement-a1` | `upgraded` | 100.1 | 75 | 0 | 2,104 | 0, 0 | 1,451 | 208 | 1,202 |
| `ct1778-tm-replacement-a1` | `finishing` | 100.0 | 72 | 0 | 2,103 | 0, 0 | 1,300 | 202 | 1,255 |
| `ct1778-rescale-in-a2` | `baseline` | 102.8 | 64 | 0 | 2,160 | 0, 0 | 1,029 | 210 | 838 |
| `ct1778-rescale-in-a2` | `upgraded` | 100.1 | 172 | 0 | 2,104 | 0, 0 | 2,014 | 198 | 997 |
| `ct1778-rescale-in-a2` | `finishing` | 111.1 | 177 | 0 | 2,334 | 0, 0 | 2,323 | 212 | 772 |

The rescale in's later windows run one sink subtask, which then carries the whole input: its busy time more than doubles and its staged batch grows to about twice the two-subtask figure, with no back-pressure and empty buffer pools.

## Observations

- **A single `INTERNAL` or `CANCELLED` from `CreateTask` restarted the job.**
  Before #1794 the staged committer treated `INTERNAL` as terminal: `ct1778-rescale-out-a4` lost its run to one at 02:01:06Z on 2026-10-16, and the pilot met one at 16:04:17Z on 2026-10-15, after its last window.
  After #1794, `ct1778-rescale-out-a6` met a single `CANCELLED` at 00:01:45Z on 2026-10-17, which the committer still treats as terminal; [#1796](https://github.com/flink-gcp/flink-connector-gcp/issues/1796) records it.
  Each restart recovered from the retained checkpoint, which is what the sink documents.
- **A job restart on the same Pods is invisible to the verdict.**
  The exercise reads interruptions from Kubernetes only, so the pilot's restart after its windows left its verdict `usable`, and `ct1778-rescale-out-a6`'s restart inside its `upgraded` window did not stop the run; [#1798](https://github.com/flink-gcp/flink-connector-gcp/issues/1798) records the gap.
  The pilot listed every sequence once, so its restart does not touch its oracle result; the rescale in's JobManager log shows no restart, and neither does the exported log of `ct-1362-a1`.
- **The platform interrupted most attempts.**
  Compute Engine reclaimed a Spot node carrying TaskManagers twice (`ct1778-rescale-out-a1` and `ct1778-rescale-out-a5`), and a `kube-system` Pod of system priority preempted the JobManager once (`ct1778-rescale-out-a2`) and the supervisor twice before the queue was created (`ct1778-rescale-out-a3` and `ct1778-rescale-in-a1`).
  One Kubernetes read was refused with HTTP 429 (`ct1778-rescale-out-a6`), which the rig does not retry; [#1797](https://github.com/flink-gcp/flink-connector-gcp/issues/1797) records it.
  The Spot reclamations are read from Compute Engine's `compute.instances.preempted` audit entries and the preemptors from the Kubernetes audit log, since the exported evidence carries only the Pods' `Killing` and `Preempted` events.

## Cleanup

After every attempt, and before the next was dispatched, these were read: the queue `ct1362-<run-id>` described as not found and no `ct1362-` queue was listed, nothing remained under the run's prefixes in `gs://flink-gcp-cloudtasks-benchmark`, `tier3-cloudtasks` held no Pod or FlinkDeployment with its `tier3-idle` quota at zero Pods, `tier3-system` held nothing of the run, and the run record and the environment lock were gone.
The `flink-gcp`, `tier3-bootstrap` and `tier3-operator` plans are the ones the receipt refreshed after the attempt, all empty, rather than plans refreshed again by hand.

## Every admitted attempt

Before each dispatch the reads the preregistration lists passed: the environment lock and run records absent, every earlier approval with its result, the run's evidence prefix and queue name free, the three custom roles bound as #1768 applied them, `pins.cue` naming the lifecycle-tools digest it names, the `cloudtasks-recovery` digest present as the publication in use lists it, and the window ending before 2026-10-20T12:17:04Z.

| Run ID | Workflow run | Dispatched (UTC) | Image | Outcome | Stop condition |
| --- | --- | --- | --- | --- | --- |
| `ct1778-tm-replacement-a1` | [38760750156](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38760750156) | 2026-10-15T15:29Z | `3546b6ed8` | `usable` | none |
| `ct1778-rescale-out-a1` | [38768481780](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38768481780) | 2026-10-15T16:22Z | `3546b6ed8` | A Spot reclamation sent the TaskManager SIGTERM about a second before the upgrade patch; "Unexpected fresh start or unplanned initial recovery" | Platform, after the revision that named this message |
| `ct1778-rescale-out-a2` | [38822773608](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38822773608) | 2026-10-15T23:18Z | `3546b6ed8` | Both recoveries proved; a `konnectivity-agent` Pod preempted the JobManager in the `finishing` window; "Unplanned workload interruption" | Platform, the second such result |
| `ct1778-rescale-out-a3` | [38835267354](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38835267354) | 2026-10-16T01:28Z | `3546b6ed8` | `kube-dns` preempted the supervisor before the queue was created | Supervisor preempted before the queue |
| `ct1778-rescale-out-a4` | [38836907829](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38836907829) | 2026-10-16T01:46Z | `3546b6ed8` | A single `INTERNAL` failed the staged commit in the initial phase; "Unexpected fresh start or unplanned initial recovery" | Any other inconclusive: stop and repair (#1794) |
| `ct1778-rescale-out-a5` | [38923620357](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38923620357) | 2026-10-16T15:24Z | `3a5832b88` | Upgrade proved; a Spot reclamation of the node both TaskManagers ran on in the `upgraded` window; "Unplanned workload replacement" | Platform |
| `ct1778-rescale-in-a1` | [38928827276](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38928827276) | 2026-10-16T16:02Z | `3a5832b88` | `kube-dns` preempted the supervisor before the queue was created | Supervisor preempted before the queue |
| `ct1778-rescale-in-a2` | [38977597941](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38977597941) | 2026-10-16T22:45Z | `3a5832b88` | `usable` | none |
| `ct1778-rescale-out-a6` | [38982390338](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38982390338) | 2026-10-16T23:37Z | `3a5832b88` | Upgrade proved; a single `CANCELLED` restarted the job in the `upgraded` window; the run ended during the failover on a Kubernetes read refused with HTTP 429 | Any other inconclusive, a state-read error: stop and repair (#1797) |

Every attempt dispatched the rig at the `main` commit its image was built from.
Seven attempts ran the application, and in two the supervisor was preempted before the queue was created; `ct1778-rescale-out-a4` and `ct1778-rescale-in-a2` are the redispatches that condition allows.
The estimate charges USD 2.13 per attempt, so the nine attempts come to at most USD 19.17 on its basis, against USD 6.39 approved for three; the two stopped before the queue held only the supervisor and the Operator for a few minutes, and the actual charges were not read from billing.

## Deviations from the preregistration

The preregistration's [revisions](0165-cloudtasks-fault-trials-preregistration-1778.md#revisions-after-the-pilots-dispatch) record each one with the owner's decision:

- The reading rules were revised while the pilot ran, before any result was read.
- "Unexpected fresh start or unplanned initial recovery" joined the platform condition after `ct1778-rescale-out-a1`.
- `rescale-out` ran beyond the one repeat the record allows (interpretation 1's one attempt per trial), and after `ct1778-rescale-out-a4`'s stop the campaign resumed on a new image once #1794 merged.
- `rescale-in` ran before the last `rescale-out` attempt.
- After `ct1778-rescale-out-a6` the owner closed the rescale out without a verdict instead of repairing and approving again, and #1799 reruns it.
- The campaign took about 33 hours, from the pilot's approval at 2026-10-15T15:30Z to the last attempt's refreshed plans at 2026-10-17T00:10Z, rather than the six hours of serial dispatch the record expected, and every window still ended before the deadline.
- The refreshed plans in the cleanup checks are the receipt's, as above.

No result triggered interpretations 3 or 4: no task was missing or listed twice, and no recovery deadline expired.

## Coverage of #1362

These trials extend the [coverage of #1362](0165-cloudtasks-recovery-findings-1362.md#coverage-of-1362) that `ct-1362-a1` recorded; rows not listed here stand as that record gives them.

| #1362 acceptance item | Where it stands |
| --- | --- |
| TaskManager replacement | `ct1778-tm-replacement-a1`: the TaskManager that reported the latest input was deleted and replaced while the other kept its UID, and all 180,000 sequences were listed once; whether the deletion caught a pending commit stays open, since the deduplication counter matched |
| Rescale in and out | Rescale in: `ct1778-rescale-in-a2`, from two sink subtasks to one. Its listing tests the JobManager failover at one subtask, not the savepoint restore, whose envelopes were already tasks; the restore is read from the deduplication counter, which shows no shortfall. Rescale out: the upgrade's rescale from one to two was proved in three attempts and its JobManager failover in one, but no attempt reached a listing; #1799 reruns it. #1245's partial-commit cases remain the controlled evidence for both directions |
| A successful savepoint upgrade with stable sink UIDs, the same queue, retained state and bounded restarts | Unchanged, with one limit these trials found: a job restart on the same Pods is not detected ([#1798](https://github.com/flink-gcp/flink-connector-gcp/issues/1798)); `ct-1362-a1`'s exported JobManager log shows none |
| The latest authoritative checkpoint or savepoint preserved; no silent restart from an older checkpoint | The exercise does not check a job restart it did not plan (#1798); the pilot's and `ct1778-rescale-out-a6`'s restored the retained checkpoint, read from the JobManager log |
| Run, Pod, job, checkpoint and savepoint identities, fault boundaries, reconciliation and recovery metrics exported, with external timestamps; incomplete evidence withholds a verdict | These trials add each recovery's duration, the window each failover's replay began in (no upgrade's was readable), and the connector's metrics in three measurement windows, the deduplication counter and the samples of the replay-budget gauges among them |
| Preregistered and approved resource, attempt, byte, duration and cost ceilings, restart bounds and stop conditions | The attempts exceeded the preregistered one per trial, each by the owner's decision recorded in the revisions |

## What this does not establish

Each `usable` trial has one usable attempt at one input rate on Flink 2.2.1, so variance across repetitions and Flink 1.20 are not claimed, and the queue never dispatched, so nothing is shown about a handler.
The rescale out has no oracle result, and the rescale in's listing does not test its savepoint restore, which only the deduplication counter reads.
The deduplication counter matched each failover's restored records, which leaves open whether either failover restored a commit that had not finished.
Every TaskManager these trials lost shut down on a SIGTERM, the deleted one and the Spot-reclaimed ones alike, as each TaskManager's own log in Cloud Logging records (`RECEIVED SIGNAL 15`); a TaskManager lost without one, detected only when its heartbeat times out, did not occur.
The pilot ran the image without #1794, and its result says nothing about how that change behaves under a TaskManager replacement; the current image still fails a job on a single `CANCELLED` (#1796).
An unplanned job restart is not checked by the exercise (#1798), so these trials establish no restart bound.
