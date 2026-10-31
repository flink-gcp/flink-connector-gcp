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

# Spanner recovery trials: findings

These are the findings of the deployed Spanner trials of [#1878](https://github.com/flink-gcp/flink-connector-gcp/issues/1878), run under their [preregistration](0165-spanner-recovery-preregistration-1878.md) between 2026-10-30 and 2026-10-31 for [#1360](https://github.com/flink-gcp/flink-connector-gcp/issues/1360) and its Change Streams sub-issue [#1872](https://github.com/flink-gcp/flink-connector-gcp/issues/1872).
One FlinkDeployment carried the production `SpannerSink` into `input`, the production `SpannerChangeStreamSource` relaying `input`'s change stream into `relay`, and the production `SpannerSource` copying `seed` into `batch_out`.
Each of the four trial kinds reached a `usable` verdict, so all three components took a savepoint upgrade, a JobManager replacement, a TaskManager replacement, a rescale out and a rescale in on the deployed rig without losing a record; in the TaskManager replacement, though, the relay subtask that restarted was not the one carrying the change stream's data, as [TaskManager replacement](#taskmanager-replacement) shows.
Reaching them took nine attempts; the [list of attempts](#every-admitted-attempt) gives each one, and none showed a connector defect.

## Result

| Trial | Run ID | Verdict | `input` keys matched, missing; executions per key | `relay` keys matched, missing; replays | `batch_out` seed rows matched, missing; replayed keys, extra deliveries |
| --- | --- | --- | --- | --- | --- |
| `jm-replacement` (pilot) | `sp1878-jm-a1` | `usable` | 180,000, 0; one each | 180,000, 0; 2,500 | 120,000, 0; 41,913, 74,854 |
| `tm-replacement` | `sp1878-tm-a2` | `usable` | 180,000, 0; one each | 180,000, 0; 0 | 120,000, 0; 42,153, 59,822 |
| `rescale-out` | `sp1878-ro-a3` | `usable` | 180,000, 0; one each | 180,000, 0; 3,000 | 120,000, 0; 38,677, 56,425 |
| `rescale-in` | `sp1878-ri-a3` | `usable` | 180,000, 0; one each | 180,000, 0; 3,000 | 120,000, 0; 40,417, 57,741 |

Every trial ran the `spanner-recovery` image `sha256:75a2d78a2315c02406c3dbc5dc7576d245f68ad110a23d94628541faffcf5c1f` from the [Spanner recovery publication](../../../kubernetes/images/README.md#spanner-recovery-publication) of main `34be4f08c`.
No oracle found a foreign key, a wrong value, a relayed commit after the change stream's end, or a copied late row.
In each trial the seed report matched all 120,016 rows, the 120,000 seeded and the 16 late ones.
Each relay was judged: the input finished 1,262 to 1,472 seconds before the change stream's end, more than the 120 seconds the oracle needs, and the source's watermark rose from the `baseline` window to the `upgraded` one and again to the `finishing` one.

For each trial the final receipt and `just tier3-analyze` agree on `usable`, the analyzer reports no problem, and the fault reads under the preregistration's acceptance criteria pass:

- `sp1878-jm-a1`: the approval's kind is `jm-replacement`, the timing table names `savepoint-upgrade` and `jm-replacement`, and the failover proof records a new JobManager Pod UID (`cda6ef12…`, after `47f8f366…`).
- `sp1878-tm-a2`: the kind is `tm-replacement`, and the timing table names `savepoint-upgrade` and `tm-replacement`. The supervisor deleted `sp1878-tm-a2-taskmanager-1-1` (`9cfe6b2b…`), the TaskManager its rule named, and the JobManager's UID (`b8ef80dc…`) is the same before and after the failover.
- `sp1878-ro-a3`: the kind is `rescale-out`, the timing table names `rescale-out` and `jm-replacement`, the upgrade proof records the checkpointed subtasks moving from 4 to 7, and the failover proof records a new JobManager UID (`7783c1a5…`, after `0cdde573…`).
- `sp1878-ri-a3`: the kind is `rescale-in`, the timing table names `rescale-in` and `jm-replacement`, the upgrade proof records the checkpointed subtasks moving from 7 to 4, and the failover proof records a new JobManager UID (`5fc6cff5…`, after `caf074f9…`).

The input runs at one subtask throughout, so the move between 4 and 7 checkpointed subtasks is the three component vertices going between one subtask and two.
Each trial read every family in its `baseline`, `upgraded` and `finishing` windows, four or five readings in each; the supervisor also measures outside the windows, so each run holds 42 to 46 readings in all.

### Readings the pilot measures

The preregistration's [pilot readings](0165-spanner-recovery-preregistration-1878.md#what-the-pilot-measures) were premises of the [Spanner recovery foundation](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#spanner-recovery-foundation), because #1877's probes ran every call as the owner.
The pilot held each of them, and so did every later attempt that reached the same step:

- **`CreateDatabase` with its extra statements under `spanner.databases.create` alone**: every one of the eight attempts that reached admission read back the four tables, the `input_changes` change stream and the two-hour version retention in `spanner-instance-admitted`.
- **The runner's session, seed commits and split points under the instance-name condition**: each of their receipts records 120,000 seed rows and the split points `4`, `8` and `c`, and no runner stop names a refused call.
- **The workload's data calls under the same condition**: the oracles found the input, the relay and the copy complete in every usable trial, and no exported evidence of any of the nine attempts contains `PERMISSION_DENIED`.
- **The supervisor's calls under the same condition**: each of the eight recorded `late_seed`, every usable one the three oracle reports, and every attempt that created an instance records it `cleaned`.
- **Admission within the 600-second startup budget**: the eight reached `spanner-instance-admitted` 336 to 472 seconds after the approval's `started_at`, including the supervisor's and the Operator's startup.
- **A split point repeated after a lost response**: no attempt stopped on `:addSplitPoints`, so no repeat was refused; whether any request was repeated is not recorded.
- **The batch plan**: the read planned seven partitions, once, in each of the eight, as #1877 measured.

The identical-write probe ran before the pilot, as the preregistration records: a write that repeats identical values produces a change record.

### Recovery

Each recovery is timed from the supervisor's record of its stage, as [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#recovery-timing-from-flinks-timestamps) defines it.
`Restored after` is Flink's restore of the state, which precedes the tasks' deployment, so it is a lower bound; `Acknowledged after` is the last acknowledgement of the checkpoint that proved the restored job, which follows its first input past the fault, so it is an upper bound.
The table also gives the recoveries that inconclusive attempts proved before they stopped.

| Run | Verdict | Recovery | Fault | Restored from | Restored after | Proving checkpoint | Acknowledged after |
| --- | --- | --- | --- | --- | ---: | --- | ---: |
| `sp1878-jm-a1` | `usable` | upgrade | `savepoint-upgrade` | savepoint 15 | 46.0 s | 16 | 74.6 s |
| `sp1878-jm-a1` | `usable` | failover | `jm-replacement` | checkpoint 27 | 38.9 s | 28 | 47.9 s |
| `sp1878-tm-a1` | inconclusive | upgrade | `savepoint-upgrade` | savepoint 15 | 43.6 s | 16 | 83.6 s |
| `sp1878-tm-a1` | inconclusive | failover | `tm-replacement` | checkpoint 26 | 10.9 s | 29 | 101.2 s |
| `sp1878-tm-a2` | `usable` | upgrade | `savepoint-upgrade` | savepoint 15 | 55.3 s | 16 | 113.7 s |
| `sp1878-tm-a2` | `usable` | failover | `tm-replacement` | checkpoint 27 | 10.9 s | 28 | 63.4 s |
| `sp1878-ro-a2` | inconclusive | upgrade | `rescale-out` | savepoint 15 | 40.7 s | 16 | 75.6 s |
| `sp1878-ro-a2` | inconclusive | failover | `jm-replacement` | checkpoint 26 | 33.9 s | 27 | 49.0 s |
| `sp1878-ro-a3` | `usable` | upgrade | `rescale-out` | savepoint 15 | 40.8 s | 16 | 92.5 s |
| `sp1878-ro-a3` | `usable` | failover | `jm-replacement` | checkpoint 27 | 35.6 s | 29 | 72.5 s |
| `sp1878-ri-a1` | inconclusive | upgrade | `rescale-in` | savepoint 15 | 36.4 s | 16 | 86.1 s |
| `sp1878-ri-a1` | inconclusive | failover | `jm-replacement` | checkpoint 26 | 36.5 s | 28 | 80.7 s |
| `sp1878-ri-a3` | `usable` | upgrade | `rescale-in` | savepoint 15 | 43.3 s | 16 | 98.2 s |
| `sp1878-ri-a3` | `usable` | failover | `jm-replacement` | checkpoint 26 | 36.4 s | 28 | 91.8 s |

Every recovery proved within 114 seconds, under the 600-second budget each recovery has.
A TaskManager replacement restored its state about 11 seconds after the deletion, against 34 to 39 seconds for a JobManager replacement, which has to start a new JobManager first.

#### TaskManager replacement

The supervisor deleted `sp1878-tm-a2-taskmanager-1-1` at 17:50:12Z on 2026-10-30.
The JobManager logged three restarts, each restoring checkpoint 27: three tasks for the input with its sink, one for copy subtask 1 of 2, and one for relay subtask 1 of 2, which is the three-region case the preregistration accepts for this job.
Checkpoint 27 had been triggered 3.5 seconds before the deletion.
The relay replayed no key in this trial, because the restarted relay subtask was not the one that carried the input's changes.
In every reading of the trial one relay writer subtask had sent nothing and the other everything, and the busy one's `numRecordsSend` kept rising through the deletion, 31,866 at 17:49:34Z and 37,887 at 17:51:09Z, where a restarted writer counts again from zero, as `sp1878-jm-a1`'s did after its JobManager failover.
The restarted subtask held one partition query with no data, so the TaskManager replacement restarted a relay reader but not the relay's data path.

#### Sink executions and relay replays

Every input key of every usable trial shows one counted execution, so no recovery produced a second execution the relay delivered.
That is consistent with how the sink sent its writes at this rate: in `sp1878-ri-a3`, with one sink subtask after the rescale in, the writer sent 15 batches while 15 checkpoints completed, and it buffered at most 2,853 mutations at any reading, under one 30-second interval of input.
So a fault would drop records the writer had buffered but not yet sent, and the restored input would emit them again for one write each.
The count is the number of executions, by the identical-write probe, only provided the relay delivered every change record; the Change Streams oracle judges delivery by key, so a second commit of a key whose change record the relay missed is not excluded, and the receipts label the count a lower bound.

The relay's replays are reported apart from the sink's executions, and no exactly-once delivery is claimed.
Each trial with a JobManager failover replayed 2,500 to 3,000 keys, about one 30-second checkpoint interval at 100 records per second, and `sp1878-tm-a2`, whose recoveries are a savepoint upgrade and the TaskManager replacement, replayed none.
The oracle counts replays per run, so it does not split a run's replays between its two recoveries.
The deliveries of each phase were, initial then upgraded: `sp1878-jm-a1` 41,769 and 140,731, `sp1878-tm-a2` 41,123 and 138,877, `sp1878-ro-a3` 41,337 and 141,663, and `sp1878-ri-a3` 39,795 and 143,205.

#### Batch read

The read planned seven partitions in every trial, and the late commit followed the plan's read timestamp by 116 to 149 seconds, so `late_after_plan` is true in each and no late row was copied.

| Run | Instance admitted | Plan's read timestamp | Late commit | Cleanup finished |
| --- | --- | --- | --- | --- |
| `sp1878-jm-a1` | 2026-10-30T15:04:22Z | 15:07:22.5Z | 15:09:51.7Z | 16:04:20Z |
| `sp1878-tm-a2` | 2026-10-30T17:31:08Z | 17:33:44.8Z | 17:36:02.4Z | 18:32:20Z |
| `sp1878-ro-a3` | 2026-10-31T00:33:58Z | 00:35:58.8Z | 00:38:25.0Z | 01:38:45Z |
| `sp1878-ri-a3` | 2026-10-31T07:12:14Z | 07:13:15.0Z | 07:15:11.2Z | 08:13:16Z |

`Cleanup finished` is the later of the supervisor's and the runner's `cleanup-ready` records, each emitted after the instance's deletion was read back, the run's Flink state removed and the Operator scaled to zero.
The copy was still reading after both recoveries of every trial: its rate in the `upgraded` and `finishing` windows was 49.9 rows per second at one subtask and 99.6 to 99.8 at two, the 50 rows per second per subtask of its throttle.
Each recovery replayed whole partitions: a prefix holds 7,500 keys, a prefix replayed at both recoveries shows 15,000, such as `4`, `5`, `8` and `9` in `sp1878-jm-a1`, and a count that is not a multiple of 7,500, such as 7,437 or 8,735, fits a partition boundary inside that prefix, so that only part of it was in a replayed partition.
`partitionsReread` read zero in every window of every trial.
It counts a partition reopened after a wake-up cancelled it part-way, as the [source documentation](../../content/docs/connectors/datastream/spanner.md) describes, not a partition restored from a checkpoint, so zero is consistent with the replays above.

| Run | Extra deliveries by first character |
| --- | --- |
| `sp1878-jm-a1` | `4` 15,000, `5` 15,000, `6` 7,437, `8` 15,000, `9` 15,000, `a` 7,417 |
| `sp1878-tm-a2` | `4` 15,000, `5` 15,000, `6` 8,735, `c` 7,500, `d` 7,500, `e` 6,087 |
| `sp1878-ro-a3` | `8` 15,000, `9` 15,000, `a` 8,686, `c` 7,500, `d` 7,500, `e` 2,739 |
| `sp1878-ri-a3` | `8` 7,500, `9` 7,500, `a` 5,214, `c` 15,000, `d` 15,000, `e` 7,527 |

#### Change Streams partitions

The coordinator's `changeStreamPartitionsDiscovered` read five in the `baseline` window of every usable trial, before any restart.
Each upgrade and each JobManager replacement starts a new enumerator, which counts from zero and starts from its restored ledger, and the counter rises only for a child token that ledger does not hold.
In `sp1878-tm-a2`, whose enumerator ran from the upgrade to the end, the counter read zero in the `upgraded` window and two in the `finishing` window, so the service ended partitions and reported new child partitions during the trial, as a split, a merge or a move does.
The other trials read three in the `finishing` windows of `sp1878-jm-a1` and `sp1878-ro-a3` and one in the `upgraded` window of `sp1878-ri-a3`, after a restart; there a token first accepted between the restored state and the fault would count again, but the two failovers restored checkpoints triggered about seven seconds before their faults and the upgrade a savepoint taken at its stop, so these readings too are most likely service partition changes.
The evidence does not record which partitions changed, or how, and nothing forced a change; the relay still delivered every key.
The readers held five or six partition queries open in aggregate in every window, at one subtask as at two, six only in the three `finishing` windows that counted new children, and queued none, so the queued-partition lag read zero.

### Measurement families

The largest value any reading in the window returned, so readings outside the windows are not in it; they describe these runs at their fixed rate, and none is a threshold.
The components share TaskManagers, so the heap column describes the job.

| Run | Window | `input` records in/s | `input` busy ms/s | `relay` busy ms/s | Back-pressured ms/s | Checkpoint end-to-end ms | Copy rows/s | Open partition queries | Coordinator partitions discovered | TaskManager heap MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `sp1878-jm-a1` | `baseline` | 100.1 | 7 | 10 | 0 | 522 | 99.7 | 5 | 5 | 235 |
| `sp1878-jm-a1` | `upgraded` | 100.0 | 8 | 13 | 0 | 645 | 99.7 | 5 | 0 | 199 |
| `sp1878-jm-a1` | `finishing` | 100.1 | 8 | 13 | 0 | 570 | 99.7 | 6 | 3 | 205 |
| `sp1878-tm-a2` | `baseline` | 100.0 | 11 | 15 | 0 | 555 | 99.7 | 5 | 5 | 215 |
| `sp1878-tm-a2` | `upgraded` | 100.1 | 9 | 12 | 0 | 1,311 | 99.8 | 5 | 0 | 214 |
| `sp1878-tm-a2` | `finishing` | 100.1 | 7 | 11 | 0 | 513 | 99.8 | 6 | 2 | 212 |
| `sp1878-ro-a3` | `baseline` | 100.1 | 11 | 16 | 0 | 590 | 49.9 | 5 | 5 | 147 |
| `sp1878-ro-a3` | `upgraded` | 100.0 | 11 | 14 | 0 | 561 | 99.7 | 5 | 0 | 198 |
| `sp1878-ro-a3` | `finishing` | 100.0 | 10 | 17 | 0 | 526 | 99.6 | 6 | 3 | 230 |
| `sp1878-ri-a3` | `baseline` | 100.1 | 9 | 14 | 0 | 649 | 99.7 | 5 | 5 | 233 |
| `sp1878-ri-a3` | `upgraded` | 100.1 | 10 | 13 | 0 | 560 | 49.9 | 5 | 1 | 234 |
| `sp1878-ri-a3` | `finishing` | 100.0 | 8 | 10 | 0 | 482 | 49.9 | 5 | 0 | 226 |

The copy's busy time read 1,000 ms/s in every window, because the throttle that paces it is chained into the source's task, as the [runbook](../../../kubernetes/apps/spanner/README.md) describes, so its rate is the column to read.
The network family read zero input and output buffer-pool usage on every vertex in every window. No vertex was back-pressured, none of the three writers retried a mutation in any reading, and the checkpoint end-to-end duration stayed under 1.4 seconds in the windows.

## Observations

- **The platform stopped or interrupted four of the five attempts that did not reach `usable`.**
  A `kube-system` `konnectivity-agent` Pod of system priority preempted `sp1878-tm-a1`'s JobManager during an Autopilot defragmentation at 17:18:00Z on 2026-10-30, after both recoveries had proved.
  A `kube-dns` Pod preempted `sp1878-ro-a1`'s supervisor about 30 seconds after it started, before admission, so the attempt created no instance; its exported evidence records only that admission stopped and that the supervisor Job had no Pod, so the preemption and the Job's end were read from the cluster at the time.
  `sp1878-ri-a1`'s TaskManager stopped when its node logged a `Shutdown` event at 06:31:23Z on 2026-10-31, about two and a half minutes after the JobManager failover had proved.
  A Pod outside the run's namespaces preempted `sp1878-ri-a2`'s JobManager at 06:57:09Z, eight seconds after the `baseline` window's planned end and before the upgrade.
  The exercise read the three after admission as "Unplanned workload interruption", which the campaign repeats, from the namespace's `Preempted` events and, for `sp1878-ri-a1`, the node's `Shutdown` event that [#1892](https://github.com/flink-gcp/flink-connector-gcp/issues/1892) added.
  The exported events carry only the preemptor's Pod UID, so the `konnectivity-agent` and `kube-dns` preemptors were named from the cluster's events read at the time.
- **One checkpoint-storage timeout restarted the job.**
  In `sp1878-ro-a2`, after both recoveries had proved, one GCS `compose` call timed out while writing checkpoint 71's `_metadata`.
  With Flink's default of zero tolerable checkpoint failures, that one failure failed the job, which restored checkpoint 70 at 19:54:59Z on 2026-10-30, and the exercise stopped at "Unplanned job restart".
  Every application now sets `execution.checkpointing.tolerable-failed-checkpoints: 2`, as ADR-0165's [unplanned job restarts](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#unplanned-job-restarts) section records; no attempt here ran with it.
- **A dropped connection cut a campaign short.**
  After `sp1878-ro-a3` was usable, the runner's settle read of the `tier3-system` namespace failed on one connection reset by peer, so the lifecycle job failed before choosing the next attempt and the campaign dispatched none.
  [#1937](https://github.com/flink-gcp/flink-connector-gcp/issues/1937) made every Tier-3 Kubernetes call repeat such a failure before the `rescale-in` dispatch.
- **The supervisor Job does not replace a preempted Pod.**
  The preregistration says, in its list of events the Bigtable campaign met, that the supervisor Job replaces a disrupted Pod up to twice before supervision starts.
  That is false: the Job's `backoffLimit` is zero, as the [lifecycle runbook](../../../kubernetes/lifecycle/README.md) states, and `sp1878-ro-a1`'s Job ended at once with `BackoffLimitExceeded`.
  The campaign's repeat of a stop before the application existed is what covered it.

## Cleanup

After each attempt's receipt was written these were read: `sp1360-<run-id>` not found, nothing under `runs/<run-id>/` or `.inprogress/` in `gs://flink-gcp-tier3-spanner`, the run record gone, and the receipt's verified idle with the empty refreshed plans.
The pilot's were read before its report on #1878 at 16:16Z on 2026-10-30, and the three `rescale-in` attempts' at 06:39Z, 07:06Z and 08:17Z on 2026-10-31; the reads after the five attempts of the two campaigns were made at the time and their times were not kept, which [Deviations](#deviations-from-the-preregistration) lists.
Every receipt that created an instance records it `cleaned`, and `sp1878-ro-a1` created none.
After the last attempt, at 08:24:45Z on 2026-10-31, the instance listing held no `sp1360-` instance, none of the nine run IDs had Flink state in the state bucket, `_control/environment.json` and every `_control/runs/` record were gone, and no `tier3-run` workflow run was queued or in progress.
The `tier3-spanner` namespace was read from each receipt's verified-idle listing, which held no Pod and no FlinkDeployment, only the namespace's root CA ConfigMap; the operator's account is not granted to list the namespace directly.
The `flink-gcp`, `tier3-bootstrap` and `tier3-operator` plans are the ones each receipt refreshed after its attempt, all empty.
The evidence under `runs/sp1878-*/` in `gs://flink-gcp-tier3-evidence` is left to its 30-day lifecycle and was downloaded before these findings were written.

## Every admitted attempt

Before each dispatch by hand the reads the preregistration lists passed, with the deadline its revisions give.
For an attempt a campaign dispatched itself, its admission's own reads of the lock and the run record stood in for them.

| Run ID | Workflow run | Dispatched (UTC) | Rig | Outcome | Stop condition |
| --- | --- | --- | --- | --- | --- |
| `sp1878-jm-a1` | [41076072497](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/41076072497) | 2026-10-30T14:56Z | `229fddd44` | `usable` | none |
| `sp1878-tm-a1` | [41088249409](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/41088249409) | 2026-10-30T16:15Z | `229fddd44` | Both recoveries proved; `konnectivity-agent` preempted the JobManager; "Unplanned workload interruption" | Platform: the campaign repeats it |
| `sp1878-tm-a2` | [41098607645](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/41098607645) | 2026-10-30T17:24Z | `229fddd44` | `usable` | none |
| `sp1878-ro-a1` | [41108397107](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/41108397107) | 2026-10-30T18:36Z | `229fddd44` | `kube-dns` preempted the supervisor before admission | A stop before the application existed: the campaign repeats it |
| `sp1878-ro-a2` | [41110961490](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/41110961490) | 2026-10-30T18:55Z | `229fddd44` | Both recoveries proved; a checkpoint-storage timeout restarted the job; "Unplanned job restart" | No condition names it: the campaign ended, and the owner approved a second campaign |
| `sp1878-ro-a3` | [41150053853](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/41150053853) | 2026-10-31T00:25Z | `229fddd44` | `usable`; the next attempt was not dispatched (#1937) | none |
| `sp1878-ri-a1` | [41174978891](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/41174978891) | 2026-10-31T06:02Z | `ceae8b28d` | Both recoveries proved; the TaskManager's node shut down; "Unplanned workload interruption" | Platform: the dispatch repeats it |
| `sp1878-ri-a2` | [41177444761](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/41177444761) | 2026-10-31T06:38Z | `3535c4647` | The JobManager was preempted in the `baseline` window; "Unplanned workload interruption" | Platform: the dispatch repeats it |
| `sp1878-ri-a3` | [41179143818](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/41179143818) | 2026-10-31T07:04Z | `f390c7e95` | `usable` | none |

The pilot was dispatched alone, the first campaign ran `sp1878-tm-a1` to `sp1878-ro-a2` and spent its two repeats, the second ran `sp1878-ro-a3`, and the `rescale-in` dispatch ran the last three, each approved by the owner as the preregistration's [revisions](0165-spanner-recovery-preregistration-1878.md#revisions-after-dispatch) record.
Every attempt dispatched the rig at the `main` commit current at its dispatch.
Between the three `rescale-in` commits the rig's paths, `tools/`, `kubernetes/`, `opentofu/`, `.github/` and the `justfile`, differ only in `.github/dependabot.yml`, so the three attempts ran the same rig, which includes #1937's repair.
Every attempt ran the one application image above.
Eight attempts ran the application, and `sp1878-ro-a1` stopped before admission.
The estimate charges USD 2.24 per attempt, so the nine come to at most USD 20.12 (`20.1256875`) on its basis, the total the owner approved; the actual charges were not read from billing.

## Deviations from the preregistration

The preregistration's [revisions](0165-spanner-recovery-preregistration-1878.md#revisions-after-dispatch) record each one with the owner's decision:

- `tm-replacement` took two attempts, `rescale-out` three and `rescale-in` three, beyond the six attempts first approved; the owner approved the second campaign and the `rescale-in` dispatch, nine attempts in all.
- The rig changed once between attempts, before the `rescale-in` dispatch: #1937's repair, with a `mise-action` update in the run workflows ([#1942](https://github.com/flink-gcp/flink-connector-gcp/pull/1942)) and new recipes in the `justfile` ([#1934](https://github.com/flink-gcp/flink-connector-gcp/pull/1934)). The application image did not change.
- The cleanup reads after the five campaign attempts were made but their times not kept, and the whole-environment reads were recorded once, after the last attempt, rather than also after the pilot and each campaign.
- Within a campaign the next attempt was dispatched by the workflow itself, so the before-dispatch and cleanup reads the record places between attempts were replaced by the admission's own reads and read afterwards, as above.
- The namespace's Pods, FlinkDeployments and quota were read from the receipts' verified-idle records rather than by a direct listing, which the operator's account is not granted.

The preregistration's three corrections are carried here: the supervisor Job's `backoffLimit` is zero, under [Observations](#observations); a `kube-system` Pod preempting the JobManager after admission, the variant its list of Bigtable events does not name, read as "Unplanned workload interruption", which the campaign repeated; and the checkpoint-storage timeout is reported above as inconclusive, together with the application's zero tolerance of checkpoint failures and the change that ends it.
One more reading of the preregistration is narrowed here: its [pilot readings](0165-spanner-recovery-preregistration-1878.md#what-the-pilot-measures) say the trials read the sink's executions without the lower-bound caveat, but that holds only provided the relay delivered every change record, as its oracle description says, so the receipts and the runbook keep the caveat for a commit the relay missed.
No result triggered interpretation 3: no input key or sink value was missing or wrong, no relayed key was missing or late, every watermark advanced, and no batch row was missing, wrong or late.

## Coverage of #1360 and #1872

The [scope](https://github.com/flink-gcp/flink-connector-gcp/issues/1360#issuecomment-6270858712) is done when four deployed trials reach `usable`, one per kind, each with its savepoint upgrade, all three oracles passing, and cleanup verifying the instance absent, the namespace idle and the plans empty; the result above meets it.
Against #1360's acceptance list, with #1872's Change Streams items folded into the fifth row:

| Acceptance item | Where it stands |
| --- | --- |
| The shared image publication, CUE application, external state, admission and independent cleanup, ADR-0165's ownership, native workload ADC, and provisioning and cleanup authority kept separate | [#1873](https://github.com/flink-gcp/flink-connector-gcp/issues/1873) to [#1877](https://github.com/flink-gcp/flink-connector-gcp/issues/1877); every trial created, wrote, read and deleted its instance under those grants, with no `PERMISSION_DENIED` in any attempt |
| Frozen digests, Flink version, dialect and API matrix, the production DataStream and Table entry points, and every unmeasured combination named | The image digest above, Flink 2.2.1 and Java 17, the GoogleSQL dialect and the DataStream entry points. The PostgreSQL dialect and the Table API entry points were set aside by the owner's scope decision, and Flink 1.20 was not run |
| For the sink, identifiable replay-safe mutations through a JobManager and a TaskManager replacement in separate trials, every key and value read back, and no exactly-once claim from an idempotent table | `sp1878-jm-a1` and `sp1878-tm-a2`, and the other two trials' JobManager failovers; every key held its sequence, and the executions are counted from the relay rather than inferred from the table. No second execution was counted, so how a re-executed write lands was not shown |
| For batch reads, a recovery while partitions are unfinished, complete coverage, bounded completion within the snapshot's lifetime, whole-partition replay recorded, and no silent re-plan at a newer snapshot | The copy read through both recoveries of every trial, every seed row was copied with its value, every job finished well within the two-hour retention, the replays are reported by prefix, and no late row was copied with `late_after_plan` true |
| For Change Streams, a completed external checkpoint, active Pods replaced, savepoint rescales in and out with stable UIDs, continued coverage and watermark progress, and service partition changes kept distinct from Flink rescaling | Each failover restored a completed external checkpoint; `sp1878-ro-a3` and `sp1878-ri-a3` rescaled through savepoints with the application's fixed UIDs; every key was relayed and the watermark advanced after each recovery. The TaskManager replacement restarted a relay subtask without data, so the relay's data path took only JobManager replacements and savepoint upgrades. Every trial queued no partition, so the queued-split path was not exercised. Service partition changes occurred after the recoveries, which the coordinator counted as new child partitions, and every key was still relayed; which changes they were is not recorded |
| Input identities, readback and events, checkpoint and savepoint paths, Pod and job identities, fault timing, restart history and source and sink metrics exported outside the job JVM, failing on missing or empty evidence | Partly: Pod, job, checkpoint and savepoint identities, events, logs and metrics are exported, and the verdict withholds `usable` from missing coverage or an unproved recovery, but the oracles export counts and the first keys of any deviation rather than the row identities, which the instance's deletion removes |
| Preregistered ceilings and stop conditions, approved separately, the standing cluster and Spot placement, ownership proved before removal, cleanup while the Operator runs, and zero Operator replicas, idle quotas and empty plans verified | The preregistration and its two approved revisions; the per-scenario caps were not carried over by the scope; and [Cleanup](#cleanup) |

The remaining items of #1872 are the replays reported apart from the sink's executions, under [Sink executions and relay replays](#sink-executions-and-relay-replays), and the source vertex's recovery figures and families, under [Recovery](#recovery) and [Measurement families](#measurement-families).

## What this does not establish

Each kind has one usable attempt at one input rate on Flink 2.2.1, so variance across repetitions and Flink 1.20 are not claimed, and neither the PostgreSQL dialect nor the Table API entry points were run.
The application image was built from main `34be4f08c`, so its connector predates later Spanner changes: [#1919](https://github.com/flink-gcp/flink-connector-gcp/pull/1919), which keeps the enumerator's metrics across a coordinator reset, [#1920](https://github.com/flink-gcp/flink-connector-gcp/pull/1920), which orders the sink's writes per key, and [#1929](https://github.com/flink-gcp/flink-connector-gcp/pull/1929), [#1930](https://github.com/flink-gcp/flink-connector-gcp/pull/1930) and [#1933](https://github.com/flink-gcp/flink-connector-gcp/pull/1933), which changed the sink's schema reads.
No recovery produced a counted second execution, so the trials show the at-least-once sink losing nothing, not how a re-executed write lands.
The relay judges keys rather than commits, and the TaskManager replacement did not restart the relay subtask carrying the change stream's data, so a TaskManager loss under that subtask is not shown.
No partition was queued, so the source's handling of a queued partition under recovery is not shown, and the service partition changes the trials met are not identified, so which kinds of change the source handled is not shown either.
Every JobManager and TaskManager these trials lost shut down on a SIGTERM, whether the supervisor deleted it, a Pod preempted it or its node shut down; a process lost without one, detected only by a heartbeat timeout, did not occur.
