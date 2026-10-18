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

# Cloud Tasks rescale out rerun: findings

These are the findings of the Cloud Tasks `rescale-out` rerun of [#1799](https://github.com/flink-gcp/flink-connector-gcp/issues/1799), run under its [preregistration](0165-cloudtasks-rescale-out-preregistration-1799.md) on 2026-10-17.
The staged `EXACTLY_ONCE` sink took a savepoint rescale out from one sink subtask to two and a JobManager failover to a `usable` verdict on the deployed rig, on the second of the four counted attempts the record allows.
With the TaskManager replacement and the rescale in of [#1778](0165-cloudtasks-fault-trials-findings-1778.md), and the savepoint upgrade and JobManager failover of [`ct-1362-a1`](0165-cloudtasks-recovery-findings-1362.md), each of the five faults of the Tier-3 measurement set has now taken the staged sink to a `usable` verdict at least once on the deployed rig, though not under one image or one rule: the other four ran images built before #1796, and before the exercise stopped on an unplanned restart ([#1798](https://github.com/flink-gcp/flink-connector-gcp/issues/1798)), under which the TaskManager replacement would have stopped as inconclusive, as the [#1778 findings](0165-cloudtasks-fault-trials-findings-1778.md#revision-after-1798) record.

## Result

| Run ID | Image | Verdict | Listed, missing, repeated, foreign, dispatched |
| --- | --- | --- | --- | --- |
| `ct1799-rescale-out-a2` | [CANCELLED retry publication](../../../kubernetes/images/README.md#cloud-tasks-cancelled-retry-publication) (`526a186f0`) | `usable` | 180,000, 0, 0, 0, 0 |

The final receipt and `just tier3-analyze` agree on `usable`, and the three reads under the preregistration's [acceptance criteria](0165-cloudtasks-rescale-out-preregistration-1799.md#acceptance-criteria) pass:

- The receipt's `cloudtasks_trial.kind` is `rescale-out`, and the analyzer's timing table names `rescale-out` for the upgrade and `jm-replacement` for the failover.
- The completion record's upgrade proof records the rescale `out`, with the checkpointed subtasks moving from 2 to 3: the input vertex and one subtask per sink subtask.
- The queue read back `PAUSED` at admission, at 15:29:16Z, and again before the listing, and the oracle passed over the first listing attempt, one whole listing of 181 pages, from the listing stage's start at 16:05:17Z to the completion record at 16:05:54Z.

Each of the `baseline`, `upgraded` and `finishing` windows read every family the runbook names, four readings each, and no `unplanned-restore` event was exported.

## Recovery

Each recovery is timed as the [#1778 findings](0165-cloudtasks-fault-trials-findings-1778.md#recovery) time theirs: `Restored after` is Flink's restore of the state, a lower bound, and `Acknowledged after` the last acknowledgement of the checkpoint that proved the restored job, an upper bound.

| Run | Recovery | Fault | Restored from | Restored after | Acknowledged after |
| --- | --- | --- | --- | ---: | ---: |
| `ct1799-rescale-out-a1` | upgrade | `rescale-out` | savepoint 15 | 39.4 s | 89.3 s |
| `ct1799-rescale-out-a2` | upgrade | `rescale-out` | savepoint 15 | 31.0 s | 64.5 s |
| `ct1799-rescale-out-a2` | failover | `jm-replacement` | checkpoint 26 | 31.3 s | 53.7 s |

The `ct1799-rescale-out-a1` row is a recovery the exercise proved in an attempt that later stopped, so it has no verdict; its upgrade proof also records the checkpointed subtasks moving from 2 to 3.
In `ct1799-rescale-out-a2`, the committer's replay of checkpoint 26 began 49.0 to 70.6 s after that checkpoint's last acknowledgement.
The analyzer reports the upgrade's replay window unreadable, as it did for every upgrade in #1778, because no exported `checkpoints` poll saw the restored savepoint.

### Deduplication counter

The preregistration reads the rescale's restore from the deduplication counter, `tasksDeduplicated`, in the `upgraded` window, where only a shortfall says something.

- The savepoint's own `cloudtasks-snapshot` line is not in the exported Pod logs, so its records are read, as #1778 read them, from the upgraded job's first progress line, which names the first sequence the input maps after the restore: 41,716, against 41,093 at checkpoint 14.
  The 623 records that leaves are what the counter read throughout the `upgraded` window, all of them on one of the two sink subtasks the savepoint was restored into, and none on the other.
  `ct1799-rescale-out-a1`, which stopped later, shows the same for its own savepoint: a first upgraded sequence of 41,792 against 40,385 at checkpoint 14 leaves 1,407 records, and the counter read 1,407 throughout its `upgraded` window, again on one subtask.
  So the rescale out shows no shortfall in either attempt: no evidence that restoring one committer's envelopes into two subtasks dropped one, which the listing could not have shown.
- The failover's counter matched the records of the checkpoint it restored: checkpoint 26's `cloudtasks-snapshot` lines account for 3,003 records beyond checkpoint 25, and the counter read 3,003 across the two subtasks, 1,502 and 1,501, from the first reading after the failover to the last.
  As in #1778, a matching count is consistent with every restored envelope having already been created when the JobManager was deleted, and does not prove it.

One reading outside the windows, at 15:58:28Z, caught both subtasks' `currentCommit` gauges during an ordinary commit: a remaining replay budget of 3,248,173 and 3,248,175 ms and an oldest task age of 31.8 s.

## Measurement families

The largest value any reading in the window returned, read as the #1778 findings read theirs; none is a threshold:

| Run | Window | Sink records in/s (sum) | Busy ms/s | Back-pressured ms/s | Sink bytes in/s (sum) | Buffer pool usage, in and out | Staged tasks | TaskManager heap MiB | Sink checkpoint end-to-end ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `ct1799-rescale-out-a2` | `baseline` | 100.1 | 110 | 0 | 2,103 | 0, 0 | 2,872 | 202 | 659 |
| `ct1799-rescale-out-a2` | `upgraded` | 100.0 | 80 | 0 | 2,103 | 0, 0 | 1,453 | 198 | 721 |
| `ct1799-rescale-out-a2` | `finishing` | 100.1 | 61 | 0 | 2,104 | 0, 0 | 1,429 | 169 | 5,437 |

The baseline runs one sink subtask, which carries the whole input, and the later windows two: the staged batch halves after the rescale, and the busy time falls, the mirror of the rescale in's change in #1778.
The `finishing` window's largest checkpoint end-to-end time, 5,437 ms, is one subtask's in its first reading, checkpoint 29; every later reading in that window stayed below 600 ms.
Each JobMaster counted failed checkpoints only at its edges: the initial job four before its first completed checkpoint, the upgraded job one by its first poll, and the failed-over job one, checkpoint 63, triggered at 16:04:56Z and refused because a task had already finished with the input.

## Every admitted attempt

Both dispatched attempts admitted their queue, so both are counted, and no other attempt was dispatched.
Before each dispatch the reads the preregistration lists passed, the second attempt's including #1828's merge on `main`, and the owner approved each dispatch, the second after #1828 merged.

| Run ID | Workflow run | Dispatched (UTC) | Counted | Outcome | Stop condition |
| --- | --- | --- | --- | --- | --- |
| `ct1799-rescale-out-a1` | [39024374566](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39024374566) | 2026-10-17T10:18Z | yes | Rescale out proved (restored savepoint 15 after 39.4 s); one read of the control record timed out during the JobManager failover, and the supervisor stopped the run on the evidence failure | Any other inconclusive, a state-read error: stop and repair ([#1827](https://github.com/flink-gcp/flink-connector-gcp/issues/1827)). The receipt stored no completion and `just tier3-analyze` read the attempt `inconclusive` (`recovery-incomplete`), so the environment condition for an analysis without a verdict did not apply |
| `ct1799-rescale-out-a2` | [39044864964](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39044864964) | 2026-10-17T15:23Z | yes | `usable` | none |

Both attempts ran the same image; `ct1799-rescale-out-a1` dispatched the rig at `main` `00562d599` and `ct1799-rescale-out-a2` at `917dac357`, after [#1828](https://github.com/flink-gcp/flink-connector-gcp/pull/1828) made the rig repeat a GCS read that meets a transient failure.
The two counted attempts come to USD 4.26 on the estimate's basis, against the USD 8.52 approved for four; the actual charges were not read from billing.

## Deviations from the preregistration

None.
No condition, interpretation or budget changed after a result was seen.
The preregistration's [revisions](0165-cloudtasks-rescale-out-preregistration-1799.md#revisions-after-the-first-dispatch) record the repair after `ct1799-rescale-out-a1`, which its last stop condition prescribes, the before-dispatch read that repair added, and the deadline correction carried to the #1778 preregistration, which changed nothing here.

No attempt met a platform cause, a recovery deadline, an unplanned restart, a missing task or a task listed twice; the creates the service refused as existing, 623 and 3,003 in the accepted attempt, are the replays interpretation 3 reads as no defect.

## Cleanup

After each attempt these were read directly: the queue `ct1362-<run-id>` described as not found and no `ct1362-` queue listed, nothing under the run's prefixes in `gs://flink-gcp-cloudtasks-benchmark`, `tier3-cloudtasks` holding no Pod or FlinkDeployment with its `tier3-idle` quota at zero Pods and PersistentVolumeClaims, nothing of the run in `tier3-system`, and the run record and the environment lock gone.
The `flink-gcp`, `tier3-bootstrap` and `tier3-operator` plans the receipt refreshed after each attempt were empty.

## Coverage of #1362

This rerun completes the row the [#1778 findings](0165-cloudtasks-fault-trials-findings-1778.md#coverage-of-1362) left open, and updates three others for the rescale out; the rest stand as those findings give them.

| #1362 acceptance item | Where it stands |
| --- | --- |
| Rescale in and out | Rescale out: `ct1799-rescale-out-a2`, from one sink subtask to two, listed all 180,000 sequences once; as for the rescale in, the listing tests the JobManager failover at two subtasks, and the savepoint restore is read from the deduplication counter, which shows no shortfall. Rescale in: `ct1778-rescale-in-a2`, as the #1778 findings record. #1245's partial-commit cases remain the controlled evidence for both directions |
| A successful savepoint upgrade with stable sink UIDs, the same queue, retained state and bounded restarts | The exercise now stops on a restart it did not plan ([#1798](https://github.com/flink-gcp/flink-connector-gcp/issues/1798)), and `ct1799-rescale-out-a2` met none |
| The latest authoritative checkpoint or savepoint preserved; no silent restart from an older checkpoint | For the rescale out, each restore was the planned one, savepoint 15 and then checkpoint 26, with the restore counts the exercise expects; the earlier trials' limits stand |
| Preregistered and approved resource, attempt, byte, duration and cost ceilings, restart bounds and stop conditions | The rerun stayed within its preregistered budget, two of four counted attempts, with every attempt classified under a preregistered condition |

## What this does not establish

The five faults reached `usable` on four images under rules that changed between them, so no single image is shown to pass all five; only the rescale out ran an image with #1796 and under the check for an unplanned restart.
The rescale out has one usable attempt at one input rate on Flink 2.2.1, so variance across repetitions and Flink 1.20 are not claimed, and the queue never dispatched, so nothing is shown about a handler.
The listing does not test the savepoint restore, which only the deduplication counter reads, and the matching failover count leaves open whether the failover restored a commit that had not finished.
The exported evidence does not establish whether the usable attempt exercised the retries of #1794 and #1796: no job failed on a `CreateTask` status, but the committer repeats a transient one without logging it and the rig does not export its error counters. Likewise, no GCS read is known to have needed #1827's retry, since the rig does not record a read it repeated.
