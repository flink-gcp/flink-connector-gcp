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

# BigQuery exactly-once fault sequences: findings

These are the findings of the exactly-once trials of [#2148](https://github.com/flink-gcp/flink-connector-gcp/issues/2148), which moved BigQuery's two exactly-once write methods to [ADR-0204](../0204-tier3-recovery-trials-tolerate-faults-and-fail-only-on-lost-data.md) after the at-least-once path of [#2150](0204-bigquery-fault-sequence-findings-2150.md).
The jobs are the ones the [BigQuery runbook's fault sequence](../../../kubernetes/apps/bigquery/README.md#fault-sequence) describes: the production `STORAGE_API_EXACTLY_ONCE` (EO) sink in `eo-10-sequence`, and the production `FILE_LOADS` sink with the background staging uploader on in `fl-10-uploader-sequence`, each writing a finite input of 1 KiB rows into ten tables.

Each trial ran once and injected all five faults while the input flowed: a savepoint upgrade, a JobManager deletion, a TaskManager deletion, a rescale in and a rescale out.
Both reached a `usable` verdict, with every input row in its table exactly once and with its expected payload, 19.4 and 19.3 minutes after their windows started, and both were idle within 21 minutes.
They were these trials' first attempts under ADR-0204, and neither lost or duplicated a row; the EO run showed the known symptom of [#2237](https://github.com/flink-gcp/flink-connector-gcp/issues/2237), and the FILE_LOADS sink logged a staging-delete warning that most likely reports leftovers that do not exist, [described below](#where-the-faults-landed-against-the-commits).

## Result

| Run ID | Trial | Run | Verdict | Rows valid, invalid | Sequences missing | Duplicate rows |
| --- | --- | --- | --- | --- | --- | --- |
| `bq2148-eo-seq-a1` | `eo-10-sequence` | [44921116819](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/44921116819) | `usable` | 368,640, 0 | 0 | 0 |
| `bq2148-fl-seq-a1` | `fl-10-uploader-sequence` | [44923101369](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/44923101369) | `usable` | 368,640, 0 | 0 | 0 |

Both runs were dispatched on 2026-11-21 on `main` at `6cb3c329636ed7e24fc4197e646710c66fd0d8e1`, the merge of [#2253](https://github.com/flink-gcp/flink-connector-gcp/pull/2253), which added the two trials.
They ran the `bigquery-recovery` image `sha256:aa14b09643e3709224e79ea6be37dee8006de2054eb78e7718fac34627f578b1` [described below](#publication).
Each input was 368,640 rows of 1 KiB at 1 MiB/s, 1,024 rows a second, to ten destinations.

One query read the ten tables after each job finished, and its report is the verdict.
Each table held its 36,864 expected sequences, each once, and no row had another run ID, a sequence outside the input, a wrong destination or a payload other than the one the application writes.
The queries billed 104,857,600 and 381,681,664 bytes, under their 4 GiB limit.

Both final receipts record `success` and `idle`, and `just tier3-analyze` recomputes `usable` for both with no problem and lists the five faults.
After the runs, the dataset `flink_gcp_tier3_bigquery` held none of their tables, and nothing remained under either run's prefix in `gs://flink-gcp-tier3-bigquery`.
The FILE_LOADS run's job history holds 80 load jobs, ten for each of its eight committed checkpoints and savepoints, all `DONE` without an error, so no load ran twice.

## The fault sequences

| Run | Fault | Injected (UTC) | Running again (UTC) | Seconds | Restored |
| --- | --- | --- | --- | --- | --- |
| EO | savepoint upgrade | 15:57:00 | 15:58:42 | 102 | savepoint 2 |
| EO | JobManager deletion | 15:58:43 | 15:59:35 | 52 | checkpoint 3 |
| EO | TaskManager deletion | 15:59:35 | 16:00:27 | 52 | checkpoint 4 |
| EO | rescale in, two writers to one | 16:00:28 | 16:02:10 | 102 | savepoint 6 |
| EO | rescale out, one writer to two | 16:02:10 | 16:03:54 | 104 | savepoint 8 |
| FILE_LOADS | savepoint upgrade | 16:18:28 | 16:19:56 | 88 | savepoint 2 |
| FILE_LOADS | JobManager deletion | 16:19:56 | 16:20:49 | 53 | savepoint 2 |
| FILE_LOADS | TaskManager deletion | 16:20:49 | 16:21:25 | 35 | savepoint 2 |
| FILE_LOADS | rescale in, two writers to one | 16:21:25 | 16:22:51 | 86 | savepoint 3 |
| FILE_LOADS | rescale out, one writer to two | 16:22:51 | 16:24:19 | 87 | savepoint 5 |

"Running again" is the poll at which the supervisor found the job running again after the fault, by the rule every scenario under ADR-0204 shares; the seconds are measured between the unrounded times, and the rig injected each next fault in the same poll.
The EO faults took 6 minutes 54 seconds and the FILE_LOADS faults 5 minutes 51 seconds.
The FILE_LOADS job checkpoints every 120 seconds, and none completed between the upgrade and the rescale in, so the JobManager's and the TaskManager's deletions both restored the upgrade's savepoint again.

The EO run recorded no event that ADR-0204 waits out as unplanned.
The FILE_LOADS run recorded two `recovery-rest-unavailable` events, at 16:18:46Z and 16:23:09Z, the 404 the supervisor reports when it finds no JobManager REST Service it owns, while the upgrade and the rescale out replaced the JobManager.
The supervisor waited both out.
In each run, the JobManager deletion's Deployment had eight attempts to create a replacement refused with `exceeded quota: tier3-idle` while the deleted Pod still counted, as in the [ALO run](0204-bigquery-fault-sequence-findings-2150.md#the-fault-sequence).
Pod events otherwise show Autopilot scaling up for the first Pods, startup probes failing while JobManagers started, a FILE_LOADS JobManager waiting for ConfigMaps the Operator had not yet recreated, and the faults' own deletions, and no preemption, eviction or node shutdown.

When the EO JobManager's deletion cancelled the upgraded job, the TaskManager logged `Trying to access closed classloader` at 15:58:46Z, the symptom of [#2237](https://github.com/flink-gcp/flink-connector-gcp/issues/2237).
The image predates that issue's fix in [#2249](https://github.com/flink-gcp/flink-connector-gcp/pull/2249), and the symptom changed nothing the run observed.

## Where the faults landed against the commits

[ADR-0204](../0204-tier3-recovery-trials-tolerate-faults-and-fail-only-on-lost-data.md) declines aiming a fault at an exactly-once commit and asks the findings to report which commit state each restore met where the evidence shows it.
Every restore whose commit state the evidence shows, the two EO deletions and all five FILE_LOADS restores, met a commit whose work had already finished, and the restored committer repeated it without effect.
Each FILE_LOADS savepoint stop ran its own savepoint's loads, and the rescale out's stop also ran a regular checkpoint's, which the restored committer then found done.

The EO committer logged 361 flushes, each a `Flushed stream … up to offset …` line, and no `already flushed` line.
Every restored writer's probe append was accepted, all 80 of them, so no writer abandoned a restored stream after its probe.

- **JobManager deletion:** checkpoint 3's 20 flushes, one per destination and writer, ended at 15:58:40.736Z, 2.6 seconds before the rig recorded the fault, which it does before deleting the Pod.
  After restoring checkpoint 3, the committer flushed the same 20 streams to the same offsets at 15:59:22Z, and the service answered each repeated flush as a success rather than with `ALREADY_EXISTS`.
- **TaskManager deletion:** checkpoint 4's 20 flushes ended at 15:59:34.030Z, 1.5 seconds before the rig recorded the fault, and the restored committer repeated the same 20 from 16:00:06.9Z and from 16:00:11.4Z, one subtask at a time.
- **Savepoint restores:** the first flushes after the upgrade and the two rescales named offsets no earlier line had named.
  The supervisor's reads of a Pod's log miss its last lines before a stop, so these logs cannot show whether the stopping job had already flushed the savepoint's offsets or whether the restored committer flushed them first; one of checkpoint 5's flushes was logged 1.9 seconds before the rescale in's suspension began, and the rest of that commit was not.
- **Rescale in:** the restored committer flushed both former writers' 20 streams to their savepoint offsets at 16:01:47Z to 16:01:48Z, and the single restored writer then logged that it dropped ten superseded destination states: for each destination it keeps one restored stream, chosen by checkpoint ID with ties broken by stream name, and drops the other.

The FILE_LOADS committer logged a submission or a re-attachment for every load, and BigQuery's job history gives each load's times.

- **Savepoint upgrade:** checkpoint 1's ten loads were created at 16:18:25.6Z to 16:18:26.0Z, and the last ended at 16:18:28.986Z, 0.29 seconds after the rig recorded the fault and before or about when it patched the application; the logs do not order the two.
  The Operator began suspending the job with a savepoint at 16:18:29.572Z, 0.59 seconds after that load ended, so no load was running when the stop began.
  The stopping job submitted the savepoint's own ten loads at 16:18:31.6Z to 16:18:31.9Z, and they ended by 16:18:35.1Z, before the Operator recorded the suspension at 16:18:36.5Z.
- **Restores of savepoint 2:** after the upgrade, the JobManager deletion and the TaskManager deletion, the restored committer re-attached to the same ten loads at 16:19:41Z, 16:20:26Z and 16:21:21Z, found them `DONE`, and completed the commit without submitting.
- **Rescale in:** savepoint 3's ten loads were created from 16:21:29.1Z and ended by 16:21:31.8Z, inside the stop, and the restored committer re-attached to them at 16:22:33Z, `DONE`.
- **Rescale out:** the Operator suspended the job from 16:22:52.568Z to 16:23:05.316Z.
  Checkpoint 4's ten loads, created from 16:22:56.155Z and ended by 16:22:58.406Z, ran inside that stop, followed by savepoint 5's, and the restored committer re-attached at 16:24:04Z to both checkpoint 4's and savepoint 5's, so savepoint 5 still carried checkpoint 4's committables.
  The stop ran through checkpoint 4's commit, and the restore found its loads `DONE`.

Each re-attached commit then logged `Failed to delete 20 of 20 staging objects; a bucket lifecycle rule is recommended to expire leftovers`, or 10 of 10, six times in all, while the commits of checkpoints 6 to 8, which submitted their own loads, logged none.
The staging storage counts any delete that does not return true as a failure, a missing object included, and the commit that first loaded those files most likely had already deleted them, although its last log lines were not read; the warning then reports leftovers that do not exist.
When checkpoint 4's and savepoint 5's re-attached commits completed, 0.6 seconds apart, the committer also logged once that checkpoints were completing less than 120 seconds apart, its warning about the per-table load quota.

## Timeline

| Event | EO (UTC) | EO, seconds into the window | FILE_LOADS (UTC) | FILE_LOADS, seconds into the window |
| --- | --- | --- | --- | --- |
| Window start | 15:48:53 | 0 | 16:11:11 | 0 |
| JobManager Pod created | 15:52:05 | 192 | 16:14:21 | 190 |
| JobManager container started | 15:53:36 | 283 | 16:15:28 | 257 |
| First input progress line | 15:56:42 | 469 | 16:18:16 | 426 |
| First fault | 15:57:00 | 488 | 16:18:28 | 438 |
| Last fault running again | 16:03:54 | 902 | 16:24:19 | 788 |
| Input's last progress line, the whole input processed | 16:07:35 | 1,122 | 16:29:16 | 1,086 |
| Job `FINISHED`, as the JobManager logged it | 16:07:38 | 1,126 | 16:29:24 | 1,093 |
| Query job created | 16:07:56 | 1,143 | 16:30:13 | 1,143 |
| Verdict | 16:08:16 | 1,163 | 16:30:27 | 1,157 |
| Run idle | 16:09:26 | 1,233 | 16:31:43 | 1,232 |

The EO input took 10 minutes 53 seconds through the five faults, 293 seconds longer than its length, and the FILE_LOADS input 11 minutes, 300 seconds longer.
Their last progress lines came 221 and 297 seconds after the last fault ran again, so the jobs had consumed about 139 and 63 seconds of input by then.
The FILE_LOADS job had consumed less because three restores went back to the same savepoint, whose input position was 14,230 rows, 13.9 seconds of input.
Its first fault came 12 seconds after its input began.
The job had been running for 135 seconds, waiting for its TaskManagers, when checkpoint 1 was triggered 4.3 seconds into the input; it completed 7.3 seconds in, and the first fault waits only for that.
No restore followed either input's last progress line; the EO job finished 3.4 seconds after it, and the FILE_LOADS job 7.5 seconds after it, the time its final checkpoint and that checkpoint's ten loads took.

## Publication

No image was published for these runs.
They ran the `bigquery-recovery` image of the [BigQuery fault sequence publication](0204-bigquery-fault-sequence-findings-2150.md#publication), built from `ed697211`, which already carried both write methods, and the `operator` mirror and `lifecycle-tools` image that [pins.cue](../../../kubernetes/images/pins.cue) and the Helm values still pinned.
Between that build and the dispatch, the BigQuery connector changed only through [#2249](https://github.com/flink-gcp/flink-connector-gcp/pull/2249), which runs its gRPC clients on a connector-owned event-loop group, and the base module gained a lookup-cache class the sinks do not use; the image predates #2249, which is why the EO run still showed #2237's symptom.

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:91250bb08c4f1ea358b201680e355139a530cb7318439926d62c6387d7c1369d` | 2026-11-16T16:41:15Z | 2026-11-23T16:41:15Z |
| `lifecycle-tools` | `sha256:0b0bd1495af3b2e9e5000cd09e068215b829cb2e36e78f3db6d714b3a85b1a38` | 2026-11-16T16:41:36Z | 2026-11-23T16:41:36Z |
| `bigquery-recovery` | `sha256:aa14b09643e3709224e79ea6be37dee8006de2054eb78e7718fac34627f578b1` | 2026-11-21T04:55:50Z | 2026-11-28T04:55:50Z |

Each run's approved expiry left about 47 hours before the earliest deletion-eligible time, more than the day dispatch requires.

## Observations

Against the runbook's plan, both verdicts came inside its 18 to 23 and 28 minute estimates, and the inputs were delayed slightly longer than the earlier runs it quotes.
Its estimates assume an input beginning as late as the earlier runs', 477 to 677 seconds into the window; these began at 469 and 426 seconds.
It expected the faults to delay the input by about as much as the earlier sequence runs' 245 to 292 seconds, and allowed a FILE_LOADS restore to replay up to one 120-second interval; they delayed the inputs by 293 and 300 seconds.
It expected six minutes of input to leave about four after the faults, room for two interruptions as long as the slowest fault then measured, 87 seconds; the EO run left 221 seconds, and its slowest fault took 104, so two of those would still have fitted, with 13 seconds to spare.
It expected the FILE_LOADS run's first fault to wait up to about 90 seconds longer than EO's for a completed checkpoint, but its first checkpoint completed 7.3 seconds into the input.
It expected the verdict about 18 to 23 minutes into the window for EO and until about 28 minutes for FILE_LOADS, and the runs reached it at 19.4 and 19.3 minutes, under the 20 to 30 minutes ADR-0204 asks of a run.

## What this does not establish

- No restore the evidence shows met a commit still in progress: the two EO deletions came after their checkpoints' flushes had ended, and every FILE_LOADS re-attachment found its loads `DONE`, including those of checkpoint 4, whose commit the rescale out's stop ran through.
  So neither run exercised a restored committer awaiting a load still running or resubmitting a failed load under a retry id, and the EO deletions did not exercise a restored committer flushing a stream for the first time.
  The cited [#1685 trial](0165-bigquery-fileloads-inflight-findings-1685.md) did not either: its loads had also finished before the restored commit re-attached, as ADR-0204 records.
- Neither EO deletion reached a writer that had appended past the state it restored, so the runs did not exercise the path that abandons a restored stream after its probe.
- Whether each EO savepoint's commit finished before its job stopped is not shown, because the stopping jobs' last log lines were not read; the FILE_LOADS savepoints' loads all ended before the Operator recorded each suspension.
- The FILE_LOADS logs carry no line from the background staging uploader, so whether any fault met an upload in flight is not shown.
- The rig recorded the FILE_LOADS deletions, which it does before deleting the Pods, 18 and 26 seconds after their restored jobs began processing, so each set the input back by at least 16,490 rows, 16 seconds of input, nothing near the 120 seconds the runbook's 28-minute estimate allows a restore to set it back.
- One run of each trial shows that the sinks kept every row exactly once through these five faults; it is not a measure of how often a fault loses or duplicates data.
