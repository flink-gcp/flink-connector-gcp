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

# FILE_LOADS recovery with the background staging uploader: findings

This records the deployed BigQuery FILE_LOADS trials of [#2034](https://github.com/flink-gcp/flink-connector-gcp/issues/2034), which ran the [recovery scenario](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md) with the background staging uploader on, as [ADR-0195](../0195-file-loads-offers-an-opt-in-background-staging-uploader.md) required before turning its default on.
All four trials reached a `usable` verdict on 2026-11-05, between 16:57Z and 20:15Z, and every one loaded every record exactly once.
Times are UTC.

## Setup

Each trial is `fl-10` with `--background-staging-upload-enabled true` and `--max-in-flight-upload-operations 2`: FILE_LOADS into 10 destinations, 1,843,200 records of about 1 KiB at a fixed 1 MiB/s, Avro staging with Zstandard, a 120-second checkpoint interval, 16 MiB rolls, two writer subtasks, and two recoveries per run.
The cells and their trial names come from [#2033](https://github.com/flink-gcp/flink-connector-gcp/issues/2033).
Every attempt pulled the `bigquery-recovery` image `sha256:0c273fb480dc04b3dd60bd6d19a02dc1bbe0b8d5725f8e412d5c6caef3962361` of the [BigQuery uploader trial publication](../../../kubernetes/images/README.md#bigquery-uploader-trial-publication), built from main `d11be40422901cd305ed70fdf4b90f0be6c5a4ec`.
The attempts ran the rig at the head of [#2044](https://github.com/flink-gcp/flink-connector-gcp/pull/2044) through `rig_sha`, before it merged, because `pins.cue` on `main` still named the earlier publication's lifecycle-tools image, too close to deletion for dispatch to admit.
`9a22a0bd4` and `913f3e200` differ only in prose; both carry the 600-second recovery deadline.

## Result

| Trial | Attempt | Run | Rig commit | Verdict |
| --- | --- | --- | --- | --- |
| `fl-10-uploader` | `bq2034-fl-10-uploader-a1` | [42063094407](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42063094407) | `bb6ab8d21` | inconclusive |
| `fl-10-uploader` | `bq2034-fl-10-uploader-a2` | [42069771659](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42069771659) | `9a22a0bd4` | `usable` |
| `fl-10-uploader-tm-replacement` | `bq2034-fl-10-uploader-tm-replacement-a1` | [42077629590](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42077629590) | `913f3e200` | `usable` |
| `fl-10-uploader-rescale-out` | `bq2034-fl-10-uploader-rescale-out-a1` | [42085195325](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42085195325), recovered by [42092491854](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42092491854) | `913f3e200` | `usable` |
| `fl-10-uploader-rescale-in` | `bq2034-fl-10-uploader-rescale-in-a1` | [42093905807](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42093905807) | `913f3e200` | `usable` |

`just tier3-analyze` over each exported evidence prefix recomputed the stored verdict, with 25 or 26 measurement samples and no reasons or problems for the four `usable` attempts.
Each receipt's `bigquery_trial` names `FILE_LOADS`, 10 destinations, `uploader: true` and the trial's fault.

The first attempt ended inconclusive at its upgrade, with `Recovery exercise deadline expired: upgrade`.
Its upgraded TaskManagers waited 75 seconds for Autopilot to provision a node (`FailedScheduling` at 16:39:54, scheduled at 16:41:09), the source ran 207 seconds after the upgrade began, and the first checkpoint after the restore, up to one 120-second interval later, missed the 300 seconds the upgrade then had.
Its five checkpoints before the upgrade completed, and nothing in it pointed at the uploader; [#2044](https://github.com/flink-gcp/flink-connector-gcp/pull/2044) raised BigQuery's recovery deadline to 600 seconds, as Bigtable's had been for the same wait, and the second attempt ran under it.

The rescale-out attempt's workflow failed after its lifecycle had completed: the step that proves the three infrastructure plans empty could not download the Operator chart (`Network is unreachable`), so the runner wrote no final receipt and kept the lock.
The automatic recovery workflow verified idle and empty plans, wrote the receipt and released the lock about five minutes later, and the analyzer then read the attempt as `usable`; its supervisor had recorded `recovery-complete` with the oracle below before the failure.

### Query oracle

| Attempt | Expected records | Rows | Distinct sequences | Duplicates | Missing | Invalid |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `bq2034-fl-10-uploader-a2` | 1,843,200 | 1,843,200 | 1,843,200 | 0 | 0 | 0 |
| `bq2034-fl-10-uploader-tm-replacement-a1` | 1,843,200 | 1,843,200 | 1,843,200 | 0 | 0 | 0 |
| `bq2034-fl-10-uploader-rescale-out-a1` | 1,843,200 | 1,843,200 | 1,843,200 | 0 | 0 | 0 |
| `bq2034-fl-10-uploader-rescale-in-a1` | 1,843,200 | 1,843,200 | 1,843,200 | 0 | 0 | 0 |

The oracle reads the end state of a whole run, so each row covers both of its recoveries: a savepoint upgrade, then a JobManager replacement, or the fault in place of one of them.
The rescale-out's upgrade proof records 3 checkpointed subtasks before and 5 after, the rescale-in's 5 and 3, and the TaskManager replacement's failover proof records the replaced Pod.
That is one run of each fault, not a variance.

### Recovery

| Attempt | Recovery | Fault | Restored from | Restored after | Checkpoint acknowledged after |
| --- | --- | --- | --- | ---: | ---: |
| `bq2034-fl-10-uploader-a2` | upgrade | `savepoint-upgrade` | savepoint 6 | 43.1 s | 93.1 s (chk-7) |
| `bq2034-fl-10-uploader-a2` | failover | `jm-replacement` | checkpoint 10 | 27.3 s | 90.5 s (chk-11) |
| `bq2034-fl-10-uploader-tm-replacement-a1` | upgrade | `savepoint-upgrade` | savepoint 6 | 60.1 s | 100.5 s (chk-7) |
| `bq2034-fl-10-uploader-tm-replacement-a1` | failover | `tm-replacement` | checkpoint 10 | 10.7 s | 68.9 s (chk-11) |
| `bq2034-fl-10-uploader-rescale-out-a1` | upgrade | `rescale-out` | savepoint 6 | 38.5 s | 83.3 s (chk-7) |
| `bq2034-fl-10-uploader-rescale-out-a1` | failover | `jm-replacement` | checkpoint 10 | 27.3 s | 144.4 s (chk-11) |
| `bq2034-fl-10-uploader-rescale-in-a1` | upgrade | `rescale-in` | savepoint 6 | 54.3 s | 98.9 s (chk-7) |
| `bq2034-fl-10-uploader-rescale-in-a1` | failover | `jm-replacement` | checkpoint 10 | 36.8 s | 148.0 s (chk-11) |

Each recovery is timed as [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#recovery-timing-from-flinks-timestamps) defines it, from `just tier3-analyze`.
Every proof came within the 600-second deadline, the slowest at 148 seconds.

## The uploader carried full chunks

Neither the connector nor the application logs or meters the uploader, so these runs show it configured, and the staged files' sizes show that it had work.
A sample of 20 of `bq2034-fl-10-uploader-a2`'s 180 load jobs, every ninth in BigQuery's job history, read two input files per steady-state job and 12.23–12.25 MB per job, about 6.1 MB (5.8 MiB) per staged file after compression; jobs around the input's start, the recoveries and its end were smaller.
Each staged file therefore crossed the 4 MiB upload chunk once per checkpoint and, with the uploader on, sent that full chunk from the upload thread; its close at the checkpoint, below the 16 MiB roll, stayed on the checkpoint's own finalization, as the uploader design leaves it.

## Observations

These are the largest values any writer subtask or TaskManager reported in the measurement windows; they describe these runs at their fixed rate, and none is a threshold.

| Attempt | Writer busy ms / s | Back-pressured ms / s | Heap used (of 644 MiB) | Direct | Checkpoint end-to-end |
| --- | ---: | ---: | ---: | ---: | ---: |
| `bq2034-fl-10-uploader-a2` | 49–72 | 0 | 576 MiB | 173 MiB | ≤ 2.1 s |
| `bq2034-fl-10-uploader-tm-replacement-a1` | 61–69 | 0 | 575 MiB | 172 MiB | ≤ 2.5 s |
| `bq2034-fl-10-uploader-rescale-out-a1` | 46–65 | 0 | 576 MiB | 173 MiB | ≤ 2.1 s |
| `bq2034-fl-10-uploader-rescale-in-a1` | 49–64 | 0 | 520 MiB | 172 MiB | ≤ 2.1 s |

- **Busy time** stayed under 75 ms per second, against 81–121 in the uploader-off [`bq1313-fl-10-a1`](0165-bigquery-fileloads-findings-1313.md#observations); the two are different runs on different days, so this is consistent with the uploader taking upload waits off the task thread rather than a measurement of it.
- **Heap** peaked at 520–576 MiB, against 474 in `bq1313-fl-10-a1`; the samples rise and fall with collection, `bq2034-fl-10-uploader-a2`'s 30 readings spanning 94–576 MiB with a median near 300, and cumulative GC time stayed under two seconds. The uploader's own share at these settings is two 4 MiB chunks per writer; the rest of the difference was not attributed.
- **Back-pressure** read 0 in every window.

## Cleanup

Every attempt ended with the supervisor's and the runner's cleanup, or the recovery workflow's, reporting success, and its receipt records verified idle and three empty plans.
