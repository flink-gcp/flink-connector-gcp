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

# BigQuery FILE_LOADS in-flight failover trial: findings

This records the deployed FILE_LOADS trial of [#1685](https://github.com/flink-gcp/flink-connector-gcp/issues/1685), within [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#bigquery-file_loads-failover-while-a-load-job-runs).
It sits beside the [findings of #1552](0165-bigquery-fileloads-findings-1313.md), whose failover landed after every load of its commit had finished.
The trial ran under [that trial's preregistration](0165-bigquery-fileloads-preregistration-1313.md), plus the [additions preregistered on #1685](https://github.com/flink-gcp/flink-connector-gcp/issues/1685#issuecomment-6118809133); the one change was the rig commit, which deletes the JobManager only once a load job of the run is listed `RUNNING`.
The one trial, `fl-10`, reached a `usable` verdict on its first attempt on 2026-10-12, with its JobManager deleted while all ten loads of a commit ran server-side.
Times are UTC.

## Result

| Trial | Mode | Destinations | Attempt | Run | Rig commit | Verdict |
| --- | --- | ---: | --- | --- | --- | --- |
| `fl-10` | FILE_LOADS | 10 | `bq1685-fl-10-a1` | [38212203234](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38212203234) | `20fa691b2` | `usable` |

The reads before dispatch passed at 06:21: no environment lock or run record existed, the preceding Pub/Sub trial `ps1720-jm-a1` of [#1720](https://github.com/flink-gcp/flink-connector-gcp/issues/1720) had finished and released its lock, the weekly E2E was not running, and the rig commit had passed its review rounds and CI.
The final receipt records `success: true`, the `usable` verdict with no reasons, verified idle, and empty plans for `flink-gcp`, `tier3-bootstrap` and `tier3-operator`, read at 07:16:46.
`just tier3-analyze` over the exported evidence recomputed `usable`, with 25 measurement samples and no reasons or problems.
The attempt used the application image `bigquery-recovery@sha256:a8d13036…` of the [FILE_LOADS trial publication](../../../kubernetes/images/README.md#bigquery-file_loads-trial-publication); its window ended at 07:53:11, inside the 2026-10-15T16:22:01Z deadline.

### Query oracle

| Attempt | Expected records | Rows | Distinct sequences | Duplicates | Missing | Invalid | Billed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `bq1685-fl-10-a1` | 1,843,200 | 1,843,200 | 1,843,200 | 0 | 0 | 0 | 100 MiB |

Every table held exactly its 184,320 sequences, on the first query slot.
The job history agrees: the run's 190 load jobs are 19 commits of one job per destination, all `DONE` without an error, and the interrupted commit, chk-11 of the upgraded job, has exactly its ten, so nothing was resubmitted.

### Recovery

| Attempt | Baseline window ends | Upgrade begun | Failover begun | Finishing | Visibility | Complete |
| --- | --- | --- | --- | --- | --- | --- |
| `bq1685-fl-10-a1` | 06:53:14 | 06:54:01 | 06:57:54 | 06:59:27 | 07:11:45 | 07:12:06 |

The upgrade restored its savepoint and the failover the latest completed checkpoint.

### The failover while the loads ran

The times below are BigQuery's job times, read with `bq show -j` after the run, unless they name the supervisor or a log; log times are Flink's own.

| Event | Time |
| --- | --- |
| chk-11's ten loads created | 06:57:54.047–06:57:54.362 |
| Supervisor lists eight of them `RUNNING` | 06:57:54.023 (start of the listing) |
| JobManager delete request (`failover_at`) | 06:57:54.718 |
| Supervisor lists all ten `RUNNING` again | 06:57:55.892 |
| Committer (2/2) task `CANCELED` | 06:57:55.691 (TaskManager log) |
| Committer (1/2) task `CANCELING` | 06:57:55.696 (TaskManager log) |
| The ten loads end | 06:57:57.951–06:58:00.560 |
| Committer (1/2) logs its awaited load complete, then `CANCELED` | 06:58:01.024, 06:58:01.027 |
| Restored committers re-attach to all ten, each `DONE` | 06:58:37.578–06:58:37.667 (Flink log) |

The receipt's `failover_in_flight` records eight loads in `running_through`: listed `RUNNING` before the delete request and again after it.
The job history shows more than the record can: all ten loads were running when the delete request went out and when both committer tasks began cancelling, and they ended between 2.3 and 4.9 seconds after that cancellation began.
Committer (2/2) reached `CANCELED` before any of them ended.
Committer (1/2), which had submitted all ten, stayed `CANCELING` until it logged the load it was awaiting complete, 5.3 seconds after its cancellation began, and reached `CANCELED` 3 milliseconds after that log line, about half a second after the last load ended server-side; so the loads outlived the JobManager's deletion and ran through the cancellation of the task that submitted them, but that task finished cancelling only after them.
The log does not show why the cancellation waited for the load, so this record does not claim a bound on how long a longer load could hold it.
The restored committers re-attached to all ten loads 43 seconds after the delete request, found them `DONE`, and completed chk-11 without resubmitting, as the issue's premise expected of the re-attachment, though not to jobs still running: a load lasts a few seconds, and the restored committers start well after that.

### FILE_LOADS acceptance items

Cleanup recorded no unfinished connector job, no temporary table and no object left under the staging prefix, and the run's tables were deleted.
The interrupted commit left no staged file behind either: the restored committer's completed commit deleted what its loads had loaded.

## Input margin

Input completed at 07:11:12.8, 105.7 seconds after the 600-second post-recovery window ended at 07:09:27.0.
The upgrade began 46.7 seconds after the baseline window ended, and the in-flight wait ended at 06:57:54, so the run used part of the margin the [ADR section](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#bigquery-file_loads-failover-while-a-load-job-runs) computed from `bq1313-fl-10-a1`; a run whose upgrade waited a whole checkpoint interval and whose wait ran its full 130 seconds would have less, which is why the wait is not longer.

## Deviations from the preregistration

None.
The dispatch ran the reviewed head of [PR #1762](https://github.com/flink-gcp/flink-connector-gcp/pull/1762) through `rig_sha` before the change merged, as [a run may check out a chosen rig commit](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#a-run-may-check-out-a-chosen-rig-commit) allows; the measurement is of that commit, which this pull request merges unchanged apart from this record.
