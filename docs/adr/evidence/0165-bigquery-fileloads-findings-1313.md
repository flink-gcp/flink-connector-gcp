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

# BigQuery FILE_LOADS deployed recovery trial: findings

This records the deployed FILE_LOADS trial of [#1552](https://github.com/flink-gcp/flink-connector-gcp/issues/1552), the correctness half of [#1313](https://github.com/flink-gcp/flink-connector-gcp/issues/1313), run against the [preregistration](0165-bigquery-fileloads-preregistration-1313.md) within [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md).
The one preregistered trial, `fl-10`, reached a `usable` verdict on its first attempt on 2026-10-10.
Its JobManager failover landed while the committer was still completing a commit whose load jobs had all finished server-side, and the restored committer completed it by re-attaching to them; no load job was still running at either recovery, and [#1685](https://github.com/flink-gcp/flink-connector-gcp/issues/1685) owns that case.
Its final receipt records `success: false`, for a reason the rig did not retain; [Why the receipt says false](#why-the-receipt-says-false) explains why the attempt is accepted.
Times are UTC.

## Result

| Trial | Mode | Destinations | Attempt | Run | Rig commit | Verdict |
| --- | --- | ---: | --- | --- | --- | --- |
| `fl-10` | FILE_LOADS | 10 | `bq1313-fl-10-a1` | [38011689555](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38011689555) | `30247860e` | `usable` |

The preregistration's reads before dispatch passed between the OpenTofu apply that granted `bigquery.jobs.listAll`, which finished at 00:21, and the dispatch at 01:04: the refreshed `flink-gcp` and `tier3-bootstrap` plans were empty, and the apply's own drift check found the `tier3-operator` plan empty; `tier3BigQuerySupervisorJobs` read back with `bigquery.jobs.list` and `bigquery.jobs.listAll` and was bound to `tier3-supervisor`; and no lock, run record or approval without a receipt existed.
The final receipt records the `usable` verdict with no reasons, verified idle, and empty plans for `flink-gcp`, `tier3-bootstrap` and `tier3-operator`, read at 01:51:47.
`just tier3-analyze` over the exported evidence recomputed `usable`, with 26 measurement samples and no reasons or problems.
The verdict's six families are the sink's task, network and connector metrics, TaskManager memory, the committer's metrics and per-subtask checkpoint statistics.
The attempt used the application image `bigquery-recovery@sha256:a8d13036…` of the [FILE_LOADS trial publication](../../../kubernetes/images/README.md#bigquery-file_loads-trial-publication), created on 2026-10-09; its window ended at 02:34:44, inside the 2026-10-15T16:22:01Z deadline.

### Query oracle

| Attempt | Expected records | Rows | Distinct sequences | Duplicates | Missing | Invalid | Billed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `bq1313-fl-10-a1` | 1,843,200 | 1,843,200 | 1,843,200 | 0 | 0 | 0 | 100 MiB |

Every table held exactly the 184,320 sequences routed to it, `sequence mod destinations`.
No duplicate appeared across a savepoint upgrade and a JobManager failover, which is what the FILE_LOADS commit owes; this is one run, not a variance.
The job history agrees: the run's 190 load jobs are 19 commits of one job per destination, all `DONE` without an error, and their 380 source files each appear in exactly one job.
Billing is BigQuery's 10 MiB minimum per table read, far below the 4 GiB reservation.

### Recovery

| Attempt | Baseline window | Upgrade begun | Failover begun | Finishing | Visibility | Complete |
| --- | --- | --- | --- | --- | --- | --- |
| `bq1313-fl-10-a1` | 01:15:02 | 01:29:02 | 01:30:53 | 01:32:09 | 01:46:38 | 01:46:58 |

The upgrade restored its savepoint and the failover the latest completed checkpoint, each proved in under two minutes from the stage's start.
The first job failed its first checkpoint trigger because its source task was not yet running, then completed 8 before the upgrade; after the failover the upgraded job's JobManager reported 9 completed, 1 restored and none failed.

No load job was running server-side at either recovery; the times below are BigQuery's job times unless they name the committer.
The last load of the commit before the upgrade ended at 01:28:47.9, and the savepoint's own commit's loads ran from 01:29:07.3 to 01:29:09.5 inside the stopping job.
chk-10's last acknowledgement arrived at 01:30:42.9 and the JobManager logged it completed at 01:30:46.1; its commit's 10 loads were submitted at 01:30:47 and had all ended server-side by 01:30:52.0, about 1.7 s before the JobManager was deleted. The committer had logged 9 of them complete, the last at 01:30:51.1, when its task was cancelled at 01:30:54.5, so the failover landed inside that commit. chk-11 was triggered only after the restore, at 01:31:47.
Both restored committers re-attached to their predecessor's jobs instead of resubmitting: to the savepoint commit's 10 at 01:30:29, and to chk-10's 10 at 01:31:36, after which the restored committer logged all 10 complete, finishing the interrupted commit.

### FILE_LOADS acceptance items

| Item | Result |
| --- | --- |
| Connector-issued jobs unfinished at cleanup | none |
| Temporary tables | none |
| Staged objects left under `staging/` when cleanup recorded them | 0 objects, 0 bytes; the listing was readable |
| Run tables and `tmp_` tables after the run | none, read directly |
| Objects under `runs/<run-id>/` and `.inprogress/…/runs/<run-id>/` after the run | none, read directly |
| Run load jobs pending or running after the run | none; all 190 `DONE`, none with an error, read directly |
| Run objects in `tier3-system`, the run's control record and the environment lock | none, read directly after the recovery workflow |

No checkpoint was pending when the JobManager was deleted, so the documented orphan case, a staged file finalized for a checkpoint a failure abandons, could not arise in this run.
The rig's state cleanup deletes the run's prefixes itself, so reading them empty afterwards confirms that cleanup, not the connector; the connector's account is the staging record above.

## Observations

These are the largest values any subtask or TaskManager reported among each stage's samples.
Samples are at least 60 seconds apart, 74–111 s in this run: 13 in the baseline stage, from 01:14 to 01:29, which includes its warm-up, and 11 in the finishing stage, from 01:33 to 01:46.
The commit duration column is the range of the committer's last-commit gauge.
They describe this run at its fixed rate; none is a threshold or a production target.

| Stage | Records in / s per writer | Busy ms / s | Back-pressured ms / s | Heap used (of 644 MiB) | Direct | GC time | Open destinations | Commit duration |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 513 | 121 | 0 | 466 MiB | 173 MiB | 827 ms | 10 | 5.3–9.6 s |
| finishing | 523 | 81 | 0 | 474 MiB | 174 MiB | 1,120 ms | 10 | 5.9–11.5 s |

- **Finalization.** Flink 2.2.1 runs the writer's finalization in its pre-barrier step, before the synchronous part's timer starts, so it appears in neither the synchronous duration, 0–13 ms, nor the asynchronous one, 0–4 ms. Per writer subtask, the end-to-end checkpoint duration was 1.5–3.3 s, of which the barrier's start delay was 0.25–0.65 s and the remainder, finalization plus the barrier broadcast and acknowledgement, 1.2–3.1 s. The committer waits for the slower writer: its start delay was 1.5–2.4 s and its alignment up to 1.1 s, and its own work after the last barrier took under 70 ms.
- **Commit.** The committer's last-commit gauge read 5.3–11.5 s in the 16 of 19 commits a sample caught, against a 120-second interval; it spans the commit from planning through its last completed job and the deletion of its staged files, while the job history puts each load at 1.1–6.1 s, median 2.4 s, and 1.4–11.8 MiB of Avro. The connector's `maxConcurrentDestinations` default of 8 bounds how many destinations the committer works on at once, both while it submits their jobs and while it awaits them; submission is quick, so server-side all 10 loads of every commit ran overlapping, and the gauge's one reading of 8 active and 1 queued, at 01:22:49, came while that commit's jobs were already running.
- **Load jobs.** The job history counts 190, one per destination per commit. The committer's `loadJobsSubmitted` counter counts submission attempts, including the 20 that re-attached after the recoveries, and read 90 in the last finishing sample.
- **Staging.** Each writer staged one file per destination per checkpoint, 380 in all, below the 16 MiB rollover; neither capacity nor idle evictions occurred.
- **Throughput** followed the source's fixed 1 MiB/s, about 1,024 records of 1 KiB per second, which each of the two writers received half of. No sample showed back-pressure.
- **Memory.** Heap reached 474 MiB of 644 MiB, about twice what the EO trials of [#1312](https://github.com/flink-gcp/flink-connector-gcp/issues/1312) reported at the same rate; direct memory stayed under 175 MiB and mapped memory at 0. Its cause was not measured. GC time is cumulative since the TaskManager started and stayed near one second.
- **Coverage gap.** The upgrade stage's one sample returned only the checkpoint and memory families, and the failover stage's one sample all six; the verdict requires baseline and finishing, which both read all six.

## Supervisor job listing

ADR-0165's [#1551](https://github.com/flink-gcp/flink-connector-gcp/issues/1551) refinement records why the supervisor holds `bigquery.jobs.listAll`: a listing without it returns other principals' job ids as a placeholder, measured before this trial, and the rig would then have read the workload's jobs as foreign.
The read-back before dispatch, under [Result](#result), is the evidence that the grant and its binding were in place, because a missing binding would leave the supervisor listing only its own jobs, which nothing in the run would show.
The run itself does not test the listing: it ran before the oracle read and in cleanup, raised nothing and found no unfinished job, but every load had finished by 01:46:07.9, before the first of those listings.

## Every admitted attempt

| Attempt | Run | Rig commit | Stopped at | Cause | Repair |
| --- | --- | --- | --- | --- | --- |
| `bq1313-fl-10-a1` | [38011689555](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38011689555) | `30247860e` | complete | `usable`; the receipt says `success: false` after the supervisor's evidence-failure flag was set during cleanup, cause not retained | none; [#1683](https://github.com/flink-gcp/flink-connector-gcp/issues/1683) retains such a cause |

No dispatch was refused before admission.

### Why the receipt says false

The receipt's `success` requires the cleaned record's own success, which cleanup writes as false whenever the writing process's evidence-failure flag is set.
The supervisor's `cleanup-ready` event, at 01:47:59, shows `success: false` with state and queue clean after the supervisor had concluded the exercise as finished, so its flag was set; the runner's later `cleanup-ready` follows from that record.
The exercise refuses to continue once the flag is set, and its last check passed as it recorded the completed stage, just before the `recovery-complete` event at 01:46:58, so the flag was set between that event and the supervisor's `cleanup-ready`: by a failed control-record write in that cleanup pass, which sets the flag and prints nothing, or by a failed evidence write, which prints only its exception class to the supervisor Pod's standard output.
Neither is retained: the run's final supervisor log is empty, Cloud Logging holds no container log of that Pod, and the evidence bucket keeps no object generations.
ADR-0165's deployed-verdict section reads the receipt's `success` in one direction only, because it is a conjunction the runner may refuse for reasons of its own, and the preregistration accepts an attempt when its receipt and `just tier3-analyze` agree on a `usable` verdict, which they do; every acceptance item above holds.
The workflow's final step fails on that receipt, which is why the run reports failure.
The owner accepted the attempt on that basis, and [#1683](https://github.com/flink-gcp/flink-connector-gcp/issues/1683) is filed to retain the cause in later runs.

## Deviations from the preregistration

- **The receipt says `success: false`, and the workflow failed at its final step**, as [Why the receipt says false](#why-the-receipt-says-false) explains.
- **No load job was running server-side at either recovery**, so the issue's "load jobs that outlive a Pod" was exercised as a commit the failover interrupted after its loads had finished, completed by re-attachment; [#1685](https://github.com/flink-gcp/flink-connector-gcp/issues/1685) owns a failover while a load job is still running.
- **The dispatch crossed the weekly E2E exclusion, whose timing note was wrong.** The preregistration said to dispatch outside that workflow's run and placed its start at its 01:17 cron time on Saturdays, and this attempt, admitted at 01:04 on a Saturday, ran across that time. Its scheduled runs on the two previous Saturdays had started at 06:09 and 06:30, and none had started on 2026-10-10 by the time this attempt finished, so no E2E run overlapped it.
- **Pre-dispatch reads.** The `tier3-operator` plan was taken from the apply workflow's post-apply plan, which reported no changes, because a local plan of that root needs the Operator chart archive the rig prepares; the reads fell between the apply at 00:21 and the dispatch at 01:04 rather than immediately before it.
- **Spend.** USD 2.35 was approved before the dispatch. The observed usage is 100 MiB of billed queries, 190 load jobs on the free shared slot pool and 380 staged files; total charges were not read from billing, so whether they stayed within the estimate is not established.
