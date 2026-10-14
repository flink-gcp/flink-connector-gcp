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

# BigQuery TaskManager replacement and rescale trials: findings

This records the deployed BigQuery trials of [#1779](https://github.com/flink-gcp/flink-connector-gcp/issues/1779), run against the [preregistration](0165-bigquery-fault-trials-preregistration-1779.md) within [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md).
All three preregistered `eo-10` trials reached a `usable` verdict on their first attempt on 2026-10-14, between 15:19Z and 17:47Z.
The `fl-10` fault trials were offered and not run, as the owner decided on 2026-10-14 and the preregistration's [campaign](0165-bigquery-fault-trials-preregistration-1779.md#campaign) records, so nothing here is a claim about FILE_LOADS under these faults.
Times are UTC.

## Result

| Trial | Attempt | Run | Rig commit | Verdict |
| --- | --- | --- | --- | --- |
| `eo-10-tm-replacement` | `bq1779-eo-10-tm-replacement-a1` | [38588239047](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38588239047) | `837d9c88f` | `usable` |
| `eo-10-rescale-out` | `bq1779-eo-10-rescale-out-a1` | [38595896893](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38595896893) | `837d9c88f` | `usable` |
| `eo-10-rescale-in` | `bq1779-eo-10-rescale-in-a1` | [38602918901](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38602918901) | `837d9c88f` | `usable` |

Each final receipt records `success: true`, stage `complete`, verified idle and empty plans for `flink-gcp`, `tier3-bootstrap` and `tier3-operator`.
`just tier3-analyze` at the rig commit recomputed the same verdict for each from the three exported evidence prefixes, with 25, 26 and 25 measurement samples and no reasons or problems; the [deployed verdict](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#bigquery-deployed-verdict) section says which revision reads archived evidence.
Each receipt's `bigquery_trial.fault` and the analyzer's timing table name the trial's fault, which the preregistration's [acceptance criteria](0165-bigquery-fault-trials-preregistration-1779.md#acceptance-criteria) require because the verdict does not check it.
All three pulled the images of the [fault trial publication](../../../kubernetes/images/README.md#bigquery-fault-trial-publication).
The pilot was `usable`, so the other two ran; their estimate was the preregistered USD 7.05, and the actual charges were not read from billing.

### Query oracle

| Attempt | Expected records | Rows | Distinct sequences | Duplicates | Missing | Invalid | Billed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `bq1779-eo-10-tm-replacement-a1` | 1,843,200 | 1,843,200 | 1,843,200 | 0 | 0 | 0 | 100 MiB |
| `bq1779-eo-10-rescale-out-a1` | 1,843,200 | 1,843,200 | 1,843,200 | 0 | 0 | 0 | 100 MiB |
| `bq1779-eo-10-rescale-in-a1` | 1,843,200 | 1,843,200 | 1,843,200 | 0 | 0 | 0 | 100 MiB |

Every table held exactly the sequences routed to it, and each oracle passed on its first query slot.
The oracle reads the end state of a whole run, so each result covers both of its recoveries: the exactly-once sink wrote no duplicate and lost no sequence across a TaskManager replacement, a savepoint restore of one writer's state into two, and one of two writers' state into one.
That is one run of each fault, not a variance.

### Recovery

| Attempt | Baseline window | Upgrade begun | Failover begun | Finishing | Visibility | Complete |
| --- | --- | --- | --- | --- | --- | --- |
| `bq1779-eo-10-tm-replacement-a1` | 15:29:04 | 15:42:34 | 15:44:25 | 15:46:00 | 16:01:05 | 16:01:26 |
| `bq1779-eo-10-rescale-out-a1` | 16:20:33 | 16:34:02 | 16:35:53 | 16:37:33 | 16:52:35 | 16:52:56 |
| `bq1779-eo-10-rescale-in-a1` | 17:08:48 | 17:22:23 | 17:24:13 | 17:25:53 | 17:40:43 | 17:41:04 |

Each recovery is timed as [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#recovery-timing-from-flinks-timestamps) defines it, from `just tier3-analyze`: `restored after` is a lower bound and `checkpoint acknowledged after` an upper bound on when the restored job was processing again.
The last column, read from the JobManager log, is when the last restored task switched to `RUNNING` after a failover.

| Attempt | Recovery | Fault | Restored from | Restored after | Checkpoint acknowledged after | Last task running after |
| --- | --- | --- | --- | ---: | ---: | ---: |
| `bq1779-eo-10-tm-replacement-a1` | upgrade | `savepoint-upgrade` | savepoint 29 | 45.1 s | 99.4 s (chk-30) | |
| `bq1779-eo-10-tm-replacement-a1` | failover | `tm-replacement` | checkpoint 30 | 10.7 s | 86.9 s (chk-32) | 45.7 s |
| `bq1779-eo-10-rescale-out-a1` | upgrade | `rescale-out` | savepoint 29 | 50.6 s | 96.7 s (chk-30) | |
| `bq1779-eo-10-rescale-out-a1` | failover | `jm-replacement` | checkpoint 30 | 36.1 s | 83.6 s (chk-32) | 40.0 s |
| `bq1779-eo-10-rescale-in-a1` | upgrade | `rescale-in` | savepoint 29 | 49.5 s | 100.2 s (chk-30) | |
| `bq1779-eo-10-rescale-in-a1` | failover | `jm-replacement` | checkpoint 30 | 35.9 s | 90.1 s (chk-32) | 38.8 s |

The upgrades restored their savepoint 45 to 51 seconds after they began, and the JobManager failovers 36 seconds after the deletion began; the earlier trials' figures are in the [Storage Write](0165-bigquery-trial-findings-1312.md#recovery), [FILE_LOADS](0165-bigquery-fileloads-findings-1313.md#recovery) and [in-flight failover](0165-bigquery-fileloads-inflight-findings-1685.md#recovery) findings.
The two bounds are far apart for the TaskManager replacement, so its restore time is not comparable with the others': it restored first but ran all its tasks last.

#### TaskManager replacement

The failover deleted `taskmanager-1-1`, the TaskManager whose task had reported the latest input; it held the source and one writer.
The JobManager kept its UID through the failover, and the other TaskManager, `taskmanager-1-2`, kept its UID from the last inventory before the failover to the last of the run, so only the approved Pod was lost.
The JobManager log gives the sequence:

- At 15:44:25.6, 0.6 seconds after the deletion began, the JobManager closed the TaskManager's connection because the TaskExecutor announced it was shutting down. A loss without that announcement, such as a node's, is detected by the heartbeat instead, through its timeout or failed heartbeat requests, which these trials did not measure.
- At 15:44:25.8 it restarted all three tasks, the writer on the surviving TaskManager included, since every edge of this graph is all-to-all, and the job stayed `RESTARTING` for the fixed restart strategy's 10-second delay.
- At 15:44:35.8 it restored checkpoint 30, which is the 10.7 seconds above.
- At 15:44:35.9, once the restarted job declared its second slot, it requested `taskmanager-1-5`, which registered at 15:45:00.4; the last writer ran at 15:45:10.7.

The namespace quota has no slot for a replacement, as the preregistration expected.
The JobManager's first replacement request, for `taskmanager-1-3` at 15:44:25.8, was refused with `exceeded quota: tier3-idle` while the deleted Pod still counted (`used: pods=3`).
By then the restarting job required one slot, so the JobManager cancelled its second request, for `taskmanager-1-4`, and released the worker it no longer needed.
The replacement that ran was requested only when the job left `RESTARTING`, ten seconds later, so no delay attributable to the quota is visible in this run.

#### Rescales

The rescale-out trial's baseline ran one TaskManager and one writer, and the restored job two of each: its proving checkpoint reported 3 subtasks against 2 before the upgrade, the input vertex and one per writer.
The rescale-in trial moved from 3 subtasks to 2 and ran one TaskManager after the upgrade.
The checkpointed state followed the writers: 11.2 to 11.4 KiB at two writers and 6.0 KiB at one.
No rescale's request for a TaskManager was refused: from the upgraded JobManager's request to registration took 24.3 seconds for the rescale-out's two and 19.6 for the rescale-in's one, against 24.5 for the replacement.

## Observations

These are the largest values any writer subtask or TaskManager reported in each measurement window, as in the [Storage Write findings](0165-bigquery-trial-findings-1312.md#observations).
They describe these runs at their fixed rate; none is a threshold.

| Attempt | Window | Writers | Records in / s | Bytes out / s | Busy ms / s | Back-pressured ms / s | Heap used (of 644 MiB) | Direct | GC time | Append retries |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `tm-replacement-a1` | baseline | 2 | 513 | 513 KiB | 26 | 0 | 230 MiB | 181 MiB | 849 ms | 0 |
| `tm-replacement-a1` | finishing | 2 | 513 | 514 KiB | 34 | 0 | 222 MiB | 179 MiB | 841 ms | 10 |
| `rescale-out-a1` | baseline | 1 | 1,026 | 1,108 KiB | 28 | 0 | 228 MiB | 173 MiB | 706 ms | 0 |
| `rescale-out-a1` | finishing | 2 | 515 | 513 KiB | 56 | 0 | 242 MiB | 185 MiB | 875 ms | 10 |
| `rescale-in-a1` | baseline | 2 | 513 | 513 KiB | 37 | 0 | 223 MiB | 179 MiB | 736 ms | 0 |
| `rescale-in-a1` | finishing | 1 | 1,026 | 1,026 KiB | 21 | 0 | 233 MiB | 179 MiB | 721 ms | 10 |

- **Throughput** followed the source's fixed 1 MiB/s: about 1,024 records per second in all, which one writer took whole and two writers took half each.
  One rescale-out baseline sample read 1,108 KiB/s out, above that rate; these trials did not attribute it.
- **Busy time** stayed under 60 ms per second at one writer and at two, with no back-pressure in any window.
- **Memory** stayed flat: heap under 250 MiB of 644 MiB and direct memory under 190 MiB, whether one TaskManager carried both halves of the input or two shared it.
- **Append retries** read 10 for each writer in every finishing window and 0 in every baseline, and the TaskManager logs give their cause.
  Right after each failover (15:45:14–19, 16:36:41–46 and 17:24:56–17:25:00) every recovered writer's restore probe was rejected with `OFFSET_ALREADY_EXISTS` once per destination, because the job before the fault had appended past the restored offset; none followed a savepoint upgrade.
  As the [buffered-stream writer](../../../flink-connector-gcp-bigquery/src/main/java/io/github/flink/gcp/connector/bigquery/sink/storage/writer/BigQueryBufferedStreamWriter.java) documents, the writer then abandons that stream and re-appends the batch to a fresh one, which the counter counts as a retry; nothing appended past a restored offset is ever named by a committable, so the oracle found no duplicate.
- **In-flight appends** read 10 in one sample each of `tm-replacement-a1`'s baseline and `rescale-in-a1`'s finishing window and 0 in every other; a 60-second sampler catches an append in flight only by chance.
- **Checkpoints**: the proving checkpoints' end-to-end durations were 0.3 to 0.7 seconds.

The preregistration compares a figure against `bq1312-eo-10-a1` only where the writers match, so the windows with one writer are compared within their own trial only.

## Cleanup

Every attempt ended with the supervisor's and the runner's cleanup reporting success, and its receipt records verified idle and three empty plans.
After each attempt, and before the next was dispatched, these were read directly:

- No table named for the attempt's run ID remained in `flink_gcp_tier3_bigquery`.
- No object remained under `gs://flink-gcp-tier3-bigquery/runs/<run-id>/` or `gs://flink-gcp-tier3-bigquery/.inprogress/flink-gcp-tier3-bigquery/runs/<run-id>/`.
- No object of the run remained in `tier3-system` or `tier3-bigquery`, and `_control/environment.json` and the run's `_control/runs/` record were gone.

## Every admitted attempt

Each attempt was dispatched after the preregistration's [before-dispatch](0165-bigquery-fault-trials-preregistration-1779.md#before-dispatch) reads: `pins.cue` at `837d9c88f` named the publication's `lifecycle-tools` digest, the `bigquery-recovery` digest was in the registry, the lock and every run record were absent, and each window ended by 18:31:04, well before 2026-10-20T12:17:04Z.
The three attempts in the [result](#result) are every admitted attempt, and no dispatch was refused before admission.

## Deviations from the preregistration

- The campaign took about two and a half hours of serial dispatch, not the six the preregistration estimated: each attempt's workflow finished 46 to 48 minutes after dispatch, and the next was dispatched within minutes of the previous one's checks.

No stop condition matched: the replacement's refused worker request came with no deadline or input shortfall, which that condition also requires.
The interpretations were applied as the preregistration states them, and none of the conditional ones was triggered.
