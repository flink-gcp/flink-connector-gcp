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

# BigQuery deployed recovery trials: findings

This records the deployed BigQuery Storage Write trials of [#1312](https://github.com/flink-gcp/flink-connector-gcp/issues/1312), run under [#1427](https://github.com/flink-gcp/flink-connector-gcp/issues/1427) against the [preregistration](0165-bigquery-trial-preregistration-1312.md), within [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md).
All four preregistered trials reached a `usable` verdict between 2026-09-25T03:03Z and 10:51Z.
Every admitted attempt is listed, including the ones that failed, and each failure's repair is linked.
Times are UTC.

## Result

| Trial | Mode | Destinations | Attempt | Run | Rig commit | Verdict |
| --- | --- | ---: | --- | --- | --- | --- |
| `alo-10` | ALO | 10 | `bq1312-alo-10-a7` | [36088775447](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36088775447) | `2cd181fee` | `usable` |
| `eo-10` | EO | 10 | `bq1312-eo-10-a1` | [36093383585](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36093383585) | `2cd181fee` | `usable` |
| `alo-50` | ALO | 50 | `bq1312-alo-50-a4` | [36113939447](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36113939447) | `c743fdbc7` | `usable` |
| `eo-50` | EO | 50 | `bq1312-eo-50-a2` | [36122418749](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36122418749) | `c743fdbc7` | `usable` |

Each final receipt records `success: true`, stage `complete`, verified idle and empty plans for `flink-gcp`, `tier3-bootstrap` and `tier3-operator`.
`just tier3-analyze` over the four exported evidence prefixes recomputed the same verdict for each, with 26 measurement samples per run and no reasons or problems.
All four used the application image published for the campaign, `bigquery-recovery@sha256:6fd22da4…`, whose version was created on 2026-09-23 and was well inside its retention margin.

### Query oracle

| Attempt | Expected records | Rows | Distinct sequences | Duplicates | Missing | Invalid | Billed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `bq1312-alo-10-a7` | 28,800 | 28,800 | 28,800 | 0 | 0 | 0 | 100 MiB |
| `bq1312-eo-10-a1` | 1,843,200 | 1,843,200 | 1,843,200 | 0 | 0 | 0 | 100 MiB |
| `bq1312-alo-50-a4` | 28,800 | 28,800 | 28,800 | 0 | 0 | 0 | 500 MiB |
| `bq1312-eo-50-a2` | 1,843,200 | 1,843,200 | 1,843,200 | 0 | 0 | 0 | 500 MiB |

Every table held exactly the sequences routed to it, `sequence mod destinations`, so routing was exact in every trial.
The ALO trials wrote no duplicate across a savepoint upgrade and a JobManager failover; under the preregistration's third interpretation that is an observation of these runs, not a property of at-least-once delivery.
The EO trials wrote no duplicate, which is what exactly-once requires, and this is the only exactly-once evidence claimed: one run at each destination count, not a variance.
Billing is BigQuery's 10 MiB minimum per table read, once per table, far below the preregistered 4 GiB reservation per query.

### Recovery

| Attempt | Baseline window | Upgrade begun | Failover begun | Finishing | Visibility | Complete |
| --- | --- | --- | --- | --- | --- | --- |
| `bq1312-alo-10-a7` | 03:11:50 | 03:25:10 | 03:27:00 | 03:28:56 | 03:43:47 | 03:44:07 |
| `bq1312-eo-10-a1` | 04:19:48 | 04:33:23 | 04:35:12 | 04:37:03 | 04:52:06 | 04:52:26 |
| `bq1312-alo-50-a4` | 08:46:16 | 08:59:47 | 09:01:20 | 09:03:19 | 09:17:58 | 09:18:20 |
| `bq1312-eo-50-a2` | 10:17:40 | 10:31:01 | 10:32:52 | 10:33:51 | 10:49:37 | 10:50:30 |

Each upgrade restored from its savepoint and each failover from the latest completed checkpoint, and each proof took under two minutes from the stage's start.
Each trial's first job failed its first three or four checkpoint triggers because its source task was not yet running (`Checkpoint triggering task … is not being executed`), then completed 28 checkpoints before the upgrade; the upgraded job failed one trigger the same way before its first checkpoint, and after the failover its JobManager reported 32 or 33 completed and none failed.

## Observations

These are the largest values any TaskManager reported in each measurement window.
They describe these runs at their fixed rates; none is a threshold or a production target, as [#1312](https://github.com/flink-gcp/flink-connector-gcp/issues/1312) and the preregistration require.

| Attempt | Window | Records in / s | Bytes out / s | Busy ms / s | Back-pressured ms / s | Heap used (of 644 MiB) | Direct | GC time | Open destinations | Append retries |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `alo-10-a7` | baseline | 8.0 | 514 KiB | 8 | 0 | 223 MiB | 181 MiB | 445 ms | 10 | 0 |
| `alo-10-a7` | finishing | 8.0 | 514 KiB | 10 | 0 | 232 MiB | 189 MiB | 496 ms | 10 | 0 |
| `eo-10-a1` | baseline | 513 | 513 KiB | 22 | 0 | 224 MiB | 183 MiB | 495 ms | – | 0 |
| `eo-10-a1` | finishing | 513 | 514 KiB | 37 | 0 | 232 MiB | 191 MiB | 551 ms | – | 10 |
| `alo-50-a4` | baseline | 8.3 | 533 KiB | 18 | 0 | 240 MiB | 195 MiB | 720 ms | 50 | 0 |
| `alo-50-a4` | finishing | 8.3 | 525 KiB | 15 | 0 | 236 MiB | 199 MiB | 786 ms | 50 | 0 |
| `eo-50-a2` | baseline | 592 | 555 KiB | 219 | 0 | 216 MiB | 187 MiB | 836 ms | – | 0 |
| `eo-50-a2` | finishing | 513 | 513 KiB | 200 | 0 | 249 MiB | 199 MiB | 972 ms | – | 0 |

- **Throughput** followed the source's fixed byte rate, about 512 KiB/s: 64 KiB records at about 8 per second for ALO, 1 KiB records at about 513 per second for EO. No window showed back-pressure, and network buffer pools stayed at 0 % use.
- **Memory** stayed flat: heap under 250 MiB of a 644 MiB maximum, direct memory under 200 MiB, non-heap under 180 MiB and no Flink managed memory, which the hashmap backend does not use. GC time is the JVM's cumulative total since the TaskManager started, which the upgrade restarts, and stayed under one second. Process memory is read as these JVM pools and mapped memory, which stayed at 0; the container's resident set was not collected.
- **Active writers**: the ALO writer reported one open destination per table, 10 and 50, at every reading. No `openDestinations` reading was collected for the EO writer; its `inFlightAppends` read 0 at every sample, which a 60-second sampling interval cannot distinguish from short appends.
- **Append retries**: `eo-10-a1` reported 10 in its finishing window and none before; the query oracle found no duplicate or missing sequence, so the retries were absorbed. The others reported none.
- **Busy time** grew most with mode and destination count: EO at 50 destinations was 5 to 10 times busier than EO at 10, with no back-pressure.
- **Checkpoints**: the longest end-to-end durations were 1.8 s for `alo-10-a7`, 1.5 s for `eo-10-a1`, 5.4 s for `alo-50-a4` and 15.6 s for `eo-50-a2`; checkpointed state was 862 B for ALO, 10.5 KiB for EO at 10 destinations and 43.1 KiB for EO at 50.
- **Coverage gap**: `alo-10-a7`'s upgrade window returned no metric family and its failover window returned memory only. The verdict requires baseline and finishing, which all four covered; a recovery window of about two minutes gives a 60-second sampler no guarantee of a reading.

## Every admitted attempt

| Attempt | Run | Rig commit | Stopped at | Cause | Repair |
| --- | --- | --- | --- | --- | --- |
| `bq1312-alo-10-a1` | [35883183162](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35883183162) | `a5a188789` | before admission | The supervisor audit refused the node-affinity term Autopilot adds for `safe-to-evict: "false"` | [#1483](https://github.com/flink-gcp/flink-connector-gcp/issues/1483) |
| `bq1312-alo-10-a2` | [35888569915](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35888569915) | `b02641e9d` | before admission | kube-dns preempted the supervisor on a one-node cluster | [#1487](https://github.com/flink-gcp/flink-connector-gcp/issues/1487), later [#1510](https://github.com/flink-gcp/flink-connector-gcp/issues/1510) |
| `bq1312-alo-10-a3` | [35919107483](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35919107483) | `b02641e9d` | baseline | A JobManager startup burst filled a 64 KiB later log read | [#1489](https://github.com/flink-gcp/flink-connector-gcp/issues/1489) |
| `bq1312-alo-10-a4` | [35961108435](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35961108435) | `45eb8524b` | baseline | Input began 16 s before the 600-second startup budget expired | [#1495](https://github.com/flink-gcp/flink-connector-gcp/issues/1495) |
| `bq1312-alo-10-a5` | [35996216533](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35996216533) | `5971cc236` | baseline | The first checkpoint's staged upload under `.inprogress/` was refused with 403 | [#1498](https://github.com/flink-gcp/flink-connector-gcp/issues/1498) |
| `bq1312-alo-10-a6` | [36017459729](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36017459729) | `8febc8a2e` | finishing | One Kubernetes API `503` ended the runner after both recoveries | [#1514](https://github.com/flink-gcp/flink-connector-gcp/issues/1514) |
| `bq1312-alo-10-a7` | [36088775447](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36088775447) | `2cd181fee` | complete | – | – |
| `bq1312-eo-10-a1` | [36093383585](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36093383585) | `2cd181fee` | complete | – | – |
| `bq1312-alo-50-a1` | [36096546395](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36096546395) | `2cd181fee` | upgrade | The upgrade read the new job ID with the old `FINISHED` state; cleanup then crashed on a Cloud Storage `429` | [#1521](https://github.com/flink-gcp/flink-connector-gcp/issues/1521), [#1522](https://github.com/flink-gcp/flink-connector-gcp/issues/1522) |
| `bq1312-alo-50-a2` | [36101928561](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36101928561) | `9ed8dfc72` | provisioning | Admission stopped after 25 of 50 tables, most likely on the same control-record write burst | [#1525](https://github.com/flink-gcp/flink-connector-gcp/issues/1525) |
| `bq1312-alo-50-a3` | [36112510192](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36112510192) | `c743fdbc7` | baseline | GCE reclaimed the Spot node holding both TaskManagers; `inconclusive` | none; repeated |
| `bq1312-alo-50-a4` | [36113939447](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36113939447) | `c743fdbc7` | complete | – | – |
| `bq1312-eo-50-a1` | [36118130526](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36118130526) | `c743fdbc7` | upgrade | Replacement nodes were refused on `SSD_TOTAL_GB` (500 GB in `us-central1`); the upgrade deadline expired | quota raised |
| `bq1312-eo-50-a2` | [36122418749](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36122418749) | `c743fdbc7` | complete | – | – |

`bq1312-alo-10-a4` ran rig commit `45eb8524b`, the head of the then-unmerged [#1490](https://github.com/flink-gcp/flink-connector-gcp/pull/1490), through `rig_sha`; every other attempt ran `main`.
Three dispatches of 2026-09-23 stopped before admission and burned no run ID: [35876102571](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35876102571) on the lifecycle CLI's refusal of BigQuery inputs ([#1481](https://github.com/flink-gcp/flink-connector-gcp/issues/1481)), [35881883978](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35881883978) on an approval that did not name the current `main`, and [35882107398](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35882107398) on a transient `500` from the Kubernetes API during the pre-lock snapshot.
Every failed attempt spent no BigQuery query and ended at verified idle with empty plans, apart from the two below.

### Manual repairs

Two attempts retained the environment lock because only the supervisor may delete tables and it had stopped:

- `bq1312-alo-50-a1`: the supervisor crashed with 13 of 50 tables deleted.
- `bq1312-alo-50-a2`: the provision call's in-flight marker stayed unresolved, although the refused intent write had come before any further create.

With the owner's approval, each was repaired by hand:

1. Delete the remaining tables after checking their run and nonce labels.
2. Mark the control record cleaned with a generation-matched write, clearing the provision marker for `alo-50-a2`.
3. Remove the recovery claim.
4. Dispatch `tier3-recover`, which restored verified idle, proved the plans empty and released the lock: [36099058332](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36099058332) and [36108743957](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/36108743957).

`bq1312-alo-50-a3` and `bq1312-alo-50-a4` then provisioned and cleaned 50 tables without intervention.

### Environment change

The `SSD_TOTAL_GB` quota in `us-central1` was raised from 500 GB to 1000 GB on 2026-09-25, request `8e73f38c86bb441aa4`.
Each Autopilot node has a 100 GB SSD boot disk, and an upgrade provisions replacement nodes while the old ones drain, so a 50-destination EO upgrade needed more than five nodes at once.
The same quota most likely produced the `GCE quota exceeded` scale-up refusals that delayed, without failing, `bq1312-alo-10-a4`'s JobManager.

## Deviations from the preregistration

- **The pilot took seven attempts.** The preregistration stops the campaign on an unusable pilot until this document is revised and approved again. Instead each attempt's defect was fixed in its own change and the next attempt was approved on its own; the preregistration was not revised between attempts, because none of the fixes changed the campaign, its estimate or its criteria.
- **`alo-50` took four attempts and `eo-50` two**, beyond the preregistered single `-a2` repeat for a platform cause. `alo-50-a1` and `alo-50-a2` failed on rig defects, not on the platform, and each repeat was approved on its own. `alo-50-a3` is the one platform repeat the preregistration allows. `eo-50-a1` failed on a project quota, repaired before its repeat.
- **Spend.** The four usable trials were estimated at USD 9.40. Every failed attempt ran its cluster for part of a window, and only the four usable trials were billed for queries, about 1.2 GiB in all; the owner approved every attempt's estimate before its dispatch, as ADR-0165 requires.
- **Capacity gate.** The preregistration's precondition of at least two nodes was replaced during the campaign: [#1510](https://github.com/flink-gcp/flink-connector-gcp/issues/1510) removed the one-node refusal after an idle cluster held one node for more than three hours.
