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

# Bigtable fault sequence: findings

These are the findings of the Bigtable Tier-3 rework of [#2153](https://github.com/flink-gcp/flink-connector-gcp/issues/2153), the third scenario moved to [ADR-0204](../0204-tier3-recovery-trials-tolerate-faults-and-fail-only-on-lost-data.md) after Firestore's and Spanner's.
The job is the one the [Bigtable runbook](../../../kubernetes/apps/bigtable/README.md) describes: the production exactly-once sink writing a finite input into `eo`, the production at-least-once sink writing the same input into `alo`, and the production Change Streams source relaying `eo`'s commits into `relay`.

One run, `bt2153-seq-a1`, injected all five faults while the input flowed: a savepoint upgrade, a JobManager deletion, a TaskManager deletion, a rescale in and a rescale out.
It reached a `usable` verdict 31.3 minutes after its window started, with every input record in `eo` exactly once and none lost from `alo` or `relay`, and was idle after 32.6 minutes.
It was the scenario's first attempt under ADR-0204, and it showed no connector defect.

## Result

| Run ID | Run | Verdict | `eo` keys matched, missing | `alo` keys matched, missing, extra contributions | `relay` keys matched, missing, replays |
| --- | --- | --- | --- | --- | --- |
| `bt2153-seq-a1` | [44717053687](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/44717053687) | `usable` | 2,400, 0 | 2,400, 0, 403 | 2,400, 0, 240 |

The run was dispatched on 2026-11-20 on `main` at `ef1c72f280ecbed0aad8e711274fec777a2d6f1d`, the merge of [#2214](https://github.com/flink-gcp/flink-connector-gcp/pull/2214); its rig is that of [#2210](https://github.com/flink-gcp/flink-connector-gcp/pull/2210), and #2214 changed only the BigQuery connector, its documentation page and an ADR.
It ran the `bigtable-recovery` image `sha256:c6d31e30a52406d57a08ed14c025bb4c47c2418999dd4623c7a97c3b684e49e1`, built from the same commit by the [publication below](#publication).
The input was 2,400 records at 10 records per second, and the change stream ended at 10:41:50Z, 1,800 seconds after the window started.

No oracle found a foreign key, a foreign column family or a wrong value.
Every input key holds in `eo` a SUM equal to its one contribution beside exactly one commit marker, so each staged commit took effect once through the five faults.
Every input key reached `alo` with its value; 325 keys hold more than one contribution, 403 extra contributions in all, which ADR-0204 reports rather than fails; a SUM cannot tell a replay after a restore from a client retry of a write that was applied, so the record does not name their cause.
Every input key was relayed from `eo`'s change stream, no relayed key was one `eo` had not applied, and 160 keys were delivered more than once, 240 extra deliveries in all, which are the source's replays.
The input's last progress line came 752 seconds before the change stream's end, so the relay was judged.

The final receipt records `success` and `idle`, and `just tier3-analyze` recomputes `usable` with no problem and lists the five faults.
After the run, the instance `bt1316-bt2153-seq-a1` read `NOT_FOUND` and nothing remained under the run's prefix in `gs://flink-gcp-tier3-bigtable`.

## The fault sequence

| Fault | Injected (UTC) | Running again (UTC) | Seconds | Restored |
| --- | --- | --- | --- | --- |
| savepoint upgrade | 10:20:52 | 10:22:20 | 87 | savepoint 2 |
| JobManager deletion | 10:22:20 | 10:23:29 | 69 | checkpoint 3 |
| TaskManager deletion | 10:23:29 | 10:24:39 | 70 | checkpoint 3 |
| rescale in, two subtasks to one | 10:24:40 | 10:26:05 | 85 | savepoint 6 |
| rescale out, one subtask to two | 10:26:05 | 10:27:31 | 86 | savepoint 8 |

"Running again" is the first poll at which the job was `RUNNING` on the fault's deployed specification, had a JobManager and at least the fault's parallelism of running TaskManagers, and had logged input progress after a restore that followed the fault.
The rig injected each next fault in the same poll, so the five faults took 6 minutes 39 seconds.
Checkpoint 3 completed at 10:22:14Z, six seconds before the JobManager's deletion, so the deletion restored it rather than the upgrade's savepoint, and the TaskManager's deletion restored it again because no later checkpoint had completed.
The TaskManager deletion restarted two of the job's three failover regions, the input's and one of the relay's, and the job's restore count rose from one to three.

The run recorded no event that ADR-0204 waits out as unplanned: no `unplanned-recovery`, `patch-conflict`, `patch-unconfirmed`, `fault-target-gone`, `fresh-start` or `recovery-rest-unavailable`.
When the JobManager's Pod was deleted, its Deployment's first nine attempts to create a replacement, between 10:22:20Z and 10:22:22Z, were refused with `exceeded quota: tier3-idle` while the deleted Pod still counted, as [#1779](0165-bigquery-fault-trials-findings-1779.md) recorded for a TaskManager, and the replacement started at 10:22:23Z.
Its Pod events otherwise show Autopilot scaling up for the Pods, startup probes failing while JobManagers started, and the faults' own deletions, and no preemption, eviction or node shutdown.

## Timeline

| Event | UTC | Seconds into the window |
| --- | --- | --- |
| Window start | 10:11:50 | 0 |
| Instance and tables admitted | 10:15:46 | 236 |
| JobManager container started | 10:17:57 | 367 |
| TaskManager containers started | 10:19:54 | 484 |
| First input progress line | 10:20:25 | 516 |
| First fault | 10:20:52 | 543 |
| Last fault running again | 10:27:31 | 942 |
| Input's last progress line, the whole input processed | 10:29:17 | 1,048 |
| Change stream's end | 10:41:50 | 1,800 |
| Job `FINISHED` | 10:42:45 | 1,855 |
| Tables read; verdict | 10:43:07 | 1,878 |
| Run idle | 10:44:25 | 1,956 |

The four-minute input took 8 minutes 52 seconds through the five faults, 292 seconds longer than its length, and finished 752 seconds before the change stream's end, so the relay was judged with 632 seconds to spare beyond its 120-second commit margin.
No restore followed the input's last progress line.
The job finished 55 seconds after the change stream's end, as its JobManager logged; the Operator recorded it at 10:42:50Z.

## Publication

The [Bigtable fault sequence publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/44715911448) built `main` at `ef1c72f280ecbed0aad8e711274fec777a2d6f1d` with `application=bigtable` on 2026-11-20, for this run.
A first dispatch, [44715845075](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/44715845075), named `9dde1b5741deba5d4feba250dfad68855f460fcd`, the merge of #2210, and stopped at its revision check before pushing anything, because `main` had moved to the merge of #2214.

The run used the `operator` mirror and the `lifecycle-tools` image that [pins.cue](../../../kubernetes/images/pins.cue) and the Helm values pinned for the [Spanner run](0204-spanner-fault-sequence-findings-2154.md#publication), whose deletion-eligible times left more than a day beyond the run's end, so the pins did not change.
The publication copied the `operator` and `flink` mirrors onto their existing versions, which kept their creation times, and built a `lifecycle-tools` image that nothing pins.

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:91250bb08c4f1ea358b201680e355139a530cb7318439926d62c6387d7c1369d` | 2026-11-16T16:41:15Z | 2026-11-23T16:41:15Z |
| `flink` | `sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd` | 2026-11-16T16:41:17Z | 2026-11-23T16:41:17Z |
| `lifecycle-tools`, pinned and run | `sha256:0b0bd1495af3b2e9e5000cd09e068215b829cb2e36e78f3db6d714b3a85b1a38` | 2026-11-16T16:41:36Z | 2026-11-23T16:41:36Z |
| `lifecycle-tools`, built and unused | `sha256:8469205c724ee7e62512f2ca613474570365f24816ddcf25a678285ce444892f` | 2026-11-20T10:09:57Z | 2026-11-27T10:09:57Z |
| `bigtable-recovery` | `sha256:c6d31e30a52406d57a08ed14c025bb4c47c2418999dd4623c7a97c3b684e49e1` | 2026-11-20T10:10:17Z | 2026-11-27T10:10:17Z |

The new digests are the ones the `sha-ef1c72f2…` tag resolves to in GAR, and the `operator` `1.16.1` and `flink` `2.2.1` tags resolve to the mirrors.
The `flink` image is the base the `bigtable-recovery` image was built on, so the run's retention record lists only the other three it ran.

## Observations

The runbook's plan held.
It expected the input to begin 505 to 722 seconds into the window, and it began at 516 seconds.
It expected the faults to delay the input by about as much as the Spanner and Firestore runs' 245 to 263 seconds, and they delayed it by 292 seconds, which still left 632 seconds, within the eight to eleven minutes the runbook gives for interruptions.
It expected the verdict about 31 minutes into the window, and the run reached it at 31.3 minutes.
Like Spanner's 30.2 minutes, that is just past the 20 to 30 minutes ADR-0204 asks of a run, because the job cannot finish before the change stream's end, which the plan fixes 30 minutes into the window.

## What this does not establish

- The run recorded none of the events ADR-0204 waits out as unplanned, so, like Firestore's and Spanner's, it did not exercise the paths that tolerate an interruption; an interruption that delays the input by more than about ten minutes would leave the relay unjudged.
- The relay's oracle judges keys, not commits; here each key had one commit to relay, because the input has one record per key and `eo` holds one contribution per key, but an input that commits a key more than once could lose a later commit without notice.
- The exactly-once oracle reads the end state, so it shows that each staged commit took effect once, not that no commit was ever attempted twice.
- One run shows that the three components kept every record through these five faults once; it is not a measure of how often a fault loses data.
