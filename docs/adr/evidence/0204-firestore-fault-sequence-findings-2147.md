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

# Firestore fault sequence: findings

These are the findings of the Firestore Tier-3 work of [#1879](https://github.com/flink-gcp/flink-connector-gcp/issues/1879).
They report the run made for [#2147](https://github.com/flink-gcp/flink-connector-gcp/issues/2147) under [ADR-0204](../0204-tier3-recovery-trials-tolerate-faults-and-fail-only-on-lost-data.md), and list the single-fault attempts of [#2085](https://github.com/flink-gcp/flink-connector-gcp/issues/2085) that its [preregistration](0165-firestore-recovery-preregistration-2085.md) governed until the owner paused them on 2026-11-11.
The job is the one that preregistration describes: the four production Firestore and Datastore components, two sinks into `input` and two batch sources copying `seed` into `batch_out`, each pair in its own database.

One run, `fs2147-seq-a1`, injected all five faults while the input flowed: a savepoint upgrade, a JobManager deletion, a TaskManager deletion, a rescale in and a rescale out.
It reached a `usable` verdict 26.4 minutes after its window started, with no record lost in either database, and was idle after 27.6 minutes.
No attempt, before it or in it, showed a connector defect.

## Result

| Run ID | Run | Verdict | `input` keys matched, missing (Native; Datastore) | `batch_out` seed rows matched, missing, late (Native; Datastore) | Extra deliveries (Native; Datastore) |
| --- | --- | --- | --- | --- | --- |
| `fs2147-seq-a1` | [43257321939](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/43257321939) | `usable` | 6,000, 0; 6,000, 0 | 6,000, 0, 0; 6,000, 0, 0 | 120; 0 |

The run was dispatched on 2026-11-12 on `main` at `6f7c1463eb815429bba31540884ca2467f4fe30a`, the merge of [#2155](https://github.com/flink-gcp/flink-connector-gcp/pull/2155).
It ran the `firestore-recovery` image `sha256:bff4b450c47c46e8d6d0588e3cc0d4ece1885151505b5d4163e16641b90ee7d1` from the [Firestore fault sequence publication](0165-tier3-image-publications.md#firestore-fault-sequence-publication) of the same commit.
The input was 6,000 records at 10 records per second, and each database's seed was 6,000 records, copied at 5 records per second per reading subtask.

No oracle found a foreign key, a wrong value or a copied late row.
In each database, every input record's key holds its sequence in `input`, and every seed record was copied into `batch_out` with its value.
The supervisor committed 16 late records to each database at 16:28:16Z and 16:28:17Z, after both databases' read time of 16:25:46Z, and none was copied.
The Native-mode copy delivered 112 of its rows more than once, 120 extra deliveries in all, which ADR-0204 reports rather than fails; the Datastore-mode copy delivered each row once.
Each batch read was planned once, as four splits.

The final receipt records `success` with the databases `cleaned`, and `just tier3-analyze` recomputes `usable` with no problem.

## The fault sequence

| Fault | Injected (UTC) | Running again (UTC) | Seconds | Restored |
| --- | --- | --- | --- | --- |
| savepoint upgrade | 16:28:17 | 16:29:26 | 69 | savepoint 2 |
| JobManager deletion | 16:29:27 | 16:30:19 | 53 | checkpoint 3 |
| TaskManager deletion | 16:30:20 | 16:31:12 | 53 | checkpoint 4 |
| rescale in, two subtasks to one | 16:31:13 | 16:32:22 | 69 | savepoint 6 |
| rescale out, one subtask to two | 16:32:22 | 16:33:31 | 69 | savepoint 7 |

"Running again" is the first poll at which the job was `RUNNING` on the fault's deployed specification, had a JobManager and at least the fault's parallelism of running TaskManagers, and had logged input progress after a restore that followed the fault.
The rig injected the next fault in the same poll, so the five faults took 5 minutes 14 seconds.
The input, ten minutes long without faults, ran from 16:27:40Z to 16:41:45Z, so every fault landed while it flowed.
The TaskManager deletion restarted the job's failover regions, which the job's statistics counted as three restores.

The window started at 16:19:43Z.
The runner read both new databases back at 16:23:21Z and admitted them with both seeds at 16:23:33Z.
The exercise finished at 16:46:04Z and the supervisor's cleanup was ready at 16:46:44Z; the run was idle at 16:47:17Z.
The two copies read their seeds for 1,088 and 1,196 seconds, which set the run's length.

## Every admitted attempt

| Run ID | Run | Started (UTC) | Rules | Outcome | What it led to |
| --- | --- | --- | --- | --- | --- |
| `fs2085-jm-a1` | [42496716547](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42496716547) | 2026-11-08T15:51:55Z | ADR-0165 | stopped before the application existed: the Datastore API refused the seed commit for a missing routing header | [#2130](https://github.com/flink-gcp/flink-connector-gcp/issues/2130), rig |
| `fs2085-jm-a2` | [42846539231](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42846539231) | 2026-11-10T15:40:55Z | ADR-0165 | stopped before the application existed: the 600-second startup deadline passed inside the Datastore-mode seed | [#2135](https://github.com/flink-gcp/flink-connector-gcp/issues/2135), rig |
| `fs2085-jm-a3` | [42936263803](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42936263803) | 2026-11-11T01:05:28Z | ADR-0165 | inconclusive: both recoveries proved, then the platform preempted the JobManager Pod after the last window | [#2140](https://github.com/flink-gcp/flink-connector-gcp/issues/2140), rig |
| `fs2085-jm-a4` | [43036762889](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/43036762889) | 2026-11-11T14:16:22Z | ADR-0165 | `usable` | none |
| `fs2085-tm-a1` | [43044865388](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/43044865388) | 2026-11-11T15:06:22Z | ADR-0165 | inconclusive: both recoveries proved and all four oracles passed, but the `finishing` window lost the Datastore-mode copy's metric readings | [#2143](https://github.com/flink-gcp/flink-connector-gcp/issues/2143) |
| `fs2147-seq-a1` | [43257321939](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/43257321939) | 2026-11-12T16:19:43Z | ADR-0204 | `usable` | none |

The preregistration's [revisions](0165-firestore-recovery-preregistration-2085.md#revisions-after-dispatch) give the first three attempts, their rig commits and their repairs in full; `fs2085-jm-a4` and `fs2085-tm-a1` ran on `main` at `75de1b994f1f71a9ef709b818abf6305ce71e57b`.
`fs2085-tm-a1` lost its readings, as it was diagnosed then, because Flink 2.2.1's aggregated subtask-metrics endpoint answers an empty list once one of the requested metrics is gone, which it is after one of the vertex's subtasks finished.
The campaign stopped there: the owner paused the trials for the rework of [#2143](https://github.com/flink-gcp/flink-connector-gcp/issues/2143), so the rescale-out and rescale-in trials never ran, and the single-fault trials are not run again.
Under ADR-0204 neither that lost reading nor the preemption of `fs2085-jm-a3` decides a verdict, and `fs2147-seq-a1` covers the two rescales the campaign left.

## Observations

`just tier3-analyze` still words its note on a usable verdict for ADR-0165's two recoveries and three windows.
For a sequence run it lists no windows and decides the verdict from the oracles alone, so the note does not describe what it judged.

## What this does not establish

- The run recorded none of the events ADR-0204 waits out: no `unplanned-recovery`, `patch-conflict`, `patch-unconfirmed`, `fault-target-gone` or `fresh-start`. It did not exercise the paths that tolerate an interruption.
- These findings replace the findings file the preregistration named, and do not give the readings it promised: the recovery durations, metric families and copies by phase of its attempts, and the times of their cleanup reads. ADR-0204 decides a run by its end-state data, so none of those readings decides a verdict here.
- One run under ADR-0204 shows that the four components kept every record through these five faults once; it is not a measure of how often a fault loses data.
