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

# Spanner fault sequence: findings

These are the findings of the Spanner Tier-3 rework of [#2154](https://github.com/flink-gcp/flink-connector-gcp/issues/2154), the second scenario moved to [ADR-0204](../0204-tier3-recovery-trials-tolerate-faults-and-fail-only-on-lost-data.md) after Firestore's.
The job is the one the [Spanner runbook](../../../kubernetes/apps/spanner/README.md) describes: the production `SpannerSink` writing a finite input into `input`, the production `SpannerChangeStreamSource` relaying what it wrote into `relay`, and the production `SpannerSource` copying a seeded table into `batch_out`.

One run, `sp2154-seq-a1`, injected all five faults while the input flowed: a savepoint upgrade, a JobManager deletion, a TaskManager deletion, a rescale in and a rescale out.
It reached a `usable` verdict 30.2 minutes after its window started, with no record lost from any of the three tables, and was idle after 31.7 minutes.
It was the scenario's first attempt under ADR-0204, and it showed no connector defect.

## Result

| Run ID | Run | Verdict | `input` keys matched, missing | `relay` keys matched, missing, replays | `batch_out` seed rows matched, missing, late, extra deliveries |
| --- | --- | --- | --- | --- | --- |
| `sp2154-seq-a1` | [43914934714](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/43914934714) | `usable` | 3,600, 0 | 3,600, 0, 125 | 3,600, 0, 0, 776 |

The run was dispatched on 2026-11-16 on `main` at `dc42dbc82822d9da3ecce4dc0099e88000103abe`, the merge of [#2174](https://github.com/flink-gcp/flink-connector-gcp/pull/2174), which adopted the [publication below](#publication)'s supervisor image; its rig is that of [#2171](https://github.com/flink-gcp/flink-connector-gcp/pull/2171).
It ran the `spanner-recovery` image `sha256:7ebee0ef952b30b81e5153e57232db0ce4faaacfeb5801b15363e27ecdfddbe3`, built from `74d52004a3eb0dd4a88351cf950a1c3f7cb3a115`, the merge of #2171.
The input was 3,600 records at 10 records per second, the seed 3,600 rows copied at 5 rows per second per reading subtask, and the change stream ended at 17:37:47Z, 1,800 seconds after the window started.

No oracle found a foreign key, a wrong value, a relayed commit after the change stream's end or a copied late row.
Every input record's key holds its sequence in `input`, and every input key was delivered to `relay`.
Each key's relayed commits are one, so the sink executed each write once as far as the relay observed; the relay's 125 extra deliveries, one each on 125 keys, are the source's replays, which ADR-0204 reports rather than fails.
The batch read was planned once, as seven partitions, at read timestamp 17:17:13.18Z.
The supervisor committed the 16 late seed rows at 17:19:42.40Z, 149 seconds after that read timestamp, and none was copied.
Every seed row was copied with its value; 502 keys were delivered more than once, 776 extra deliveries in all, under the key prefixes `4` (274), `8` (388) and `9` (114), which fits the partitions in flight at a restore being read again from their starts.

The final receipt records `success` and `idle`, and `just tier3-analyze` recomputes `usable` with no problem and lists the five faults.
After the run, `sp1360-sp2154-seq-a1` read `NOT_FOUND` and nothing remained under the run's prefix in `gs://flink-gcp-tier3-spanner`.

## The fault sequence

| Fault | Injected (UTC) | Running again (UTC) | Seconds | Restored |
| --- | --- | --- | --- | --- |
| savepoint upgrade | 17:19:42 | 17:20:52 | 69 | savepoint 2 |
| JobManager deletion | 17:20:52 | 17:21:44 | 52 | savepoint 2 |
| TaskManager deletion | 17:21:44 | 17:22:36 | 52 | savepoint 2 |
| rescale in, two subtasks to one | 17:22:36 | 17:23:47 | 71 | savepoint 3 |
| rescale out, one subtask to two | 17:23:48 | 17:25:13 | 86 | savepoint 4 |

"Running again" is the first poll at which the job was `RUNNING` on the fault's deployed specification, had a JobManager and at least the fault's parallelism of running TaskManagers, and had logged input progress after a restore that followed the fault.
The rig injected each next fault in the same poll, so the five faults took 5 minutes 31 seconds.
The JobManager and TaskManager deletions both restored the upgrade's savepoint, because each came less than a second after the job ran again and no 30-second checkpoint had completed since.
The TaskManager deletion restarted the job's failover regions, and the job's restore count rose from one to four.

The run recorded no event that ADR-0204 waits out as unplanned: no `unplanned-recovery`, `patch-conflict`, `patch-unconfirmed`, `fault-target-gone` or `fresh-start`.
During the rescale in, the REST Service read `HTTP 404` once at 17:22:54Z, between the Operator's deletion of the old cluster and the new one's creation, which the run recorded as `recovery-rest-unavailable` and waited out.
Its Pod events show Autopilot scaling up for the Pods, startup probes failing while JobManagers started, and the faults' own deletions, and no preemption, eviction or node shutdown.

## Timeline

| Event | UTC | Seconds into the window |
| --- | --- | --- |
| Window start | 17:07:47 | 0 |
| Instance and database admitted with the seed and split points | 17:14:38 | 411 |
| JobManager running; batch read planned | 17:17:13 | 566 |
| TaskManager containers started | 17:18:29 | 642 |
| Tasks deployed; first input progress line | 17:19:04 | 677 |
| First fault | 17:19:42 | 715 |
| Last fault running again | 17:25:13 | 1,046 |
| Input's last progress line, the whole input processed | 17:29:27 | 1,300 |
| Batch copy finished | 17:30:53 | 1,386 |
| Change stream's end; job `FINISHED` | 17:37:48 | 1,801 |
| Tables read; verdict | 17:37:58 | 1,811 |
| Run idle | 17:39:28 | 1,901 |

The six-minute input took 10 minutes 23 seconds through the five faults and finished 499 seconds before the change stream's end, so the relay was judged with 379 seconds to spare beyond its 120-second commit margin.
The batch copy outlasted the input by 86 seconds, so the batch read was reading through every fault, and the job then waited for the change stream's end.

## Publication

The [Spanner fault sequence publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/43908302149) built `main` at `74d52004a3eb0dd4a88351cf950a1c3f7cb3a115` with `application=spanner` on 2026-11-16, for this run.
GAR no longer held the `operator` and `flink` mirrors or the `lifecycle-tools` version then pinned, all past their deletion-eligible times, and the publication recreated the mirrors under their unchanged digests.
[#2174](https://github.com/flink-gcp/flink-connector-gcp/pull/2174) adopted its `lifecycle-tools` digest in [pins.cue](../../../kubernetes/images/pins.cue).

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:91250bb08c4f1ea358b201680e355139a530cb7318439926d62c6387d7c1369d` | 2026-11-16T16:41:15Z | 2026-11-23T16:41:15Z |
| `flink` | `sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd` | 2026-11-16T16:41:17Z | 2026-11-23T16:41:17Z |
| `lifecycle-tools` | `sha256:0b0bd1495af3b2e9e5000cd09e068215b829cb2e36e78f3db6d714b3a85b1a38` | 2026-11-16T16:41:36Z | 2026-11-23T16:41:36Z |
| `spanner-recovery` | `sha256:7ebee0ef952b30b81e5153e57232db0ce4faaacfeb5801b15363e27ecdfddbe3` | 2026-11-16T16:41:55Z | 2026-11-23T16:41:55Z |

The digests are the ones the `sha-74d52004…` tag, and the `operator` `1.16.1` and `flink` `2.2.1` tags, resolve to in GAR.
The publication built no `smoke` image, and GAR no longer holds the version the `smoke` pin names.

## Observations

The plan placed six minutes of input, about four of the faults' downtime and the commit margin seven to eleven minutes before the change stream's end, assuming the input begins when the batch read is planned.
It began 111 seconds later, once the TaskManagers' containers had started, at 17:18:29Z, and received their tasks, by 17:19:04Z, so the measured margin was six minutes 19 seconds, below that range; the runbook now gives the measured timeline instead of the estimate.

## What this does not establish

- The run recorded none of the events ADR-0204 waits out as unplanned, so, like Firestore's, it did not exercise the paths that tolerate an interruption; an interruption that delays the input by more than about six minutes would leave the relay unjudged.
- The change stream's oracle judges keys, not commits, so a second commit of a key whose first commit was relayed could be lost without notice; the sink's execution count is a lower bound for the same reason.
- One run shows that the three components kept every record through these five faults once; it is not a measure of how often a fault loses data.
