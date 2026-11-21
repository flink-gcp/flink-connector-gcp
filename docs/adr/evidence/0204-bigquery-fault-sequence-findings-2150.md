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

# BigQuery fault sequence: findings

These are the findings of the BigQuery Tier-3 rework of [#2150](https://github.com/flink-gcp/flink-connector-gcp/issues/2150), the fourth scenario moved to [ADR-0204](../0204-tier3-recovery-trials-tolerate-faults-and-fail-only-on-lost-data.md) after Firestore's, Spanner's and Bigtable's.
The job is the one the [BigQuery runbook's fault sequence](../../../kubernetes/apps/bigquery/README.md#fault-sequence) describes: the production at-least-once sink writing a finite input of 64 KiB rows through the Storage Write API's default stream into ten tables.
The exactly-once and FILE_LOADS paths keep their single-fault trials until [#2148](https://github.com/flink-gcp/flink-connector-gcp/issues/2148).

One run, `bq2150-seq-a1`, injected all five faults while the input flowed: a savepoint upgrade, a JobManager deletion, a TaskManager deletion, a rescale in and a rescale out.
It reached a `usable` verdict 20.0 minutes after its window started, with every input row in its table once and with its expected payload, and was idle after 21.4 minutes.
It was the scenario's first attempt under ADR-0204, and it showed no connector defect.

## Result

| Run ID | Run | Verdict | Rows valid, invalid | Sequences missing | Duplicate rows |
| --- | --- | --- | --- | --- | --- |
| `bq2150-seq-a1` | [44868503032](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/44868503032) | `usable` | 5,760, 0 | 0 | 0 |

The run was dispatched on 2026-11-21 on `main` at `ed697211a912a2a2a51121c0f5eebce836d1a6df`, the merge of [#2224](https://github.com/flink-gcp/flink-connector-gcp/pull/2224); its rig is that of [#2229](https://github.com/flink-gcp/flink-connector-gcp/pull/2229), and #2224 changed only the Pub/Sub connector, its documentation and its ADRs.
It ran the `bigquery-recovery` image `sha256:aa14b09643e3709224e79ea6be37dee8006de2054eb78e7718fac34627f578b1`, built from the same commit by the [publication below](#publication).
The input was 5,760 rows of 64 KiB at 1 MiB/s, 16 rows a second, to ten destinations.

One query read the ten tables after the job finished, and its report is the verdict.
Each table held its 576 expected sequences, each once, and no row had another run ID, a sequence outside the input, a wrong destination or a payload other than the one the application writes, the SHA-256 digest of the run ID and sequence repeated to the row's size.
The query billed 227,540,992 bytes, under its 4 GiB limit.

No sequence was duplicated, which ADR-0204 would have reported rather than failed, although both deletions made the job process input again.
The JobManager deletion's restore processed sequences 404 to at least 479 again, and the TaskManager deletion's restore processed sequence 623 again and whatever followed it in the nine seconds between checkpoint 3's snapshot and the deletion.
None of those rows had been appended.
The writer appends a destination's batch when its next row would take it past 512 KiB, eight of these rows, and appends every pending batch at each checkpoint; each writer receives a row for each destination every 1.25 seconds, so a batch fills about 11 seconds after a checkpoint's flush.
Both deletions came sooner: the upgraded job's writers had existed four seconds, and checkpoint 3's flush was nine seconds old.
The application's appender observations, which the supervisor read from the Pods' logs, agree.
The upgraded job's two writers opened their ten default streams and closed them at 05:07:31Z without appending a row; after checkpoint 3's flush, the surviving writer closed without appending, and the deleted Pod's writer had appended nothing by the supervisor's last read of its log, 1.2 seconds before the deletion.
The observations record appends for 5,405 of the 5,760 rows; the other 355 were appended by the jobs the upgrade and the two rescales stopped, 147, 107 and 101 rows, after the supervisor's last read of their Pods, in the last seconds before each stop and in its savepoint flush.

The final receipt records `success` and `idle`, and `just tier3-analyze` recomputes `usable` with no problem and lists the five faults.
After the run, the dataset `flink_gcp_tier3_bigquery` held none of the run's tables, and nothing remained under the run's prefix in `gs://flink-gcp-tier3-bigquery`.

## The fault sequence

| Fault | Injected (UTC) | Running again (UTC) | Seconds | Restored |
| --- | --- | --- | --- | --- |
| savepoint upgrade | 05:06:21 | 05:07:30 | 69 | savepoint 2 |
| JobManager deletion | 05:07:30 | 05:08:22 | 52 | savepoint 2 |
| TaskManager deletion | 05:08:23 | 05:09:14 | 52 | checkpoint 3 |
| rescale in, two writers to one | 05:09:15 | 05:10:40 | 86 | savepoint 5 |
| rescale out, one writer to two | 05:10:41 | 05:12:07 | 86 | savepoint 7 |

"Running again" is the poll at which the supervisor found the job running again after the fault, by the rule every scenario under ADR-0204 shares; the seconds are measured between the unrounded times, and the rig injected each next fault in the same poll, so the five faults took 5 minutes 46 seconds.
The upgraded job was `RUNNING` from 05:07:01Z, but its tasks were deployed only at 05:07:24Z, once its new TaskManagers had registered, and it lost its JobManager six seconds later, before any checkpoint, so the deletion restored the upgrade's savepoint again.
Checkpoint 3 completed at 05:08:15Z, eight seconds before the TaskManager's deletion, which restarted the job and restored it.

The run recorded no event that ADR-0204 waits out as unplanned: no `unplanned-recovery`, `patch-conflict`, `patch-unconfirmed`, `fault-target-gone`, `fresh-start` or `recovery-rest-unavailable`.
When the JobManager's Pod was deleted, its Deployment's first eight attempts to create a replacement, at 05:07:30Z and 05:07:31Z, were refused with `exceeded quota: tier3-idle` while the deleted Pod still counted, as in the [Bigtable run](0204-bigtable-fault-sequence-findings-2153.md#the-fault-sequence), and the replacement was created at 05:07:32Z.
Its Pod events otherwise show Autopilot scaling up for the first Pods, startup probes failing while JobManagers started, and the faults' own deletions, and no preemption, eviction or node shutdown.

When the JobManager's deletion cancelled the upgraded job, the TaskManager logged `Trying to access closed classloader` at 05:07:31Z: a gRPC Netty event-loop thread, still running about 0.3 seconds after the writers had closed their streams, failed to log a warning because the job's class loader had already closed, and Flink's uncaught-exception handler failed the same way when it tried to log the resulting exception; only log4j's own error reports of the four failed appends remain.
It happened once in the run and changed nothing the run observed; the record does not establish why the thread outlived the writers.

## Timeline

| Event | UTC | Seconds into the window |
| --- | --- | --- |
| Window start | 04:57:20 | 0 |
| JobManager Pod created | 05:01:55 | 275 |
| JobManager container started | 05:03:03 | 343 |
| First TaskManager container started | 05:05:04 | 464 |
| First input progress line | 05:05:58 | 518 |
| First fault | 05:06:21 | 541 |
| Last fault running again | 05:12:07 | 887 |
| Input's last progress line, the whole input processed | 05:16:27 | 1,147 |
| Job `FINISHED` | 05:16:29 | 1,149 |
| Query job created | 05:17:05 | 1,185 |
| Verdict | 05:17:19 | 1,199 |
| Run idle | 05:18:41 | 1,281 |

The six-minute input took 10 minutes 29 seconds through the five faults, 269 seconds longer than its length.
Its last progress line came 260 seconds after the last fault ran again, so the job had consumed about 100 seconds of input by then.
No restore followed the input's last progress line, and the job finished two seconds after it, as its JobManager logged; the Operator recorded it at 05:16:34Z.

## Publication

The [BigQuery fault sequence publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/44868099791) built `main` at `ed697211a912a2a2a51121c0f5eebce836d1a6df` with `application=bigquery` on 2026-11-21, for this run.

The run used the `operator` mirror and the `lifecycle-tools` image that [pins.cue](../../../kubernetes/images/pins.cue) and the Helm values pinned for the [Spanner run](0204-spanner-fault-sequence-findings-2154.md#publication), whose deletion-eligible times left more than a day beyond the run's end, so the pins did not change.
The publication copied the `operator` and `flink` mirrors onto their existing versions, which kept their creation times, and built a `lifecycle-tools` image that nothing pins.

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:91250bb08c4f1ea358b201680e355139a530cb7318439926d62c6387d7c1369d` | 2026-11-16T16:41:15Z | 2026-11-23T16:41:15Z |
| `flink` | `sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd` | 2026-11-16T16:41:17Z | 2026-11-23T16:41:17Z |
| `lifecycle-tools`, pinned and run | `sha256:0b0bd1495af3b2e9e5000cd09e068215b829cb2e36e78f3db6d714b3a85b1a38` | 2026-11-16T16:41:36Z | 2026-11-23T16:41:36Z |
| `lifecycle-tools`, built and unused | `sha256:679c02f181f2240e8b0b0b30e62412271e27c658379c8f5d778ab3eb32749e90` | 2026-11-21T04:55:31Z | 2026-11-28T04:55:31Z |
| `bigquery-recovery` | `sha256:aa14b09643e3709224e79ea6be37dee8006de2054eb78e7718fac34627f578b1` | 2026-11-21T04:55:50Z | 2026-11-28T04:55:50Z |

The new digests are the ones the `sha-ed697211…` tag resolves to in GAR, and the `operator` `1.16.1` and `flink` `2.2.1` tags resolve to the mirrors.
The `flink` image is the base the `bigquery-recovery` image was built on, so the run's retention record lists only the other three it ran.

## Observations

The runbook's plan held.
It expected the input to begin 477 to 677 seconds into the window, and it began at 518 seconds.
It expected the faults to delay the input by about as much as the earlier sequence runs' 245 to 292 seconds, and they delayed it by 269 seconds.
It expected six minutes of input to leave about four after the faults, because those runs had consumed 106 to 134 seconds of theirs by then; this run had consumed about 100 seconds, which left about four and a third.
It expected the verdict about 18 to 23 minutes into the window, and the run reached it 1,199 seconds in, at the lower edge of the 20 to 30 minutes ADR-0204 asks of a run.
Unlike the Spanner and Bigtable runs it stayed under 30 minutes, because the job finishes with its input instead of waiting for a change stream's end.

## What this does not establish

- The run recorded none of the events ADR-0204 waits out as unplanned, so, like the earlier sequence runs, it did not exercise the paths that tolerate an interruption.
- It ran only the at-least-once path, so it shows nothing about the commits a fault could interrupt on the exactly-once and FILE_LOADS paths.
- Neither deletion reached a writer that had appended past the state it restored or had an append in flight, so the run exercised neither the replay of appended rows that at-least-once delivery permits nor an append whose answer was lost; zero duplicates follows from that timing, not from the path.
- One run shows that the sink kept every row through these five faults; it is not a measure of how often a fault loses data.
