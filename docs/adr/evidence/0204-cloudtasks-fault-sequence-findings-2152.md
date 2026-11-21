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

# Cloud Tasks fault sequence: findings

These are the findings of the Cloud Tasks Tier-3 rework of [#2152](https://github.com/flink-gcp/flink-connector-gcp/issues/2152), the sixth scenario moved to [ADR-0204](../0204-tier3-recovery-trials-tolerate-faults-and-fail-only-on-lost-data.md) after Firestore's, Spanner's, Bigtable's, BigQuery's and Pub/Sub's.
The job is the one the [Cloud Tasks runbook's fault sequence](../../../kubernetes/apps/cloudtasks/README.md#fault-sequence) describes: the production sink in its `EXACTLY_ONCE` mode, staging each record as a named task and creating it after the checkpoint that owns it, into a queue that stays paused.
The run judges that sink by ADR-0204's exactly-once rule, because it is the only sink the application writes through.

One run, `ct2152-seq-a1`, injected all five faults while the input flowed: a savepoint upgrade, a JobManager deletion, a TaskManager deletion, a rescale in and a rescale out.
It reached a `usable` verdict 20.3 minutes after its window started, with every input record listed once as its own task, carrying its own body, and nothing else in the queue, and was idle after 21.4 minutes.
It was the scenario's first attempt under ADR-0204, and its data showed no connector defect; the one connector symptom it logged, gRPC's closed-classloader reports at the JobManager deletion, is tracked by [#2243](https://github.com/flink-gcp/flink-connector-gcp/issues/2243).

## Result

| Run ID | Run | Verdict | Listed, matched, missing, duplicates, foreign, wrong body, dispatched |
| --- | --- | --- | --- |
| `ct2152-seq-a1` | [44919061527](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/44919061527) | `usable` | 3,600, 3,600, 0, 0, 0, 0, 0 |

The run was dispatched on 2026-11-21 on `main` at `fb34f9ced4dfde9c19aa059b44b8e2c747db274a`, the merge of [#2249](https://github.com/flink-gcp/flink-connector-gcp/pull/2249), which changed only the base module's gRPC event loops and the BigQuery connector.
Its rig is that of [#2251](https://github.com/flink-gcp/flink-connector-gcp/pull/2251), which added the scenario's `sequence` kind and merged as `a11370851161c0809ef6d3130ecd31107416f53d`, the parent of that merge.
It ran the `cloudtasks-recovery` image `sha256:9f2d3fae7642ee01e95aa9239744c9a43725d0d48c1d92fdcdc578e906be8f7c`, built from `a1137085` by the [publication below](#publication), so the application did not include #2249.
The input was 3,600 records at 10 records per second, six minutes of input.

After the job finished, the supervisor read the queue back as `PAUSED` and listed it once, in the full view, in five pages: three of 1,000 tasks, one of 600, and an empty page that followed the page token the service returned with the 600.
Each listed task's ID, the last segment of its name, was the SHA-256 digest of `<run-id>/<sequence>` for a sequence of the input, each sequence's task was listed once, and each task's body was the key its ID is the digest of.
No task had a dispatch attempt, and no name was outside the run's queue.
This was the first run whose oracle checked each task's body, which [#2250](https://github.com/flink-gcp/flink-connector-gcp/issues/2250) added after the independent review of #2251 found that the name alone does not show a record's value.

Both deletions made the job read input again.
The committer creates a checkpoint's tasks only once that checkpoint has completed ([ADR-0158](../0158-cloud-tasks-checkpointed-creation-stages-named-tasks-and-commits-after-the-checkpoint.md)), so a record staged after the snapshot a restore returned to had not been sent before the fault, and its create after the restore was its first.
When the JobManager was deleted, the upgraded job had snapshotted checkpoint 3 at 477 processed records just before, but that checkpoint never completed, so sequences 282 to 476 had to be read again; the restored job began again at sequence 282, as its first progress line, which counted 283 processed records, shows, and the TaskManager deletion stopped it at about sequence 450, by the input's rate, so the next restore read the rest.
When the TaskManager was deleted, checkpoint 4, which the input had snapshotted at 351 processed records, had completed about eight seconds earlier, as the JobManager logged it, and the input operator logged its completion callback 12 milliseconds later by its own node's clock; the committers log no notification, and the next checkpoint was triggered only after the deletion; the restore from checkpoint 4 read again from sequence 351 whatever the stopped job had read after it, about ten seconds of input at this rate between checkpoint 4's snapshot and the deletion, although no line records how far it had read.
Each restore also had the committer create the restored snapshot's own tasks again, and the service refuses a name it already holds, which the connector counts as success.
The `sequence` kind reads no metrics, so the run does not show when checkpoint 4's completion reached the committers or how many of its creates had been sent or had returned before the deletion.

The final receipt records `success` and `idle`, and `just tier3-analyze` recomputes `usable` with no problem and lists the five faults.
After the run, the queue `ct1362-ct2152-seq-a1` no longer existed, and nothing remained under the run's prefixes in `gs://flink-gcp-tier3-cloudtasks`.

## The fault sequence

| Fault | Injected (UTC) | Running again (UTC) | Seconds | Restored |
| --- | --- | --- | --- | --- |
| savepoint upgrade | 15:35:08 | 15:36:34 | 87 | savepoint 2 |
| JobManager deletion | 15:36:35 | 15:37:27 | 52 | savepoint 2 |
| TaskManager deletion | 15:37:28 | 15:38:21 | 53 | checkpoint 4 |
| rescale in, two sink subtasks to one | 15:38:21 | 15:40:22 | 121 | savepoint 6 |
| rescale out, one sink subtask to two | 15:40:22 | 15:41:49 | 87 | savepoint 8 |

"Running again" is the poll at which the supervisor found the job running again after the fault, by the rule every scenario under ADR-0204 shares; the seconds are measured between the unrounded times, and the rig injected each next fault in the same poll, so the five faults took 6 minutes 42 seconds.
The upgraded job's tasks began reading 19 seconds before the JobManager's deletion, but no checkpoint had completed by then: the first one the job tried to trigger, at 15:36:04Z, failed because its tasks were not yet running, and checkpoint 3, triggered 30 seconds later, was in flight at the deletion.
So the deletion restored the upgrade's savepoint again.
Checkpoint 4 completed at 15:37:19.991Z, as the JobManager logged it, about eight seconds before the TaskManager's deletion, which restarted the job's three tasks and restored it.
The JobManager counted two restores by then, from savepoint 2 after its own replacement and from checkpoint 4; each fault caused one restore.

The run recorded one event that the fault sequence waits out under ADR-0204: the Kubernetes API answered the rescale in's first patch, at 15:38:22Z, with HTTP 422, because the application had changed since the copy the patch tested, and the rig recorded `patch-conflict` and sent the patch again from the next poll's copy.
The repeated patch reached the application at 15:38:39Z, and the rescale in took 34 seconds longer than the upgrade and the rescale out.
The run recorded no `unplanned-recovery`, `patch-unconfirmed`, `fault-target-gone`, `fresh-start` or `recovery-rest-unavailable`.
When the JobManager's Pod was deleted, its Deployment's first nine attempts to create a replacement, at 15:36:35Z to 15:36:37Z, were refused with `exceeded quota: tier3-idle` while the deleted Pod still counted, as in the [Bigtable](0204-bigtable-fault-sequence-findings-2153.md#the-fault-sequence) and [BigQuery](0204-bigquery-fault-sequence-findings-2150.md#the-fault-sequence) runs, and the replacement was created at 15:36:37Z.
When the TaskManager's Pod was deleted, Flink's first attempt to create its replacement, at 15:37:29Z, was refused the same way.
The Pod events otherwise show Autopilot scaling up for the first Pods, startup probes failing while JobManagers started, and the faults' own deletions and upgrades, and no preemption, eviction or node shutdown.

When the JobManager's deletion cancelled the upgraded job, both TaskManagers logged, at 15:36:37Z, about 1.8 seconds after the deletion, that a `grpc-default-worker-ELG-1-1` thread had failed to log because the job's class loader had already closed (`Trying to access closed classloader`); only log4j's own reports of the failed appends remain.
It is the symptom the [BigQuery](0204-bigquery-fault-sequence-findings-2150.md#the-fault-sequence) and [Pub/Sub](0204-pubsub-fault-sequence-findings-2151.md#the-fault-sequence) runs recorded.
[#2237](https://github.com/flink-gcp/flink-connector-gcp/issues/2237) moved BigQuery's gRPC clients onto a connector-owned event-loop group, as [ADR-0208](../0208-grpc-channels-run-on-a-connector-owned-event-loop-group.md) records, and [#2243](https://github.com/flink-gcp/flink-connector-gcp/issues/2243) tracks the other connectors', Cloud Tasks' among them.
It changed nothing this run observed.

## Timeline

| Event | UTC | Seconds into the window |
| --- | --- | --- |
| Window start | 15:25:41 | 0 |
| Queue created and read back paused | 15:29:35 | 234 |
| JobManager Pod created | 15:29:44 | 243 |
| JobManager container started | 15:31:19 | 338 |
| Job `RUNNING`, as the JobManager logged it | 15:32:05 | 384 |
| TaskManager containers started | 15:33:49 | 488 |
| First input progress line | 15:34:41 | 541 |
| First fault | 15:35:08 | 567 |
| Last fault running again | 15:41:49 | 969 |
| Input's last progress line, the whole input processed | 15:45:39 | 1,198 |
| Job `FINISHED`, as the JobManager logged it | 15:45:41 | 1,201 |
| First listing page | 15:45:55 | 1,215 |
| Verdict | 15:45:56 | 1,216 |
| Run idle | 15:47:06 | 1,285 |

The UTC times are cut to the second and the seconds are rounded from the unrounded times, so a row can differ by one from the difference of the UTC times; the Pod and container rows have the second resolution of the Kubernetes events they come from.
The progress-line times are the ones the application's log lines carry, not the times the supervisor read them.
The six-minute input took 10 minutes 58 seconds through the five faults, 298 seconds longer than its length.
Its last progress line came 229 seconds after the last fault ran again, so the job had consumed about 131 seconds of input by then.
No restore followed the input's last progress line; the job finished two seconds after it, the supervisor's next poll found it finished, and the listing stage began at 15:45:54.8Z, timestamped its five checked pages within 0.7 seconds of each other, and decided the verdict two seconds after it began.

## Publication

The [Cloud Tasks fault sequence publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/44918582241) built `main` at `a11370851161c0809ef6d3130ecd31107416f53d` with `application=cloudtasks` on 2026-11-21, for this run.

The run used the `operator` mirror and the `lifecycle-tools` image that [pins.cue](../../../kubernetes/images/pins.cue) and the Helm values pinned for the [Spanner run](0204-spanner-fault-sequence-findings-2154.md#publication).
Their deletion-eligible times left more than a day beyond the run's end, so the pins did not change.
The publication copied the `operator` and `flink` mirrors onto their existing versions, which kept their creation times, and built a `lifecycle-tools` image that nothing pins.

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:91250bb08c4f1ea358b201680e355139a530cb7318439926d62c6387d7c1369d` | 2026-11-16T16:41:15Z | 2026-11-23T16:41:15Z |
| `flink` | `sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd` | 2026-11-16T16:41:17Z | 2026-11-23T16:41:17Z |
| `lifecycle-tools`, pinned and run | `sha256:0b0bd1495af3b2e9e5000cd09e068215b829cb2e36e78f3db6d714b3a85b1a38` | 2026-11-16T16:41:36Z | 2026-11-23T16:41:36Z |
| `lifecycle-tools`, built and unused | `sha256:05453c526a87136346f8dcc152286a8925953242cfb19d6d5f22d98681c2cd90` | 2026-11-21T15:23:45Z | 2026-11-28T15:23:45Z |
| `cloudtasks-recovery` | `sha256:9f2d3fae7642ee01e95aa9239744c9a43725d0d48c1d92fdcdc578e906be8f7c` | 2026-11-21T15:24:03Z | 2026-11-28T15:24:03Z |

The new digests are the ones the `sha-a1137085…` tag resolves to in GAR, and the `operator` `1.16.1` and `flink` `2.2.1` tags resolve to the mirrors.
The `flink` image is the base the `cloudtasks-recovery` image was built on, so the run's retention record lists only the other three it ran.

Before the publication, the [OpenTofu apply](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/44917134471) of #2251's merge added `cloudtasks.tasks.fullView` to the supervisor's custom role, which the full-view listing needs.

## Observations

The runbook's plan held.
It expected the input to begin 477 to 677 seconds into the window, and it began at 541 seconds.
It expected the faults to delay the input by about as much as the first Firestore, Spanner, Bigtable and BigQuery `sequence` runs' 245 to 292 seconds, and they delayed it by 298 seconds, six more than the most.
It expected the input to finish 18 to 22 minutes into the window, and it finished 20.0 minutes in.
It expected the verdict a minute or so later; it came 17 seconds later, at 20.3 minutes, inside the 20 to 30 minutes ADR-0204 asks of a run: the job finished two seconds after its input, the supervisor found it at its next poll, and the listing stage took two seconds.

## What this does not establish

- Every create for a record names its task with the digest of the record's key, so the service can hold at most one task per record. The listing shows that no record was lost and none carried another's body; it does not show how many creates the sink sent, and the sink's `AT_LEAST_ONCE` mode with the same names would leave the same listing. Nor does it show ADR-0158's other property, that no task is created for a checkpoint that does not complete, since a record staged for checkpoint 3 and created later leaves the same task.
- No fault was aimed at a commit. The JobManager deletion met checkpoint 3 in flight, none of whose tasks had been sent, and the TaskManager deletion came about eight seconds after checkpoint 4's completion, at a commit state the evidence does not show. Whether faults aimed at the staged sink's commits are needed stays with [#2148](https://github.com/flink-gcp/flink-connector-gcp/issues/2148).
- The queue stayed paused, so the run shows nothing about a handler receiving the tasks.
- The only event the run waited out was one refused patch, so, like the earlier sequence runs, it did not exercise the paths that tolerate an interruption.
- One run shows that the sink kept one task per record through these five faults; it is not a measure of how often a fault loses one.
