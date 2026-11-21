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

# Pub/Sub fault sequence: findings

These are the findings of the Pub/Sub Tier-3 rework of [#2151](https://github.com/flink-gcp/flink-connector-gcp/issues/2151), the fifth scenario moved to [ADR-0204](../0204-tier3-recovery-trials-tolerate-faults-and-fail-only-on-lost-data.md) after Firestore's, Spanner's, Bigtable's and BigQuery's.
The job is the relay the [Pub/Sub runbook's fault sequence](../../../kubernetes/apps/pubsub/README.md#fault-sequence) describes.
It reads two subscriptions through the production source, with restore deduplication on, and writes one observation per processing through the production sink to an output topic, which the supervisor pulls.
The runner publishes the whole input at the start, and the relay paces it by waiting 100 ms before each record.

One run, `ps2151-seq-a1`, injected all five faults while the input flowed: a savepoint upgrade, a JobManager deletion, a TaskManager deletion, a rescale in and a rescale out.
It reached a `usable` verdict 24.9 minutes after its window started, with every input on the output under the message ID it was published with, and was idle after 26.2 minutes.
It was the scenario's first attempt under ADR-0204, and it showed no connector defect.

## Result

| Run ID | Run | Verdict | Logical inputs | Missing | Repeated processings | Output duplicates |
| --- | --- | --- | --- | --- | --- | --- |
| `ps2151-seq-a1` | [44891796299](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/44891796299) | `usable` | 9,600 | 0 | 1,587 | 0 |

The run was dispatched on 2026-11-21 on `main` at `682fb2e4fb6d9bf16a5adf72fe5adc68b82de613`, the merge of [#2241](https://github.com/flink-gcp/flink-connector-gcp/pull/2241).
It ran the `pubsub-recovery` image `sha256:46b5beea7e424070b8ba0aa843a03dea5ecb3bff82ce6c9411a692b7fb6754c1`, built from the same commit by the [publication below](#publication).
The input was 4,800 records on each of the two subscriptions, read at ten a second by each subtask: 480 seconds of input at parallelism two.

The run completed because every input of both subscriptions was on the output (`all-observed`), not because the redelivery horizon passed.
The oracle accepted all 11,187 collected lines.
Every output named its input under an ID that the runner's publication receipt for that input holds, and every input had a receipt.
No input was published twice, no observation was published twice, the supervisor received no output message twice, and no pull was refused.

Each output names the attempt that processed its input, the checkpoint the attempt restored and the latest checkpoint barrier it had passed, so the repeated processings can be traced to the restores that caused them.
Of the inputs processed more than once, 987 were processed twice and 300 three times; each repeat below is counted against the restore whose attempt made it.

| Restore | Repeated processings | Earlier processing |
| --- | --- | --- |
| Checkpoint 3, after the JobManager deletion | 978, 435 on subscription 0 and 543 on subscription 1 | The upgraded job's, after checkpoint 3's barrier |
| Checkpoint 4, after the TaskManager deletion | 302 on subscription 0 | The deleted TaskManager's attempt, after checkpoint 4's barrier |
| Checkpoint 4, after the TaskManager deletion | 108 on subscription 0 | The upgraded job's, after checkpoint 3's barrier |
| Savepoint 7, after the rescale out | 199 on subscription 0 | The deleted TaskManager's attempt, after checkpoint 4's barrier |

Each restore replayed the inputs the attempts it stopped had processed after the restored checkpoint's barrier, which at-least-once delivery requires.
The upgraded job had processed 1,086 inputs after checkpoint 3's barrier, and the deleted TaskManager's attempt 501 after checkpoint 4's; every one of them was processed again by a later attempt.
The attempts that followed did not finish those populations before the next fault stopped them in turn: the subscription-0 attempt restored from checkpoint 3 had processed 435 of its 543 when the TaskManager deletion stopped it, and the attempt restored from checkpoint 4 had processed 302 of its 501 when the rescale in stopped it.
The rest, 108 and 199, were processed by the attempts after those.
The output shows when an input was processed, not when the service redelivered it, so it does not tell whether these inputs waited in the next attempt's buffers or came back later.
None of the attempts that the savepoints stopped had processed an input after its savepoint's barrier, and none of their inputs was processed again.

The oracle counted no covered redelivery: no repeat was of an input that the state its attempt restored already covered, so no repeat was one that restore deduplication should have suppressed.
The counter can miss coverage at a region failover that leaves subtasks running, and this run's TaskManager deletion was one, but every repeat after that restore was on subscription 0, the region the deletion restarted.
A redelivery the ledger suppresses is acknowledged rather than emitted and leaves no output, and the sequence reads no metrics, so whether the ledger suppressed anything is not visible.

The final receipt records `success` and `idle`, and `just tier3-analyze` recomputes `usable` with no problem and lists the five faults.
After the run, no topic or subscription of the run remained, and nothing remained under the run's prefix in `gs://flink-gcp-tier3-pubsub`.

## The fault sequence

| Fault | Injected (UTC) | Running again (UTC) | Seconds | Restored |
| --- | --- | --- | --- | --- |
| savepoint upgrade | 10:08:16 | 10:10:25 | 129 | savepoint 2 |
| JobManager deletion | 10:10:25 | 10:12:10 | 105 | checkpoint 3 |
| TaskManager deletion | 10:12:11 | 10:13:21 | 71 | checkpoint 4 |
| rescale in, two subtasks to one | 10:13:23 | 10:15:00 | 97 | savepoint 6 |
| rescale out, one subtask to two | 10:15:01 | 10:16:37 | 96 | savepoint 7 |

"Running again" is the poll at which the supervisor found the job running again after the fault, by the rule every scenario under ADR-0204 shares; for Pub/Sub, the progress line that rule reads must come from an attempt that announced itself after the fault.
The seconds are measured between the unrounded times, and the rig injected each next fault in the same poll, so the five faults took 8 minutes 21 seconds.

The faults took longer than in the Firestore, Spanner, Bigtable and BigQuery sequence runs, whose faults took 52 to 87 seconds each, mostly because of how long the supervisor's polls took.
Each Pub/Sub poll first pulls the output subscription until a pull comes back empty, up to 20 pulls, and saves each pull's evidence to Cloud Storage, and only then decides whether the job runs again.
The run's 40 polls made 393 pulls, of which only 23 came back empty; the polls started 18.5 to 44.3 seconds apart, 20.9 at the median, against the policy's 15-second wait, and each poll that found the job running again decided so 20 to 26 seconds after it started.
The upgrade shows the effect most.
The upgraded job restored savepoint 2 at 10:08:57, and its attempts announced themselves at 10:09:16 and logged their first progress at 10:09:19.
The poll that started at 10:09:18 read the logs up to the announcements but before the progress lines, and the next poll started 44 seconds later, at 10:10:02, read the progress and decided after its 20 pulls, at 10:10:25.

The TaskManager deletion restarted only the failover region on the deleted Pod.
The JobManager logged the job `RESTARTING` at 10:12:11 and `RUNNING` again at 10:12:21, restoring checkpoint 4, and the attempt on the other TaskManager kept processing subscription 1 until the rescale in stopped it; its last output was published at 10:13:24.
The supervisor found the job running again at 10:13:21, after the attempt on the replacement Pod, which announced itself at 10:12:37, had logged progress.
The surviving attempt's own lines after the restore did not count, which is the rule's purpose.

The run recorded no event that ADR-0204 waits out as unplanned, and no `pubsub-pull-retried`.
The JobManager's replacement Pod was created at 10:10:26, a second after the deletion, with no `exceeded quota` refusal, unlike in the [Bigtable](0204-bigtable-fault-sequence-findings-2153.md#the-fault-sequence) and [BigQuery](0204-bigquery-fault-sequence-findings-2150.md#the-fault-sequence) runs.
Twice a checkpoint could not be triggered because the job's tasks were not yet running: at 10:05:24, in the first job, and at 10:16:03, in the rescale-out's job.
The Pod events otherwise show Autopilot scaling up for the first Pods, startup probes failing while JobManagers started, and the faults' own deletions and upgrades, and no preemption, eviction or node shutdown.

When the JobManager's deletion cancelled the upgraded job, both TaskManagers logged `Trying to access closed classloader` at 10:10:27, about 1.8 seconds after the deletion: a gRPC Netty event-loop thread failed to log because the job's class loader had already closed, and on one TaskManager Flink's uncaught-exception handler failed the same way when it tried to log the resulting exception; only log4j's own reports of the eight failed appends remain.
It is the symptom the [BigQuery run](0204-bigquery-fault-sequence-findings-2150.md#the-fault-sequence) recorded, which [#2237](https://github.com/flink-gcp/flink-connector-gcp/issues/2237) tracks; it changed nothing this run observed, and the record does not establish why the thread outlived the job.

## Timeline

| Event | UTC | Seconds into the window |
| --- | --- | --- |
| Window start | 09:56:55 | 0 |
| First cohort published, 1,600 records on each subscription | 10:02:45 | 351 |
| JobManager Pod created | 10:02:54 | 359 |
| Third cohort published, the whole input on both subscriptions | 10:03:50 | 415 |
| JobManager container started | 10:04:35 | 460 |
| Job `RUNNING`, as the JobManager logged it | 10:05:16 | 502 |
| TaskManager containers started | 10:06:26 | 571 |
| First output published | 10:06:59 | 605 |
| Checkpoint 1 completed | 10:07:26 | 632 |
| First fault | 10:08:16 | 681 |
| Last fault running again | 10:16:37 | 1,183 |
| Last output published, the whole input processed | 10:21:45 | 1,491 |
| Verdict, every input on the output | 10:21:50 | 1,495 |
| Run idle | 10:23:08 | 1,574 |

The output times are the service's publish times of the relay's observations, which the saved pull responses carry; the Pods' logs, which the supervisor reads once a poll, miss the last lines of an attempt that stops between two reads.
The runner published the whole input before the job started, so the relay never waited for the input's publication.
The 480 seconds of input took 886 seconds from the first output to the last, 406 seconds longer than its length.
The attempts each fault started published their first output 64, 40, 62 and 68 seconds after it, and subscription 0's replacement attempt 30 seconds after the TaskManager deletion, 263 seconds in all.
The 1,587 repeated processings took about 79 seconds of the relay's pace; 1,044 of them were on subscription 0, whose last output came 52 seconds after subscription 1's.
The rescale-in's single attempt processed for 37 seconds, at half the pace, before the rescale out stopped it.
The jobs before the rescale out had processed 4,323 of the 9,600 inputs, about 216 seconds of input; the rescale-out's job processed the rest, and its last output came 308 seconds after the last fault ran again.

## Publication

The [Pub/Sub fault sequence publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/44890913048) built `main` at `682fb2e4fb6d9bf16a5adf72fe5adc68b82de613` with `application=pubsub` on 2026-11-21, for this run.

The run used the `operator` mirror and the `lifecycle-tools` image that [pins.cue](../../../kubernetes/images/pins.cue) and the Helm values pinned for the [Spanner run](0204-spanner-fault-sequence-findings-2154.md#publication).
Their deletion-eligible times left more than a day beyond the run's end, so the pins did not change.
The publication copied the `operator` and `flink` mirrors onto their existing versions, which kept their creation times, and built a `lifecycle-tools` image that nothing pins.

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:91250bb08c4f1ea358b201680e355139a530cb7318439926d62c6387d7c1369d` | 2026-11-16T16:41:15Z | 2026-11-23T16:41:15Z |
| `flink` | `sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd` | 2026-11-16T16:41:17Z | 2026-11-23T16:41:17Z |
| `lifecycle-tools`, pinned and run | `sha256:0b0bd1495af3b2e9e5000cd09e068215b829cb2e36e78f3db6d714b3a85b1a38` | 2026-11-16T16:41:36Z | 2026-11-23T16:41:36Z |
| `lifecycle-tools`, built and unused | `sha256:2e6308cffbd0cb157e34afc0fed6587e29791758572c26df691813524fed19fd` | 2026-11-21T09:48:12Z | 2026-11-28T09:48:12Z |
| `pubsub-recovery` | `sha256:46b5beea7e424070b8ba0aa843a03dea5ecb3bff82ce6c9411a692b7fb6754c1` | 2026-11-21T09:48:32Z | 2026-11-28T09:48:32Z |

The new digests are the ones the `sha-682fb2e4…` tag resolves to in GAR, and the `operator` `1.16.1` and `flink` `2.2.1` tags resolve to the mirrors.
The `flink` image is the base the `pubsub-recovery` image was built on, so the run's retention record lists only the other three it ran.

## Observations

The runbook's plan held at its ends, with a longer middle than it expected.
It expected the input to begin 477 to 677 seconds into the window, and it began at 605 seconds.
It expected the faults to delay the input by about as much as the earlier sequence runs' 245 to 292 seconds; they delayed it by 406 seconds, for the reasons in the timeline.
It expected the input to have advanced by 100 to 134 seconds when the last fault came, as in those runs; it had advanced by about 216, because the long polls let more input through between faults.
It expected the verdict about 20.5 to 25.6 minutes into the window, and the run reached it 1,495 seconds in, 24.9 minutes, inside the 20 to 30 minutes ADR-0204 asks of a run.

Because a Pub/Sub poll decides only after its pulls, each fault's confirmation lags the job by about 20 seconds plus the wait for the next poll.
The completion rule's 600-second horizon rests on the client library's cap on a single lease extension, 600 seconds in google-cloud-pubsub 1.160.0, not on anything this run measured; the run did not need it, since every input was on the output first.

## What this does not establish

- The run recorded none of the events ADR-0204 waits out as unplanned, so, like the earlier sequence runs, it did not exercise the paths that tolerate an interruption.
- No pull was refused, so the retry of a refused pull was not exercised.
- The run completed because every input was on the output, so the 600-second redelivery horizon, the rule for an input that never arrives, was not exercised.
- Whether restore deduplication suppressed any redelivery is not visible: a suppressed redelivery leaves no output, and the sequence reads no metrics. The run shows that no repeat was one the ledger should have suppressed, and that every input a restore had to replay was processed again.
- The output shows processing, not delivery: an attempt stopped before its sink published leaves its processing unseen, and the late repeats do not tell when the service redelivered their inputs.
- The counts cover the output collected up to the verdict; the job was still running then, and anything it published later was not collected.
- One run shows that the source and sink kept every input through these five faults; it is not a measure of how often a fault loses data.
