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

# Pub/Sub redelivery across a replacement: findings

This records the two replacement trials rerun under [#1720](https://github.com/flink-gcp/flink-connector-gcp/issues/1720) on 2026-10-12.
The deployed campaign of [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361) showed that no message is lost across a JobManager or TaskManager replacement, but both of its replacement trials ended `replay-unobserved`, as [its findings](0165-pubsub-trial-findings-1361.md#replay-was-not-observed) record.
These reruns asked the question those trials left open: whether a message that the job processed after the retained checkpoint is redelivered by the service once the job restores that checkpoint.
Both reruns observed it.
Times are UTC.

## What changed in the rig

The evidence of the #1361 trials showed that the application was not the slow part.
In `ps1361-tm-a4` the job had published the whole replay cohort's output by 14:44:19, and in `ps1361-jm-a6` by 09:54:41, yet the faults came at 14:46:59 and 09:56:30.
The supervisor collected that output with immediate pulls, which return what the service has ready at once (2 to 100 messages in those trials), and its drain ended each poll at the first batch shorter than 100.
In `ps1361-tm-a4` the cohort's 666 output lines therefore took eleven pulls spread over seven polls, and the next checkpoint completed before the fault.

[#1763](https://github.com/flink-gcp/flink-connector-gcp/pull/1763) makes the drain end at an empty pull instead, with at most 20 pulls a poll.
It changes neither the application nor its 120-second checkpoint interval, and both reruns used the image the #1361 campaign used.

## Result

| Trial | Attempt | Run | Rig commit | Verdict | Logical inputs | Lines | Missing | Repeated processing | Duplicate deliveries |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `jm-replacement` | `ps1720-jm-a1` | [38209612729](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38209612729) | `5e83bac29` | `usable` | 2,000 | 2,666 | 0 | 666 | 0 |
| `tm-replacement` | `ps1720-tm-a1` | [38216995542](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38216995542) | `5e83bac29` | `usable` | 2,000 | 2,333 | 0 | 333 | 0 |

Each trial ran once, as the first attempt, on the DataStream entry point with 1,000 records per subscription.
The rig commit is the head of #1763's branch, dispatched through `rig_sha`; the application image is `pubsub-recovery@sha256:06fdc988…` and the supervisor image `lifecycle-tools@sha256:69d4424c…`, both published from `ef183e0c6` on 2026-10-09.
`flink-tier3 analyze` recomputed both verdicts from the exported evidence alone and agreed with the stored ones, with no problems.

Each oracle found all 1,000 logical inputs of both subscriptions on the output subscription.
The lines beyond 2,000 are the replay: the oracle counts them as repeated processing of an input, and it counted no repeated delivery of one output message and no duplicate publication on either side.
Both runs ended with the supervisor's and the runner's cleanup reporting success, the cluster idle, and the `flink-gcp`, `tier3-bootstrap` and `tier3-operator` plans empty.

## The fault landed before the next checkpoint

| Attempt | Retained checkpoint | Cohort published | Cohort collected | Fault | Next checkpoint due | Restored | Restored from |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `ps1720-jm-a1` | chk-2 at 06:07:33 | 06:07:55 | 06:08:11 | decided at 06:08:14; JobManager Pod shutting down by 06:08:15 | 06:09:32 | 06:08:58 | chk-2 |
| `ps1720-tm-a1` | chk-2 at 07:31:13 | 07:31:32 | 07:31:51 | decided at 07:31:54; TaskManager Pod shutting down by 07:31:56 | 07:33:13 | 07:32:06 | chk-2 |

The retained checkpoint is the one completed before the supervisor requested the replay cohort, and the next checkpoint was due 120 seconds after its trigger.
In both trials the supervisor collected the whole replay cohort's output in one poll of ten pulls, nine of them non-empty and the last one empty, from 06:07:57 to 06:08:13 in `ps1720-jm-a1` and from 07:31:42 to 07:31:53 in `ps1720-tm-a1`.
The supervisor decided the fault 19 and 22 seconds after the cohort's publication, against 1 minute 49 seconds and 2 minutes 34 seconds in the #1361 trials.
It records that decision before it requests the Pod's deletion, and the cluster's reaction bounds when the shutdown began: Kubernetes recorded the old JobManager container being stopped at 06:08:15, and the JobManager logged at 07:31:56 that the deleted TaskManager was shutting down and switched the job to `RESTARTING`.
Each Pod's shutdown had therefore begun about 77 seconds before the next checkpoint was due.
Both jobs therefore restored the retained checkpoint, which the verdict records as `boundary_held: true`.

Judged by the creation times of each pull's evidence objects, which have one-second resolution, most pulls in these runs took a second or less (68 of 89 and 66 of 80) and none more than two; the same measure over `ps1361-tm-a4`'s objects, under `runs/ps1361-tm-a4/` in the evidence bucket, gives two to three seconds for 52 of its 61 pulls and one to six seconds overall.
The rig simplification of [#1722](https://github.com/flink-gcp/flink-connector-gcp/issues/1722), merged between the two campaigns, cut each pull's updates of the shared control record to two, and neither run stopped on a control-record write.
Non-empty pulls averaged 62 messages in `ps1720-jm-a1` and 50 in `ps1720-tm-a1`.
The margin before the checkpoint therefore depends on batch sizes like these; a run whose pulls return a few messages each would still spread the cohort over several polls, as ADR-0165 records.

## Redelivery was observed

The fault displaced the replay cohort's processing, and the service delivered those messages again to the attempts the restored job started.
In `ps1720-jm-a1` the JobManager's loss restarted every attempt, so all 666 messages of the cohort were expected again, and all 666 were seen.
In `ps1720-tm-a1` the fault deleted one TaskManager, so only the 333 messages that the attempt on that Pod had processed were expected again, and those 333 were seen.
Each redelivered message arrived under the input message ID it had been processed under before the fault (`replay_ids_preserved: true`), which shows that the service redelivered it rather than that the runner published it again.

This establishes on the deployed service the path the #1361 trials could not: a message that the job processed and acknowledged to Flink, but that no completed checkpoint covered, is redelivered after recovery, and the job's output then contains it again.
At-least-once delivery predicts exactly this repetition, and the oracle counts it as repeated processing rather than as a duplicate.
The no-loss result of #1361 stands, and these two runs add the redelivery that makes it hold when the fault lands between checkpoints.
