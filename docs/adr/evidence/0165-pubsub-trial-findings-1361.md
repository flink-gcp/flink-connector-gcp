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

# Pub/Sub deployed recovery trials: findings

This records the deployed Pub/Sub recovery campaign of [#1435](https://github.com/flink-gcp/flink-connector-gcp/issues/1435), run against the goal fixed on [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361) on 2026-10-10.
That goal was to run each of the four reviewed trials once on the real GKE cluster and Pub/Sub, on the DataStream entry point with 1,000 records per subscription, and to show that the connector's source and sink lose no message when the job recovers.
A trial meets the goal when its output oracle finds every logical input, with duplicates reported but not failing it, and its resources are cleaned up.
All four met it on 2026-10-10.
The two replacement trials are `inconclusive` by the supervisor's stricter verdict, because a checkpoint completed before the fault and left nothing to redeliver; [Replay was not observed](#replay-was-not-observed) explains why that does not weaken the no-loss result and what it leaves unestablished.
Times are UTC.

## Result

| Trial | Attempt | Run | Rig commit | Verdict | Logical inputs | Lines | Missing | Duplicates |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| `jm-replacement` | `ps1361-jm-a6` | [38041988313](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38041988313) | `c100486b5` | `inconclusive` (`replay-unobserved`) | 2,000 | 2,000 | 0 | 0 |
| `tm-replacement` | `ps1361-tm-a4` | [38059668918](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38059668918) | `ba37b8adc` | `inconclusive` (`replay-unobserved`) | 2,000 | 2,000 | 0 | 0 |
| `rescale-out` | `ps1361-out-a1` | [38061784056](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38061784056) | `ba37b8adc` | `usable` | 2,000 | 2,000 | 0 | 0 |
| `rescale-in` | `ps1361-in-a1` | [38063693719](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38063693719) | `5f64c0f86` | `usable` | 2,000 | 2,000 | 0 | 0 |

Each oracle saw both subscriptions' 1,000 logical inputs exactly once on the output subscription.
It counted no repeated input processing, no repeated output delivery and no duplicate publication on either side, and it rejected no line.
`flink-tier3 analyze` recomputed each verdict from the exported evidence alone and agreed with the stored one, with no problems.
Every trial used the application image `pubsub-recovery@sha256:06fdc988…` and the supervisor image `lifecycle-tools@sha256:69d4424c…`, both published from `ef183e0c6` on 2026-10-09.
The rig's own code reaches the supervisor in the run's ConfigMap rather than in that image, so it is the `main` head of each dispatch; fixes landed between attempts, as [every admitted attempt](#every-admitted-attempt) lists.

The duplicates are zero in one run of each kind; at-least-once delivery permits them, and this campaign does not show they cannot occur.

### Cleanup

Every accepted attempt ended with the supervisor's and the runner's cleanup reporting success.
The run's three topics and three subscriptions were deleted, and the Pub/Sub control portion was recorded `cleaned`.
The runner verified the cluster idle, and the `flink-gcp`, `tier3-bootstrap` and `tier3-operator` plans were empty.
The final receipts record `idle: true` and release the environment lock.

### Recovery

| Attempt | Retained checkpoint | Cohort published | Fault | Restored | Restored from | Complete |
| --- | --- | --- | --- | --- | --- | --- |
| `ps1361-jm-a6` | chk-3 at 09:53:24 | 09:54:41 | JobManager Pod deleted at 09:56:30 | 09:57:04 | chk-4 | 10:02:11 |
| `ps1361-tm-a4` | chk-4 at 14:43:45 | 14:44:25 | TaskManager Pod deleted at 14:46:59 | 14:47:10 | chk-5 | 14:52:15 |
| `ps1361-out-a1` | chk-3 at 15:13:59 | 15:14:52 | savepoint upgrade, parallelism 1 to 2, at 15:16:08 | 15:16:40 | savepoint | 15:20:06 |
| `ps1361-in-a1` | chk-4 at 15:43:21 | 15:43:45 | savepoint upgrade, parallelism 2 to 1, at 15:45:00 | 15:45:38 | savepoint | 15:48:41 |

The retained checkpoint is the one completed before the supervisor requested the replay cohort; the cohort published after it is the population a replacement must see again if the fault lands before the next checkpoint.
Each job restored within 40 seconds of its fault, and the last cohort, published after the observed recovery, arrived in full before completion.

## Replay was not observed

The supervisor injects a replacement's fault only after it has collected the whole replay cohort's output, so that the fault displaces messages it knows were processed.
In both replacement trials that collection took longer than the application's 120-second checkpoint interval.
The cohort published 40 seconds after the retained checkpoint in `ps1361-tm-a4`, and the fault followed 2 minutes 34 seconds later.
In `ps1361-jm-a6`, the gap from publication to fault was 1 minute 49 seconds.
By then the next checkpoint had completed and covered the cohort, so both jobs restored a checkpoint later than the retained one and nothing was expected again.
The verdict records this as `replay-unobserved`, with `boundary_held: false`, 0 of the 666 and 333 expected replays seen, and no unexpected replay.

That outcome does not weaken the no-loss result.
The oracle counts the output subscription's lines against the published inputs, whichever checkpoint the job restored; every input was there.
What the two trials do not establish is the redelivery path, in which a message that was processed and acknowledged to Flink but not covered by a completed checkpoint is redelivered by the service after recovery.
Observing it needs the fault to land before the next checkpoint, either through a longer checkpoint interval or through faster output collection, which the [rig fixes](#every-admitted-attempt) did not change.
The owner accepted the inconclusive replacement verdicts as meeting the goal on 2026-10-10, on the condition that this limit is stated, as [#1435](https://github.com/flink-gcp/flink-connector-gcp/issues/1435#issuecomment-6099559941) records.

## Redelivery after a savepoint

ADR-0165 treats replay-cohort output from the attempts a rescale started as `inconclusive` (`replay-after-savepoint`) until a deployed run measures whether the service redelivers after a savepoint.
Neither rescale saw any: from the savepoint restore until completion, about three minutes in each, the attempts the upgrade started produced no replay-cohort output, and the trials report `replay_by_new_attempts: 0`.
This is one observation of each direction, bounded by completion; a redelivery after a longer lease extension remains outside what the trial observes.
The rule stays as it is.

## Every admitted attempt

Twelve dispatches made the four accepted attempts.
The rig had never run against the real services before this campaign, and each failure below was its first contact with one of them; every fix is merged.
The causes come from each attempt's workflow log and its exported evidence under `runs/<attempt>/` in the evidence bucket, and for `ps1361-tm-a1` from Compute Engine's `compute.instances.preempted` audit entry for the node, at 12:31:26.

| Attempt | Run | Outcome | Cause | Fix |
| --- | --- | --- | --- | --- |
| `ps1361-jm-a1` | [38031760914](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38031760914) | refused before admission | the OpenTofu apply triggered by the merge held the environment lock | none; dispatch waits for the apply |
| `ps1361-jm-a2` | [38032494354](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38032494354) | refused before admission | the reviewed commit was no longer the `main` head | none; dispatch names the head |
| `ps1361-jm-a3` | [38032863061](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38032863061) | stopped at preparation | a topic reads back without `state`, and the rig required `ACTIVE` | [#1697](https://github.com/flink-gcp/flink-connector-gcp/pull/1697) |
| `ps1361-jm-a4` | [38034912496](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38034912496) | both actors stopped while publishing | Cloud Storage refused the shared control record's write rate with 429 | [#1701](https://github.com/flink-gcp/flink-connector-gcp/pull/1701) |
| `ps1361-jm-a5` | [38039376941](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38039376941) | stopped after the boundary | the runner lost five control-record races in a row | [#1702](https://github.com/flink-gcp/flink-connector-gcp/pull/1702) |
| `ps1361-jm-a6` | [38041988313](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38041988313) | accepted | | |
| `ps1361-tm-a1` | [38051115493](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38051115493) | `inconclusive` after recovery | the Spot VM holding both TaskManagers was preempted | none; an unplanned interruption is inconclusive by design |
| `ps1361-tm-a2` | [38052834491](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38052834491) | stopped at the baseline | the runner's control-record reads lost five races in a row | [#1710](https://github.com/flink-gcp/flink-connector-gcp/pull/1710) |
| `ps1361-tm-a3` | [38056086523](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38056086523) | stopped after the boundary | publishing the replay cohort in batches of 100 outlasted its 90-second deadline | [#1712](https://github.com/flink-gcp/flink-connector-gcp/pull/1712) |
| `ps1361-tm-a4` | [38059668918](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38059668918) | accepted | | |
| `ps1361-out-a1` | [38061784056](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38061784056) | accepted | | |
| `ps1361-in-a1` | [38063693719](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38063693719) | accepted | | |

Every failed attempt except `ps1361-jm-a4` cleaned up on its own and returned the environment to idle.
In `ps1361-jm-a4` the supervisor stopped on the 429 during the cleanup step that deletes the run's Pub/Sub resources, which only the supervisor performs, so the run's recovery could not settle.
With the owner's approval, the six resources were deleted by hand and the control record's Pub/Sub portion was marked `cleaned` under its generation, after which the recovery workflow restored the environment to idle and released the lock, as [#1701](https://github.com/flink-gcp/flink-connector-gcp/pull/1701) describes.
The repair edited no other control state, receipt or lock.

The four fixes after [#1697](https://github.com/flink-gcp/flink-connector-gcp/pull/1697) share one cause: the runner and the supervisor update the same control record several times for every service call, which a busy trial pushes past Cloud Storage's rate of about one mutation a second on one object.
[#1701](https://github.com/flink-gcp/flink-connector-gcp/pull/1701), [#1702](https://github.com/flink-gcp/flink-connector-gcp/pull/1702) and [#1710](https://github.com/flink-gcp/flink-connector-gcp/pull/1710) let writes and reads wait out that contention, and [#1712](https://github.com/flink-gcp/flink-connector-gcp/pull/1712) cut a cohort's publications fourfold, from four per subscription to one, by publishing up to 1,000 messages a request.
Writing the record less often, which would remove the contention instead of waiting it out, was not attempted under this goal.
