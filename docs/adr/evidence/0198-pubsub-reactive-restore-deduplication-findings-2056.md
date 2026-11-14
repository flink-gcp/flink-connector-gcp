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

# Pub/Sub restore deduplication under the reactive scheduler: findings

This records the deployed reactive-mode trials of [#2056](https://github.com/flink-gcp/flink-connector-gcp/issues/2056), which ran the [reactive Pub/Sub kinds](../../../kubernetes/apps/pubsub/README.md#restore-deduplication-trials) with the source's restore deduplication ([ADR-0198](../0198-pubsub-restore-deduplication-records-message-ids-not-ack-ids.md)) off and on, as [#2054](https://github.com/flink-gcp/flink-connector-gcp/issues/2054) asked.
The six trials ran between 2026-11-07 15:06Z and 2026-11-08 11:56Z, all reached a `usable` verdict, and none lost an input.
One scale-out lost a completed checkpoint's notification to the rescale, and the ledger suppressed 2,390 redeliveries, as many as the ids that checkpoint had covered; no other trial gave the ledger anything to suppress.
Times are UTC.

## Setup

Each trial runs the DataStream entry point with 10,000 records per subscription, 20,000 input messages over two subscriptions, under the Operator's standalone mode with `scheduler-mode: reactive` and a 120-second checkpoint interval.
The replay cohort is 6,666 messages, 3,333 per subscription; the relay sleeps 100 ms for each of them, and the fault is injected as soon as the cohort is published.
The trial files are the six `reactive-*` files under [`kubernetes/lifecycle/pubsub-trials`](../../../kubernetes/lifecycle/pubsub-trials):

- **`reactive-scale-out`**: two TaskManagers patched to three, so that two subtasks then share subscription 0.
- **`reactive-scale-in`**: three TaskManagers patched to two.
- **`reactive-tm-eviction`**: three TaskManagers, one Pod deleted and replaced by its Deployment.

Every usable attempt pulled the `pubsub-recovery` image `sha256:4faf9b7dbeadeee4721d42f69c229bab6a3802181ae35bc565f94252874a0734` and the Operator 1.16.1 mirror `sha256:91250bb08c4f1ea358b201680e355139a530cb7318439926d62c6387d7c1369d` of the [Pub/Sub reactive trial publication](0165-tier3-image-publications.md#pubsub-reactive-trial-publication), and the pinned `lifecycle-tools` image `sha256:3ecd5d8572105d7502ac9ba71940627d6299ac7b63d8bf4fd69d919184471dfb`.
Each was dispatched from `main`, so the rig commit moved between attempts; the commits after `163b2d1b6` that touched `tools/tier3`, `kubernetes` or the run workflow added the Firestore scenario and its namespace, and none changed the Pub/Sub scenario.

## Attempts

| Trial | Attempt | Run | Rig commit | Verdict |
| --- | --- | --- | --- | --- |
| `reactive-scale-out-deduplication-off` | `ps2056-rso-off-a1` | [42357644264](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42357644264) | `62c693f25` | inconclusive |
| `reactive-scale-out-deduplication-off` | `ps2056-rso-off-a2` | [42370571035](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42370571035) | `38618b479` | inconclusive |
| `reactive-scale-out-deduplication-off` | `ps2056-rso-off-a3` | [42384776298](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42384776298) | `163b2d1b6` | `usable` |
| `reactive-scale-out-deduplication-on` | `ps2056-rso-on-a1` | [42387175441](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42387175441) | `163b2d1b6` | `usable` |
| `reactive-scale-in-deduplication-off` | `ps2056-rsi-off-a1` | [42389696271](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42389696271), recovered by [42430885188](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42430885188) | `b90abdc59` | `usable` |
| `reactive-scale-in-deduplication-on` | `ps2056-rsi-on-a1` | [42459429992](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42459429992) | `06d8deb2b` | `usable` |
| `reactive-tm-eviction-deduplication-off` | `ps2056-tme-off-a1` | [42462168308](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42462168308), recovered by [42464558027](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42464558027) | `06d35237a` | `usable` |
| `reactive-tm-eviction-deduplication-on` | `ps2056-tme-on-a1` | [42474828891](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42474828891) | `a11d3cf17` | inconclusive |
| `reactive-tm-eviction-deduplication-on` | `ps2056-tme-on-a2` | [42475532991](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42475532991), recovered by [42477575625](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42477575625) | `a11d3cf17` | `usable` |

`flink-tier3 analyze` over each exported evidence prefix recomputed the stored verdict of every usable attempt, with 10 to 13 measurement samples and no reasons or problems.

The first two attempts ran the `pubsub-recovery` image `sha256:98e5729f6841375e89bf2962cc2e87a6c0e9c22ae270260d6984cf6cbf52c11b`, built from `62c693f25`, under Operator 1.15.0.
`ps2056-rso-off-a1` stopped on an unplanned JobManager container restart, because the Operator mounted an empty volume over `/opt/flink/usrlib` and hid the relay's JAR.
`ps2056-rso-off-a2` ended `FAILED` because Operator 1.15.0 passed `--host` to a Flink 2.2.1 standalone application.
[ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#reactive-pubsub-trials) records the template change for the first and the move to [Operator 1.16.1](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#operator-1161) for the second.

`ps2056-tme-on-a1` stopped before it created the application.
Its supervisor recorded `admission-stopped` ("Cancellation or recovery requested") at 11:16:59, and the runner then failed its own admission check, settled the run and found the supervisor Job without a Pod.
Nothing in its evidence names what requested the stop, and the cause was not established.
`ps2056-tme-on-a2` ran the same trial on the same rig commit and image nine minutes later.

Three attempts finished through the recovery workflow, after their lifecycles had completed with the oracle below.
The step that proves the three infrastructure plans empty failed for each of them.
For `ps2056-rsi-off-a1`, a change to the `flink-gcp` root had merged while the run held the lock, and its apply had not completed.
For `ps2056-tme-off-a1`, GitHub Releases returned errors to `tofu init`'s provider downloads, as it did intermittently between about 08:45Z and 10:50Z on 2026-11-08.
For `ps2056-tme-on-a2`, `tofu init` of the `flink-gcp` root failed again at 11:51Z; the step keeps no init log, so that failure's cause is not recorded.
Each recovery verified idle and empty plans, wrote the receipt and released the lock.

## Result

| Attempt | Logical inputs | Lines | Missing | Repeated processing | Covered redeliveries | Duplicate publications or deliveries |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `ps2056-rso-off-a3` | 20,000 | 20,022 | 0 | 22 | 0 | 0 |
| `ps2056-rso-on-a1` | 20,000 | 20,019 | 0 | 19 | 0 | 0 |
| `ps2056-rsi-off-a1` | 20,000 | 20,750 | 0 | 750 | 0 | 0 |
| `ps2056-rsi-on-a1` | 20,000 | 20,917 | 0 | 917 | 0 | 0 |
| `ps2056-tme-off-a1` | 20,000 | 21,178 | 0 | 1,178 | 0 | 0 |
| `ps2056-tme-on-a2` | 20,000 | 21,015 | 0 | 1,015 | 0 | 0 |

Covered redeliveries are inputs that a restored attempt processed again although an earlier run had processed them before a barrier in the restored checkpoint's lineage, which is what restore deduplication exists to suppress; the [oracle](../../../kubernetes/apps/pubsub/README.md#restore-deduplication-trials) defines them.
Repeated processing also counts inputs processed after the restored checkpoint's barrier and before the restart, which at-least-once delivery processes again by design.
The last column sums duplicate input publications, duplicate output publications and repeated output deliveries.

| Attempt | Fault decided | Checkpoint before the fault | Restored | From | Restored parallelism |
| --- | --- | --- | --- | --- | ---: |
| `ps2056-rso-off-a3` | 15:20:35 | chk-3 | 15:23:45 | chk-5 | 3 |
| `ps2056-rso-on-a1` | 15:50:19 | chk-2 | 15:53:25 | chk-4 | 3 |
| `ps2056-rsi-off-a1` | 16:22:24 | chk-3 | 16:24:36 | chk-3 | 2 |
| `ps2056-rsi-on-a1` | 07:52:09 | chk-3 | 07:54:21 | chk-3 | 2 |
| `ps2056-tme-off-a1` | 08:30:11 | chk-3 | 08:32:22 | chk-3 | 3 |
| `ps2056-tme-on-a2` | 11:38:29 | chk-3 | 11:40:41 | chk-3 | 3 |

Each attempt restored once.
In the scale-in and eviction trials the scheduler waited out a 120-second resource stabilization after the restart before it restored, and an eviction's replacement TaskManager registered within that wait (08:30:28 and 11:38:52), so the job restored at three subtasks without a second restart.

## The scale-out restores a checkpoint whose acknowledgements may not have been sent

A scale-out restores the checkpoint whose completion triggered the rescale.
Both scale-outs rescaled at the second checkpoint after the new TaskManager offered its slot (15:20:52 and 15:50:37), and the first had come 50 and 46 seconds after it.
The replay cohort was still being processed at 100 ms per message when that checkpoint completed, so the checkpoint covered messages that only its completion notification would acknowledge.

**With the setting on, the notification lost the race.**
The ledger the restore collected held 1,195 message ids per subscription, 2,390 in all, the messages of checkpoint 4's interval on each of the two readers.
The enumerator forwarded one chunk to each of the three restored readers, so both readers of subscription 0 held its 1,195 ids; each reader's `ledgerEntries` read 1,195, and their sum, 3,585, counts that chunk twice.
The source's `redeliveriesSuppressed` reached 2,390 by 15:54:58 and stayed there, and `ledgerEntriesExpired` stayed at 0.
The counter counts suppressed deliveries, not distinct ids, and an entry also suppresses a second copy, so it does not prove that each of the 2,390 ids came back once; that equal count is the simplest reading, and nothing in the evidence distinguishes it from a few ids redelivered twice.
The per-reader readings were 549 and 1,195 at the extremes, which with the sum leaves 646 for the third; that is consistent with subscription 1's reader suppressing its 1,195 and subscription 0's redeliveries split between its two readers.
The restored attempts emitted the remaining 2,360 replay-cohort messages, which checkpoint 3's notification had not acknowledged and checkpoint 4 had not covered, and the oracle counted no covered redelivery.

**With the setting off, the notification won.**
Before the restart the readers had acknowledged 2,047 of the cohort at checkpoint 4, and after it the restored attempts emitted 2,224, which leaves about 2,400 messages in checkpoint 5's interval, two minutes at 100 ms on each of two readers.
None of those came back, so the notification had acknowledged them before the cancellation, and the oracle again counted no covered redelivery.

The two settings therefore did not meet the same event, and the pair compares no duplicate counts.
What the on attempt shows is the event [#2049](https://github.com/flink-gcp/flink-connector-gcp/issues/2049) reported, on the deployed service and with real Pod scheduling, and the ledger removing all of it without losing an input.
Had the setting been off in that attempt, its 2,390 suppressed deliveries would have been processed again.
The trials of [#2127](https://github.com/flink-gcp/flink-connector-gcp/issues/2127) make that case occur on every run, as their [findings](0198-pubsub-restore-deduplication-trial-findings-2127.md) record.
The report in [#2049](https://github.com/flink-gcp/flink-connector-gcp/issues/2049) lost the race in one of three reactive scale-outs; two deployed scale-outs say nothing about the rate.

## The scale-in and eviction gave the ledger nothing to hold

A scale-in or eviction restarts the job when a TaskManager leaves, and the job restores its latest completed checkpoint.
In all four trials that was checkpoint 3, triggered between 31 and 46 seconds before the replay cohort's publication finished, and the source's last samples before the fault read the first cohort's 6,666 messages both received and acknowledged.
Checkpoint 3 therefore covered no unacknowledged message, and the enumerator logged `Deduplication ledger collected: 0 chunk(s) across 0 subscription(s)` in both attempts with the setting on, whose three ledger metrics read 0 throughout.
Checkpoint 3 held 1,468 bytes in both of those attempts, against 22,701 and 22,653 for their checkpoint 1, which covered the first cohort.

The 750 to 1,178 repeated inputs of these trials are replay-cohort messages that the attempts processed after checkpoint 3 and before the restart; the restored job had to process them again, and the setting does not, and should not, suppress them.
The two attempts with the setting on confirm that an empty ledger changes nothing, and that the enumerator waited for both remaining readers before it assigned splits after the scale-in, but they do not exercise suppression.

## Coverage limits

- **One attempt per cell.** Neither the frequency of the lost notification nor the spread of the counts is measured.
- **Suppression ran once.** Only `ps2056-rso-on-a1` lost a notification, so one restore exercised the ledger, with one shared subscription and no expiry.
- **The failover kinds miss the ledger by construction.** They inject the fault as soon as the cohort is published, so the latest completed checkpoint almost always predates the cohort; a ledger reaches such a restore only when a checkpoint completes between the publication and the fault, which the two-minute interval makes uncommon.
- **The rate is fixed.** The cohort's 100 ms per message keeps the task thread busy; an idle thread processed the notification first in every local cycle, so these trials say nothing about jobs whose thread is idle at the checkpoint.
- **No autoscaler.** The scale is a patch of the TaskManager replicas; an autoscaler's rescale reaches the same scheduler path, but none ran here.

## Cleanup

Every usable attempt ended with the supervisor's and the runner's cleanup, or the recovery workflow's, reporting success, and its receipt records a verified idle cluster and empty `flink-gcp`, `tier3-bootstrap` and `tier3-operator` plans.
