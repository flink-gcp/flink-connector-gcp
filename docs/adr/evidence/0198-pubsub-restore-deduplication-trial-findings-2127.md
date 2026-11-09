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

# Pub/Sub restore deduplication trials: findings

This records the deployed trials of [#2127](https://github.com/flink-gcp/flink-connector-gcp/issues/2127), which ran the three [restore-deduplication kinds](../../../kubernetes/apps/pubsub/README.md#restore-deduplication-trials) that [#2126](https://github.com/flink-gcp/flink-connector-gcp/issues/2126) built, so that the source's restore deduplication ([ADR-0198](../0198-pubsub-restore-deduplication-records-message-ids-not-ack-ids.md)) meets the case it exists for on every run, as [#2054](https://github.com/flink-gcp/flink-connector-gcp/issues/2054) asked.
It follows the [reactive-mode findings](0198-pubsub-reactive-restore-deduplication-findings-2056.md) of [#2056](https://github.com/flink-gcp/flink-connector-gcp/issues/2056), whose trials left that case to chance and met it once.
The three trials ran on 2026-11-09 between 00:29Z and 01:50Z, one attempt each, and each reached a `usable` verdict.
In each, the job restored a checkpoint whose completion notification had not run, the restored ledger suppressed as many redeliveries as it held distinct message ids, and the oracle found every input once, with no repeated processing.
Times are UTC.

## Setup

Each trial runs the DataStream entry point with restore deduplication on and 10,000 records per subscription, 20,000 input messages over two subscriptions, with a 120-second checkpoint interval.
The replay cohort is 6,666 messages, 3,333 per subscription.
The relay sleeps 100 ms for each of them, and after each snapshot of an attempt that restored nothing it holds the first replay-cohort message for the kind's hold, which keeps that checkpoint's completion notification queued behind it on the task thread, and logs `event=pubsub-hold`.
The trial files are the three files under [`kubernetes/lifecycle/pubsub-trials`](../../../kubernetes/lifecycle/pubsub-trials) that carry these names:

- **`reactive-scale-out`**: reactive mode, two TaskManagers scaled to three, so that two subtasks then share subscription 0; a 30-second hold.
- **`reactive-tm-kill`**: reactive mode, three TaskManagers, one deleted without a grace period inside a 90-second hold.
- **`adaptive-scale-in`**: native mode under the AdaptiveScheduler, three subtasks bounded to two by a parallelism override; a 30-second hold.

Every attempt was dispatched from `main` at `f2ebb1af0`, the merge of [#2129](https://github.com/flink-gcp/flink-connector-gcp/pull/2129), which built the held relay of [#2126](https://github.com/flink-gcp/flink-connector-gcp/issues/2126), and pulled the `pubsub-recovery` image `sha256:66bdf70a584f64f1c75731d16a77b294e781be5c45347376f7b7b352c2f2894b` of the [Pub/Sub restore-deduplication trial publication](../../../kubernetes/images/README.md#pubsub-restore-deduplication-trial-publication), built from the same commit, with the Operator 1.16.1 mirror `sha256:91250bb08c4f1ea358b201680e355139a530cb7318439926d62c6387d7c1369d` and the pinned `lifecycle-tools` image `sha256:3ad53a9c6e2c5edac17228bc3b284ab43957db4e7287eef85fb77add89ee07db`, which that publication's section explains.

## Attempts

| Trial | Attempt | Run | Lifecycle (UTC) | Verdict |
| --- | --- | --- | --- | --- |
| `reactive-scale-out` | `ps2127-rso-a1` | [42540417228](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42540417228) | 00:29 to 00:54 | `usable` |
| `reactive-tm-kill` | `ps2127-tmk-a1` | [42542767609](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42542767609) | 00:55 to 01:23 | `usable` |
| `adaptive-scale-in` | `ps2127-asi-a1` | [42545398726](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42545398726) | 01:24 to 01:50 | `usable` |

`flink-tier3 analyze` over each exported evidence prefix recomputed the stored verdict, with 11, 10 and 12 measurement samples and no reasons or problems.

## Result

| Attempt | Logical inputs | Lines | Missing | Repeated processing | Covered redeliveries | Duplicate publications or deliveries |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `ps2127-rso-a1` | 20,000 | 20,000 | 0 | 0 | 0 | 0 |
| `ps2127-tmk-a1` | 20,000 | 20,000 | 0 | 0 | 0 | 0 |
| `ps2127-asi-a1` | 20,000 | 20,000 | 0 | 0 | 0 | 0 |

Covered redeliveries are inputs that a restored attempt processed again although an earlier run had processed them before a barrier in the restored checkpoint's lineage, which is what restore deduplication exists to suppress; the [oracle](../../../kubernetes/apps/pubsub/README.md#restore-deduplication-trials) defines them.
Repeated processing also counts inputs processed after the restored checkpoint's barrier and before the restart, and there were none: the hold stopped each subtask's thread at the first replay-cohort message after that barrier, and the fault came inside the hold.
The last column sums duplicate input publications, duplicate output publications and repeated output deliveries.

## Each fault landed inside a hold

| Attempt | Checkpoint triggered | Attempts that logged a hold after it | Completed | Fault | `CANCELLING` | Restored | Restored parallelism |
| --- | --- | ---: | --- | --- | --- | --- | ---: |
| `ps2127-rso-a1` | chk-5, 00:47:03.755 | 2 | 00:47:05.126 | rescale on that completion | 00:47:05.134 | 00:47:05.714 | 3 |
| `ps2127-tmk-a1` | chk-4, 01:11:05.030 | 3 | 01:11:06.228 | Pod deleted 01:11:22.266 | 01:11:22.357 | 01:13:32.952 | 3 |
| `ps2127-asi-a1` | chk-4, 01:39:11.611 | 3 | 01:39:12.992 | rescale on that completion | 01:39:12.997 | 01:39:13.486 | 2 |

Times of completion and `CANCELLING` are the JobManager's log times, and restore times are those of the job's checkpoint statistics.
Every Flink attempt that ran before the fault logged a hold after the checkpoint the job then restored, and each trial restored once.
The JobManager logged `CANCELLING` 8 and 5 ms after the completion that triggered each rescale, and 91 ms after the supervisor's clock timed the TaskManager's deletion, all far inside the hold, so no subtask's thread could have run that checkpoint's notification before the cancellation dropped it.

The scale-out's patch came at 00:44:03.906, and the scheduler rescaled on the second checkpoint after it; checkpoint 4, which completed at 00:45:05 inside its own hold, did not trigger the rescale, and its notification ran once that hold ended.
The kill's restore came 130 seconds after the deletion, past the scheduler's 120-second resource stabilization, at three subtasks, so the Deployment's replacement TaskManager had registered within that wait and the job restored once.
For the scale-in, Operator 1.16.1 logged the patch as `SCALE change(s) detected` with the override `null -> 1fc1e910bda7a08a74f8b1a434d3e9b9:2` at 01:38:07.090, and the job's resource requirements dropped to two slots 0.9 seconds later; the diff also listed `job.upgradeMode : stateless -> savepoint`, which did not change that classification.
The Operator then logged `In-place scaling triggered` and reported generation 2 deployed; no savepoint was taken, and the job kept its JobManager and restored checkpoint 4, so the change did not fall back to an upgrade.

## Suppression totals matched the distinct ledger sizes

| Attempt | Chunks | Subscriptions | Restored readers | `ledgerEntries` per reader | Distinct ids | `redeliveriesSuppressed` per reader | Suppressed, summed |
| --- | ---: | ---: | ---: | --- | ---: | --- | ---: |
| `ps2127-rso-a1` | 2 | 2 | 3 | 898, 898, 898 | 1,796 | 319, 579, 898 | 1,796 |
| `ps2127-tmk-a1` | 3 | 2 | 3 | 1,754, 1,754, 836 | 2,590 | 811, 943, 836 | 2,590 |
| `ps2127-asi-a1` | 3 | 2 | 2 | 2,202, 1,066 | 3,268 | 2,202, 1,066 | 3,268 |

The measurement samples carry each metric's minimum, maximum and sum over the subtasks; with at most three readers that fixes the third value, and the enumerator's `Forwarded` log lines say which reader held which chunks.
Each reader of a subscription holds that subscription's whole ledger, so where two readers share one, as in the scale-out and the kill, the sum of `ledgerEntries` counts its ids twice; the distinct ids are one copy per subscription.
In the scale-out, subscription 0's 898 ids reached both of its readers, and the readings are consistent with those two suppressing 319 and 579 of them and subscription 1's reader its 898; the samples do not name which reader read which value.
In the kill, subscription 0's two readers had each uploaded a chunk, so both of its restored readers held both chunks, 1,754 ids, and the readings are consistent with those two suppressing 811 and 943 of them and subscription 1's reader all 836 of its own.
In the scale-in, the two chunks of the subscription that two readers had shared went to its one restored reader, and the readings are consistent with that reader holding and suppressing 2,202 and the other 1,066.

`redeliveriesSuppressed` counts suppressed deliveries, not distinct ids, so a sum equal to the distinct ids does not by itself prove that each id came back once; that is the simplest reading in all three, and nothing in the evidence distinguishes it from a few ids delivered twice and as many not at all.
The counters reached those values within one minute of the restore in the scale-out and within three in the others, and stayed there to the end; `ledgerEntriesExpired` stayed at zero throughout.
With the setting off, those deliveries would have been processed again and counted as covered redeliveries; these trials did not run that configuration, because the local reproduction of [#2049](https://github.com/flink-gcp/flink-connector-gcp/issues/2049) and `PubSubSourceRestoreDeduplicationITCase`'s control run already show it.

## Coverage limits

- **One attempt per kind.** A single usable attempt does not measure how often a kind reaches its verdict.
- **The hold makes the case certain.** The trials show that the ledger handles the case when it occurs, not how often a job without the hold meets it.
- **The kill does not control how the leases end.** The kubelet's shortest termination still signals the container, so whether the dying reader nacked its leases or left them to lapse is not recorded; the notification was lost either way.
- **No autoscaler.** The scale-in changes the parallelism override that Operator 1.16.1 applies through the same resource-requirements path its autoscaler uses, but no autoscaler ran.
- **No expiry.** No ledger entry expired, so the trials do not exercise expiry.
- **No post-barrier replay.** Because the hold stops each subtask at the barrier, these trials do not exercise the ordinary at-least-once replay of inputs processed after the restored checkpoint, which the [#2056](https://github.com/flink-gcp/flink-connector-gcp/issues/2056) trials did and which the setting leaves alone.

## Cleanup

Every attempt's workflow completed successfully, including the step that verifies an idle cluster again, retains the result and releases the environment lock; since plan-free finalization, these runs plan no OpenTofu root.
