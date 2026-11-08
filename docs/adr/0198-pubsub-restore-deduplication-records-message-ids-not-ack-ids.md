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

# ADR-0198: Pub/Sub restore deduplication records message ids, not acknowledgement ids

- Status: Accepted
- Date: 2026-11-06 (client library facts read in google-cloud-pubsub 1.158.0; Flink runtime facts read in 2.2.1 and flink-core 1.20.5; emulator behavior measured 2026-11-06)
- Issues: [#2049]
- Modules: pubsub (`io.github.flink.gcp.connector.pubsub.source.streamingpull`)
- Refines: [ADR-0011] (readers now checkpoint ledger chunks, never consuming splits)
- Current behavior: `docs/content/docs/connectors/datastream/pubsub.md` § Restore deduplication
- Evidence: [reactive-mode trial findings](evidence/0198-pubsub-reactive-restore-deduplication-findings-2056.md)

## Context

The source acknowledges a checkpoint's messages when `notifyCheckpointComplete` reaches the reader, and Flink documents that notification as best effort (`CheckpointListener`).
A job cancelled between a checkpoint's completion and the notification restores that checkpoint, while the reader's close has nacked its messages, so they are processed again.
Under the AdaptiveScheduler the race is routine: a completed checkpoint is what triggers a rescale (`Executing.onCompletedCheckpoint` leads to `Restarting`, which cancels the graph), and the notification travels on a separate path from the checkpoint coordinator's thread.
[#2049] measured one lost race in three reactive-mode scale-outs, duplicating about 188,000 records, a whole two-minute interval; jobs writing through `FILE_LOADS` run longer intervals.
The Apache connector's legacy source keeps the acknowledgement ids of each checkpoint in operator state and acknowledges them after the restore; this repository's users migrated from it and lost that behavior.

## Decision

An opt-in setting, `restoreDeduplication(true)` (Table API `scan.restore-deduplication.enabled`), records with every checkpoint the message ids it covers that are not acknowledged yet, and after a restore acknowledges instead of emitting a redelivery whose id is in the restored ledger.

- **Content.** The ids of messages bound to a checkpoint and not yet acknowledged when the snapshot is taken (`AckTracker.checkpointedMessageIds`), keyed by subscription.
  A message received but not emitted is never included: its records are not in the checkpoint, so suppressing its redelivery would lose it.
- **Encoding.** The proto defines `message_id` only as a string unique within a topic, so the codec assumes no alphabet: ids are sorted by UTF-8 bytes and front-coded into self-contained chunks of about 1 MiB.
  An id of at most 17 ASCII digits, the shape of every id the service assigned in the measurement below, is packed into a `long` (`DecimalMessageIds`) whose order is the ids' byte order, so the codec sorts primitives and the restored ledger holds an open-addressing table of them; any other id stays a string, so the packing is an optimization and never an assumption.
  A string holding an unpaired surrogate has no UTF-8 form, and encoding it would turn it into a different id, so it is left out rather than altered; protobuf decodes a delivered id from UTF-8, so a delivered id never holds one.
  A malformed chunk is rejected with an `IOException` before any allocation its lengths claim, and the reader ignores a chunk it cannot read, since failing would fail every restore of that checkpoint.
- **State home.** A ledger chunk is a second kind of `SubscriptionSplit` that consumes nothing.
  A reader's snapshot holds only chunks, so [ADR-0011]'s rule (no consuming split is ever checkpointed) still holds and the enumerator remains the only owner of consumption.
  The split serializer moved to version 2 with a kind byte; version 1 was never written into a checkpoint, so a job with the setting off writes the same bytes as before.
- **Routing.** A restored chunk lands on whatever reader the round-robin operator state gives it, while Pub/Sub picks the stream a redelivery reaches.
  The reader releases the chunk's source output at once (`SourceOperator` opens one per restored split, and one that never emits would pin the watermark), uploads the chunk to the enumerator with a `SourceEvent` from `start()`, and reports completion even when it holds none.
  The enumerator parks every restored reader until all of them have reported, or until two minutes after the first registers, then sends each reader the chunks of every subscription its plan gives it, before its split assignment on the same ordered gateway.
  A restored enumerator awaits every subtask; a running one awaits each subtask that `addSplitsBack` resets, because a non-global recovery (`SchedulerBase.restoreState` with `isGlobalRecovery` false, the default scheduler's region failover) restores those subtasks' state without recreating the enumerator.
  The timeout counts from a registration, not from the enumerator's start, so slot allocation and the restore of chained operators' state stay out of it.
  The enumerator keeps every chunk it forwarded until a checkpoint completes outside a wait: a subtask that resets alone before then restores the same checkpoint, yet may have been forwarded chunks that lie in another subtask's state.
  A reader takes the payload out of each restored chunk split, because `SourceOperator` keeps the restored split objects for the execution's lifetime.
- **Lifetime.** The restored ledger lives in memory only, is never written into later checkpoints, and expires after a time-to-live counted from the reader's first consuming split, checked at every lookup and checkpoint.
  The default is the subscriber's `maxAckExtensionPeriod` (ten minutes when that is zero), capped at `Duration.ofNanos(Long.MAX_VALUE)` because that option has no upper bound of its own (ADR-0068); the owner chose the longest lease the previous execution could have held over the 600-second per-request maximum, since leases are extended.
  An entry stays after its first match, so a second copy is also suppressed.
- **Scope.** A restore of the whole source, which every AdaptiveScheduler restart is, and any region failover for the subtasks it restores.
  A region that leaves some source subtasks running covers only redeliveries that reach a restored subtask: a running reader is not sent a restored ledger, and under `NONE` Pub/Sub may route a redelivery to it.
- **Flink versions.** Both the 1.20 and 2.x lines, since the relay uses no 2.x-only API.

## Alternatives declined

- **Checkpointing acknowledgement ids**, as the Apache connector does.
  The streaming-pull `Subscriber` hands its receiver a `PubsubMessage` and an `AckReplyConsumer` only; the id stays in the private `MessageDispatcher.AckHandler` (`MessageDispatcher.java:694-701`).
  Not nacking on close would not help either: `MessageDispatcher.stop()` nacks every outstanding message under both shutdown modes (`:337-384`).
  Owning the stream through the GAPIC `streamingPullCallable()` would mean re-implementing lease extension, flow control and ordered dispatch.
- **`SupportsSplitReassignmentOnRecovery`** (flink-core 2.2.1, absent from 1.20.5), which hands restored splits to the enumerator directly.
  It sends all of a reader's restored splits in one `ReaderRegistrationEvent`; a scale-in to one reader would put the whole job's ledger into one RPC past `pekko.framesize`, and a failed operator-event send fails the task (`RpcTaskOperatorEventGateway`, `TaskExecutor`), which restores the same state and sends the same event again.
  Whether the frame-size bound fails that send was not measured; the restart loop it would cause decided against relying on it.
- **Carrying unmatched entries into later checkpoints** until no redelivery is possible.
  No signal says when that is, so the ledger would grow with every restore; a missed entry costs today's duplicate, never loss.
- **A probabilistic set**: a false positive would acknowledge a message whose records no checkpoint holds.

## Consequences

Each checkpoint holds about one interval's message ids, encoded on the task thread during the synchronous snapshot.
Measured on 2026-11-06 with 20,000 ids that the service assigned to a fresh topic (17-digit decimal strings, not contiguous across publish batches), the front-coded encoding took 3.12 bytes per id against 17 as strings, and 3.85 for a random tenth of them, the shape one of several readers sharing a subscription holds.
The id format is still undocumented, so the codec keeps assuming nothing about it.
Measured on 2026-11-07 on one workstation core with synthetic 17-digit ids, the packed path encodes 200,000 ids in about 15 ms (20 ms sorting strings) and one million in about 75 ms (100 ms), held in arrival order as the tracker holds them; the restored ledger takes about 40 bytes per id against about 110 for a `HashSet<String>`, which matters because every reader of a subscription under `NONE` holds the whole subscription's ledger.
The ids do not grow with publish time (twelve sequential publishes to one topic on 2026-11-07 came back in no order), so a range check cannot skip the lookup for a message published after the restore; a lookup costs about the same in either form, roughly 50 to 170 ns in the same measurement, depending on whether the id's table slot is in cache.
A checkpoint taken with the setting on holds ledger chunks in split serializer version 2, which a connector before 1.4.0 rejects, so that checkpoint cannot be restored by a downgrade; turning the setting off and completing one checkpoint empties the reader state again.
A restore transfers the ledger to the enumerator and on to each reader of its subscription, and consumption waits for that.
A seek inside the ledger's lifetime acknowledges, rather than emits, replayed messages that are in it.
A restore that drops downstream state, such as a sink whose uid changed, can lose the output the ledger assumes committed; that is a state-compatibility break the connector cannot detect.

`PubSubSourceRestoreDeduplicationITCase` reproduces the lost notification deterministically with a savepoint that does not stop the job, which Flink never notifies to tasks (`CheckpointCoordinator` notifies checkpoints and synchronous savepoints only).
Its control run, with the setting off, shows the emulator redelivering the covered messages, and its scale-out and scale-in runs fail when the emitter's suppression is disabled.
The emulator delivered every redelivery to one stream, so the IT still passed with the ledger forwarded to subtask 0 alone; `PubSubSplitEnumeratorLedgerTest` pins forwarding to every reader of a subscription.

Deployed reactive-mode trials on 2026-11-07 and 2026-11-08 ([#2056]) observed the lost notification once, in a scale-out with the setting on: the job restored the checkpoint whose completion triggered the rescale, and the ledger suppressed 2,390 redeliveries against the 2,390 ids that checkpoint covered, with the ledger forwarded to all three restored readers, two of which shared a subscription, and no input lost ([findings](evidence/0198-pubsub-reactive-restore-deduplication-findings-2056.md)).
The matching scale-out with the setting off acknowledged its checkpoint before the cancellation, and the scale-in and TaskManager eviction trials restored a checkpoint that covered no unacknowledged message, so their ledgers were empty.

[ADR-0011]: 0011-the-pubsub-source-is-flip27-streaming-pull-and-readers-checkpoint-no-splits.md
[#2049]: https://github.com/flink-gcp/flink-connector-gcp/issues/2049
[#2056]: https://github.com/flink-gcp/flink-connector-gcp/issues/2056
