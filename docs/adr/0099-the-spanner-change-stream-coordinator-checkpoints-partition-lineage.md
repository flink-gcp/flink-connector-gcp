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

# ADR-0099: The Spanner Change Streams coordinator checkpoints partition lineage

- Status: Accepted
- Date: 2026-08-12; revised 2026-08-13, by [#1141](https://github.com/flink-gcp/flink-connector-gcp/issues/1141) on 2026-08-29, by [#1842](https://github.com/flink-gcp/flink-connector-gcp/issues/1842) on 2026-10-20, and by [#2193](https://github.com/flink-gcp/flink-connector-gcp/issues/2193) on 2026-11-19
- Issues: [#222](https://github.com/flink-gcp/flink-connector-gcp/issues/222),
  [#534](https://github.com/flink-gcp/flink-connector-gcp/issues/534),
  [#535](https://github.com/flink-gcp/flink-connector-gcp/issues/535),
  [#635](https://github.com/flink-gcp/flink-connector-gcp/issues/635),
  [#1141](https://github.com/flink-gcp/flink-connector-gcp/issues/1141),
  [#1842](https://github.com/flink-gcp/flink-connector-gcp/issues/1842),
  [#2193](https://github.com/flink-gcp/flink-connector-gcp/issues/2193)
- Modules: base, spanner (`source.changestream`)
- Current behavior: `source.changestream.enumerator.SpannerChangeStreamSplitEnumerator`

## Context

A Spanner change-stream query begins with a null partition token and ends after returning every child-partitions record for that partition.
A child names its parent tokens, and a merged child must not start until every parent query has finished.
Starting it earlier can reorder changes for one key across the parent-child boundary.

Apache Beam's splittable-function implementation stores the partition lifecycle in a Spanner metadata table.
FLIP-27 already has a coordinator whose state participates in Flink checkpoints.
Adding the metadata table here would create a second recovery record with separate permissions, lifecycle, and consistency failure modes.

## Decision

The split enumerator owns one checkpointed partition ledger.
Each entry derives a stable split id from its optional Spanner token and carries parent identities, start and end timestamps, heartbeat interval, current read position, lifecycle state, and watermark.
The lifecycle is `CREATED`, `SCHEDULED`, `RUNNING`, then `FINISHED`.
The initial entry has no Spanner token, and the coordinator gives its null parent a reserved identity that cannot collide with a real token-derived id.

Child discovery and parent completion are separate source events.
A parent query can return several child-partitions records with the same start timestamp, so receiving one record does not prove that the query finished.
The reader reports every child record first and reports completion only after the query ends successfully.
The coordinator deduplicates identical child reports and moves a child from `CREATED` to `SCHEDULED` only when every named parent is `FINISHED`.

A finished parent's children wait, by default, for a checkpoint as well ([#2193](https://github.com/flink-gcp/flink-connector-gcp/issues/2193)).
A parent's finish is its reader's report that the query ended, and the parent's last records can still be in that reader's network buffer or a sink writer's batch at that moment.
A child read by another subtask sends its records down a different path, so they can overtake them and reach a keyed operator out of commit order.
With `PartitionHandoff.AFTER_CHECKPOINT`, the coordinator records each finished parent as held, gives a held parent the id of the first checkpoint whose `snapshotState` runs after the finish, and schedules its children only once a checkpoint with that id or a later one completes.
The reader emitted the parent's records before it sent the finish, and the coordinator snapshots before the readers receive the barrier, so those records precede the barrier.
A completed aligned checkpoint therefore means every operator that handles its input before the barrier processed them and every sink that flushes on the barrier wrote them.
An unaligned checkpoint, and an operator that keeps pending elements in its state (asynchronous I/O, an asynchronous sink's buffered requests), are outside that argument, and so is a failover, which replays from the last completed checkpoint at least once.
Holds are runtime state and never enter the checkpoint: only completed checkpoints are restored, so every finish a restored ledger records is already released.
Nothing is held at source parallelism 1, where every record takes one path, or after a parent whose reader emitted no output record, which the reader reports with the finish: nothing of that parent is downstream to overtake.
That covers the initial null-token query without assuming it never returns data; Spanner's query workflow shows it returning child partitions, but its record rules allow any record type in any query.
The coordinator receives no checkpoint configuration, so it measures a job without checkpointing: when no checkpoint has completed and a child has waited `firstCheckpointTimeout` (10 minutes by default, on a monotonic clock), it releases every hold with a warning and holds nothing until one completes.
The evidence is a completed checkpoint, not a `snapshotState` call: Flink 2.2.1 snapshots coordinators for a savepoint too but notifies them only when a checkpoint or a synchronous savepoint completes (`CheckpointCoordinator`, the `!props.isSavepoint() || props.isSynchronous()` guard on `sendAcknowledgeMessages`; the same in the 1.20.5 bytecode), so a savepoint taken while a job without periodic checkpointing runs would otherwise end the measurement and hold the children for good.
`PartitionHandoff.IMMEDIATE` schedules children as soon as their parents finish, for consumers that order each key's changes by their commit coordinates.

The coordinator checkpoints only unfinished entries.
A finished parent leaves the live ledger immediately and remains as a compact split-id proof only while an already-discovered `CREATED` child still names it.
Successful query completion comes after every child record, so no future child can first name that parent; after all known children become schedulable, the proof is no longer needed and is removed.
Reader progress events keep the running entries' current position and watermark monotonic.
A returned split keeps that progress and moves back to `SCHEDULED`; a restored running entry waits for Flink to return its reader-owned split rather than being assigned twice.
The checkpoint records boundedness separately, so a bounded source whose final live entry has been removed can restore an empty ledger and still signal no more splits.

The coordinator also owns the source-wide event-time frontier.
It is the minimum safe watermark across every unfinished `CREATED`, `SCHEDULED`, and `RUNNING` entry in the compact ledger, including partitions that no reader currently owns.
Finished-parent proofs carry no progress and do not participate in this minimum.
The enumerator checkpoints the last emitted source frontier beside the ledger and sends it to every registered reader, including readers with no split, so restore and rescaling cannot move event time backwards.
At runtime, a counted ordered index tracks unfinished partition watermarks and is rebuilt from the ledger after restore; ordinary data progress with an unchanged partition watermark does not scan or rewrite that index.

Fresh initialization resolves `StartPosition` once and creates exactly one null-token entry.
Restore validates every unfinished entry against the one retained window from ADR-0094 and never creates an initial entry while the ledger remains valid.
The connector reads retention and partition mode through dialect-specific `CHANGE_STREAM_OPTIONS` queries behind a serializable factory and a small runtime client interface.
An absent retention row uses the configured fallback, whose connector default is seven days, while `MUTABLE_KEY_RANGE` is rejected because its partition start, end, move-in, and move-out records are outside this source's model.

Spanner resets the whole ledger when an explicit fallback recovers an expired restore.
Advancing one old partition token can skip the child-partitions record that ended its query, leaving the coordinator unable to discover the next topology.
The reset discards every stale dependency and starts one new null-token query at the resolved fallback, with a warning that names the unavailable range and duplicate boundary.
Without the explicit fallback, expiry fails the job.

A bounded source carries its bound in the ledger, and [#1842](https://github.com/flink-gcp/flink-connector-gcp/issues/1842) made the bound public as `boundedTimestamp(...)` and `scan.bounded.timestamp-millis`.
The initial entry takes the configured bound as its end timestamp, and every child inherits its parent's.
Spanner's read function returns records committed at or before its end timestamp and then ends the query, and its [documentation](https://cloud.google.com/spanner/docs/change-streams/details) states that a child-partitions record's start timestamp always falls between the query's start and end timestamps, so every partition of the lineage reads to the same inclusive bound and the bounded ledger finishes when its last entry does.
A fresh start or a resume fallback that resolves after the bound fails initialization, since the query would have nothing to read.
A restore must configure the bound the ledger was checkpointed with, or none for an unbounded ledger, and otherwise fails initialization.
The checkpoint records the bound itself, because a bounded ledger whose every partition has finished holds no partition to carry it, and without it a restore under a later bound would finish at once without reading the interval between the two; a completed version-3 ledger, which recorded only that it was bounded, is compared by boundedness alone.
The check is skipped for one case: the whole-ledger fallback is the one restore that starts under the configured bound, because it begins a new lineage.

Readers wait for a coordinator initialization event before opening any restored split.
The event preserves reader-owned splits after a valid restore and discards them after an explicit whole-ledger fallback.
This handshake is necessary because Flink restores reader state independently of the enumerator's asynchronous metadata check; without it an expired token can reach Spanner before the coordinator has decided whether to fail or fall back.

Both checkpoint serializers use connector-owned versioned formats.
They encode optional values and lifecycle states with explicit tags and reject unknown versions, tags, negative counts, and invalid timestamps.
Enumerator state version 4 writes the bound or its absence, unfinished entries, finished-parent proof IDs, and the source-wide frontier.
Version 3 wrote only whether the ledger was bounded, so a version-3 ledger's bound is read from its partitions and is unknown once every partition has finished.
Versions 1 and 2 remain readable as complete ledgers; restore validates their full graphs before deriving the conservative version-1 frontier and compacting finished entries into only the proofs still needed by `CREATED` children.

## Evidence

The real-GCP acceptance produced two child partitions in each 5,000-row recovery run and observed work on both reader subtasks.
It intentionally failed after a completed checkpoint, recovered every unique mutation id with the allowed inclusive-boundary duplicates, stopped with a savepoint, and consumed both a mutation written while stopped and one written after restore.
Separate stale-state savepoints verify the reader handshake and whole-ledger fallback in both dialects.
A slow serializer test migrates a version-2 chain with 100,000 historical transitions and requires version 3 to retain one live entry and no proofs.
Its 2026-08-29 local measurement reduced the checkpoint from 10,477,838 bytes to 123 bytes; the test reports checkpoint bytes, migration, snapshot and restore times, exact retained lineage-entry counts, and current-thread heap allocation during legacy and compact snapshot serialization rather than fixing environment-dependent byte or timing observations as thresholds.

The bounded contract is exercised on the emulator in both dialects.
A DataStream job bounded at the last seeded commit timestamp recovers from a failure after a checkpoint and still delivers that last record, and a Table API source bounded in milliseconds finishes with every change up to the bound and none committed after it.

The partition hand-off ([#2193](https://github.com/flink-gcp/flink-connector-gcp/issues/2193)) was measured on 2026-11-18 against emulator 1.5.59 with partition tokens alive for 2 to 4 seconds, one writer changing 4 keys with an increasing value for 60 seconds, source parallelism 4 and a checkpoint every second.
The emulator also moves a key between partitions outside the lineage, which the service does not, so each per-key inversion at a `keyBy` recorder was classified by whether the earlier record's partition is an ancestor of the later one's.
Without the hold, 3 of 6 runs showed ancestor-partition inversions (17, 102 and 74); with a prototype of it, 10 runs showed none, and a parallelism-1 control showed none either.
With a 1-second network buffer timeout, 2 runs under `IMMEDIATE` showed 66 and 67 ancestor-partition inversions, while 3 runs of the hold as implemented showed none.
Those 3 runs still showed inversions between partitions the emulator moved a key across outside the lineage (81, 188 and 102), because a parent that emitted nothing now hands off at once; an earlier version that held after every parent showed none of those either, which was a side effect of its delay rather than a guarantee, since the service keeps a key in its lineage.
A chained `KafkaSink` showed no ancestor-partition inversion in these runs; its window, the producer's linger time and in-flight batches, is narrower than a network buffer's.
Flink CDC's MySQL source makes the same trade between its snapshot and binlog phases: its assigner waits for a checkpoint taken after every snapshot split finished, and skips the wait at parallelism 1.
Unit tests cover a checkpoint snapshotted before the finish, an uncompleted checkpoint covered by a later one, restore, parallelism 1, the immediate mode, a parent that emitted nothing, the first-checkpoint timeout and its single warning, a savepoint that does not end the wait, a wait measured from the oldest hold, and a zero timeout.
An integration test runs a scripted two-level lineage through Flink's own source coordinator at parallelism 2, with checkpointing and without it.

## Alternatives declined

- **Let a restored ledger keep its bound whatever the configuration says.**
  The source's declared boundedness comes from the configuration, so a restore would then read to a bound other than the one configured, or not stop at all, without telling the caller.
- **Re-bound a restored ledger to the configured bound.**
  The compact ledger has already dropped finished partitions, so a later bound could not resume the ones that ended at the old bound, and an earlier bound cannot withdraw records already emitted beyond it.
- **Use Beam's external metadata table.**
  Flink checkpoints already provide one atomic recovery record for enumerator and reader state, while a table would add resource provisioning and another consistency boundary.
- **Mark a parent finished on its first child record.**
  One query can return several child records, so this could schedule a child while its parent still has records to report.
- **Restart only each expired token.**
  The skipped interval can contain that token's terminal child record, which would strand all descendants even though the restarted query itself succeeded.
- **Delay the finish in the reader until its own checkpoint completes.**
  It holds the same order, but the reader would have to checkpoint finished-but-unreported splits and replay their finish after a restore, while the coordinator's hold is runtime state that a restore discards correctly.
- **Fail a job without checkpointing, as the Pub/Sub source's first-checkpoint detector does.**
  A Spanner job without checkpointing loses nothing by starting children at once; it loses only the cross-partition order, so a warning and a fallback are proportionate where a Pub/Sub job would stall.
- **Make `IMMEDIATE` the default.**
  The reordering is silent, and an upsert sink then keeps a stale row; a delay of one checkpoint interval per partition split or merge is the cheaper default.
- **Accept unknown partition modes as immutable.**
  A newly added mode can introduce record types that change the lifecycle protocol, so consuming it without an explicit implementation would be partial support.

## Consequences

- Checkpoint and coordinator lineage state scale with unfinished topology and unresolved parent proofs rather than job lifetime.
- By default each partition split or merge delays its children until the next checkpoint completes, up to one checkpoint interval plus that checkpoint's duration, and the source watermark holds at the children's start meanwhile; once a checkpoint has completed, the children do not start while later checkpoints keep failing.
- The order holds between failures and for operators that handle their input before the barrier; an unaligned checkpoint, an operator that stores pending elements, and a failover's replay are outside it.
- `changeStreamPartitionLedgerEntries` and `changeStreamFinishedParentProofs` expose those two bounded structures without partition-token labels.
- Version-1 and version-2 restore pays one validation and compaction pass before subsequent checkpoints use version 4.
- The public source and reader can be added independently in #536 because the coordinator-facing split and event contracts are now fixed.
- Real-service partition, checkpoint, savepoint, retention, and fallback behavior is covered by the gated acceptance in #535.
