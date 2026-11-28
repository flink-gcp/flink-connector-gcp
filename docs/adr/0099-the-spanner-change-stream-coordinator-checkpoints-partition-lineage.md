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
- Date: 2026-08-12; revised 2026-08-13, by [#1141](https://github.com/flink-gcp/flink-connector-gcp/issues/1141) on 2026-08-29, by [#1842](https://github.com/flink-gcp/flink-connector-gcp/issues/1842) on 2026-10-20, by [#2193](https://github.com/flink-gcp/flink-connector-gcp/issues/2193) on 2026-11-19, by [#2205](https://github.com/flink-gcp/flink-connector-gcp/issues/2205) on 2026-11-20, by [#2204](https://github.com/flink-gcp/flink-connector-gcp/issues/2204) on 2026-11-21, by [#2257](https://github.com/flink-gcp/flink-connector-gcp/issues/2257) on 2026-11-21, and by [#2404](https://github.com/flink-gcp/flink-connector-gcp/issues/2404) on 2026-11-28
- Issues: [#222](https://github.com/flink-gcp/flink-connector-gcp/issues/222),
  [#534](https://github.com/flink-gcp/flink-connector-gcp/issues/534),
  [#535](https://github.com/flink-gcp/flink-connector-gcp/issues/535),
  [#635](https://github.com/flink-gcp/flink-connector-gcp/issues/635),
  [#1141](https://github.com/flink-gcp/flink-connector-gcp/issues/1141),
  [#1842](https://github.com/flink-gcp/flink-connector-gcp/issues/1842),
  [#2193](https://github.com/flink-gcp/flink-connector-gcp/issues/2193),
  [#2205](https://github.com/flink-gcp/flink-connector-gcp/issues/2205),
  [#2204](https://github.com/flink-gcp/flink-connector-gcp/issues/2204),
  [#2257](https://github.com/flink-gcp/flink-connector-gcp/issues/2257),
  [#2404](https://github.com/flink-gcp/flink-connector-gcp/issues/2404)
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

A reader keeps a finished split in its checkpointed state until the coordinator acknowledges the finish ([#2205](https://github.com/flink-gcp/flink-connector-gcp/issues/2205)).
Flink snapshots the coordinator before it injects a checkpoint's barriers, and a reader's event sent between the two reaches neither snapshot.
Before this, a finish sent there left the partition running in the restored ledger while no restored reader held it, so its children never started.
The finished split now carries the child reports the reader forwarded, and a restored reader sends those reports and the finish again once the coordinator is initialized.
The coordinator acknowledges every finish when it handles it, unless the reader may not be reached yet (below), in which case the acknowledgement goes out with the reader's next poll report.
Applying a report again changes nothing: a finish or child report for a partition the ledger no longer runs is ignored.
A partition that finished but is still pending, as described next, stays in the ledger, so a repeated report for it is ignored as well.
Flink holds a coordinator's events to a subtask back between the coordinator's snapshot and that subtask's barrier, so a finish the coordinator applied after its snapshot is still in the reader's snapshot of the same checkpoint.
The `TaskExecutor` of Flink 2.2.1 and 1.20.5 fails the sending task when a reader-to-coordinator event cannot be delivered, so a restore is the only way a finish can be lost; unlike Flink CDC's MySQL source, which keeps its finished snapshot splits the same way, the coordinator does not periodically ask readers to report again.
A re-sent finish says that its reader emitted nothing, because what the earlier attempt emitted before the restored checkpoint is downstream already, so it holds no child back.
The reader's split position and watermark also stop before a partition's first child-partitions record, as the Debezium Spanner connector's offset does.
Spanner ends a partition at its children's start timestamp, and what a query starting exactly there returns is neither documented nor measured here, so a restored query starts before it and meets the child records again; a restore repeats the records from that position on, never behind the split's own watermark.

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

A finish is final only once a completed checkpoint covers it ([#2204](https://github.com/flink-gcp/flink-connector-gcp/issues/2204)), as a Debezium partition is `FINISHED` only once its offsets are committed.
Region failover, the default, restarts the failed tasks from the last completed checkpoint but keeps this coordinator, and Flink returns the splits it assigned since that checkpoint through `addSplitsBack`.
A finished partition therefore stays in the live ledger as a pending `FINISHED` entry, with the id of the first checkpoint snapshotted after its finish, until a checkpoint with that id or a later one completes; only then does it leave the ledger.
A returned split restarts where Flink assigned it, not where its reader reported progress since, because that subtask's downstream rolled back with it; a returned split whose finish is pending is read again and becomes unfinished.
A restored reader names the unfinished splits it restored from `start()`, before its task reports `RUNNING`, and a pending finish among them becomes running again, so a parent read again makes its children wait for its next finish.
After either rollback, a scheduled child of an unfinished parent goes back to `CREATED`, a finished parent it still names is proven again, and the runtime indexes are rebuilt from the ledger; a child a live reader already reads keeps running, so its order relative to the re-read parent is not restored.
Every finish is pending in this way, one whose reader emitted nothing included: a parent read again reports that partition as its child again, and only a ledger entry lets the report find it.
When no checkpoint completes within `firstCheckpointTimeout` of the source starting, every finish becomes final at once until one does, because a job without checkpointing would otherwise keep every pending finish; a region failover then does not read a finished partition again.

The coordinator checkpoints only unfinished entries.
A pending finish is final relative to the checkpoint whose snapshot sees it, so the snapshot leaves it out.
A finished parent remains as a compact split-id proof only while an already-discovered `CREATED` child still names it.
Successful query completion comes after every child record, so no future child can first name that parent; after all known children become schedulable, the proof is no longer needed and is removed.
Readers report progress in two ways, never per record: when a checkpoint completes, each reader sends the positions its snapshot recorded for it, as Flink CDC's `IncrementalSourceReaderWithCommit` commits its offsets, and until its first completed checkpoint it also sends its current positions at most once a second.
Committed reports keep the running and pending entries' positions and watermarks monotonic; current positions apply only while no checkpoint has completed within `firstCheckpointTimeout`.
A restored running entry waits for Flink to return its reader-owned split rather than being assigned twice.
The checkpoint records boundedness separately, so a bounded source whose final live entry has been removed can restore an empty ledger and still signal no more splits.

The coordinator also owns the source-wide event-time frontier.
It is the minimum safe watermark across every `CREATED`, `SCHEDULED`, `RUNNING` and pending `FINISHED` entry in the ledger, including partitions that no reader currently owns.
Because running entries carry committed positions and a pending finish still counts, the frontier never passes a record that a region failover reads again, as Debezium's low watermark follows committed offsets; it trails the newest records by about one checkpoint interval.
Finished-parent proofs carry no progress and do not participate in this minimum.
The enumerator checkpoints the last emitted source frontier beside the ledger and sends it to every registered reader, including readers with no split, each once it may be reached (below), so restore and rescaling cannot move event time backwards.
At runtime, a counted ordered index tracks unfinished partition watermarks and is rebuilt from the ledger after restore; ordinary data progress with an unchanged partition watermark does not scan or rewrite that index.

No coordinator event reaches a reader that may have finished ([#2404](https://github.com/flink-gcp/flink-connector-gcp/issues/2404)).
A stop-with-savepoint without drain stops each source before its task snapshots the savepoint (`SourceOperatorStreamTask#triggerStopWithSavepointAsync` in Flink 2.2.1 and 1.20.5), and the task finishes once the savepoint completes, while the JobManager can still see it running.
An event that reaches the task then is lost, and `SubtaskGatewayImpl` fails the task for it, which fails the stop.
Only an event that declares itself loss-tolerant is exempt, as Flink's own `WatermarkAlignmentEvent` does; a connector `SourceEvent` travels in `SourceEventWrapper`, which does not, and neither do split assignments and end signals.
`notifyCheckpointComplete` does not say whether the checkpoint is that savepoint, and completing a checkpoint makes finishes final and releases children, so the coordinator would send exactly then.
A stopped source still snapshots but is never polled again (`SourceOperator` returns `STOPPED` without calling `pollNext`), so each reader reports, from its first poll after a snapshot, which checkpoint it snapshotted, and an idle reader wakes for that poll.
After the coordinator's own `snapshotState` for a checkpoint, it sends a reader nothing, no assignment, watermark, finish acknowledgement or end signal, until that reader reports that checkpoint or a later one; what waited goes out when the report arrives.
The coordinator snapshots before the readers do, so a report can only follow the snapshot it names.
The gating snapshot is the latest one the coordinator took and was not told was aborted, and an abort makes the latest earlier one gate instead; when every snapshot was aborted, nothing gates.
A stop-with-savepoint's savepoint is the latest snapshot when it is taken, so a snapshot taken after it, a manual savepoint for instance, leaves it gating when that one is aborted; its own abort fails the stop anyway, because Flink fails a task whose synchronous savepoint is aborted.
A batch job's recovery snapshot, which a `HybridSource` forwards with Flink's `BATCH_CHECKPOINT_ID`, stops no task and has no reader snapshot to report, so it gates nothing.
When a poll report or a registration finds more than one snapshot held, those below the oldest one every registered reader has reported are forgotten, since they could gate no reader.
Flink notifies a coordinator of every abort of a checkpoint it snapshotted, because `CheckpointCoordinator` creates the pending checkpoint before it snapshots coordinators, and aborting a pending checkpoint calls `notifyCheckpointAborted` on them.
The initialization event goes out at once, because only a newly registered reader receives it, and such a reader is running.
A newly registered reader is reached at once as well.
A reader registers while its task initializes, and Flink triggers a checkpoint only once every task runs, so an attempt the coordinator learns of after a snapshot normally takes no part in it; making it wait for that snapshot instead would leave it waiting for good after an ordinary savepoint, whose completion Flink does not announce.
The window this leaves, a registration that reaches the coordinator after the snapshot of a checkpoint the attempt takes part in, is the gap between two messages one TaskManager sends one after the other: the reader's registration, then the task's switch to running.
A request, a finish acknowledgement or a watermark that waited for a failed attempt does not pass to the next one.
A reader that has read all its input reports nothing: it needs nothing more, and under a `HybridSource`, whose coordinator forwards every reader event other than its own finish event to the current source's coordinator, the next source would receive the report.
The wait is runtime state: a restored coordinator has snapshotted nothing yet and sends at once.
The gate covers only snapshots this coordinator took.
Under a `HybridSource` whose later source is this one, the switch between sources is outside it: Flink's `SwitchSourceEvent`, the initialization broadcast of a coordinator that has taken no snapshot yet, and a reader that registers when it finishes replaying the previous source can all reach a task a stop-with-savepoint stopped around the switch ([#2406](https://github.com/flink-gcp/flink-connector-gcp/issues/2406)); schedule a planned stop after the change-stream source has become active.

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
The discard applies until a checkpoint completes after the fallback: a reader that fails before then restores the discarded ledger's splits again, while one that fails later restores the new checkpoint's.
This handshake is necessary because Flink restores reader state independently of the enumerator's asynchronous metadata check; without it an expired token can reach Spanner before the coordinator has decided whether to fail or fall back.

Both checkpoint serializers use connector-owned versioned formats.
Split format version 2 appends the finish report; a version-1 split reads as one without a report, and the enumerator state's per-split layout does not carry it.
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

The lost finish ([#2205](https://github.com/flink-gcp/flink-connector-gcp/issues/2205)) is reproduced by an integration test that runs a second source whose coordinator blocks in its own snapshot, which holds the checkpoint's barriers back while a scripted partition finishes.
On the code before this revision, a restore from that checkpoint never read the partition's child, both when only the finish and when the child report as well fell between the snapshot and the barrier; the control, a partition that finished before the checkpoint, read it.
A region failover is reproduced by an integration test with a `keyBy` behind the source at parallelism 2, explicit checkpoints, and a map that throws once; a downstream operator counts a record only once a checkpoint covering it completes.
After checkpoint 1, one child runs to its finish and another emits ten records before the failure; on the code before the [#2204](https://github.com/flink-gcp/flink-connector-gcp/issues/2204) revision 15 or 16 of their 35 records, depending on timing, were never counted, while the control, with a second checkpoint covering both, lost none, and with the revision neither variant loses a record.
For the lost finish, with the acknowledgement, all three cases read the child and never reopen the finished partition, and the window cases fail unless the restored reader logs that it reported a finish again, so a run whose coordinator snapshot came too late to open the window cannot pass silently.

The stop-with-savepoint failure ([#2404](https://github.com/flink-gcp/flink-connector-gcp/issues/2404)) was found by the E2E run of 2026-11-28 and reproduced locally with the scripted two-partition source at parallelism 2, a checkpoint every 250 ms, and a stop-with-savepoint as soon as its first records arrived.
On `9af92ddc0` the stop failed in 41 of 200 runs, each time with `TaskNotRunningException` for a `SpannerChangeStreamWatermarkEvent` sent from `notifyCheckpointComplete`; on v1.4.0, whose coordinator sent nothing at checkpoint completion, it failed in none of 200, and with this revision in none of 200.
Unit tests hold that a completed checkpoint, a committed progress report, a split request and a report for an older snapshot send nothing to a reader that has not reported, that a report before the completion lets it send at once, that an aborted snapshot leaves the latest earlier one gating until every one is aborted, in either order, and then sends at once to a reader that already reported the one gating now, that a batch recovery snapshot gates nothing, that a new attempt is reached at once and inherits neither its predecessor's request nor its acknowledgements, and that a finish acknowledgement and a bounded ledger's end signal, whether its request came before or after the ledger drained, wait and then go out once; together they fail on the code before this revision, and targeted mutants of the abort, batch-snapshot, attempt and finished-reader rules are each killed.

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
  It holds the same order, but each reader would decide from its own checkpoint notifications when a child may start, while the coordinator already sees every finish and every completed checkpoint in one place.
  The reader does keep a finished split until the coordinator acknowledges it, but only so that a restore cannot lose the finish.
- **Keep the reader's per-record progress events and roll the frontier back after a region failover.**
  Readers had already emitted the higher frontier, so re-read records would arrive behind it unless every rolled-back split were also pinned to its restored subtask; frontier and pinning bookkeeping cost more than following committed positions, and one event per record is a coordinator RPC per change.
- **Escalate every region failover to a global failover.**
  It restores the coordinator with the readers, but it consumes two restart attempts per failure and gives up region isolation for a job whose regions are independent.
- **Restart every running ledger entry from the coordinator's position after a restore.**
  It would recover a lost finish without a reader-side record, but it re-reads every partition's tail since the coordinator's last progress event and makes the coordinator's per-record progress a correctness input; a finish the reader kept is reported once and re-reads nothing.
- **Fail a job without checkpointing, as the Pub/Sub source's first-checkpoint detector does.**
  A Spanner job without checkpointing loses nothing by starting children at once; it loses only the cross-partition order, so a warning and a fallback are proportionate where a Pub/Sub job would stall.
- **A gauge of held children** (#2257). A held child stays `CREATED`, outside the unassigned
  gauges, and `changeStreamFinishedParentProofs` stays raised while it waits, as it does while a
  merge child waits for a parent still reading. As for the Bigtable source (ADR-0097), a hold that
  lasts most often means checkpoints are not completing, and a dedicated gauge was judged not
  worth its metric name and reference row.
- **Send what checkpoint N released from the coordinator's snapshot of checkpoint N+1** ([#2404](https://github.com/flink-gcp/flink-connector-gcp/issues/2404)).
  A split assigned inside that snapshot reaches its reader before the barrier, but `SourceCoordinator` calls `context.onCheckpoint` before the enumerator's snapshot, so Flink's assignment tracker counts the split toward the following checkpoint, and a region failover to N+1 could return a split the restored reader also holds.
  It would also delay the watermark and the children by a checkpoint interval, or indefinitely when no later checkpoint comes.
- **Send only in reply to a reader event** ([#2404](https://github.com/flink-gcp/flink-connector-gcp/issues/2404)).
  A watermark or a released child goes to readers other than the sender, which may have finished, and every reader can be idle with nothing more to send.
- **Delay the sends with a timer** ([#2404](https://github.com/flink-gcp/flink-connector-gcp/issues/2404)).
  A timer moves the race rather than removing it: a task that is slow to finish meets it again.
- **Make `IMMEDIATE` the default.**
  The reordering is silent, and an upsert sink then keeps a stale row; a delay of one checkpoint interval per partition split or merge is the cheaper default.
- **Accept unknown partition modes as immutable.**
  A newly added mode can introduce record types that change the lifecycle protocol, so consuming it without an explicit implementation would be partial support.

## Consequences

- Checkpoint and coordinator lineage state scale with unfinished topology and unresolved parent proofs rather than job lifetime; the live ledger also holds the partitions that finished since the last completed checkpoint.
- The source watermark trails the newest records by about one checkpoint interval plus that checkpoint's duration, and holds before the first checkpoint completes; a job without checkpointing sees it follow current positions once `firstCheckpointTimeout` has passed.
- Each reader sends one small report per completed checkpoint, plus at most one a second until the first completes, instead of one coordinator event per record.
- The coordinator checkpoints the positions the previous completed checkpoint recorded, so a restore within about one checkpoint interval of the stream's retention can be refused as expired, or, with `resumeFallback`, discard a ledger its readers could still resume.
- A running child may outlive its parent's finish in the ledger: a region failover reads the parent again while another reader keeps reading the child, so restore validation accepts a running partition with an unfinished parent.
- By default each partition split or merge delays its children until the next checkpoint completes, up to one checkpoint interval plus that checkpoint's duration, and the source watermark holds at the children's start meanwhile; once a checkpoint has completed, the children do not start while later checkpoints keep failing.
- The order holds between failures and for operators that handle their input before the barrier; an unaligned checkpoint, an operator that stores pending elements, and a failover's replay are outside it.
- Each reader sends one small report per coordinator snapshot ([#2404](https://github.com/flink-gcp/flink-connector-gcp/issues/2404)). What a checkpoint releases goes to a reader once it has polled after the coordinator's latest snapshot, which normally happens while the checkpoint completes, so the hand-off and the watermark keep their delay. What waits goes out when a report from that reader names the gating snapshot or a later one, or when aborts lower the gate to a snapshot the reader has reported or remove it, so a reader that is not polled, under backpressure or while watermark alignment holds it, waits longer, and one whose reports keep naming an older snapshot, under sustained backpressure with frequent checkpoints for instance, can keep waiting.
- `changeStreamPartitionLedgerEntries` and `changeStreamFinishedParentProofs` expose those two bounded structures without partition-token labels.
- Version-1 and version-2 restore pays one validation and compaction pass before subsequent checkpoints use version 4.
- The public source and reader can be added independently in #536 because the coordinator-facing split and event contracts are now fixed.
- Real-service partition, checkpoint, savepoint, retention, and fallback behavior is covered by the gated acceptance in #535.
