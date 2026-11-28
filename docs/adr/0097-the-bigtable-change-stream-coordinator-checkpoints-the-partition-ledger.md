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

# ADR-0097: The Bigtable Change Streams coordinator checkpoints the partition ledger

- Status: Accepted
- Date: 2026-08-11 (revised 2026-08-14, 2026-08-17, 2026-11-21, 2026-11-28)
- Issues: [#35](https://github.com/flink-gcp/flink-connector-gcp/issues/35),
  [#510](https://github.com/flink-gcp/flink-connector-gcp/issues/510),
  [#532](https://github.com/flink-gcp/flink-connector-gcp/issues/532),
  [#533](https://github.com/flink-gcp/flink-connector-gcp/issues/533),
  [#586](https://github.com/flink-gcp/flink-connector-gcp/issues/586),
  [#951](https://github.com/flink-gcp/flink-connector-gcp/issues/951),
  [#2206](https://github.com/flink-gcp/flink-connector-gcp/issues/2206),
  [#2194](https://github.com/flink-gcp/flink-connector-gcp/issues/2194),
  [#2257](https://github.com/flink-gcp/flink-connector-gcp/issues/2257),
  [#2404](https://github.com/flink-gcp/flink-connector-gcp/issues/2404)
- Modules: bigtable (`source.changestream`)
- Current behavior: `source.changestream.BigtableChangeStreamSplitEnumerator`

## Context

Bigtable Change Streams starts with `GenerateInitialChangeStreamPartitions`, but those partitions
do not stay fixed. A stream ends with `CloseStream` continuation tokens for successor partitions.
A split gives each child its own token; a merge gives the same target to several parents, and the
target cannot start until its parent-token ranges cover it. Rapid split/merge sequences can also
lose a merge close message, so the coordinator eventually needs a keyspace reconciler.

Apache Beam keeps this protocol in an external metadata table because its splittable functions have
no single coordinator. FLIP-27 already has one. Adding Beam's metadata table here would create a
second mutable recovery system beside Flink checkpoints and would make a new external resource part
of every job's correctness.

The java-bigtable methods, query and mutation records, and mutation entry types used by this
protocol carry `@InternalApi("Intended for use by the BigtableIO in apache/beam only.")` in 2.80.0;
the aggregate `Value` model is `@BetaApi`. They are nevertheless the client library's only typed
entry points to these RPCs. The acceptance is the same shape as ADR-0041: the annotations and
surface are checked facts, reread on a client upgrade, rather than an accidental dependency.

## Decision

The `SplitEnumerator` owns the partition topology. Its checkpoint contains unassigned and assigned
splits, pending merge targets, the reconciler's missing-partition timers, a bounded run's completed
ranges, the resolved fresh-start time and the next split id. The reader added
by #511 will checkpoint the matching exact continuation tokens and low watermark. No external metadata
table or change-stream name exists; cross-job handoff is a Flink savepoint.

A successor is parked until the ranges named by all tokens received for it tile the target range.
Only then is one split created with the full token list. Split ids come from a checkpointed monotonic
counter rather than from ranges: a range can disappear and reappear after another topology change,
and reusing its old id would confuse a late completion with the new split.

Pending merge tokens accumulate in a coordinator-thread range index.
Each arrival reevaluates at most three affected adjacency relationships; it does not copy or sort every token already received.
The index freezes to the immutable `PendingMerge` model in range order at completion, checkpoint, and reconciliation boundaries, preserving the connector-owned checkpoint format and deterministic restore behavior.

The keyspace a reconciliation scan is measured against is what the run still owes, not what it
still holds. A bounded run ends a partition by closing it without a successor at the end time,
which retires that range from the live ledger while the service goes on reporting it for as long as
the table exists. The coordinator therefore records those completed ranges and counts them as
covered, and checkpoints them with the rest of the ledger so a restore keeps the same account. A
continuous run records nothing: with no end time to close a stream at, a partition that disappears
without successors is a loss rather than a completion, and must still be restarted.

A close is final only once a completed checkpoint covers it (#2206), as a Debezium Spanner
partition is `FINISHED` only once its offsets are committed. Region failover, Flink's default,
restarts the failed tasks from the last completed checkpoint but keeps the coordinator, so the
ledger must not have moved past what that checkpoint restores. A closed partition therefore stays
in a pending-close index until a checkpoint whose coordinator snapshot ran after the transition
completes, and the snapshot leaves it out, because the checkpoint it belongs to covers it. A split
Flink returns through `addSplitsBack` restarts from the returned split, which is where Flink
assigned it, and one whose close is pending is read again; a reader names the unclosed splits it
restored in a `PartitionsRestoredEvent` from `start()`, and a pending close among them is taken
back. The successors of such a partition already exist, so its id is recorded, and checkpointed,
as a re-read until its next close is final, and that close creates no successor. A successor that
another reader already reads keeps running beside the re-read partition.

Every transition is acknowledged, and applying one is idempotent (#2206). A reader keeps a closed
split, with the successors its `CloseStream` named, in its checkpointed state until the
coordinator's `PartitionTransitionAckEvent` arrives, and a restored reader sends the transition
again. Flink restores a reader-to-coordinator event sent between the coordinator's snapshot and
the reader's barrier into neither side, and holds coordinator-to-reader events back over the same
window, so the closed split is in the reader's snapshot of exactly the checkpoints whose
coordinator snapshot missed its transition. A transition for a split the ledger does not hold as
assigned is acknowledged and otherwise ignored: its successors already exist. This is Flink CDC's
`finishedUnackedSplits`.

Readers report positions per completed checkpoint rather than per heartbeat, the shape of Flink
CDC's `IncrementalSourceReaderWithCommit`: a reader keeps the positions each snapshot recorded and
sends the latest one at or before a completed checkpoint from `notifyCheckpointComplete`. Until
one completes, it also sends its current positions at most once a second. An assigned split takes
every report, older or newer. The positions serve the reconciler, not a restart or a restore
check: an assigned split is in the restored state of the reader that holds it, and that reader
checks it against retention, a closed one included, while the coordinator's copy carries only the
previous completed checkpoint's position and would refuse a restore whose readers are still within
retention. A restored closed split whose position has expired is read again from `resumeFallback`
rather than reported, because its successors' tokens are as expired as its own position. A job
without checkpointing never completes a checkpoint, so its pending closes would pile up: once a
close has waited `firstCheckpointTimeout` with none completed, every close is final at once, with
a warning, until a checkpoint completes. The wait counts from the oldest pending close rather than
from the start, so a job whose first checkpoint is slow but whose partitions do not close in the
meantime is not warned. A savepoint does not end that wait, because Flink reports completion only
for a checkpoint or a stopping savepoint.

Since #2194 a closed partition's successors also wait for that completed checkpoint, as the Spanner
source's children do (ADR-0099). A partition's last records can still be in a network buffer or a
sink writer's batch when its reader reports the close; a successor read by another subtask can
overtake them, so one row key's changes reach a keyed operator out of commit order. The reader
emits the partition's last record before it reports the close, so that record precedes the
reader's barrier for the covering checkpoint. The coordinator still creates the successors when the
transition arrives and holds back only their assignment: a snapshot that covers the close must
carry them, because the reader drops the closed split once the transition is acknowledged, and a
restore from that snapshot would otherwise find the range nowhere. The hold is therefore runtime
state only, and a restored checkpoint, having completed, releases everything it records.

A held partition blocks every split created after it whose range overlaps its own, rather than a
list of its children. Split ids come from a monotonic counter, and every split created after a
partition closed over its keys descends from it or is a reconciler recovery of part of its range,
a merge child or a recovery from only some of a merge's tokens included. The direction matters: a
partition read again after a rollback never waits for its own successors, so two holds cannot wait
for each other. A close taken back by a region failover keeps its hold until the close of the
re-read is final. Whether one partition blocks another never changes, so the coordinator remembers,
for each waiting split, the held partition that blocked it, and rescans the holds only once that
one is released. `partitionHandoff(PartitionHandoff.IMMEDIATE)` turns the hold off. Nothing is
held at source parallelism 1, or after a partition whose reader attempt emitted no output record,
which the transition reports. `firstCheckpointTimeout` bounds the hold too: it measures from the
oldest pending close or the oldest hold, whichever is older, and a hold keeps its age when its
close is taken back. When it fires, every hold ends, every pending close is final, and a partition
being read again keeps its re-read mark, so its next close still creates nothing.

The coordinator acknowledges a transition only to the reader attempt that sent it: one that has
failed since has no gateway, and its restored attempt reports the transition again, and a later
attempt of the same subtask may close the same split again and would drop that close from its
checkpoint on a stale acknowledgement. Flink names the sending attempt only under concurrent
execution attempts, so the coordinator counts reader registrations per subtask and acknowledges a
transition deferred during its initialization only if no attempt has registered since. A reader
releases the split output Flink creates for each restored closed split, which nothing emits to and
which would otherwise hold the source watermark back.

No coordinator event reaches a reader that may have finished (#2404), the rule ADR-0099 records
for the Spanner source and its Flink evidence. A stop-with-savepoint without drain stops a source
before its task snapshots the savepoint, and the task finishes once the savepoint completes while
the JobManager can still see it running, so a split assignment, an acknowledgement or an end signal
that reaches it then fails the task and the stop. Completing a checkpoint makes closes final and
releases successors, which this coordinator would assign at that moment. Each reader therefore
reports, from its first poll after a snapshot, which checkpoint it snapshotted, and a stopped
source is never polled again. After the coordinator's own snapshot it sends a reader nothing until
that reader reports that checkpoint or a later one; an aborted snapshot leaves the latest earlier
one gating, and a newly registered reader is reached at once, as ADR-0099 explains. Free slots wait
for their own reader's report. Beyond the free slots, the hand-out still waits until every source
subtask has registered and reported capacity, and then gives each split to the least-loaded reader
that has polled since the gating snapshot, so one reader that has not cannot hold the excess back. A
bounded run's end signal waits for each reader's report as well, a reader that has read all its
input reports nothing, and a batch recovery snapshot gates nothing. The gate covers only snapshots
this coordinator took; a `HybridSource` switch to this source is outside it (#2406).

Initial partitions are generated only for a fresh start. A restored enumerator validates the
checkpointed low watermarks no reader holds (unassigned splits, pending merges and missing
partitions) through ADR-0094, and each reader validates the assigned splits it restores, closed
ones included; the enumerator never calls the initial-partition RPC on a restore. The source
requires an application profile id because Change Streams accepts only single-cluster routing. The
coordinator preflights the routing policy when the principal can read it; a data-plane-only
principal is allowed through and the reader translates the service rejection if the profile is
multi-cluster.

The reader reconstructs every SDK-bound partition as an explicit closed-start/open-end range before
building either a `ReadChangeStream` request or a continuation token. Empty boundary keys mean
negative or positive infinity in this API, but their protobuf oneof cases must still be set. The
connector's general-purpose range copy normalizes an empty key to an unbounded model value, so it
remains safe for internal range algebra and is deliberately not the SDK representation.

The reader evaluates output filters against each complete SDK mutation and converts only retained
entries into the connector-owned immutable `BigtableChangeStreamMutation` before application
deserialization.
The public model carries every mutation-level field and an ordered typed representation of the
five entry kinds and three aggregate value kinds exposed by java-bigtable 2.80.0.
Unknown SDK entry or value subtypes fail before any partial mutation reaches the application.
The connector-owned model has tagged Flink serialization and type information, so public output
does not depend on the SDK's Apache-Beam-only `@InternalApi` record model or reflective Kryo.

Family and qualifier regular expressions select SDK entries with full-match semantics.
Qualified-column expressions match `family:` plus canonical padded RFC 4648 standard Base64, and
family deletes use only the family filter because they have no qualifier.
Filtering runs after the service has delivered the complete atomic mutation and does not reduce
RPC traffic.
An empty projection is delivered by default; the explicit skip flag bypasses deserialization while
still advancing the continuation token and estimated low watermark.
When no entry filter is configured, the reader bypasses filter evaluation and its projection result
allocation, but the SDK-to-public-model conversion remains the cost of removing the unstable SDK
type from the public SPI.
When an explicit skip removes every entry, the reader advances directly from SDK metadata without
materializing the public mutation or any public entry/value/range object.

## Evidence

- In java-bigtable 2.80.0, `ReadChangeStreamQuery.continuationTokens` accepts a list and is mutually
  exclusive with `startTime`, which permits a merged partition to resume from all parent tokens.
- `CloseStream` carries paired successor ranges and tokens; `ChangeStreamContinuationToken`
  exposes the token's partition range, which is what proves that a merge target is complete.
- `Table.getChangeStreamRetention()` exposes the configured retention to the connector, while a
  fresh `latest()` avoids that admin read through ADR-0094's lazy lookup. Since
  [#1622](https://github.com/flink-gcp/flink-connector-gcp/issues/1622) the coordinator reads the
  same value from the protobuf `Table`'s `change_stream_config` through `getBaseClient()` (the
  `BigtableTableAdminClientV2` it returned is opened directly since
  [#2322](https://github.com/flink-gcp/flink-connector-gcp/issues/2322)), so a family value type
  the client's model cannot parse no longer fails the read.
- The first real-service run on 2026-08-12 rejected a partition whose boundary oneofs had been
  cleared with `INVALID_ARGUMENT: partition.row_range must be the form of [start_key, end_key)`.
  java-bigtable 2.80.0 and Apache Beam both preserve `ByteStringRange.create(empty, empty)`, which
  sets the required closed/open cases while using empty values for the unbounded keyspace.
- The next real-service run reached the mutation stream and exposed the same normalized range at
  `ChangeStreamContinuationToken.create` as `IllegalStateException: Start is unbounded`; all
  connector-created SDK ranges therefore share the same boundary conversion.
- After that fix, the live stream emitted a service mutation but default Kryo copying failed with
  `UnsupportedOperationException` while rebuilding its immutable `entries`; #586 replaces that
  public SDK output with the connector-owned tagged representation.
- The following live run completed a checkpoint and entered the fixture's controlled failure. Its
  generic `RichMapFunction` then erased the schema's output type before the collect network edge;
  the gated fixture now declares that output type explicitly so recovery uses the same serializer.
- The final production-service run on 2026-08-12 used instance
  `flink-it-1786493698-968f3051`, observed all 100 seeded rows, completed a checkpoint, triggered the
  one controlled failure, recovered without loss, and completed at the bounded end time in 132.3
  seconds. The instance returned `NOT_FOUND` immediately after teardown.
- `CloseStream.create` in java-bigtable 2.81.0 enforces the biconditional the completed-range
  record depends on: an OK status must carry no continuation tokens, and a non-OK status must
  carry at least one. A transition with no successors is therefore an OK close rather than a split
  or a merge, checked by the client library rather than inferred from the protocol description.
- The gated bounded run on 2026-08-17 never completed, restarting all four partitions on every
  20-minute cycle for over 40 minutes. Its first observation of them as missing was 21:17:28,
  which is the run's own end time, and the gaps rendered as the four whole service partitions —
  only possible against an empty ledger, that is, after every split had finished. A non-empty
  missing ledger blocks the bounded completion signal, and that signal is the only thing that stops
  the scans, so the two guards deadlocked and each cycle re-emitted the mutations it had already
  delivered.
- On `main` at `dc7d8acb`, a `MiniCluster` job keyed behind the source at parallelism 2 failed
  once after checkpoint 1, with `P0` splitting into `C1` (5 mutations, then its end) and `C2` (30
  mutations) after it. A downstream operator counted a record only once a checkpoint covering it
  completed. Every one of the 35 records was lost: both successors had closed by the time Flink
  returned them, and the coordinator dropped a returned split it no longer held as assigned. The control,
  with a second checkpoint after `C1` closed, lost none. After #2206 both variants deliver all 35,
  and `P0` is read again without creating its successors twice
  (`BigtableChangeStreamSourceRegionFailoverITCase`).
- In the same harness, a second source's coordinator held the barrier back while `P0` closed
  between the Bigtable coordinator's snapshot and the reader's barrier. On `main` the restored job
  read nothing for 60 seconds while it kept running: the restored ledger held `P0` as assigned and
  no reader held it. After #2206 the restored reader reports the transition again and `C1` is read
  (`BigtableChangeStreamSourceRestoreWindowITCase`).
- #2194 asked for a service measurement before adding the hold, because the emulator does not
  implement Change Streams. The maintainer decided on 2026-11-20 that Bigtable gets the hold Spanner
  got in #2193, and on 2026-11-21 that the decision stands in for the measurement: the coordinator
  has the shape ADR-0099 measured on the Spanner emulator, where 3 of 6 runs without the hold
  delivered ancestor-partition inversions to a keyed operator and none of 10 with it did. No
  Bigtable measurement was taken.
- `BigtableChangeStreamSourceHandOffITCase` runs a scripted split through Flink's own source
  coordinator at parallelism 2: `P0` emits `P0#1` and splits into `C1` and `C2`, which emit `C1#2`
  and `C2#3`. An operator in its own task behind the source keeps `P0#1` until a successor's record
  reaches the keyed operator, or for five seconds, standing in for a parent's last record still in
  a network buffer. Under `IMMEDIATE` without checkpointing a successor's record reaches the keyed
  operator first; under `AFTER_CHECKPOINT` neither successor opens before `P0#1` arrives; without
  checkpointing neither a successor record nor a successor open, timed by the stream itself, comes
  within 900 ms of the parent under a one-second `firstCheckpointTimeout`, and the warning naming
  the successors is logged. The open is timed because a successor on the parent's own reader would
  queue behind the delayed record and pass an arrival check even if it opened at once. With a
  100 ms checkpoint interval the `IMMEDIATE` case did not always reorder, and the default case
  passed with the hold disabled: an aligned barrier in flight blocked the successor's channel at
  the keyed operator until `P0#1` arrived. The test therefore runs `IMMEDIATE` without
  checkpointing, uses a 2-second interval, and judges the default by when the successors open.
- The gated real-GCP Bigtable suite, run on 2026-11-21 for an earlier implementation of #2194,
  passed; one of its change-stream jobs is pinned to parallelism 2 with checkpointing, two to
  parallelism 1, and one inherits the environment's parallelism. Its logs record only the initial partitions (1 or 4); the coordinator does not log
  a split or merge, and nothing in the suite forces one, so the suite is not evidence about the hold
  itself.
- A coordinator unit measurement on 2026-11-21 closed every one of n partitions at parallelism 64
  with output, so n holds and n waiting successors coexisted. Rescanning every hold for every
  unassigned split on each event took 0.7, 5.2 and 33.7 seconds of coordinator time in all for n
  = 500, 1000 and 2000, growing with n cubed. Remembering, for each waiting split, the hold that
  last blocked it took 0.07, 0.06 and 0.28 seconds.
- Unit tests round-trip pending merge state, prove one token cannot release a two-parent merge,
  prove restore does not generate new initial partitions, prove many parents require only bounded neighboring coverage checks, and prove expired coordinator-held state fails unless fallback was opted in.
- Deterministic tests cover all 2.80.0 entry and value kinds, full-match family and qualified-column
  filtering, default-empty and explicit-skip dispositions, source serialization, metrics, and
  progress through filtered mutations. No new service behavior is assumed, so #586 requires no
  additional real-GCP acceptance run.

- The stop-with-savepoint failure (#2404) was reproduced only for the Spanner source, as ADR-0099
  records; the Bigtable coordinator assigned released successors from `notifyCheckpointComplete`
  in the same way. Unit tests hold that a completed checkpoint and a capacity report send nothing to
  a reader that has not reported a poll, that a transition acknowledgement waits for a poll after
  the gating snapshot and then goes out once, that an aborted snapshot leaves the earlier one gating
  until every one is aborted and then sends at once to a reader that reported it, that a batch
  recovery snapshot gates nothing, that a new attempt is reached at once and inherits no
  acknowledgement, that the hand-out beyond free slots goes to the least-loaded reader that has
  polled, and that a bounded
  run's end signal waits for each reader and goes out once; together they fail on the code before
  the revision, and targeted mutants of the hand-out, batch-snapshot, attempt and finished-reader
  rules are each killed.

## Alternatives declined

- **Beam's metadata table.** Flink already checkpoints a coordinator and reader state; the table
  would duplicate it and add lifecycle, permissions, locks and cleanup to every job.
- **Start merged partitions on the first token.** The uncovered parent range would be silently
  dropped, which is worse than holding the merge.
- **Derive split ids from ranges.** The same range can recur after a split/merge cycle, so the id is
  not an identity.
- **Require admin metadata permission.** The routing preflight is diagnostic rather than part of
  the data protocol; denying a principal that can read changes would turn an improved message into
  a new permission requirement.
- **Keep the raw SDK mutation as a public escape hatch.** That would preserve the unstable type
  dependency the connector-owned model removes and would make filter output semantics differ by
  deserializer choice.
- **Push the filters into the service request.** `ReadChangeStream` returns complete logical
  mutations and exposes no equivalent family or qualifier predicate in the pinned client.
- **A close final at once, with a returned closed split read again only when Flink returns it.**
  A partition assigned before the restored checkpoint is restored into its reader rather than
  returned, so the coordinator would not hear that it is being read again; nor could it tell a
  re-read close from a duplicate one, and a global restore would then create its successors
  twice. The pending close, the restored-splits event and the checkpointed re-read mark are what
  make the second close recognisable.
- **Recognise a repeated successor by its token.** A successor is a range, and the same range can
  recur after another split or merge with a different token; once the successor reads, the
  ledger no longer holds the token it started from. The re-read mark on the parent is the
  identity that survives.
- **Measure the reordering on the service before holding** (#2194's first plan). Bigtable splits
  and merges partitions on its own schedule and no API forces a split, so a measurement would wait
  an unbounded time on a new instance, and a run that showed no inversion would prove nothing,
  because the race is in Flink's network path rather than in the service.
- **Hold the close in the reader until its own checkpoint completes.** The reason ADR-0099 records
  for Spanner applies: it would need closed-but-unreported splits in the reader's checkpoint, while
  the coordinator already sees every close and hears of completed checkpoints, a later completion
  covering one whose notification was lost.
- **Create the successors only when the close becomes final**, and drop them when the close is
  taken back. The covering snapshot would then hold neither the closed split nor its successors,
  and the reader has dropped the split on the acknowledgement, so a restore from it would lose the
  range until the reconciler's 20-minute tokenless restart.
- **Hold a list of children per parent.** Each merge accumulator and each reconciler recovery
  would have to inherit its parents' holds, and a recovery can use only some of a merge's tokens or
  none, so a transfer misses cases a range overlap covers by construction.
- **Hold by range overlap alone.** A restore can leave a partition and its successor both
  unassigned; if both close with output and are taken back before a checkpoint, each overlaps the
  other's held range and neither is assigned again. Ordering by creation breaks that cycle.
- **A separate timeout check for holds**, as the Spanner source keeps one for children and one
  for its watermark. Every hold starts with a pending close, so one periodic check covers both; it
  reads the oldest pending close and the oldest hold, because a close taken back and made again
  moves its pending close to the end with a new time while the hold keeps its own.
- **A gauge of held successors** (#2257). A held successor already counts in `unassignedSplits`
  and `unassignedChangeStreamPartitionLagMillis`, and a hold that lasts most often means checkpoints
  are not completing, which Flink's checkpoint metrics show. A dedicated gauge was judged not worth
  its metric name, reference row and Spanner counterpart; it is worth reconsidering if users report
  unassigned lag they cannot attribute.
- **Keep one progress event per heartbeat.** It sends every partition's position to the
  JobManager every five seconds and lets the ledger run ahead of the restored checkpoint, while
  nothing the coordinator decides needs a position newer than the last completed checkpoint.
- **Stop reconciling once a bounded run passes its end time**, or never reconcile a bounded run at
  all. Either removes the loop without a checkpoint field, and both give up the recovery the
  reconciler exists for at exactly the point a bounded run cannot afford it: a merge close lost
  near the end time would leave a pending merge that nothing completes, and a pending merge blocks
  the completion signal too. That trades a hang that restarts partitions for one that does
  nothing.

## Consequences

Enumerator state intentionally carries an assigned ledger, unlike the bounded scan source in
ADR-0080: a continuous source must know which ranges are live before it can find a missing one.
The final implementation phase adds that reconciler and checkpoints its timers in the same state.
The emulator covers none of these RPCs, so deterministic tests use hand-written seams and the final
protocol proof runs against gated real Bigtable.
Output filters do not alter the partition ledger or checkpoint format.
Operators can distinguish removed entries from explicitly skipped empty projections through the
two Change Streams reader counters.
The completed-range list grows only under an end time and only by the partitions that run reads,
so it is bounded by the run's own topology rather than by its duration; a continuous job never
adds to it. Reading it costs a checkpoint format version, and a bounded run restored from an
older checkpoint comes back without it, which puts that run back where the loop found it for one
grace period rather than corrupting it.
A tokenless restart's WARN names the four collections its gap was measured against, because the
counter says a range was uncovered and only the ledger says by what.

Since #2206 a region failover reads a partition again from where it was assigned, so records after
the restored checkpoint can be delivered twice, as at-least-once already allowed. A successor
already running beside a re-read partition, because the hand-off was immediate, the partition
emitted nothing, or the timeout had fired, keeps running, so their relative order is not restored.
The coordinator's positions are those of the previous completed checkpoint. A job without
checkpointing keeps each close pending, and its successors waiting, for up to
`firstCheckpointTimeout` (10 minutes by default); zero keeps them so for good, one small entry per
closed partition.

Since #2194 each partition split or merge delays its successors by up to one checkpoint interval
plus that checkpoint's duration, and longer while checkpoints fail; a waiting successor counts as
unassigned. With a checkpoint interval above the reconciler's two-minute token grace, a merge whose
other parent has not closed yet is recovered from the closed parent's token, so
`changeStreamPartitionsReconciled` can count in a healthy job. Order holds only under aligned
checkpoints, for operators that finish their input before the barrier, and between failures; the
DataStream page states these limits. The split
checkpoint format is version 2 and the enumerator state version 4; older versions still restore.

Since #2404 each reader sends one small report per coordinator snapshot, and what a checkpoint
releases goes to a reader once it has polled after that checkpoint's snapshot, which normally
happens while the checkpoint completes. A reader that is not polled, under backpressure for
instance, or whose poll reports keep reaching the coordinator after it has taken a newer
snapshot, waits longer for its successors, and the excess beyond free slots goes to the readers
that have reported meanwhile, so such a reader may get none of it.
