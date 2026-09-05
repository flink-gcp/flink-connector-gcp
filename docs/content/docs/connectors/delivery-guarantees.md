---
title: Delivery Guarantees
type: docs
weight: 5
---

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

# Delivery Guarantees

This page compares what a completed Flink checkpoint means for each sink and what can happen when
Flink restores an earlier checkpoint.

The short version is that flushing before a checkpoint makes data durable, but does not by itself
prevent a restored job from writing the same record again.
Only BigQuery currently has checkpoint-coordinated exactly-once write methods.
Some other sinks can make a replay harmless when the record supplies a stable identity, but that is
not the same contract as a general-purpose exactly-once sink.

## Terms used here

These terms describe different properties and should not be used interchangeably.

**Checkpoint-durable** means that a completed checkpoint covers every preceding record the sink did
not deliberately skip or route to a dropping failure policy.
The sink has waited until the destination acknowledged those records before allowing the checkpoint
barrier to pass.

**At-least-once** means that recovery does not lose a checkpoint-covered record, but may apply a
record again when the source and sink restore an earlier checkpoint.

**Idempotent or effectively-once effect** means that the repeated request is still possible, but the
chosen record identity and destination operation make the observable result the same as one write.
This property belongs to that operation and schema, not automatically to every use of the connector.

**Exactly-once service write** means that the sink protocol prevents one logical input record from
creating more than one visible destination effect within its documented scope and recovery window.

**End-to-end exactly-once** also includes the source and everything that consumes the destination.
It requires checkpointing, an exactly-once-capable source, preservation of checkpoint state across
deployments, and failure policies that do not drop records.
A sink cannot provide this property on its own.

Two-phase commit is one way to obtain an exactly-once service write, not the definition of it.
A Flink committer only helps when the destination exposes a transaction, an invisible prepared
write, or an idempotent commit token that survives recovery.
Adding a committer around an eager non-transactional API does not undo a write made before failure.

## Current sink matrix

All stateless sinks below call `flush()` at a checkpoint and wait for outstanding service requests.
Checkpointing must be enabled in a streaming job for that durability boundary to run.

| Connector and method | Current checkpoint protocol | Effect of replay | Current guarantee |
|---|---|---|---|
| [BigQuery]({{< relref "docs/connectors/datastream/bigquery" >}}) `STORAGE_API_EXACTLY_ONCE` | Writer state records BUFFERED streams and explicit append offsets; a committer calls `FlushRows` after checkpoint completion | An uncommitted tail stays invisible; restored committables can be flushed again idempotently | Exactly-once service writes for fixed or dynamic destinations, subject to the documented stream lifetime and preserved Flink state |
| [BigQuery]({{< relref "docs/connectors/datastream/bigquery" >}}) `FILE_LOADS` | Checkpointed staged objects become deterministic load and copy jobs through a committer; batch `WRITE_TRUNCATE_DATA` overflow adds a terminal query job | Restored jobs reuse deterministic object and job identities | Exactly-once service writes for batch and checkpoint-triggered streaming loads |
| [BigQuery]({{< relref "docs/connectors/datastream/bigquery" >}}) `STORAGE_API_AT_LEAST_ONCE` | Stateless writer flushes the default stream before the barrier | The same row may be appended again | At-least-once |
| [Pub/Sub]({{< relref "docs/connectors/datastream/pubsub" >}}) sink | Stateless writer flushes SDK publishers and waits for publish acknowledgements | A replay is a new publish with a new service-assigned message ID | At-least-once; no publisher-side idempotent mode |
| [Cloud Tasks]({{< relref "docs/connectors/datastream/cloudtasks" >}}) sink | Stateless writer waits for every `CreateTask` request | Unnamed tasks can be created again; named tasks return `ALREADY_EXISTS` while the service remembers the name | At-least-once by default; bounded effectively-once **task creation** with `taskIdExtractor(...)` or Table API `task-id` metadata |
| [Bigtable]({{< relref "docs/connectors/datastream/bigtable" >}}) sink | Stateless writer sends buffered `MutateRows` entries and waits for every entry | A regenerated `setCell` timestamp can add a version; a stable timestamp targets the same version. Aggregate Sum inputs or states can contribute again even at the same timestamp | At-least-once; replay safety depends on the mutation shape |
| [Bigtable]({{< relref "docs/connectors/datastream/bigtable" >}}#single-row-request-writes) single-row request runtime | Sink surface: stateless writer waits for every `CheckAndMutateRow` or `ReadModifyWriteRow` request. Async surface: Flink's async operator checkpoints un-emitted inputs and replays them | A replayed conditional write re-evaluates its predicate against the state the first attempt left; a replayed read-modify-write applies its increment or append again | At-least-once; neither RPC is idempotent, and the runtime retries neither. Conditional sink and async entry points are available; read-modify-write entry points remain [#1180]({{< param BookRepo >}}/issues/1180) |
| [Spanner]({{< relref "docs/connectors/datastream/spanner" >}}) sink | Stateless writer consumes `BatchWrite` responses before the barrier | Spanner documents no replay protection; the selected mutation operation may nevertheless be idempotent | At-least-once; `insertOrUpdate`, `replace`, `update`, and delete effects can be idempotent within their operation constraints |

Follow the connector-specific delivery section for failure-handler behavior, state-loss hazards, and
operation-specific caveats.

## Write and key-collision semantics

Flink's changelog contract and the destination's key-collision behavior are separate choices.
Declaring a SQL `PRIMARY KEY` tells the planner how rows relate; it does not uniformly ask a
destination to reject an existing key.
Likewise, an insert-only changelog says which row kinds reach a sink, not whether the service
implements insert-if-absent.

| Connector API | How the identity and write shape are selected | Effect of submitting the identity again | Replay boundary |
|---|---|---|---|
| Spanner Table API | A declared `PRIMARY KEY` selects `insertOrUpdate` and key deletes; without one the sink accepts inserts only and uses `insert` | Replaying one upsert or delete repeats its effect idempotently; `insert` can return `ALREADY_EXISTS` | At-least-once submission; `BatchWrite` does not preserve the order of successive same-key mutation groups |
| Spanner DataStream API | The serializer chooses the key and `Mutation` operation | Replaying one `insertOrUpdate`, `replace`, `update`, or delete can be idempotent within its operation constraints; `insert` can return `ALREADY_EXISTS` | At-least-once submission; replay safety belongs to each mutation, and `BatchWrite` does not preserve same-key group order |
| Bigtable Table API, default `upsert` | The one atomic column is always the row key; a declared `PRIMARY KEY` improves planner handling, while `sink.insert-only-input-mode` changes only the accepted changelog | The same row key is physically upserted; a stable explicit cell timestamp targets the same version, while an omitted timestamp uses the writer's wall clock and a Flink replay can create another version | At-least-once submission; this mode performs no atomic existence check |
| Bigtable Table API, `insert-if-absent` | The ordinary row-key/family schema with `sink.write-mode = insert-if-absent`; only INSERT input is accepted | Any stored cell, including undeclared families, selects an empty branch; an absent row receives the input cells atomically | At-least-once submission; an initial insert followed by replay may be ignored or fail according to the empty-branch policy |
| Bigtable DataStream API, bulk sink | The serializer chooses the `RowMutationEntry` row key, mutations, qualifiers, and cell timestamps | Replaying `setCell` with a stable explicit timestamp targets the same version; a regenerated timestamp can create another version. Aggregate Sum inputs or states can contribute again even at the same timestamp | At-least-once submission; replay safety belongs to the mutation shape |
| Bigtable single-row request runtime | The request names the row key; `CheckAndMutateRow` carries a predicate and an ordered mutation list for each branch, `ReadModifyWriteRow` an ordered list of append and increment rules | Repeating a conditional write re-runs the predicate, so a marker it writes can make the second attempt a no-op; repeating a read-modify-write appends or increments again, with no timestamp to target | At-least-once submission, one attempt per request; a conditional write is the only shape here that can be made replay-safe, and only by what the application puts in the row |
| Cloud Tasks Table API | The sink accepts inserts only; writable `task-id` metadata optionally selects a stable task identity | A remembered ID returns `ALREADY_EXISTS`; the existing task is neither compared nor updated | At-least-once submission; bounded effectively-once task creation when `task-id` is selected |
| Cloud Tasks DataStream API | `taskIdExtractor(...)` optionally selects a stable task identity; otherwise Cloud Tasks assigns one | A remembered extracted ID returns `ALREADY_EXISTS`; an unnamed replay creates another task | At-least-once submission; bounded effectively-once task creation with an extractor |

None of these key choices turns a non-BigQuery connector into a checkpoint-coordinated
exactly-once sink.
They make a particular destination effect replay-safe only within the identity, operation, and
retention constraints in the table.

## BigQuery and the Storage Write API example

The [BigQuery Storage Write API documentation](https://cloud.google.com/bigquery/docs/write-api-streaming)
demonstrates exactly-once writes with an application-created **COMMITTED** stream and explicit
append offsets.
The connector uses the same offset-based replay protection, but not that exact stream lifecycle.

`STORAGE_API_EXACTLY_ONCE` creates one **BUFFERED** stream per active destination in each writer
subtask and reuses it across checkpoints.
Appends stay invisible until a checkpoint completes, when the Flink committer calls `FlushRows` up
to the checkpointed offset.
This visibility boundary is necessary because a COMMITTED-stream append would already be visible if
Flink later rolled back to an earlier checkpoint.

The writer persists each active destination's stream name and next append offset in Flink state.
`prepareCommit()` emits the highest offset that the checkpoint may make visible, and `FlushRows` is
safe to repeat after recovery.
This is a Flink two-phase commit built from BigQuery's buffered-stream protocol, rather than a copy
of the committed-stream sample.

The method still depends on preserved Flink state.
Restarting the job without its state while buffered rows or committables are pending can skip up to
one checkpoint of data, so deployment tooling must use a savepoint or retained checkpoint rather
than a stateless restart.

## Why the other services need different mechanisms

### Pub/Sub

Pub/Sub [exactly-once delivery](https://cloud.google.com/pubsub/docs/exactly-once-delivery) is a
**subscription-side** receive and acknowledgement feature.
It does not deduplicate calls made by a publisher.

The service assigns [`messageId`](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage)
after accepting a publish, and the publisher cannot supply that ID as an idempotency key.
Pub/Sub also exposes no publish transaction that a Flink committer can prepare and commit.
The connector therefore cannot turn a repeated publish into one physical Pub/Sub message.

A producer-assigned event ID in an attribute can let a downstream consumer deduplicate effects.
That is an end-to-end application protocol and the topic may still contain duplicate messages.
A transactional outbox can provide a stronger architecture when the source state and outbox share a
database transaction, but it is not a connector-only Pub/Sub guarantee.

### Cloud Tasks

Cloud Tasks already exposes the useful service primitive: a caller-chosen task name is rejected with
`ALREADY_EXISTS` while that name remains in the service's
[deduplication window](https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create).
The sink's `taskIdExtractor(...)` hashes a stable application key into such a name and treats that
response as success.

This mechanism does not need a Flink committer because eager task creation is idempotent during the
window.
It is deliberately described as bounded effectively-once task creation.
The pinned v2 protocol says a deleted or executed task name remains unavailable for about one hour,
while the current REST reference says up to 24 hours, so applications must design against the
shorter statement.

Cloud Tasks [delivers the task handler at least once](https://cloud.google.com/tasks/docs/dual-overview)
even when task creation was deduplicated.
The handler must therefore be idempotent or maintain its own durable event ledger.
Named task creation also performs an extra lookup and Google documents significantly increased
latency, so its practical recommendation depends on the measured workload.

### Bigtable

The current sink can make an individual `setCell` effect idempotent when the serializer writes the same value with a stable explicit timestamp.
That does not cover a writer-clock timestamp, arbitrary mutation shapes, or two legitimate events
that happen to target the same cell version.

That idempotence is a property of Bigtable's storage model, not an exactly-once sink protocol.
The sink has no committer, no writer state, and no step that makes a replayed record invisible or
rejected; a replayed `setCell` carrying the same row key, family, qualifier, and timestamp
overwrites the same cell version, and the overwrite is what makes the duplicate disappear.
The [Bigtable sink of google/flink-connector-gcp](https://github.com/google/flink-connector-gcp/blob/main/connectors/bigtable/README.md#exactly-once)
calls this same effect "Exactly Once out of the box": its writer flushes at the checkpoint barrier
with no committer, and three of its four built-in serializers stamp each cell with the Flink record
timestamp when the record carries a positive one, so the two connectors offer the same mechanism
under different names.

Aggregate `addToCell` and `mergeToCell` mutations have a different replay effect.
Their timestamp identifies the aggregate cell, so a stable timestamp targets the same cell but does not identify or deduplicate an input.
An Int64 Sum can count the input or accumulated state again after replay.
Regenerating the timestamp can target another cell version instead.
An upstream Flink aggregation emitting ordinary upserts remains subject to same-key ordering whenever it emits repeated updates; aggregation removes that collision only if the entire write emits at most one mutation per key.

An immediate keep-latest mutation deletes a column's versions and then sets its replacement inside one atomic `RowMutationEntry`.
Separate entries can execute in any order, including for the same row, and replaying an older replacement can delete a newer value.
Neither a batch size of one nor `maxVersions(1)` GC establishes application order or an immediate ordered-upsert guarantee.
See the [aggregate update]({{< relref "docs/examples/bigtable" >}}#updating-aggregate-cells) and [column replacement]({{< relref "docs/examples/bigtable" >}}#replacing-a-column-immediately) examples, and ADR-0093.

A stronger opt-in design is feasible for effects contained in one row.
[`CheckAndMutateRow`](https://cloud.google.com/bigtable/docs/writes#conditional) can test for an
event marker and, only when it is absent, atomically write both the data mutations and the marker in
that row.
Recovery can submit the same request again and observe the marker without repeating the effect.

Such a mode would require all of the following:

- a stable event ID supplied by the application;
- the marker and every protected mutation in the same row;
- a garbage-collection policy that retains the marker longer than the maximum replay horizon;
- [single-cluster routing](https://cloud.google.com/bigtable/docs/routing#single-row-transactions),
  which Bigtable requires for single-row transactions; and
- a serializer or request API different from the current `RowMutationEntry`, whose public surface
  does not expose its mutations for wrapping in a conditional request.

It cannot protect mutations spanning rows or tables.
It also replaces the current bulk `MutateRows` path with one conditional request per row, so
performance is a first-order part of the support decision.

The runtime behind the last of those requirements now exists: the
[single-row request runtime]({{< relref "docs/connectors/datastream/bigtable" >}}#single-row-request-writes)
sends `CheckAndMutateRow` and `ReadModifyWriteRow` as one request per row, with one attempt each
and no retry.
The public conditional request API can express the predicate and mutations for a same-row marker;
read-modify-write entry points remain [#1180]({{< param BookRepo >}}/issues/1180).
The connector offers no managed marker mode: callers own the marker identity, retention and
protected mutations.
The runtime settles the replay boundary underneath that protocol. On its sink surface a completed checkpoint means every request
up to the barrier was answered. On its async surface — a `RichAsyncFunction` under
`AsyncDataStream` — the guarantee is Flink's: the async operator checkpoints every input whose
result it has not yet emitted and replays those after a restore, so a completed checkpoint means
*emitted or replayed*, never *applied*, and a replayed `ReadModifyWriteRow` applies its increment or
append again. Both are at-least-once, and a failure that ends a request before the service answers
(`DEADLINE_EXCEEDED`, `UNAVAILABLE`, `ABORTED`, `CANCELLED`) fails the job saying so rather than
retrying a request the service may already have applied.

What is now planned is not that eager marker mode but a committer-based one
([#1211]({{< param BookRepo >}}/issues/1211)): staged records applied at checkpoint completion
through `CheckAndMutateRow`, under a predicate that issue's design ADR settles.

### Spanner

Spanner [`BatchWrite`](https://cloud.google.com/spanner/docs/batch-write) is optimized for
high-throughput writes without a preceding read, and explicitly provides no replay protection.
The current connector keeps one mutation in each reported group so that one bad record can be routed
without failing the other groups.

A stable event ID, a durable ledger row, and the record's data mutations can instead be committed in
one short [read-write transaction](https://cloud.google.com/spanner/docs/transactions) in the same
database.
The transaction reads the ledger marker and applies both the data and new marker only when it is
absent.
This can protect non-idempotent database effects and remains safe when Flink submits the event again.

The transaction must not remain open across a Flink checkpoint.
Spanner may abort idle read-write transactions, and a checkpoint has no atomic relationship with the
source's offset commit.
The safe design is an eager, retryable service transaction whose stable event ID makes the whole
operation idempotent.

This mode would require a stable event ID, a ledger in the same database, ledger retention longer
than the replay horizon, and keys distributed well enough to avoid hotspots.
Batching several events in one transaction can amortize the read and commit, but one poison record
then fails that whole transaction and changes the current per-record failure-routing behavior.
That API and isolation policy belong in a connector-specific design ADR if performance justifies an
implementation.

## Performance decision rule

Correctness is a prerequisite for every candidate.
A mode that produces duplicate protected effects during replay is rejected regardless of speed.

Candidate and current paths are compared in the same region with the same service capacity,
payload, key distribution, client concurrency, Flink parallelism, and checkpoint configuration.
Every reported cell uses a warm-up and three measured repetitions.
More than 10% run-to-run throughput variation is inconclusive and must be repeated or reported as
such.

The support gates are:

| Outcome | Throughput against the current path | Candidate p95 latency against the current path |
|---|---:|---:|
| General support | at least 70% | no more than 2x |
| Constrained opt-in | at least 25% | no more than 4x |
| Decline | below 25% | or above 4x |

Stage 1 screens the service primitive with a 1 KiB payload, evenly distributed keys, bounded
asynchronous concurrency, a 10% replay arm, and a deliberately serialized control that verifies
the harness can detect a known regression.

Only a candidate that passes Stage 1 proceeds to connector-level Stage 2.
Stage 2 adds 64 KiB payloads, a hot-key distribution, concurrency and parallelism of 1, 4, and 16,
and checkpoint intervals of 1, 10, and 60 seconds.
The 64 KiB ceiling leaves room for Cloud Tasks metadata under its 100 KB task limit and keeps the
cross-service comparison consistent.

## Evaluation status

Stage 1 ran against the real services on 2026-08-13 with the repository's pinned Google Cloud
clients; the Bigtable candidate ran again on 2026-09-05 with evenly distributed keys, and once
more the same day under an amended protocol with every repetition in its own JVM.
These results measure the service primitives, not end-to-end Flink jobs, and none of the
not-yet-implemented modes below is currently available through a connector builder.
Apart from the committer-based Bigtable mode planned under
[#1211]({{< param BookRepo >}}/issues/1211), no non-BigQuery exactly-once implementation or
additional performance stage is planned without a concrete non-idempotent requirement that the
existing write shapes cannot satisfy.

| Candidate | Stage 1 result | Current decision |
|---|---|---|
| Bigtable same-row conditional marker | Passed on 2026-09-05 under the amended protocol: 146.7% of baseline throughput at 0.65x baseline p95 with run-to-run ranges of at most 2.4%, after the same-day repeat had exceeded the 10% limit twice at 110.0% and 100.4% | The eager marker mode is not built; the conditional write is the commit path of the committer-based mode planned under [#1211]({{< param BookRepo >}}/issues/1211) |
| Spanner 100-record ledger transaction | Inconclusive: observed 44.6% of baseline throughput and 3.12x baseline p95, but keys were increasing rather than evenly distributed | Keep the existing mutation choices; reopen measurement only for a concrete non-idempotent database effect |
| Cloud Tasks deterministic task ID | Inconclusive: averages met the general gate, but throughput varied by 10.9% | Keep the existing bounded task-creation guarantee; no broader mode or repeat is planned |
| Pub/Sub publisher | No candidate because the service exposes no publisher idempotency key or publish transaction | No connector-only implementation is planned |

The raw repetitions, replay checks, declined alternatives, and cleanup evidence are in
[ADR-0104]({{< param BookRepo >}}/blob/main/docs/adr/0104-exactly-once-modes-use-service-native-replay-protection-and-pass-a-performance-gate.md),
[issue #591]({{< param BookRepo >}}/issues/591), and, for the 2026-09-05 Bigtable runs,
[#1208]({{< param BookRepo >}}/issues/1208) and [#1210]({{< param BookRepo >}}/issues/1210).
The current support decision is tracked by
[#596]({{< param BookRepo >}}/issues/596).
