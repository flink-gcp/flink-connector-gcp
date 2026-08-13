<!--
Copyright 2026 laughingman7743

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

# ADR-0085: The Spanner batch source splits by server-planned partition, and a partition is the unit of progress

- Status: Accepted
- Date: 2026-08-10, revised by [#587] (2026-08-13)
- Issues: [#221], [#36], [#224], [#587]
- Modules: spanner (`source`, `source.batch`)
- Current behavior: `docs/content/docs/connectors/datastream/spanner.md` § Source

## Context

The connector could write and not read. [#36] settled the shape on 2026-08-02 and split it into
[#221] (this DataStream batch source), #222 (change streams), #223 (Table API) and [#224] (the
gated real-GCP suite). The design references were Apache Beam's `SpannerIO` and
GoogleCloudDataproc/spark-spanner-connector, with no code copied from either; the JDBC baseline
(apache/flink-connector-jdbc#156) is what the native route replaces.

The pattern Beam and the Spark connector converged on maps onto FLIP-27 directly: the enumerator
opens a `BatchReadOnlyTransaction`, asks the service to plan partitions, and each `Partition`
becomes one split that a reader rejoins the same snapshot to read. That is the property the JDBC
column-range model (`scan.partition.column` / `lower` / `upper`) structurally cannot express — one
consistent snapshot across every subtask, on server-planned boundaries, with no numeric-column
requirement.

Since [#452] the assignment protocol is `base.source.PullAssignmentSplitEnumerator` (ADR-0083),
which named this issue as its expected next consumer. What is decided here is the planning step,
the split, and what a checkpoint of one means.

## Evidence

Checked against the pinned client (`google-cloud-spanner` 6.119.0, read from the sources jar) and
the pinned emulator (`gcr.io/cloud-spanner-emulator/emulator:1.5.56`), all on 2026-08-10.

### The SDK

- **`Partition` and `BatchTransactionId` are `Serializable`** with fixed `serialVersionUID`s, and
  every field type they hold is serializable too (`ByteString`, `KeySet`, `Options`, `Statement`,
  `PartitionOptions`, `Timestamp`). A Java-serialization round trip of both was measured to
  preserve them: `BatchTransactionId.equals` holds across the trip, and a `Partition` rebuilt from
  the bytes reads its 4,000 rows when handed to `execute`.
- **Neither type can be read back or rebuilt.** Every accessor is package-private except
  `Partition.getPartitionToken()`, and the two factories (`Partition.createReadPartition` /
  `createQueryPartition`) and the `BatchTransactionId` constructor are package-private as well.
  So Java serialization is not one option among several for the checkpointed split — it is the
  only one, and a unit test cannot mint either value.
- **`batchReadOnlyTransaction(BatchTransactionId)` does not call `initTransaction()`.** Rejoining
  the snapshot costs no RPC, so a reader may open one per split.
- **`AbstractReadContext.close()` does not close the session** — it ends the span and marks the
  context done. Only `cleanup()` closes it, and `execute(partition)` simply replays the table,
  index, key set, columns and options the partition carries, with its token.
- **`cleanup()` is a no-op against this client.** `BatchClientImpl` serves every batch transaction
  from a *multiplexed* session (`getMultiplexedSession`, cached for the multiplexed-session
  maintenance duration — 7 days by default), and `SessionImpl.close()` returns immediately when
  `getIsMultiplexed()`. Measured against the emulator: `cleanup()` returns. So [#36]'s premise —
  "the batch transaction stays valid with activity within ~1 h (session GC)" — describes pooled
  sessions and no longer governs; `version_retention_period` is the bound that does.
- **`Options.dataBoostEnabled(Boolean)` is a `ReadAndQueryOption`**, so it is accepted by
  `partitionQuery` and by both read forms, and the partition carries it to execution.
- **The client, not the service, refuses the two single-use-only timestamp bounds.**
  `batchReadOnlyTransaction(bound)` throws `IllegalArgumentException: Bounded staleness mode
  MAX_STALENESS is not supported for multi-use read-only transactions. Create a single-use read or
  read-only transaction instead.`, and the same for `MIN_READ_TIMESTAMP`.
- **`partitionRead` refuses a `limit` option** (`Preconditions.checkArgument`).
- **`ResultSet` has `close()` and no `cancel()`**, and `close()` is called automatically when
  `next()` returns false.

### What a `close()` does to a thread inside `next()`

The question ADR-0080 had to answer for gax's `ServerStream`, asked again here because the answer
decides whether a wake-up can be told from an end of stream. Measured over six runs, closing from
a second thread at staggered delays while a reader iterated a 4,000-row partition:

| What happened | Outcome seen |
| --- | --- |
| `close()`, then `next()` on the same thread | `next()` returned `false` |
| `close()` twice | returned; idempotent |
| `close()` from another thread, reader inside `next()` | **either** `next()` returned `false` **or** `SpannerException: CANCELLED: User cancelled stream` |

The reader thread terminated within the timeout on every run. **Both shapes occur**, so a
cancelled read is indistinguishable from a finished one at the `ResultSet` — the same conclusion
ADR-0080 reached, by a different mechanism.

### What the emulator does

| Probe | Result |
| --- | --- |
| `partitionQuery`, default hints, 4,000-row table | 2 partitions |
| `maxPartitions = 10` | 2 partitions — the hint is ignored |
| `partitionSizeBytes = 1024` | 2 partitions — the hint is ignored |
| `partitionRead`, `KeySet.all()` | 2 partitions |
| Rows per partition | partition[0] **0 rows**, partition[1] 4,000 rows |
| Union of the partitions | 4,000 rows, 4,000 distinct — complete and disjoint |
| `Options.dataBoostEnabled(true)` | accepted, no error, no change |
| Re-executing one partition token twice | same size, same order, and **not** primary-key order |
| `SELECT COUNT(*)`, `ORDER BY`, `LIMIT` | rejected, `INVALID_ARGUMENT` |
| `WHERE`, `LIKE`, a column alias | accepted |
| `@{spanner_emulator.disable_query_partitionability_check=true}` before `SELECT` | accepted, bypasses the check |
| the same hint after `SELECT` | rejected, `Invalid emulator-only hint` |

Two of these overturn what [#36] assumed. **The emulator does validate partitionability** — the
design comment said it does not — but its check is *conservative*: the message reads "The emulator
is not able to determine whether this query is partitionable", so it rejects more than the service
does, not less. And **the emulator plans more than one partition, one of which is empty**, which
is real coverage rather than the single-split degenerate case Bigtable's sampling has on its
emulator.

## Decision

**A split is one server-planned partition, and the partition is the unit of progress.** The
enumerator plans once, hands the partitions out one per request, and a reader that is interrupted
re-reads its partition from the start on restore. `PartitionSplitState.toSplit()` returns the
split unchanged: there is no offset to resume at that the service will honour.

**An offset-skip resume is declined**, and the reason is the API rather than the cost.
`partitionQuery` gives no order contract — a root-partitionable query cannot carry a top-level
`ORDER BY`, which the emulator confirms by rejecting one — so skipping *n* rows on re-execution
would rest on an ordering nothing promises. The emulator's own re-execution being stable, and in
an order that is *not* primary-key order, is the shape of the thing: stable in practice, promised
nowhere. A per-shape resume (offset-skip for `partitionRead`, whole for `partitionQuery`) was
declined on top of that, because one source would then hold two recovery semantics and a user
would have to know which read shape they had asked for to know what a restart costs them.

**The split carries the `BatchTransactionId` and the `Partition` as typed values, and the split
serializer writes each as a length-prefixed Java-serialized blob** behind its own version byte.
This is the one place this repository's "a checkpointed split owns a byte format this connector
controls" rule (ADR-0080) is not met, and it is not met because the SDK leaves nothing else: the
partition token is opaque, the accessors are package-private, and there is no public factory to
rebuild either value from parts. The exposure is bounded by the snapshot rather than by the
format — a savepoint older than `version_retention_period` cannot be resumed whatever its bytes,
because the transaction it names is gone.

**The `cancelled` flag decides whether a split finished, never the stream's behaviour**, for the
reason measured above. A fetch that ends either way while cancelled hands over what it read and
reopens next time; the same two shapes without the flag are a finished split and a real failure.

**A wake-up therefore costs a partition its progress, and that is chosen rather than inherited.**
Reopening a cancelled partition delivers the rows it had already handed on a second time, in a run
that never failed — which is not the same thing as duplicates across a restart, and is why it is
logged at WARN and counted under `partitionsReread` rather than left to be discovered downstream.
The duplicate window is one partition either way, so a job's deduplication strategy does not change.
The alternative — reading on rather than cancelling, and returning between rows instead — was
declined: it cannot interrupt a fetch that is blocked on a service which has gone quiet, so a job
being cancelled would wait for the client's own read deadline while a subtask held its slot. Prompt
teardown was preferred to an exactness this source does not promise anywhere else.

Note what makes this rare rather than routine, and note that it is a property of the enumerator
rather than of Flink: `PullAssignmentSplitEnumerator` assigns only in answer to a request, and this
reader requests only when it holds no partition, so the mid-partition wake-ups a job actually meets
come from `SplitFetcher.shutdown()`. A shutdown never fetches again, so the re-read is not reached.

**An empty partition is normal, not an error.** The emulator plans one on every run, and a reader
must finish such a split without complaint — which is also what a restored split whose partition
was fully read looks like from the outside.

**The enumerator owns `cleanup()`, and it reaches `PullAssignmentSplitEnumerator` as one
`AutoCloseable`** wrapping both it and the `Spanner` handle, as ADR-0083 prescribes for a
connector whose teardown is more than one seam. Its cost is nil against this client, and the
obligation is kept anyway: it is the documented contract, and a client that stopped multiplexing
would need it. Readers never call it — they close their own transaction handle, which the SDK
does not route to the session.

**`INVALID_ARGUMENT` from the planning call is surfaced, not wrapped away.** A non-root-
partitionable query is the user's to fix, and the service's message is the only thing that says
which part of their query is the problem.

**The two single-use-only timestamp bounds are rejected at the builder** even though the client
rejects them too. The client's refusal arrives when the enumerator plans, on the JobManager,
naming a transaction the user never asked for; the builder's arrives when the job is assembled,
naming the knob they set.

**The deserialization SPI emits zero or more non-null records synchronously through a collector.**
Emitting nothing skips the input row and increments `recordsSkipped` once; retaining the collector
or using it after the call returns is invalid.
All source paths use the shared synchronous invocation collector in ADR-0108 before progress can
advance.
This does not add row-level recovery state: the source still checkpoints only completed partitions,
so any interrupted partition is replayed from its start regardless of how many outputs each row
produced.

## Consequences

- **Parallelism is the service's decision**, and the gated suite measured how completely. Over
  5,000 rows in a 100-processing-unit regional instance (2026-08-10,
  `SpannerSourceRealGcpITCase`), Spanner planned **one** partition — for `partitionQuery` with
  default hints, with `maxPartitions = 16`, with `partitionSizeBytes = 1024`, and for
  `partitionRead`. A table that small is one split, so this is a measurement of that scale rather
  than evidence about a large one; what it does settle is that a hint cannot manufacture
  parallelism the data's layout does not offer, which is why the enumerator warns when it plans
  fewer partitions than there are subtasks rather than trying to ask for more.
- **A restart re-reads at most one partition per in-flight subtask**, which is what the docs must
  say rather than "at-least-once" alone: the duplicate window is a partition, not a record.
- **Split planning has emulator coverage, and the partitionability path does too** — which [#36]
  expected to need real GCP for. What [#224] then measured, and what it found:
  - **The service refuses the same three shapes the emulator does.** `SELECT COUNT(*)`, `ORDER BY`
    and `LIMIT` are all rejected with `INVALID_ARGUMENT: Query is not root partitionable since it
    does not have a DistributedUnion at the root`, plus a `links` block pointing at the documented
    conditions; a plain scan and a `WHERE` predicate are planned. The emulator's conservatism, real
    in principle because its check is its own, did not manifest on any shape tried. What differs is
    the *message*: the service names the condition and links its documentation, where the emulator
    says only that it could not determine partitionability — which is the argument for surfacing
    the refusal unwrapped rather than wrapping it.
  - **Data Boost serves a read**, on the cheapest instance the suite can create: a boosted
    `partitionQuery` returned all 5,000 rows, and a job built with `dataBoostEnabled(true)` read the
    table end to end. It needs no edition upgrade — Google lists Data Boost from `STANDARD` up — and
    the `spanner.databases.useDataBoost` permission is exercised by the read succeeding at all. Its
    quota and its billing stay unmeasured; a suite this size reaches neither.
- The emulator's bypass hint is a **statement-level** hint and must precede `SELECT`; placed after
  it, the emulator answers `Invalid emulator-only hint`. Any IT wanting a query shape the
  emulator's check refuses has to write it that way.

## Alternatives declined

- **Reconstructing the `Partition` from its token plus the connector's own read operation**,
  so the split could own its byte format. The factories are package-private; reaching them from
  production code would put this connector inside the vendor's package, which is a place ADR-0067
  admits *tests* to and nothing else.
- **Two opaque `byte[]` fields on the split** instead of the typed values, which would have
  removed the need for the test helper below. Declined: the split stops reading as what it is in a
  log line, a restore-time failure moves to read time, and the seam's signature stops naming what
  it takes.
- **A single-partition fallback** when `partitionQuery` refuses a query (Beam's `NaiveSpannerRead`
  equivalent). [#36] holds the deferral; the failure message names the constraint until a user
  asks for it.

## The vendor-package test helper

`flink-connector-gcp-spanner/src/test/java/com/google/cloud/spanner/TestPartitions.java` mints
`Partition` and `BatchTransactionId` for the unit tests, and is the **second** file in this
repository whose package is outside `io.github.flink.gcp.*` — ADR-0067 records the first
(BigQuery's `TestJobs`) and says a second is a decision to take rather than a precedent to follow.

It is taken here because ADR-0067's bar is met on both halves: neither type has a public
constructor, factory or reachable super-constructor, *and* the behaviour under test genuinely
reads the value — the split serializer round-trips it, the enumerator builds splits from it, and
the reader hands it to `execute`. The helper reaches two package-private factories and one
package-private constructor, its javadoc names them and the SDK version they were verified
against, and an SDK release that moves any of them fails a test at compile time.

[#36]: https://github.com/laughingman7743/flink-connector-gcp/issues/36
[#221]: https://github.com/laughingman7743/flink-connector-gcp/issues/221
[#224]: https://github.com/laughingman7743/flink-connector-gcp/issues/224
[#452]: https://github.com/laughingman7743/flink-connector-gcp/issues/452
[#587]: https://github.com/laughingman7743/flink-connector-gcp/issues/587
