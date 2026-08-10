---
title: Spanner
type: docs
weight: 50
---

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

# Spanner connector

An at-least-once sink that applies one Spanner `Mutation` per record, and a bounded source that
reads a database at one snapshot on partitions the service planned. Both dialects — GoogleSQL and
PostgreSQL — are supported from the same code; the dialect is a property of the database, and the
sink reads it rather than being told.

Every option is listed on the [Spanner options]({{< relref "docs/reference/spanner" >}}) page. What
is implemented and what is planned is in the
[module README](https://github.com/laughingman7743/flink-connector-gcp/blob/main/flink-connector-gcp-spanner/README.md).

## The destination is a database, not a table

The sink is configured with a `SpannerDatabase`. Which *table* a record goes to is not configured
at all: the serializer returns a `Mutation`, and a mutation names its own table. One sink therefore
writes to as many tables of that database as its serializer produces.

That is a shape of its own among the connectors here. The BigQuery, Pub/Sub, Cloud Tasks and
Bigtable sinks resolve a destination per record, through a resolver the builder is given; this one
fixes the database and leaves the table to the mutation, so there is no resolver to configure.

```java
Sink<OrderEvent> sink =
        SpannerSink.<OrderEvent>builder()
                .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
                .serializer(
                        (event, context) ->
                                Mutation.newInsertOrUpdateBuilder("Orders")
                                        .set("OrderId").to(event.getId())
                                        .set("Total").to(event.getTotal())
                                        .build())
                .build();
```

## How the sink writes

The sink buffers mutations and applies them with `DatabaseClient.batchWriteAtLeastOnce`, **one
mutation per mutation group**. The group is the unit Spanner reports a status for, so a refusal
names exactly the record it is about — which is what makes per-record failure routing possible at
all. A plain commit would fail the whole batch over one bad row.

The writer is synchronous: there is no mailbox, no callback thread and no in-flight bookkeeping,
because this RPC has no asynchronous or self-batching form to wrap. It is one streaming call the
writer makes and consumes to completion on the task thread. The decision, and the facts behind it,
are in [ADR-0075](https://github.com/laughingman7743/flink-connector-gcp/blob/main/docs/adr/0075-the-spanner-sink-batch-writes-and-owns-the-whole-retry-loop.md).

### Delivery guarantee, and why the mutation operation is your decision

The sink is **at-least-once** and stateless: it keeps nothing across checkpoints, because it empties
its batch before the barrier passes. A completed checkpoint means every record up to it was
applied, skipped by the serializer, or handed to the failure handler.

Spanner's batch write has **no replay protection** — the service's own documentation says a mutation
may be applied more than once. A record can therefore reach the database twice: after a job
restart, and also within one attempt, when a request whose outcome never arrived is re-sent.

Which mutation operation the serializer builds is what decides whether that matters:

| Operation | Replayed |
|---|---|
| `insertOrUpdate`, `replace` | Idempotent — the sink is effectively-once |
| `delete` | Idempotent, and a delete of a row that is not there is simply applied |
| `insert` | Refused with `ALREADY_EXISTS`, routed to the failure handler |
| `update` | Idempotent — but if the row was deleted in between, Spanner answers `NOT_FOUND`, which **fails the job**. See below |

## Batching

A request Spanner refuses is refused as a whole, taking every mutation in it with it, so a sink that
accumulates mutations has to bound the request it builds. That is what the three batch limits are
for, and it makes them correctness rather than tuning.

**Which limit each one defends is narrower than three knobs make it look.** Spanner's quotas page
documents "mutations per commit (including indexes)" of 80,000 and a "commit size" of 100 MiB — both
about `Commit`, which this sink does not use. The one row that names batch write is "mutations per
*mutation group* in a batch write request", also 80,000. The batch write page adds a single sentence,
about size only: *"the maximum size for a batch write request is the same as the limit for a commit
request"*. So:

- **No per-request mutation count is documented for batch write at all.** `maxBatchCells` and
  `maxBatchMutations` bound the request as a proxy for its size, and keep a batch far below the
  per-mutation-group 80,000 — which this sink, putting one mutation in each group, reaches only
  through a single mutation that breaches it alone: a range delete over a table with secondary
  indexes, which costs one mutation for the table plus one per index for every row the range
  matches. On a table with no secondary index a range delete costs one however many rows it hits.
- **`maxBatchBytes` is the one defending a documented request-level limit**, and how large that
  limit is could be read two ways: 100 MiB by the sentence above, or 10 MiB by the quotas page's
  "request size other than for commits". The gated real-GCP suite measured it
  ([#441]({{< param BookRepo >}}/issues/441)): the service accepts a request of roughly 12 MiB and
  refuses one of roughly 110 MiB, naming `104857600` bytes — 100 MiB exactly. Note what the refusal
  is: a transport-level `RESOURCE_EXHAUSTED`, which this sink treats as transient and retries, not
  a fail-fast rejection.

**All three are bounded at the setter**, so a value a request could not carry fails the job at
submission rather than on a task manager:

- `maxBatchBytes` at 100 MiB — the figure the service was measured to enforce. This is the ceiling
  that defends a refusal Spanner documents.
- `maxBatchCells` at 80,000. Precautionary rather than a refusal anyone has seen: Spanner documents
  no request-level mutation count either way, so the cap holds a batch to the only mutation figure
  it does publish.
- `maxBatchMutations` at 80,000 too — and *derived* from the cell ceiling rather than repeated,
  because every mutation costs at least one cell, so a batch never holds more mutations than cells.

**The three limits are ANDed** — a batch flushes on whichever binds first — so raising one alone
usually changes nothing: `maxBatchCells` of 500,000 against the default 1 MiB and 500 mutations
produces exactly the batches the defaults did. `maxBatchCells` and `maxBatchBytes` are the pair to
reach for; lower `maxBatchMutations` to cap a batch by count regardless of how wide the rows are.

That ANDing has one case worth naming, because nothing else would tell you: **`maxBatchMutations`
set above the configured `maxBatchCells` can never take effect**, since the cell cap is reached
first however cheap the mutations are. The configuration works, so it is not refused — building the
options **logs a warning** instead, naming both values. It is written wherever the job's `main`
runs: the client log under `flink run`, the JobManager log in application mode, the console in an
IDE. The warning suggests no remedy, because neither obvious one is safe — lowering
`maxBatchMutations` below `maxBatchCells` does not make the count cap bind (what each mutation costs
in cells decides that), and raising `maxBatchCells` spends the headroom above.

`maxBatchCells` is counted the way Spanner counts a mutation. A written column costs one cell for
the table plus one for every secondary index that contains it — as a key column or as a `STORING`
column, since both rewrite an index entry — and a delete costs one plus the table's index entries.
The index part is a property of the schema, so the sink reads it from the database's
`INFORMATION_SCHEMA` once, when the writer opens. That read needs `spanner.databases.select` as well
as write access. On a wide row that count is a better proxy for the request's size than a mutation
count is, which is why the sink keeps it rather than counting mutations alone.

Two consequences worth knowing:

- **A table the sink did not see is counted without its index entries** — one created after the job
  started, or one in a named schema rather than the default one. That undercounts, and the default
  `maxBatchCells` of 5,000 is deliberately 16 times under the 80,000 ceiling so the undercount has
  room. Raising the limit toward 80,000 removes that room.
- **The byte limit is an estimate.** The client library exposes no public way to size a `Mutation`
  as it goes on the wire, so the sink adds up the values it can see and ignores framing — about
  sixty bytes a mutation, measured — and so it reads low. The default of 1 MiB sits 100 times under
  the 100 MiB limit, which is the room the estimate is allowed to be wrong in.
  One value type is *not* counted as itself: a `BYTES` column is counted at its **base64** length,
  because a Spanner value travels inside a `google.protobuf.Value`, which has no bytes kind. That
  was measured, not assumed — 83,886,080 raw bytes arrived as 111,852,884, four thirds of
  themselves. Counting the raw length made the estimate read a quarter low for a `BYTES`-heavy
  batch, which is more than the room above once `maxBatchBytes` is raised toward its ceiling.

The defaults are Apache Beam's, and Beam batches for `Commit` rather than for batch write — which is
where the commit-shaped 80,000 entered this connector. They sit far under every reading of every
limit all the same.

There is no primary-key sorted batching. Apache Beam defaults its grouping factor to 1 — which
skips its sort — for unbounded input, and a streaming sink's per-checkpoint batches do not amortize
one.

[ADR-0077](https://github.com/laughingman7743/flink-connector-gcp/blob/main/docs/adr/0077-batch-limits-are-counted-in-index-aware-cells-read-once-at-open.md)
carries the measurements and the declined alternatives.

## Error handling

Every refusal Spanner reports comes back per mutation group, so the sink can act per record. It
sorts them three ways.

**Routed to the `failedMutationHandler`** — the service refused this one mutation and would refuse
it again:

- `ALREADY_EXISTS` — a replayed `insert`, or a collision on a `UNIQUE` index.
- `INVALID_ARGUMENT` — a malformed argument.

**Retried** on the sink's own budget — the service, not the mutation: `ABORTED`, `UNAVAILABLE`,
`DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`.

**Fails the job** — everything else, and every failure of the *request* whatever its status. A
request-level failure names no mutation, and the mutations it carried have no reported outcome, so
dropping them would discard records the service may not have looked at yet.

### What does not reach the failure handler, and why

This is the part worth reading before configuring a dead-letter queue. Measured against the
emulator, one run, 2026-08-09:

| What you did wrong | Status | What the sink does |
|---|---|---|
| `insert` of a key that already exists | `ALREADY_EXISTS` | Routed |
| `insert` colliding on a `UNIQUE` index | `ALREADY_EXISTS` | Routed |
| `NULL` in a `NOT NULL` column | `FAILED_PRECONDITION` | **Fails the job** by default |
| A value longer than the column allows | `FAILED_PRECONDITION` | **Fails the job** by default |
| A foreign-key violation | `FAILED_PRECONDITION` | **Fails the job** by default |
| A `CHECK` constraint violation | `OUT_OF_RANGE` | **Fails the job** by default |
| A column the table does not have | `NOT_FOUND` | Fails the job |
| A table the database does not have | `NOT_FOUND` | Fails the job |
| An `update` whose row is not there | `NOT_FOUND` | Fails the job |
| A `delete` whose row is not there | *applied* | — |

So the ordinary schema violations are not routed **by default** — and that default is a choice you
can change.

`constraintViolationPolicy(ROUTE_TO_FAILURE_HANDLER)` moves both of those statuses into the failure
handler, which then decides between failing, dropping and dead-lettering like it does for every
other per-record refusal. The default is `FAIL_JOB` for two reasons:

- A constraint violation usually says the **mapping** from records to columns is wrong, not that one
  record is anomalous. Every record of that shape will be refused, so shedding them one at a time
  hides a systematic problem behind a green job.
- `FAILED_PRECONDITION` has one documented cause that is neither data nor permanent: while a
  database's [CMEK key](https://cloud.google.com/spanner/docs/cmek) is disabled, destroyed or
  unreachable, **every** write is refused with it, and service is restored automatically when the
  key comes back. A job routing that status into a dropping handler would shed its whole stream
  through a key incident rather than waiting it out.

Failing the job loses nothing: the records are replayed from the source on restart. What it costs is
progress, which is the trade the opt-in exists to let you make differently.

Two things that are *not* covered by that policy, because they are not constraint violations: a
duplicate key or unique-index collision is `ALREADY_EXISTS` and is always routed, and a schema
change in progress is `ABORTED`, which the sink retries.

`NOT_FOUND` is not routed either: a missing table or column fails every record alike, so it is a
configuration error rather than a bad row. **This sink creates nothing** — no database, no table, no
column.

The last row of that table is the uncomfortable one. An `update` whose row has since been deleted
is *data*, and it still fails the job, because `NOT_FOUND` does not say whether the table or the
row is the thing that is missing — and routing it would mean dropping every record of a job that
merely has a table name wrong. If your stream can legitimately update rows that may be gone,
`insertOrUpdate` is the operation that says so.

The reasoning, the full measurement and the reopen condition are in
[ADR-0076](https://github.com/laughingman7743/flink-connector-gcp/blob/main/docs/adr/0076-two-spanner-statuses-are-routed-and-a-request-failure-never-is.md).

### Retries belong to the sink

Unlike every other Google client this project builds on, the Spanner client library does **not**
retry the RPC this sink writes with — its generated settings give batch write an empty
retryable-code set. So the retry loop is the sink's, which is why `SpannerWriterOptions` carries
retry knobs where the Bigtable sink's deliberately does not.

A retry re-sends exactly what is still undecided: the mutations whose group came back transient,
plus the mutations whose group the service never reported on, which is what a stream failing
part-way through leaves behind. Mutations already applied are never re-sent, so a retry does not
multiply the duplicates an at-least-once sink can produce. Exhausting `retryMaxAttempts` fails the
job.

### Dead-letter payloads

`FailedMutation.getPayloadBytes()` is the **Java-serialized** `Mutation`, not a protobuf. That is
not a preference: the client library exposes no public route from a `Mutation` to its wire form,
and its debug rendering truncates every string value at 36 characters, so it cannot stand in. A
handler that wants the mutation itself should take `FailureHandler<FailedMutation>` and read
`getMutation()`; the bytes exist for the cross-connector dead-letter queue, which sees only the
shared `FailedElement` view.

## Skipping records

Returning `null` from the serializer **skips** the record: it is written nowhere, is not a failure,
never reaches the failure handler, and is counted by `recordsSkipped`. Throwing marks the record as
failed and routes it instead. This is the same contract every connector here follows.

## Source

The source reads a Spanner database at one snapshot and finishes.

```java
Source<Singer, ?, ?> source =
        SpannerSource.<Singer>builder()
                .database(SpannerDatabase.of("my-project", "my-instance", "my-db"))
                .readOperation(
                        SpannerReadOperation.query(
                                Statement.of("SELECT id, name FROM singers")))
                .deserializer(new SingerDeserializer())
                .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "singers");
```

Bounded is not the same as batch-only: a bounded source runs inside a streaming pipeline and simply
ends, which is what makes reading a Spanner table and joining it against an unbounded stream work.

The options are in [the reference]({{< relref "docs/reference/spanner" >}}#spannersourcebuilder).

### Splits, partitions and recovery

**Spanner decides how the read is divided, not the job.** The enumerator opens one batch read-only
transaction, asks the service to partition it, and turns each partition into one split. Every
subtask then rejoins that same transaction and streams the partitions it is given, so every row the
job sees comes from one consistent snapshot — across every subtask, with no column to split on and
no bounds to supply.

`maxPartitions` and `partitionSizeBytes` are **hints**, which is Google's own word for them. The
service may return more partitions or fewer, and a job that plans for a particular parallelism has
to cope with getting a different one. The enumerator logs the count it received, and warns when it
is smaller than the parallelism — the subtasks left without a partition finish immediately.

**A partition is the unit of progress, and the unit of re-reading.** There is no position inside a
partition to resume at: `execute` replays the whole thing, and a partitioned query's row order is
not contractual — a query is partitionable only when the first operator of its execution plan is a
distributed union, which rules out the top-level `ORDER BY` that would fix an order — so a count of
rows already read would not mean the same thing on a second execution. A checkpoint therefore records which partitions a reader still holds, and recovery
re-reads each of them from the start.

That makes the delivery guarantee **at-least-once, with a duplicate window of one partition**. It
applies in two places, and the second one is the surprising one:

- a job that fails and restarts re-reads the partitions that were in flight;
- a partition **cancelled mid-read while the job is running** — which is what Flink does to a
  reader it needs to interrupt — is opened again at its start, so the rows it had already emitted
  are emitted again. It is logged at `WARN` and counted by `partitionsReread`, which is the metric
  that explains duplicate output from a run that never failed.

If your pipeline cannot tolerate that, deduplicate on the primary key downstream. The alternative —
reading on rather than interrupting — would hold a subtask until the client's own read deadline
whenever the service went quiet, including while a job was being cancelled.

### The snapshot, and how long it lasts

The default is a **strong** read: the latest committed data at the moment the read is planned.
`ofReadTimestamp` and `ofExactStaleness` are the other two bounds a batch read can take, and a
stale read is cheaper for the service to serve because any replica can answer it.

`ofMaxStaleness` and `ofMinReadTimestamp` are **rejected by the builder**. Spanner allows those two
only on a single-use transaction, and a batch read is by construction a multi-use one — every
partition rejoins the transaction the plan was made in.

What bounds how long a read may take is the database's **`version_retention_period`** (one hour by
default, up to a week). A batch read holds a snapshot at a fixed timestamp, and once that timestamp
falls outside the retention window the data behind it is gone: a long backfill either finishes
inside the window or runs against a database whose retention was raised first. The same applies to a
savepoint — resuming a read whose snapshot has expired is not possible, whatever the state file
holds.

### Read shapes and push-down

A read is either a query or a table read, never both:

- `SpannerReadOperation.query(Statement)` — any root-partitionable query. Predicates and projections
  are pushed to the service, and a parameterised `Statement` carries its bindings.
- `SpannerReadOperation.read(table, keySet, columns)` — a key set and a column list, which is the
  cheapest shape for reading a table or a key range whole.
- `SpannerReadOperation.readUsingIndex(table, index, keySet, columns)` — the same through a
  secondary index, with the key set interpreted in the index's key space. A column the index does
  not store is read back from the base table, which costs a lookup per row.

**Not every query can be read this way.** Spanner partitions a query only when its execution plan
begins with a distributed union — in practice a scan of one table, with predicates and projections
but no aggregate, no `ORDER BY` and no `LIMIT`. A query that is not root-partitionable is refused
when the source plans, and the service's own message says which part it could not distribute; the
connector surfaces it rather than wrapping it, because that message is the only thing that tells you
what to change. Reading a non-partitionable query on a single subtask is deliberately not offered
([#36]({{< param BookRepo >}}/issues/36) holds the deferral).

### Deserialization

`SpannerStructDeserializationSchema` turns one `Struct` into at most one record, and declares its own
`TypeInformation` so a job needs no `returns(...)` after the source. Returning `null` **skips** the
row: it is emitted nowhere, is not a failure, and `recordsSkipped` is the only thing that reports it
— the same contract the sink's serializer has in the other direction. A row you could not read is a
failure and should be thrown, not skipped.

One row becomes at most one record. A Spanner row is a relational row, so fanning one out is a
`flatMap` in the job rather than a shape this SPI takes.

### Serverless reads with Data Boost

`dataBoostEnabled(true)` runs the read on compute that is not the instance's, so a large scan does
not contend with the workload the instance is serving. Three things come with it:

- the caller needs `spanner.databases.useDataBoost` on the database, which
  `roles/spanner.databaseReader` does **not** carry — `roles/spanner.databaseReaderWithDataBoost`
  is the role that does;
- the read is billed separately;
- its concurrency has a quota of its own, so `RESOURCE_EXHAUSTED` is a shape a boosted read can meet
  that an ordinary one does not.

`rpcPriority` is the cheaper lever, and a different one: `LOW` keeps the read on the instance's own
compute but tells Spanner to shed it first when that instance is at capacity, so a backfill yields
to serving traffic instead of competing with it. It costs nothing extra, where Data Boost is billed
separately; it also does not remove the read from the instance, where Data Boost does. The priority
applies to the reads that move the rows — the partition the service plans carries it and the read
replays it — and not to the one call that plans the partitions.

Data Boost has been exercised end to end against the service, by the gated real-GCP suite
([#224]({{< param BookRepo >}}/issues/224), measured 2026-08-10): a boosted read of a 5,000-row
table returned every row, both through `partitionQuery` directly and through a job built with
`dataBoostEnabled(true)`. Two things that measurement settles. It needs **no edition upgrade** —
Google lists Data Boost from `STANDARD` up, and the suite's instance is a 100-processing-unit
`STANDARD` one — and a read succeeding at all is the `spanner.databases.useDataBoost` permission
being honoured. What it does not reach is Data Boost's concurrency quota or its billing; a suite
this size touches neither, so the `RESOURCE_EXHAUSTED` a boosted read can meet is still described
here rather than demonstrated.

### Reading against the emulator

The emulator is a convenience, never evidence about the service, and on the read path it deviates in
one direction that is easy to get backwards. Measured against `emulator:1.5.56` on 2026-08-10:

| Deviation | Consequence | What the service does |
|---|---|---|
| It planned exactly two partitions, one of them empty, for every table measured (500 and 4,000 rows) | Split planning has no emulator coverage. What the emulator *does* give is real coverage of an empty partition, which a reader must finish without complaint | Planned **one** partition over 5,000 rows on a 100-processing-unit instance — a table that small is one split. Neither count is evidence about a large table |
| It ignores `maxPartitions` and `partitionSizeBytes` | Neither hint can be shown to have any effect here | Neither hint changed the count either, at that scale. A hint cannot manufacture parallelism the data's layout does not offer |
| Its partitionability check is **stricter** than the service's | It refuses aggregates, `ORDER BY` and `LIMIT` with "not able to determine whether this query is partitionable" — a query it rejects may be one Spanner would plan. A test needing such a shape prefixes the statement with `@{spanner_emulator.disable_query_partitionability_check=true}`, which works only *before* the `SELECT` | Refused the same three shapes, so the difference did not show on any shape tried — but with a different and far more useful message: `Query is not root partitionable since it does not have a DistributedUnion at the root`, and a link to the documented conditions |
| It accepts `dataBoostEnabled` and does nothing with it | The IAM permission, the quota and the billing are the gated suite's to show | Served the read: every row came back, on a `STANDARD`-edition instance. Quota and billing remain unmeasured |
| No IAM | `PERMISSION_DENIED` on a read is never exercised | Still not exercised: the E2E account holds `roles/spanner.editor`, which carries every permission the suite uses, so there is no unauthorized identity in it to refuse |
| It accepts extra DDL in a `CreateDatabase` request for a PostgreSQL-dialect database | A harness that creates its schema in one call works here | Refuses it — "DDL statements other than &lt;CREATE DATABASE&gt; are not allowed in database creation request for PostgreSQL-enabled databases" — so the gated harness issues a separate `updateDatabaseDdl` for that dialect |

### Not here yet

- Table API and SQL, which is [#223]({{< param BookRepo >}}/issues/223).
- A change-stream source, which is [#222]({{< param BookRepo >}}/issues/222) — a different API with
  different semantics, and a source of its own.

## Metrics

Registered on the sink writer's metric group.

| Metric | Type | Meaning |
|---|---|---|
| `numRecordsSend` | counter (Flink standard) | Records handed to the service. Counted once per record, never again on a retry |
| `numBytesSend` | counter (Flink standard) | Their estimated size |
| `numRecordsSendErrors` | counter (Flink standard) | Records routed to the failure handler, whether the serializer rejected them or the service refused the mutation |
| `recordsSkipped` | counter | Records the serializer returned `null` for |
| `mutationsRetried` | counter | Mutations re-sent after a transient failure, one per mutation per re-send. This is the sink's retry volume, and it exists here because the retry loop does |
| `batchesSent` | counter | Batch write requests, first attempts and re-sends alike |
| `bufferedMutations` | gauge | Mutations held for the next flush |
| `bufferedCells` | gauge | Their cost in the cells `maxBatchCells` counts, index entries included |
| `bufferedBytes` | gauge | Their estimated size |
| `errorClass.CODE.errors` | counter | Failed writes by status code, `CODE` being a gRPC status name or `UNCLASSIFIED` |

Two of these mean something here that they cannot mean on the sibling sinks. `mutationsRetried` and
the transient half of `errorClass.CODE.errors` are visible because this connector performs its own
retries — on the connectors whose SDK retries internally, the same work happens out of sight and
only the give-ups are counted. Read `bufferedCells` beside `bufferedBytes` to see which of the three
batch limits is actually firing, and therefore which knob to move.

There are no per-destination counters. The sink writes one database but any number of its tables, so
the meaningful cut would be per table — and its cardinality is the serializer's to decide, not a
bill the connector should sign on your behalf.

`currentSendTime` is deliberately unset: a batch write's latency covers a whole request of unrelated
mutations, so attributing it to records would say nothing an operator can act on.

### Source metrics

Registered on the split enumerator's group, on the coordinator, and on the source reader's group, on
each subtask.

| Metric | Type | Meaning |
|---|---|---|
| `readsPlanned` | counter | Planning calls that completed. One on a fresh run, zero on a restored one — which is how a restore tells itself apart at runtime |
| `splitsAssigned` | counter | Partition splits handed to a reader |
| `splitsReturned` | counter | Partition splits a failed reader gave back, to be handed out again |
| `unassignedSplits` | gauge (Flink standard) | Partition splits nobody holds yet |
| `numRecordsIn` | counter (Flink standard) | Records handed downstream |
| `rowsRead` | counter | Rows pulled off a partition, including rows the deserializer skipped |
| `partitionsReread` | counter | Partitions opened again from their start after a wake-up cancelled them part-way. Non-zero means some rows were delivered twice by a run that never failed |

`recordsSkipped` is registered on this side too, and means the same thing it does above: rows the
deserializer returned `null` for.

There is no bytes-read counter and no rows-remaining gauge. The client hands over a decoded `Struct`
and says nothing about what it cost on the wire, so any byte figure would be this connector's
arithmetic wearing the look of the quantity Spanner bills for; and a partition is an opaque token, so
nothing knows how many rows are left inside one.

## Testing

Functional coverage runs against the
[Cloud Spanner emulator](https://github.com/GoogleCloudPlatform/cloud-spanner-emulator) in
testcontainers, over both dialects, driving the sink's production writer-creation path — so the
client, the schema read and the batch write are all the real ones. A MiniCluster job test covers the
path a real job takes, with flushes driven by checkpoint barriers.

The emulator image is pinned separately from the other connectors' `google-cloud-cli` bundle: the
Spanner emulator implements the `BatchWrite` RPC only from v1.5.31, and the bundled one predates it,
so this sink's entire write path would answer `UNIMPLEMENTED` against it.

### Emulator deviations

An emulator is a convenience for fast feedback, never evidence about the service. Where the two
disagree, the service decides. Known deviations, and what covers them instead:

| Deviation | Consequence |
|---|---|
| One read-write transaction at a time | Tests that write concurrently serialize. The `ABORTED` the emulator answers with is classified transient, so the sink retries through it |
| No IAM | Neither `PERMISSION_DENIED` nor the `spanner.databases.select` requirement of the schema read is exercised. The gated real-GCP suite ([#224]({{< param BookRepo >}}/issues/224)) reads the schema over real credentials, but holds `roles/spanner.editor`, so a refusal is still not exercised anywhere |
| Rejection statuses are the emulator's | The table above was measured against the emulator, and **every row of it is now confirmed against the service** (2026-08-10) — same status, same per-group reporting. The gated suite asserts each row, so a change on either side has to be declared |

### The gated real-GCP suite

Everything above runs in an ordinary build. What only the service can answer runs in a separate,
opt-in suite ([#224]({{< param BookRepo >}}/issues/224)): the rejection statuses and their per-group
reporting, the mutation-cell weights read from the service's own `INFORMATION_SCHEMA` in both
dialects, how many partitions Spanner plans and for which query shapes, and Data Boost end to end.
It is also the only place the connector's clients are built over application-default credentials
rather than an emulator endpoint.

The suite is opt-in per command rather than per shell: each class carries `@Tag("gated")`, which
every ordinary build excludes, and `just e2e` is the one thing that clears the exclusion. Each class
creates a 100-processing-unit regional Spanner instance in the `STANDARD` edition, uses it, and
deletes it — nothing persistent is provisioned, because an instance bills for as long as it exists.
`STANDARD` is enough, and the suite shows it rather than citing it: the Data Boost tests run on that
instance and read every row, so exercising Data Boost needs no edition upgrade. Instance names carry their creation time, so a run that dies before its teardown
is reclaimed both by the next run and by a daily sweep.
