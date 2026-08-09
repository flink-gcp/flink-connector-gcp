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

An at-least-once sink that applies one Spanner `Mutation` per record. Both dialects — GoogleSQL and
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

Spanner's commit limits apply to a batch write **request** as a whole: 80,000 mutations *including
index entries*, and 100 MiB. A request over either is refused outright, taking every mutation in it
with it. The three batch limits are what keeps the writer under those, so they are correctness
rather than tuning.

`maxBatchCells` is counted the way Spanner counts. A written column costs one cell for the table
plus one for every secondary index that contains it — as a key column or as a `STORING` column,
since both rewrite an index entry — and a delete costs one plus the table's index entries. The index part
is a property of the schema, so the sink reads it from the database's `INFORMATION_SCHEMA` once,
when the writer opens. That read needs `spanner.databases.select` as well as write access.

Two consequences worth knowing:

- **A table the sink did not see is counted without its index entries** — one created after the job
  started, or one in a named schema rather than the default one. That undercounts, and the default
  `maxBatchCells` of 5,000 is deliberately 16 times under Spanner's 80,000 so the undercount has
  room. Raising the limit toward 80,000 removes that room.
- **The byte limit is an estimate.** The client library exposes no public way to size a `Mutation`
  as it goes on the wire, so the sink adds up the values it can see and ignores framing. The
  default of 1 MiB sits 100 times under the real limit, which is the room the estimate is allowed
  to be wrong in.

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
| `bufferedCells` | gauge | Their cost against Spanner's per-request mutation limit, index entries included |
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
| No IAM | Neither `PERMISSION_DENIED` nor the `spanner.databases.select` requirement of the schema read is exercised. The gated real-GCP suite ([#224]({{< param BookRepo >}}/issues/224)) is the only coverage |
| Rejection statuses are the emulator's | The table above is measured against the emulator. The same suite confirms each row against the service |
