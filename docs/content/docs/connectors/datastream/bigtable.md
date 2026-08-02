---
title: Bigtable
type: docs
weight: 40
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

# Bigtable Connector

Writes a DataStream into a Cloud Bigtable table, one row mutation per record, at-least-once. Every
option is in the [Bigtable reference]({{< relref "docs/reference/bigtable" >}}); the runnable job is
the [quickstart]({{< relref "docs/quickstart/bigtable" >}}); implementation status is the table in
the module
[README](https://github.com/laughingman7743/flink-connector-gcp/tree/main/flink-connector-gcp-bigtable).

```java
Sink<OrderEvent> sink =
        BigtableSink.<OrderEvent>builder()
                .table(TableDestination.of("my-project", "my-instance", "orders"))
                .serializer(
                        (event, context) ->
                                RowMutationEntry.create(event.id())
                                        .setCell("cf", "payload", event.timestampMicros(),
                                                event.body()))
                .build();
```

## What this connector is for

Bigtable is a wide-column store keyed by a single row key, and what a streaming pipeline usually
wants from it is a materialized view: the latest state per key, or an append-only history of events
under a key that a serving path reads by prefix. The sink is shaped for that — the record decides
its own row key and cells, and the sink's whole job is to get mutations there in bounded memory and
to make a checkpoint mean something.

Two properties of the service shape the design more than anything else. Mutations of **one** row
apply atomically while mutations of different rows do not, so ordering across rows is not something
this sink can offer or is asked for. And a write carries a **cell timestamp**, which turns
at-least-once from a duplicate problem into a choice — the same mutation applied twice is either an
overwrite or a second version, depending on what the serializer set.

## API notes

The record-to-mutation step is the whole public surface beyond the builder:

```java
@PublicEvolving
public interface BigtableSerializationSchema<T> extends Serializable {
    default void open(SerializationSchema.InitializationContext context) throws Exception {}

    @Nullable
    RowMutationEntry serialize(T element, SinkWriter.Context context) throws IOException;
}
```

Returning a `RowMutationEntry` rather than a narrower value type is deliberate: it is the client's
own mutation builder, so `setCell`, `deleteCells`, `deleteFamily` and `deleteRow` are all
expressible, several of them per record, and the sink adds no vocabulary of its own to learn.
Returning `null` **skips** the record — it is written nowhere and is not a failure — which is how a
filter that depends on the mutation being built belongs in the serializer rather than upstream of
the sink.

The signature and the null-means-skip convention are taken from the `BaseRowMutationSerializer` of
[google/flink-connector-gcp](https://github.com/GoogleCloudPlatform/flink-connector-gcp), so a
serializer written against that connector ports by changing the interface name. Its built-in
`GenericRecord` and `RowData` serializers are deliberately not ported: `RowData` conversion belongs
to the Table API layer ([#217]({{< param BookRepo >}}/issues/217)), and an Avro convenience is
additive whenever there is a use case asking for it.

`context` is Flink's write context, so `context.timestamp()` is the record's event time — usually
the right cell timestamp when the record carries none of its own.

## One table per sink

The table is fixed at build time. This is the one place the Bigtable sink is narrower than its
siblings, which both take per-record destinations, and the reason is in the client: a bulk mutation
batcher is bound to one table, so per-record tables would mean a pool of batchers, a share of the
in-flight budget for each, and an eviction policy for the tail. Writing to several tables means
several sinks today; a batcher pool waits for a use case
([#232]({{< param BookRepo >}}/issues/232) records the deferral).

The sink also never **creates** a table or a column family, so both must exist
([#233]({{< param BookRepo >}}/issues/233)). That is a smaller decision than it is for the Pub/Sub
sink, where auto-creation is a real feature: a Bigtable table's schema is its column families and
their garbage-collection policies, which is exactly the part a sink cannot guess.

## Delivery guarantees and state

**At-least-once.** The writer is stateless — it stores nothing in Flink state — and `flush()` runs
at every checkpoint barrier: it sends what the client has buffered and then waits until every
outstanding mutation has been acknowledged. So a completed checkpoint means Bigtable has applied
every record up to the barrier, and discarding operator state can never lose sink-buffered records.

Checkpointing must be enabled in a streaming job. Without it `flush()` never runs mid-stream and
outstanding mutations are lost on a failure; batch execution is covered by the end-of-input flush.

**Whether a replay is idempotent is the serializer's decision.** After a restart the records since
the last checkpoint are written again, and:

- a `setCell` carrying an **explicit** timestamp overwrites the same cell — the second write is
  invisible, and the sink is effectively exactly-once for that column;
- a `setCell` **without** one takes the server's clock, so the replay writes another version of the
  cell, and the table's garbage-collection policy (`maxVersions`, `maxAge`) decides how long both
  survive.

Neither is wrong, but only one of them is a choice made on purpose. Setting the timestamp from the
record — its event time, an updated-at column, `context.timestamp()` — is what makes a replay a
no-op. Note that a cell timestamp is in microseconds but a table's granularity is milliseconds, so
the value must be a multiple of 1000 — `context.timestamp()`, which is in milliseconds, has to be
multiplied rather than passed through. Bigtable answers a violation with `INVALID_ARGUMENT`
(*"Timestamp granularity mismatch. Expected a multiple of 1000 (millisecond granularity)"*), which
makes it a [row-level failure](#error-handling) and so droppable — measured against the service, not
inferred. Read that together with the batch blast radius described there before running a dropping
policy: a job that multiplies the timestamp wrongly produces the violation on *every* record.

Deletes replay the same way and are naturally idempotent, with one caveat worth stating: a
`deleteRow` replayed after later writes for the same key would delete those too. That is a property
of the mutation, not of the sink.

## Retries belong to the client

The client ships retry settings for `MutateRows` — per entry, for the transient codes
(`UNAVAILABLE`, `DEADLINE_EXCEEDED`, …), with its own exponential backoff — and this sink leaves
them alone rather than adding a loop around them. That is the opposite of the
[Cloud Tasks]({{< relref "docs/connectors/datastream/cloudtasks" >}}) sink, whose generated client
retries `CreateTask` on nothing at all and therefore has to; the difference is in the clients, not
in a preference. What reaches this writer is a failure the client already gave up on, so it is
classified and either routed or fatal — never retried again.

## Tuning

Two pairs of knobs, doing different jobs, both on
[`BigtableWriterOptions`]({{< relref "docs/reference/bigtable" >}}#bigtablewriteroptions).

**The batch thresholds** (`batchElementCount`, `batchByteSize`) are handed to the client and decide
when it sends a batch. Both are unset by default, which leaves the client's own values (100
mutations, 20 MB, and a one-second timer) in place — recorded in the reference for sizing rather
than restated in this project's code, so a client upgrade that retunes them is inherited.

**The in-flight bounds** (`maxInFlightMutations`, `maxInFlightBytes`) are the writer's own, and they
are what backpressures the stream: at either cap `write()` yields to the task mailbox until
completions bring the counters down. Both are needed — a mutation may be megabytes, so a count alone
bounds no memory. Admission is checked as "below the cap", never as "does this mutation fit", so a
mutation larger than the byte cap is admitted on an empty writer and overshoots it until it
completes; that is deliberate, because yielding to the mailbox blocks until a mail arrives and no
mail can arrive with nothing in flight, which would make a fits-predicate a task hang rather than
backpressure.

**The client's own flow controller is why raising the bounds has a ceiling.** It permits 1000
entries per channel and 100 MB of accumulated size, and when either is reached it *blocks* the
calling thread — which is Flink's task thread, the one that has to stay free to run mailbox mails
and checkpoint barriers. Its static limits are not settable through the client's public API (only
latency-based throttling can be turned on and off), so the sink's answer is to keep its own bounds
below them: the defaults are, and a much larger `maxInFlightMutations` simply moves the effective
bound into the client, where it stalls instead of backpressuring. This is the same defect class the
Pub/Sub sink removed its SDK flow-control knobs over
([#85]({{< param BookRepo >}}/issues/85)).

There are no rate knobs beyond this. Bigtable's throughput is a property of the instance's nodes and
of how well the row keys spread across tablets; a sink-side rate limit would not change either.

## Error handling

Failures are classified on the task thread — mutation completion callbacks re-dispatch onto Flink's
mailbox, so the writer's state is touched from one thread only — and routed by class:

| Class | Examples | Behavior |
|---|---|---|
| Row-level | `INVALID_ARGUMENT` — a cell timestamp that is not a multiple of 1000, an empty row key | Routed to the configured [failed-mutation handler](#failed-mutation-policy); applying the same mutation again could not succeed |
| Fatal | `NOT_FOUND` (a missing table or column family), `PERMISSION_DENIED`, `UNAUTHENTICATED`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`; an outage the client's own retries gave up on (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `ABORTED`, `RESOURCE_EXHAUSTED`); failures carrying no status at all | Fail the ongoing write or checkpoint |

Those two examples are the ones measured against the service, and they are the whole list this page
will vouch for — see [what the gated suite measures](#testing). Two conditions that read like
`INVALID_ARGUMENT` candidates are not: an entry carrying more than 100,000 mutations, or more than
200 MiB of them, is rejected by the **client**, before any RPC, so it arrives as a serialization
failure rather than as a service rejection (see
[serializer failures](#failed-mutation-policy) — such a `FailedMutation` carries no entry and no row
key). The check sits in the mutation list itself, so it covers `deleteCells` and `deleteRow` exactly
as it covers `setCell`.

The split's purpose is that a *dropping* handler never sees a condition. An outage would otherwise
bleed the stream one mutation at a time instead of backpressuring it, and a missing column family —
which fails every record alike — would empty the whole stream into the dead-letter destination under
a green job.

**A routed rejection is not confined to the mutation that caused it.** Bigtable rejects the whole
`MutateRows` request, and the client then fails every entry of that batch with the same status, so
one malformed record takes its whole batch with it: measured against the service, one bad record
written beside a good one had **both** delivered to the handler and neither written. Under a
dropping policy that is silent loss of the good records, bounded by the batch size
(`batchElementCount`) rather than by anything about the bad record. Whether the sink should tell a
request-level rejection from a per-entry one — the former is safe to retry, since a failed
`MutateRows` wrote nothing — is [#239]({{< param BookRepo >}}/issues/239). Until it does, a
dropping policy on this connector is a choice about batches, not about records.

**Only a status that is unrecoverable by definition is routed**, which is why the row-level class is
`INVALID_ARGUMENT` alone. gRPC defines it as *"problematic regardless of the state of the system"*
and [AIP-194](https://google.aip.dev/194) lists it as must-not-retry; `FAILED_PRECONDITION`, by the
same definition, means the system is *not in the required state*, so a mutation rejected with it
might well be accepted later — dropping it would be data loss, however data-shaped the failures it
names look.

Routing takes **both halves** of a condition, and each half reads the cause chain differently on
purpose: no transient status *anywhere* in the chain, so an unstable service cannot produce a dead
letter even when a data-shaped status sits in front of it; and the chain's *first* classifiable
status is `INVALID_ARGUMENT`, because one buried under an `INTERNAL` or an `UNKNOWN` describes the
inner call, and dropping the mutation over it would discard a record on a server-side failure. The
two mistakes are mirror images, and the classifier's tests pin both.

A failure captured in a completion callback is rethrown on the task thread from the next `write()`
or `flush()`, and `flush()` waits for every outstanding mutation, so a failure can never slip past a
checkpoint barrier.

### Failed-mutation policy

Two data-shaped failures are pluggable: a record the serializer rejects, and a row-level rejection.
The policy is `failedMutationHandler(...)`, taking the shared `FailureHandler<FailedMutation>` SPI
from `flink-connector-gcp-base` ([#37]({{< param BookRepo >}}/issues/37) standardizes it across the
connectors in this repository):

```java
BigtableSink.<OrderEvent>builder()
        .table(TableDestination.of("my-project", "my-instance", "orders"))
        .serializer(new OrderEventMutations())
        .failedMutationHandler(FailureHandler.logAndDrop())
        .build();
```

- `FailureHandler.failJob()` (default) — every failed mutation fails the checkpoint
- `FailureHandler.logAndDrop()` — logs each failed mutation at WARN and drops it
- `FailureHandler.sendToDeadLetterQueue(...)` — forwards each one to a `DeadLetterQueue`
  (experimental)

`FailedMutation` carries the destination, the `RowMutationEntry` (`null` when serialization itself
failed), the row key, and — as the shared contract's payload — the **serialized
`MutateRowsRequest.Entry`**, so a dead-letter consumer recovers every mutation of the row with
`MutateRowsRequest.Entry.parseFrom(bytes)` rather than just learning which row it was. Delivery of
handled elements is at-least-once for failures that recur on replay; the SPI's own page states that
guarantee in full.

## Scope

Not implemented, each with its issue rather than a promise:

- reading — the bounded scan source is [#216]({{< param BookRepo >}}/issues/216) and change streams
  are [#35]({{< param BookRepo >}}/issues/35);
- Table API and SQL, including a `RowData` serializer:
  [#217]({{< param BookRepo >}}/issues/217);
- per-record table destinations and table or column-family auto-creation, both deferred above;
- conditional and read-modify-write mutations (`checkAndMutateRow`, `readModifyWriteRow`). These are
  request-response primitives rather than a write path a sink batches: each is one RPC whose result
  the caller is expected to read, and neither participates in `MutateRows`.

## Testing

Unit tests cover the writer against a fake batcher and a fake mailbox: the skip contract, both
failure classes, both in-flight caps engaging, the drain-then-flush ordering the failure SPI
requires, and a handler failure raised inside a completion callback surviving to the next call. The
fake completes nothing on its own, which is what lets a test hold the writer at a cap; the writer
tests carry a timeout, because a broken admission predicate hangs rather than fails.

Integration tests run against the
[Bigtable emulator](https://cloud.google.com/bigtable/docs/emulator) in a container, through the
production client-construction path in its emulator mode, plus two MiniCluster jobs — streaming with
checkpoints while the source is still producing, and batch with nothing but the end-of-input flush —
built through the public builder with no test seams. They need no credentials and run on every pull
request.

**The emulator is a convenience, not an authority.** It implements `MutateRows` and the table admin
surface, which is enough to prove that mutations arrive and that a flush means what it says, and
nothing there asserts a rejection the real service would produce.

A **gated suite against real Cloud Bigtable** covers what the emulator cannot
([#218]({{< param BookRepo >}}/issues/218)). It runs weekly, and locally when `BIGTABLE_IT_PROJECT`
is set; without that variable its classes skip. Note what setting it locally implies: the gate is
read by the classes themselves, not by a build profile, so **an ordinary local build runs the gated
suite too** and creates the instances it needs. That is the same shape the BigQuery and Pub/Sub gates
have, but this is the first one that bills for a resource rather than using a standing one, so it is
worth choosing deliberately whether the variable lives in a shell you build from every day. Two
things only this suite can show:

- **The client-construction path every real job takes.** Every emulator test passes
  `emulatorEndpoint(...)`, so the branch that builds a client over application-default credentials
  against the production endpoint runs nowhere else. A MiniCluster streaming job with checkpoints
  covers it.
- **Which status Bigtable rejects a mutation with**, and therefore which side of the
  [row-level/fatal boundary](#error-handling) each rejection lands on. This is where the two
  `INVALID_ARGUMENT` examples in that table come from, where the fatal `NOT_FOUND` of a missing
  column family is pinned, and where the batch blast radius was found.

There is no persistent instance to run it against: a one-node instance is a standing cost of roughly
$470 a month, so each gated class **creates an instance and deletes it afterwards**, and a run that
dies before deleting is swept by the next one — instance names carry their creation time, and
anything older than two hours is reclaimed. That is why nothing in `opentofu/` declares a Bigtable
instance, only the API enablement and the grant.

### Where the emulator differs from the service

Measured 2026-08-02, against the pinned `google-cloud-cli:441.0.0-emulators` image and real Bigtable
in `us-central1`, for the same three inputs. Every row is asserted from both sides, so an emulator
image bump has to state what it changed rather than making this table quietly wrong.

| Input | Real Bigtable | Emulator |
|---|---|---|
| Cell timestamp not a multiple of 1000 | `INVALID_ARGUMENT`, the whole request rejected: every entry of the batch routed to the handler, nothing written | `INTERNAL` ("invalid timestamp 1234"), the offending entry only — the rest of the batch is written |
| Empty row key | `INVALID_ARGUMENT`, "Row keys must be non-empty" | **Accepted.** The row it stores then breaks the client's own read state machine ("rowKey missing"), a state the service cannot reach |
| Mutation naming a column family the table does not have | `NOT_FOUND`, reported for **every** entry of the batch, nothing written | `INTERNAL` ("unknown family"), the offending entry only |

The status is the deviation that matters. `INTERNAL` is [fatal](#error-handling) to this sink while
`INVALID_ARGUMENT` is routed, so an emulator test would conclude "fails the job" for a condition the
service makes droppable — the wrong lesson, learned cheaply. It is also why the emulator suite
asserts no rejection except in the class that exists to record these differences.

## Provenance and attribution

No code is copied from any other project. The serializer's shape — the `RowMutationEntry` return
type and null-means-skip — is adopted from
[google/flink-connector-gcp](https://github.com/GoogleCloudPlatform/flink-connector-gcp)
(Apache-2.0) so its users migrate mechanically, and Apache Beam's `BigtableIO` (Apache-2.0) was read
as a design reference for how a runner drives the bulk mutation batcher. Depending on the former, or
vendoring it, was evaluated and rejected on
[#33]({{< param BookRepo >}}/issues/33), which records the grounds.
