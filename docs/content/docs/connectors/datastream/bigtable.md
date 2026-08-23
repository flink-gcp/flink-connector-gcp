---
title: Bigtable
type: docs
weight: 40
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

# Bigtable Connector

Writes a DataStream into Cloud Bigtable, one row mutation per record, at-least-once, into a
fixed table or one each record names. Every
option is in the [Bigtable reference]({{< relref "docs/reference/bigtable" >}}); the runnable job is
the [quickstart]({{< relref "docs/quickstart/bigtable" >}}); implementation status is the table in
the module
[README]({{< param BookRepo >}}/tree/main/flink-connector-gcp-bigtable).

{{< java-snippet file="BigtableConnectorOverview.java" tag="bigtable-connector-overview" >}}

## What this connector is for

Bigtable is a wide-column store keyed by a single row key, and what a streaming pipeline usually
wants from it is a materialized view: the latest state per key, or an append-only history of events
under a key that a serving path reads by prefix. The sink is shaped for that — the record decides
its own row key and cells, and the sink's whole job is to get mutations there in bounded memory and
to make a checkpoint mean something.

Two properties of the service shape the design more than anything else. The mutations inside one
`RowMutationEntry` apply atomically to its row, while different entries do not form one atomic
operation. A write also carries a **cell timestamp**, which turns at-least-once from a duplicate
problem into a choice — the same mutation applied twice is either an overwrite or a second version,
depending on what the serializer set.

**Submission order is not a same-key last-write-wins guarantee.** The client batches entries into
`MutateRows`, whose contract permits arbitrary application order even between entries for the same
row; concurrent requests are unordered too. A 2026-08-11 real-service campaign observed zero
reversals in 86,196 mirrored same-row pairs, but that bounded observation does not replace the
contract. If dependent mutations need a defined winner, encode the version in the row key or split
them into writes whose completion is awaited before the next begins.
`batchElementCountThreshold(1)` only makes concurrent one-entry requests and does not establish
order. ADR-0093 records the measurement and decision.

## API notes

The record-to-mutation step is the whole public surface beyond the builder:

The following signature excerpt omits its package declaration, imports, and Javadocs.

```java
@Public
public interface BigtableSerializationSchema<T> extends Serializable {
    default void open(SerializationSchema.InitializationContext context) throws Exception {}

    @Nullable
    RowMutationEntry serialize(T element, SinkWriter.Context context) throws IOException;
}
```

Returning a `RowMutationEntry` rather than a narrower value type is deliberate: it is the client's
own mutation builder, so `setCell`, `deleteCells`, `deleteFamily`, `deleteRow` and the aggregate
`addToCell` and `mergeToCell` are all expressible, several of them per record, and the sink adds no
vocabulary of its own to learn.
The two aggregate mutations take a value model the client library has not settled, so a client
upgrade may move their argument types.
Returning `null` **skips** the record — it is written nowhere, is not a failure, and never reaches
the failed-mutation handler — which is how a filter that depends on the mutation being built belongs
in the serializer rather than upstream of the sink. Every serializer in this connector family reads
`null` that way. A skip is counted by [`recordsSkipped`](#metrics), the only thing that reports
it: a serializer skipping every record would otherwise leave an empty table under a green job.

The signature and the null-means-skip convention are shared with the `BaseRowMutationSerializer` of
[google/flink-connector-gcp](https://github.com/google/flink-connector-gcp), so a
serializer written against that connector ports by changing the interface name. Its built-in
`GenericRecord` and `RowData` serializers are deliberately not ported: `RowData` conversion belongs
to the [Table API layer]({{< relref "docs/connectors/table/bigtable" >}}), which supplies its own,
and an Avro convenience is additive whenever there is a use case asking for it.

`context` is Flink's write context, so `context.timestamp()` is the record's event time — usually
the right cell timestamp when the record carries none of its own.

`serviceAccountKeyFile(path)` authenticates every data client and the table auto-creation admin
with the service-account JSON key at `path`.
The file is read when each writer starts, so the same path must be readable on every eligible
TaskManager.
When the setter is absent, application-default credentials remain in effect, including
`GOOGLE_APPLICATION_CREDENTIALS`.
A read or parse failure reports neither the path nor credential material.
The option is rejected beside `emulatorEndpoint(...)`, whose channel carries no credentials.
See [Credential file deployment](#credential-file-deployment) before deploying a key file.

`emulatorEndpoint("host:port")` points the sink at a Bigtable emulator over a plaintext channel with
no credentials, so it must only ever be used against an emulator — never against production
Bigtable. The setter parses it into the host and port the client's emulator settings take, so a
malformed value is rejected by that call on the client rather than when the writer builds its client
on a task manager ([#235]({{< param BookRepo >}}/issues/235)).

## Credential file deployment

> **Authentication recommendation.**
> Google recommends [avoiding service-account keys whenever possible](https://cloud.google.com/iam/docs/best-practices-service-accounts#choose-when-to-use).
> Prefer keyless application-default credentials from an attached service account or Workload Identity.
> Use `serviceAccountKeyFile(path)` only when the job must select an explicit service account that the process environment cannot provide.
>
> On Kubernetes, store the JSON key in a `Secret` and mount it as a read-only volume at the same absolute container path in every pod that may load it.
> A sink needs the path on every eligible TaskManager; either source also needs it on the JobManager.
> This path is inside the container, not a path that merely exists on the Kubernetes node.
> Do not store credential material in a `ConfigMap`, SQL DDL, a savepoint or connector state.
> Mount the Secret directory rather than one file through `subPath` when in-place rotation is expected, because Kubernetes does not update a Secret mounted with `subPath`.
>
> On a session cluster, the same path must remain readable by every eligible JobManager and TaskManager process, including replacements and newly allocated TaskManagers.
> Each writer, reader or enumerator reads the file once when that runtime component starts.
> Replacing the mounted file does not hot-reload credentials in an already running component.
> Wait until a normally projected Secret has updated in every eligible pod before restarting the affected job; with a `subPath` mount, recreate the affected pods or cluster first.
> Replace the key in every workload that uses it and validate those workloads before disabling the replaced key.
> Monitor them after disabling it, then delete it after confirming that they still work, following Google's [service-account key rotation guidance](https://cloud.google.com/iam/docs/key-rotation#process).
>
> Mounting several job-specific keys into one shared session cluster weakens isolation because co-located jobs share the cluster environment.
> Prefer an application or per-job cluster with Workload Identity when jobs require separate identities.

## Per-record destinations

`table(...)` writes every record to one table. `destinationResolver(...)` names the table per
record instead, so one sink writes to many — a table per tenant, a table per day — the same shape
the BigQuery, Pub/Sub and Cloud Tasks sinks take. The two setters write the same field, so the last
one wins, and one of them is required.

The following builder excerpt omits the application serializer supplied to `serializer(...)`.

```java
Sink<OrderEvent> sink =
        BigtableSink.<OrderEvent>builder()
                .destinationResolver(
                        (event, context) ->
                                TableDestination.of("my-project", "my-instance",
                                        "orders-" + event.day()))
                .serializer(...)
                .build();
```

The resolver runs once per record, **before** the serializer, so a record the serializer then
rejects is still reported against the table it was headed for. It must be cheap and deterministic,
and it should return cached `TableDestination` instances when destinations repeat — a small
`Map.computeIfAbsent` keyed on the varying part is enough. Returning `null` fails the job: a
resolver that cannot name a table is a configuration error, not a bad record, so it is never handed
to the [failed-mutation policy](#failed-mutation-policy).

What a destination costs is a **bulk mutation batcher of its own**, because the client binds one to
one table. Under those batchers the sink keeps one client per (project, instance), shared by every
table of that instance, so a resolver spreading records over many tables of one instance multiplies
batchers rather than channel pools. A batcher is dropped once its table has gone
`writerOptions(...).destinationIdleTimeout(...)` without a mutation — one hour by default, swept at
the end of a checkpoint's flush when the batcher is already empty — and rebuilt transparently if
that table is written to again. So a resolver's *cardinality* is a sink resource decision, not only
a routing one.

The in-flight bounds are the writer's, summed across every destination rather than split among
them: `maxInFlightEntries` and `maxInFlightBytes` mean the same thing whether the sink writes one
table or fifty. What grows with the destination count is one accumulator per live batcher, on top
of those bounds.

By default the sink never **creates** a table or a column family, so both must exist. Opting into
[auto-creation](#table-auto-creation) requires declaring the schema, not only permitting the
creation ([#233]({{< param BookRepo >}}/issues/233)): a Bigtable table's schema is its column
families and their garbage-collection policies, which is exactly the part a sink cannot guess — and
unlike a topic, a table created bare would reject every mutation.

**Auto-creation beside a resolver is a different risk profile** from auto-creation of one fixed
table. One schema serves every table the sink creates — a resolver names tables, not schemas — and
a resolver that computes a table id from record data can invent one table per record, each an admin
RPC against an instance-level table limit. Prefer a resolver whose range you can state, and keep
`CREATE_NEVER` where you cannot.

## Table auto-creation

Off by default. `createDisposition(CREATE_IF_NEEDED)` opts in, and requires
[`tableCreateOptions(...)`]({{< relref "docs/reference/bigtable" >}}#tablecreateoptions) naming at
least one column family — the disposition says the sink *may* create, the options say *what*, and
the builder rejects each without the other:

{{< java-snippet file="BigtableConnectorTableAutoCreation.java" tag="bigtable-connector-table-auto-creation" >}}

Creation is **reactive**, the shape the
[Pub/Sub sink]({{< relref "docs/connectors/datastream/pubsub" >}}#topic-auto-creation) uses: no
admin client is even constructed unless a mutation actually fails with `NOT_FOUND`. When one does,
the failed mutations are *parked*, the sink ensures the table and its declared families exist —
idempotently, so parallel subtasks race safely; a lost race falls through to adding whatever
families are still missing, in one atomic request, re-reading at most once more per family it
declares — and then re-applies the parked mutations,
retrying on a jittered backoff (the
[`recovery*` knobs]({{< relref "docs/reference/bigtable" >}}#bigtablewriteroptions): 500 ms
doubling to 10 s, at most 10 attempts, ±25% jitter so subtasks resuming against the same fresh
table do not re-apply in lockstep). The repair runs before the next record and inside every
`flush()`, so a completed checkpoint never leaves a mutation parked; a repair that exhausts its
budget fails the job with the incident's cause. A creation that itself fails spends attempts from
the same budget — the admin client retries neither of its RPCs, so this schedule is what stands
between one transient admin failure and a restart, and it is where an ensure whose declared
families keep vanishing between the read and the addition ends up as well, rather than in a loop
whose only symptom would be checkpoints that stop completing. Nothing is ever dropped by the repair — a mutation
is re-applied or the job fails — which is why `NOT_FOUND` may be acted on even when an outage
status arrives beside it.

**Creation only, per family.** An existing table is used as it is; the families declared in the
options that it lacks are added, with their rules, and an existing family's garbage-collection
rule is neither compared nor updated. The one condition creation cannot repair is a mutation
naming a family the options do not declare. After ensuring the table, the sink compares a
missing-family response with the entry and the families the ensure observed; an absent referenced
family fails immediately with the table and family named instead of spending the remaining
recovery attempts. A `NOT_FOUND` that does not specifically identify a missing family keeps the
existing bounded retry behaviour ([#432]({{< param BookRepo >}}/issues/432)).

**The garbage-collection rule is the decision that matters.** This sink is at-least-once: a replay
whose serializer sets no explicit cell timestamps writes duplicate cell versions, and the family's
rule is what decides whether those accumulate forever. A family declared without a rule keeps
Bigtable's default of collecting nothing;
`GcRule.union(GcRule.maxVersions(1), GcRule.maxAge(...))` is the usual shape for keeping only the
latest cell. The [reference]({{< relref "docs/reference/bigtable" >}}#tablecreateoptions) lists the
four rule shapes.

Under the default `CREATE_NEVER` a `NOT_FOUND` stays **fatal** — a missing table fails every
record alike, so it must never reach a handler that may drop records — and the failure names the
disposition, so the reader meeting it learns the knob that changes it. Auto-creation needs the
`bigtable.tables.create` and `bigtable.tables.update` permissions (`roles/bigtable.admin` carries
both) on top of the data-plane role; a job whose table exists never exercises them.

Two caveats. The repair happens inside `write()`/`flush()`, so an incident's backoff extends
checkpoint duration by up to the recovery budget — about a minute at the defaults. And the sink
creates tables, never instances — provisioning an instance is capacity planning, not schema — so a
job pointed at a missing instance fails when the repair's own creation is refused.

## Delivery guarantees and state

See [Write and key-collision semantics]({{< relref "docs/connectors/delivery-guarantees" >}}#write-and-key-collision-semantics)
for the Table and DataStream API comparison.

**At-least-once.** The writer is stateless — it stores nothing in Flink state — and `flush()` runs
at every checkpoint barrier: it sends what the client has buffered and then waits until every
outstanding mutation has been acknowledged. So a completed checkpoint means Bigtable has applied
every record up to the barrier — other than those the [serializer skipped](#api-notes), which are
written nowhere by design — and discarding operator state can never lose sink-buffered records.

That guarantee assumes the default `FailureHandler.failJob()` policy. Under `logAndDrop()` or
`sendToDeadLetterQueue(...)` a completed checkpoint means every record up to the barrier was either
applied, [skipped by the serializer](#api-notes), or handed to the
[failed-mutation policy](#failed-mutation-policy), which says which failures reach it.

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
inferred. Worth reading before running a dropping policy: a job that multiplies the timestamp
wrongly produces the violation on *every* record, which is the case that puts the sink into the
[isolation pass](#error-handling) for the whole stream and costs it its batching.

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

**Every count among them counts entries, not mutations.** An entry is one `RowMutationEntry` — one
record the serializer returned — and it carries as many mutations as the serializer put `setCell`
calls in it, so a job writing ten cells per record puts ten mutations behind each unit these knobs
count. Bigtable's own documented limit is stated in the other unit: no more than
[100,000 mutations](https://cloud.google.com/bigtable/quotas) in a batch. **The two never have to be
reconciled by a job**, because the client enforces the mutation limit itself and unconditionally: it
flushes the accumulated batch as soon as one more entry would carry it past 100,000 mutations,
whatever `batchElementCountThreshold` says, and refuses to build a single entry carrying more than
that on its own. So no setting of these knobs produces an over-limit request, and a test pins both
facts so that a client upgrade moving either one goes red.

**The batch thresholds** (`batchElementCountThreshold`, `batchRequestByteThreshold`) are handed to
the client and decide when it sends a batch. Both are unset by default, which leaves the client's
own values (100 entries, 20 MiB, and a one-second timer) in place — recorded in the reference for
sizing rather than restated in this project's code, so a client upgrade that retunes them is
inherited. A batch goes out on whichever of five conditions arrives first: those two, the
one-second timer, the client's 100,000-mutation guard, and a full writer sending every batcher.
Any claim of the form
"setting `batchElementCountThreshold` to *N* makes batches of *N*" has to name the condition that
*binds*, or it is false — no batch ever holds more than `maxInFlightEntries` entries whatever this
knob says, because an entry counts as unacknowledged from the moment the batcher accepts it, so
what a batcher is still accumulating is part of a total the writer stops admitting past.

**The in-flight bounds** (`maxInFlightEntries`, `maxInFlightBytes`) are the writer's own, and they
are what backpressures the stream: at either cap `write()` yields to the task mailbox until
completions bring the counters down. Both are needed — an entry may be megabytes, so a count alone
bounds no memory. Admission is checked as "below the cap", never as "does this entry fit", so an
entry larger than the byte cap is admitted on an empty writer and overshoots it until it
completes; that is deliberate, because such a wait ends only when a completion arrives and none can
with nothing in flight, which would make a fits-predicate a task hang rather than backpressure.

**The client's own flow controller is why raising the bounds has a ceiling.** It permits 20,000
outstanding entries and 100 MiB of accumulated size, and when either is reached it *blocks* the
calling thread — which is Flink's task thread, the one that has to stay free to run mailbox mails
and checkpoint barriers. Its static limits are not settable through the client's public API (only
latency-based throttling can be turned on and off), so the sink's answer is to keep its own bounds
below them: the defaults are, and a much larger `maxInFlightEntries` simply moves the effective
bound into the client, where it stalls instead of backpressuring. This is the same defect class the
Pub/Sub sink removed its SDK flow-control knobs over
([#85]({{< param BookRepo >}}/issues/85)).

**It is also where the batch thresholds' ceilings come from — 19,999 entries and 100 MiB − 1
byte.** The client's settings builder requires each threshold to stay *strictly* below the matching
flow-control budget and refuses to build a client at all otherwise, so a job configured past either
one does not get a bigger batch; it dies on the task manager as the writer opens, reported as
`Failed to create a Bigtable mutation batcher`. Rejecting those values at the setter is what turns
that into a message at submission ([#436]({{< param BookRepo >}}/issues/436)).

**The in-flight bounds are warned about at the same two figures, not capped.** An
`maxInFlightEntries` above 20,000 or a `maxInFlightBytes` above 100 MiB still describes a working
job — what changes is which layer bounds it — so `build()` logs a `WARN` naming the value and the
cost rather than refusing it. Refusing would be wrong: that budget is per *client*, and this sink
holds one per (project, instance), so a resolver spreading records over several instances draws on
several budgets and can legitimately want a writer-global bound above one of them. Nothing at
`build()` knows how many instances a resolver will name, which is exactly why this one is advice
and the batch thresholds' ceilings are not.

There are no rate knobs beyond this. Bigtable's throughput is a property of the instance's nodes and
of how well the row keys spread across tablets; a sink-side rate limit would not change either.

**A sink whose client has stopped answering says so in the log, and nothing else does.** Both waits
— the admission gate in `write()` and the drain at a checkpoint — emit a `WARN` naming the
connector, the wait, the in-flight entry count and the number of live tables once a minute has
passed with no mutation answered, repeating no more than once a minute however many waits the writer
makes. There is no knob and no sink-side timeout, because the client already has one: it gives up on
a stalled `MutateRows` at its own 10-minute total timeout (measured: 10 min 1 s against an endpoint
that accepts and never answers, 9 min 46 s against one that refuses the connection). The warning
exists because of what happens in those ten minutes — no counter moves, since `numRecordsSend` only
counts what was sent and a mutation that never answers is never counted as a failure — and because
of what happens at the end of them: with Flink's defaults
(`execution.checkpointing.timeout` 10 min, `execution.checkpointing.tolerable-failed-checkpoints`
0) the checkpoint can expire first, failing the job with a message that names nothing about
Bigtable. See [#431]({{< param BookRepo >}}/issues/431).

## Error handling

Failures are classified on the task thread — mutation completion callbacks re-dispatch onto Flink's
mailbox, so the writer's state is touched from one thread only — and routed by class:

| Class | Examples | Behavior |
|---|---|---|
| Row-level | `INVALID_ARGUMENT` — a cell timestamp that is not a multiple of 1000, an empty row key | Routed to the configured [failed-mutation handler](#failed-mutation-policy) once confirmed against the one mutation (below); applying the same mutation again could not succeed |
| Missing table | `NOT_FOUND` — the table or one of its column families does not exist | [Repaired](#table-auto-creation) under `CREATE_IF_NEEDED`; fatal under the default `CREATE_NEVER`, with the disposition named in the failure |
| Fatal | `PERMISSION_DENIED`, `UNAUTHENTICATED`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`; an outage the client's own retries gave up on (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, the two it retries) or a contended or overloaded service it does not retry at all (`ABORTED`, `RESOURCE_EXHAUSTED`); failures carrying no status at all | Fail the ongoing write or checkpoint |

The row-level examples are the ones measured against the service, and they are the whole list this
page will vouch for — see [what the gated suite measures](#testing). Two conditions that read like
`INVALID_ARGUMENT` candidates are not: an entry carrying more than 100,000 mutations, or more than
200 MiB of them, is rejected by the **client**, before any RPC, so it arrives as a serialization
failure rather than as a service rejection (see
[serializer failures](#failed-mutation-policy) — such a `FailedMutation` carries no entry and no row
key). The check sits in the mutation list itself, so it covers `deleteCells` and `deleteRow` exactly
as it covers `setCell`.

The split's purpose is that a *dropping* handler never sees a condition. An outage would otherwise
bleed the stream one mutation at a time instead of backpressuring it, and a missing column family —
which fails every record alike — would empty the whole stream into the dead-letter destination under
a green job. `NOT_FOUND` is checked ahead of everything else in the chain, because acting on it is
safe where a drop would not be: the [repair](#table-auto-creation) re-applies and never discards,
and under `CREATE_NEVER` the outcome is a job failure either way.

**A rejection is confirmed against one mutation before it is routed.** Bigtable may reject a whole
`MutateRows` request rather than the entry that provoked it, and the client then fails every entry of
that batch with the same status — measured against the service, one bad record written beside a good
one had **both** futures fail with the same `INVALID_ARGUMENT` and neither row written. Routing on
that report would hand a dropping handler a whole batch for one bad record.

So a row-level rejection answering a batched request is *parked* rather than routed, and the sink
runs an **isolation pass**: each parked mutation is re-submitted as the only entry of its own
request, so the service answers it alone. One that succeeds was collateral damage and is now
applied; one rejected again is the mutation the service really refused, and only that one reaches
the handler. The pass runs before every checkpoint completes and again as soon as the next record is
written, so nothing waits in the park across a checkpoint —
[`parkedEntries`](#metrics) is what reports its depth.

The cost is a real one and worth planning for: while isolating, the sink spends roughly one request
per record, so a stream with frequent rejections loses the batching it normally gets. That is the
price of not discarding the records batched with a bad one
([#239]({{< param BookRepo >}}/issues/239)).

Under the default `failJob()` policy that cost is bounded by the failure itself — the first
confirmed rejection fails the job, so the pass isolates one mutation and stops. It is a **dropping**
policy that pays it, and what ends the pass there is
[`maxConsecutiveRejections`]({{< relref "docs/reference/bigtable" >}}#bigtablewriteroptions)
([#361]({{< param BookRepo >}}/issues/361)): a dropping policy is a decision to keep running
through *anomalous* records, and a stream being refused wholesale is not that — it is broken data
degraded to one request per record under a green job — so once that many confirmed rejections
arrive in a row, with not one successfully applied mutation between them, the job fails with a
message naming the option, the count and the last rejection's status. Every mutation rejected up to
that point — the tripping one included — was routed to the handler first; what a handler had
durably delivered by then follows the `FailureHandler` contract's own checkpoint-contingent
guarantee. Any applied mutation resets the count — an occasional bad record can never accumulate into a
failure — and only rejections the pass has *confirmed* count: records the serializer rejects say
nothing about the service's view of the stream. The `-1` sentinel removes the bound for a pipeline
that really does want to trickle through arbitrarily bad data.

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

**A failure the sink has already acted on is not reported a second time when the task closes.** The
client's batcher accumulates every entry failure of its lifetime and re-states all of them as it
shuts down, which consuming a mutation's own future does not clear — so the sink absorbs that report
and logs it rather than letting it fail a job the configured policy had deliberately kept running.
Logging is also all that can be done with a failure that *first* appears during close, carried by a
batch the shutdown itself sent: Flink stops accepting mailbox work before it closes operators, so
the completion that would classify and route such a failure can no longer run, and that log line is
its only record. The mutation itself is covered by at-least-once, since a close with unsent work
only happens on a path that is already ending the job.

### Failed-mutation policy

Two data-shaped failures are pluggable: a record the serializer rejects, and a row-level rejection.
A record the serializer *skips* by returning `null` is neither: it is not a failure, so it never
reaches the handler and is counted by [`recordsSkipped`](#metrics) rather than
`numRecordsSendErrors`.
The policy is `failedMutationHandler(...)`, taking the shared `FailureHandler<FailedMutation>` SPI
from `flink-connector-gcp-base` ([#37]({{< param BookRepo >}}/issues/37) standardizes it across the
connectors in this repository):

{{< java-snippet file="BigtableConnectorFailedMutationPolicy.java" tag="bigtable-connector-failed-mutation-policy" >}}

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

`PubSubDeadLetterQueue`, this repository's one shipped implementation, reports what it published,
what it still holds and how long its waits take on **this sink's** writer
group — documented once, with the queue, under
[Dead-letter metrics]({{< relref "docs/connectors/datastream/pubsub" >}}#dead-letter-metrics).

`PubSubDeadLetterQueue.builder().serviceAccountKeyFile(path)` selects credentials for the dead-letter
publisher independently of this Bigtable sink's credentials.
Each sink writer reads the file when it opens the queue, so the path must be readable on every
TaskManager that can run the sink.
If the setting is absent, the queue uses application-default credentials.
The Pub/Sub [credential file deployment]({{< relref "docs/connectors/datastream/pubsub" >}}#credential-file-deployment)
note covers Kubernetes Secret mounts, session clusters and rotation.

Watch [`numRecordsSendErrors`]({{< relref "docs/connectors/datastream/bigtable" >}}#metrics) rather
than the job status when running anything other than `failJob()`: it counts every mutation the
handler received. A **serializer** bug rejecting every record shows up only as a rate — serializer
rejections never count toward
[`maxConsecutiveRejections`]({{< relref "docs/reference/bigtable" >}}#bigtablewriteroptions), since
they say nothing about the service's view of the stream — where a stream the *service* refuses
wholesale fails the job at that bound. It counts records rather than batches: a rejection is
[confirmed against one mutation](#error-handling) before the handler sees it.

## Source

Reads the rows of a table into a `DataStream`.

{{< java-snippet file="BigtableConnectorSource.java" tag="bigtable-connector-source" >}}

API notes:

- The scan is **bounded**: the configured ranges are read once and the source finishes. That is not
  the same as batch-only — a bounded source runs inside a streaming pipeline and simply ends, which
  is what makes reading a Bigtable table and joining it against an unbounded stream work.
- `table(...)` takes the same `TableDestination` the sink does, and `appProfileId(...)` is a source
  option for the same reason it is a sink one: it chooses a path to the data, not the data's
  address.
- `serviceAccountKeyFile(path)` is read by the JobManager when the enumerator plans or restores
  splits and by each TaskManager when its reader starts.
  The key authenticates both `SampleRowKeys` and `ReadRows` clients.
  It is rejected beside `emulatorEndpoint(...)`; when absent, ADC remains in effect.
- Reading and writing this table from SQL exist today, on the [Bigtable SQL
  connector]({{< relref "docs/connectors/table/bigtable" >}}) page: its `ScanTableSource` maps onto
  this source, and its [lookup joins]({{< relref "docs/connectors/table/bigtable" >}}#lookup-joins)
  serve point lookups over the same table.
- Every option is listed, with its default, in the
  [configuration reference]({{< relref "docs/reference/bigtable" >}}#bigtablesourcebuilder).

### Splits, ranges and recovery

**A split is one row-key range, and the range is the work that is left.** The enumerator asks
Bigtable where the table's sections begin (`SampleRowKeys`), cuts each configured range at every
boundary that falls strictly inside it, and hands the pieces out one per request — a reader asks
for the next when it finishes one, so a dense range does not hold up a subtask that could be reading
another.

A checkpoint **truncates** the range to start just past the last row successfully deserialized,
including one that produced no output, so a restore resumes rather than replays. `ReadRows` has no
row offset to resume at, only a range to ask for, which is what makes the range and not a count the
unit of progress. Delivery is
**at-least-once**: rows emitted after the last completed checkpoint are read again after a restore.

**Parallelism is the service's decision, not the job's.** The boundaries come from how Bigtable
stores the table, so a table held in few tablets is read by few subtasks whatever the parallelism —
the subtasks left without a split finish immediately, and the enumerator logs a warning naming both
numbers. Splitting more finely than a tablet is not something the read path can do.

A restore does **not** sample the table again. Tablets split and merge while a job runs, so a second
sampling would name different ranges under the split ids the readers are already holding.

**How a range is written in a log line or an error.** Both this source and the Change Streams source
render ranges as `[start, end)`, where `[` and `]` include the key, `(` and `)` exclude it, and `*`
stands for a bound the range does not have — so `(*, row-9)` is everything below `row-9` and
`(*, *)` is the whole table. A key is shown as text where it is printable, and any other byte as
`\xNN`. Three printable bytes are also shown escaped, because each means something in the notation
itself: `\x5c` for a backslash, `\x2a` for `*`, and `\x2c` for a comma. That last one is what keeps
`[a\x2c b, c)` — the range from `a, b` to `c` — from reading as a range from `a` to `b, c`. Two
different ranges therefore never print the same way, which is what lets you tell two of them apart
in a warning; the rendering is for reading, though, and the connector never uses it to decide
whether two ranges are the same.

### Push-down: ranges, prefixes and filters

`rowRange(...)` and `prefix(...)` are repeatable and additive; with none set the whole table is
read. A `prefix` is sugar for the range it describes, converted by the client library so that an
all-`0xFF` prefix — which has no successor — becomes a range running to the end of the table.

Overlapping ranges are **merged**, not rejected. Two nested prefixes are easy to write by accident,
and left alone the rows they share would land in two splits read by two subtasks, so a single
*successful* run would emit them twice. An **empty** range is rejected instead, at `build()`: a
range that reads nothing under a green job looks exactly like a job with nothing to read.

`filter(...)` takes one `Filters.Filter` and applies it to every split. That is safe by
construction, and the reason is worth stating: Bigtable's filter language has no row-count limiter —
its limit and offset filters count cells *within* a row — so nothing expressible through a filter
can depend on how the key space was divided. Per-cell shaping is all expressible there too: which
families and qualifiers to return, which timestamp window, how many versions of a cell. A filter is
also the cheapest thing a scan can carry, since what it excludes never leaves the server.

A filter naming a **column family the table does not have** fails the read with `NOT_FOUND` — the
service checks the family against the table's schema rather than matching it against nothing
(measured 2026-08-10, [#481]({{< param BookRepo >}}/issues/481)). The job fails loudly instead of
finishing empty, and the source deliberately does not pre-validate a filter's families: that would
cost every scan a metadata read to soften an error the service already reports precisely.

### Deserialization

`BigtableRowDeserializationSchema<T>` turns a row into **zero or more** records through a
`Collector`. A Bigtable row is a whole row — many column families, many qualifiers, many timestamped
cell versions — so fanning one out into a record per qualifier or per cell is a mapping wide-table
jobs want. Emitting nothing filters the row: it is not a failure, it reaches no handler, and
`recordsSkipped` is the only thing that reports it.
Every collected record must be non-null and emitted synchronously during the deserialization call.
Do not retain the collector or use it from another thread.

The other row-oriented source SPIs use the same zero-to-many collector contract.
A successfully deserialized row producing none or five advances input progress by exactly one row
either way: this source resumes at a row *key*, BigQuery stores a consumed-input-row count, and
Spanner replays an interrupted partition from its start.
If deserialization or downstream collection fails, the row-key progress does not advance; a retry
can therefore duplicate an output emitted earlier from that same row before a later output failed.

Records are emitted **without a timestamp**. A Bigtable row has one per cell rather than one per
row, so any row-level event time would be a choice the connector made on the job's behalf; assign a
watermark strategy over the records instead.

### Serverless reads with Data Boost

Data Boost is Bigtable's read-only serverless compute, selected **by the application profile** — so
a job points at it with `appProfileId(...)`, exactly as it would name any other profile. There is
no separate switch, and there is nothing for the connector to validate: the profile is the
server-side source of truth, and asking what kind a profile is would need admin permissions a read
job should not have.

Its eligible methods are `ReadRows`, `SampleRowKeys` and `PingAndWarm` — this source's whole RPC
set, so both the scan and the split planning behind it are compatible with such a profile.

Three things to know before pointing a job at one.

**Recent writes may not be readable.** Bigtable documents no guarantee for data written less than
**35 minutes** before the read, so a pipeline that writes with this sink and reads back through a
Data Boost profile may not see its own recent work. That is a stronger statement than "the read is
a little stale": the data is not merely old, it may be absent.

**Eligibility is a property of the traffic, not only of the profile**, and a parallel source is
exactly the shape that can lose it: traffic above **1,000 read requests per second per cluster** is
reported as *ineligible* rather than rejected, so the symptom is a bill and a metric
(`data_boost/ineligible_reasons`) rather than an error. Reverse scans are ineligible too; this
source issues none.

**A Data Boost profile is read-only**, and takes single-cluster routing with no request priority:
naming one on the sink's `appProfileId` breaks writes.

**This project has not exercised Data Boost.** It needs an Enterprise-edition instance and SPU
billing; the verification is [#248]({{< param BookRepo >}}/issues/248). What is covered here is that
a configured `appProfileId` reaches the client, which the gated real-GCP suite asserts.

### Not here yet

- `Query.limit()`, a global row limit. It cannot be partitioned across splits without coordination,
  and the client library says the same thing from the other side by refusing to shard a query that
  carries one.
- Read-ahead and paging knobs. How many rows one fetch hands to the task thread is a fixed internal
  bound — a correctness floor that lets a checkpoint land inside a long range — rather than a knob,
  and turning it into one needs a measurement rather than a preference.

## Change Streams source

`BigtableChangeStreamSource` is a separate FLIP-27 source because `ReadChangeStream` has a moving
partition topology and continuation-token checkpoints rather than the bounded scan's row ranges.

{{< java-snippet file="BigtableConnectorChangeStreamSource.java" tag="bigtable-connector-change-stream-source" >}}

The example permits at most four open `ReadChangeStream` RPCs in each of three source subtasks.
Its configured job-wide read capacity is therefore `3 * 4 = 12`.
This number bounds connector activity; it is not a Bigtable quota or a statement about how many service partitions a table has.

Source parallelism distributes the service partition set across Flink subtasks, while `maxConcurrentStreamsPerSubtask` bounds open RPCs and their callback state in each subtask.
Increasing source parallelism can reduce the reads and resource use in one subtask, provided the Flink deployment has enough task slots and TaskManagers to run that parallelism.
Adding slots or TaskManagers alone does not change partition concurrency when source parallelism stays fixed.
A parallelism change takes effect through a checkpoint or savepoint restart, when Flink redistributes the checkpointed partition splits.

The source converts the client library's internal record into the connector-owned immutable
`BigtableChangeStreamMutation` before invoking the deserializer.
With output filters, only retained SDK entries are converted; an explicitly skipped empty
projection is not materialized as a public mutation.
With no output filter, filter evaluation is bypassed, although the public-model conversion still
occurs.
The model preserves the row key, mutation type, source cluster, commit timestamp, tie breaker,
continuation token, estimated low watermark, and every ordered `SetCell`, `DeleteCells`,
`DeleteFamily`, `AddToCell`, and `MergeToCell` entry the client returns.
Mutations are immutable and compare by value, so two of them can be compared, put in a set, or
asserted on directly.
Each entry reports which of those five it is through `Entry.getKind()`, and the `Value` an
`AddToCell` or `MergeToCell` entry carries reports its own through `Value.getType()`, so a
deserializer can tell them apart without a chain of `instanceof` checks — reading a subtype's own
fields still needs the cast that the kind has just made safe.
Both hierarchies have private constructors, so the subtypes listed above are the complete set and
no entry arrives that is none of them.
They deliberately have no `toString`: the row key and the cell values are your own table data, and a
value type that renders them is one accidental log line away from putting that data somewhere it
does not belong.
Print what you choose through the accessors.
Its Flink type information selects the connector serializer even when a later transformation asks
for `TypeInformation.of(BigtableChangeStreamMutation.class)`.
An explicit `.returns(...)` remains useful when a transformation erases its declared output type:

{{< java-snippet file="BigtableConnectorChangeStreamTypeInformation.java" tag="bigtable-connector-change-stream-type-information" >}}

Family and qualifier filters project entries after a complete mutation crosses the service and
reader protocol boundary, but before the application deserializer runs.
They do not reduce `ReadChangeStream` network traffic.
Each Java regular expression uses full-match semantics.
A qualifier expression matches `family:qualifierBase64`, where `qualifierBase64` is canonical
padded RFC 4648 standard Base64; for example, qualifier bytes `temporary` become
`dGVtcG9yYXJ5`.
Family-delete entries have no qualifier and are governed only by the family filter.
Include and exclude lists are mutually exclusive within each dimension, while a family filter and
a qualifier filter compose by intersection.

Filtering preserves entry order and every mutation-level field.
If it removes every entry, the default delivers the mutation with an empty entry list so the
atomic row mutation remains observable.
`skipMessagesWithoutChange(true)` instead bypasses the deserializer for that mutation.
Both dispositions advance the continuation token and estimated low watermark, so filtering does
not change checkpoint, restore, partition-transition, or heartbeat progress.
The filter configuration belongs to the submitted source, not to checkpointed split state.
A failure restart of the same job keeps that configuration, while a newly submitted job restoring
a savepoint applies its new filter configuration to the restored split progress.
These entry-projection setters belong to the DataStream mutation API and have no Table API or SQL
option; SQL Change Streams preserve their documented envelope or selected-cell changelog contract.

The application profile is required and must use single-cluster routing. The Bigtable emulator
does not implement Change Streams, so this builder deliberately has no emulator option.

`serviceAccountKeyFile(path)` is read by the JobManager coordinator and by each TaskManager reader.
The coordinator shares one provider among its data, table-admin and instance-admin clients; the
reader shares one provider between streaming and restore-time table metadata calls.
When the setter is absent, ADC remains in effect.
The same path must therefore be mounted on every eligible JobManager and TaskManager process.

Fresh jobs default to `StartPosition.latest()`. Restores use the exact checkpointed continuation
token and estimated low watermark instead. If that position has fallen outside the table's
retention, restore fails by default; `resumeFallback(...)` explicitly accepts the resulting gap and
restarts the affected partition without its stale token. The check covers every restored position:
assigned and unassigned partitions, pending merges, and the reconciler's checkpointed
missing-partition ledger. A bounded run's completed ranges are restored without one, because a
range that has already been read to the end time is never resumed from.
`boundedTimestamp(...)` makes the source bounded.

Each mutation is handed to `BigtableChangeStreamDeserializationSchema` and may produce zero or more
records. Every produced record carries the mutation's commit time as its Flink timestamp. A
heartbeat produces no record but advances the checkpointed continuation token and low watermark.
`CloseStream` transfers successor partitions and their tokens to the coordinator.
Every collected record must be non-null and emitted synchronously during the deserialization call;
retaining the collector or using it from another thread is invalid.

Each subtask uses asynchronous `ReadChangeStream` calls with manual inbound flow control and one shared bounded handover budget.
Callbacks may receive a continuation token before the task thread consumes its record, but checkpoints retain only the position the task thread has emitted.
When assigned partitions outnumber the subtask limit, the reader queues the excess and rotates stable streams at service heartbeats so every queued partition can open or reopen from its checkpointed position.
Connector-initiated rotation and shutdown cancellation do not complete or fail a partition.
Every request asks for a five-second heartbeat, so a partition queued behind others waits several rotations, and a stream still draining responses rotates only once its next heartbeat reaches the task thread.
That interval is not configurable, and the `heartbeatInterval` of the Spanner Change Streams source has no counterpart here; [ADR-0103]({{< param BookRepo >}}/blob/main/docs/adr/0103-the-bigtable-change-stream-reader-bounds-asynchronous-partition-reads.md) records why.

The coordinator also compares the live service keyspace with its checkpointed assigned,
unassigned, and pending-merge ledger every 10 seconds. A missing partition is checkpointed with
its first-observed time. Compatible parent tokens may reconstruct it after two minutes; only after
20 minutes without a complete token set does the connector restart the remainder at its tracked
low watermark, emit a WARN naming that ledger, and increment `changeStreamTokenlessRestarts`.
These intervals are internal protocol constants rather than public tuning options.

Under `boundedTimestamp(...)` the comparison also counts the ranges the run has already read to
that end time. A bounded partition ends by closing without a successor, which retires its range
from the live ledger, while the service goes on reporting that keyspace for as long as the table
exists. Those completed ranges are checkpointed beside the rest of the ledger.
A bounded run neither reports its own finished work as missing nor re-reads it.
A restore resumes with the same account of what is left to do.

A tracked low watermark that falls before one minute inside the retention window is moved forward
to that point, because the service answers no request for a position it no longer retains. Such a
restart skips the changes between the position it tracked and the one it starts from, and nothing
tells the two apart: `changeStreamTokenlessRestarts` counts a clamped restart and an unclamped one
alike, and the WARN names the position the restart used rather than the one it replaced. An
expired restored position cannot reach this clamp, because the restore-expiry check fails the job
or applies `resumeFallback(...)` before the reconciler runs; the clamp covers only a position that
ages past retention while the job runs.

Change Streams cover every column family. Garbage-collection changes can therefore appear beside
application writes. Bigtable preserves the service's per-partition order, but records from
different partitions have no connector-imposed global order. Delivery is at least once across a
failure between downstream emission and the next completed checkpoint, so side effects should be
idempotent. A savepoint is the supported cross-job handoff: starting a separate job at a wall-clock
time is not an exact continuation-token transfer.

Watermark generation remains the job's `WatermarkStrategy`; the source does not emit Bigtable's
estimated low watermark as a Flink watermark.
Bigtable describes that value as an estimate and explicitly permits a future record with an older
commit timestamp, so even a minimum across every active, queued, unassigned, pending-merge and
missing partition would not prove Flink's non-early watermark contract.
The estimate remains checkpoint progress and a reader metric for lag monitoring.

An application can replace `WatermarkStrategy.noWatermarks()` with its own bounded-out-of-orderness
and idleness policy over the commit timestamps attached to emitted records.
That policy is an application-owned completeness/latency trade-off rather than a Bigtable guarantee:
the service publishes no finite maximum lateness, and connector concurrency can leave a partition
queued or unassigned before Flink has a split output for it.
Configure downstream allowed lateness or late-data routing for records that violate the chosen
assumption.
The decision and the difference from Spanner's heartbeat contract are recorded in
[ADR-0109]({{< param BookRepo >}}/blob/main/docs/adr/0109-bigtable-change-stream-estimates-do-not-become-native-source-watermarks.md).

## Metrics

Registered on the sink writer's metric group, one set per subtask:

| Metric | Type | Meaning |
|---|---|---|
| `numRecordsSend` | counter (Flink standard) | records handed to the client library for application |
| `numBytesSend` | counter (Flink standard) | their serialized size |
| `numRecordsSendErrors` | counter (Flink standard) | records routed to the failed-mutation handler |
| `recordsSkipped` | counter | records the serializer skipped by returning `null` — neither sent nor failed |
| `inFlightEntries` | gauge | entries the service has not acknowledged, against `maxInFlightEntries` |
| `inFlightBytes` | gauge | their serialized size, against `maxInFlightBytes` |
| `parkedEntries` | gauge | entries held for [the isolation pass](#error-handling) or the [auto-creation repair](#table-auto-creation) |
| `errorClass.CODE.errors` | counter | failed mutations by status code, `CODE` being a gRPC status name or `UNCLASSIFIED` |
| `tablesCreated` | counter | tables the [auto-creation repair](#table-auto-creation) created, declared families included |
| `columnFamiliesAdded` | counter | families the repair added to an already-existing table |
| `destination.TABLE.recordsSend`, `destination.TABLE.sendErrors` | counter | the same two counts per table, **only** with `perDestinationMetrics(true)` |

**`numRecordsSendErrors` is the counter to watch when the handler is not `failJob()`.** It counts
exactly what reached `failedMutationHandler(...)` — a record the serializer rejected, and a mutation
the service answered `INVALID_ARGUMENT` — whether the handler then dropped it or failed the job. A
serializer bug that makes *every* record invalid is dropped one at a time under a dropping policy,
and this counter is what shows it while the job stays green.

**`parkedEntries` is what to watch beside it.** As
[the error-handling section](#error-handling) describes, a row-level rejection reported against a
whole batch is held for the isolation pass rather than routed, and a `NOT_FOUND` under
`CREATE_IF_NEEDED` is held for the repair; this gauge is the only thing that reports those
entries: they have already left `inFlightEntries` and have not yet reached the handler. It is a
*transient* reading rather than a backlog — both passes empty their park at the next record or the
next checkpoint, whichever comes first — so what a dashboard shows is how often a sample catches
the writer mid-isolation or mid-repair. Frequent non-zero samples mean the sink is spending its
requests one entry at a time, or waiting out a recovery backoff. A batched rejection is
deliberately *not* counted under `errorClass.INVALID_ARGUMENT.errors`, so that counter reports
records the service refused rather than the batch sizes they travelled in; a parked `NOT_FOUND`
**is** counted under `errorClass.NOT_FOUND.errors`, per entry, because a missing table leaves no
identity to confirm — every entry genuinely failed on it, and each failed re-application during a
repair is a further give-up.

**`tablesCreated` and `columnFamiliesAdded` split first contact from schema drift.** A created
table's families ride along in `tablesCreated`; `columnFamiliesAdded` counts only families the
repair added to a table that already existed — the signal that what a job declares and what the
table holds had drifted apart. Both are registered whatever the disposition, so a `CREATE_NEVER`
dashboard reads zeroes rather than holes.

**`errorClass` does not measure retry volume here.** Cloud Tasks owns its retries, but its error
classes still describe failed attempts rather than retry count: first failures count, and a retry
selected from a nested status is classified under the outer status. This connector leaves retrying
to the client (see [Retries belong to the client](#retries-belong-to-the-client)), which retries per
entry inside the batcher and surfaces nothing until it gives up. Every code counted here is
therefore a mutation the client already exhausted its budget on — `UNAVAILABLE` here means an outage
that outlasted ten minutes of backoff, not one slow call. The client's own retry attempts are
invisible to this sink, and no metric in this table reports them.

**`numRecordsSend` counts records, not attempts**, as it does in every connector here, which is what
makes the number comparable across them. Retries inside the SDK cost nothing to exclude, since they
never surface; the one re-entered call site is the isolation pass, and a mutation it re-submits was
counted by the write that admitted it. The
consequence is the same one the other pages state — `numBytesSend` is payload volume rather than
wire volume, since a mutation the client retried three times moved three times its size.

**`perDestinationMetrics` is off by default, and should stay off for a resolver whose destinations
are many.** Flink cannot unregister a metric, so every table the job has ever written to keeps its
counters for the lifetime of the task — and, for the same reason, a table whose batcher was
[evicted](#per-record-destinations) and later rebuilt resumes its old counters rather than
restarting at zero. Switch it on when the table set is small and known; the option is in
[`BigtableWriterOptions`]({{< relref "docs/reference/bigtable" >}}#bigtablewriteroptions). A sink
built with `table(...)` is not excepted: the writer does not inspect the resolver, so there the two
counters simply restate the totals above.

`currentSendTime` is deliberately **not** set: the client batches mutations and completes their
futures asynchronously, so any latency this writer could report would measure its own bookkeeping
rather than the service's response time — a missing number beats a wrong one. There is no committer
either (the sink is single-phase), so Flink's committer metrics do not apply.

### Source metrics

Registered on the source reader's and the split enumerator's metric groups:

| Metric | Type | Meaning |
|---|---|---|
| `rowsRead` | counter | rows this subtask pulled off a stream |
| `changeStreamMutationsRead` | counter | change-stream mutations this subtask received |
| `changeStreamHeartbeatsRead` | counter | heartbeats this subtask received; they advance state without producing a record |
| `changeStreamReadsStarted` | counter | `ReadChangeStream` RPCs this subtask opened, including token-based reopens and fair rotations |
| `activeChangeStreamReads` | gauge | `ReadChangeStream` RPCs currently open in this subtask |
| `queuedChangeStreamPartitions` | gauge | assigned partition splits waiting for a read slot in this subtask |
| `queuedChangeStreamPartitionLagMillis` | gauge | wall-clock lag of the oldest checkpointed position among this subtask's queued partitions, or zero when none are queued |
| `missedHeartbeatIntervals` | gauge | maximum whole five-second heartbeat intervals since any active read last returned a mutation or heartbeat |
| `changeStreamCloseStreamsRead` | counter | `CloseStream` records this subtask received |
| `changeStreamUserMutationsRead` | counter | user-initiated mutations this subtask received |
| `changeStreamGarbageCollectionMutationsRead` | counter | garbage-collection mutations this subtask received |
| `changeStreamMutationEntriesFiltered` | counter | mutation entries removed by configured family or qualifier output filters |
| `changeStreamRecordsSkippedWithoutChange` | counter | mutations whose every entry was filtered and whose deserializer was bypassed because `skipMessagesWithoutChange(true)` was configured |
| `partitionLowWatermarkMillis` | gauge | minimum checkpointed low watermark across every active and queued partition assigned to this subtask, as epoch milliseconds |
| `recordsSkipped` | counter | rows or change-stream mutations the deserializer emitted no record for |
| `numRecordsIn` | counter (Flink standard) | records handed downstream. With a one-to-many deserializer this is neither `rowsRead` nor `rowsRead` minus `recordsSkipped` |
| `splitsAssigned` | counter | splits handed to a reader. On the enumerator, so one set per job |
| `splitsReturned` | counter | splits a failed reader gave back. On the enumerator |
| `changeStreamPartitionsDiscovered` | counter | initial and successor service partitions first accepted into the coordinator ledger. On the enumerator |
| `changeStreamPartitionSplits` | counter | successor partitions released from one parent, including a one-to-one move. On the enumerator |
| `changeStreamPartitionMerges` | counter | successor partitions released after tokens from multiple parents cover their target. On the enumerator |
| `unassignedChangeStreamPartitionLagMillis` | gauge | wall-clock lag of the oldest coordinator-held unassigned partition, or zero when none are unassigned. On the enumerator |
| `changeStreamPartitionsReconciled` | counter | missing service partitions reconstructed from tokens or a tracked low watermark. On the enumerator |
| `changeStreamTokenlessRestarts` | counter | reconciliations that had to restart without a continuation token after the long grace period. On the enumerator |
| `rowKeySamplesTaken` | counter | `SampleRowKeys` calls, on the enumerator: `1` on a fresh start, `0` after a restore. Anything else means the plan was recomputed, which would renumber the splits the readers hold |
| `unassignedSplits` | gauge (Flink standard) | splits planned but not yet handed out. On the enumerator |

There is deliberately **no bytes-read counter**. A row does not report its serialized size, and a
number summed from its keys, qualifiers and values would look exactly like the quantity Bigtable
bills for while not being it. There is no records-remaining gauge either: the samples estimate bytes
for a table's sections and say nothing about how many rows are left inside a range.

### Operating Change Streams capacity

Read `activeChangeStreamReads`, both queued metrics, `unassignedSplits`, and `unassignedChangeStreamPartitionLagMillis` together.
If aggregate active reads reach `source parallelism * maxConcurrentStreamsPerSubtask` while queued or unassigned lag grows, increase source parallelism or raise the per-subtask bound after checking subtask and Bigtable cluster resources.
If the goal is fewer reads and less callback work in each subtask without reducing aggregate capacity, increase source parallelism before lowering the per-subtask bound.

Persistently queued partitions beside free read slots indicate a reader scheduling, rotation, or handover problem; infer free slots in a subtask from its configured bound minus its active and queued counts.
Persistently unassigned partitions while readers have inferred free slots indicate an assignment-protocol problem.
Alert before the minimum active or queued low watermark, or the oldest unassigned position, approaches the table's configured Change Streams retention boundary.

The connector metrics cannot reproduce Bigtable cluster load or retained log volume.
Monitor `bigtable.googleapis.com/cluster/cpu_load_by_app_profile_by_method_by_table`, filtered to the Change Streams application profile, table, and `ReadChangeStream` method, together with `bigtable.googleapis.com/table/change_stream_log_used_bytes`.
The Bigtable `server/latencies` metric measures the lifetime of the streaming request, so it is not a change-processing latency signal.

The counters follow Beam's initial-partition, split, merge, reconciliation, heartbeat, `CloseStream`, user-mutation, garbage-collection-mutation, and active-stream signals where the runtimes have equivalent lifecycle points.
Flink keeps the partition ledger in checkpoint state, so Beam's orphaned metadata-table counter has no counterpart.
Flink's `currentEmitEventTimeLag` describes the latest emitted record rather than Beam's lifetime processing-delay distribution, and the connector does not duplicate Flink's `numRecordsIn`, `watermarkLag`, split `currentWatermark`, or `sourceIdleTime` metrics.

## Scope

Not implemented, with the reason rather than a promise:

- conditional and read-modify-write mutations (`checkAndMutateRow`, `readModifyWriteRow`). These are
  request-response primitives rather than a write path a sink batches: each is one RPC whose result
  the caller is expected to read, and neither participates in `MutateRows`.

## Testing

Unit tests cover the writer against a fake batcher and a fake mailbox: the skip contract, both
failure classes, both in-flight caps engaging, the drain-then-flush ordering the failure SPI
requires, and a handler failure raised inside a completion callback surviving to the next call. The
fake completes nothing on its own, which is what lets a test hold the writer at a cap; the writer
tests carry a timeout, because a broken admission predicate hangs rather than fails.

The adapter that wraps the client's batcher is unit-tested too
([#324]({{< param BookRepo >}}/issues/324)): its teardown shuts the batcher down and then releases
the client, and releases it whatever that shutdown throws — the sink absorbs only the batcher's
report of its accumulated entry failures, so anything else propagates, and the client must not be
left holding a channel when it does. The batcher's operations reach the adapter as functional
values, because the client library's `Batcher` may not be implemented by a fake — the same reason
this connector defines its own batcher interface.

The source's coverage is split three ways, because no one level can carry it. **Split planning is a
unit test**, over a pure function fed sampled boundaries directly: the emulator models no tablets,
so every plan built against it is one split, and its assertions compare the reconstructed ranges
rather than counting splits — a planner that loses the tail of a table produces a job that succeeds
and reads less. **Resume across a failure is a MiniCluster test** over scripted seams rather than
the emulator, for the same reason: one split cannot show a split being reassigned, and the
assertion that distinguishes "resumed" from "restarted" is that a reopened range starts past the
rows already handed over. **The emulator suite** drives the whole assembly through the public
builder — ranges, prefixes and filters surviving into the query the reader sends — and proves the
wiring, nothing more.

Real `SampleRowKeys` over a **pre-split table** is the gated suite's, and so is the measurement the
truncation design leans on: the service **refuses** a range whose start is exclusive at its own end
key — the state a split reaches after emitting its last row — with `INVALID_ARGUMENT` rather than
answering it empty (measured 2026-08-10, [#481]({{< param BookRepo >}}/issues/481)). That is what
makes the reader finishing such a split without opening a stream load-bearing rather than merely
tidy, and the unit test pinning that short-circuit asserts zero open calls, not just an empty
result.

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
([#218]({{< param BookRepo >}}/issues/218)). It runs weekly, and locally through `just e2e` with
`BIGTABLE_IT_PROJECT` set; without that variable its classes skip. Because this suite bills for the
instances it creates rather than using a standing resource, running it is **opt-in per command**
([#245]({{< param BookRepo >}}/issues/245)): its classes carry `@Tag("gated")`, which the build
excludes from every test run by default, so an ordinary `mvn verify` does not select them at all,
whatever the environment holds. `just e2e` clears that exclusion (`-Dtest.excluded.groups=`, which
is also how to run a single gated class by hand). The exclusion is Maven's, so a run started
straight from an IDE bypasses it — there the environment variable is again the only thing standing
between you and a new instance. The gated suite shows:

- **The production-endpoint ADC client-construction path.** Every emulator test passes
  `emulatorEndpoint(...)`, so the branch that builds a client over application-default credentials
  against the production endpoint runs nowhere else. A MiniCluster streaming job with checkpoints
  covers it. The explicit-key branch cannot accompany the emulator and needs no service call to
  prove that it supplies a credentials provider: unit and runtime-boundary tests parse a key file
  and inspect every affected client settings family.
- **Which status Bigtable rejects a mutation with**, and therefore which side of the
  [row-level/fatal boundary](#error-handling) each rejection lands on. This is where the two
  `INVALID_ARGUMENT` examples in that table come from, where the `NOT_FOUND` of a missing table
  and of a missing column family are pinned, and where the batch-wide rejection that the isolation
  pass answers was both found and, since [#239]({{< param BookRepo >}}/issues/239), verified to be
  answered: a good record written beside a bad one is applied, and only the bad one is routed.
- **How same-row entries behaved in one `MutateRows` request under a bounded campaign**
  ([#471]({{< param BookRepo >}}/issues/471), ADR-0093): 86,196 pairs across mirrored submission
  arms and request sizes from 2 through 19,998 produced zero reversals. The probe was deliberately
  not retained as a regression test because the service contract permits arbitrary order; a test
  requiring zero reversals would pin an observation the connector cannot promise.
- **The missing-family leg of [auto-creation](#table-auto-creation)**, which the emulator cannot
  drive at all (it answers `INTERNAL` where the service says `NOT_FOUND` — the table below), and
  the repair against real metadata propagation: a family the options declare but an existing table
  lacks is added through the write path, and an existing family's garbage-collection rule survives
  the repair untouched. The missing-*table* leg runs against the emulator too, whose `NOT_FOUND`
  matches the service's.

There is no persistent instance to run it against: a one-node instance is a standing cost of roughly
$470 a month, so each gated class **creates an instance and deletes it afterwards**, and a run that
dies before deleting is swept by the next one — instance names carry their creation time, and
anything older than two hours is reclaimed. Cleanup disables Change Streams on every table before
deleting its instance, because Bigtable refuses the instance deletion while retained change data
exists. The per-class teardown, its startup sweep and the scheduled sweep all use that order. That is
why nothing in `opentofu/` declares a Bigtable instance, only the API enablement and the grant.

### Where the emulator differs from the service

Measured 2026-08-02 (the missing-table row 2026-08-09), against the pinned
`google-cloud-cli:441.0.0-emulators` image and real Bigtable in `us-central1`, for the same inputs
on both sides. Every row is asserted from both sides, so an emulator image bump has to state what
it changed rather than making this table quietly wrong.

| Input | Real Bigtable | Emulator |
|---|---|---|
| Cell timestamp not a multiple of 1000 | `INVALID_ARGUMENT`, the whole request rejected: every entry of the batch routed to the handler, nothing written | `INTERNAL` ("invalid timestamp 1234"), the offending entry only — the rest of the batch is written |
| Empty row key | `INVALID_ARGUMENT`, "Row keys must be non-empty" | **Accepted.** The row it stores then breaks the client's own read state machine ("rowKey missing"), a state the service cannot reach |
| Mutation naming a column family the table does not have | `NOT_FOUND`, reported for **every** entry of the batch, nothing written | `INTERNAL` ("unknown family"), the offending entry only |
| Mutation against a table that does not exist | `NOT_FOUND`, for every entry — worded "No tables found for instance …" against an instance holding no tables | `NOT_FOUND` ("table ... not found") — the one rejection the emulator answers with the service's status, which is what lets the emulator suite drive the [auto-creation](#table-auto-creation) repair end-to-end; only the wording differs, and the sink classifies by status alone |

The status is the deviation that matters. `INTERNAL` is [fatal](#error-handling) to this sink while
`INVALID_ARGUMENT` is routed, so an emulator test would conclude "fails the job" for a condition the
service makes droppable — the wrong lesson, learned cheaply. It is also why the emulator suite
asserts no rejection except in the class that exists to record these differences.

The read path has its own table, measured 2026-08-09 against the same pinned image (the last two
rows 2026-08-10, [#481]({{< param BookRepo >}}/issues/481)):

| Behaviour | Real Bigtable | Emulator |
|---|---|---|
| `SampleRowKeys` on a populated table | one boundary per tablet, so a pre-split table samples deterministically | the table's final key plus others at roughly one-in-a-hundred probability, whatever the table holds |
| `SampleRowKeys` on an empty table | one response carrying the empty end-of-table key | no samples at all |
| Application profile named on a request | honoured | ignored entirely |
| Empty row key | rejected | accepted, which is why the connector's range algebra can express progress past one |
| Range whose start is exclusive at its own end key | `INVALID_ARGUMENT`, "start_key must be less than end_key" | answered empty, no error |
| Filter naming a column family the table does not have | `NOT_FOUND`, the read fails | answered empty, no error |

The first row is why **split planning is never an emulator test**: every plan built against the
emulator is effectively one split, so an emulator suite could not tell a working planner from one
that loses the tail of a table. The third is why a configured `appProfileId` is covered only by the
gated suite.

## Provenance and attribution

No code is copied from any other project. The serializer's shape — the `RowMutationEntry` return
type and null-means-skip — is shared with
[google/flink-connector-gcp](https://github.com/google/flink-connector-gcp)
(Apache-2.0) so its users migrate mechanically, and Apache Beam's `BigtableIO` (Apache-2.0) was read
as a design reference for how a runner drives the bulk mutation batcher. Depending on the former, or
vendoring it, was evaluated and rejected on
[#33]({{< param BookRepo >}}/issues/33), which records the grounds.
