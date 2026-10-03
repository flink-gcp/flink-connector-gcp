---
title: Firestore
type: docs
weight: 60
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

# Firestore connector

The connector reads and writes a Firestore database in Native mode.
A bounded source reads a whole collection group or one query at a single snapshot time, and finishes.
An at-least-once sink applies one document write per record, through the client library's `BulkWriter`, which batches writes into `BatchWrite` requests and throttles them to Firestore's ramp-up guidance.

The connector has not been released yet ([#1547]({{< param BookRepo >}}/issues/1547) tracks the release that first publishes it).
The module is also the home of the Datastore-mode surface, under its own package root; that half is not implemented yet ([#1542]({{< param BookRepo >}}/issues/1542)).
Every option is listed on the [Firestore options]({{< relref "docs/reference/firestore" >}}) page.

## Credentials

The source and the sink use Application Default Credentials when neither `serviceAccountKeyFile(...)` nor `emulatorEndpoint(...)` is set.
Set the key file only when the job must select a service-account JSON key that the runtime environment cannot supply through ADC.

{{< java-snippet file="FirestoreConnectorCredentials.java" tag="firestore-connector-credentials" >}}

The connector serializes only the path into the job graph, and each TaskManager reads the file when a writer or a source reader starts; the JobManager reads it too, whenever it creates or restores the source's enumerator.
A deployment must therefore mount the same path in every container that runs the job.
`serviceAccountKeyFile(...)` and `emulatorEndpoint(...)` are mutually exclusive, because the emulator channel carries no credentials.
A loading failure is sanitized, so neither the path nor the key's contents enter the exception.

The client library also reads the `FIRESTORE_EMULATOR_HOST` environment variable on its own whenever no emulator endpoint is set, and then sends every request to that host over plaintext, with the emulator's placeholder token in place of the job's credentials.
The connector cannot switch that lookup off.
A writer or a source that starts in an environment carrying the variable, with no `emulatorEndpoint(...)` configured, logs a warning naming the variable and its value.

The identity the job runs as needs permission to read documents for the source, and to create, update and delete them for the sink; [`roles/datastore.user`](https://cloud.google.com/firestore/docs/security/iam) carries all four, and `roles/datastore.viewer` is enough for a job that only reads.
Neither direction creates anything else: collections need no creating, and the database must exist.

## Lineage

The source and the sink implement Flink's `LineageVertexProvider`, and extraction opens no client.
A collection-group scan reports the group, with namespace `firestore://{project}/{database}`, the group id as its name, and a `gcp` physical-resource facet.
A source that reads a query reports an empty dataset list, and so does the sink: a database does not establish which collections a query factory or a serializer will address, and extraction calls neither.
Flink 2.x extracts lineage automatically; Flink 1.20 supports direct inspection but not native listener delivery.

## Source

The source is bounded: it reads a snapshot of the database and finishes.
Bounded is not batch-only; the source runs inside a streaming job too, and ends there.
It reads one of two things, set on the builder as `collectionGroup(...)` or `query(...)`.

A **collection-group scan** reads every document of every collection with one id, at any depth of the database.
The service cuts it into partitions, so the scan reads in parallel.

{{< java-snippet file="FirestoreConnectorSource.java" tag="firestore-connector-source" >}}

A **query** reads what one query of your own returns: filters, ordering, a projection, cursors, `limit` and `offset` are all honoured.
The service partitions only a whole collection group, so a query is read as one split, by one subtask.

{{< java-snippet file="FirestoreConnectorSourceQuery.java" tag="firestore-connector-source-query" >}}

The factory runs on the JobManager when the read is planned, and builds the query from the client the source hands it; a query addressing another database is refused.
An `offset` is resolved once, when the read is planned, into a position after the documents it skips, so the service reads and bills those documents then, as it would for the offset itself.
A `limitToLast` query reads the same documents in the reverse of its stated order, because the client library carries that reversal outside the query the source ships to its readers.

### One snapshot for the whole read

Every split reads at the same read time, so the job sees the database as it stood at one instant, whatever is written while it runs.
Without `readTime(...)`, the source takes the service's own time when it plans the read, rather than the JobManager's clock, which could run ahead of the service's: the service refuses a read time in the future.

The service keeps old versions for one hour, or for seven days when [point-in-time recovery](https://cloud.google.com/firestore/native/docs/pitr) is enabled, and a read time older than an hour must then fall on a whole minute.
The read time the source takes by default is the service's own, to the microsecond, so it is good for an hour whatever the database's settings: a read that may run, or be restored, later than that needs point-in-time recovery and a `readTime(...)` on a whole minute.
Rounding the default down to a minute was declined, because it would hide what was written in the minute before the job started.
A configured read time outside that window fails the job when it plans the read.
The window keeps moving while the job runs, so a read that takes longer than the window, or a restore from a checkpoint older than it, fails every remaining read with an error naming the split and the read time.
Such a job has to start over; nothing can resume a snapshot the service no longer holds.

The plan, and with it the read time, is recorded in the enumerator's checkpoint.
A job that restarts before any checkpoint has completed plans again, at a new read time unless `readTime(...)` is set, and a sink that already wrote the first attempt's records then holds records of two snapshots.
Set `readTime(...)` when that matters.

### Splits, pages and recovery

A scan asks the service for `partitionCount` partitions, the source's parallelism by default.
It is an upper bound: a small collection group comes back in fewer partitions, and the subtasks left without one finish immediately, which the JobManager logs.
Asking for more partitions than subtasks lets a subtask that finishes early take another partition while a slower one is still reading.
The partitions are cut by document name at the time of planning, not at the read time, because the client library's partitioning call takes no read time; that moves nothing, because together they cover every name, so a document of the snapshot falls in exactly one of them.

A reader fetches a split in pages of `pageSize` documents, one request each, continuing after the last document of the page before.
The client library retries a page whose stream breaks, from the page's last document, without lowering its limit; the reader keeps only the documents the page asked for, and the next page reads the rest.
A page is held in memory whole before any of its documents is handed on, so `pageSize` times the largest document is what one fetch ordinarily holds; lower it for large documents.
Until the reader cuts it, a retried page holds the retry's documents as well, up to one page more for each break, so a fetch that meets breaks can briefly hold a multiple of that bound.

A checkpoint records, for each split being read, the query that is left: its start moved to just after the last document the job has passed, its `limit` reduced by the documents already passed, and its `offset` dropped.
A restore resumes there, without reading a document twice and without skipping one.
The one query this cannot continue is one whose projection leaves out a field it orders by or filters with an inequality (`<`, `<=`, `>`, `>=`, `!=`, `not-in`), because the position after a document is made of those fields' values.
The source reads a document when it plans the read, and refuses such a query then, with a message naming the field to add.

The design, and the alternatives it declined, are recorded in [ADR-0173]({{< param BookRepo >}}/blob/main/docs/adr/0173-the-firestore-source-reads-partition-cursor-ranges-at-one-read-time.md).

### Deserialization

A `FirestoreDocumentDeserializationSchema` turns each document into zero or more records.
It receives the client library's `DocumentSnapshot`, whose read time is the job's and which holds only the projected fields when the scan sets `select(...)`.

{{< java-snippet file="FirestoreConnectorSourceDeserializer.java" tag="firestore-connector-source-deserializer" >}}

A document that produces no record is skipped, counted in `recordsSkipped`, and still passed: a restore does not read it again.
A deserializer that throws fails the job, and the document is read again after the restore.

### Source metrics

Registered on the source reader's and the split enumerator's metric groups.

| Metric | Type | Meaning |
|---|---|---|
| `numRecordsIn` | counter (Flink standard) | Documents handed to the deserializer, one per document whatever it produced, counted before it runs |
| `documentsRead` | counter | Documents the reader handed on from its pages, counted when a page arrives; a page the client library's retry overfilled counts only what the reader kept |
| `recordsSkipped` | counter | Documents the deserializer produced no record for |
| `splitsAssigned` | counter | Splits the enumerator handed to a reader |
| `splitsReturned` | counter | Splits a failed reader gave back to the enumerator |
| `readsPlanned` | counter | Planning calls that completed: one on a fresh run, zero on one restored from a checkpoint that recorded the plan |

### Not here yet

There is no unbounded source; [Change data capture](#change-data-capture) explains why.
Reading a Datastore-mode database is a source of its own, not implemented yet ([#1543]({{< param BookRepo >}}/issues/1543)).

## Sink

### The destination is a database, not a collection

The sink is configured with a `DatabaseDestination`: a project and a database id, `(default)` unless another is named.
Which document a record goes to is not configured at all.
The serializer returns a `FirestoreWrite`, and a write names its own document by a path relative to the database, such as `orders/o-1` or `customers/c-1/orders/o-1`.
One sink therefore writes to as many collections as its serializer produces, and there is no destination resolver to configure.
There is nothing to create either: Firestore creates a collection with its first document, and creating a database is a provisioning step outside a job's reach.

{{< java-snippet file="FirestoreConnectorSink.java" tag="firestore-connector-sink" >}}

### Operations and field values

A `FirestoreWrite` carries one of five operations.

| Operation | Effect |
|---|---|
| `set` | Replaces the document, creating it if it is missing |
| `setMerge` | Merges the fields into the document, creating it if it is missing; a nested map is merged key by key |
| `create` | Creates the document; refused with `ALREADY_EXISTS` if it exists |
| `update` | Replaces the named top-level fields of an existing document; refused with `NOT_FOUND` if it is missing |
| `delete` | Deletes the document; deleting a missing document succeeds |

Every field name is literal in every operation.
`a.b` names one top-level field whose name contains a dot, never field `b` inside map `a`, although the client library would read an update's keys as dotted paths.
One spelling meaning two things depending on the operation is the confusion this rules out.
To change one key of a nested map, use `setMerge`.

Field values come from a closed list: `null`, `String`, `Long`, `Double`, `Boolean`, `com.google.cloud.Timestamp`, `GeoPoint`, `Blob`, a `List` of values, or a `Map` from non-empty field names to values.
Anything else is rejected when the write is built, which the sink reports as a failure to serialize that record.
The list is closed because of how the client library fails.
A value it cannot encode, such as a `Short` or a `byte[]`, makes it throw after it has already queued the operation, and the rest of that request is then answered out of step: in the measurement, one write was applied while its result never arrived.
Write an `int` as a `Long`, a `float` as a `Double`, and bytes as `Blob.fromBytes(...)`; an `Integer`, a `Float` or a `byte[]` is rejected.
A `Blob` must be a plain one: a BSON binary `Blob` of any subtype other than 0 is rejected, because the client library encodes it as a reserved map rather than as bytes and the sink's request-size accounting does not count that form ([#1589]({{< param BookRepo >}}/issues/1589)).

Firestore's own limits stay the service's to enforce.
A document over 1 MiB, a reserved `__name__`-style field or document id, a map nested more than 20 levels deep, or (in a Standard-edition database) an array inside an array is refused with `INVALID_ARGUMENT`, and the next section says what the sink does with that.

`update` and `delete` also take a `lastUpdateTime` precondition: the write applies only if the document was last updated at exactly that time, and Firestore refuses it with `FAILED_PRECONDITION` otherwise.
That is the only precondition the client library exposes publicly.
`FieldValue` transforms such as `serverTimestamp()` and `increment()` are not supported yet; the reopen condition is recorded on [#355]({{< param BookRepo >}}/issues/355).

### How the sink writes

Each writer subtask holds one `BulkWriter` and hands it every write.
The library sends a request once it holds 20 writes (10 once it holds a retried write), or when the writer asks it to.
It sends on no timer, and a write it retries after a backoff joins whatever batch is open at that moment, so the writer asks on every pass of every wait: at each checkpoint barrier, and whenever its in-flight bounds are full.
It also asks before the writes since the last request would pass 9 MiB on the wire, because Firestore limits an API request to 10 MiB, and 20 documents of up to 1 MiB each can exceed that.
The writer counts a write's size as the protobuf message the library sends for it, masks and preconditions included; Firestore's storage-size formula, the unit of the 1 MiB document limit, can undercount that by half for a document of many small fields.
A retried write rejoins a later batch outside that count, so a request carrying retries can still pass the limit; the service then refuses it whole, and its writes are confirmed alone, which is slower but loses nothing.

The writer bounds what it has handed over and not yet seen answered, by count (`maxInFlightWrites`) and by size (`maxInFlightBytes`), and yields to the Flink mailbox at either bound.
The client library offers no backpressure of its own: past 500 pending writes it queues further ones without limit.
`maxInFlightWrites` is therefore capped at 500, and defaults to half that, for the reason the [pending-operation leak](#the-client-librarys-pending-operation-leak) section gives.

Returning `null` from the serializer skips the record: it is written nowhere, is not a failure, never reaches the failure handler, and is counted by `recordsSkipped`.
Throwing marks the record as failed and routes it instead.
This is the same contract every connector here follows.

## Delivery guarantee

See [Write and key-collision semantics]({{< relref "docs/connectors/delivery-guarantees" >}}#write-and-key-collision-semantics) for the comparison with the other connectors.

The sink is at-least-once and stateless.
At each checkpoint barrier it sends everything the library holds and waits until every write is answered, so a completed checkpoint means every record up to it was applied, skipped by the serializer, or handed to the failure handler.
A record can nonetheless reach the database twice: after a job restart, and also within one attempt, when the library retries a request whose outcome never arrived.
Which operation the serializer builds decides whether that matters.

| Operation | Same write replayed |
|---|---|
| `set`, `setMerge` | Idempotent for that write |
| `delete` | Idempotent, and deleting a missing document succeeds |
| `create` | Refused with `ALREADY_EXISTS`, routed to the failure handler |
| `update` | Idempotent, but if the document was deleted in between, Firestore answers `NOT_FOUND`, which fails the job |
| Any write with `lastUpdateTime` | Refused with `FAILED_PRECONDITION`, because the first application changed the update time; `preconditionFailurePolicy` decides |

Two consequences are worth planning for.
Under the default `FailureHandler.failJob()`, a stream of `create` writes fails again on every restart whose replay reaches a create that had already been applied; a handler that drops or dead-letters is what lets such a job recover.
The same holds for conditional writes, with `preconditionFailurePolicy(ROUTE_TO_FAILURE_HANDLER)` in addition.

This per-write property is not an ordering guarantee.
The library sends several requests at once, `BatchWrite` applies the writes of one request in no guaranteed order, and a failed write is retried in a later request.
Two writes to the same document can therefore be applied in either order, even within one subtask; a `set` followed by a `delete` can leave the document in place.
An opt-in mode that keeps submission order per document is tracked on [#1556]({{< param BookRepo >}}/issues/1556).

There is no exactly-once mode.
Firestore offers a primitive that could support one (a transaction that binds a ledger document to its writes with an update-time precondition), and the design comment on [#355]({{< param BookRepo >}}/issues/355) records it, but [ADR-0104]({{< param BookRepo >}}/blob/main/docs/adr/0104-exactly-once-modes-use-service-native-replay-protection-and-pass-a-performance-gate.md) requires a concrete requirement that idempotent writes cannot meet, and correctness and performance gates, before one is built.

## Error handling

The client library retries a write itself (see [Retries](#retries)), so a failure that reaches the writer is one the library gave up on.
The writer sorts those three ways.

**Routed to the `failedWriteHandler`**, because the service refused this one write and would refuse it again:

- `INVALID_ARGUMENT`, after it is confirmed alone (see below).
- `ALREADY_EXISTS` for a `create`.
- `FAILED_PRECONDITION` for a write carrying a `lastUpdateTime`, only under `preconditionFailurePolicy(ROUTE_TO_FAILURE_HANDLER)`.

**Fails the job**: everything else, including those statuses on any other write, any status the library's retries gave up on, and a failure carrying no status at all.
`INVALID_ARGUMENT` may reach a handler that might drop it because gRPC defines it as problematic whatever the system's state. `ALREADY_EXISTS` for a create and a failed `lastUpdateTime` precondition do depend on state, but on a state the write itself chose: its own earlier application, or the precondition the pipeline set (the reasoning of [ADR-0076]({{< param BookRepo >}}/blob/main/docs/adr/0076-two-spanner-statuses-are-routed-and-a-request-failure-never-is.md)). No transient status ever reaches a handler, so an unstable service can never produce a dead letter.
A chain that carries a transient status anywhere is never routed, and the first classifiable status in the chain decides the rest.

{{< java-snippet file="FirestoreConnectorFailedWritePolicy.java" tag="firestore-connector-failed-write-policy" >}}

### Invalid writes are confirmed alone

The library reports a failure of a whole `BatchWrite` request against every write the request carried, so an `INVALID_ARGUMENT` may describe the request rather than the write.
Measured against the emulator, one invalid write among five valid ones answered all six with `INVALID_ARGUMENT`.
The writer therefore parks a write refused this way and, before the next record, re-sends each parked write as the only write of its request.
Only a refusal that repeats alone is routed; a write that succeeds alone is simply applied.
`parkedWrites` shows the queue, and `writesConfirmedAlone` counts every re-send, including those that turn out to be applied, which is the only trace a request refused as a whole leaves.
`ALREADY_EXISTS`, `NOT_FOUND` and `FAILED_PRECONDITION` came back for their own write only, with the other writes of the request applied, which is why those are acted on as they arrive.

### What does not reach the failure handler, and why

Measured against the emulator, one run, 2026-09-27:

| What the write did | Status | What the sink does |
|---|---|---|
| `create` of a document that exists | `ALREADY_EXISTS` | Routed |
| A document over 1 MiB, a reserved field name or id, nested arrays, a map nested past 20 levels | `INVALID_ARGUMENT` | Confirmed alone, then routed |
| `update` or `delete` with a stale `lastUpdateTime`, whether or not the document exists | `FAILED_PRECONDITION` | **Fails the job** by default |
| `update` of a missing document | `NOT_FOUND` | Fails the job |
| `delete` of a missing document | *applied* | None |

`preconditionFailurePolicy(ROUTE_TO_FAILURE_HANDLER)` moves the stale-precondition row into the failure handler, which then decides between failing, dropping and dead-lettering.
The default is `FAIL_JOB`, because it cannot lose a record: a stream in which every precondition fails says the pipeline reads stale update times, and shedding those records one at a time would hide that behind a green job.
The policy never covers a `FAILED_PRECONDITION` answering a write without a precondition.
That status then describes the database rather than the record; a database in Datastore mode is reported to refuse Firestore API writes with it, every write alike (not yet measured; the gated suite, [#1546]({{< param BookRepo >}}/issues/1546), is where it will be).
A stream made only of conditional writes to such a database would still have every write routed under the policy, which is one more reason the policy is an opt-in.

`NOT_FOUND` is never routed, although an `update` of a document deleted in the meantime is data.
The status does not say whether the document or the database is missing, and a job pointed at a database that does not exist can be expected to see it for every write (the emulator cannot show this, and the gated suite will measure it), so routing it would drop every record of such a job.
A stream that can legitimately update documents that may be gone should use `setMerge`, which creates the document instead.

`maxConsecutiveRejections` fails the job once that many confirmed refusals arrive with no write applied between them, after routing each of them.
It matters only beside a handler that does not fail the job, dropping or dead-lettering, where it stops a stream whose writes Firestore refuses wholesale from being shed one record at a time.
`ALREADY_EXISTS` for a create and `FAILED_PRECONDITION` for a conditional write do not count toward it, because a restart's replay answers exactly those for every create and conditional write it repeats; without that, a replay window longer than the bound would fail the job again on every restart.
Records the serializer rejects do not count either: the bound is about Firestore's refusals.

The reasoning and the declined alternatives are in [ADR-0171]({{< param BookRepo >}}/blob/main/docs/adr/0171-the-firestore-sink-writes-through-bulkwriter-and-confirms-invalid-writes-alone.md).

### Dead-letter payloads

`FailedWrite.getPayloadBytes()` is the Java-serialized `FirestoreWrite`, not the Firestore `Write` protobuf.
The client library's conversion from Java values to the wire form is package-private or marked internal, so its supported public API offers no route to the protobuf; `FirestoreWrite` is the connector's own serializable value.
A handler that wants the write itself should take `FailureHandler<FailedWrite>` and read `getWrite()`; the bytes exist for the cross-connector dead-letter queue, which sees only the shared `FailedElement` view.

## Retries, throttling and metrics

### Retries

The client library retries a write refused with `RESOURCE_EXHAUSTED`, `UNAVAILABLE` or `ABORTED`, with a jittered backoff that starts at one second and grows by half up to a minute, except that after `RESOURCE_EXHAUSTED` it waits the full minute from the first retry.
`writeMaxAttempts` is the one part of that loop the library lets a caller decide; it counts the first attempt, and its default of 11 is the library's own ten retries.
The sink owns no retry loop, and this loop's backoff has no knobs, because the library exposes none.
A write the database keeps refusing with `RESOURCE_EXHAUSTED` can therefore take about ten minutes to exhaust the default budget, close to Flink's default checkpoint timeout, and every failed attempt counts as an answer, about two minutes at most after the one before it (a minute of backoff and a 60-second attempt), so the stall warning below stays silent; `writesRetried` is the signal that shows it, together with `errorClass.RESOURCE_EXHAUSTED.errors` once a write gives up.
Underneath, the library's transport also retries each `BatchWrite` call, and by default bounds each call, its own retries included, at 60 seconds.
The `retry*` options set that layer, as the Pub/Sub sink's options of the same names set its publish call; the [reference]({{< relref "docs/reference/firestore" >}}#transport-retries) lists the library's values they replace.
The two-minute gap above assumes those values: a longer `retryTotalTimeout` or per-attempt timeout lengthens it, and a gap past three minutes then logs the warning below for a write that is still being retried.
Zero per-attempt timeouts, which gax reads as no deadline, remove the bound on an attempt altogether.

A wait that goes three minutes without the library answering anything logs a warning naming the database and the writes in flight.
The sink does not fail such a wait, because every attempt is bounded by the `BatchWrite` call's timeouts unless they are set to zero, and the one stall the options could cause, a rate below the library's batch size, is refused when the options are built.
The library can still stall on a defect of its own: its rate limiter throws, inside a scheduled task whose failure nothing reads, if the wall clock steps backwards.
A streaming job's checkpoint timeout then fails the job, with a message that names nothing about Firestore; a bounded job has no such backstop, and the warning is what names the cause.

### Throttling

The library throttles itself to Firestore's 500/50/5 ramp-up guidance: it starts at 500 operations per second and raises the rate by half every five minutes.
The rate is per writer subtask, so a sink of parallelism 4 starts at 2,000 operations per second across the job.
`initialOpsPerSecond` and `maxOpsPerSecond` move the start and cap the ramp; `throttlingEnabled(false)` removes it, and a database without the warm-up it paces may then answer a burst with `RESOURCE_EXHAUSTED`.
Neither rate may be set below 20.
The library opens its first batch at 20 writes before it lowers its batch size to a slower rate, and a full batch of 20 cannot pass a throttle below 20: the library retries sending it with no delay, spinning its thread, until the ramp-up lifts the rate to 20, which takes minutes, or forever when the ceiling is below 20.

{{< java-snippet file="FirestoreConnectorTuning.java" tag="firestore-connector-tuning" >}}

Throughput is also bounded by the documents a stream writes to.
Firestore limits how fast one document can be updated, at a rate that depends on the workload, and document ids that increase monotonically, such as timestamps or sequence numbers, concentrate writes on one key range; the [best practices](https://cloud.google.com/firestore/docs/best-practices) page describes both.
A serializer that derives ids from a hash, or that spreads a hot document's updates across several documents, avoids both limits.

### The client library's pending-operation leak

The library never releases the pending-operation slot of a failed write: it frees a failed write's slot only in a handler for gax's `ApiException`, and a failed write completes with a `BulkWriterException`, which is not one.
After 500 failed writes in one `BulkWriter`'s life, it sends nothing more.
The writer counts the failures of its `BulkWriter` and, once nothing is in flight, replaces it before a submission could reach that ceiling.
A new `BulkWriter` starts its throttle's ramp-up again, and `bulkWritersReplaced` counts the replacements.
That cost is why `maxInFlightWrites` defaults to 250 rather than to the ceiling: the other 250 slots absorb failed writes, such as the creates a restart replays, without a replacement for each one.
`BulkWriterDefectsITCase` pins this defect and the synchronous-refusal one above, so a library release that fixes either fails that test.

### Sink metrics

Registered on the sink writer's metric group.

| Metric | Type | Meaning |
|---|---|---|
| `numRecordsSend` | counter (Flink standard) | Records handed to the client library. Counted once per record, never again for a retry or a solo re-send |
| `numBytesSend` | counter (Flink standard) | Their size in the `BatchWrite` request, computed from the protobuf message the library sends for each |
| `numRecordsSendErrors` | counter (Flink standard) | Records routed to the failure handler, whether the serializer rejected them or the service refused the write |
| `recordsSkipped` | counter | Records the serializer returned `null` for |
| `writesRetried` | counter | Write attempts the client library retried, one per write per retry |
| `writesConfirmedAlone` | counter | Writes re-sent alone to confirm an `INVALID_ARGUMENT`, whatever the verdict. A rise with no matching `errorClass.INVALID_ARGUMENT.errors` means requests refused as a whole whose writes were fine alone |
| `bulkWritersReplaced` | counter | Times the writer replaced its `BulkWriter` to clear the library's failed-write slots; each replacement restarts the throttle's ramp-up |
| `inFlightWrites` | gauge | Writes handed to the client library and not yet answered |
| `inFlightBytes` | gauge | Their size in the request |
| `parkedWrites` | gauge | Writes waiting to be re-sent alone to confirm an `INVALID_ARGUMENT` |
| `errorClass.CODE.errors` | counter | Failed writes by status code, `CODE` being a gRPC status name or `UNCLASSIFIED`, counted once a write's verdict is final |

There is no `batchesSent`: the library batches internally and exposes no hook for a request it sends.
There are no per-destination counters, because the sink writes any number of collections and that cardinality is the serializer's to choose.
`currentSendTime` is unset, as on the sibling sinks, because a request's latency covers unrelated writes.

## Testing

Functional coverage runs against the Firestore emulator, the `gcloud emulators firestore` binary in the same `google-cloud-cli` image the Bigtable and Pub/Sub tests use, through testcontainers.
The sink tests drive the production writer-creation path, so the client and `BulkWriter` are the real ones, and a MiniCluster job covers checkpoint-driven and end-of-input flushes.
The source tests read through the production planner and page reader, and a MiniCluster job that fails once after a checkpoint shows a restored split resuming after its last document.
The endpoint reaches the client through the builder, never through `FIRESTORE_EMULATOR_HOST`.

### Emulator deviations

An emulator is a convenience for fast feedback, never evidence about the service.
Where the two disagree, the service decides.

| Deviation | Consequence |
|---|---|
| Request size is not enforced | A `BatchWrite` of about 10.5 MiB (twelve documents of 900 KiB) was applied. The writer's 9 MiB request budget is untested against a real refusal until the gated suite ([#1546]({{< param BookRepo >}}/issues/1546)) covers it, including whether the service answers an oversized request with `INVALID_ARGUMENT` |
| Rejection statuses are the emulator's | The error-handling table was measured against the emulator only. The gated real-GCP suite ([#1546]({{< param BookRepo >}}/issues/1546)) is where it is confirmed |
| No IAM checks | The emulator accepts its placeholder token for everything, so `PERMISSION_DENIED` is not exercised |
| No ramp-up or quota behavior | Throttling and `RESOURCE_EXHAUSTED` handling are not exercised |
| `PartitionQuery` is not implemented | Asking for two partitions fails with `UNIMPLEMENTED`; one partition is the client library's own answer, made without a call. The source's emulator tests choose partition boundaries themselves, so the service's partitioning and its partition counts are exercised only by the gated suite ([#1546]({{< param BookRepo >}}/issues/1546)). A scan against the emulator needs a partition count of one |
| Old read times are answered | A read time two hours old was answered, where the service keeps versions for one hour without point-in-time recovery. The read-time window is exercised only by the gated suite ([#1546]({{< param BookRepo >}}/issues/1546)) |

## Scope and provenance

The [module README]({{< param BookRepo >}}/blob/main/flink-connector-gcp-firestore/README.md) records implementation status and provenance.
Why one module holds both the Native-mode and the Datastore-mode surfaces is recorded in [ADR-0170]({{< param BookRepo >}}/blob/main/docs/adr/0170-firestore-and-datastore-share-one-module-with-two-package-roots.md).

### Change data capture

There is no change-stream source, and the reason is the service rather than the connector.
The `firestore.v1` API has no change-stream read, its `Listen` call is a live snapshot listener with no replay of history (a restart loses the deletes that happened while it was down), and the Datastore API has no listener at all.
Firestore's change streams are documented only for Enterprise edition, through the MongoDB-compatible API, and in Preview (checked 2026-09-26).
The conditions that would reopen the question are recorded on [#355]({{< param BookRepo >}}/issues/355).

Until then, the supported way to stream Firestore changes into Flink is through Pub/Sub.
An [Eventarc trigger](https://cloud.google.com/firestore/native/docs/eventarc) on `google.cloud.firestore.document.v1.written` invokes a Cloud Run function, the function publishes the event to a Pub/Sub topic, and this repository's [Pub/Sub source]({{< relref "docs/connectors/datastream/pubsub" >}}) reads it.
Eventarc delivers those events at least once and in no guaranteed order, so the job has to tolerate duplicates and reordering.
