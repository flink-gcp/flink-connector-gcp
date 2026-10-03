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

The connector reads and writes a Firestore database in Native mode or in Datastore mode.
A bounded source reads a whole collection group or one query at a single snapshot time, and finishes.
An at-least-once sink applies one document write per record, through the client library's `BulkWriter`, which batches writes into `BatchWrite` requests and throttles them to Firestore's ramp-up guidance.
A second bounded source and a second at-least-once sink read and write a database in Datastore mode; [Datastore mode](#datastore-mode) covers both.

The connector has not been released yet ([#1547]({{< param BookRepo >}}/issues/1547) tracks the release that first publishes it).
The Datastore-mode source and sink live under their own package root, `io.github.flink.gcp.connector.datastore`, and reach the database through the Datastore client library.
Every option is listed on the [Firestore options]({{< relref "docs/reference/firestore" >}}) page.

## Credentials

Both sources and both sinks use Application Default Credentials when neither `serviceAccountKeyFile(...)` nor `emulatorEndpoint(...)` is set.
Set the key file only when the job must select a service-account JSON key that the runtime environment cannot supply through ADC.

{{< java-snippet file="FirestoreConnectorCredentials.java" tag="firestore-connector-credentials" >}}

The connector serializes only the path into the job graph, and each TaskManager reads the file when a writer or a source reader starts; the JobManager reads it too, whenever it creates or restores the source's enumerator.
A deployment must therefore mount the same path in every container that runs the job.
`serviceAccountKeyFile(...)` and `emulatorEndpoint(...)` are mutually exclusive, because the emulator channel carries no credentials.
A loading failure is sanitized, so neither the path nor the key's contents enter the exception.

The client library also reads the `FIRESTORE_EMULATOR_HOST` environment variable on its own whenever no emulator endpoint is set, and then sends every request to that host over plaintext, with the emulator's placeholder token in place of the job's credentials.
The connector cannot switch that lookup off.
A writer or a source that starts in an environment carrying the variable, with no `emulatorEndpoint(...)` configured, logs a warning naming the variable and its value.
The Datastore-mode source and sink are not exposed to the same lookup: they always give their clients a host, the service's own or the emulator's, and the Datastore client library takes its endpoint from `DATASTORE_EMULATOR_HOST` only when none is given.

The identity the job runs as needs permission to read documents for the sources, and to create, update and delete them for the sinks; [`roles/datastore.user`](https://cloud.google.com/firestore/docs/security/iam) carries all four, in either mode (the Datastore-mode sink also reads, through the lookup it makes before routing an update's `NOT_FOUND`), and `roles/datastore.viewer` is enough for a job that only reads.
Nothing creates anything else: collections and kinds need no creating, and the database must exist.

## Lineage

Both sources and both sinks implement Flink's `LineageVertexProvider`, and extraction opens no client.
A collection-group scan reports the group, with namespace `firestore://{project}/{database}`, the group id as its name, and a `gcp` physical-resource facet.
A Datastore-mode source whose kind is known when the job is built (a `kind(...)`, or a `query(...)` naming exactly one kind) reports the kind as its name, with namespace `datastore://{project}/{database}`, followed by `/{namespace}` outside the default namespace; the namespace belongs there rather than in the name because a kind may contain `/` and a namespace may not.
A Native-mode source that reads a query reports an empty dataset list, and so do a Datastore-mode GQL query, which the service parses only when the read is planned, a Datastore-mode query naming no kind or several, and both sinks: a database does not establish which collections or kinds a query factory or a serializer will address, and extraction calls neither.
Flink 2.x extracts lineage automatically; Flink 1.20 supports direct inspection but not native listener delivery.

## Source

The Native-mode source is bounded: it reads a snapshot of the database and finishes.
Bounded is not batch-only; the source runs inside a streaming job too, and ends there.
The Datastore-mode source is described under [Reading in Datastore mode](#reading-in-datastore-mode).
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
The source passes the library's decoding of the values through unchecked.
Besides the types the sink writes, a field can decode to a `DocumentReference`, a `VectorValue` or a `Blob` with a BSON binary subtype, and, in an Enterprise-edition database, to one of the library's BSON value classes (`MinKey`, `MaxKey`, `RegexValue`, `BsonObjectId`, `BsonTimestamp`, `Int32Value`, `Decimal128Value`), which the Firestore API carries as a reserved single-key map such as `{"__int__": 5}`; a Standard-edition database refuses to store those classes ([#1589]({{< param BookRepo >}}/issues/1589)).

{{< java-snippet file="FirestoreConnectorSourceDeserializer.java" tag="firestore-connector-source-deserializer" >}}

A document that produces no record is skipped, counted in `recordsSkipped`, and still passed: a restore does not read it again.
A deserializer that throws fails the job, and the document is read again after the restore.

### Source metrics

Registered on the Native-mode source reader's and split enumerator's metric groups.

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
A `Blob` must have subtype 0, and the library's BSON value classes (`Int32Value`, `Decimal128Value` and the rest) are not accepted.
The library encodes both as a reserved map rather than as the value itself, and the sink's request-size accounting does not count that form ([#1589]({{< param BookRepo >}}/issues/1589)).
A document the source read can therefore hold a value the sink refuses: a subtyped `Blob`, which a Standard-edition database stores too, or one of the classes an Enterprise-edition database stores.

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

## Datastore mode

A database in Datastore mode answers the Datastore API (`datastore.v1`) rather than the Firestore one, and refuses Firestore API writes.
The module reads it through a source of its own, `DatastoreSource`, and writes it through a sink of its own, `DatastoreSink`, which share nothing with the Native-mode source and sink beyond the credentials and emulator conventions above.
Both reach the database through the Datastore client library: the sink carries its entity and key types, and the source hands its `Entity` to the deserializer while taking a query in the API's own protobuf form.

### Keys address the entity

The sink is configured with the Datastore root's own `DatabaseDestination` (`io.github.flink.gcp.connector.datastore.DatabaseDestination`, not the Native-mode type of the same name): a project and a database id.
The Datastore API names the default database with an empty id and refuses the spelling `(default)`, so `DatabaseDestination.of(project)` and `DatabaseDestination.of(project, "(default)")` both address the default database with the empty id.
The serializer returns a `DatastoreMutation`, and its key names the entity: project, database, namespace, kind, ancestors and a name or an id.
One sink therefore writes to as many kinds and namespaces as its serializer produces, and nothing needs creating.
A key must address the sink's own project and database; one that does not is routed to the failure handler before it is sent, because the service would refuse the whole commit it joined (the emulator answers `INVALID_ARGUMENT`, "mismatched databases within request").

{{< java-snippet file="DatastoreConnectorSink.java" tag="datastore-connector-sink" >}}

A `DatastoreMutation` carries one of four operations, named as the Datastore API names its mutations.

| Operation | Effect |
|---|---|
| `upsert` | Writes the entity, replacing whatever the key held |
| `insert` | Writes the entity; refused with `ALREADY_EXISTS` if the key holds one |
| `update` | Replaces the entity whole; refused with `NOT_FOUND` if the key holds none |
| `delete` | Deletes the entity; deleting a missing entity succeeds |

The key must be complete, a name or an id rather than a key the service fills in.
An id allocated on the first attempt would be lost to a retry, which would then write a second entity, so the sink leaves id allocation to the pipeline.
Property values are the client library's own value types, and the service enforces its limits: an indexed string over 1,500 bytes, an entity over 1 MiB, or a reserved `__name__`-style kind or property is refused with `INVALID_ARGUMENT`.
`setExcludeFromIndexes(true)` lifts the 1,500-byte limit for a property that no query filters on.

### How the Datastore sink writes

Each writer subtask buffers writes and applies them in one non-transactional `Commit` at a time, waiting for each before it sends the next.
It commits the buffer at every checkpoint barrier, when the next write would take it past `maxBatchMutations` (500 by default) or `maxBatchBytes` (9,000,000 bytes by default, against the documented 10 MiB request limit), and before a write to a key the buffer already holds.
The size is the request's protobuf size on the wire: each mutation's, which the client library computes for an entity, plus the request's project, database and mode fields.
A commit names each key at most once, which the Datastore API requires of a non-transactional commit, and that rule has a useful side effect: the writes of one subtask to one key are applied in the order the serializer returned them, including across retries.
The one exception is a commit that timed out on the client: the writer retries it and moves on, and the service may still apply the abandoned attempt after a later commit to the same key.
Writes from different subtasks are not ordered against each other.

Returning `null` from the serializer skips the record, as on every sink here, and throwing routes it.

### Delivery guarantee in Datastore mode

The Datastore sink is at-least-once and stateless, with the same checkpoint behavior as the Native-mode sink: a completed checkpoint means every record up to it was applied, skipped, or handed to the failure handler.
A record can reach the database twice: after a job restart, when a commit whose outcome never arrived is retried, and when a refused commit is re-sent one write at a time (see below).
A failed non-transactional commit "may not apply as all or none", in the service's own words, so a re-send can repeat a write that was already applied.

| Operation | Same write replayed |
|---|---|
| `upsert`, `delete` | Idempotent for that write |
| `insert` | Refused with `ALREADY_EXISTS`, routed to the failure handler |
| `update` | Idempotent, but if the entity was deleted in between, Datastore answers `NOT_FOUND`, routed to the failure handler |

Under the default `FailureHandler.failJob()`, a stream of `insert` writes, or of `update` writes followed by deletes of the same keys, fails again on every restart whose replay reaches a write already applied.
`upsert` is the operation for a stream that needs neither failure.
There is no exactly-once mode, for the reasons the Native-mode section gives.

### Refused writes in Datastore mode

A `Commit` reports no outcome per mutation: a refusal is the request's, and does not say which write earned it.
The writer therefore confirms a refusal before it routes anything.
When a commit is refused with a status one of its writes could have earned, the writer re-sends each write of the commit as a commit of its own, in order.
A write that succeeds alone is applied; a write refused alone is routed if the status is one that write can earn.

| What the write did | Status | What the sink does |
|---|---|---|
| `insert` of a key that holds an entity | `ALREADY_EXISTS` | Confirmed alone, then routed |
| `update` of a key that holds none | `NOT_FOUND` | Confirmed alone, checked with a lookup, then routed |
| An indexed string over 1,500 bytes, an entity over 1 MiB, a reserved kind or property name | `INVALID_ARGUMENT` | Confirmed alone, then routed |
| `delete` of a key that holds none | *applied* | None |
| `ALREADY_EXISTS` or `NOT_FOUND` for any other operation, and every other status | | **Fails the job** |

Measured against the emulator, 2026-10-03: the refusals in this table applied none of the other writes of their commit, except an entity over 1 MiB, before which the commit had already applied the writes ahead of it in the request; the client library orders a request by operation (inserts, updates, upserts, deletes), not as the writes arrived.
That is why the confirmation pass re-sends every write of a refused commit rather than only the ones it suspects: a write the commit already applied is applied again, which `upsert`, `update` and `delete` absorb, and which an `insert` answers with a routed `ALREADY_EXISTS`.

`NOT_FOUND` is routed only after a lookup of the same key is answered.
The status alone does not say whether the entity or the database is missing, and a job pointed at a missing database could otherwise drop every update it makes.
A lookup of a missing entity is an ordinary answer, while a missing database refuses the lookup as it refused the update, so a refused lookup fails the job instead.
That rests on the service's behavior, which the emulator cannot show, because it serves any database id; the gated suite ([#1546]({{< param BookRepo >}}/issues/1546)) is where it is measured.

Confirming costs one commit per write of the refused commit, sequentially on the task thread, and `mutationsConfirmedAlone` counts them.
`maxConsecutiveRejections` (100 by default) fails the job once that many confirmed `INVALID_ARGUMENT` refusals, or keys addressing another database, arrive with no write applied between them, after routing each of them; a dropping or dead-lettering handler is where it matters.
A key addressing another database counts because it is what the service would refuse with `INVALID_ARGUMENT`, and a sink whose every key names the wrong database is a configuration error.
`ALREADY_EXISTS` and `NOT_FOUND` do not count toward it, because they are what a restart's replay answers.
A transient status anywhere in a failure's cause chain is never routed, and a failure of the request itself, such as `PERMISSION_DENIED`, or the `FAILED_PRECONDITION` a database in Native mode is reported to answer (not yet measured; [#1546]({{< param BookRepo >}}/issues/1546)), fails the job.

`FailedMutation.getPayloadBytes()` is the Java-serialized `DatastoreMutation`; reading it back takes an `ObjectInputStream` with this connector and the Datastore client library on the classpath.
A handler that wants the mutation itself should take `FailureHandler<FailedMutation>` and read `getMutation()`; the bytes exist for the cross-connector dead-letter queue, which sees only the shared `FailedElement` view.
The client library's conversion of an entity to its protobuf is package-private, while its entity and key types are serializable, so this is the only encoding its public API supports.
The reasoning and the declined alternatives are in [ADR-0175]({{< param BookRepo >}}/blob/main/docs/adr/0175-the-datastore-sink-commits-batches-and-confirms-a-refusal-one-write-at-a-time.md).

### Retries and ramp-up in Datastore mode

The writer owns the retry loop, and its client makes one attempt per call.
The client library's own retries would re-send a commit on their own schedule, and the writer has to see each refusal to confirm it.
A commit that failed with `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `ABORTED` or `RESOURCE_EXHAUSTED` is re-sent whole, on the `recovery*` schedule (500 ms doubling to 10 s, jittered, at most 10 attempts by default); exhausting it fails the job, naming `recoveryMaxAttempts`.
Each attempt is bounded by `requestTimeout`, 60 seconds by default.
With the defaults, a service answering every attempt at once with a transient status spends the budget in about a minute of backoff, while one whose every attempt times out takes about eleven minutes, longer than Flink's default checkpoint timeout; each solo re-send of a refused commit has a budget of its own, so size the two settings and the checkpoint timeout together.
The writer logs nothing while it retries; `mutationsRetried` and `errorClass.CODE.errors` are the signal.

By default each writer subtask paces itself with the 500/50/5 rule from Datastore's [best practices](https://cloud.google.com/datastore/docs/best-practices): start new traffic at 500 operations per second, and raise it by at most 50% every five minutes.
The sink as a whole starts at 500, shared among `throttlingParallelism` subtasks (the sink's parallelism unless set), and the budget grows by half for every five minutes past the first five since a subtask's first write.
A restarted job ramps up again from its first write.
`throttlingEnabled(false)` removes the pacing, and a burst may then be answered with `RESOURCE_EXHAUSTED`, which the retry loop absorbs within its budget; the same status for an exhausted quota is not cured by retrying, and fails the job once the budget is spent.
Set `throttlingParallelism` above the sink's parallelism when other writers share the database, so that together they start where the guidance says.

{{< java-snippet file="DatastoreConnectorTuning.java" tag="datastore-connector-tuning" >}}

The same best practices warn that writing one entity at a high rate causes contention, and against keys that increase monotonically; a serializer that derives names from a hash avoids the second.

### Datastore sink metrics

Registered on the Datastore sink writer's metric group.

| Metric | Type | Meaning |
|---|---|---|
| `numRecordsSend` | counter (Flink standard) | Records added to a commit. Counted once per record, never again for a retry or a solo re-send |
| `numBytesSend` | counter (Flink standard) | Their size on the wire, computed from each mutation's protobuf size |
| `numRecordsSendErrors` | counter (Flink standard) | Records routed to the failure handler, whether the serializer rejected them, their key addressed another database, or the service refused the write |
| `recordsSkipped` | counter | Records the serializer returned `null` for |
| `batchesSent` | counter | Commit requests sent: first attempts, retries and solo re-sends alike |
| `mutationsRetried` | counter | Mutations re-sent after a transient failure, one per mutation per retry |
| `mutationsConfirmedAlone` | counter | Mutations re-sent alone to confirm a refused commit, whatever the verdict |
| `throttledMillis` | counter | Cumulative milliseconds the ramp-up throttle has held the writer; its rate is the share of time spent waiting |
| `bufferedMutations` | gauge | Mutations waiting for the next commit |
| `bufferedBytes` | gauge | Their size on the wire |
| `errorClass.CODE.errors` | counter | Failed calls by status code, `CODE` being a gRPC status name or `UNCLASSIFIED`: every transient failure the writer retries, and every final verdict. A refused commit that is then confirmed write by write is counted by those confirmations. A verdict that fails the job is counted as the task fails, so the job's failure cause, rather than this counter, is where it shows reliably |

`errorClass` counts the transient failures the writer recovered from, because the writer sees its own retries; on the Native-mode sink the client library absorbs those.

### Reading in Datastore mode

The Datastore source is bounded, as the Native-mode source is, and reads one of three things, set on the builder as `kind(...)`, `query(...)` or `gqlQuery(...)`, in the namespace `namespace(...)` names (the default namespace when unset).
A **kind** reads every entity of one kind, cut into key ranges read in parallel.

{{< java-snippet file="DatastoreConnectorSource.java" tag="datastore-connector-source" >}}

A **query** is the Datastore API's protobuf `com.google.datastore.v1.Query`, the form the service and the client library's query splitter take; the client library's own `com.google.cloud.datastore.Query` is not accepted, because its conversion to this form is not public API.
The `DatastoreHelper` class the example uses to build a filter comes from `com.google.datastore.v1.client`, in the `datastore-v1-proto-client` library that the Datastore client library brings.
The source cuts a query into key ranges only when it names exactly one kind and filters only with equality (`EQUAL`) and ancestor (`HAS_ANCESTOR`) filters, combined with `AND`, because each range is the query with `__key__ >= start AND __key__ < end` added to it.
A query with an ordering, a `limit`, an `offset`, a cursor, a `DISTINCT ON`, an `OR`, or a filter with another operator or of no type, or one naming no kind or several, is read as one split, by one subtask, which the JobManager logs with the reason.
The splitter itself refuses only an ordering, the `<`, `<=`, `>` and `>=` filters, a filter of no type, and a query not naming one kind; the source refuses the rest too, because `limit`, `offset` and cursors are positions in the whole result rather than in a key range, `!=` and `NOT_IN` are inequalities that a second inequality on `__key__` would meet, and `IN` and `OR` are disjunctions the key range would have to distribute over.
Projections are read as the query states them.
A nearest-neighbour search (`find_nearest`) is refused when the source is built: the service applies a query's cursor and limit before the search, so the page limit and the resume cursor every read sets would change which entities it finds.

{{< java-snippet file="DatastoreConnectorSource.java" tag="datastore-connector-source-query" >}}

A query that needs an index the database does not have fails the job when the read is planned, because the planner reads one entity of it first, and the error then carries the service's answer.
A key range adds a `__key__` inequality to the query, which for a projection may need an index the whole query does not; that is not measured yet ([#1546]({{< param BookRepo >}}/issues/1546)), and a projection that fails that way can be read as one split by giving it `splitCount(1)`.

A **GQL query** is parsed by the service when the read is planned, and the source then reads the query it was parsed into, by the same rules.
Literals are allowed and bindings are not.
To parse it without reading an entity, the source asks for the query with `LIMIT 0` appended; a GQL query that already ends in a `LIMIT` or an `OFFSET` clause cannot take another, so the source parses it by running it as written, which reads and bills its first batch of entities once more.
A GQL query the service refuses fails the job when the read is planned, with the query's text in the message, and so does one that parses into a nearest-neighbour search.

{{< java-snippet file="DatastoreConnectorSource.java" tag="datastore-connector-source-gql" >}}

The snapshot works as the [Native-mode one](#one-snapshot-for-the-whole-read) does: every split reads at one read time, the service's own unless `readTime(...)` sets it, inside the same one-hour or seven-day window, and a job that restarts before its first completed checkpoint plans again, at a new read time unless `readTime(...)` is set.
The planner takes the read time from a one-entity probe of the query, which also fails the job at planning when a configured read time lies outside the window.

### Key ranges and the split count

A kind, or a query that can be split, is cut by the client library's `QuerySplitter`, which samples the kind's keys through the `__scatter__` property, 32 for each boundary, and picks the range boundaries among them.
The splitter sorts what it samples by comparing kind names and key names as Java strings, while the service sorts them by their UTF-8 bytes, and the two orders differ for a name holding a character above U+FFFF beside one holding a character from U+E000 to U+FFFF.
The source therefore lays the splitter's ranges out again in the service's order, so that they still cover each key once.

`splitCount(...)` sets how many ranges to ask for, at most 50,000; it is an upper bound, because a small kind yields fewer sampled keys.
The subtasks left without a range finish immediately, and the JobManager logs a warning; a `splitCount(...)` below the parallelism has the same effect, and so does the emulator, which samples no keys.
Unset, the count is estimated from the database's statistics: the kind's `entity_bytes` in the latest `__Stat_Kind__` entry (`__Stat_Ns_Kind__` in a namespace), one split per 64 MiB, at least 12, at least the source's parallelism, and at most 50,000.
The estimate is the whole kind's size, whatever the query's filter, so an equality query that matches a few entities of a large kind still asks for as many ranges as the kind would; set `splitCount(...)` for such a query.
The service computes statistics periodically rather than as entities are written, so a new kind has none, and the estimate then asks for the lower bound and logs that it found none.
A failure to read the statistics fails the job when the read is planned, with a message naming `splitCount(...)` as the way to skip the estimate.
The 12, the 64 MiB and the 50,000 are Apache Beam's `DatastoreIO` defaults; the parallelism as a further floor is this source's, so that a job of high parallelism over a kind without statistics still gives each subtask a range to read.
At 50,000 ranges the splitter samples and holds about 1.6 million keys on the JobManager while it plans.
A `splitCount(...)` on a query that cannot be split is refused when the source is built, or, for a GQL query, when the read is planned.

The splitter samples the keys at the current time, not at the read time: its read-time variant is a beta API, and the ranges cover the whole key space between them, so an entity of the snapshot falls in exactly one range however the kind has changed since.
The splitter runs over its own HTTP client, the one the `datastore-v1-proto-client` library takes, built on the JobManager only while the read is planned, with the source's credentials or the emulator endpoint.
The readers use a gRPC client built as the sink's is, except that it keeps the client library's retries where the sink's makes one attempt per call.

### Pages and recovery in Datastore mode

A reader fetches a split in pages of `pageSize` entities (500 by default), one `RunQuery` call each, at the split's read time.
The service may answer a call with fewer entities than the page asked for and say more remain, and the next page then starts at the cursor the answer ended at.
A page is held in memory whole before any of its entities is handed on, so `pageSize` times the largest entity bounds what one fetch holds.
An `offset` the service had not finished skipping in one page is carried, reduced, into the next.

The client library retries a call that fails with `UNAVAILABLE` or `DEADLINE_EXCEEDED`, on its default schedule of up to six attempts within 50 seconds; a retried call is the same request at the same read time, so it reads the same entities.
Any other failure, or a call still failing at the end of that schedule, fails the job, which restarts from its last checkpoint under Flink's restart strategy.
No metric or log line counts these retries; a page that is being retried shows only as a lower `entitiesRead` rate.
The call does not answer a thread interrupt: a reader that closes while a call runs waits for its fetcher up to Flink's `source.reader.close.timeout` (30 seconds by default), and a call still running after that fails against the closed client.

The service sets a cursor on every entity it returns, and a checkpoint records, for each split being read, the query started at the cursor after the last entity the job has passed, its `limit` reduced by the entities passed, and its `offset` dropped, because the service returns an entity only once the whole offset is skipped.
A restore resumes there, without reading an entity twice and without skipping one, with one exception the service documents: a query that orders by, or filters with an inequality on, a property holding a list of values removes the duplicates such a property produces only within one request, so across pages, and across a restore, it can return an entity again, and each repeat counts toward the query's `limit`.
The source passes such repeats on as the service returns them.
A job that must not see one either avoids ordering and inequality filters on list properties, or removes repeats downstream in checkpointed state, for example by keying the stream by the entity key and keeping a flag per key; a set kept in the deserializer does not survive a restore, because the deserializer's fields are not checkpointed.
Continuing needs no knowledge of the query, because the cursor is the service's; the emulator tests restore a kind scan across a failure, and continue queries with an ordering, a filter, an `offset`, a `limit` and an end cursor across pages.
The reasoning and the declined alternatives are in [ADR-0177]({{< param BookRepo >}}/blob/main/docs/adr/0177-the-datastore-source-reads-key-ranges-the-client-librarys-splitter-cuts.md).

### Deserializing entities

A `DatastoreEntityDeserializationSchema` turns each entity into zero or more records.
It receives the client library's `Entity`, whose key names it, namespace included, and which holds only the projected properties when the query projects some.
A projection returns a timestamp as an integer of microseconds and a blob as a string, both marked as index values (measured against the emulator, 2026-10-03).
The API also allows a projection result without a key, which an `Entity` cannot hold; the source fails the job on one with a message saying so (none has been observed against the emulator, and the service is measured in [#1546]({{< param BookRepo >}}/issues/1546)).
For a query that projects, the reader replaces those two with the timestamp and the blob they stand for before the deserializer sees the entity, by the rules the client library's own projection results read them back with, so `getTimestamp` and `getBlob` answer on a projected property as on a whole entity; `getLong` and `getString` no longer answer on them.

{{< java-snippet file="DatastoreConnectorSourceDeserializer.java" tag="datastore-connector-source-deserializer" >}}

An entity that produces no record is skipped, counted in `recordsSkipped`, and still passed; a deserializer that throws fails the job, and the entity is read again after the restore.

### Datastore source metrics

Registered on the Datastore source reader's and split enumerator's metric groups.

| Metric | Type | Meaning |
|---|---|---|
| `numRecordsIn` | counter (Flink standard) | Entities handed to the deserializer, one per entity whatever it produced, counted before it runs |
| `entitiesRead` | counter | Entities the reader handed on from its pages, counted when a page arrives |
| `recordsSkipped` | counter | Entities the deserializer produced no record for |
| `splitsAssigned` | counter | Splits the enumerator handed to a reader |
| `splitsReturned` | counter | Splits a failed reader gave back to the enumerator |
| `readsPlanned` | counter | Planning calls that completed: one on a fresh run, zero on one restored from a checkpoint that recorded the plan |

## Testing

Functional coverage runs against the Firestore emulator, the `gcloud emulators firestore` binary in the same `google-cloud-cli` image the Bigtable and Pub/Sub tests use, through testcontainers.
The sink tests drive the production writer-creation path, so the client and `BulkWriter` are the real ones, and a MiniCluster job covers checkpoint-driven and end-of-input flushes.
The source tests read through the production planner and page reader, and a MiniCluster job that fails once after a checkpoint shows a restored split resuming after its last document.
The endpoint reaches the client through the builder, never through `FIRESTORE_EMULATOR_HOST`.
The Datastore-mode tests run the same binary under `--database-mode=datastore-mode`, through the production client and MiniCluster jobs, the source's failing once after a checkpoint as the Native-mode one does; the legacy Datastore emulator, which Google's documentation directs Datastore-mode users away from, is not used.

### Emulator deviations

An emulator is a convenience for fast feedback, never evidence about the service.
Where the two disagree, the service decides.

| Deviation | Consequence |
|---|---|
| Request size is not enforced | A `BatchWrite` of about 10.5 MiB (twelve documents of 900 KiB) was applied. The writer's 9 MiB request budget is untested against a real refusal until the gated suite ([#1546]({{< param BookRepo >}}/issues/1546)) covers it, including whether the service answers an oversized request with `INVALID_ARGUMENT` |
| Rejection statuses are the emulator's | Both error-handling tables were measured against the emulator only. The gated real-GCP suite ([#1546]({{< param BookRepo >}}/issues/1546)) is where it is confirmed |
| No IAM checks | The emulator accepts its placeholder token for everything, so `PERMISSION_DENIED` is not exercised |
| No ramp-up or quota behavior | Throttling and `RESOURCE_EXHAUSTED` handling are not exercised |
| `PartitionQuery` is not implemented | Asking for two partitions fails with `UNIMPLEMENTED`; one partition is the client library's own answer, made without a call. The source's emulator tests choose partition boundaries themselves, so the service's partitioning and its partition counts are exercised only by the gated suite ([#1546]({{< param BookRepo >}}/issues/1546)). A scan against the emulator needs a partition count of one |
| Any Datastore-mode database id is served | A database that was never created answered a lookup and an update alike, so the lookup that tells a missing entity from a missing database before a `NOT_FOUND` is routed is exercised only by the gated suite ([#1546]({{< param BookRepo >}}/issues/1546)) |
| Datastore-mode request size is not enforced | A commit of about 10.5 MiB was applied, so `maxBatchBytes` is untested against a real refusal until the gated suite covers it |
| Old read times are answered | A read time two hours old was answered, in both modes, where the service keeps versions for one hour without point-in-time recovery. The read-time window is exercised only by the gated suite ([#1546]({{< param BookRepo >}}/issues/1546)) |
| `__scatter__` sampling finds no keys | The splitter's sampling query answers with no entity, so the splitter answers every request with the whole query. The Datastore source's emulator tests choose key-range boundaries themselves, so the service's sampling and its range counts are exercised only by the gated suite ([#1546]({{< param BookRepo >}}/issues/1546)) |
| No statistics | `__Stat_Total__` and `__Stat_Kind__` are empty, and so are a namespace's `__Stat_Ns_Total__` and `__Stat_Ns_Kind__`, so the split-count estimate always takes its lower bound against the emulator. Unit tests cover the estimate from statistics shaped as the service documents them; the estimate from real statistics belongs to the gated suite ([#1546]({{< param BookRepo >}}/issues/1546)) |
| An offset-only batch carries no cursor | A `RunQuery` with an `offset` and a limit of zero reports the entities it skipped with neither a skipped cursor nor an end cursor, where the service documents a skipped cursor. No page the source reads asks for a limit of zero (only the GQL parse does, and it takes no cursor from the answer), and a batch that returned entities does carry its end cursor |

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
