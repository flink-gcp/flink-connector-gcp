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

# ADR-0171: The Firestore sink writes through BulkWriter and confirms invalid writes alone

- Status: Accepted
- Date: 2026-09-27 (client library facts read in google-cloud-firestore 3.46.0 through
  libraries-bom 26.87.0; emulator behavior measured 2026-09-27 against
  `google-cloud-cli:583.0.0-emulators`, one run); revised 2026-10-10 by [#1680] (`add`)
- Issues: [#1540], [#355], [#1556], [#1606], [#1680]
- Modules: firestore (`sink`, `sink.writer`)
- Current behavior: `docs/content/docs/connectors/datastream/firestore.md` § Sink, § Error handling

## Context

[#355]'s design comment settled the shape of the Native-mode sink: a serializer SPI returning a connector-owned `FirestoreWrite`, a writer on the client library's `BulkWriter`, at-least-once delivery through a flush at each checkpoint barrier, and the shared `base.failure` SPI.
It left four decisions to the implementation: which statuses are routed, whether `FAILED_PRECONDITION` gets a policy, whether the sink or the library owns the retry loop, and what the value type carries.
Reading the library and measuring it against the emulator added facts the design comment had not known, two of them defects.

## Evidence

Read from google-cloud-firestore 3.46.0:

- `BulkWriter` sends a request when it holds 20 writes (10 once the open batch holds a retry), when a same-document write forces a new batch, or when `flush()` is called, and on no timer. A write it retries after a backoff joins whatever batch is open at that moment; the library sends that batch only if a flush is still outstanding when an earlier batch completes, so a retry arriving after the last flush waits for the next one.
- After `RESOURCE_EXHAUSTED` the library sets the full 60-second backoff from the first retry (`BulkWriterOperation.updateBackoffDuration`; the identity comparison holds because per-write statuses come from `Status.fromCodeValue`), so a throttled write can take about ten minutes to exhaust the default budget.
- Its throttle opens the first batch at 20 writes before lowering the batch size to a slower configured rate, and a full batch larger than the rate cannot pass it: `sendBatchLocked` reschedules itself with no delay until the ramp-up lifts the rate to 20, or without end when the ceiling is below 20.
  Past 500 pending operations it queues further writes in an unbounded list instead of sending them, and never blocks the caller.
- It retries a failed write through a replaceable error listener; the default retries `RESOURCE_EXHAUSTED`, `UNAVAILABLE` and `ABORTED` (the `BatchWrite` call settings' retryable codes) while at most ten attempts have failed.
  The transport also retries each `BatchWrite` call, within a 60-second total timeout.
- A failure of the whole `BatchWrite` request is reported to every write the request carried, with the request's status (`BulkCommitBatch.processExceptions`).
- `close()` flushes and waits without a bound. Given an executor through `BulkWriterOptions`, a `BulkWriter` owns no threads.
- Only `Precondition.updatedAt(Timestamp)` is public; `Precondition.exists(...)` is package-private. `set` takes no precondition, `create` implies that the document does not exist, and `update` implies that it does.
- `update(DocumentReference, Map)` splits each key on dots; the `FieldPath` overload does not.

Measured against the emulator, each shape sent as the only write of its request; then, inside a request with five valid writes, a create of an existing document, an update of a missing one, a stale update, and a reserved field name:

| Write | Status | Reported for |
|---|---|---|
| `create` of an existing document | `ALREADY_EXISTS` | the write |
| `update` of a missing document | `NOT_FOUND` | the write |
| `update` or `delete` with a stale update time, whether or not the document exists | `FAILED_PRECONDITION` | the write |
| `delete` of a missing document | applied | |
| a reserved field name or document id, a document over 1 MiB, nested arrays (Standard edition), a map nested past 20 levels | `INVALID_ARGUMENT` | **every write of the request** (measured for the reserved field name; the others alone only) |
| twelve documents of 900 KiB in one request (about 10.5 MiB) | applied | the emulator does not enforce the 10 MiB request limit |

Two defects, measured the same day and pinned by `BulkWriterDefectsITCase`:

- **A failed write never releases its pending-operation slot.** The slot is released in a `catchingAsync` for `ApiException`, and a failed write completes with a `BulkWriterException`, which extends `FirestoreException` rather than `ApiException`. After 500 failed writes, a new write on the same `BulkWriter` never completed within the observation window, while the same write on a fresh `BulkWriter` did.
- **A synchronous refusal corrupts its request.** A value the library cannot encode (a `Short`) makes `set` throw after the operation has already been queued. The next write of the same request was applied to the database, and its future never completed.

## Decision

### The writer

- **The writer sends through one `BulkWriter` per subtask, on an executor the writer owns.** It never calls `BulkWriter.close()`: on success a flush has already drained it, and on failure an unbounded flush is what a closing task must not do. Closing shuts the executor down, which cancels every send and retry still scheduled.
- **The writer runs the mailbox model** of the Bigtable and Pub/Sub writers: completions re-dispatch onto the task mailbox, and all logical state stays on the task thread. It bounds unanswered writes by count (`maxInFlightWrites`, at most 500, the library's pending ceiling, default 250) and by size (`maxInFlightBytes`), and every pass of a wait that finds the mailbox empty asks the library to send, because a retry can join a batch after any earlier request to send. The default leaves half the library's slots for failed writes, so a burst of refusals does not force a `BulkWriter` replacement, and its ramp-up restart, per refusal.
- **The writer asks the library to send before the bytes since the last send would pass 9 MiB**, a mebibyte under Firestore's documented 10 MiB request limit for the request's envelope. A write's bytes are the protobuf wire size of the `Write` the library sends for it, masks and preconditions included, computed from the values and held equal to the serialized message by a test; Firestore's storage-size formula was the first choice and was rejected on measurement: for 100,000 boolean fields of one to four characters the formula gives 552,064 bytes and the serialized `Write` 1,152,062, about 2.09 times (measured in self-review, 2026-09-27). A first-attempt batch never spans a send, so none grows past the budget; a retried write rejoins a later batch outside the count, and a request it pushes past the limit is refused whole and confirmed write by write.
- **Neither throttle rate may be below 20**, the library's batch size, because of the throttle fact above.
- **The writer replaces its `BulkWriter` before a submission could reach the pending ceiling**, counting the slots its failed writes hold, and only once nothing is in flight. A fresh `BulkWriter` restarts the throttle's ramp-up, which `bulkWritersReplaced` counts.
- **`FirestoreWrite` accepts a closed value vocabulary** (`null`, `String`, `Long`, `Double`, `Boolean`, `Timestamp`, `GeoPoint`, `Blob` of subtype 0, `FirestoreDocumentReference`, `List`, `Map` with non-empty `String` keys, nested no deeper than the library's recursion bound) and checks it when the write is built, so a value the library would refuse synchronously is a routed serialization failure instead. A synchronous refusal that still reaches the writer fails the job. `FirestoreDocumentReference` joined on 2026-10-03 ([#1606]): it is a connector-owned, serializable document path that the writer replaces, at any depth, with a `DocumentReference` of its own client just before the write reaches `BulkWriter`, because the library's `DocumentReference` cannot travel in a serializable write. The replacement is not optional: the library encodes an object of a class it does not know as a bean (`CustomClassMapper`, google-cloud-firestore 3.49.0), so an unreplaced reference would be stored as a map with a `documentPath` field, without an error; `FirestoreWriterITCase` reads each operation's reference back as a `DocumentReference`. The subtype restriction dates from 2026-10-03: google-cloud-firestore 3.49.0, adopted with libraries-bom 26.90.0, made BSON binary `Blob`s of other subtypes constructible, and the library encodes them as a reserved map that the request-size accounting does not count ([#1589]).
- **Every field name is literal in every operation**; `update` goes through the `FieldPath` overload, so `a.b` never means a path in one operation and a name in another.
- **The library owns both retry loops.** The writer installs its own error listener, which keeps the library's retryable set (pinned by a test against the call settings) and makes the attempt count configurable as `writeMaxAttempts`, defaulting to the library's 11; that loop's backoff has no knobs because the library exposes none. The transport's retries of each `BatchWrite` call are set through `FirestoreOptions.setRetrySettings`, which the library applies to every call, and the sink exposes them as the Pub/Sub sink's `retry*` options, an unset one keeping the library's `BatchWrite` value (five attempts, which `GrpcFirestoreRpc` sets over the generated settings). Building the sink refuses a maximum retry delay or attempt timeout shorter than its initial value, counting an unset one as the library's, which gax would otherwise refuse with a message naming no option; and, because the library treats settings equal to gax's `ServiceOptions` defaults as unset, it refuses overrides that add up to exactly those. The write budget is not `retryMaxAttempts`, so that the name means the transport's attempts here as it does on the Pub/Sub sink.

### Routing

`INVALID_ARGUMENT` may reach a handler that might drop it because gRPC defines it as state-independent (ADR-0042). `ALREADY_EXISTS` for a create and a failed precondition are state-dependent, but on a state the write itself chose, its own earlier application or the precondition the pipeline set, which is ADR-0076's argument for routing `ALREADY_EXISTS`. ADR-0042's two halves apply to all of them: no transient status anywhere in the cause chain, and the first classifiable status decides.

- **`INVALID_ARGUMENT` is routed after solo confirmation** (ADR-0045 shape). Because the library reports a request's status against every write, the writer parks a write refused with it and, before the next record, re-sends each parked write alone through the same `BulkWriter`, after draining everything else. Only a refusal that repeats alone is routed; a write that succeeds alone is applied. `maxConsecutiveRejections` bounds a run of confirmed refusals.
- **`ALREADY_EXISTS` for an `add` is not routed: the writer sends the record again under a new id** (revised 2026-10-10, [#1680]). `FirestoreWrite.add(collectionPath, fields)` draws a 20-character id from a `SecureRandom`, the client library's `CollectionReference.add` shape, and sends a `create` of it, so a drawn id that names an existing document cannot replace it. The refusal is not the record's failure, so the writer parks the write and, before the next record or within the flush, sends it again through `FirestoreWrite.add` under a new id, waiting for room under the in-flight bounds like a record. The same refusal answers a retry of a create that was applied but whose answer was lost: the transport retries a `BatchWrite` on `RESOURCE_EXHAUSTED`, `UNAVAILABLE` and `ABORTED` (`retry_policy_6_codes`, google-cloud-firestore 3.49.0), and `BulkWriter` retries a write on the same set. The writer cannot tell a collision from its own retry without a read, so a retry stores the record twice; the owner accepted duplicates and refused silent replacement (2026-10-10). `idsRedrawn` counts the re-sends, and ten refusals in a row for one record fail the job. Measured on the emulator on 2026-10-10 (one run, `FirestoreWriterITCase`): the existing document kept its fields and the record was stored under a new id.
- **`ALREADY_EXISTS` is routed for any other `create`**, as it arrives. It is what a replayed create answers (ADR-0076's reasoning), and the measurement shows it for the write alone. Neither it nor a routed `FAILED_PRECONDITION` counts toward `maxConsecutiveRejections`: both are what a restart's replay answers for every create and conditional write it repeats, so counting them would fail a job again on each restart whose replay window is longer than the bound.
- **`FAILED_PRECONDITION` is the job's choice through `preconditionFailurePolicy`, defaulting to `FAIL_JOB`**, and only for a write that carries a `lastUpdateTime`. A pipeline writing conditionally may well want lost races captured, which is the ADR-0076 argument for an opt-in over a fixed rule. The status on a write without a precondition always fails the job, because it then describes the database; a database in Datastore mode is reported to refuse every Firestore API write with it (unmeasured; [#1546]). A missing database is likewise expected to answer `NOT_FOUND` for every write, which is why `NOT_FOUND` is never routed; the emulator cannot show it, and the gated suite will measure both.
- **`NOT_FOUND` is never routed.** A missing database and an update of a missing document share it, and routing it would drop every record of a misconfigured job.
- **Everything else fails the job**, including those statuses on any other write.

### What the sink does not have

- **No destination resolver and no create disposition.** A write names its own document path, collections are implicit, and creating a database is provisioning (ADR-0138's structural-difference rule).
- **No `batchesSent` metric.** The library exposes no hook for a request it sends.
- **No same-document ordering.** The library sends several requests at once and retries in later requests, and `BatchWrite` applies a request's writes in no guaranteed order. This matches the Spanner and Bigtable sinks, and an opt-in per-document order is [#1556].
- **No flush or progress timeout.** Every attempt is bounded by the transport's total timeout (60 seconds by default) and the attempt budget, and the one stall the options could cause is refused; a wait silent for three minutes, a minute past the longest gap between two answers to a write the library keeps retrying, logs a warning instead. The library can still stall on a defect of its own (its rate limiter throws, unobserved, if the wall clock steps backwards); a streaming job's checkpoint timeout ends that, and a bounded job relies on the warning.

The public types enter at `@PublicEvolving` (ADR-0170).

## Consequences

- An `INVALID_ARGUMENT` costs one solo round trip per write of the refused request, sequentially on the task thread. A stream whose data is broken wholesale therefore slows to one write per round trip until `maxConsecutiveRejections` or the default `failJob()` handler stops it.
- Under the default `failJob()` handler, a stream of creates or conditional writes fails again on each restart whose replay reaches a write already applied. The docs say so beside the replay table.
- The two defect workarounds are held by `BulkWriterDefectsITCase`, which fails when a library release fixes either defect, so the workaround is re-examined rather than kept on faith.
- The emulator enforces no request size, so the 9 MiB budget is exercised against a real refusal only by the gated suite ([#1546]).

## Alternatives declined

- **A hand-rolled `BatchWrite` batcher on Apache Beam's `RpcQosOptions` shape** (batches of up to 500, adaptive sizing and throttling). Google's guidance for bulk entry is a bulk writer or parallel individual writes, and `BulkWriter` brings the ramp-up and retry schedule with it; Beam was read as a design reference only. Reopen if the library's 20-write batches measure as the throughput ceiling in the gated suite.
- **Solo confirmation through a single-write `Commit`** instead of the same `BulkWriter`. A `Commit` has its own retry settings and bypasses the throttle, and the confirmation should answer through the same RPC the write first failed on.
- **Solo confirmation for every routed status.** `ALREADY_EXISTS` and `FAILED_PRECONDITION` are answered for the write in the measurement, and confirming them would cost one round trip per replayed create after every restart.
- **Waiting on `BulkWriter.flush()`'s future.** A write the library mishandles keeps that future pending forever; the writer waits on its own ledger of answered writes instead.
- **Closing a replaced or finished `BulkWriter`.** `close()` waits without a bound; with a writer-owned executor there is nothing to release.
- **Accepting the library's whole value vocabulary** (`Integer`, `Float`, `Date`, `DocumentReference`, POJOs). Each is either a convenience a `Long`, `Double` or `Timestamp` already covers, not serializable, or bound to a client instance, and the closed list is what makes the synchronous-refusal defect unreachable. `FieldValue` transforms are additive later ([#355]); the reference type was added this way ([#1606]).
- **Accepting the library's `DocumentReference` in `FirestoreWrite` and keeping only its path** instead of a type of this connector's own ([#1606]). A serialization schema has no client to make a `DocumentReference` from, so a pipeline that builds writes from records would still need a path-only value; and a reference from another database would be silently re-pointed at the sink's database, which the write cannot detect because it does not know where it will be sent. `FirestoreDocumentReference` carries only the path, so that limit is part of the value's shape rather than a hidden rewrite.
- **Naming the reference value `FirestoreReference`, `DocumentReferenceValue` or `FirestoreDocumentPath`** ([#1606]). `FirestoreDocumentReference` is the product prefix on the client library's word, the shape `FirestoreWrite` and `DatastoreMutation` already take for a connector-owned stand-in of a vendor type ([ADR-0137]'s vendor's word at an SDK seam; [ADR-0175]). The prefix is required rather than decorative: a pipeline that copies documents imports the library's `DocumentReference` beside it, the import clash [ADR-0137] renamed `PubSubStartPosition` to avoid. `FirestoreReference` drops the library's word for no real difference, an unprefixed `DocumentReferenceValue` reads as one more library value beside `GeoPoint` and `Blob`, and a path-named type names the representation rather than what Firestore stores, and would read as a write's own target (`getDocumentPath()`).
- **Accepting the BSON values of google-cloud-firestore 3.49.0**: a `Blob` of a non-zero subtype, sized in its reserved-map form, and the seven BSON value classes (`MinKey`, `MaxKey`, `RegexValue`, `BsonObjectId`, `BsonTimestamp`, `Int32Value`, `Decimal128Value`), which the library also encodes as reserved maps. Measured on 2026-10-03 against temporary databases: a Standard-edition database stores such a `Blob` and refuses the seven classes ("Value type INT is not allowed"), as the emulator does, and an Enterprise-edition database stores all of them. They were still declined: no write this sink was built for needs them, accepting the `Blob` would mean sizing its map form, and admitting them later widens the closed list without breaking a pipeline written against it. The cost is that a document the source reads can hold a value this sink refuses ([#1589]).
- **Reading the document after an `add`'s `ALREADY_EXISTS`**, to count a create whose stored fields equal the write's as applied rather than send it again ([#1680]). It would store a record twice only after a restart rather than also after a lost answer, but it adds a read path for a rare branch and needs field comparison that tolerates the service's normalization, such as timestamps cut to microseconds; the owner chose the simpler rule on 2026-10-10.
- **Service-assigned ids through `CreateDocument` with no `document_id`** for `add` ([#1680]). It is one RPC per document, outside `BatchWrite` and so outside `BulkWriter`'s throttle and solo confirmation, and the transport retries it on `RESOURCE_EXHAUSTED` and `UNAVAILABLE` (`retry_policy_0_codes`, 3.49.0), so a lost answer stores the document twice there too.
- **Failing a job whose `FIRESTORE_EMULATOR_HOST` is set without an emulator endpoint.** The library reads the variable itself and the connector cannot stop it; a warning names it, and failing would break a deployment that sets it deliberately.

[#355]: https://github.com/flink-gcp/flink-connector-gcp/issues/355
[#1540]: https://github.com/flink-gcp/flink-connector-gcp/issues/1540
[#1546]: https://github.com/flink-gcp/flink-connector-gcp/issues/1546
[#1556]: https://github.com/flink-gcp/flink-connector-gcp/issues/1556
[#1589]: https://github.com/flink-gcp/flink-connector-gcp/issues/1589
[#1606]: https://github.com/flink-gcp/flink-connector-gcp/issues/1606
[#1680]: https://github.com/flink-gcp/flink-connector-gcp/issues/1680
[ADR-0137]: 0137-a-cross-connector-name-diverges-only-to-name-a-real-difference.md
[ADR-0175]: 0175-the-datastore-sink-commits-batches-and-confirms-a-refusal-one-write-at-a-time.md
