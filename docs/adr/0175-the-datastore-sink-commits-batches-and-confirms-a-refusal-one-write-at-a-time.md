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

# ADR-0175: The Datastore sink commits batches and confirms a refusal one write at a time

- Status: Accepted
- Date: 2026-10-03 (client library facts read in google-cloud-datastore 3.4.0 through
  libraries-bom 26.87.0 and found unchanged in 3.7.0 through 26.90.0; emulator behavior measured
  2026-10-03 against
  `google-cloud-cli:587.0.0-emulators` in Datastore mode, one run); revised 2026-10-10 by [#1651]
  (writer-side id allocation for the table sink) and 2026-10-11 by [#1707] (the service's answers,
  and an insert a refused commit already wrote), [#1729] (a commit the writer encodes, and the
  merge operation), [#1744] (each write encoded once), [#1743] (the masked update operation) and
  [#1750] (missed updates count toward the rejection bound), and 2026-10-17 by [#1819] (the service
  refuses an array inside an array), and 2026-10-24 by [#1895] (it stores one again)
- Issues: [#1542], [#355], [#1546], [#1651], [#1707], [#1729], [#1744], [#1743], [#1750], [#1819], [#1895]
- Modules: firestore (`io.github.flink.gcp.connector.datastore`: `sink`, `sink.writer`)
- Current behavior: `docs/content/docs/connectors/datastream/firestore.md` § Datastore mode

## Context

[#355]'s design comment settled the shape of the Datastore-mode sink: a serializer returning an upsert, insert or update over a client-library entity with a complete key, or a delete over a key; non-transactional `Commit` batches bounded by count and bytes; a retry loop the sink owns; ramp-up throttling on by default; and solo confirmation of a row-level status, because a `Commit` reports no outcome per mutation.
It left the confirmation that the client library's types serialize, the routed statuses' exact conditions, and the write path's client surface to this implementation.
The Native-mode sink (ADR-0171) shares the module but nothing else (ADR-0170).

## Evidence

Read from google-cloud-datastore 3.4.0, and unchanged in 3.7.0 (the files below compared identical):

- `Datastore.newBatch()` sends every write it holds in one `CommitRequest` in `NON_TRANSACTIONAL` mode, mixing inserts, upserts, updates and deletes. A second write to a key it already holds is merged into the first or refused on the client (`BaseDatastoreBatchWriter`), never sent.
- The per-operation methods (`put`, `add`, `update`, `delete`) each send a commit of one operation; `put`, `update` and `delete` deduplicate a repeated key silently, and `add` refuses one.
- `DatastoreImpl` runs every call through `RetryHelper` with the client's `RetrySettings`, retrying `ABORTED`, `DEADLINE_EXCEEDED` and `UNAVAILABLE`, and `GrpcDatastoreRpc` applies the same settings to every generated call setting. One single-attempt setting therefore turns off both layers.
- The client takes its endpoint from `DATASTORE_EMULATOR_HOST` only when built without a host (its telemetry reads the variable too, to turn client-side metrics off, which does not move the endpoint), and switches to plaintext with no credentials when its credentials are `NoCredentials`.
- The `datastore.v1` proto requires that "no two mutations may affect a single entity" in a `NON_TRANSACTIONAL` commit.
- `Datastore.get` re-sends a `Lookup` for as long as the service answers with the key deferred, with no bound (`ResultsIterator.loadResults`), so neither a call timeout nor a retry budget bounds it. The client's RPC object (`ServiceOptions.getRpc()`, a `DatastoreRpc`) sends one `Lookup` under the client's own call settings.
- `FullEntity`, `Key` and every `Value` are `Serializable`. Their protobuf conversions (`toPb`) are package-private; `Entity.calculateSerializedSize` is public.
- The `datastore.v1` proto documents a non-transactional commit as one whose "mutations may not apply as all or none", and its `database_id` fields refuse `(default)` in favor of an empty id.

Measured against the emulator, each shape sent alone and, where the table says so, inside a commit with valid upserts:

| Write | Status | Rest of the commit |
|---|---|---|
| `insert` of an existing key | `ALREADY_EXISTS` | not applied |
| `update` of a missing key | `NOT_FOUND` | not applied |
| an indexed string over 1,500 bytes, a reserved kind or property name | `INVALID_ARGUMENT` | not applied |
| an entity over 1 MiB | `INVALID_ARGUMENT` | **the write before it applied**, the write after it not (two runs) |
| a key of another project or database | `INVALID_ARGUMENT` ("mismatched databases within request") | not applied |
| `delete` of a missing key | applied | |
| a commit of about 10.5 MiB | applied | the emulator does not enforce the 10 MiB request limit |
| a lookup of a missing key | answered | |
| an update and a lookup against a database id that was never created | `NOT_FOUND`, answered | the emulator serves any database id |

The oversized-entity row is the proto's "may not apply as all or none" observed rather than read: the emulator applied the request's mutations in order and stopped at the refused one, so the write ahead of it was in the database when the refusal arrived. The request's order is not the writer's: the client's `Batch` groups mutations by operation (inserts, updates, upserts, deletes; `BaseDatastoreBatchWriter.toMutationPbList`), so which writes of a refused commit were applied depends on their operations as well as their positions ([#1729] replaced `Batch` with the writer's own encoding, in the writer's order; its revision below re-measures the emulator). That is what the confirmation pass is built for: every write of a refused commit is re-sent alone, and a write the commit had already applied is applied again, which is safe for `upsert`, `update` and `delete` and surfaces as a routed `ALREADY_EXISTS` for an `insert`.

## Decision

### The write path

- **The SPI carries the client library's own types**: `DatastoreMutation` holds a `FullEntity<Key>` or a `Key`, and its factories take `FullEntity<Key>` so an incomplete key is a compile error, with a runtime check for an erased or raw caller. That they serialize was confirmed by a round trip of every value type; `FailedMutation`'s payload is the Java-serialized write, the Native-mode sink's choice for the same reason (no public protobuf conversion).
- **The writer commits through `Batch`** (until [#1729], after which the writer encodes the `CommitRequest` itself; see its revision), one request at a time on the task thread, with no mailbox: the Spanner sink's synchronous shape (ADR-0075). It commits before a write to a key the buffer already holds, so `Batch` never merges or refuses one, and a commit never names a key twice. A consequence the docs state: one subtask's writes to one key are applied in submission order, retries and solo re-sends included, with one exception the writer cannot see: an attempt that timed out on the client may still be applied by the service after a later commit.
- **Batches are bounded by `maxBatchMutations` (500) and `maxBatchBytes` (9,000,000, at most 10 MiB)**: 500 is Apache Beam's ceiling on its adaptive batch size (Beam starts at 50) and 9,000,000 bytes Beam's byte threshold; Datastore documents no count for a non-transactional commit, so the count is the connector's choice. A mutation's size is its protobuf size: the client library's entity size plus the framing of the mutation and request fields, computed by `MutationSizeEstimator`; the request's own project, database and mode fields count against the cap too, so the 10 MiB ceiling holds for the whole request.
- **A key must address the sink's project and database**, checked before the write joins a batch and routed when it does not, because the service refuses the whole commit for it. A key whose database id is null (the client library does not refuse one) is routed the same way.
- **The writer owns the retry loop** (ADR-0075 shape): the client makes one attempt per call, bounded by `requestTimeout` (60 s, the generated commit setting's own timeout), and a commit failing with `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `ABORTED` or `RESOURCE_EXHAUSTED` is re-sent whole on the `recovery*` schedule. `INTERNAL` is not retried, as on the Native-mode sink.
- **The writer re-checks a deserialized options object** through the builder's own setters, the batch bounds, the request timeout and the rejection bound alike (ADR-0068 leaves the re-check per connector).
- **A confirmation pass stops at an interrupt** before its next solo commit, because the client's blocking call does not observe one.
- **Ramp-up throttling is on by default**, re-implemented on Beam's `RampupThrottlingFn` shape: 500 operations per second shared by `throttlingParallelism` subtasks (the sink's parallelism unless set), growing by half for every five minutes past the first five since a subtask's first write (Beam measures from one job-wide instant and hints 500 workers by default), one permit per record in `write()`, on a monotonic clock; retries and solo re-sends are not throttled.

### Routing

- **A commit refused with a status one of its writes could have earned is confirmed one write at a time** (ADR-0045 shape): `INVALID_ARGUMENT` for any commit, `ALREADY_EXISTS` only beside an insert, `NOT_FOUND` only beside an update. Each write is re-sent as a commit of its own, in order, with its own retry budget. A write applied alone was collateral; a write refused alone is routed if the status is one its operation can earn. A commit of one write is its own confirmation. Every other refusal of a commit fails the job without routing.
- **`NOT_FOUND` is routed for an update only after a lookup of the same key is answered**, sent as one `Lookup` through the client's RPC object rather than `Datastore.get`, whose deferral loop is unbounded; any answer, a deferral included, shows the database is there. The status does not say whether the entity or the database is missing; a missing entity is an ordinary lookup answer, while a missing database is expected to refuse the lookup too, which then fails the job. The owner chose this discriminator over routing on the status alone and over never routing (the Native-mode sink's choice, ADR-0171) on 2026-10-03. The expectation is not measured: the emulator serves any database id, and [#1546] owes the measurement.
- **`maxConsecutiveRejections` (100) counts confirmed `INVALID_ARGUMENT` refusals, keys addressing another database, and routed `NOT_FOUND` refusals of an update**. A key addressing another database is what the service would refuse with `INVALID_ARGUMENT` had the writer sent it (round two found that leaving it uncounted let a misconfigured sink shed its whole stream under a dropping handler), and a `NOT_FOUND` is what a serializer naming the wrong kind or namespace earns for every update; the [#1750] section below weighs it against a restart's replay. `ALREADY_EXISTS` for an insert is what a restart's replay answers, the ADR-0171 reasoning for leaving replay answers out.
- Classification reads the cause chain both ways (ADR-0042): a transient status anywhere wins; otherwise the outermost status decides.

### Names

The owner settled three names on 2026-10-03 under ADR-0137, departing from the issue's working names:

- **`DatabaseDestination`**, in the Datastore root, rather than `DatastoreDatabase`: ADR-0137 renamed Spanner's `SpannerDatabase` for naming nothing a `*Destination` does not. It shares a simple name with the Native-mode type in the same module; the two differ in the default database's spelling (empty here, `(default)` there), which is why they are two types.
- **`DatastoreMutation`**, `DatastoreMutationSerializationSchema` and `FailedMutation#getMutation()`, rather than a `DatastoreWrite` beside a `FailedMutation`: one noun per write path, and at an SDK seam the vendor's word, which here is the Datastore API's `Mutation` (the Spanner sink's vocabulary, so `failedMutationHandler`, `maxBatchMutations`, `bufferedMutations` and `mutationsRetried` carry over unchanged; `mutationsConfirmedAlone` is new here).
- **`requestTimeout`** rather than `commitTimeout`: the bound covers every call the writer makes, the lookup included, the Bigtable and Cloud Tasks spelling for such a bound.

The public types enter at `@PublicEvolving` (ADR-0170).

### What the service applies of a refused commit ([#1707], 2026-10-11)

`DatastoreRejectionRealGcpITCase` measured the table above against the service (one run, Standard edition, google-cloud-datastore 3.7.0). Every status holds, and so does the lookup discriminator: a database that does not exist, and a malformed id (`Bad_Id`), refuse the update and the lookup alike with `NOT_FOUND`. Three things differ from the emulator.

- **A commit refused with `ALREADY_EXISTS` or `NOT_FOUND` applies every other write it carries**, wherever the refused write sits in the request (four positions, and one refusal among twenty upserts). The emulator applies none of them (in `Batch` order, which sent the refused insert or update ahead of every upsert; the [#1729] revision re-measured it in the writer's order).
- **A commit refused with `INVALID_ARGUMENT` applies none of its writes**, the entity over 1 MiB included, where the emulator applied the write ahead of it.
- **A commit of about 10.5 MiB is applied**, as on the emulator, so `maxBatchBytes` rests on the documented limit rather than on a refusal anyone has seen.

The first broke the confirmation pass for inserts. A commit holding one insert of an existing key and several new ones was refused, the service wrote the new ones, and their re-sends then answered `ALREADY_EXISTS` and were routed as duplicates although they were written. The emulator never showed it, and the unit tests' fake modelled the emulator. The writer now looks the key up before routing an insert's `ALREADY_EXISTS`, through the same one-call `Lookup` the `NOT_FOUND` discriminator uses, now returning the entity it finds. It counts the insert as applied when the key holds exactly the entity the insert writes, every property value and index flag alike (the owner asked for the fix in this change, 2026-10-11).

- That also counts as applied a retried commit that had in fact been applied, and a replay of an identical insert after a restart. Both were routed before, as the documented cost of at-least-once; now the key holds what the record asked for, and routing it would report a write that succeeded.
- An insert whose key holds a different entity is routed as before. The lookup is retried on a transient status within the recovery budget, like the `NOT_FOUND` lookup. One that still fails routes the insert with the lookup's failure attached as suppressed; a stored entity the client library cannot read (an array inside an array, which the service has both stored and refused; measured stored on 2026-10-11, refused on 2026-10-17 [#1819], and stored again on 2026-10-24 [#1895]) is answered as no entity and routes it with nothing attached, the decoding failure logged at DEBUG. Either way a failed lookup costs no more than the routing it replaces.
- Neither outcome counts toward `maxConsecutiveRejections`. A match ends a run of rejections as an applied write does, and a routed insert leaves it as it was (the [#1750] section below).
- A timestamp finer than a microsecond is stored floored, so an insert carrying one never matches and is routed as before.
- Declined: sending inserts one per commit, which would cost a request per insert to avoid a refusal that is rare outside a replay, and reading the refused key out of the error message, whose wording is not an API.

`DatastoreWriterRealGcpITCase` drives the writer against the service through such a commit, and the fake behind `DatastoreWriterTest` gained the service's semantics. Removing the lookup fails both.

A database in Native mode does not answer the Datastore API with `FAILED_PRECONDITION`, as the first cut's classifier Javadoc had it reported: it applied a commit and answered a lookup and a query, and the entity was then a document to the Firestore API.

### Id allocation for the table sink ([#1651], 2026-10-10)

The writer gained one internal capability: it allocates ids (`AllocateIds`), `idAllocationBatchSize` at a time (a writer option, 1,000 by default; ADR-0184 records the measurement), through its own client, credentials, single-attempt deadline and recovery budget, retrying a transient failure as it retries a commit, and hands that allocator to a serializer implementing the `@Internal` `KeyAllocatingSerializationSchema` when the writer is created. The Datastore-mode table sink uses it for a table without a PRIMARY KEY (ADR-0184). The public SPI is unchanged: `DatastoreMutation` still takes complete keys only, and a user serializer that wants service ids allocates them before the sink, as the Decision's complete-key rule requires. An allocation the service refuses, a spent budget, a short answer or an interrupted backoff fails the job whatever the failure handler, as a commit the budget cannot finish does: it is the database's failure, not the record's.

### A commit the writer encodes, and the merge operation ([#1729], 2026-10-11)

[#1729] made a merge the default write of the Datastore-mode table sink (ADR-0184): an `upsert` whose `Mutation.property_mask` names the values the row holds, so the properties it does not hold keep their values. The client library's `Batch` builds its `Mutation` messages itself and has no way to set a mask (google-cloud-datastore 3.7.0), so the owner chose (2026-10-11) to send every commit through one path the writer encodes, over sending the masked writes in commits of their own beside `Batch`, which would split one buffer into two requests and the confirmation pass into two routes.

- **`DatastoreMutation.upsertMerge(FullEntity<Key>)`** is the public operation (`Operation.UPSERT_MERGE`). Its mask names, for each value the entity holds, the value's path: one backquoted segment per property name, a backquote or a backslash in it escaped by a backslash, which is the lookup's quoting (ADR-0184), now shared by both as `PropertyPaths.segment`, joined by dots into an embedded entity, so one without properties adds no path and leaves the stored property as it is, as the Native-mode client library's merge treats an empty map (`FieldMask.fromObject`, google-cloud-firestore 3.49.0), and stopping at an array, which `datastore.proto` says a path may not enter, and at a null. It never names a property the entity lacks, which `datastore.proto` says the service would delete, so a merge sets values, to null included, and never removes one. It is idempotent like `upsert`, and earns neither `ALREADY_EXISTS` nor `NOT_FOUND`, so routing treats it as an `upsert`.
- **`EntityProtos` encodes every mutation**, repeating the client library's package-private conversion value type by value type, each value's index mark and meaning included; the meaning is reachable only through `value.toBuilder().getMeaning()`. `EntityProtosTest` holds the result equal to the library's: `Entity.fromPb` reads it back as the same entity, at the size `Entity.calculateSerializedSize` reports, for an entity holding every `ValueType`, so a type a libraries-bom bump adds fails there rather than being dropped. A key is encoded as before, and checked against the library's `toUrlSafe` form.
- **The request is sent through the client's own `DatastoreRpc`**, the object the `NOT_FOUND` lookup already used, with the client's single-attempt settings; it throws the gax exception itself rather than the library's `DatastoreException` around it, which the classifier reads alike. `MutationSizeEstimator` now takes each mutation's size from the same encoding, mask included.
- **A request holds the writes in the order the serializer returned them**, where `Batch` grouped them by operation. Re-measured on the emulator (`DatastoreRejectionITCase`, the pinned image, one run): a commit refused with `ALREADY_EXISTS` or `NOT_FOUND` applies the writes ahead of the refused one and none after it, so the "applies none of them" above was `Batch`'s order, which put the refused insert or update first; one refused with `INVALID_ARGUMENT` still applies nothing, except the writes ahead of an entity over 1 MiB. The service's answers are unchanged under the new path (`DatastoreRejectionRealGcpITCase` and `DatastoreWriterRealGcpITCase`, one run, 2026-10-11). The unit tests' fake now keeps the writer's order and models the emulator's partial application.

The merge was measured on the emulator and on the service (`DatastoreWriterITCase`, `DatastoreTableSinkITCase`, `DatastoreTableRealGcpITCase`, one run each): an undeclared property, top-level or inside an embedded entity, kept its value; a masked property was replaced, an array whole; names holding a dot, a backquote or a backslash addressed one property each; and a mask of no path, the merge of an entity without properties, created a missing entity and left a stored one as it was, which `datastore.proto` does not describe. On the emulator only, a masked path into a property that held a string replaced it with an embedded entity, and a merged value marked excluded from indexes kept the mark while the stored embedded entity and its other properties kept theirs.

### Each write encoded once ([#1744], 2026-10-11)

After [#1729] the writer encoded every write twice on its way to a successful commit: once to size it against `maxBatchBytes` when it entered the buffer, and again when the access built the `CommitRequest`.
A retried commit encoded the whole batch again on every attempt, and a solo re-send encoded its write a third time.
The writer now encodes a write once, when it enters the buffer, and keeps that encoding beside the write (`EncodedMutation`, `@Internal`).
The size is taken from it, and `DatastoreDatabaseAccess.commit` takes the encodings, so every commit carrying the write, retries and solo re-sends included, sends the same message instance.
`DatastoreWriterTest` holds that the instances buffered by `write()` are the ones a batch, its retry and its confirmation pass send (re-encoding at the flush or on an attempt fails it), and `DatastoreServiceAdapterTest` holds that the adapter sends the instances it is given.
That the size comes from the buffered encoding rather than from a second, equal one is held by the code's shape only: encoding is deterministic, so no test can tell the two apart without counting encodings.
The public SPI is unchanged, and `FailedMutation` still carries the write the serializer returned.

A JMH benchmark run outside the repository measured the change on one workstation (JDK 17, protobuf-java 4.33.6, google-cloud-datastore 3.7.0, two forks of five iterations, 2026-10-11).
It took a batch of 500 writes of a ten-property entity (a 500-character unindexed string, an array, an embedded entity) from `write()` to the bytes of the request, the serialization both paths share included:

| Operation | Attempts | Before, per write | After, per write |
|---|---|---|---|
| `upsert` | 1 | 3.5 µs, 11.5 KB allocated | 2.3 µs, 6.4 KB |
| `upsertMerge` | 1 | 5.0 µs, 14.5 KB | 2.9 µs, 7.7 KB |
| `upsert` | 3 | 8.1 µs, 24.2 KB | 4.0 µs, 9.8 KB |
| `upsertMerge` | 3 | 11.2 µs, 29.7 KB | 5.1 µs, 10.4 KB |

The saving is 1.3 to 2.1 µs of CPU and about half the allocation per write on a commit that succeeds first time, and more on a retried one.
Against the service it is not measurable: the keyed table measured about 1.5 ms per row per subtask end to end (ADR-0184), so the saving is about 0.1% of a row's time, well inside the spread of an end-to-end run.
It was therefore not run on the Tier-3 rig; the change is kept for the CPU and the garbage it removes, a cost that grows with entity size.

Holding the encoding beside the write does not raise the buffer's peak heap, because the commit already held both: the access built the whole request while the batch still held the writes.
What changes is that the encoding now lives from `write()` to the commit instead of for the commit alone, and one consequence of that adds to the peak: a write whose arrival flushes the buffer is encoded, to be sized, before the batch it overflows is committed, so its encoding lives through that commit and its retries, where the sizing encoding used to be garbage by then.
That is one write's encoding beside a full buffer's: for an entity the service accepts, at most 1 MiB on the wire, about 1 MiB of heap for large values and about 14 MiB for an array of small longs, the encoding's share of the ratio measured below; a larger write, which the writer still sends alone for the service to refuse, adds its own encoding at that size.
JOL measured the retained heap of a full buffer, an object both forms share counted once:

| Buffer | Wire size | Writes alone | Writes and encodings |
|---|---|---|---|
| 500 of the entity above | 0.39 MB | 0.95 MB | 2.2 MB (2.6 MB merged) |
| 9 entities of a 1,000,000-character string | 9.0 MB | 9.0 MB | 9.2 MB |
| 9 entities of a 1,000,000-byte blob | 9.0 MB | 9.0 MB | 9.2 MB (18.2 MB before this change) |
| 300 entities of a 30,000-character string | 9.1 MB | 9.4 MB | 10.3 MB |
| 200 entities of 4,000 unindexed long properties | 13.4 MB | 89 MB | 178 MB |
| 100 entities of 4,000 null or boolean properties | 6.3 MB | 35 MB | 71 MB |
| 100 entities of one array of 4,000 longs | 2.0 MB | 24 MB | 51 MB |

The encoding shares the entity's strings, so a large string value costs its bytes once.
It used to copy a blob's bytes; `EntityProtos` now wraps them (`UnsafeByteOperations.unsafeWrap` over the blob's read-only buffer, since the library's `Blob` is immutable and its `ByteString` accessor package-private), so a blob costs its bytes once too, which also halves the commit's peak for blob-heavy batches.
That holds for a blob in one piece, which every factory of the library's `Blob` makes; a blob read back with `Value.fromPb` from a protobuf whose bytes are a concatenation keeps that `RopeByteString`, whose read-only buffer is a copy (protobuf-java 4.33.6), so it is held twice, as the old encoding held it.
What cannot be shared is the objects each value takes.
Held both ways, entities of small top-level values (longs, booleans, nulls) took 11 to 13 times their wire size, against 6 to 7 times for the writes alone, and an array of small longs, whose elements carry no name on the wire, about 26 times, against 12; a merge's mask paths add to the wire size, so a merged write's ratio is lower (11 times for the longs).
So a full buffer holds about `maxBatchBytes` of heap when large values fill it, and up to about 26 times `maxBatchBytes` (about 230 MB per subtask at the default) when arrays of small numbers do; the docs state the range and name `maxBatchBytes` as the lever.

Two alternatives were declined on these measurements.

- **Keeping only the encoding**, and rebuilding the `DatastoreMutation` from it (`Entity.fromPb`) for a routed write or the `ALREADY_EXISTS` lookup's comparison. It would lower the peak for small long properties by about 28% (128 MB of the 178 MB above) and a large-value one not at all. In exchange the handler would receive a decoded copy rather than the write the serializer returned, its fidelity resting on `EntityProtosTest` alone, and an `upsertMerge` of an entity without properties would be told from an `upsert` only by the presence of an empty mask.
- **Sizing without building a message**, as the Native-mode `DocumentSizeEstimator` does, and encoding at the commit. The commit would still hold both forms, so the peak would be the same but for the one write that overflows a batch, which it would not encode until after that commit, while a second walk of every value type would have to stay equal to `EntityProtos`, and retries and solo re-sends would still need the commit's encodings kept.

### The masked update operation ([#1743], 2026-10-11)

The Datastore-mode table's `update` write mode (ADR-0184) changes the declared properties of an existing entity and keeps the others, which `update` cannot express: it replaces the entity whole.

- **`DatastoreMutation.updateProperties(FullEntity<Key>)`** is the public operation (`Operation.UPDATE_PROPERTIES`): an `update` whose mask names each property the entity holds, one `PropertyPaths.segment` per top-level name (`EntityProtos.topLevelPaths`), so each property, an embedded entity included, replaces the stored one whole. A top-level mask is what the table needs and what Native mode's `update` does; a recursive one would be `upsertMerge`'s mask on an `update`, which no consumer has asked for and can be added beside it. The name says what is updated, the entity's properties ("property" is Datastore mode's noun, `Entity.properties` and `PropertyMask`; "field" is Native mode's), rather than the mask's shape. Two names were declined on review (owner, 2026-10-11): renaming `update` to `replace` and giving the masked operation `update`, which would match Native mode's and the SQL mode's `update` but make the connector's `update` differ from the API's mutation of that name and its `replace` differ from the SQL `replace`, which creates a missing entity; and `updateMasked`, which names the mechanism and suggests what a mask can also do, delete a named property the entity lacks, which this operation never does. An entity without properties is refused when the write is built, as `FirestoreWrite.update` refuses no fields: its mask would name nothing, so the write would change nothing.
- **It earns `NOT_FOUND` as an `update` does**, so a commit refused with it is confirmed one write at a time beside either operation, and routed after the lookup; `DatastoreWriter.earns` holds the rule, and the unit-test fake refuses it under `servesLikeTheService()` as it refuses an `update`. Measured on the emulator and the service in `DatastoreRejectionITCase` and `DatastoreRejectionRealGcpITCase` (ADR-0184's [#1743] section): a commit refused for it applied the other writes as a commit refused for an `update` does.

### Missed updates count toward the rejection bound ([#1750], 2026-10-11)

Under a dropping or dead-lettering handler, a sink whose updates all addressed entities that do not exist routed every record while the job stayed green. A serializer naming the wrong kind or namespace is enough: the lookup ahead of routing answers "missing entity", not "missing database", and a `NOT_FOUND` did not count toward `maxConsecutiveRejections`. The table sink was never exposed, because its handler fails the job on the first refusal, and neither was the Native-mode sink, which never routes `NOT_FOUND` (ADR-0171). The owner chose on 2026-10-11 to count it.

- **A `NOT_FOUND` for an `update` or an `updateProperties` counts toward `maxConsecutiveRejections` once the lookup of its key is answered.** A refusal of the lookup still fails the job as a missing database, as before. `ALREADY_EXISTS` for an insert keeps the ADR-0171 reasoning and does not count.
- **A run ends at a commit the service applies, and at an insert whose key is found holding exactly the entity it writes.** The second used to leave the run as it was, so that replayed identical inserts could not break a run of `INVALID_ARGUMENT` refusals; replayed upserts and deletes break such a run anyway. Counting misses made the old rule cost more: a commit refused with `NOT_FOUND` applies the inserts beside it, and each of them answers its own re-send with `ALREADY_EXISTS`, so without the reset a run of misses grew across inserts the service had written. An insert whose key holds a different entity, or whose lookup still fails, neither counts nor ends the run.
- **The replay case.** A replayed update of an entity the stream deleted afterwards answers `NOT_FOUND`. The delete came after the update in the stream and was applied before the failure, so it is replayed too; a delete of a missing key is applied, and the writer commits before a key repeats, so on a keyed stream the delete ends the run its update began. A restart therefore fails only when `maxConsecutiveRejections` such updates come ahead of their deletes with no write applied between them, as when a stream marks a set of entities and purges them within one checkpoint interval. It then fails on every restart, because the job stops before it reaches the deletes; nothing is lost, and a larger bound lets the replay through. Under the default `failJob()` the same replay already failed on its first `NOT_FOUND` (the DataStream page's delivery-guarantee table), so the change reaches only dropping and dead-lettering handlers, which no longer pass every such replay.
- **The misconfiguration case.** A stream of updates alone that addresses no stored entity fails the job at the bound, after routing each update. A stream that mixes updates with upserts, inserts or deletes is not caught: each applied write ends the run, and an upsert under the wrong kind or namespace creates the entity a later update of the same key then finds. No count of refusals catches writes the service applies.
- **The cost falls on a stream that expects updates to miss**: one whose entities a TTL policy or another writer deletes, or one that uses `update` so as not to recreate deleted entities. It can reach the bound with nothing misconfigured, and its only remedy, a larger bound or none, loosens the bound for `INVALID_ARGUMENT` too. **Reopen** with a separate bound for `NOT_FOUND` when such a stream also needs the `INVALID_ARGUMENT` bound.
- **The bound's message** names those causes when the last refusal is a `NOT_FOUND`. It no longer says none was applied between the refusals, since a commit refused with `NOT_FOUND` applies its other writes and the run ends only at a write the writer sees applied.
- `DatastoreWriterTest` holds both cases under the fake's `servesLikeTheService()`: updates of keys the fake does not hold fail at the bound after a lookup each, updates each followed by their deletes pass, and updates ahead of their deletes fail on a second writer over the same state too and pass with a larger bound. The statuses the change rests on, `NOT_FOUND` for an update and an applied delete of a missing key, were already measured against the service ([#1707]).
- Declined: **a separate, larger bound for `NOT_FOUND`**, a second option whose value no measurement supports better than the shared default's, kept as the reopen above; **counting only before the first applied update after the writer opens**, which counts a replay exactly as the shared bound does, since a replay arrives as the writer opens, while missing a run that starts later; **keeping the decision and documenting it**, which leaves a misconfigured stream shedding every update behind a green job, the outcome the bound exists to stop; and **a probe for any entity of the kind in the key's namespace**, a keys-only query of one result that would tell a kind with nothing stored from one missing entity, at the cost of a query path, a cache per kind and namespace, the `datastore.entities.list` permission, and an intentionally empty kind read as a misconfiguration.

## Consequences

- A refused commit costs one solo commit per write, sequentially on the task thread, plus one lookup per update refused with `NOT_FOUND` and per insert refused with `ALREADY_EXISTS`; `mutationsConfirmedAlone` counts each mutation re-sent alone, once; retries of those solo commits count in `batchesSent`.
- With the defaults, one commit can take about a minute of backoff against fast transient refusals, and about eleven minutes when every attempt times out, before the job fails, and each solo re-send has a budget of its own, so a confirmation pass can exceed Flink's default checkpoint timeout under a sustained outage; the docs say to size them together.
- The lookup needs `datastore.entities.get`, which `roles/datastore.user` carries.
- `errorClass` counts the transient failures the writer recovered from, as on the Spanner sink (ADR-0076), because the writer sees its own retries.
- The request-size limit, the lookup discriminator and the statuses above were emulator evidence when this was decided; the revision above records the service's answers.

## Alternatives declined

- **The proto `Mutation` on the SPI.** It covers all four operations in one type, but leaves users building protobuf `Entity` and `Value` messages by hand, and the high-level types serialize.
- **The per-operation client methods.** Each sends a commit of one operation type, so a mixed batch would cost one request per operation, and they merge a repeated key silently (`put`, `update`, `delete`) or refuse it (`add`).
- **The generated `DatastoreClient` with hand-built protobufs.** The entity-to-protobuf conversion is package-private, so the writer would carry its own converter. [#1729] adopted the converter, behind the same SPI, once a property mask needed it, and kept the client's RPC object rather than a second generated client.
- **The client library's retries.** They would re-send a commit on their own schedule with no hook, and the writer must see each refusal to confirm it.
- **Bisecting a refused commit** instead of re-sending each write. It costs fewer requests for one bad write among many, but confirms nothing about a commit with several, and the sibling sinks' solo shape is already documented and measured.
- **Beam's adaptive batch sizing.** Not needed for correctness; reopen if a measurement shows the fixed bounds limiting throughput.
- **`base_version` conflict detection and an exactly-once mode** stay deferred with their reopen conditions on [#355].

[#355]: https://github.com/flink-gcp/flink-connector-gcp/issues/355
[#1542]: https://github.com/flink-gcp/flink-connector-gcp/issues/1542
[#1546]: https://github.com/flink-gcp/flink-connector-gcp/issues/1546
[#1651]: https://github.com/flink-gcp/flink-connector-gcp/issues/1651
[#1707]: https://github.com/flink-gcp/flink-connector-gcp/issues/1707
[#1729]: https://github.com/flink-gcp/flink-connector-gcp/issues/1729
[#1744]: https://github.com/flink-gcp/flink-connector-gcp/issues/1744
[#1743]: https://github.com/flink-gcp/flink-connector-gcp/issues/1743
[#1750]: https://github.com/flink-gcp/flink-connector-gcp/issues/1750
[#1819]: https://github.com/flink-gcp/flink-connector-gcp/issues/1819
[#1895]: https://github.com/flink-gcp/flink-connector-gcp/issues/1895
