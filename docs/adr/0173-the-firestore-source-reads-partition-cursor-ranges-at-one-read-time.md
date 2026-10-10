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

# ADR-0173: The Firestore source reads partition cursor ranges at one read time

- Status: Accepted
- Date: 2026-09-30 (client library facts read in google-cloud-firestore 3.46.0 through
  libraries-bom 26.87.0; emulator behavior measured 2026-09-30 against
  `google-cloud-cli:583.0.0-emulators`, one run; the source ITs re-run 2026-10-03 against
  `587.0.0-emulators` and unchanged); revised 2026-10-10 by [#1689] (a configured read time is
  truncated to the microsecond)
- Issues: [#1541], [#355], [#1546], [#1689]
- Modules: firestore (`source`, `source.batch`); base (`lineage.internal`)
- Current behavior: `docs/content/docs/connectors/datastream/firestore.md` § Source

## Context

[#355]'s design comment settled the shape of the Native-mode bounded source: a FLIP-27 source on `PullAssignmentSplitEnumerator` (ADR-0083) with a per-enumerator planner (ADR-0128); two read shapes, a collection-group scan cut by `PartitionQuery` and an arbitrary query read as one split; every split read inside a read-only transaction at one `readTime`; a `Collector`-based deserializer over `DocumentSnapshot` (ADR-0108); and a partially read split resumed by cursor, the Bigtable scan's shape (ADR-0080) rather than the Spanner batch source's re-read (ADR-0085).
It left open how a split is represented, how the read time is chosen, how a page is read, and what the emulator can show.
Reading the library and measuring the emulator decided each.

## Evidence

Read from google-cloud-firestore 3.46.0:

- `CollectionGroup.getPartitions(n)` answers a single partition without a call when `n` is 1. Otherwise it sends `PartitionQuery` for `n - 1` cursors, sorts them, and returns partitions `[previous, cursor)` followed by a last one with no end, over `orderBy(__name__)`. The partitions therefore tile the document-name space. The request never sets `read_time`.
- `Query`, `QueryPartition` and `DocumentReference` are not `Serializable`. `Query.toProto()` returns a `RunQueryRequest` without a read time, and `Query.fromProto(Firestore, RunQueryRequest)` rebuilds the query on a client, refusing one whose parent names another database. A `limitToLast` query's wire form carries the reversed ordering, and its results come back in that order.
- A read-only transaction with a read time is a client-side `ReadTimeTransaction`: no service transaction, no retry runner, and each request carries the read time. Its `get(Query)` materialises the whole result; there is no streaming read at a read time through the high-level `Query` API.
- `Query.startAfter(DocumentSnapshot)` first makes the implicit ordering explicit (explicit orderings, then inequality-filtered fields, then `__name__`) and takes the cursor values from the snapshot in that order. It throws `IllegalArgumentException` when the snapshot lacks an ordered field, which is what a projection that leaves one out produces.
- `DocumentSnapshot` and `QueryDocumentSnapshot` have no public constructor. The library's public but `@InternalApi` class `Internal` builds one from a `Document` proto (`snapshotFromProto`), given a `GrpcFirestoreRpc`.
- A query read at a read time goes through `StreamableQuery.internalStream`, whose `onError` retries a stream that broke after its first document as `startAfter(lastDocument)` with the query's `limit` and `offset` unchanged, and appends the retry's documents to the same result. A page can therefore hold more than its limit, and an `offset` is applied a second time after the break. The retry keeps the read time: it reads at the response's read time whenever the query requires consistency, which `Query` does by default and `fromProto` keeps.
- `Firestore.runTransaction(Function, options)` runs the function on the caller's thread inside the library and turns anything it throws, an `InterruptedException` included, into a failed future; `runAsyncTransaction(AsyncFunction, options)` leaves the wait to the caller.

Measured against the emulator:

- `PartitionQuery` answers `UNIMPLEMENTED` for a count of two, the count the deviation test pins.
- A read at an older read time returns the documents as they stood; a document written later is absent.
- A read time in the future is refused with `INVALID_ARGUMENT` ("The requested 'read_time' cannot be in the future").
- A read time two hours old is answered, where the service documents a one-hour window without point-in-time recovery.
- A query's read time comes back on its `QuerySnapshot` whether or not it matched a document.

## Decision

- **A split is a query in its wire form plus the job's read time.** One split type serves both shapes: a scan partition is the collection group, projected, between the partition's two cursors; the arbitrary query is the user's. The split serializer writes the split id and read time in the connector's own format and the query as the length-prefixed protobuf encoding of `google.firestore.v1.RunQueryRequest`. That encoding is the service's published wire contract, not a library's Java serialization, and it is the only complete form of a Firestore query; re-encoding filters, orderings and cursor values field by field would be a second query model to keep in step.
- **The read time is the service's.** Without a configured `readTime`, the planner runs a one-document probe of the base query and takes its read time from the response. A read time from this process's clock could lie in the future of the service's, which the service refuses. With a configured `readTime`, the probe runs at it, so a time outside the service's window fails the job at planning rather than on every reader.
- **The default read time is not rounded.** The service accepts a read time older than an hour only on a whole minute and with point-in-time recovery, so the probe's microsecond read time is good for an hour. Rounding it down would hide what was written in the minute before the job started; a read that must outlive the hour sets a whole-minute `readTime`.
- **A query's `offset` is resolved at planning**, at the read time, into a cursor after the last document it skips, so no split carries one and the library's mid-stream retry cannot apply it twice.
- **The probe also proves the query can be continued.** When it returns a document, the planner takes a cursor after it; a projection that leaves out an ordered field fails planning with a message that names the fix, instead of failing at the first page boundary or checkpoint.
- **Partition boundaries come from `PartitionQuery` at the current time.** The client library's `getPartitions` sets no read time, although the RPC accepts one. That moves nothing: the partitions tile the name space, so a document of the snapshot falls in exactly one of them however the collection group has changed.
- **Readers page.** Each fetch is one request inside a `ReadTimeTransaction`: the split's query continued after the last document handed over, limited to `pageSize` or to what is left of the query's own `limit`. A page shorter than it was allowed to be ends the split, and a page longer than it asked for — the library's retry — is cut to its limit, the rest read again by the next page. The page size bounds a fetch's memory, except that each mid-stream retry can add up to one page more before the cut: the library retries on the default `RunQuery` retryable codes whatever the client's settings, and bounds the retries only by the client's total request timeout, which governs every other call's retries as well, so the source leaves it alone. A fetch is one request, so a wake-up waits for it rather than cancelling it. The read runs through `runAsyncTransaction`, so an interrupt reaches the waiting thread.
- **Both clients are built lazily and closed one way** (`LazyFirestoreClient`, the Bigtable `LazyBigtableDataClient` shape): a planning call that outlives the enumerator's close is refused a client rather than building one nothing closes.
- **Resume is by cursor.** A checkpoint rewrites a split's query with `startAfter` the last document successfully deserialized, the `limit` reduced by the documents passed and the `offset` dropped. Because the document name is always among the orderings, the cursor names one position, and a restore neither repeats nor skips a document. A split whose limit is used up finishes without a request.
- **Values reach the deserializer as the client library decodes them.** The source neither checks nor converts a document's fields, so a deserializer receives whatever the library decodes, and google-cloud-firestore 3.49.0 widened that: a reserved single-key map such as `{"__int__": 5}` decodes to one of seven BSON value classes, and a `{"__binary__": ...}` map to a `Blob` with a subtype ([#1589]). Measured on 2026-10-03, a Standard-edition database stores the subtyped `Blob` and refuses the seven classes, and an Enterprise-edition database stores all of them. A cursor is the one place the source handles a value, in paging, in a checkpoint's resume point and in planning's offset resolution alike: `startAfter` decodes the ordered field and encodes it again. `QueryCursorsTest` holds that round trip equal to the stored value for each reserved map the library decodes to a type of its own, and pins the forms where it is not, which the consequences list.
- **The planner's partition call is a protected seam.** The emulator cannot partition, so the emulator tests replace `ClientQueryPlanner.partitions` with boundaries they choose and read them for real. Real partition counts are the gated suite's ([#1546]).
- **A collection-group scan reports a `firestore-collection-group` lineage resource** (namespace `firestore://{project}/{database}`, name the group id); a query reports none, as the sink does, because its collections are the factory's. ADR-0160 records the kind.
- **A user query is planned in its wire form.** The planner rebuilds the factory's query from `toProto()`, as every reader will, and resolves the offset and runs the probes on that. For a `limitToLast` query the wire form is the reversed ordering with an ordinary limit, so the offset counts from the end, as the service applies it.
- **The plan is recorded in the enumerator's checkpoint**, and a restore from a checkpoint that holds it never plans again. A job that restarts before any checkpoint has completed plans again, at a new read time unless one is configured.
- **Unit tests mint snapshots through the library's `Internal.snapshotFromProto`** (`TestDocuments`, in this project's test package). It is public, so ADR-0067's vendor-package helper is not engaged; being `@InternalApi`, a `libraries-bom` bump that changes it fails the tests at compile time.

### A configured read time is truncated to the microsecond ([#1689], 2026-10-10)

`readTime` accepted any `Instant`, and the planner passed its nanoseconds on.
`google/firestore/v1/firestore.proto` (proto-google-cloud-firestore-v1 3.49.0) documents `read_time` as "a microsecond precision timestamp", and nothing between the builder and the wire truncates it: not google-cloud-core's `Timestamp`, nor the library's `ReadTimeTransaction` and `Query`.
Measured on 2026-10-10 against `google-cloud-cli:587.0.0-emulators`, one run: a read time with sub-microsecond digits is refused with `INVALID_ARGUMENT` ("timestamp cannot have more than microseconds precision"), on the raw `RunQuery`, through `ReadTimeQueries` and through the `firestore` table's `scan.read-time`, which failed the job on the JobManager with the read-window message; the same time in whole microseconds is answered.
On the emulator, a document's versions sit on whole microseconds: for an update time W, a read at W − 1 ns to W − 999 ns is refused, at W − 1 µs finds nothing, and at W finds the write.
The service was not measured, but `google/firestore/v1/common.proto` (same artifact) says of `Precondition.update_time` that the "Timestamp must be microsecond aligned", which implies its update times are whole microseconds too.
`Instant.now()` carries sub-microsecond digits on Linux with JDK 17 (not on macOS with JDK 21), so `readTime(Instant.now().minusSeconds(30))` failed there.

The builder now truncates the time to the microsecond (owner's decision, 2026-10-10).
The decision rests on the read-time contract, not on how versions are stored: the service accepts only a microsecond-precision read time, so the instant given could never be read, and truncation floors it to the latest microsecond at or before it.
With update times on whole microseconds, as `common.proto` implies and the emulator shows, the floored time reads exactly the data the time given names.
The Table API's `scan.read-time` reaches the builder through `OptionSetters` and is truncated there too.

## Consequences

- A read that outlasts the service's version window, or a restore from a checkpoint older than it, fails every remaining read; the job has to start over with a new read time. The docs state the window.
- A page is materialised, so `pageSize` times the largest document is what one fetch ordinarily holds, and each mid-stream retry can add up to one page more.
- Planning costs up to two document reads beyond the data, plus the documents an `offset` skips, which the service would read and bill for the offset anyway, and fails at planning if the user's query needs an index that does not exist.
- Paging and resume need no knowledge of the query beyond what the library already applies, so filters, orderings, projections, cursors, `limit` and `offset` on a user query all resume correctly; a `limitToLast` query reads its documents in reverse.
- A restart before the first completed checkpoint reads at a new read time unless `readTime` is configured, so a sink that wrote the first attempt's records may hold two snapshots.
- A cursor on some stored values names another position, so a read ordered by such a field can repeat or skip documents: the library narrows an `__int__` outside 32 bits, re-encodes a `__binary__` of subtype 0 as plain bytes, and decodes a vector's non-double elements as zero. A stored value it cannot decode at all, such as a `__binary__` with no bytes or a `__request_timestamp__` outside 32 unsigned bits, would fail the job with the library's own error once a page or a checkpoint continued after the document holding it. The service refuses to store every one of these forms, in a Standard-edition and an Enterprise-edition database alike and on the emulator (measured on 2026-10-03), so none written through the Firestore API reaches a read; writes through the Enterprise edition's MongoDB-compatible API were not tried, and the tests pin the library's handling in case one ever arrives.

## Alternatives declined

- **The partition's two document names as the split, re-building the query per read.** It covers only the scan shape; the arbitrary query would need its own representation, and resume would need a second.
- **The client clock as the default read time.** A clock ahead of the service's fails planning outright; subtracting a margin hides writes the job was started to see.
- **Streaming `RunQuery` through the low-level stub.** It would stream at a read time, but its documents are protos, and building a `DocumentSnapshot` from one is reachable only through the `@InternalApi` `Internal` class, which production code does not depend on; the deserializer contract names the library's snapshot.
- **`maxDocumentsPerFetch` for the page size**, after the sibling sources' `max*PerFetch`. Here one fetch is exactly one request and the value is that request's `limit`, which is what Firestore and Beam call a page; there is no byte bound beside it.
- **Applying an `offset` in the reader**, by dropping documents client-side. It works, but moves the offset's arithmetic into the paging and resume logic, where planning resolves it once.
- **Rejecting `limitToLast` queries.** Planned in its wire form, such a query reads the documents it names, offset included, in reverse; the order is documented instead.
- **Rejecting BSON values in the source.** It would mean decoding every field of every document, and failing the job on data the pipeline cannot change.
- **Refusing a read time finer than a microsecond in the builder** ([#1689]), with a message naming the precision, and in the Table API the option. It would report the value when the job is built rather than when it is planned, but the value is not wrong: the instant given can never be read, flooring it reads at the latest microsecond before it, and a refusal would fail an ordinary `Instant.now()` on Linux (owner's decision, 2026-10-10).
- **Falling back to one partition when `PartitionQuery` answers `UNIMPLEMENTED`.** Only the emulator answers so, and a silent fallback would turn a misrouted production job into a one-subtask read.

[#355]: https://github.com/flink-gcp/flink-connector-gcp/issues/355
[#1541]: https://github.com/flink-gcp/flink-connector-gcp/issues/1541
[#1546]: https://github.com/flink-gcp/flink-connector-gcp/issues/1546
[#1589]: https://github.com/flink-gcp/flink-connector-gcp/issues/1589
[#1689]: https://github.com/flink-gcp/flink-connector-gcp/issues/1689
