# Detailed guidance — flink-connector-gcp-firestore

Module-scoped guidance, read when working in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `AGENTS.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Module shape (`docs/adr/0170`)

- **One module, two package roots**: `connector.firestore` (Native mode, `firestore.v1`) and
  `connector.datastore` (Datastore mode, `datastore.v1`). They share nothing beyond
  `flink-connector-gcp-base`; extract a helper only when its second consumer exists. Each root
  carries its own `CrossVersionSink` under `src/main/java-flink1` / `java-flink2`.
- The design comment and implementation order on #355 settled the sub-issues #1540–#1546; read
  them before starting one. CDC and an exactly-once mode are deferred there with reopen conditions
  — do not start either without engaging those conditions.
- Public types enter at `@PublicEvolving` (ADR-0141's youth clause).

## Native-mode sink (`docs/adr/0171`)

- **The writer sends through the client library's `BulkWriter`, on an executor the writer owns,
  and never calls `BulkWriter.close()`** (it waits without a bound). Closing the access shuts the
  executor down.
- **Two library defects are worked around, and `BulkWriterDefectsITCase` pins both**: a failed
  write never releases its pending-operation slot (the writer replaces the `BulkWriter` before a
  submission could reach 500), and a synchronous refusal corrupts its request (the closed
  `FirestoreWrite` value vocabulary keeps it unreachable; a refusal that still arrives fails the
  job). When that test fails after a BOM bump, re-examine the workaround rather than the test.
- **The library sends on no timer** — at 20 writes (10 with a retry) or on `flush()`, and a
  retried write joins whatever batch is open. Every pass of a wait that finds the mailbox empty
  asks it to send (`sendOutstanding`); asking once per wait can strand a retry. Never wait on the
  library's `flush()` future; wait on the writer's own ledger.
- **Rates below 20 are refused** (the library's first batch is 20 and never passes a lower
  rate), and `maxInFlightWrites` defaults to 250 so failed writes do not force a replacement each.
- **`DocumentSizeEstimator` is the `Write`'s wire size**, not Firestore's storage formula;
  `DocumentSizeEstimatorTest` holds it equal to a proto reconstruction of the library's `Write`,
  and `DocumentSizeEstimatorITCase` to the request the library actually sends — add a new
  vocabulary type to `everyValueType()` or an operation to `everyOperation()`, which both read.
- **The library reports a request-level status against every write of the request.** That is why
  `INVALID_ARGUMENT` is parked and confirmed alone before routing, and why its `errorClass` count
  waits for the solo verdict. Measured on the emulator: `INVALID_ARGUMENT` fans out across the
  request; `ALREADY_EXISTS`, `NOT_FOUND` and `FAILED_PRECONDITION` answer only their write.
- **Routing**: `INVALID_ARGUMENT` (after solo confirmation); `ALREADY_EXISTS` for `CREATE` only,
  outside `maxConsecutiveRejections`; `FAILED_PRECONDITION` only for a write carrying
  `lastUpdateTime` and only under `preconditionFailurePolicy(ROUTE_TO_FAILURE_HANDLER)`.
  `NOT_FOUND` is never routed. Both ADR-0042 halves apply.
  `FirestoreErrorClassifierTest` iterates every gRPC code.
- **Retries are the library's, in two layers**: `BulkWriterRetryPolicy` keeps the library's
  retryable set (pinned against the `BatchWrite` call settings) and makes the write's attempt
  count configurable as `writeMaxAttempts`; that backoff has no knobs because the library exposes
  none. The transport's per-call retries are the Pub/Sub-shaped `retry*` options, mapped by
  `DefaultFirestoreDatabaseAccessFactory.retrySettings` onto the library's `BatchWrite` values.
- **Field names are literal in every operation**; `update` goes through the `FieldPath` overload.
- **`FirestoreDocumentReference` never reaches the library**: `BulkWriterDatabaseAccess` replaces
  it, at any depth, with a `DocumentReference` of its own client. The library would encode an
  unreplaced one as a bean map without an error, so any new path that hands fields to the library
  must replace it too; `FirestoreWriterITCase` reads each operation's reference back.
- **Same-document order is not kept** — documented, and #1556 is the opt-in mode. Do not add a
  gate outside that issue.
- The emulator endpoint reaches the client only through the builder; the library also reads
  `FIRESTORE_EMULATOR_HOST` itself, and `FirestoreClients` warns when it is set without an
  endpoint.

## Native-mode source (`docs/adr/0173`)

- **A split is a `RunQueryRequest` plus the job's read time.** Both read shapes share it; the
  serializer writes the query's protobuf encoding length-prefixed. Never put a `Query`,
  `QueryPartition` or `DocumentReference` in state: none is `Serializable`, and each belongs to a
  client.
- **One snapshot per job.** The planner takes the read time from a one-document probe's response,
  never from the process clock (the service refuses a future read time); a restore from a
  checkpoint that recorded the plan never plans again. The library's `getPartitions` sets no read
  time (the RPC accepts one), which is harmless because its partitions tile the document-name
  space.
- **Cursors are the library's `startAfter(DocumentSnapshot)`** (`QueryCursors`), for pages and for
  checkpoints alike; a checkpoint also shrinks the `limit` and drops the `offset`. Do not hand-build
  a cursor: the implicit ordering (inequality fields, then `__name__`) is the library's to derive.
  The planner's probe refuses a query whose projection omits an ordered field.
- **A page is materialised** (a read-time transaction has no streaming read), so `pageSize` bounds a
  fetch's memory — plus up to one page per mid-stream library retry, appended before the reader's
  cut — and a fetch is one request, which `wakeUp()` waits for.
- **The library retries a broken page from its last document with `limit` and `offset`
  unchanged.** So the reader cuts every page to its limit, and the planner resolves a query's
  `offset` into a cursor; a split never carries an offset. Read through `ReadTimeQueries`
  (`runAsyncTransaction`, interruptible), and hold clients in `LazyFirestoreClient` (one-way close).
- The emulator answers `PartitionQuery` with `UNIMPLEMENTED`; emulator tests cut partitions through
  `FixedPartitionsPlannerFactory` (an override of `ClientQueryPlanner.partitions`). Real partition
  counts and the read-time window belong to the gated suite (#1546).
- Unit tests mint snapshots through the library's `@InternalApi` `Internal.snapshotFromProto`
  (`TestDocuments` in this project's test package), so no vendor-package helper exists here.
- A user query is planned from its wire form (`Query.fromProto(toProto())`), which is what readers
  rebuild; planning on the original reverses a `limitToLast` offset.

## Native-mode Table API sink (`docs/adr/0179`)

- **A table is one collection; the PRIMARY KEY (one `STRING` column) is the document id** and is
  never a field. No key means `SET` under a 20-character `SecureRandom` id (a `CREATE` would fail
  the job on a library retry). A key that is empty or holds `/` fails the record: it would
  silently address another document. `update` declares no deletes (restart loop otherwise).
- **Types are checked when the statement is planned by `FirestoreTableSchema`**, and the converter
  (`RowDataToFirestoreConverter`) assumes that check ran. Add a type to both, plus the type table
  on the table docs page. The read side (#1608) reuses the schema class and its markers.
- **Markers are `geo-point-field-paths` / `reference-field-paths`** (BigQuery's path grammar,
  ARRAY transparent, MAP values `.value`). **No option key may start with `schema`**: Flink's
  `CatalogPropertiesUtil` drops such options from a persisted table silently. An unknown
  or mistyped marker path is refused, never ignored.
- **A builder cross-check whose message names setters is restated in option keys** in
  `WriterOptionsMapper` (throttling) or called with the keys as names (`retrySettings` overload).
  Assert a phrase only the connector's message carries: `FactoryUtil` echoes every option.
- **The failure handler stays `failJob`**, so `maxConsecutiveRejections` has no option; the parity
  test records why. Same-document order is not kept (#1556 is the opt-in, v1.3.0 by owner
  decision 2026-10-04); a test must not write one key twice and expect order.
- **Run module tests with `-am`** (or install base first): another session's install can replace
  the base SNAPSHOT in `~/.m2`, and `LineageIdentifiers.firestoreCollection` then fails with
  `NoSuchMethodError`. In `flink-sql-connector-gcp-firestore`, never pass an ITCase to `-Dtest`:
  the default test execution would run it against the unshaded classes before the jar exists.

## Datastore-mode sink (`docs/adr/0175`)

- **The SPI carries the client library's `FullEntity<Key>` and `Key`** (both serializable; a test
  round-trips every value type). Their protobuf conversion is package-private, so do not add a
  proto-building path; `FailedMutation`'s payload is the Java-serialized `DatastoreMutation`.
- **The writer is synchronous** (Spanner shape): one non-transactional commit at a time through
  the client's `Batch`, on the task thread. **It commits before a key repeats** — `Batch` would
  merge or refuse a second write to one key — and that rule is also what keeps one subtask's
  writes to a key in order (barring a timed-out attempt the service applies late). Do not let a
  batch hold a key twice.
- **The client makes one attempt per call** (`DatastoreClients` sets single-attempt
  `RetrySettings`, which the library applies to `RetryHelper` and to every generated call); the
  writer owns retries on the `recovery*` schedule. **The host is always set**, so
  `DATASTORE_EMULATOR_HOST` never chooses the endpoint; the emulator path uses `NoCredentials`.
- **The default database id is empty** in the Datastore API, which refuses `(default)`;
  `DatabaseDestination` normalizes it. A key of another project or database is routed before it is
  sent.
- **Routing**: a refused commit whose status a write could earn (`INVALID_ARGUMENT` always,
  `ALREADY_EXISTS` beside an insert, `NOT_FOUND` beside an update) is re-sent one write at a time;
  a write refused alone is routed only for an operation that can earn the status. **`NOT_FOUND` is
  routed only after a lookup of the key is answered** (owner's decision, 2026-10-03); a refused
  lookup fails the job as a missing database. The lookup is one `Lookup` through the client's RPC
  object, never `Datastore.get`, which re-sends a deferred key without a bound. `maxConsecutiveRejections` counts `INVALID_ARGUMENT`
  and keys addressing another database, never the replay answers. `DatastoreErrorClassifierTest` iterates every gax code.
- **Ramp-up throttling** (`RampUpThrottle`) is Beam's `RampupThrottlingFn` shape, one permit per
  record that reaches the buffer; retries and solo re-sends are not throttled.
- The emulator (same binary, `--database-mode=datastore-mode`, set in
  `AbstractDatastoreEmulatorITCase`) applies nothing of a refused commit except, for an oversized
  entity, the writes ahead of it in the request, which `Batch` orders by operation (so a refused
  commit can be partly applied), serves any database id
  and enforces no request size; `DatastoreRejectionITCase` pins each fact it asserts. The lookup
  discriminator and request size belong to the gated suite (#1546).

## Datastore-mode source (`docs/adr/0177`)

- **The input is a kind, a protobuf `com.google.datastore.v1.Query`, or a GQL string** (owner's
  decision, 2026-10-03). Do not accept the high-level `EntityQuery`: its conversion to the wire
  form is package-private or `@InternalApi`. A GQL query is parsed at planning with `LIMIT 0`
  appended, falling back to running it as written on `INVALID_ARGUMENT`.
- **`QuerySplitter` is called over the proto-client HTTP `Datastore`** (`SplitterClient`, built
  through `HttpDatastoreRpc`'s factory; owner's decision, 2026-10-03), on the JobManager only while
  planning. Readers use the gRPC `DatastoreRpc` of a client `DatastoreClients` builds with the
  library's retry settings (`callTimeout` null; `LazyDatastoreClientTest` pins it).
- **The splitter's ranges are rebuilt in the service's key order** (`KeyRanges`; owner's decision,
  2026-10-03): its comparator orders names by UTF-16 code unit, the service by UTF-8 byte
  (measured on the emulator), and misordered boundaries make ranges overlap. Do not hand the
  splitter's output to readers directly. `datastore-v1-proto-client` is not managed by
  libraries-bom; it arrives through `google-cloud-datastore`, never at a hand-kept version.
- **`SplittableQueries` is the splittability rule, stricter than the splitter's**: one kind, no
  order/limit/offset/cursor/`DISTINCT ON`, filters only `EQUAL` and `HAS_ANCESTOR` under `AND`,
  none of no type. Anything else is one split. `SplittableQueriesTest` iterates every operator.
  A nearest-neighbour search is refused outright (`whyNotReadable`): paging would change what it
  finds.
- **The estimated split count is Beam's (`entity_bytes`/64 MiB, 12..50,000) with the parallelism
  as a further floor.** Statistics failures fail planning; absent statistics take the floor.
- **Projected index values are read back** (`ProjectedValues`, owner's decision 2026-10-03):
  meaning-18 integers become timestamps (microseconds), meaning-18 strings become blobs, as
  `ProjectionEntity` does, and only for a query that projects; `ProjectionEntity` itself cannot be
  built outside its package.
- **`splitCount` is bounded at `MAX_SPLIT_COUNT` (50,000)**: the splitter checks no upper bound,
  sizes a list from the count and holds 32 sampled keys per boundary.
- **A page is one unary `RunQuery`**; the offset stays on the query and the service spends it
  page by page. A checkpoint starts the query at the cursor after the last emitted entity, drops
  the offset and reduces the limit. A batch that claims more without moving fails the read, and a
  spent limit finishes the split whatever the batch says, so no page asks for zero entities.
  Repeats a list-valued ordering or inequality produces across pages are the service's and are
  passed on (documented, owner's decision); a keyless projection result fails the read.
- The emulator samples no `__scatter__` keys and keeps no statistics; emulator tests cut ranges
  through `FixedSplitsPlannerFactory` (an override of `ClientQueryPlanner.split`), and the planner's
  RPC-level steps are unit-tested through `FakeDatastoreRpc`. `DatastoreEmulatorReadDeviationITCase`
  pins each emulator fact the docs' deviation table states; with `DatastoreSourceEmulatorITCase`
  it pins the emulator evidence ADR-0177 records.

## Testing

- The emulator is `gcloud emulators firestore` from the shared `google-cloud-cli` image
  (`FirestoreEmulatorContainers` in test-utils, pinned with the Bigtable and Pub/Sub classes).
  It does not enforce the 10 MiB request limit or IAM, and its statuses are not evidence about
  the service; the real-GCP suite is #1546.
