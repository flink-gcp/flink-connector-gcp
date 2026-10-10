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
  waits for the solo verdict. Measured on the emulator and on the service (#1706): every
  `INVALID_ARGUMENT` shape fans out across the request; `ALREADY_EXISTS`, `NOT_FOUND` and
  `FAILED_PRECONDITION` answer only their write. A missing or malformed database id answers
  `NOT_FOUND`, and a Datastore-mode database `FAILED_PRECONDITION`, for every write.
- **Routing**: `INVALID_ARGUMENT` (after solo confirmation); `ALREADY_EXISTS` for `CREATE` only,
  outside `maxConsecutiveRejections`, except for an `add` (`hasDrawnId`), which is re-sent under
  a new id, never routed; `FAILED_PRECONDITION` only for a write carrying
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
- **Both builders' `readTime` truncate to the microsecond** (#1689, owner's choice over refusing):
  the service accepts only a microsecond-precision read time, so the instant given could never be
  read, and flooring picks the latest microsecond at or before it. That it sees the same writes
  is backed for Native mode by `common.proto`'s "microsecond aligned" `Precondition.update_time`,
  measured on the service for Native mode (#1706) and only on the emulator for Datastore mode.
  Every path, the Table API's `scan.read-time` included, goes through
  the setter; a new path that builds a read time must truncate too.
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
  counts are `FirestoreSourceRealGcpITCase`'s. A database younger than an hour refuses an older
  read time for predating it (`INVALID_ARGUMENT`), so the one-hour and PITR windows were measured
  once by hand (ADR-0185), not in the suite.
- Unit tests mint snapshots through the library's `@InternalApi` `Internal.snapshotFromProto`
  (`TestDocuments` in this project's test package), so no vendor-package helper exists here.
- A user query is planned from its wire form (`Query.fromProto(toProto())`), which is what readers
  rebuild; planning on the original reverses a `limitToLast` offset.

## Native-mode Table API sink (`docs/adr/0179`)

- **A table is one collection; the PRIMARY KEY (one `STRING` column) is the document id** and is
  never a field. No key means `FirestoreWrite.add`: a `CREATE` under a drawn id that the writer
  re-draws on `ALREADY_EXISTS`, so a row never replaces a document (a lost answer stores it twice;
  #1680, owner's choice over reading back). A key that is empty or holds `/` fails the record: it would
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

## Native-mode Table API scan (`docs/adr/0179`, scan section)

- **The read mapping is the write mapping, strictly** (owner, 2026-10-04): only a 32-bit BSON
  integer widens (into BIGINT) and an exact integer into DOUBLE (|n| ≤ 2^53); every other value
  outside the mapping is a mismatch under `type-mismatch-policy`, including a reference into
  another database (`DocumentReference.getPath()` drops the database). Do not add a lenient
  string rendering; widening the mapping is a decision, recorded in ADR-0179.
- **A failure's remedy follows `Mismatch.readableAsNull()`**: offer `type-mismatch-policy = 'null'`
  only when a nullable field lies around the value, which a `NOT NULL` column can still contain.
- **Every field is selected as a literal `FieldPath`**: the single collection's
  `FirestoreCollectionQueryFactory`, and the collection-group scan through the builder's
  `select(FieldPath...)`, whose field mask holds encoded paths read back with
  `FieldPath.fromServerFormat`. Never pass a column name to `select(String...)`, which splits dots.
- **A collection-group table declares no PRIMARY KEY** (ids repeat across the group, and the planner
  trusts a key to be unique); `document-path` identifies a document there.
- **The emulator can run a collection-group scan only with `scan.partition.max-partitions = 1`**:
  the library answers `getPartitions(1)` without the RPC the emulator lacks.
- `type-mismatch-policy` and the markers carry no `scan.` prefix because the lookup reads
  through the same converter (`FirestoreToRowDataConverter`).

## Native-mode Table API lookup (`docs/adr/0179`, lookup section)

- **Only the document id addresses a lookup**; a keyless table (every collection-group table) is
  refused in `FirestoreDynamicSource.lookupKeys`. Additional top-level physical scalar keys are
  post-read equalities through `base.table.LookupKeyFilter`, before caching by the complete tuple.
  Rows come from the scan's
  `RowDataDeserializationSchema`, so projection, metadata and the mismatch policy stay one path.
- **A key that cannot be an id joins no row without a read**
  (`FirestoreDocumentLookups.documentId`): NULL, empty, `.`, `..`, `__…__`, over 1,500 bytes, or
  holding `/` (`document("a/")` reads `a`).
- **The client library has no blocking read**: `FirestoreDocumentLookup` is `readAsync` only, and
  the blocking function waits on it through `FirestoreDocumentLookups.await`, which rethrows the
  library's exception.
- **`FirestoreLookupErrorClassifier` mirrors the client's `BatchGetDocuments` retry set**
  (`retry_policy_1_codes`); `FirestoreLookupErrorClassifierTest` compares the two, so a
  libraries-bom bump that moves the library's set fails there.
- The lookup functions are `public`: the planner refuses a function class that is not.

- Asynchronous lookup retries use `base.table.AsyncLookupRetries` (ADR-0039); keep the read,
  row conversion and failure classifier here, and the stack-safety and late-callback tests in base.

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
  finds. So are the metadata kinds `__namespace__`, `__kind__` and `__property__`
  (`whyKindNotReadable`, exact names; #1690, owner's decision 2026-10-10): the source can neither
  page through nor resume their results. Google documents them as "generated dynamically, based on
  the current state of your database"; on the emulator `__kind__` and `__namespace__` answer without
  per-entity or end cursors (`__property__` returned nothing); the service's answer is unmeasured
  (#1546). `kind(...)` and `query(...)` refuse at build, a GQL query in
  `ClientQueryPlanner.parseGql`. Statistics kinds (`__Stat_*__`) stay readable. Key-based resume
  is deferred and a split replayed from its beginning declined (ADR-0177 revision); read the
  revision before adding either.
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

## Datastore-mode Table API sink (`docs/adr/0184`)

- **Factory `datastore` (`DatastoreDynamicTableFactory`) is the second line of the module's
  `Factory` service file**; the uber jar's `FirestoreSqlConnectorPackagingITCase` and
  `DatastoreSqlConnectorSmokeITCase` hold both factories. The scan (#1652) and lookup (#1653)
  extend this factory and `DatastoreTableSchema`; the type mapping is their read-back contract.
- **Every write is `upsert` or `delete`; there is no `sink.write-mode`** (owner, 2026-10-10):
  `insert`/`update` replays fail the job on every restart under the table's fail-job handler. A
  keyless table upserts under ids the service allocates (`AllocateIds`, owner 2026-10-10: a
  guarantee against silently replacing an entity, which random names only made improbable). The
  guarantee covers allocated ids only, not self-chosen numeric ids in the same kind. The writer
  allocates and hands its allocator to the serializer through the `@Internal`
  `KeyAllocatingSerializationSchema`; an allocation failure fails the job (it is the database's,
  not the record's); the serializer keeps only the answered id. Batch size is the writer option
  `idAllocationBatchSize` / `sink.id-allocation.batch-size`, default 1,000: measured 2026-10-10,
  100 per call halved key-less throughput, 1,000 is within 7% of a keyed table, and the real
  service accepts 20,000 keys per call (no 500 limit). The public SPI still takes complete keys
  only.
- **The PRIMARY KEY is one `STRING` (name) or `BIGINT` (id) column.** Kind, namespace, project and
  property names are checked at planning only for what every write would be refused for
  (ADR-0127). Project and namespace grammar go through a probe `Key`; the reserved pattern
  (`__.+__`: the emulator stores the kind `____`) and the 1,500-byte limit live once in
  `DatastoreTableSchema`. A key name is data and stays the service's.
- **`sink.unindexed-columns` marks the column and every value nested in it**; an array's elements
  carry the mark because `entity.proto` forbids it on the array value. The emulator enforces the
  1,500-byte indexed-string limit, so `DatastoreTableSinkITCase` has a real firing control.
- **The sink declares key-only deletes (`upsert(true)`) unconditionally.** Flink 2.2/2.3 fail a
  key-only delete into a table with a `NOT NULL` non-key column (FLINK-40477, fixed in 2.4.0);
  that is Flink's bug, and asking for full deletes instead drops deletes of keys the job never saw
  (measured). Owner, 2026-10-10: do not work around it; document nullable columns. No test may
  assert the failure: the weekly next-Flink build would break on the fix.
- **Option keys follow ADR-0137**: `sink.buffer-flush.*` (connector-owned buffer),
  `sink.request-timeout`, `sink.recovery.*`, `sink.throttling.*`. Docs live on their own page,
  `docs/content/docs/connectors/table/datastore.md`, one page per `connector` identifier.
- **Names follow the Firestore table package** (`RowDataSerializationSchema`,
  `WriterOptionsMapper`, `CrossVersionChangelogMode` in `datastore.table.sink`). The changelog shim
  is a per-package copy, as Spanner, BigQuery and Bigtable keep theirs. The timestamp conversion
  repeats the Native-mode package's; hoisting it into base is deferred (PR #1677 round one).
- **A kind's lineage identity comes from `datastore.DatastoreLineage.kind`**, which the source and
  the table sink both call, so the two report the same `datastore-kind` resource.


## Datastore-mode Table API scan (`docs/adr/0184`, scan section)

- **Options**: `scan.partition.max-partitions` (→ `splitCount`), `scan.read-time`,
  `scan.max-rows-per-fetch`, and `type-mismatch-policy`, its own enum in `datastore.table`
  because the roots share only base.
- **No projection reaches the service** (owner, 2026-10-10): a Datastore projection query drops
  entities without an indexed value of every projected property and repeats array values; the
  scan reads whole entities and converts only produced columns. Do not "optimize" this into a
  projection query.
- **Metadata** `key-name`/`key-id`/`version`/`create-time`/`update-time`/`read-time` reach the
  deserializer through the `@Internal` `EntityMetadata` and
  `DatastoreEntityMetadataDeserializationSchema` path (ADR-0177 revision); the public SPI is
  unchanged. `key-id` is how a key-less table reads its allocated ids.
- **Key column**: STRING ← key name, BIGINT ← key id; the other form, and a key with a parent,
  fail the read under either policy. A kind with mixed key forms or child entities is read by a
  key-less table via the metadata.
- **Reserved kinds stay refused for the scan too**: the source itself refuses the metadata kinds
  (#1690, ADR-0177 revision), and a statistics kind stays refused until the gated suite (#1546)
  has measured a read of one against the service.

## Datastore-mode Table API lookup (`docs/adr/0184`, lookup section)

- **Read through the generated `v1.DatastoreClient`, not `Datastore` or `DatastoreRpc`**:
  `lookupCallable().futureCall()` is the only future-returning `Lookup`; the blocking function
  waits on it (owner, 2026-10-10). Settings come from `DatastoreClients.lookupSettings`
  (`EmulatorChannels` plaintext for the emulator). Never `Datastore.get` (unbounded deferral loop,
  ADR-0175).
- **A deferred key is a transient read failure** (`DatastoreEntityLookups.DeferredException`)
  within `lookup.max-retries`, never a miss (owner). `DatastoreLookupErrorClassifier` otherwise
  mirrors the generated client's `Lookup` retry set, `UNAVAILABLE` and `DEADLINE_EXCEEDED` only
  (no `INTERNAL`, unlike Native mode); its test compares the two sets.
- **`DatastoreLookupKeys` skips the read** for NULL, an empty name, id `0`, a name over 1,500
  bytes or `__.+__`: the emulator refuses a lookup of the empty name, id `0` or the over-long name
  with `INVALID_ARGUMENT`, and answers `__x__` as missing. A negative id
  is a real id (measured) and is read.
- **The request carries a property mask** of the produced columns, each name backquoted with `\`
  and `` ` `` escaped; `__key__` alone when only key/metadata are read (owner). Unlike a
  projection query it keeps unindexed values and arrays whole. No read options: strong reads;
  `scan.read-time` is scan-only and `read-time` is `LookupResponse.read_time` (owner).
- `DatastoreLookupConfig` is a per-connector copy of the lookup config, as Firestore, Spanner and
  Bigtable keep theirs.

## Testing

- The emulator is `gcloud emulators firestore` from the shared `google-cloud-cli` image
  (`FirestoreEmulatorContainers` in test-utils, pinned with the Bigtable and Pub/Sub classes).
  It does not enforce IAM, refuses an array inside an array that the service stores, and its
  statuses are not evidence about the service. Neither it nor the service refused a 10.5 MiB
  request.
- The gated suite (`FIRESTORE_IT_PROJECT`, ADR-0185) extends `AbstractFirestoreRealGcpITCase`: one
  Standard-edition database per class from `EphemeralDatabases`, public so that the
  Datastore-mode harness (#1707) can share it. Gate annotations stay on the concrete classes. Teardown waits for a delete's acceptance
  and the database's absence, never for the operation: a database holding data outlived the
  client's five-minute polling. `scripts/sweep-e2e.sh` deliberately does not sweep databases.
