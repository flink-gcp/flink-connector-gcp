# Detailed guidance — flink-connector-gcp-spanner

Module-scoped guidance, read when working in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `AGENTS.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Sink design (`docs/adr/0075`)

- **The destination is a database, not a table.** The serializer's `Mutation` names its own table,
  so one sink writes to as many tables as it produces. That is why `SpannerDatabase` is at the
  module root, why the cell weights cover the whole database, and why there are no
  per-destination metrics — the cardinality would be the serializer's to choose.
- **One mutation per `MutationGroup`.** The group is the unit `BatchWriteResponse` reports a
  status for; anything else gives up per-record failure routing, which is the whole reason this
  sink uses `batchWriteAtLeastOnce` rather than a commit. Measured, not assumed: every rejection
  shape `SpannerRejectionITCase` provokes comes back per group.
- **The writer is synchronous — no mailbox, no in-flight bookkeeping.** This RPC has no
  asynchronous or self-batching form, so there is nothing to wrap. Do not port the Bigtable or
  Pub/Sub writer's machinery here on the assumption that it was forgotten.
- **The client library retries this RPC not at all** (`SpannerStubSettings.batchWriteSettings` is
  `no_retry_0_codes`; `runWithSessionRetry` recovers only a lost session). The retry loop and its
  knobs are the connector's, the Cloud Tasks shape rather than the Bigtable one. Recheck on a
  client upgrade.
- **Batch write has no replay protection**, so idempotence is the serializer's to supply; it is
  documented on the SPI and on the sink, never enforced.
- `SpannerDatabaseAccess` holds its three operations as functional values over the `Spanner`
  service handle, for the reason `docs/adr/0047` gives for the Bigtable batcher adapter. A test
  faking `DatabaseClient` itself is the thing this exists to avoid.
- `null` from the serializer is skip, and the check sits immediately after the serializer's
  `catch`, ahead of the buffer, the weights and every metric that counts a send (`docs/adr/0001`).

## Error handling (`docs/adr/0076`)

- **Routed: `INVALID_ARGUMENT` and `ALREADY_EXISTS`, per group only.** Retried: `ABORTED`,
  `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`. Everything else fails the job.
- **A constraint violation is `constraintViolationPolicy`'s decision, defaulting to `FAIL_JOB`.**
  It covers **both** `FAILED_PRECONDITION` (NOT NULL, over-long value, foreign key) and
  `OUT_OF_RANGE` (`CHECK`) — measured; a policy covering one of them is a bug, not a subset.
  The default is failing for two reasons, and the first is the load-bearing one: a constraint
  violation usually means the record-to-column mapping is wrong rather than that one record is
  anomalous, so failing is right *even under a dropping handler*; and `FAILED_PRECONDITION` is
  also what every write gets while a CMEK key is unreachable, which self-heals.
- **Never split a status by its message text** to separate those cases. Spanner's own error-codes
  page says the message may change at any time.
- **Do not add a three-valued fail/DLQ/drop option.** `failedMutationHandler` already is that
  choice; two policies over one decision can contradict each other (ADR-0076).
- **A failure of the request is never routed**, whatever its status: it names no mutation, and the
  mutations it carried have no reported outcome.
- Both code mappings (`ErrorCode` → gax, canonical number → gax) are written-out switches, and
  `SpannerErrorClassifierTest` iterates every value of both. A code added to the client library
  must fail a test rather than silently become unclassifiable.

## Batching and cell weights (`docs/adr/0077`)

- **Bounding the request the writer builds is correctness, not tuning** — but read the quotas page,
  not only the batch-write how-to, before saying which limit a knob defends. 80,000 mutations and
  100 MiB are `Commit`'s; the only row naming batch write is **per mutation group** (80,000), and
  the batch-write page's one sentence is about **size**. So no per-request mutation count is
  documented for this RPC at all: `maxBatchBytes` defends a documented request-level limit,
  `maxBatchCells` and `maxBatchMutations` are proxies for size. Beam's defaults are commit-shaped,
  which is how the per-request framing got in.
- **All three batch knobs are bounded at the setter** (#435): 80,000 / 80,000 / 100 MiB,
  package-private `*_LIMIT` constants with the figure named in the `@param` (the `OptionChecks`
  rule — a public compile-time constant inlines into callers).
  The byte ceiling is **measured** (#441, 2026-08-10): the service refuses at exactly 104,857,600
  bytes and accepts well above 10 MiB, so the looser of the two readings was right and the constant
  did not move. It defends a refusal Spanner documents — under `RESOURCE_EXHAUSTED`, which this
  connector retries, so it is not a fail-fast boundary. The **cell ceiling is
  precautionary** — no request-level mutation count is documented either way.
- **`MAX_BATCH_MUTATIONS_LIMIT` is *derived* — `= MAX_BATCH_CELLS_LIMIT`, never a second literal.**
  Every mutation costs at least one cell, so a batch never holds more mutations than cells; that
  derivation is *why* the number is the same, and writing 80,000 twice would leave the mutation
  ceiling cutting below what a batch may hold the moment the cell ceiling moved.
  `SpannerWriterOptionsTest` pins the equality rather than the value.
- **The three limits are ANDed**, so a batch flushes on whichever binds first and raising one alone
  usually changes nothing. Any claim of the form "setting X large breaks the job" has to name the
  knob that *binds*, or it is false — round two of #435 caught exactly that claim, inherited from
  the issue. The one statically detectable case is `maxBatchMutations` above the *configured*
  `maxBatchCells`, which can never take effect; `build()` **warns** rather than rejecting, because
  the configuration works and refusing it would reject something harmless. That is the only log
  statement in an options class in this repository, and the reason is that nothing else — no
  exception, no changed value — could tell the user.
- **A single mutation that breaches a limit on its own is still never rejected** — the check runs
  before a mutation joins the batch, so Spanner's own refusal names the real limit. A range delete
  over an indexed table costs one for the table **plus one per index per row it matches**, which is
  the one way this sink can reach the per-group 80,000 at all; with no secondary index it costs one
  however many rows it hits, and the sink's single-row estimate is then exact.
- `maxBatchCells` counts index entries, read once from `INFORMATION_SCHEMA` when the writer opens
  — dialect-branched across every visible user schema, primary-key index excluded by name, and
  keyed by schema plus table. GoogleSQL names match case-insensitively; PostgreSQL catalog names
  preserve the distinction created by quoted identifiers. Reading it at creation is what makes an
  unreadable schema a job that never starts.
- Table API schema-object options accept one unquoted or canonically quoted component.
  The connector checks component and quote structure but does not copy Spanner's character,
  length, or keyword rules; it decodes SQL quoting before assembling the native data-API name,
  and `INFORMATION_SCHEMA` and the native data APIs remain authoritative.
- **A table the weights do not know is counted without index entries, never rejected.** The
  default's 16-fold headroom under the 80,000 ceiling is what absorbs that, so raising
  `maxBatchCells` toward it is a real trade and the docs say so.
- **The client library exposes no public route from a `Mutation` to its wire form**, and
  `Mutation.toString()` truncates strings at 36 characters. Hence: the byte limit is an estimate,
  and the dead-letter payload is the Java-serialized `Mutation`. Recheck both on a client upgrade
  — a public conversion appearing upstream would reopen the payload encoding.

## Batch source (`docs/adr/0085`, `docs/adr/0083`)

- **The assignment protocol is the base module's**: `SpannerPartitionSplitEnumerator` extends
  `PullAssignmentSplitEnumerator` and supplies only the planning step — `restore`, the partition
  call, the plan and its report, the counters, its own `snapshotState`. A change to assignment
  changes both sources and belongs in `flink-connector-gcp-base` (`docs/adr/0083`).
- **A split is one server-planned partition, and the partition is the unit of progress.** There is
  no position inside one to resume at, so a checkpoint records which partitions a reader still
  holds and recovery re-reads each whole. Do not add an offset: `partitionQuery` gives no order
  contract — a query is partitionable only when its plan begins with a distributed union, which
  rules out the top-level `ORDER BY` that would fix an order — and the emulator's own re-execution
  being stable is not a promise. (The emulator refuses `ORDER BY` too, but its check is *stricter*
  than the service's, so it is not evidence for what Spanner does.)
- **A wake-up costs a partition its progress, deliberately**, and the duplicate is logged at WARN
  and counted by `partitionsReread`. Reading on instead would hold a subtask until the client's own
  read deadline whenever the service went quiet. The case is rare because this enumerator assigns
  only on request, so a mid-partition wake-up comes from `SplitFetcher.shutdown()` — which never
  fetches again.
- **The `cancelled` flag decides whether a split finished, never the read's behaviour** — measured:
  closing a `ResultSet` from another thread ends a blocked `next()` either by returning `false` or
  by throwing `CANCELLED`. Both shapes occur.
- **The split serializer writes both vendor values with Java serialization**, which is this
  repository's one exception to "a checkpointed split owns its own byte format" and is forced: the
  partition token is opaque, the accessors are package-private and there is no public factory.
  The exposure is bounded by the snapshot, not by the format.
- **The enumerator owns `cleanup()`**, wrapped with the client into the one `AutoCloseable` the
  base closes. Readers close their own transaction handle and never call it — releasing the session
  would end every other reader's read. Against the pinned client the call is a no-op (multiplexed
  session); it stays because it is the contract.
- **The deserialization SPI emits zero or more non-null records synchronously through a collector.**
  The collector is valid only for the deserializer call and must not be retained. Emitting nothing
  increments `recordsSkipped` once for the input row. The progress model is unchanged: a reader
  resumes only at a whole-partition boundary, so any interrupted partition is replayed from its
  start regardless of how many outputs each row produced.
- **`SpannerRpcPriority` is at the module root, with the enum-to-SDK mapping on it**, because both
  directions take it — `SpannerDatabase`'s reasoning applied. The mapping is one written-out switch,
  so a value added to either side fails to compile rather than silently changing what a job asked
  for; do not grow a second copy in a direction's own package.
- **The two option families are assembled separately** in `BatchClientPartitionPlanner`:
  `Options.dataBoostEnabled` answers with a `ReadAndQueryOption` and `Options.priority` with a
  `ReadQueryUpdateTransactionOption`, which are siblings rather than one hierarchy. Both are a
  `ReadOption` and a `QueryOption`, so each call site builds its own array — a cast to unify them
  compiles and fails at run time.
- `SpannerClients` at the module root builds the service handle both directions open. The
  emulator-versus-credentials branch is exactly what `docs/adr/0064` exists for; do not grow a
  second copy.
- `SpannerCredentials` loads only service-account JSON and returns `null` when no credential
  override is configured. Serialize only `serviceAccountKeyFile` paths: bounded and Change Streams
  coordinators load on the JobManager, bounded and Change Streams readers and sink writers load on
  TaskManagers, and Table lookup functions load when they open. Restored components reload
  independently, emulator endpoints are mutually exclusive, and loading failures remain cause-free
  so paths and credential material cannot leak.
- **A test needing a `Partition` or a `BatchTransactionId` goes through
  `src/test/java/com/google/cloud/spanner/TestPartitions.java`** — the second file in this
  repository declaring a vendor package, taken under `docs/adr/0067`'s bar and recorded in
  `docs/adr/0085`. Do not treat it as a precedent for a third.

## Change Streams coordinator (`docs/adr/0099`, `docs/adr/0094`)

- The enumerator state is the only partition-lifecycle recovery record; do not add Beam's external
  metadata table. It checkpoints scheduled and running entries and the finished parents that
  establish child dependencies.
- The enumerator owns one checkpointed source-watermark frontier: the minimum safe watermark across
  every unfinished `CREATED`, `SCHEDULED`, and `RUNNING` ledger entry. Broadcast it to every reader,
  including readers with no assigned split, and reject any transition that would move it backwards.
  Maintain its counted ordered runtime index incrementally and rebuild that index from the ledger;
  do not scan the complete lineage on every record-progress event.
- Child discovery does not finish a parent. A reader forwards every child-partitions record and
  sends a separate completion event only after its query ends successfully. A child becomes
  schedulable only after every named parent is finished.
- A valid restored ledger wins and never seeds another null-token query. If an unfinished position
  expired, the default is failure; an explicit fallback discards the entire Spanner ledger and
  seeds one null-token query because advancing one old token can skip its terminal child record.
- Missing explicit retention metadata uses the configured fallback, whose connector default is
  seven days.
- Initialize the dialect, stream scope, watched tables and their all-columns mode, retention,
  partition mode, value-capture type, and exclusion options before releasing readers. Log the
  effective settings and warn when an explicit column list will not follow later schema additions
  automatically.
- Restored readers queue their splits until the coordinator's metadata and expiry check completes.
  A valid restore releases them; a whole-ledger fallback discards them before the replacement
  null-token split is requested. Do not let Flink's independently restored reader state bypass the
  coordinator decision.
- A bounded ledger signals no more splits only after every partition is finished. Runtime queues
  and dependency indexes are rebuilt from the checkpointed ledger rather than serialized beside it.
- `MUTABLE_KEY_RANGE` is rejected before assignment. Its partition start, end, move-in and move-out
  records are a different protocol, not extra records this immutable-key-range ledger can ignore.

## Change Streams reader (`docs/adr/0101`, `docs/adr/0099`)

- Each reader subtask opens at most `maxConcurrentQueriesPerSubtask` asynchronous TVF queries,
  default eight. Excess restored splits stay in a FIFO owned by the reader and remain in its
  checkpoint until capacity returns.
- One query may hand the mailbox only one undrained result. Its `AsyncResultSet` callback returns
  `PAUSE`, and the reader calls `resume()` only after the mailbox emits or processes that result.
  A query error fails the task while retaining the split; only successful end-of-query sends the
  separate partition-finished event.
- Every query uses `executeQueryAsync` on a strong single-use read-only transaction. GoogleSQL is
  decoded from its nested `ARRAY<STRUCT>` envelope and PostgreSQL from the JSON TVF; Change Streams
  does not enable Data Boost.
- `DataChangeRecord.ColumnType` keeps the complete recursive descriptor as normalized JSON with
  object members sorted recursively. Do not flatten it to the pinned SDK's `Type.Code`:
  `TOKENLIST`, annotations, nested arrays, and future service codes have to survive. `Mod` also
  preserves absent values separately from explicit JSON `null`.
- `DataChangeRecord` carries a `@TypeInfo` factory that selects the connector-owned field
  serializer from `TypeInformation.of(DataChangeRecord.class)`. Keep that path independent of
  reflective Kryo and JDK module openings. Its immutable copy contract, length-prefixed JSON and
  versioned serializer snapshot are part of the raw-record network and state boundary.
- Table and column filters run after dialect-specific decoding and before the user deserializer.
  Patterns fully match the Spanner-reported table name or `table.column`; do not case-fold,
  substring-match, or look up a historical schema. Primary-key metadata and keys always survive.
- Determine filter activation once when constructing the reader. When all table and column lists
  are empty, pass the original record directly to the deserializer without calling the filter or
  allocating its result. `skipMessagesWithoutChange` alone does not activate filtering.
- When an active column filter retains every column in the record's metadata, return the original
  record before parsing or rebuilding its mod value JSON.
- Column projection removes a rejected non-key name from `columnTypes` and every mod's old and new
  value objects. Empty projections are delivered by default; `skipMessagesWithoutChange` is the
  explicit opt-in to skip them. Either filter outcome still advances split progress.
- A checkpoint stores the active and queued splits with their greatest consumed timestamp and
  watermark. Restore queries that timestamp inclusively, so the boundary can repeat and delivery
  is at least once; advancing it would skip records that share a commit timestamp.
- Data records carry their commit timestamp as the Flink event timestamp. Heartbeats advance the
  partition watermark reported to the coordinator, while child-partitions records are coordinator
  events rather than user records. Readers emit only the coordinator's complete-ledger frontier
  through the main source output and never mark a quiet partition idle.
- Convert an inclusive Spanner heartbeat instant to Flink milliseconds by subtracting one after
  `toEpochMilli()`, saturating at `Long.MIN_VALUE`. A later nanosecond instant can truncate to the
  same millisecond, and Flink treats records at or below its watermark as late.
- A reader does not open fresh or restored queries until the coordinator initialization event.
  Preserve queued state in checkpoints while waiting, and reject duplicate or unknown
  initialization events.
- The Change Streams deserialization SPI is collector-based and may emit zero or more outputs for
  one `DataChangeRecord`. Every output gets that record's commit timestamp; a successful call with
  zero outputs increments `recordsSkipped` once; and an exception leaves split progress unchanged
  for at-least-once replay.
- The enumerator owns `changeStreamPartitionsDiscovered` and the scheduled-partition lag; each
  reader owns query-open, active-query, queued-partition, queued-lag, missed-heartbeat,
  last-non-heartbeat-wait, and output-filter counters. These are aggregate coordinator or
  reader-subtask metrics: never put partition tokens in metric labels.
- `missedHeartbeatIntervals` excludes the initial null-token query and reports the maximum whole
  intervals across active token queries. Lag gauges use the oldest current position, clamp a future
  position to zero, saturate overflow, and read zero for an empty set.
- Do not duplicate `numRecordsIn`, `currentEmitEventTimeLag`, `watermarkLag`, or `sourceIdleTime`.
  The Flink source runtime derives the metrics available in that Flink version from the commit
  timestamps and source-wide watermarks this reader emits.

## Table Change Streams CDC (`docs/adr/0105`)

- `scan.mode` defaults to the independent bounded source. Change-stream mode implements only
  `ScanTableSource`: no bounded projection or filter pushdown and no lookup provider. It is
  **source-only** — reject it on the sink path, and reject the change-stream options there too,
  because both directions build the physical schema through the same converter and a write would
  otherwise land in the watched table (`docs/adr/0096`).
- Match `DataChangeRecord.tableName` through `SpannerTableName`'s dialect-aware native API key.
  A record for another watched table emits nothing and still advances source progress; never use a
  regex to approximate named-schema or quoted-identifier identity.
- Full changelog mode requires `NEW_ROW_AND_OLD_VALUES`. Emit an adjacent before/after pair by
  copying the complete new row and overlaying the reported old values. Upsert mode accepts
  `NEW_ROW` and `NEW_ROW_AND_OLD_VALUES`, requires the DDL and record primary keys to match, and
  emits key-only deletes.
- Validate every declared physical column against the record's recursive type descriptor. Ignore
  extra watched columns, but fail on missing declared columns, incompatible native types, named
  type FQNs, capture modes, or absent values needed for a complete row.
- Keep absent JSON members distinct from explicit JSON null. A missing complete-row value is not a
  nullable value and must never become SQL null.
- Stage every row from one `DataChangeRecord` before collecting any of them. A malformed later mod
  must not partially emit the record. Failures name only sanitized record identity and the mod
  index; do not attach a cause containing JSON or credential paths.
- Keep record validation and changelog construction in `DataChangeRecordToRowDataConverter`.
  `SpannerChangeStreamRowDataDeserializationSchema` is the collector and produced-type adapter;
  do not grow physical conversion branches back into that SPI wrapper.
- Expose stable scalar `DataChangeRecord` fields through `SupportsReadingMetadata` in the order the
  planner selects them. Keep raw JSON, partition tokens, low watermarks, runtime timestamps, and
  configured resource names outside the metadata surface.
- `mod-number` is zero-based in the original record's mod list. Preserve it while staging rows, and
  attach the same number to both rows of a full-mode update.
- Advertise `commit-timestamp` as non-null `TIMESTAMP_LTZ(9)`. A SQL watermark column must declare
  the key as `TIMESTAMP_LTZ(3)` because Flink rowtime precision stops at 3; the compatible planner
  cast is intentional.
- `SupportsSourceWatermark.applySourceWatermark()` adds no generator. The FLIP-27 reader already
  timestamps data records at commit time and emits the coordinator's complete-ledger heartbeat
  frontier, and that remains the only progress policy.
- Reuse `StructToRowDataConverter` through typed synthetic Spanner values so bounded, lookup, and
  CDC paths retain one decimal, UUID, JSON, PROTO, ENUM, timestamp, array, and null contract.
- Flink 1.20 uses its sole upsert declaration, while Flink 2.x declares key-only deletes through
  the boolean overload in their versioned `CrossVersionChangelogMode` sources. Do not call the
  newer overload from common code.

## Testing

- Emulator ITs pin `gcr.io/cloud-spanner-emulator/emulator`, **not** the `google-cloud-cli` bundle
  the other connectors use: its Spanner emulator predates `BatchWrite` (added in v1.5.31), so the
  whole write path would answer `UNIMPLEMENTED`.
- Shared legacy fixtures keep identifiers lower case and unquoted so one set of mutations and one
  query can serve both dialects. Named-schema coverage also uses each dialect's quoted spelling and
  verifies that the connector decodes it to the native catalog name.
- The production `createWriter(WriterInitContext)` is covered by the emulator ITs rather than by a
  closed-port unit test: this sink reads the schema while creating the writer, so a closed port
  costs the client's whole retry budget (27 s, measured 2026-08-09) to prove less.
- An emulator is never the authority. The gated real-GCP suite (#224) is what confirms the
  rejection table, the schema read in both dialects, which query shapes the service will plan, how
  many partitions it plans, Data Boost, and Change Streams checkpoint, savepoint, retention, and
  fallback behavior in both dialects — and it is the only place a client is built over
  application-default credentials rather than an emulator endpoint.
- **The gated suite's instance is ephemeral, one per gated class** (`docs/adr/0088`, following
  `docs/adr/0044`):
  100 processing units — the floor for a regional configuration — in the `STANDARD` edition, which
  is set rather than defaulted because it is the cheapest edition carrying Data Boost — **measured
  on that instance, not read off the editions page** — and because the free-trial instance that
  would otherwise be tempting is one of the few places Data Boost is
  explicitly unsupported. Nothing persistent is provisioned: an instance bills for as long as it
  exists. The E2E account holds `roles/spanner.editor`, **not** `roles/spanner.admin`: editor
  carries every permission the suite uses, and admin would add `setIamPolicy` on four resource
  kinds to a CI identity.
- **A gated class creates its databases once, in its own `@BeforeAll`, and shares them.** The
  emulator classes can afford a database per test method; against the service each one costs
  seconds, and a test that only reads a schema does not need a database of its own.
- Both markers stay on the concrete classes, never on `AbstractSpannerRealGcpITCase` — and its
  javadoc writes `{@code @EnabledIfEnvironmentVariable}` with no argument list, or
  `just check-gated-tags` picks the base class up as an unpaired gate.
- **Real Spanner refuses extra DDL in a `CreateDatabase` request for a PostgreSQL-dialect
  database**, where the emulator applies it (measured 2026-08-10) — so the gated harness issues
  `updateDatabaseDdl` separately for that dialect and keeps the one-call form for GoogleSQL. A new
  harness that creates a PostgreSQL database will meet this too.
