# Detailed guidance — flink-connector-gcp-bigquery

Module-scoped guidance, read when working in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `AGENTS.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Table CDC profiles (`docs/adr/0111`)

- Debezium MySQL ordering uses the configured append-only source UUID list as causal epochs and
  encodes epoch, GTID transaction, binlog position, and row.
  Do not infer an epoch from timestamps or accept tagged, multi-source, or interleaved histories.
  Group Replication primary changes and multi-primary histories remain unsupported until MySQL and
  Debezium expose a durable group-wide ordering coordinate; the fixed group UUID cannot supply a
  failover epoch.
  Snapshot epoch zero is valid only for an empty destination.
- TiCDC ordering uses `commit_ts` alone, as one unsigned section, and validates `cluster_id`
  against the configured `sink.cdc.ticdc.cluster-id`.
  That field is the TiCDC cluster's own identifier, defaulting to `default`, not TiDB's.
  Do not add an epoch list, a `ts_ms` tie-breaker, or acceptance of a zero timestamp; the commit
  TSO already totally orders one cluster and survives TiCDC failover, while the source object's
  `ts_ms` is that same value truncated to milliseconds and zero is what the protocol's unused
  MySQL-inherited coordinates carry.
  The profile covers row changes only; DDL and watermark events carry no row and are shapes
  `debezium-json` cannot deserialize, and the protocol has no snapshot stream.
  Only TiCDC's new architecture emits them, from v8.5.4-release.1, and it is the only architecture
  from TiDB v9.0.0; watermarks additionally need `enable-tidb-extension`.
  State both bounds: one TiDB version alone tells a classic-architecture reader the wrong thing.
- Spanner ordering uses commit timestamp nanoseconds, record sequence, and mod number as three
  sections, and needs no configuration.
  Read the timestamp from `source.ts_ns`, never from the same-named field beside `source`, which is
  the connector's own processing time.
  Both the Debezium route and the native `spanner-change-sequence` metadata row must reach BigQuery
  through the one internal encoder, so their equivalence stays true by construction.
  Reject a negative commit timestamp rather than preserving its bits as the PostgreSQL LSN profile
  does, and do not add a `ts_ms` fallback, a snapshot policy, or an epoch list: Spanner assigns the
  commit timestamps.
  Never state the sections as a total order per primary key.
  Spanner's uniqueness guarantee is over sets of fields, so two transactions writing disjoint
  columns of one row may share a commit timestamp and encode to one sequence; that residual tie is
  documented, and `server_transaction_id` cannot break it because it is a distinguisher, not an
  order.
- **The whole CDC public surface is `@Experimental` while #706 stays open** (`docs/adr/0141`):
  the upstream resolution shapes what the sequence-number providers receive, so the 12 CDC types
  sit below the `1.0.0` freeze. The four `BigQuerySinkBuilder` CDC setters are recorded closure
  stops (`docs/adr/0124`).
  The CDC table-provisioning calls `TableInfo.getLabels`/`setLabels` and
  `TimePartitioning.Builder.setField` are method-level `@BetaApi` — internal calls,
  tier-irrelevant; reread them on a BOM bump.

## Facade and serializers (`docs/adr/0016`, `0023`–`0027`, `0140`)

- One builder, per-write-method SinkV2 implementations; connection multiplexing is the SDK
  pool's, never a self-built writer pool (`docs/adr/0016`). Write-method-only options live in
  nested options objects; `build()` requires the matching one and rejects the others
  (`DefaultStreamOptions` is optional by design — `docs/adr/0028`).
- Public conversion SPIs and ready-made format facades use the `*SerializationSchema` /
  `*DeserializationSchema` vocabulary (`docs/adr/0140`). `JsonDocumentOptions` names the JSON
  conversion policy because a JSON document carries no schema of its own. The package remains
  `serializer`, and internal helpers keep `Serializer` or `Deserializer` where that word names
  their implementation role.
- A configured service-account key remains a path in the job graph and is loaded only when a
  runtime component creates its client.
  Every client in one write method must receive it; `FILE_LOADS` uses the same identity for
  BigQuery and GCS, while either emulator endpoint is mutually exclusive with it (#542).
  Credential-loading failures never include the path, key material, or parser cause.
- **A schema problem must never surface from `serialize()`** — every serializer derives eagerly
  (constructor / `of(...)`), because the lazy path reports misconfiguration through the
  `FailureHandler` catch, where log-and-drop leaves an empty table under a green job
  (`docs/adr/0023`, `0024`, `0027`). A serializer `null` is a skip (`docs/adr/0001`) and does
  not loosen this rule. **The transient-and-rebuilt half is `LazyDerivedState`, and the
  descriptor derivation is `RowDescriptors.derive`** (`sink.serializer`, both `@Internal`): the
  four record serializers — proto, Avro, JSON and `RowDataSerializationSchema` — go through them, so a new
  one adds a derivation rather than another double-checked holder (`docs/adr/0024`).
  `ProtoRowAugmentingSerializer` is not one of them: its cache is per destination and invalidated
  by fingerprint, which is a different shape.
- Additional fields are absent unless `additionalFields(...)` is configured.
  Every built-in serializer reaches the common protobuf row boundary before the fields are added.
  Configured fields are physical singular scalar columns used by all three write methods; only the
  internal CDC allowlist may add descriptor-only fields (`docs/adr/0113`).
- **The protobuf mapping is normative for every serializer**; `NULLABLE` is the default mode and
  `deriveRequiredColumns()` the opt-in on both proto and Avro sides — neither default is to be
  flipped per format again, and the method name went through two rejected candidates
  (`docs/adr/0026`).
- `JSON`/`GEOGRAPHY` are schema-derivation markers decided in the single
  `ProtoToTableSchemaConverter.markedType` point; a field claimed by both is rejected there, one
  extension number as both markers in `build()` (`docs/adr/0023`). Well-known types are
  recognised by full name **and** shape, answering `NONE` on a shape mismatch — never throwing
  (`docs/adr/0027`).
- The Avro serializer accepts `IndexedRecord` (so every temporal/decimal conversion accepts raw
  and converted values), rejects unstorable logical types at job start, and its round-trip
  identity with `TableSchemaToAvroConverter` is pinned (`docs/adr/0024`). The JSON serializer
  delegates to `JsonToProtoMessage`; its `BYTES`-column gap is pursued upstream, not patched
  locally (`docs/adr/0025`).

## Error handling and recovery (`docs/adr/0017`, `0030`, `0071`, `0114`)

- Only explicit `UnroutableRecord` results and row verdicts route to the `FailureHandler`.
  A bare resolver `null` and unexpected resolver exceptions stay fatal; an unroutable result is
  handled before serialization or per-destination state and increments only the global error
  counter (`docs/adr/0114`).
  `findRowLevel` rejects a row-detailed error
  whose own status is transient, and `replayBatches` carries the same no-progress guard as
  `retryBatches` (`docs/adr/0017`).
- The SDK watchdog timeout and `StreamWriterClosedException` are one client-side dead-writer
  verdict (`isWriterClosed`); repair in place, and do not add a bounded `future.get` — declined
  with reasons (`docs/adr/0017`).
- One default-stream repair carries its state — created table, reconciled schema, schedule,
  attempt — in the private `RepairState` value object `retryBatches` seeds and mutates; the
  repair steps read and write it instead of threading flags. Keep it private to the writer: it is
  a holder, not the shared retry executor `docs/adr/0039` evaluated and adopted nowhere.
- **A missing table answers `PERMISSION_DENIED`, not `NOT_FOUND`** — `isMissingTable` takes both
  codes, match status codes never message text (the *permission* named tracks neither the RPC nor
  whether the table is absent: three observations, `TABLES_GET` once and `TABLES_UPDATE_DATA`
  twice), exclude failures naming rows, and keep `scheduleFor`'s budget bounds at both
  `retryBatches` sites. The propagation window after auto-creation reaches
  **`BufferedStreamCommitter.flush` too** (~55 s per committable, serially, at the defaults), which
  gates on `CREATE_IF_NEEDED` (no `tableCreated` flag to key on) and takes the narrower
  **`isExistenceMasked`** on a **widen-only-what-was-observed** rule — never on a claim about what
  an expired stream answers, which is unmeasured (`docs/adr/0030`, both halves measured). **Never
  use `PERMISSION_DENIED` as a terminal example** — four tests and two javadoc sentences moved to
  `INVALID_ARGUMENT`, the second pair including the base module's `FailureHandler`.
- **The buffered writer's four append-side decisions take no missing-table verdict, and that is
  measured** (`docs/adr/0030`, its append arm): 140 trials, 0 denials at the first append against
  11 at the `FlushRows` taken on the same table immediately after — so the appends were not merely
  lucky with the timing. Do not widen `recover`, `resendAtSameOffset`, `replayBatches` or
  `probeRestoredStream` on the argument that the window reaches everything; `openAppender` is not
  a fifth site either, since `StreamWriter.build()` sends no RPC. **One** contrary observation
  earns a new issue naming the ADR, not a reopening of #382 — and the gated case *asserts* its
  append count is zero rather than logging it, so that observation stops a run instead of sitting
  in weekly output. The same run makes `createStream`'s allowance measured rather than defensive:
  a quarter of trials needed it, where the case that pre-creates its table had never once been
  denied.
- **A REST failure's retryability is `BigQueryTableAdmin`'s, never `AppendErrorClassifier`'s**, and
  it travels as `RetriableTableAdminException` so the client's `BigQueryException` stays in
  `sink.tables` — the whole point of the `TableAdmin` SPI (`docs/adr/0071`). The verdict borrows
  `BigQueryException.isRetryable()` rather than restating 5xx, adds HTTP 429 and the
  `rateLimitExceeded` reason `isLostRace` already names for the same per-table quota, and leaves
  `quotaExceeded` out (unobserved, and the reason BigQuery also uses for longer-boundary quotas).
  Measured 2026-08-08: sixteen concurrent creations of one absent table, five answered
  `403 rateLimitExceeded` with the SDK's own `isRetryable()` reporting `false`, so there is no
  client-side retry to sit behind. **None of the four creation sites retried even with the call
  inside the `try`** — two are guarded by a `tableCreated` flag, two return before their caller
  loops — so "move it inside" was never the fix.
- **The retry is `RetryingTableAdmin`, a decorator, and it is wired at the three places a
  `TableAdmin` is *constructed*** — both storage sinks on `recovery*`, `FileLoadsCommitter` on
  `schemaReconcile*` — never at the creation sites (`docs/adr/0071`). A use-site rewrite was tried
  first and **missed `LoadJobOrchestrator`**, which is the whole argument: construction sites are
  enumerable, use sites are not. Callers keep the SPI, and no site names a schedule, so none can
  name the wrong one. `create` and `ensureCdcTable` retry; `getSchema` and `updateSchema` pass
  through, because `updateSchema`'s `false` means re-read and repeating it would re-submit a stale
  proposal. **Take the wrapped set from `RetryingTableAdmin`'s javadoc, never from a count written
  here or in an ADR**: `ensureCdcTable` joined with CDC provisioning (`docs/adr/0112`), which
  ADR-0071 now records but originally predated. **Each wrap has a test asserting the schedule's
  attempt count**, since every other test injects its own admin and an unwrapped one ships green.

## FILE_LOADS (`docs/adr/0018`–`0021`, `0070`, `0071`)

- Deterministic job ids + get-then-submit re-attach; loads commit **in the committer** on the
  checkpoint, synchronously; every overflow uses temp tables and inserts deterministic
  intermediate copy levels when more than 1,200 leaves must be combined.
  Ordinary dispositions finish with one copy per destination.
  `WRITE_TRUNCATE_DATA` instead builds one aggregate temp table with `WRITE_TRUNCATE` and finishes
  with a deterministic terminal query, because copy jobs do not support the data-only disposition.
  The whole plan is validated before side effects.
  The single committer then runs at most `maxConcurrentDestinations` destination actions at once,
  with one thread-confined BigQuery client per worker.
  Each job wave is submitted across all destinations before it is awaited.
  Every copy level is fully awaited before the next level is submitted.
  Commit-wide wave sizes bound pending jobs at 50,000 and interactive terminal queries at 1,000;
  destination concurrency must never multiply those limits (`docs/adr/0018`).
  An ordinary destination action failure stops new dispatch, drains every job already submitted in
  its wave, orders the primary and suppressed failures by the plan, and starts no cleanup.
  A JVM-fatal failure aborts the current worker action immediately; the coordinator interrupts its
  peers, drains them for at most 30 seconds and then propagates the fatal failure so an unbounded
  peer cannot hide it from Flink.
  Cleanup interruption propagates — after the bounded worker drain when cleanup is concurrent;
  staged objects remain, although temporary-table cleanup may already be partial.
  The polling attempt cap stays
  `Integer.MAX_VALUE` — do not expose it.
  **`awaitJob` re-fetches
  through `BigQuery#getJob`, never `Job#reload()`**: the convenience throws `BigQueryException`
  on a job carrying an error, which routed the ordinary failure past the `IOException` the SPI
  promises (#337; `docs/adr/0018`). **All three of the runner's `jobs.get` calls go through its
  `getJob(JobId, String)` helper** for the other half of that contract — a failed lookup must not
  leave the SPI as the client's unchecked type either — and the conflict lookup keeps the 409 as
  a suppressed exception. `BigQueryLoadJobRunner`'s unit tests drive it through a scripted
  `StubBigQuery`, with `Job` values minted by `TestJobs` (`docs/adr/0067`).
- **The staging format is a real constraint**: Parquet cannot reach a `JSON` column, and "what
  the load job accepts" is answered only by a load job (`docs/adr/0019`). `DATETIME` stages as
  `local-timestamp-micros`; the literal parser keeps `ResolverStyle.STRICT`, which is
  load-bearing (`docs/adr/0020`). A new supported type owes a column in
  `everySupportedColumnTypeSurvivesTheLoad`.
- Every load reconciles against the live table via `ensureFinalTable`; the native
  `ALLOW_FIELD_*` options are kept deliberately, and the once-per-destination union warn is
  load-bearing (`docs/adr/0021`).
- **Staging files are zstandard, and the reason is CPU — never size.** Measured with this
  writer: deflate 11,436 ms against zstandard 3,182 ms for 2M rows, and 1.8% *more* bytes, so a
  size win must not be reintroduced from #283's 1,000-row probe (17% is a small-file artifact).
  Level 3 is Avro's `DEFAULT_ZSTANDARD_LEVEL`; level 1 measured slower *and* larger, so there is
  nothing below to trade for, and `CodecFactory.zstandardCodec()` has no no-arg overload. The
  library is `com.github.luben:zstd-jni`, which Avro declares `optional` — hence the explicit
  runtime-scope declaration — and the uber-jar relocates its classes while **leaving its native
  libraries unrelocated at the jar root**, which is safe only because `Native.resourceName()`
  builds their path from `os.name`/`os.arch`/version and never from the package (the opposite of
  grpc-netty-shaded, whose loader derives its library name from its own package — that argument lives in `.agents/references/modules/flink-connector-gcp-pubsub.md`). A packaging IT computes that path the same way and fails on
  whichever platform it runs on.
- **Jackson is managed by the root `jackson-bom` import, paired with `avro-parent`'s pin like
  zstd-jni** (`docs/adr/0150`): the five artifacts the bundle ships and the set the Avro tests run
  on come from it, never from a version in this pom.
- **Parquet staging is opt-in and its dependencies are `provided`** (`docs/adr/0072`). Avro is the
  default and stays it: below **256 MiB of load input** Parquet is 3-5x slower — the regime every
  streaming checkpoint sits in — and compressed Parquet cannot be written without a Hadoop runtime
  ("No Hadoop" was measured false; only `UNCOMPRESSED` escapes, at 1.21x Avro's bytes). The `JSON`
  fallback is automatic and a correctness override, not a preference. Parquet's row-group size
  comes from `maxStagingFileBytes` and **must**: at Parquet's own 128 MiB default nothing reaches
  the stream until close and the roll never fires. Both formats are written from the same Avro
  schema, so `TableSchemaToAvroConverter`'s rejections — including flexible column names — apply to
  Parquet unchanged.
- **The staging format travels in the committable, and load jobs group on it.** A committable
  restored from state must load as the format its file was written in, which configuration read at
  commit time cannot tell you — so `CommitPlanner` keys on `(destination, format)` and the
  transitional commit after a format change can contain two groups.
  `WRITE_APPEND` and `WRITE_EMPTY` retain separate direct loads when both groups fit.
  `WRITE_TRUNCATE` and `WRITE_TRUNCATE_DATA` must combine the groups through temporary tables and
  one final replacement action, so one direct truncate cannot erase the other format's rows.
  Job ids need no format segment: they hash the source URI list, which already differs.
  The serializer is v3 and **migrates v2** (the layout `main` has produced since #69, whose
  committables are all Avro by construction) where it still rejects v1.
  `enableListInference` on every Parquet load is not style: without it a `REPEATED` column loads as
  an empty array and the job reports success.
- **The staging roll threshold is a measured band, and smaller is not better**: the curve is a
  basin with a floor near 8 MiB, so any change to `maxStagingFileBytes` needs a floor as well as a
  ceiling, and the 10,000-URI cap is what the value trades *against* rather than what it is derived
  from (`docs/adr/0070`). The writer keeps no copy of the value and no test-only constructor takes
  it — it reads the option, so a test configures it the way a user would. Load-time numbers quoted
  anywhere carry their date and sample size; a superseding measurement edits them in place.
- **The FILE_LOADS writer's expensive destination state is active-only and hard-bounded**
  (#1128).
  `maxOpenDestinations` is per writer subtask and finishes the least recently used file before a
  new one opens; `destinationIdleTimeout` finishes inactive files on processing-time callbacks;
  every checkpoint finishes and clears the remainder.
  Checkpoint and end-of-input finalization is serial by default and may be bounded from 1 through 8
  with `maxConcurrentCheckpointFinalizations`; it drains every submitted close before returning on
  success, ordinary failure, or interruption, and reports results and ordinary failures in input
  order on the task thread.
  A JVM-fatal worker failure wakes the task thread, cancels its peers, and remains primary.
  Finalization does not parallelize appends, size rolls, capacity evictions, or idle closes
  (`docs/adr/0146`).
  `maxPendingFiles` separately bounds the finished and open file metadata retained until the next
  commit; reaching it fails before another file opens, and `pendingFiles` reports the current
  count.
  Do not remove this bound when changing eviction: active-file churn otherwise grows the
  committable list without limit between commits.
  Conversion succeeds before a new destination can evict a healthy one, and object names use one
  writer-global monotonic sequence because per-destination sequences cannot survive eviction.
  `maxSerializedRowBytes` applies to the serializer's protobuf bytes before parsing and Avro
  conversion; larger rows follow the failure policy without creating state.
  The documented Avro sizing baseline starts with one 4 MiB GCS chunk per active destination, but
  Parquet also budgets one `maxStagingFileBytes` row group per active destination.

## Exactly-once (`docs/adr/0022`)

- One buffered stream per (subtask, active destination), reused across checkpoints, tracked in
  writer state; **streams are never finalized anywhere**; commit = synchronous `FlushRows`,
  `ALREADY_EXISTS` = success. Restore resolves duplicate state per destination, never across
  destinations. Clean destinations use `BufferedStreamOptions.destinationIdleTimeout` with the
  same end-of-successful-flush, non-end-of-input eviction boundary as the default-stream writer.
  Exactly-once ITs run against real GCP (the emulator keeps no flush cursor).

## Tuning (`docs/adr/0028`, `0029`)

- Connector budgets are `recovery*`, SDK knobs bare `retry*`/`maxRetryDuration` — revised on
  user feedback. Spanner adopted that vocabulary in
  [#1053](https://github.com/flink-gcp/flink-connector-gcp/issues/1053); the remaining Cloud Tasks
  `retry*` exception is being retired by
  [#1051](https://github.com/flink-gcp/flink-connector-gcp/issues/1051), and ADR-0028's original
  exception is superseded by `docs/adr/0137`.
  `maxInflightRequests` defaults to 100 (multiplexing guidance); the pool guard warns, never
  throws. The schema-wait schedule is deliberately not exposed.
- Eviction sweeps at the end of a successful `flush(boolean)`, skipped on `endOfInput` —
  placement is the design. `flushInterval` is a mitigation only; the guarantee still requires
  checkpointing.
- **Two emulator endpoints, one per transport**; `FILE_LOADS` rejects both; the goccy deviations
  live in exactly one place (`StreamWriterRowAppenderFactory`) and retire piecewise as releases
  carry their upstream fixes — per-deviation status and the canary trigger
  (`BigQueryEmulatorMissingTableDeviationITCase`) are in `docs/adr/0029`.

## Table API / SQL (`docs/adr/0031`–`0033`, `0035`; shared rules `docs/adr/0014`, `docs/adr/0139`)

- A single option value a DataStream builder rejects is renamed to its option key through the
  module's `table.OptionSetters` (`docs/adr/0133`); a new mapper line goes through it, and
  cross-field checks keep the restate-in-DDL-keys judgment (`docs/adr/0007`).
- No option carries a Flink default and no description restates one — a declared default, a
  derived one, or the value absence selects (`docs/adr/0139`). `BigQueryConnectorOptionsTest`
  holds both halves; `reference/bigquery.md` carries a default with its derivation, the
  FILE_LOADS location derivation included. A guard failure edits the description, not the test.
- The factory serves both directions.
  A sink and direct table source require `project`, `dataset`, and `table`; `scan.query` requires
  either `project` or `scan.parent-project` and is mutually exclusive with
  `scan.materialize-views`.
  `scan.parent-project` separates the Storage Read/query billing project from the table-owning
  `project`; absent, it defaults to `project`.
  The bounded source maps every DDL option onto `BigQuerySource.builder()` and converts Storage
  Read Avro values to `RowData` by physical field name (`docs/adr/0100`).
- The table source pushes down top-level projection through `selectedFields(...)`, including the
  first physical column as a carrier for a zero-column planner projection.
  It does not advertise nested projection.
  Its filter pushdown is a conservative Storage Read prefilter: literal comparisons for integer,
  date, decimal, float, double, `BYTES` / `VARBINARY`, `TIME(0..3)`, `TIMESTAMP(0..6)`, and `TIMESTAMP_LTZ(0..6)` columns; boolean equality and
  inequality; BigQuery `STRING` equality; and null checks on those supported column types.
  Decimal and Flink `FLOAT` comparisons use weaker necessary bounds to cover source-side scale
  rounding and `FLOAT64`-to-`FLOAT` narrowing, respectively.
  Temporal comparisons cover the full source truncation bucket at the declared precision;
  unaligned or out-of-range literals remain residual (ADR-0100).
  Binary literals preserve raw bytes through hexadecimal escapes; ordered binary comparisons
  follow Flink's unsigned lexicographic order, held against Storage Read by the gated filter suite.
  String inequality and ordered comparisons are not translated.
  BigQuery `JSON` and `GEOGRAPHY` string equality is unsupported.
  Planning does not fetch the BigQuery schema, so a Flink `STRING` declaration cannot detect those
  unsupported physical types before the Storage Read session is created.
  Collated `STRING` equality may admit extra rows, which the Flink residual removes.
  `CHAR`, fixed-length `BINARY`, nested fields, complex types, casts,
  functions, and field-to-field comparisons remain residual.
  A generated restriction is admitted only while the explicit-plus-generated UTF-8 text remains
  within Storage Read's 1 MB limit; a predicate that would cross the limit remains residual without
  discarding other generated restrictions that fit.
  `AND` may contribute safe necessary children, `OR` requires every branch, and every accepted
  filter also remains a Flink residual.
  `scan.row-restriction` is the raw BigQuery expression surface and is combined with generated SQL
  filters using separately parenthesized `AND` operands.
  A query source never rewrites its query; the restriction applies to its materialized result read.
- Storage Read `RANGE<DATE>`, `RANGE<DATETIME>` and `RANGE<TIMESTAMP>` are source-only rows whose
  nullable `start` and `end` fields use `DATE`, `TIMESTAMP` and `TIMESTAMP_LTZ`, respectively
  (#566; `docs/adr/0100`).
  A sink derives an ordinary `STRUCT` from the same row declaration and never infers a range from
  its field names.
- BigQuery `INTERVAL` remains rejected in both directions (#566; `docs/adr/0100`).
  Measured 2026-08-13, Storage Read returned the undocumented record
  `google.sqlType.INTERVAL(months int, days int, microseconds long)` and one value populated all
  three components.
  Flink splits intervals into year-month and day-time values, and its day-time internal value is
  milliseconds, so neither mapping is lossless; reject the raw-record `ROW` escape hatch too.
- No `format` option — the DDL schema is the schema (`docs/adr/0031`). Flink 1.20 and 2.2 resolve
  SQL `TIME(p)` to `TIME(0)`, while 2.3 retains precision through `TIME(3)`; a programmatically
  constructed catalog type can retain milliseconds through `TIME(3)` on every supported version.
  `TIMESTAMP` → `DATETIME`, `TIMESTAMP_LTZ` → `TIMESTAMP` — both measured. `PARTITIONED BY` is
  rejected, not ignored.
- The buffered/FILE_LOADS mappers build unconditionally, the factory decides by write method —
  not a missing symmetry (`docs/adr/0032`). A rejection restating a builder rule fires on the
  same condition, pinned by a success-side test (the #289 lesson).
- **The factory reads the *session* configuration in exactly one place** — the two FILE_LOADS
  streaming rules, gated on `execution.runtime-mode` (#332; `docs/adr/0032`, which carries why
  that is safe and what was measured to establish it). Consequently the sink's pair is reached
  from SQL only by a value changed after the plan is built, and
  `BigQueryFileLoadsSinkTopologyTest` is their ordinary coverage.
- **`FactoryMocks` builds over an empty `Configuration`, so every `FactoryMocks` test is
  implicitly streaming.** A factory test therefore cannot round-trip a non-append
  `write-disposition`, and anything needing a checkpoint interval belongs in
  `BigQueryTableWriteMethodsPlanTest` against a real `TableEnvironment`.
- Table CDC consumes an upsert changelog only under the default stream and keeps writable sequence
  metadata after the physical row, outside schema derivation (`docs/adr/0111`).
  Preserve key-only delete serialization, reject `UPDATE_BEFORE`, and add Debezium ordering one
  connector profile at a time without a timestamp-only fallback.
- **CDC auto-creation is a recoverable Tables API plus verified-DDL protocol** (`docs/adr/0112`).
  Create schema and primary key through `tables.insert`; when maximum staleness is configured,
  use the pending/complete ownership label, verify through `INFORMATION_SCHEMA.TABLE_OPTIONS`, and
  never treat REST `Table.maxStaleness` as an oracle.
  `tables.insert`, `tables.patch`, and `tables.update` all silently dropped the field in the
  2026-08-14 measurement.
  Preserve restart completion, parallel idempotence, dynamic-destination re-entry, and the rule
  that an unlabeled existing table is verified but never modified.
- `sink.table-create.*` checks shape, never the clusterable scalar type list; the
  field-without-granularity rejection has no builder backstop and is load-bearing
  (`docs/adr/0033`).
- The uber-jar inherits `docs/adr/0015`; its own record — the slf4j exclusion, the two named
  commons relocations, the relocated Avro trade-off, the Arrow/netty weight it accepts — is
  `docs/adr/0035`. **Read both before changing this module's pom.** The shared half of that pom
  (shade `artifactSet`/`filters`/`transformers`, the dependency-recording executions, the licence
  execution) is in the **root pom's `pluginManagement`**, including the `META-INF` exclusions and
  the JDK-interface SPI filters that were found here; this module's pom carries its
  `<relocations>`, its surefire override, `japicmp.skip` and its dependencies. A change to the
  shared block is verified by a zero-delta comparison of both uber-jars' entry names and CRCs.

## Source (`docs/adr/0079`, `0083`, `0084`)

- A configured service-account key remains a path in the job graph.
  The JobManager loads it for the read-session client and for query or view materialization; each
  TaskManager loads it for its stream-reading client.
  Every source mode receives that path; the deployment must mount the same key at it on both
  process types.
  Absent uses ADC, and either emulator endpoint is mutually exclusive with it (#542).

- The whole Storage Read path runs through `BigQueryReadClient`/`BigQueryReadSettings`, both
  class-level `@BetaApi` in the pinned client while the service itself is GA — an internal call
  that does not touch the source's `@Public` tier (`docs/adr/0141`); reread the annotations on a
  BOM bump.

- **One read session creator belongs to one enumerator** (`docs/adr/0128`).
  `BigQuerySourceConfig` carries a `ReadSessionCreatorFactory`, not a creator, and
  `BigQueryStorageReadSource` mints one in both `createEnumerator` and `restoreEnumerator`, closing
  it itself if the enumerator's constructor throws before taking it. `ReadSessionCreator` is
  deliberately not `Serializable`. `QueryRunner` stays on the configuration and stays shared: it is
  not `AutoCloseable`, so no teardown makes reuse unsafe, and its lazy-build guard already names a
  second enumerator over the same object.

- **The assignment protocol is the base module's** (`docs/adr/0083`):
  `BigQueryReadSplitEnumerator` extends `PullAssignmentSplitEnumerator` and supplies the read
  session — `restore`, the planning call and its report, the counters, its own `snapshotState`.
  Everything the bullets below say about assignment still holds; it is just no longer written
  here, so a change to it is a change to both sources and belongs in `flink-connector-gcp-base`.

- **A split is one read stream plus the input rows already consumed, and the offset advances once
  after each successful deserializer call and its synchronous downstream emissions.** Zero, one,
  or many outputs advance it by one; a deserialization or downstream failure does not advance it.
  Split and split state are two types because two threads touch them. The read session is created
  once, guarded by a checkpointed flag; `readSessionsCreated` above 1 means that guard failed.
- **The enumerator keeps no ledger** — no subtask-to-splits map. That absence is the design (the
  reference implementation's change log records a "critical data loss bug in reader split handling"
  *fixed by adding* per-reader no-more-splits signalling — so the protocol, not the ledger, is what
  breaks quietly, and Flink already does the per-reader half), and it is why the
  enumerator reports counters rather than an assigned-splits gauge. Do not add one. Flink's *own*
  coordinator does suppress a request from a subtask already told there are no more splits, and
  clears that only on `subtaskReset` — the same reset that returns the splits — so do not write that
  a finished reader picks up a returned split.
- **The terminal-offset case rests on a measurement, not on a lookahead**: BigQuery answers a read at
  exactly the stream's row count with an empty stream (measured 2026-08-09). The per-record
  `lastInStream` envelope was designed and declined, and a `finished` flag on the split was
  implemented and removed — nothing can set it, since Flink drops a split's state before telling the
  reader it finished.
- Both stream-count knobs default to `0`. `maxStreamCount` is a cap and **never a floor** — a 6 MB
  table answers with one stream at `maxStreamCount(8)`, measured — so nothing may promise a count.
- **One emulator endpoint for a `table(...)` source, two for a `query(...)` one** (`docs/adr/0029`,
  `docs/adr/0087`): the read path makes no REST call, and the query job is one. That is ADR-0079's
  "revisit when the source grows a metadata call" answered rather than left open — do not restore
  the "makes no REST call at all" wording, and **do not add a metadata call to the read path**,
  which is what auto-detecting a view would have cost.
- **A view is read through a query job, and nothing infers one by default** (`docs/adr/0087`). The
  Storage Read API cannot read a logical *or* materialized view — both answer `INVALID_ARGUMENT:
  request failed: non-table entities cannot be read with the storage API`, same code and same words
  (measured 2026-08-10), which is why one mapping covers both. `table(...)` naming a view fails and
  gains a sentence naming `query(...)`. That hint is the module's one match on message text and the
  argument for it is narrow — it decorates a failure and never routes one — so do not cite it as a
  precedent for `isMissingTable`-style matching (`docs/adr/0030` still governs there), and **do not
  make it route** by materializing reactively off that error: that is the one shape of automatic
  view handling this module refuses outright.
- **`materializeViews()` is the opt-in, and it stays opt-in.** It spends one metadata call per job
  to tell a view from a table, and making it unconditional would put that call on every ordinary
  table read — the property ADR-0079 asked to be revisited deliberately rather than eroded. Its
  generated `SELECT` folds `selectedFields` and **not** `rowRestriction`: the rule is **fold into
  SQL the connector wrote, never into SQL the user wrote**, which is also why `query(...)` is
  passed through untouched. `snapshotTime` is rejected beside it.
- **Where a query's result lands is the caller's choice, and neither path deletes anything at
  teardown** (`docs/adr/0087`). Unset is BigQuery's anonymous dataset — nothing created, no storage
  charged, a re-plan a free cache hit onto the same table (measured); `queryResultDataset(...)`
  creates a table with a one-day expiration. A teardown-time delete is wrong in both: teardown also
  runs on a JobManager failover, where the restored job is still reading the session that table
  backs.
- **The query runs from `plan()` under the same flag as the read session, and neither the enumerator
  state nor any serializer changed for it.** Nothing after planning can tell the two kinds of source
  apart, since a split names a stream and the opener opens by stream name — so if a change here
  starts needing the table in the state, that is the signal something has been put on the wrong side
  of the seam. The query job's id is random by default; `reuseQueryResultWithin(...)` opts into a
  deterministic one (`docs/adr/0089`) keyed on the **Flink job name** — read out of the enumerator
  metric group's `<job_name>` variable, the one route to it, measured across a global failover on
  all three supported Flink versions by `BigQueryQueryJobIdentityITCase` — plus a digest of
  everything the runner reads and a window bucket. Do not key it on the JobID: in HA application
  deployments the JobID is derived from the HA cluster id and recurs across redeploys, so when it
  changes is not something a user can reason about (the finding that overturned #477's premise).
  The window is bounded at 24 h at the setter because both landing places expire at about a day;
  the previous window's id is only ever looked up, never submitted, which is what keeps BigQuery's
  six-month id retention from producing an unreachable id. A task failure never re-runs the query
  at all — only the global-restore path rebuilds the enumerator (measured, same ITCase) — so the
  exposure the knob closes is a JobManager failover before the first checkpoint, and the honest
  reading of the knob is "attempts inside a window share a result", redeploys included. Adopting
  a **finished** job spends one `getTable` on its result table (#485): the job's metadata names
  the table whether or not it still exists, so a table gone early — deleted by hand, or an
  anonymous cache table dropped inside its nominal day — is probed past like a failed link and
  the query runs fresh. Do not remove the check as redundant with the window arithmetic: the
  arithmetic bounds only the tables that expire on schedule.
- **No recovery test against the emulator**, which ignores `ReadRowsRequest.offset` and answers from
  row zero; a green test there proves the opposite. The read-path deviations are pinned by
  `BigQueryEmulatorReadDeviationITCase`, each with a `@Disabled` twin carrying the correct
  behaviour, and the emulator harness uses a **hyphen-free project id** because the emulator's Avro
  namespace is `<project>.<dataset>` and a hyphen is illegal in one.
- The deserializer may declare a reader schema; rows are resolved into it. It emits zero or more
  non-null records synchronously through the call's collector and must not retain that collector.
  Emitting nothing increments `recordsSkipped` once for the input row. The shipped `GenericRecord`
  implementation answers with `GenericRecordAvroTypeInfo`, which is why `flink-avro` is a
  `provided` dependency — Kryo cannot serialize a `GenericData.Record` at all (measured).
- **The client library's own `ReadRows` retry is the loop; the connector adds only a stop**
  (`docs/adr/0084`). It resumes a broken call at `originalOffset + rowsProcessed`, so a
  connector-level reopen would be the same thing less precisely — **do not add one**, and do not
  widen `setRetryableCodes` either: the vendor excludes a bare `INTERNAL` and a bare
  `RESOURCE_EXHAUSTED` with a stated reason, and there is no evidence here against it.
  `retryMaxAttempts` sets `maxAttempts` and nothing else, and **`totalTimeout` is not an
  alternative** — gax resets the attempt count on progress but carries the first attempt's start
  time forward, so one bounds a stuck stream and the other would cut off a healthy long one.
- **A progressing-but-retrying stream trips no bound**, which is exactly why `readRetries` exists;
  it is a `ThreadSafeSimpleCounter` because the client's retry scheduler increments it. The listener
  is registered through `RowStreamOpener.setRetryListener`, once per subtask **before any fetcher
  starts** — the client captures it when it is built, so a later registration reports nothing.
- **`ReadClientRowStreamOpener`'s client lifecycle is monitor-only, deliberately**
  (`docs/adr/0131`, refined by #1046). Every read and write of its `client` and `closed` fields
  happens inside `synchronized (this)`, so unlike the Bigtable and Spanner seams there is no
  `volatile` and no double-checked fast path — they would guard reads that do not exist, and
  opening a stream is a per-split rarity for which taking the monitor costs nothing. Only
  `onRetry` is volatile, because its write does not take the monitor. Credentials arrive by pull —
  the key file remains a path in the job graph and each component loads its own — not through a
  push seam, and a `LazyBigtableDataClient`-style holder stays declined: a holder with one user is
  a rename.
- **Session expiry is explained, never pre-empted** (`docs/adr/0084`): the split carries the
  session's expiry so a failure past it says a restart cannot help, and **nothing refuses to read
  on a local clock** — not the reader, not the enumerator's restore check, which stays a warning.
  Nothing claims what status BigQuery answers an expired session with; that is unmeasured, and
  keeping the message clear of it is what makes it testable without a six-hour wait.
- **Multi-stream coverage reads a public dataset**, because that removes the storage cost ADR-0079
  priced rather than the read cost. Measured 2026-08-10: BigQuery splits somewhere between 195 MB
  (one stream) and 264 MB (four), and **a projection lowers the count** — it follows the bytes
  selected, not the table's size — so the fixture reads every column, deliberately.
- **Avro is the only wire format, and Arrow was measured and declined** (`docs/adr/0090`). **Do not
  re-propose an Arrow deserializer without engaging that record**: "Arrow is faster" is what it
  confirms, not what it refutes, so a re-proposal has to exhibit a consumer that takes a batch
  without materialising a row from it — a faster decode number is not that. The same run measured
  this module's own Avro path: **the stream decoder allocates a fresh 8 KiB buffer on every
  `configure`**, so `AvroRowCursor` pays one per response block.
- **A fetch is bounded independently by 10,000 rows and an 8 MiB serialized-Avro target** (#1136;
  `docs/adr/0079`).
  The cursor measures bytes actually consumed by each row without disabling Avro's buffered decoder.
  A row that would cross the byte target is decoded once and held for the next fetch; one row larger
  than the target is emitted alone.
  That target bounds neither the retained response block nor decoded-object heap, and Flink may hold
  `(source.reader.element.queue.capacity + 2)` batches per reader.

## Metrics (`docs/adr/0034`; conventions in the base module's detailed guidance)

- Three writer metrics classes so no writer registers a metric it cannot increment;
  `numRecordsSend` counts at first hand-off to the client; error-class counters count every
  task-thread-classified failed append, nothing from a gRPC callback thread; gauges' backing
  collections are cleared in `close()`; `loadJobsSubmitted` counts load jobs only.
  FILE_LOADS commit concurrency adds only aggregate queued, active, current-duration and
  last-duration gauges; never add destination labels to them.
