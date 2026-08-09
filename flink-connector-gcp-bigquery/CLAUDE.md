# CLAUDE.md — flink-connector-gcp-bigquery

Module-scoped guidance, loaded when Claude works in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `CLAUDE.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Facade and serializers (`docs/adr/0016`, `0023`–`0027`)

- One builder, per-write-method SinkV2 implementations; connection multiplexing is the SDK
  pool's, never a self-built writer pool (`docs/adr/0016`). Write-method-only options live in
  nested options objects; `build()` requires the matching one and rejects the others
  (`DefaultStreamOptions` is optional by design — `docs/adr/0028`).
- **A schema problem must never surface from `serialize()`** — every serializer derives eagerly
  (constructor / `of(...)`), because the lazy path reports misconfiguration through the
  `FailureHandler` catch, where log-and-drop leaves an empty table under a green job
  (`docs/adr/0023`, `0024`, `0027`). A serializer `null` is a skip (`docs/adr/0001`) and does
  not loosen this rule.
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

## Error handling and recovery (`docs/adr/0017`, `0030`, `0071`)

- Only row verdicts route to the `FailureHandler`; `findRowLevel` rejects a row-detailed error
  whose own status is transient, and `replayBatches` carries the same no-progress guard as
  `retryBatches` (`docs/adr/0017`).
- The SDK watchdog timeout and `StreamWriterClosedException` are one client-side dead-writer
  verdict (`isWriterClosed`); repair in place, and do not add a bounded `future.get` — declined
  with reasons (`docs/adr/0017`).
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
  name the wrong one. Only `create` retries — `updateSchema`'s `false` means re-read, and repeating
  it would re-submit a stale proposal. **Each wrap has a test asserting the schedule's attempt
  count**, since every other test injects its own admin and an unwrapped one ships green.

## FILE_LOADS (`docs/adr/0018`–`0021`, `0070`, `0071`)

- Deterministic job ids + get-then-submit re-attach; loads commit **in the committer** on the
  checkpoint, synchronously; streaming overflow appends sequentially (`docs/adr/0018`). The
  polling attempt cap stays `Integer.MAX_VALUE` — do not expose it. **`awaitJob` re-fetches
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
  grpc-netty-shaded, whose loader derives its library name from its own package — that argument lives in `flink-connector-gcp-pubsub/CLAUDE.md`). A packaging IT computes that path the same way and fails on
  whichever platform it runs on.
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
  commit time cannot tell you — so `LoadJobOrchestrator` keys on `(destination, format)` and the
  transitional commit after a format change issues **two jobs for one table**, deliberately. Job
  ids need no format segment: they hash the source URI list, which already differs. The serializer
  is v3 and **migrates v2** (the layout `main` has produced since #69, whose committables are all Avro by
  construction) where it still rejects v1. `enableListInference` on every Parquet load is not
  style: without it a `REPEATED` column loads as an empty array and the job reports success.
- **The staging roll threshold is a measured band, and smaller is not better**: the curve is a
  basin with a floor near 8 MiB, so any change to `maxStagingFileBytes` needs a floor as well as a
  ceiling, and the 10,000-URI cap is what the value trades *against* rather than what it is derived
  from (`docs/adr/0070`). The writer keeps no copy of the value and no test-only constructor takes
  it — it reads the option, so a test configures it the way a user would. Load-time numbers quoted
  anywhere carry their date and sample size; a superseding measurement edits them in place.

## Exactly-once (`docs/adr/0022`)

- One buffered stream per subtask, reused across checkpoints, tracked in writer state; **streams
  are never finalized anywhere**; commit = synchronous `FlushRows`, `ALREADY_EXISTS` = success.
  Exactly-once ITs run against real GCP (the emulator keeps no flush cursor).

## Tuning (`docs/adr/0028`, `0029`)

- Connector budgets are `recovery*`, SDK knobs bare `retry*`/`maxRetryDuration` — revised on
  user feedback, and Cloud Tasks' bare `retry*` is a recorded asymmetry, not a drift.
  `maxInflightRequests` defaults to 100 (multiplexing guidance); the pool guard warns, never
  throws. The schema-wait schedule is deliberately not exposed.
- Eviction sweeps at the end of a successful `flush(boolean)`, skipped on `endOfInput` —
  placement is the design. `flushInterval` is a mitigation only; the guarantee still requires
  checkpointing.
- **Two emulator endpoints, one per transport**; `FILE_LOADS` rejects both; the goccy deviations
  live in exactly one place (`StreamWriterRowAppenderFactory`) and retire piecewise as releases
  carry their upstream fixes — per-deviation status and the canary trigger
  (`BigQueryEmulatorMissingTableDeviationITCase`) are in `docs/adr/0029`.

## Table API / SQL (`docs/adr/0031`–`0033`, `0035`; shared rules `docs/adr/0014`)

- No `format` option — the DDL schema is the schema (`docs/adr/0031`). `TIME(p)` caps at 3;
  `TIMESTAMP` → `DATETIME`, `TIMESTAMP_LTZ` → `TIMESTAMP` — both measured. `PARTITIONED BY` is
  rejected, not ignored.
- The buffered/FILE_LOADS mappers build unconditionally, the factory decides by write method —
  not a missing symmetry (`docs/adr/0032`). A rejection restating a builder rule fires on the
  same condition, pinned by a success-side test (the #289 lesson).
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

## Metrics (`docs/adr/0034`; conventions in the base module's CLAUDE.md)

- Three writer metrics classes so no writer registers a metric it cannot increment;
  `numRecordsSend` counts at first hand-off to the client; error-class counters count every
  task-thread-classified failed append, nothing from a gRPC callback thread; gauges' backing
  collections are cleared in `close()`; `loadJobsSubmitted` counts load jobs only.
