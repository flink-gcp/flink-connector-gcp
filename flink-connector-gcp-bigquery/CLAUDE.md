# CLAUDE.md — flink-connector-gcp-bigquery

Module-scoped guidance, loaded when Claude works in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `CLAUDE.md`.

## Design decisions (do not silently revisit)

- **BigQuery**: `BigQueryIO`-style facade — one builder, per-write-method SinkV2 implementations.
  Storage Write API connection multiplexing is delegated to the client SDK connection pool
  (`setEnableConnectionPool`); no self-built keyed writer pool. The serializer SPI is an abstract
  class (`BigQueryProtoSerializer`) with `getDescriptor(TableDestination)` + `ByteString`
  rows — not a functional interface (descriptors are not Java-serializable). `serialize` returning
  null skips the record, in all three writers (#230; the root `CLAUDE.md` carries the contract and
  its three implementation rules) — which does **not** loosen the eager-derivation rule below. A
  schema problem must still not surface from `serialize()`, and returning null instead of throwing
  would hide it *better* than the trap that rule exists for: a skip is not routed anywhere, so it
  is invisible outside `recordsSkipped`
- **BigQuery error handling** (#13; SPI extracted to the base module by #37/#205): the row-level
  failure policy is the shared `FailureHandler<FailedRow>` from `base.failure` — fail-job
  (default), log-and-drop, DLQ routing — and the base module's CLAUDE.md owns the lifecycle
  contract (open at `createWriter`, flush after each writer flush's drain, at-least-once for
  failures that recur on replay). This module keeps `FailedRow` (implements `FailedElement`;
  `getConnector()` = "bigquery", `describeDestination()` = the `p.d.t` string) and the builder
  setter keeps its `failedRowHandler` name — domain vocabulary at the surface users touch. The
  old `FailedRowHandler`/`FailedRowHandlers`/module-local `DeadLetterQueue` stub were deleted
  outright, not aliased (nothing published). The three sinks open the handler in their
  production `createWriter`/`restoreWriter` via `DefaultFailureHandlerContext.of(context)`. The
  default-stream sink's `@VisibleForTesting createWriter(appenderFactory, tableAdmin)`
  deliberately does not open, so fake-injected writer tests need no `WriterInitContext` — but
  the buffered sink's `@VisibleForTesting` 3-arg `restoreWriter` is the production delegate and
  **does** open (its writer tests bypass the sink and construct the writer directly). The three
  writers call the handler's `flush()` after their drains, and their `close()` uses
  `Closers.closeAll` (`base.lifecycle`, never Flink's `IOUtils.closeAll` — #276, whose reasoning
  is in the base module's CLAUDE.md) so the handler is closed even when closing an appender or
  service, or aborting a staged file, throws: the lifecycle contract promises close on the failure
  path too. On the FILE_LOADS path that promise is testable in exactly one shape, and the shape is
  the point rather than an accident — `StagedFileWriter.abort()` swallows an `IOException` or a
  `RuntimeException` by design, so an `Error` is the only failure that list can carry, which is
  what `closeStillClosesTheHandlerWhenAbortingAStagedFileThrowsAnError` drives through the staging
  storage's own close.
  **`findRowLevel` rejects a row-detailed error whose own status code is transient** (#213
  round-2 review): the SDK copies the response's status code verbatim onto
  `AppendSerializationError` after its in-stream retries, so row details under `UNAVAILABLE` &c.
  are an availability verdict, not a data verdict — retrying the whole batch is always safe (a
  failed append wrote nothing), while routing on it could dead-letter rows a later attempt would
  write. This makes "outage-shaped failures never reach the handler" a property of the code, not
  of the service's conventions; before the filter it held only by vendor contract on the
  SDK-exception path (the connector's own transient-before-row-errors guard in
  `responseToThrowable` sits on a path SDK 3.30.0 never takes for errored responses).
  **`replayBatches` carries the same no-progress guard as `retryBatches`**: row errors naming no
  row in the batch drop nothing, and re-appending the identical batch (with the attempt counter
  reset and no backoff) would loop for as long as the server repeats the verdict — the buffered
  writer lacked the guard the default writer had, found by trying to refute the classification
  claims rather than by reading the diff.
  `FailedRow` carries serialized protobuf bytes, not the original record (the writer is
  stateless). SDK in-stream retry settings were hardcoded in `StreamWriterRowAppenderFactory`
  until #54 exposed them on the default-stream path via `DefaultStreamOptions` and #198 exposed
  them on the buffered path via `BufferedStreamOptions` (see those entries). The SDK's
  callback-wait watchdog timeout (#163) — `MaximumRequestCallbackWaitTimeExceededException`,
  thrown when a sent append gets no response for the SDK's hardcoded 5 minutes — is a plain
  `RuntimeException` with no gRPC status, and only the **first** future of a dead-connection
  storm carries it raw (siblings get `StreamWriterClosedException`), so status-code
  classification alone would fail the job on exactly the failure that names the root cause.
  `AppendErrorClassifier.isWriterClosed` therefore matches both exceptions ("client-side dead
  writer, stream unaffected") and `requiresWriterRefresh` delegates to it; both storage writers
  repair in place through their existing paths. A bounded `future.get(timeout)` as further
  defense was **considered and declined** on #163: the watchdog's coverage was verified against
  SDK 3.30.0 (requeued-on-reconnect requests are re-timestamped and resent in the same
  append-loop iteration, so "never sent, never checked" is not a real window), the residual
  SDK-bug hang is already bounded by Flink's checkpoint timeout → failover, and a second timeout
  would race the SDK's hardcoded 5 minutes and could tear down slow-but-progressing appends
- **BigQuery FILE_LOADS** (#14, load stage revised in #69): exactly-once via deterministic
  BigQuery job ids (hash of destination + sorted staged URIs) with get-then-submit re-attach.
  Avro-only staging in v0.1, written with the `google-cloud-storage` client directly (no Flink
  filesystem plugin dependency). Load jobs run **in the committer** behind a pre-commit topology
  (`SupportsPreCommitTopology`) whose trailing `.global()` routes every subtask's committables to
  committer subtask 0 (the #14 post-commit-topology design was replaced in #69: records emitted
  to a post-commit topology during job shutdown are not guaranteed to be processed — verified
  empirically, the final streaming batch was lost — while committer commits ride the
  final-checkpoint wait and the framework's committer state). Jobs are submitted all at once then
  awaited. Cleanup is best-effort on success only; a staging bucket lifecycle rule is the
  documented mitigation for orphans
- **The staging format is a real constraint, not an interchangeable detail** (#281). Three places
  said FILE_LOADS stages Avro *only incidentally*, added by #145 as part of the argument that the
  *protobuf* mapping is the normative one, and two of them drew the substitutability conclusion
  outright — the `AvroSchemaOptions` javadoc with "a staging format rather than a contract, and
  Parquet is equally possible", this file with "could stage Parquet"; the docs page named Parquet
  nowhere. **That conclusion is withdrawn as false.** Measured against real BigQuery, a Parquet
  load cannot reach a `JSON` column by any route: with a provided schema it is refused at
  *job-configuration* level — `Unsupported field type: JSON` whenever the schema names one,
  whatever the file holds — and the schema-less routes fail the table's type check instead, except
  Parquet's own JSON annotation under autodetect, which lands **silently as `BYTES`**.
  `INTERVAL`/`RANGE` are refused by target type too. So the formats are not substitutable and a
  Parquet path cannot be a straight swap — the constraint #284's design has to work within.
  What #145 actually needed is the narrower claim, and it never depended on the staging format at
  all: every write path goes through a protobuf row, and FILE_LOADS converts *that row* into the
  file it stages — so the staging format sits downstream of the mapping. Say it that way and it
  stays true whatever FILE_LOADS stages, which is the point: a formulation that has to be revisited
  per format is how the withdrawn claim got written in the first place. The sibling entry is the
  #282 one below, reached the same way — what the load job accepts is a question only a load job
  answers. The measurements are on #281; #283 (zstd) and #285 (the 1.5 GiB roll threshold, a
  **larger** lever on load time than the format is) came out of the same round of runs
- **BigQuery streaming FILE_LOADS** (#69): same `WriteMethod.FILE_LOADS` value, allowed under
  explicit `STREAMING` + checkpointing (`AUTOMATIC` stays rejected); `WRITE_APPEND` only. The
  checkpoint is the trigger: each completed checkpoint's committables are committed
  synchronously (a slow load delays the next checkpoint = backpressure; async in-flight loads
  were evaluated and rejected — `commit()` must mean durable, or a crash after the next
  checkpoint strands submitted-but-unconfirmed loads). A `FileLoadsCheckpointStamper` pre-commit
  map stamps the checkpoint id onto committables (the `Committer` SPI cannot see it); job ids
  gain a visible `-c<checkpointId>` segment (hash material unchanged) and derive their Flink-job
  segment from the committable's originating job id (stamped by the writer) so re-commits after
  a new-JobID restore still re-attach; streaming overflow submits direct append jobs
  sequentially instead of temp-table+copy. Streaming also requires EXACTLY_ONCE checkpointing
  and checkpoints-after-tasks-finish (the final batch rides the post-finish checkpoint). Quota
  guard at graph construction: interval < `minCheckpointInterval` (default 2 min) errors, < 5
  min warns (1,500 load jobs/table/day), plus a runtime cadence warning in the committer
- **FILE_LOADS committer schedules** (#198): `loadJobPoll*` and `schemaReconcile*` on
  `FileLoadsOptions`, mapped by `toLoadJobPollSchedule()` / `toSchemaReconcileSchedule()`. Both pass
  the #54 workload-versus-service test that kept the default-stream schema-wait schedule
  unexposed: completion polling paces the **caller's own** `jobs.get` quota and latency (it covers
  the overflow path's copy job too), and the etag-race budget absorbs contention from **other
  writers of the same table** — a second job, a Storage Write API sink on the same destination,
  external tooling — a property of the deployment, not of BigQuery. **It is not about this job's
  parallelism**: `prepared.global()` routes every committable to committer subtask 0, so one job
  has exactly one reconciler. The first draft of this entry said the opposite, transplanting the
  wording from the default-stream path, where the etag loop really is per writer subtask (and is
  deliberately not exposed). **The polling attempt cap stays unexposed and hardcoded to
  `Integer.MAX_VALUE`**: a batch load may legitimately run for hours, so any bound a user could
  set would fail loads that were progressing normally, and the Flink job's own timeouts are the
  right ceiling. Exposing it "for symmetry" is the mistake to avoid. `BigQueryLoadJobRunner` takes
  its schedule as a constructor argument rather than reading the options — it implements the
  `LoadJobRunner` SPI and must not depend on the FILE_LOADS options type
- **Buffered-path SDK retry knobs** (#198): `BufferedStreamOptions` gained the `retry*` /
  `maxRetryDuration` five, mirroring `DefaultStreamOptions` per the #54 naming split (connector
  budgets are `recovery*`, SDK knobs bare `retry*`). This deleted
  `StreamWriterRowAppenderFactory.RETRY_SETTINGS`, whose only remaining consumer was the buffered
  service; the SDK mapping stays in that factory as an overloaded `toRetrySettings`, **not** on
  the options class — the mapping-on-the-options rule in the base module's CLAUDE.md is about
  `RetrySchedule`, and putting a gax type on a `@PublicEvolving` class would be worse than the
  `@Internal` project type. Reaching the service meant widening the `BufferedStreamServiceFactory`
  SPI to `create(location, options)`; both are `@Internal` and unpublished, so the signature
  changed rather than being routed around. Defaults reproduce the old constant exactly, and
  `maxRetryDuration` defaults to the SDK's own 5 minutes, which that path did not previously set —
  so the buffered path's behavior is unchanged
- **BigQuery FILE_LOADS live-table reconciliation** (#142): `ensureFinalTable` is the shared
  entry point for **every** load — direct and temp-table alike — memoized once per destination per
  run (`finalTableSchema`; the orchestrator is constructed per commit, so the memo is naturally
  per-run and streaming overflow's sequential per-partition direct loads reconcile once). The
  defect it fixes was measured on real BigQuery: a load job *adding* a `REQUIRED` column is
  rejected at submission even under `ALLOW_FIELD_ADDITION`, so the old direct path — serializer
  schema, unreconciled — failed the whole job under `allowNewFields()` exactly when the run fit
  one partition, while the temp-table path demoted the addition to `NULLABLE` and succeeded
  (tightening an *existing* column's mode, the other measured row, is silently ignored and was
  never a problem). Consequences that are decisions, not accidents: missing tables are created via
  `TableAdmin` (with `TableCreateOptions`) before the load, retiring the load-job-driven creation
  machinery (`mayCreate`/`missingTables`) and `LoadJobSpec`'s partitioning/clustering fields — so
  a failed load can leave an empty table or an applied schema union behind, as the temp path
  always could, columns being irreversible anyway; `CREATE_NEVER` + missing table is a client-side
  `IOException` before anything is submitted; and `bigquery.tables.get` became an unconditional
  FILE_LOADS requirement (one read per destination per run — previously the default config made no
  TableAdmin call on the direct path). The native `ALLOW_FIELD_ADDITION`/`ALLOW_FIELD_RELAXATION`
  options are **kept** on `WRITE_APPEND` jobs — asked, and the user chose keeping them
  (2026-07-27) as belt-and-braces against external mid-run schema changes — even though a
  reconciled provided schema makes them no-ops otherwise; do not drop them as "dead" in a cleanup.
  With updates **disabled** the live schema wins outright, and — measured — BigQuery silently
  ignores a staged Avro field the provided schema lacks: the rows load and that column's data is
  dropped, where the unreconciled direct path had failed loudly at submission ("Cannot add
  fields"). The orchestrator warns once per destination by probing the union with the disabled
  options and catching `SchemaUnionException` — that warn is what remains of the old loudness, so
  it is load-bearing, not a simplification target. `WRITE_EMPTY` + updates enabled now unions on
  the direct path too (batch-only; streaming rejects non-append). Both measured rows are pinned
  against real BigQuery by `BigQueryFileLoadsSchemaEvolutionITCase`
- **FILE_LOADS stages `DATETIME` as `local-timestamp-micros`, never as text** (#282). It used to
  stage a `string` formatted `yyyy-MM-dd'T'HH:mm:ss.SSSSSS`, on the stated grounds that *"Avro has
  no timezone-less datetime logical type universally accepted by BigQuery loads"*. That is
  measurably false, and the text form did not merely lose fidelity — **every** load job carrying a
  `DATETIME` column failed, with `useAvroLogicalTypes` on or off: `400 Field v has incompatible
  types. Configured schema: datetime; Avro file: string.` The neighbouring `JSON` and `GEOGRAPHY`
  columns survive the same shape only because BigQuery coerces text into *those*, which is
  presumably why the pattern looked safe when it was written — so do not reason from them to a
  third type. Do not reintroduce the string form for `INTERVAL` or any later type either: the
  question is always what the load job accepts, and only a load job can answer it.
  Two things this cost, worth keeping: `TableSchemaToAvroConverterTest` and
  `ProtoToAvroConverterTest` were green throughout, because they assert the two converters agree
  with **each other** and the disagreement was with the service (the
  *emulators-are-not-authorities* rule in a shape that has no emulator in it at all); and
  `BigQueryFileLoadsITCase` carried `STRING`/`INT64`/`GEOGRAPHY` only, so no load job had ever
  carried a `DATETIME`. That gap is closed by
  `everySupportedColumnTypeSurvivesTheLoad`, which loads **every** type this write method supports
  and asserts each value back — `TIME`, `NUMERIC`, `BIGNUMERIC`, `DATE`, `TIMESTAMP`, `BYTES`,
  `JSON`, `STRUCT` and `REPEATED` were all equally unverified, and all measured good on
  2026-08-04. A new supported type owes a column there.
  The **string wire form** (a hand-written serializer's `DATETIME` field declared as a proto
  `string`; the Storage Write API takes either) is parsed rather than rejected or read strictly —
  decided with the user, 2026-08-04. `ProtoToAvroConverter.DATETIME_LITERAL` expresses BigQuery's
  documented literal grammar (`YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.F]]`), so one serializer works
  unchanged under `STORAGE_API_*` and `FILE_LOADS`; rejecting it as `TIME`'s string form is
  rejected would have broken that parity, and strict ISO would have failed strings the service
  accepts. It is written from the public grammar rather than copied from the client library's
  private `JsonToProtoMessage` formatter, which is what keeps the module README's "no source code
  has been copied" claim true.
  **`.withResolverStyle(ResolverStyle.STRICT)` on that formatter is load-bearing and must not be
  dropped as noise**: `DateTimeFormatterBuilder.append(DateTimeFormatter)` copies the appended
  formatter's printer-parser but **not** its resolver style, so an assembled formatter gets the
  `SMART` default even though `ISO_LOCAL_DATE` is itself `STRICT`. Under `SMART`, `2026-02-30`
  resolves to the 28th and `24:00:00` rolls into the next day — the staged file then carries a
  date nobody wrote, under a green job, where BigQuery answers the same literal with an error.
  Found in self-review, measured on a JDK-17 probe; `STRICT` costs nothing, since it governs
  resolution and not the `parseLenient` field widths, and the usual `STRICT` trap does not apply
  because `ISO_LOCAL_DATE` appends `YEAR`, not `YEAR_OF_ERA`, so no era is demanded. The formatter
  is deliberately a **superset** of the grammar — omitted seconds, a lowercase `t`, either
  separator where the other would do, a year of other than four digits, a signed year — and
  accepting more is safe **only** because each of those has exactly one reading, which is
  precisely what the calendar cases do not. A year outside BigQuery's documented `0001`–`9999` is
  rejected in `toCivilMicros` rather than staged, for the reason a load job is all-or-nothing: a
  value no column can hold must fail its own row here, not the whole job later. That check fires
  on the **literal path only** — `CivilTimeEncoder.decodePacked64DatetimeMicrosLocalDateTime`
  applies the identical `1..9999` bound itself (`checkValidDateTimeSeconds`, measured against SDK
  3.30.0), so the packed path cannot reach it; it stays because that bound is the SDK's invariant
  rather than ours.
  One claim to **not** repeat, because it was made here first and was wrong: deleting the old
  `DATETIME_FORMAT` did *not* fix a locale hazard. `DateTimeFormatterBuilder.toFormatter()`
  hardcodes `DecimalStyle.STANDARD`, and `ofPattern`'s default locale sets only the *text* locale,
  so an all-numeric pattern stays ASCII under `th-TH-u-nu-thai` — non-ASCII digits need an explicit
  `withDecimalStyle`, which that code never called (measured). What the old formatter really
  carried was the `STRICT` trap from the other side: `yyyy` is `YEAR_OF_ERA`, so it staged
  proleptic year 0 as `0001` and year -1 as `0002` — a silently wrong year, the same shape of
  defect the resolver style now rules out
- **BigQuery STORAGE_API_EXACTLY_ONCE** (#30): buffered streams + 2PC on checkpoints. **One
  stream per writer subtask, reused across checkpoints and tracked in Flink writer state**
  (Dataproc-connector style; stream-per-checkpoint explicitly rejected — GCP support told the
  user frequent CreateWriteStream churn is not intended usage). Committable = (streamName,
  inclusive flushOffset, subtaskId); committer calls `FlushRows` synchronously, `ALREADY_EXISTS`
  = already flushed = success, everything else throws (restart + idempotent re-commit; no
  deterministic-id machinery, no checkpoint stamper, no `.global()` — committer runs at sink
  parallelism, the pre-commit topology is identity and exists only as the validation hook).
  Restore: synchronous probe append at the restored offset; offset conflicts / dead stream /
  reopen failure abandon the stream for a fresh one at offset 0 (rows past the restored offset
  were never committable, so they stay invisible). **Streams are never finalized anywhere** —
  real BigQuery rejects `FlushRows` on a finalized stream (verified; the batch IT caught it),
  so finalizing races restored-but-uncommitted commits; open streams' unflushed tails are
  invisible and cost nothing. Server-side row-level errors route to the `FailureHandler` with
  offset-recompute recovery (atomic request rejection → route failing rows, replay survivors +
  trailing batches; `ALREADY_EXISTS` during an offset-shifting replay is terminal). v1 scope:
  fixed destination only (builder rejects `destinationResolver`), no mid-stream schema
  evolution (stream schema pinned at creation), BATCH supported (commit at end of input),
  streaming requires EXACTLY_ONCE + checkpoints-after-tasks-finish; recovery knobs are
  builder-configurable via `BufferedStreamOptions` with defaults. The goccy emulator keeps no
  flush cursor (re-flush duplicates), so exactly-once ITs run against real GCP; the emulator
  gets a single-flush smoke test only
- **Per-write-method option scoping** (decided in #14, was deferred on PR #46): write-method-only
  options live in a nested immutable options object set on the builder (`FileLoadsOptions` via
  `fileLoadsOptions(...)`, `BufferedStreamOptions` via `bufferedStreamOptions(...)`); `build()`
  requires it for its write method and rejects it for others
- **BigQuery JSON columns** (#49 paths, #50 field options): a `JSON` column is carried as a string
  by the Storage Write API, so `ProtoSchemaOptions` is purely a **schema-derivation marker** —
  it decides whether the derived schema says `JSON` instead of `STRUCT`/`STRING` for table
  auto-creation, the write stream and load jobs. It covers **message and string** fields (a message
  is printed as canonical proto JSON; a string is passed through verbatim and *not* validated —
  malformed JSON is a BigQuery row-level error, routed to the `FailureHandler`). #50's issue text
  says message-only; that was widened in the implementing PR because the corpus the feature exists
  to migrate annotates **string** fields, so option selection alone would have delivered nothing.
  `isJsonField(field, path)` decides the configured JSON marking; #126 made
  `ProtoToTableSchemaConverter.markedType` the single decision point both converters consult (see the
  geography entry below). Consequences not
  to re-litigate: an unset plain proto3 string is **left unset rather than written as `""`** (the
  row descriptor's JSON field has presence, and `""` is not valid JSON, so writing it would fail
  every record that omits the field) — limited to fields without presence, since elsewhere `""` is
  the user's own statement. An option **number matching no field is deliberately not an error**,
  unlike a path, because one configuration serves every message type a job writes. **No
  `ExtensionRegistry`**: protobuf-java never resolves custom options against the descriptor pool
  (not even for a declared dependency), so the unknown-fields read is the *normal* path, and
  `getAllFields()` reaches a generated extension by number without the generated class. Because
  protobuf's private extension range has no registry, the option's **full name is its identity** —
  `jsonFieldOption(GeneratedExtension)` captures it (the extension itself is not `Serializable` and
  must not be retained); `jsonFieldOptionNumber(int)` remains for descriptors that arrive without
  the annotations artifact, and then only the wire encoding can stand in for the type check. Both
  **accumulate** like `jsonFieldPath` — one job can meet several annotation vocabularies — keyed by
  number so a named entry always wins over a bare one at the same number, and the last name wins
  when two extensions claim one number. The name **rules out a foreign declaration**; it cannot
  arbitrate between two rivals both present in the pool, since an unresolved option records only its
  number
- **BigQuery geography columns** (#126): a `GEOGRAPHY` column is carried as a string by the Storage
  Write API exactly as a `JSON` one is, so this is the same **schema-derivation marker** mechanism
  and *nothing* on the value path — a marked string is `Kind.IDENTITY` with the #50 empty-string
  rule, since `""` is no more a valid geometry than valid JSON. **The marker mirrors the JSON one
  exactly**: `geographyFieldPath`/`geographyFieldPaths` on both serializers, plus
  `geographyFieldOption`/`geographyFieldOptionNumber` on the protobuf one, unioned the same way.
  Paths-only was the first decision and it was **reversed on measurement**: the estimate behind it
  ("~200 lines of near-duplicate API, javadoc and extension-number validation") was wrong, because
  `checkExtensionNumber` and `BoolFieldOptionReader` are already static and shared — the real cost was
  ~80 lines, mostly javadoc that cross-references the JSON methods. Quantify before pricing a decline;
  the symmetry argument offered alongside it was weak anyway, since the JSON marker is *already*
  asymmetric (Avro has no annotation mechanism at all) so an option form adds no new kind of
  asymmetry. `isGeographyField` therefore takes a `FieldDescriptor` again, which the paths-only
  version had dropped as an unused parameter — the argument did not fail, its premise changed. Both
  predicates now share one `carriesAnyOption` so they cannot drift on what "carries this option"
  means, and the shared number check's message names neither marker — nor does
  `BoolFieldOptionReader`'s, which said "a JSON field option must be declared as…" to a user who had
  configured a geography one until self-review caught it. **One extension number registered as both
  markers is rejected in `build()`**, being broken for every message rather than for some; every other
  collision needs a descriptor (an option against a path, or two different numbers meeting on one
  field) and stays at derivation. Two checks because they are two rules — the first draft's comment
  claimed no vocabulary intersection was computable at build() time, which was simply false. **Strings only**, the one place
  this marker is *narrower* than the JSON one: `jsonFieldPath` also takes a message and prints its
  canonical protobuf JSON, but no protobuf message means a geography to BigQuery, so there would be
  nothing to write. That rejection is stated about the *field's type*, so it fires however the field
  was selected — a message carrying the geography annotation is rejected exactly as a marked path to
  one is, which is why the fixture keeps it in its own `AnnotatedGeographyBadType` message rather
  than as a field of `Annotated` (there it would fail every other test's conversion).
  The refactor is the point of the change as much as the feature: the JSON decision expression was
  duplicated in `ProtoToTableSchemaConverter.convertField` and `ProtoRowConverter.buildFieldPlan`
  with a comment in each saying it must stay identical to the other, and a second marker would have
  doubled that hazard. It is now one package-private `ProtoToTableSchemaConverter.markedType(field,
  path, options)` returning `JSON`, `GEOGRAPHY` or `null`, folding the automatic JSON of
  `Struct`/`Value`/`ListValue` on top of `ProtoSchemaOptions.markedType`; the row converter calls it
  rather than recomputing. `AvroSchemaOptions.markedType(path)` is the same shape on that side (no
  well-known-type layer to fold). `FieldPlan.jsonString` became `verbatimString` accordingly.
  A field claimed by **both** markers is rejected, and the check lives in `markedType` — the single
  decision point — rather than in `Builder.build()`, even though build() would catch the
  path-versus-path case client-side and earlier. Two reasons: a JSON *field option* cannot be
  intersected with a geography *path* without a descriptor, so build() could never own the whole
  rule; and every sibling rule (unmatched paths, recursion, case collisions, mappability) already
  lives at derivation, so one early check for one rule would be its own inconsistency. Derivation is
  the right place **because `ProtoMessageSerializer` now derives eagerly in its constructor**, which
  #126 fixed as part of the change: it did not, so every proto schema misconfiguration — the JSON ones
  included — was reported from `serialize()`, inside the writers' `FailureHandler` catch, where
  log-and-drop swallows it once per record for the life of the job and leaves the table empty with the
  job green. The deferral first written here ("pre-existing, wants its own issue") understated it by
  saying "from a task manager": the failure went through the *row-failure* path, not merely a remote
  one, and the fix was the one line `AvroRecordSerializer` had carried all along. A configured marking **wins over well-known-type
  recognition and is then rejected** for not being a string, rather than silently falling back to the
  automatic `JSON` — nobody should have to guess which won.
  Two things **measured**, not assumed. The goccy emulator *does* create and round-trip a
  `GEOGRAPHY` column, unlike the `ARRAY<JSON>` it rejects outright, so the marker is covered by the
  ordinary emulator IT (`BigQueryAvroSerializerITCase`, asserting the created column's **type**, since
  the value path would read identically had the marker been ignored). And FILE_LOADS carries one end
  to end: `BigQueryFileLoadsITCase` now stages a `GEOGRAPHY` column and reads it back with
  `ST_ASTEXT` against real BigQuery. That check was worth running rather than trusting — #126's body
  asserts the round trip works, but only our own converters were evidence for it, and BigQuery's
  documentation describes WKT loading for CSV and JSON and *not* for Avro. `AvroRowConverter.toKind`
  was the one exhaustive `TableFieldSchema.Type` switch without a `GEOGRAPHY` case, and its absence
  would not have failed a schema test: the column derives correctly and then throws on the first
  record, inside the writers' `FailureHandler` catch. Same rule as the Avro and #147 entries.
  `INTERVAL` and `RANGE` stay underivable, **considered and declined**, and the docs say so:
  Avro's `duration` is a `fixed(12)` of months/days/millis against BigQuery's year-month plus
  microsecond day-time, so either direction is a lossy re-encode, and `TableSchemaToAvroConverter`
  rejects both outright — deriving either would break the FILE_LOADS round trip
  `AvroSchemaRoundTripTest` pins. That is the same reasoning that killed `Duration` → `INTERVAL` in
  #147. `RANGE` has no Avro or protobuf equivalent at all. `JsonDocumentSerializer` needed **no
  change** — a supplied schema already says `GEOGRAPHY` and `JsonToProtoMessage` passes the string
  through — but that was untested, so it now is. Adding the marker to a running pipeline is a
  **breaking schema change** (`STRING` → `GEOGRAPHY`): `SchemaUnifier` only relaxes, so it rejects
  the union rather than corrupting rows, as it does for #147's `STRUCT` → scalar
- **BigQuery Avro serializer** (#66, Avro half; the JSON half closes the issue in a second PR):
  `AvroRecordSerializer` is `ProtoMessageSerializer`'s shape with an Avro front end — the
  schema is held as its **JSON text** (serializable, unlike a parsed `Schema`) and the
  `TableSchema`/descriptor/row-converter triple is rebuilt lazily. It accepts **`IndexedRecord`**,
  not `GenericRecord`, so `SpecificRecord` streams work; consequently each temporal and decimal
  conversion accepts **both** the raw Avro value and the converted one (`Instant`, `LocalDate`,
  `LocalTime`, `LocalDateTime`, `BigDecimal`, `UUID`), because a generated class with Avro's
  conversions enabled carries the latter and assuming the former would be a per-row
  `ClassCastException`. `AvroToTableSchemaConverter` is the inverse of the FILE_LOADS
  `TableSchemaToAvroConverter`, which is why `AvroSchemaRoundTripTest` pins the two against each
  other: an Avro serializer feeding FILE_LOADS goes Avro → `TableSchema` → Avro, so drift corrupts
  staged files instead of failing a build — but note the round-trip **identity** it pins holds only
  under `deriveRequiredColumns()`, since `REQUIRED` is the only mode `TableSchemaToAvroConverter` maps
  back to a bare type. The default's `["null", T]` shape is pinned separately, but that half is a
  weaker guard — there is no identity to compare against — so the *values* on the union path are
  covered by `ProtoToAvroConverterTest` instead, which is where a nullable decimal or a nullable
  struct would break. Decisions not to re-litigate: nullability is **`NULLABLE` by default with
  `AvroSchemaOptions.deriveRequiredColumns()` as the opt-in** — see the protobuf nullability entry
  below for the reasoning, which is shared and was settled in #145; it touches **schema derivation
  only**, leaves `REPEATED` alone (a BigQuery `REPEATED` column cannot be `NULLABLE`) and recurses
  into nested structs and map entry columns; Avro `map<string,V>` →
  `REPEATED STRUCT<key,value>` rather than rejected as the Dataproc connector does, because the
  proto path already gives proto maps that shape; JSON and geography columns are marked by **dotted path
  only** (Avro has no field-option mechanism, so `ProtoSchemaOptions`' annotation form has no
  analogue — a separate fact from Avro having no JSON logical type, which is why a marker is needed at
  all); and the logical types BigQuery cannot store faithfully
  (`timestamp-nanos`, `local-timestamp-nanos`, `duration`, `big-decimal`, `uuid` on a `fixed`) are
  **rejected at job start** rather than silently falling back to the base type — literally at job
  start, because the schema is derived in `AvroRecordSerializer.of(...)` rather than lazily: the
  lazy path first runs from `serialize()`, inside the writers' `FailureHandler` catch, where one
  misconfiguration would look like a poison record and a log-and-drop policy would swallow the whole
  stream. A `["null", array]` field is `REPEATED`, so a null array and an empty one are
  indistinguishable — BigQuery offers no way to keep them apart, and the alternative is rejecting
  the schema. Two things caught in self-review and worth not re-deriving: BigQuery bounds a
  parameterized decimal by its **integer** digits (`NUMERIC(P,S)` needs `S ≤ 9` and `P - S ≤ 29`,
  `BIGNUMERIC` `S ≤ 38` and `P - S ≤ 38`), not by total precision, so `decimal(35,2)` is BIGNUMERIC
  and `decimal(77,38)` is rejected; and `AvroRowConverter` pairs schema fields to descriptor fields
  **by position**, because `BQTableSchemaToProtoDescriptor` lowercases with the *default* locale —
  under `tr_TR` a column named `ID` becomes the proto field `ıd`, which no `Locale.ROOT` key
  matches. Position is exact here precisely because the descriptor is always derived from the table
  schema this connector just produced
- **BigQuery protobuf well-known types** (#147, which is #124 Part 2): the vocabulary is
  **protobuf's, not this project's** — *well-known types* names the messages in
  `google/protobuf/*.proto` (protobuf.dev/reference/protobuf/google.protobuf/), and the enum's
  grouping is Google's own (wrappers / the temporal pair / the structural trio). Same rule as #121's
  `sink.storage`: spell it the way the vendor spells it, and say in the javadoc that it is the
  vendor's word, so nobody later "improves" it into a local coinage. The test fixture follows suit —
  `WellKnownTypes` is a **noun phrase like its sibling `AllTypes`**, because the message is not
  itself well-known, it *contains* every well-known type; `WellKnown` alone was an adjective and was
  renamed for that reason. Recognition lives in a
  package-private `ProtoWellKnownType` enum keyed on **full name** — a descriptor built from a
  serialized `FileDescriptorSet` carries its own copy of `wrappers.proto`, so identity comparison
  would miss every one — replacing `ProtoToTableSchemaConverter.isTimestampMessage`, which was a
  boolean only because n was 1. **The name is necessary but not sufficient**: `of()` also checks the
  message really has the sub-fields the conversions read (`seconds`+`nanos`, `paths`, `value`), and
  answers `NONE` when it does not, so the message expands as the ordinary `STRUCT` its author
  declared. Nothing reserves the `google.protobuf` package — `package google.protobuf; message
  Duration { int64 millis = 1; }` is legal — and on the name alone that derived an `INT64` column
  and then threw a field-less `NullPointerException` on **every record**, from inside the writers'
  `FailureHandler` catch, where log-and-drop would swallow the stream. Measured, and it is the
  same rule the Avro entry above states: **a schema problem must not surface from `serialize()`**.
  Answering `NONE` rather than throwing is deliberate — there is nothing to reject, only a name that
  did not mean what it usually does. Note this could not be relocated with a `checkArgument`:
  `ProtoMessageSerializer` builds its state lazily, so on a task manager even plan construction
  happens inside that catch; the failure had to be *removed*, not moved. **Six constants, not sixteen**: the nine wrappers share one, because
  both the column type and the conversion kind come from the wrapper's `value` sub-field through the
  *same* `scalarType`/`scalarKind` functions a bare scalar goes through, so a `UInt64Value` inherits
  the `uint64` range check with no second table to keep in sync. Mappings: wrappers → the wrapped
  scalar; `Struct`/`Value`/`ListValue` → `JSON`; `Duration` → `INT64` micros; `FieldMask` → `STRING`
  of comma-joined **verbatim** paths (`FieldMaskUtil.toString`, not `toJsonString`, which
  lowerCamelCases them); `Any` → **nothing**, it stays `STRUCT<type_url, value>` because unpacking
  needs a `TypeRegistry` the connector cannot obtain — and marking it JSON is not a workaround, since
  the printer then fails per record. `INTERVAL` for `Duration` was rejected because
  `TableSchemaToAvroConverter` rejects it and would break the FILE_LOADS round trip (#126), and
  `REPEATED STRING` for `FieldMask` because a *repeated* `FieldMask` cannot be flattened, so singular
  and repeated would map differently.
  Two placements are load-bearing. Auto-JSON is folded into the **existing marking branch** in
  `convertField` rather than added as a branch in `convertMessageField` (the `jsonColumn` boolean it
  was folded into is now the `marked` type `markedType` returns, since #126): that way `modeOf`'s "a
  singular marked column is never REQUIRED" rule covers it with no new clause, the recursion guard is
  never reached (these types are mutually recursive and were rejected outright before), and **a
  configured JSON marking keeps winning** — the branch returns before the message type is inspected.
  The identical expression used to appear in `ProtoRowConverter.buildFieldPlan` under a comment saying
  the two must stay identical — an auto-JSON column's target field is a *string*, so a plan that
  disagreed would ask it for its message type and throw at construction. #126 retired that hazard by
  extracting the one `markedType` both call; the constraint is now structural, not a rule to
  remember. And the WKT switch sits **before** the recursion guard, so
  two `Timestamp`s on one path are not a rejection. Modes need no new rule at all: these are message
  fields, so they have presence. The one deviation is deliberate — a proto2 `required` wrapper
  derives `REQUIRED`, and it is mandatory, so that is faithful.
  An out-of-range `Duration` is a **row-level** failure like uint64 overflow, rewrapped so the
  message names the field (protobuf's own names none); sub-microsecond truncation is silent, as it
  already is for `Timestamp`. `FieldPlan` moved to named static factories rather than a ninth and
  tenth constructor parameter — state grew by exactly two fields, since `Timestamp` and `Duration`
  share `seconds`/`nanos`, and a wrapper's `value` and a `FieldMask`'s `paths` are both "the
  message's only field". Only `Duration`/`FieldMask` need the `instanceof`-or-rebuild shape
  `toEpochMicros` established, because only they *construct* a well-known type to hand to
  `Durations`/`FieldMaskUtil`; a wrapper does not, so one `getField` serves a generated instance and
  a `DynamicMessage` alike.
  Two things **measured**, not assumed. A zero-field message (`google.protobuf.Empty`) is rejected at
  schema derivation by a check stated about *columns* rather than about `Empty`, so it catches any
  user-written empty message too: the BigQuery client library rejects such a column itself ("The
  RECORD field must have at least one sub-field") before a request is ever sent, with a message
  naming no field. And `REPEATED JSON` works on **real** BigQuery but not on the goccy emulator,
  which rejects every insert into a table carrying an `ARRAY<JSON>` column, empty or populated —
  originally worked around with a `SingularWellKnownTypes` fixture for the emulator write test
  plus a standalone `BigQueryProtoRepeatedJsonITCase` for the repeated half; #16 folded both into
  the gated `BigQuerySerializerFidelityITCase`, which writes the full fixture on the service, and
  the emulator class keeps only the schema half. This is a
  **breaking schema change** for any existing table (`STRUCT` → scalar): `SchemaUnifier` rejects the
  union rather than corrupting rows
- **BigQuery protobuf nullability** (#124 Part 1, with Part 3's `oneof` pin; Part 2 is the entry
  above): `ProtoToTableSchemaConverter` derives the mode from presence only under
  `ProtoSchemaOptions.Builder.deriveRequiredColumns()`, and the default stays **`NULLABLE`**.
  Reasons, in order of weight: proto3's presence-less form is the spelling you get by *not* thinking
  about nullability, so deriving `REQUIRED` from it by default would make nearly every scalar column
  of an auto-created table `REQUIRED` on the strength of a syntax default; and `REQUIRED` is the mode
  BigQuery cannot walk back. **This mapping is normative for every serializer** — every write path
  goes through a protobuf row (`STORAGE_API_*` directly; the Avro and JSON serializers via
  `BQTableSchemaToProtoDescriptor`; FILE_LOADS converts that same row into the file it stages) — so
  **#145 moved Avro onto this default and this method name**, rather than the reverse, and both
  serializers now take `deriveRequiredColumns()` with only the signal differing (a `["null", T]`
  union there, presence here). #145 carried that argument on a claim about the staging format
  which #281 withdrew; the staging entry above has the measurement. **Neither default is to be
  flipped per format again**: that is the whole point of the two agreeing.
  That supersedes the "not symmetric on purpose" reasoning first recorded on #124, which weighed
  Avro-schema faithfulness in isolation, before the protobuf mapping was settled and before #142 was
  measured. There is **no inverse switch on either side** — `allFieldsNullable()` was removed from
  Avro by #145 and never added here (the #124 title notwithstanding): with a `NULLABLE` default it
  would mean exactly "don't call the opt-in", and two inverse switches need a documented meaning per
  combination. **The name went through two rejected candidates**, so don't
  re-open it: `deriveRequiredFromPresence()` names a protobuf mechanism and so cannot be shared with
  Avro, and `deriveRequiredFromSchema()` was worse — *everything* here is derived from the schema
  (types, JSON columns, the whole `TableSchema`), so the qualifier distinguished nothing.
  `deriveRequiredColumns()` names what appears on the BigQuery side, which is also where the
  irreversibility lives, and matches the `allowNewFields()` / `allowFieldRelaxation()` vocabulary
  already borrowed from Aiven's connector. Getter is `isDeriveRequiredColumns()` — `is` + verb phrase
  is clumsy but is the house style (`isAllowFieldRelaxation()`). Note the polarity is a **deliberate
  deviation** from that connector, whose `allBQFieldsNullable` defaults to `false`. The predicate is
  `isRequired() || !hasPresence()`, **two clauses because a proto2 `required` field has presence
  and is mandatory all the same** (`isRequired()` is `fieldPresence == LEGACY_REQUIRED`;
  `hasPresence()` is the **full** disjunction `isProto3Optional || MESSAGE || GROUP || isExtension()
  || containingOneof != null || fieldPresence != IMPLICIT`, guarded by `!isRepeated()` — write it out
  when reasoning, because the `MESSAGE` clause is the one that gets forgotten, and #124 Part 2 is
  entirely about message types) — presence alone would map the one unambiguous case to
  `NULLABLE`. `isRepeated()` is tested **first**, so a repeated marked field stays
  `REPEATED JSON` (or `REPEATED GEOGRAPHY`, since #126); a mutant reordering those two lines fails
  seven tests. **A singular marked column is never `REQUIRED`** — the rule was stated about JSON
  before #126 generalised the wording to the marking, but it is the same rule and the same reason —
  stated about the marking rather than about presence: `ProtoRowConverter`'s
  `omitEmptyString` (the #50 rule) is set to `!hasPresence()`, *identical* to the `REQUIRED`
  trigger, and `BQTableSchemaToProtoDescriptor` builds its row descriptor with **no syntax** →
  proto2 → `LABEL_REQUIRED` is enforced by `build()`, so the pair would throw
  `UninitializedMessageException` on every record omitting the field (verified by mutation: the
  mutant reports `missing required fields: a_string, a_twice`). The broader rule loses fidelity
  only for a proto2 `required` JSON field, which is worth one clause. A **proto3** map entry's `key`/`value` have
  implicit presence and so become `REQUIRED`, converging with the Avro path — but that scope is
  load-bearing: a message-valued map keeps a `NULLABLE` value (the `MESSAGE` clause above), and in
  proto2 both entry fields have explicit presence and stay `NULLABLE`. The **value path is
  unchanged** — the issue body's claim that this writes `0`/`""` where NULL was written before is
  wrong, since `MessagePlan.convert` skips only on `hasPresence() && !hasField()` and a
  presence-less scalar was already written with its default. `SchemaUnifier` needed no change: it
  only relaxes, so derived-`REQUIRED` against an existing `NULLABLE` column is a silent no-op
  already pinned by `modesAreNeverTightened`
- **BigQuery default-stream tuning knobs, eviction and flushInterval** (#54):
  `DefaultStreamOptions` in `sink.storage` beside `BufferedStreamOptions`, same shape, but
  **optional on the builder** — the one deliberate deviation from the two-adjacent-checks
  convention (decided with the user, 2026-07-28): the "required" half exists so that *explicitly
  choosing* a write method forces its options into view, and the default write method is chosen
  by not choosing, so only the "rejected for other methods" half carries safety and only it
  stays. Retry naming was **revised on user feedback** (2026-07-29) after a first cut shipped
  `retry*` (connector) beside `sdkRetry*` (SDK): two knobs both stemmed "retry…MaxAttempts" were
  judged a confusion source. Now the **connector budget is `recovery*`**
  (`recoveryInitialBackoff`/`recoveryMaxBackoff`/`recoveryMaxAttempts`, matching the writer's
  internal `recoverySchedule` vocabulary, renamed in `BufferedStreamOptions` too so the two
  classes agree) and the **SDK knobs are bare `retry*`/`maxRetryDuration`** — the `sdk` prefix
  became redundant once "retry" uniquely meant the SDK layer, the bare names are the vendor's
  own words per the #121/#147 rule, and the Pub/Sub builder already exposes SDK `RetrySettings`
  bare (`retryInitialDelay`, `retryDelayMultiplier`, …), so this converges the modules. Note the
  cross-module asymmetry left standing: Cloud Tasks' `retry*` names a *connector-driven*
  schedule — its module has no second retry layer, so bare `retry*` is unambiguous there and
  renaming it would churn a published-in-docs surface for no local gain. `maxInflightRequests` **defaults to 100, deviating from the SDK's 1000 on purpose**
  (official multiplexing guidance, sample value 100): a pooled connection is a scale-up
  candidate above 20% of its in-flight limits, so at the SDK default scale-up needs >200 queued
  requests per connection and rarely triggers — measured against SDK 3.30.0 sources, where the
  first writer's limits are baked into the JVM-static pool and later writers' are silently
  dropped (only a `limitExceededBehavior` mismatch throws). That first-writer-wins fact is also
  why the `ConnectionWorkerPool.setOptions` guard in `StreamWriterRowAppenderFactory` **warns
  and does not throw** on a second sink requesting different pool bounds in one JVM: a throw
  could not deliver the second value set either, and failing a session-cluster job over a
  hygiene knob is disproportionate. The pool floor is latched at pool construction, the ceiling
  is read live — hence the guard runs before this factory's first `StreamWriter.build()`, and
  its javadoc concedes another client may have created the pool first. The schema-wait schedule
  (flat 30 s × 30) is **deliberately not exposed**: it paces BigQuery metadata propagation, a
  service property, not a workload property. The writer keeps its package-private
  `(maxAppendRequestBytes, recoverySchedule, schemaWaitSchedule)` constructor for tests; the
  public options constructor takes the schedule from `DefaultStreamOptions.toRecoverySchedule()`,
  as the buffered writer and the committer take theirs from
  `BufferedStreamOptions.toRecoverySchedule()` — the mapping-on-the-options rule and the one
  shared jitter ratio are recorded in the base module's CLAUDE.md. Both mappings were jitter-free
  before #197, with no reason ever recorded for it; they now carry
  `RetrySchedule.DEFAULT_JITTER_RATIO` like every other schedule.
  **Cold-destination eviction** (`destinationIdleTimeout`, default 1 h, enabled — decided with
  the user 2026-07-28; disable = set a large duration, no separate flag) sweeps at the **end of a
  successful `flush(boolean)`, skipped on `endOfInput`**: that is the point where every pending
  batch is empty and every in-flight append awaited, so closing an appender there cannot cancel a
  live append and no `collectFailedSiblings`-style draining is needed — placement is the design.
  The `pendingCount() == 0` guard is defensive (a dropping `FailureHandler` can leave
  re-appended rows pending past the await loop); a failed appender close is WARN-logged and never
  fails the flush (hygiene must not fail a checkpoint). `lastAccessNanos` lives on
  `DestinationState`, is refreshed in `write()` only, and is initialized at creation so a state
  rebuilt by a repair is not instantly idle; boundary is strict (`> timeout` evicts, `== timeout`
  keeps), pinned by test. **`flushInterval`** (default disabled) registers a recurring
  processing-time timer from the writer constructor via `WriterInitContext.getProcessingTimeService()`
  — the first `ProcessingTimeService` use in the repository; safe because timer callbacks run on
  the mailbox/task thread, the same invariant `states` already relies on (the sink comments this
  where it passes the service). The callback checks a task-thread `closed` flag (set in
  `close()`) rather than cancelling a future, calls the real `flush(false)` (so eviction rides
  it), re-arms itself, and lets exceptions propagate — a failed flush is a failed flush. It is a
  **mitigation only** for checkpoint-less streaming jobs; the documented guarantee still requires
  checkpointing, and the docs say so in both places that used to point at #54. The issue's
  "connection injection seam" item was **not built**: #15 resolved it with the test-only
  `EmulatorAppenderFactory` through the `@VisibleForTesting createWriter` overload (recorded on
  the issue). The `maybe*` methods were renamed with it: `createTableIfMissing`,
  `reconcileSchemaIfMismatched` (kept returning "ran just now" — its caller switches to the
  schema-wait schedule on exactly that, so forcing its sibling's accumulated-flag shape would
  complicate a call site for symmetry's sake; the differing `@return` tags now carry the
  distinction), `warnIfCommitsAreTooFrequent`
- **Two emulator endpoints on the builder** (#57, groundwork for #287): `emulatorEndpoint` (gRPC,
  the Storage Write API) and `emulatorRestEndpoint` (REST, `BigQueryTableAdmin`). **Two, not one**,
  and that is the deviation from every sibling connector: BigQuery serves its transports on separate
  ports (9050/9060 on the goccy emulator), so a single value could only point half the sink at the
  emulator — silently, since a job whose tables all exist never touches the REST client at all.
  This **reverses the #15/#54 call** that the `@VisibleForTesting createWriter(appenderFactory,
  tableAdmin, metricGroup)` seam was sufficient: the SQL planner builds the sink through the
  production factory and cannot reach a seam, which is a new trigger rather than a re-argued one.
  `FILE_LOADS` **rejects both** in `build()` — it stages to GCS, which no emulator here stands in
  for, so an endpoint would be honored by the metadata half and silently ignored by the half that
  moves the rows. The emulator branch lives in `StreamWriterRowAppenderFactory` and carries the
  three goccy deviations the test-only `EmulatorAppenderFactory` used to (the `.../streams/_default`
  name form plus a `GetWriteStream` priming call, `UNKNOWN` instead of `NOT_FOUND`, and no
  connection pool); that class was **deleted** rather than left beside the production branch, so the
  emulator ITs now measure production code and exactly one copy of the workaround exists. It is
  still a workaround, not a fact about BigQuery: goccy/bigquery-emulator#342 is fixed upstream but
  unreleased (v0.8.1 shipped 2026-06-13, the issue closed the day after), so the branch goes when a
  release carries the fix. The production, no-endpoint path is untouched — it opens no client at
  all, drawing connections from the SDK's JVM-static pool, which is exactly why an endpoint cannot
  be applied to it and the emulator branch has to build its own client per destination.
  `BigQueryEmulatorEndpointITCase` is the one test that goes through the production
  `createWriter(WriterInitContext)`: every other emulator test injects through the seam, so all of
  them would pass with the endpoints reaching no client at all. `gax-grpc` moved from test to
  compile scope with this
- **BigQuery Table API / SQL** (#57, sub-issue #287): the `table` layer is a *mapping* onto
  `BigQuerySink.builder()`, never a second implementation — the Pub/Sub rules apply unchanged (one
  typed `ConfigOption` per setter, `getOptional(...).ifPresent(...)`, no default restated, a
  reflective test holding the two sets equal, enums carrying their DDL spelling in `toString()`).
  What is this module's own:
  **There is no `format` option**, the one deliberate divergence from the Pub/Sub layer's shape: a
  Pub/Sub message has an opaque payload so a `SerializationFormatFactory` decides its bytes, while a
  BigQuery row is structured and the DDL schema *is* the schema. Adding `toString()` to
  `WriteMethod` and `CreateDisposition` changed the builder's three `"(write method is %s)"`
  messages, which now pass `name()` — they name `WriteMethod.FILE_LOADS` in the same sentence, so
  the two spellings must not mix. `LoadJobSpec.toString()` uses Google's
  `JobInfo.CreateDisposition`, not ours, and the deterministic FILE_LOADS job id hashes destination
  and URIs only, so neither is affected.
  **`RowDataSerializer` is `@Internal` in `table.sink`, not a public `sink.serializer.rowdata`
  family member**: promotion is cheap later (nothing is published), and starting internal keeps the
  new Flink-type mapping out of the API-tier audit surface until it has settled. Its schema options
  are the `@Internal` `RowDataSchemaOptions` rather than a reused `AvroSchemaOptions`.
  **Two mapping rows are measured rather than inherited.** `TIMESTAMP` → `DATETIME` and
  `TIMESTAMP_LTZ` → `TIMESTAMP`, the opposite of the Dataproc connector, which stores a wall-clock
  value as an instant and vice versa. And `TIME(p)` is rejected above **p = 3, not the 6 the #57
  design table states**: `RowData` carries a time of day as an `int` of *milliseconds* (its own
  javadoc table, read off flink-table-common 2.2.1), so a `TIME(6)` column could only ever be filled
  to millisecond precision, and a schema claiming more than the values can carry is worse than a
  rejection. Related, also measured: Flink caps `DECIMAL` precision at 38, so **no SQL decimal can
  reach the BIGNUMERIC rejection** — the bound stays in the converter as the invariant it shares
  with the Avro path, and a test pins that nothing reaches it.
  **A marked `ROW` is rendered as JSON text** (`RowDataJsonRenderer`), which the Avro path has no
  counterpart for — its JSON marker is string-only, and only the protobuf path prints a message.
  Decided with the user against matching Avro, the issue text having said STRING/ROW while the
  named template said string-only. The renderer is a plan built from the column's `LogicalType`, so
  an unrenderable nested type (a `MULTISET`, a map with non-string keys) fails at graph construction
  rather than per record; `flink-json`'s `RowDataToJsonConverters` was declined as a dependency — a
  format module on the connector core, plus `@Internal` Flink types needing api-tier entries. A
  marked `STRING` still goes through verbatim and unvalidated, as everywhere else.
  `PARTITIONED BY` is **rejected, not consumed** — no `SupportsPartitioning`, so the clause fails at
  plan time rather than being silently ignored; ingestion-time partitioning has no column to name,
  so the clause could never have covered the whole feature. `perDestinationMetrics` now *does* have
  a `ConfigOption`, which supersedes the parenthetical in the #210 entry above
- **A missing table does not answer `NOT_FOUND`** (measured on #289, 2026-08-06, and the reason that
  issue grew a writer change): opening a Storage Write API stream against a table that is not there
  answers `PERMISSION_DENIED: Permission 'TABLES_GET' denied on resource '<table>' (or it may not
  exist)`. The service masks existence, as an API that must not let an unauthorised caller probe for
  table names has to. The goccy emulator answers `NOT_FOUND` (and `UNKNOWN` on the default stream),
  and `AppendErrorClassifier` recovered on `NOT_FOUND` alone — so **`CREATE_IF_NEEDED` had never once
  created a table against the real service**, while every emulator test said it did. Nothing caught
  it because the gated storage-path suites create their tables up front. The verdict is now
  `AppendErrorClassifier.isMissingTable`, taking both codes, consumed by
  `BigQueryDefaultStreamWriter` (three sites) and `BigQueryBufferedStreamWriter.createStream`.
  Three things not to re-derive. **`isRetriable`'s post-creation clause had to widen too**, and that
  is measured, not symmetry: the propagation window right after this writer creates the table masks
  the same way, naming `TABLES_UPDATE_DATA` — a run that fixed only the first site created the table
  and then failed on the very next append. **Status codes, never the message text**: the "(or it may
  not exist)" wording is the service's prose and nothing pins it, whereas a code cannot quietly stop
  matching — which is exactly how this defect survived. **A failure naming rows is excluded**, since
  the SDK copies the response's code onto a row-detailed exception, so rows plus a code is a verdict
  about the data; that guard does real work for `PERMISSION_DENIED` and none for `NOT_FOUND`, and is
  written about the shape so the two cannot drift. The cost is stated rather than hidden: a job whose
  credentials genuinely lack the permission now attempts one creation before failing — naming
  `bigquery.tables.create`, which tells a reader more than the masked `TABLES_GET` did — and if it
  holds `tables.create` but not the data-write permission it leaves behind the empty table it was
  authorised to create. Two existing tests used `PERMISSION_DENIED` as their unambiguous *terminal*
  example and now use `INVALID_ARGUMENT`; that premise is gone, so do not restore it.
  **The widening needs `scheduleFor`, and that is not tidiness**: `createTableIfMissing`
  is reached from *schema* repairs too, which run on the fifteen-minute `schemaWaitSchedule`. An
  existing table the credentials cannot write to answers the masked code, the creation attempt then
  returns HTTP 409 and is swallowed as success, and `isRetriable`'s post-creation clause is true
  from then on — so without the bound a failure that used to be immediate and well named becomes a
  checkpoint timeout with no cause attached (Flink's default timeout is ten minutes). The bound caps
  a missing-table verdict at the recovery schedule wherever the repair happens to be, at **both**
  `retryBatches` call sites — the `rebuildState` catch and the append loop; fixing only the first
  leaves the defect reachable, which is how it was found. It **also restores** the schema budget
  for a later mismatch, and that half is not symmetry for its own sake: the escalation fires only
  on the reconciliation, which runs once per repair, so a mismatch arriving *after* a missing-table
  verdict would otherwise wait out schema propagation on the one-minute budget and fail a repair
  that was progressing. Deliberately those two failures only — a transient or stale-writer failure
  during a schema repair keeps the long budget, because unlike a possibly-permanent denial those
  really are retriable.
  Two messages had to stop asserting what the masked code cannot establish: the four "does not
  exist, creating it" logs became "may not exist", and `retryFailureMessage`'s "after creating the
  table" became "after a table-creation attempt" — a 409 means the table was already there, so the
  old wording pointed at a creation that never happened and away from the real cause.
  `reconcileSchema`'s own "does not exist" log is **not** in that set: it is driven by a REST
  `getSchema` returning null, which does establish nonexistence.
  **`BigQueryBufferedStreamWriter`'s half is unmeasured**: the gated exactly-once suite pre-creates
  its tables, so whether `CreateWriteStream` masks the same way is inferred from the default-stream
  measurement rather than observed — #318
- **BigQuery Table API table-creation options** (#57, sub-issue #289): the four
  `sink.table-create.*` keys onto `TableCreateOptions` through `TableCreateOptionsMapper`, under the
  mapping rules the #287 entry above states. `TimePartitioningType` gained the `toString()` its
  siblings have; `BigQueryTableAdmin` bridges to the client library with `Type.valueOf(name())`, so
  the constant names stay the contract and only the DDL spelling is new.
  **The mapper owns seven rejections, and one of them has no builder backstop at all**: a
  `time-partitioning.field` without a `.type` is unrepresentable through the builder's two
  `timePartitioning` overloads, so there is no exception to inherit and this check is the only thing
  between a DDL and a silently unpartitioned table. Two more restate a rule the *builder* also has,
  in option keys a SQL user can act on — creation settings beside an explicit `create-never` (the
  Pub/Sub `TopicCreateOptionsMapper` precedent) and an expiration without a granularity. The
  remaining four restate a rule only the *service* has, and the builder could not make any of them
  because it never sees a schema: a column the DDL does not declare, a partitioning column of a
  type BigQuery cannot partition on, `hour` over a `DATE` column, and a repeated or nested
  clustering column.
  **The column check is a check only this layer can make**, and it is why it exists rather than being
  left to BigQuery: in SQL the DDL *is* the created table's schema, while the DataStream API takes
  its schema from the serializer per destination and does not have it when the options are
  configured. Matching is **case-insensitive** — `RowTypeToTableSchemaConverter` already rejects
  columns differing only by case, so it is unambiguous, and it cannot refuse a table BigQuery would
  have created whichever way the service resolves the name; the value reaches the builder unchanged
  rather than being normalised, since rewriting a user's value would be this layer inventing
  behaviour the DataStream API does not have.
  **The line is shape versus type list, and the first draft drew it in the wrong place.** Checked:
  existence; the three types time-unit partitioning is defined over (`DATE`, `TIMESTAMP`,
  `DATETIME` — here Flink's `DATE`, `TIMESTAMP_LTZ`, `TIMESTAMP`); that a `DATE` column has day,
  month and year granularity only; and "top-level, non-repeated" for clustering. None of those can
  grow — they are the shape of the feature. Not checked: the **clusterable scalar type list**,
  which has grown before (`RANGE`), so a copy here would eventually refuse a table BigQuery would
  create — a false rejection being worse than the late true one it prevents. A `DOUBLE` or `TIME`
  clustering column therefore still reaches the service. The declined-because-it-moves argument was
  originally written over all of it, which was too broad: it covers only the scalar list, and using
  it to wave off the structural rules is what left `ARRAY` and `hour`-on-`DATE` unguarded until
  review.
  The reflective option-completeness test keys setter **names** to a *list* of options, unlike its
  two siblings' one-to-one maps: `timePartitioning` is overloaded, so one name carries both the
  granularity and the column. `BigQueryDynamicSinkTest`'s eleven positional arguments were replaced
  by a named-argument holder plus a reflective check that every field of the sink is actually varied
  — the identity test could previously go quiet when a field was added and forgotten
- **BigQuery Table API: the other two write methods** (#57, sub-issue #288): `sink.buffered-stream.*`
  (9 keys) and `sink.file-loads.*` (10) onto `BufferedStreamOptions` / `FileLoadsOptions`, under the
  mapping rules the #287 entry states. `WriteDisposition` gained the `toString()` its sibling enums
  carry, which made `BigQueryFileLoadsSink`'s streaming message mix spellings — that message names
  `WriteDisposition.WRITE_APPEND`, `WRITE_TRUNCATE` and `WRITE_EMPTY` in prose, so the value beside
  them takes `.name()`, the #287 rule applied a second time. `LoadJobOrchestrator` is unaffected: it
  bridges through a `switch`, not through `valueOf(name())`.
  **The two new mappers build unconditionally, and the factory decides whether to call them from the
  write method** — the one place these diverge from `DefaultStreamOptionsMapper`, whose presence scan
  decides. It is not a missing symmetry: `defaultStreamOptions(...)` is *optional* on the builder
  while the other two are *required* for their write methods, so a DDL selecting exactly-once and
  tuning nothing would otherwise be told `bufferedStreamOptions(...) is required` — a method it never
  called and cannot call. Every buffered knob is defaulted, so `builder().build()` is exactly what
  that DDL means; FILE_LOADS needs its staging path, which is why that one rejection lives in
  `FileLoadsOptionsMapper`. `presentKeys` survives on all three for the wrong-family check alone.
  **Two of the four factory rejections are not about families**, and both became reachable from SQL
  for the first time here — before this only at-least-once was: `sink.schema-update.*` under
  exactly-once, and `emulator-*` under FILE_LOADS. The schema-update one fires on the *enabled*
  options object, the same condition the builder uses, so `allow-new-fields = false` passes here
  exactly as it passes there — pinned by a success-side test, the #289 lesson.
  **The FILE_LOADS keys are spelled after the setters** (`sink.file-loads.schema-reconcile.*`), not
  after the `getSchemaUpdate*` getters, which the reflective tests key off too — and which also keeps
  them clear of the unrelated `sink.schema-update.*` family. Both new mappers carry **both**
  reflective halves; the `everyOptionOfTheFamilyFeedsAKnob` prefix scan turned out to be the half
  `DefaultStreamOptionsMapperTest` had never had, so with a fourth caller the scan itself moved to
  `OptionFamilies.declaredKeysUnder` and that test gained the guard — each mapper test keeps its own
  vacuity check and its own assertion, so what a test claims stays where the test is.
  `BigQueryDynamicSink` took a `Builder` here rather than a fourteenth positional argument (decided
  with the user); `BigQueryDynamicSinkTest`'s private `Args` holder collapsed into it. The
  identity test gained two guards **because the builder weakened `copy()`**: a dropped positional
  argument does not compile, a dropped builder call does, and the old
  `aCopyEqualsTheOriginal` copied the *default* sink whose eleven optional fields are all null — so
  a `copy()` that lost one reproduced the default and compared equal (measured: that mutant
  survived). It now copies a **fully specified** sink built from the same `variations()` map, and a
  reflective check proves each entry varies the field it is keyed by, since two entries touching one
  field would leave another at its default and re-open exactly that hole.
  **Neither new write method can be exercised against the emulator**, measured rather than assumed
  (2026-08-06, goccy 0.8.1). FILE_LOADS stages to Cloud Storage that nothing stands in for — the
  factory's own refusal is what the emulator suite asserts instead. Exactly-once was attempted and
  dropped: `CreateWriteStream` answers `UNKNOWN` for a missing table, so `create-if-needed` cannot
  auto-create (#326 — the default-stream path carries that rewrite, the buffered one does not), and
  with the table pre-created the emulator assigns its own append offsets, so
  `BigQueryBufferedStreamWriter`'s consistency check fails on the first append. Both round trips are
  therefore gated: `BigQueryTableExactlyOnceITCase` (a datagen sequence spanning several
  checkpoints, so the second commit is exercised — a bounded `VALUES` insert commits once and proves
  nothing about it) and `BigQueryTableFileLoadsITCase` (streaming plus batch, the latter being the
  only place `write-disposition` has an effect). One measurement worth keeping from the first
  (2026-08-06, one run): at the planner's default parallelism every subtask races to create the same
  table and BigQuery answers *"Exceeded rate limits: too many table update operations for this
  table"* — the recovery schedule absorbs it and the job succeeds, so it is a cost rather than a
  defect, and the test pins `sink.parallelism` to 2 rather than paying it.
  `FileLoadsOptions.toString()` now renders `writeDisposition=write-append`, the visible cost of the
  enum's DDL spelling — log-only, nothing parses it, and the counterpart of the #287 entry's note
  about `StartPosition.toString()`
- **`flink-sql-connector-gcp-bigquery`, the uber-jar** (#57, sub-issue #290): the shading and
  licensing decisions are `docs/adr/0015`'s, inherited wholesale and not
  re-argued here — everything relocated including `grpc-netty-shaded` and its two `META-INF/native`
  renames, `artifactSet` `*:*`, no Google artifact declared at `test` scope, the licence machinery
  through `just update-notice` / `check-notice`. **Read that ADR before changing this module's
  pom.** What is this tree's own:
  **`org.slf4j:slf4j-api` is the one artifact deliberately kept out of the bundle**, and the
  Pub/Sub module has no counterpart because its tree carries no slf4j at all; this one gets it
  through Avro. Bundling it is wrong either way round: relocated, the connector's own
  `LoggerFactory` calls are rewritten with it, so they bind to a copy no Flink log configuration
  reaches and the connector goes silent under a green job; unrelocated, the jar puts a second
  `slf4j-api` on a classpath that already has flink-dist's. It is removed with an `<exclusion>` on
  the connector dependency rather than a shade filter, so the tree the NOTICE, the licence report
  and `BundledDependenciesNoticeTest` all read is the tree that is bundled — one fact, not a fact
  plus an exception list. That is also why the shared NOTICE test needed no
  "excluded from the bundle" hook: `includeScope=runtime` never sees it.
  **The SQL module's runtime tree is not the connector module's** — 111 third-party artifacts
  against 114 — which is worth knowing before reading a relocation list against the wrong
  `dependency:tree`: `commons-lang3` and `commons-io` appear in the connector's and *not* here, and
  `commons-compress` resolves 1.24.0 here against 1.26.0 there, because their only path is
  `flink-core`, which is `provided` on the connector and so contributes nothing transitively.
  `slf4j-api` is the third of the three, excluded here deliberately. The relocation list is derived
  from the SQL module's own `runtime-classpath.txt`, not from the connector's.
  **A relocation pattern rewrites *references*, so it must not be wider than the tree.**
  `org.apache.commons` was the tempting one line for a tree carrying only commons-codec and
  commons-compress, and it silently renamed httpclient's 113 references to
  `org.apache.commons.logging` — an artifact this tree does not have — into a name private to the
  jar, which no user can then satisfy by putting commons-logging in `lib/`. Two named patterns
  instead. The general rule is the reason: derive the relocation list from the module's own
  `runtime-classpath.txt`, not from what a package root looks like it should cover.
  `com.google` is the standing exception, wholesale in both SQL modules, and it pays the same cost
  for `com.google.appengine` — an optional dependency that was already absent.
  **Three SPI files are filtered out because their interface is a JDK type**
  (`javax.xml.stream.XML{Input,Output,Event}Factory`, `java.time.chrono.Chronology`). Only the
  implementation relocates, so the jar would otherwise register relocated Woodstox as the **JVM's**
  StAX provider for everything sharing it — and the deployment this artifact is for is Flink's
  `lib/`, so that is Flink and every job on the TaskManager, quietly changing how unrelated XML
  parses. Nothing here needs them: Woodstox arrives only because google-cloud-storage brings
  jackson-dataformat-xml, which falls back to the JDK factory. An SPI whose interface relocates
  with it (gRPC's providers, Jackson's modules) is unaffected, and the Flink factory SPI is the
  jar's whole point. Found by review, not by a test — no assertion in the packaging suite looks at
  resources.
  **`org.apache.avro` is relocated, which makes the uber-jar's `AvroRecordSerializer` unusable from
  a DataStream job** — its signature there takes a relocated `IndexedRecord`. Accepted rather than
  exempted: it is the same trade the Pub/Sub jar already makes with `PubsubMessage`, which its own
  SPI returns, and leaving Avro alone would put a second copy beside whatever `flink-avro` a SQL
  deployment carries. Both READMEs and the docs page point a DataStream user at the plain connector
  jar, which is the actual answer.
  **Four `META-INF` paths are excluded from both SQL modules, and `META-INF/LICENSE` deliberately
  is not.** `META-INF/native-image/**` and `META-INF/proguard/**` are build-tool inputs naming
  unrelocated classes — GraalVM and R8 read them, a Flink deployment reads neither, and no bundled
  class reads them at runtime (grepped, not assumed); reinstating them is what native-image support
  would cost, and it would have to relocate them. `META-INF/services/javax.xml.stream.*` and
  `java.time.chrono.Chronology` are the JDK-interface SPI files above. `META-INF/LICENSE` was
  excluded with them and **reverted**: shade takes the project jar first, so the copy that survives
  is this project's own — measured byte-identical to the repository root `LICENSE` — and dropping it
  left the two jars a user downloads directly as the only artifacts here carrying no licence, while
  two lines of the packaged NOTICE went on pointing at an "accompanying LICENSE file".
  `theProjectsOwnLicenceIsInTheJar` is what holds it now.
  **`META-INF/versions/**` is excluded from both SQL modules.** maven-shade relocates a versioned
  class's *contents* and leaves it at its original path, so jackson-core's Java 11/17/21/22
  variants shipped spelled `com/fasterxml/...` in a jar whose base copies had moved — caught by the
  packaging test, not predicted. They are dead weight either way, since an uber-jar's manifest
  carries no `Multi-Release: true` and the JVM never looks there; the Pub/Sub module takes the same
  exclusion (costing it one inert OSGi metadata file) so the two poms cannot answer one question
  two ways.
  **Arrow, netty and flatbuffers are bundled for a code path this connector never runs** — the
  Storage *Read* API in `google-cloud-bigquerystorage`. Excluding them was weighed and declined: it
  reintroduces the enumerated include list #138 removed after measuring that an unlisted transitive
  is silently *dropped* from the jar, and #64 would need them back. Priced before declining, per
  the usual rule: 3.2 MB of the 58 MB of compressed entries, about 5% (measured 2026-08-06;
  the docs page divides the same 3.2 MB by the 64 MB the file weighs on disk) — a third of what the
  first estimate assumed.
  The smoke ITCase lets the sink **create its own table**, so one job drives both relocated
  transports — REST for the metadata half, gRPC for the rows — which is the shape only this
  connector needs, since only it has two `emulator-*` options.
  This module is also what discharged the #26 trigger: the packaging/NOTICE/`ShadedJar` trio moved
  to `flink-connector-gcp-test-utils` (see that module's CLAUDE.md for the parameterisation)
  instead of being copied
- Deferred decisions are recorded on PR #46: `location()` granularity (decide in #10)
- **BigQuery JSON serializer** (#66, JSON half — closes the issue): `JsonDocumentSerializer` takes
  **`String`** records and a **supplied** schema, since JSON has none of its own — either the
  Storage `TableSchema` or the REST `Schema` (converted with the existing `BigQuerySchemaConverter`,
  which is why `google-cloud-bigquery` is now on the module's public API). That is also why it needs
  no JSON-column marker: the schema already says `JSON`, unlike the proto and Avro paths where the
  column type has to be inferred. Conversion is the client library's own `JsonToProtoMessage` — the
  one `JsonStreamWriter` uses — so there is deliberately **no row converter class here**; wrapping a
  single call in one would add a layer without adding anything. Named for its input like its
  siblings, and *not* `JsonSerializer`: that simple name collides with Jackson's and Gson's, in
  exactly the pipelines that produce JSON text. Decisions and traps not to re-derive:
  `new JSONObject(String)` **stops at the end of the first value and ignores the rest**, so a
  mis-split newline-delimited record would silently become one row and drop the remainder —
  `serialize` parses through a `JSONTokener` and rejects trailing content instead. The library
  reports every per-row problem as an **unchecked** exception (`RowIndexToErrorException`, whose
  message is a map keyed by row index; it is package-private, so it cannot be named in a catch
  clause), and a bare `IllegalStateException("JSONObject is empty.")` for `{}` — which is
  pre-empted, not caught, so the message can say which record it was. A **`BYTES` column takes a
  JSON array of byte values, never base64** (#131), and a **`JSON` column takes the JSON *text* as a
  string, never a nested object** — both contradict what a JSON document usually carries, and both
  are pinned by tests so they read as known limitations. **The `BYTES` half is pursued upstream, not
  here**: a local base64 pre-pass was designed and declined, because walking the schema for `BYTES`
  paths would make this connector own a piece of the JSON→proto mapping it otherwise delegates
  whole — and would shadow the library once it decodes base64 itself. Reported as
  googleapis/google-cloud-java#13980, with a fix proposed in googleapis/google-cloud-java#13981 that
  also covers googleapis/google-cloud-java#13979, the scalar-`byte[]` asymmetry found alongside it.
  Two things measured while writing that patch, worth not re-deriving: the fix belongs in
  `fillField`/`fillRepeatedField`, where the library's own recursion already handles the nested and
  repeated paths a pre-pass here would have had to walk itself — including matching keys
  case-insensitively, as the library does; and it must be guarded on the `TableSchema` saying
  `BYTES`, since proto `BYTES` also carries `NUMERIC`/`BIGNUMERIC` and an unguarded decode turns a
  `NUMERIC` error into a silently wrong value. Whatever upstream decides,
  `bytesColumnsTakeAJsonArrayOfByteValuesAndNotBase64` is what fails when a `libraries-bom` bump
  changes the behaviour, and that is the signal to revisit. A **bare number in a `TIMESTAMP` column
  is epoch microseconds**, so epoch-seconds and epoch-millis documents are accepted and stored as some
  other instant; pinned too, since nothing can detect it. Keys match columns **case-insensitively**,
  so a differently-spelled key is not an "unknown field". `ignoreUnknownFields` is the one option
  (default strict). `org.json:json` is declared explicitly with a version property because it is
  used directly; note that entry *overrides* bigquerystorage's own transitive version rather than
  following it, and our `dependency:tree` cannot reveal the drift. Two things not to simplify away:
  the descriptor is derived **in the constructor** for the reason the Avro entry above gives, and
  `descriptor()` is called *outside* `serialize`'s `catch (RuntimeException)` so that a schema
  problem is not reported as a bad record — on a task manager the constructor never runs, and every
  writer calls `serialize` before `getDescriptor`, so that is where the first build happens there.
  An empty schema is rejected outright, since a table with no columns is a misconfiguration rather
  than a degenerate case worth supporting
- **Sink metrics** (#210, the #37 series' last metrics sub-issue): three writer metrics classes over
  the shared `base.metrics` helpers — `DefaultStreamWriterMetrics`, `BufferedStreamWriterMetrics`
  (both `sink.storage.writer`) and `FileLoadsWriterMetrics` (`sink.fileloads.writer`) — plus one
  committer counter. Three classes rather than one conditionally-registering class, so **no writer
  registers a metric it can never increment**: the buffered path has one fixed destination with a
  schema pinned at stream creation, so `openDestinations`, `tablesCreated`, `schemaReconciliations`
  and the per-destination pair would all be constants there, and FILE_LOADS makes no per-record
  request, so it has no error-class dimension at all. What not to re-litigate:
  **`numRecordsSend` is counted where the batch is first handed to the client**, which is
  `appendPending` on the default-stream path (the repair path re-appends from `retryBatches`, a
  different call site, so no flag is needed) and a `firstAttempt` parameter on `syncAppend` for the
  buffered path (whose probe, resend and replay all share that one call). Both count *after* the
  client call returns, so a synchronous rejection — which registers no future and reaches BigQuery
  not at all — is not reported as sent. The repo-wide rule and its cost are in the base module's
  CLAUDE.md.
  **`errorClass.CODE.errors` counts every failed append the task thread classifies**, not just the
  first of a repair episode (the issue text named only `handleFailedAppend`; widened with the user,
  2026-08-03): `collectFailedSiblings`, the `retryBatches` failure branch, and the buffered path's
  drain/resend/replay/probe sites count too, which makes the sum over the transient codes the retry
  volume — the same claim the Cloud Tasks page already makes, so one dashboard reads both. **Nothing
  is ever counted from a gRPC callback thread** (the counters are plain), which is why the one
  failure a callback owns outright — a terminal one, removed from `inFlight` by `park()` — is
  counted in `checkAsyncError()` instead, behind an `asyncErrorCounted` flag because that method
  runs on every write and flush while the task is torn down. Two failures are deliberately
  **uncounted**: `OFFSET_ALREADY_EXISTS` outside a replay is a success (the original append landed),
  and the appends stranded behind a rejected offset in `recoverRowLevel` are cascades of a failure
  that is itself counted — the Pub/Sub sink's cascade-cancellation rule, applied to the shape this
  writer has. The gax code comes from a new `AppendErrorClassifier.statusCode`, which mirrors
  `PubSubErrorClassifier.statusCode` and leaves the classifier's own `io.grpc.Status.Code` routing
  untouched (#61's do-not-converge decision).
  **Every gauge's backing collection is cleared in `close()`** — `inFlight` on both storage writers
  and `destinations` on FILE_LOADS, beside the `states.clear()` the default writer already did. A
  reporter can sample a gauge between the writer's teardown and its metric group's, and on the
  failure path those collections are never drained, so without this a dead writer goes on reporting
  appends nobody will wait for; `PubSubWriter.close()` zeroing its parked count is the precedent.
  Safe because nothing re-adds an entry afterwards — the completion callbacks only remove.
  **`appendRetries` counts re-issued appends, `tablesCreated` counts creations and
  `schemaReconciliations` counts applied schema updates only** — `reconcileSchema`'s
  table-had-vanished branch is a creation and is counted as one, not as a reconciliation.
  `perDestinationMetrics` is on `DefaultStreamOptions` and `FileLoadsOptions` (default false, no
  `ConfigOption`: this module has no Table API layer yet, #57), and its handle is **looked up per
  batch rather than cached on `DestinationState`**, unlike the Pub/Sub sink's: this writer counts
  per batch, not per record, and its state is rebuilt by every repair, so caching would buy one map
  read per append at the cost of threading the handle through the rebuild path.
  `DestinationMetrics.Counters.recordsSent(long)` was added to the base helper for the same reason —
  a batching connector counts n records in one call.
  **The FILE_LOADS committer's `loadJobsSubmitted` counts load jobs only**, not the overflow path's
  copy job (a different quota, and the name is the contract), and is threaded into
  `LoadJobOrchestrator` as a `Counter` because that type is constructed per commit while the metric
  is registered once per committer. It is the whole job's rate, not a subtask's: `prepared.global()`
  means one committer subtask. The framework's own committer metrics are **documented, not built**,
  under the names a reporter sees (`totalCommittables` &c. — see the test-utils CLAUDE.md for why
  those are not the accessor names the issue text used).
  Coverage is one `*MetricsTest` per writer, asserting **by registered name** through
  `TestSinkWriterMetricGroup`; the buffered and FILE_LOADS ones ride their behavioural tests' fakes,
  while the default-stream one carries its own because the cases it needs are spread across three
  test classes whose fixtures are private
