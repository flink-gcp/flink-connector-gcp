# CLAUDE.md — flink-connector-gcp-bigquery

Module-scoped guidance, loaded when Claude works in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `CLAUDE.md`.

## Design decisions (do not silently revisit)

- **BigQuery**: `BigQueryIO`-style facade — one builder, per-write-method SinkV2 implementations.
  Storage Write API connection multiplexing is delegated to the client SDK connection pool
  (`setEnableConnectionPool`); no self-built keyed writer pool. The serializer SPI is an abstract
  class (`BigQueryProtoSerializer`) with `getDescriptor(TableDestination)` + `ByteString`
  rows — not a functional interface (descriptors are not Java-serializable)
- **BigQuery error handling** (#13): a single `FailedRowHandler` SPI covers all row-level
  failure policies — fail-job (default), log-and-drop, and DLQ routing (the `DeadLetterQueue`
  interface is an experimental stub; lifecycle and shared-module extraction are decided in #37).
  `FailedRow` carries serialized protobuf bytes, not the original record (the writer is
  stateless). SDK in-stream retry settings are hardcoded in `StreamWriterRowAppenderFactory`;
  exposing them is deferred until a real-world need shows which knobs matter
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
  invisible and cost nothing. Server-side row-level errors route to `FailedRowHandler` with
  offset-recompute recovery (atomic request rejection → route failing rows, replay survivors +
  trailing batches; `ALREADY_EXISTS` during an offset-shifting replay is terminal). v1 scope:
  fixed destination only (builder rejects `destinationResolver`), no mid-stream schema
  evolution (stream schema pinned at creation), BATCH supported (commit at end of input),
  streaming requires EXACTLY_ONCE + checkpoints-after-tasks-finish; retry knobs are
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
  malformed JSON is a BigQuery row-level error, routed to `FailedRowHandler`). #50's issue text
  says message-only; that was widened in the implementing PR because the corpus the feature exists
  to migrate annotates **string** fields, so option selection alone would have delivered nothing.
  `isJsonField(field, path)` is the single decision point both converters consult. Consequences not
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
  staged files instead of failing a build. Decisions not to re-litigate: a non-union field is
  **`REQUIRED`** (the faithful inverse), with `AvroSchemaOptions.allFieldsNullable()` as the
  escape hatch — it touches **schema derivation only**, leaves `REPEATED` alone (a BigQuery
  `REPEATED` column cannot be `NULLABLE`) and recurses into nested structs; Avro `map<string,V>` →
  `REPEATED STRUCT<key,value>` rather than rejected as the Dataproc connector does, because the
  proto path already gives proto maps that shape; JSON columns are marked by **dotted path only**
  (Avro has no standard JSON logical type to key off, so `ProtoSchemaOptions`' field-option
  mechanism has no analogue); and the logical types BigQuery cannot store faithfully
  (`timestamp-nanos`, `local-timestamp-nanos`, `duration`, `big-decimal`, `uuid` on a `fixed`) are
  **rejected at job start** rather than silently falling back to the base type — literally at job
  start, because the schema is derived in `AvroRecordSerializer.of(...)` rather than lazily: the
  lazy path first runs from `serialize()`, inside the writers' `FailedRowHandler` catch, where one
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
  schema this connector just produced. The protobuf analogue of the
  nullability work (deriving `REQUIRED` from proto3 `optional` presence, plus a matching
  `ProtoSchemaOptions` switch) is deliberately a separate issue: `ProtoToTableSchemaConverter`
  emits `NULLABLE` unconditionally today
- Deferred decisions are recorded on PR #46: `location()` granularity (decide in #10)
