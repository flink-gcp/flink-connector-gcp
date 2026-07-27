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
  included — was reported from `serialize()`, inside the writers' `FailedRowHandler` catch, where
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
  record, inside the writers' `FailedRowHandler` catch. Same rule as the Avro and #147 entries.
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
  `FailedRowHandler` catch, where log-and-drop would swallow the stream. Measured, and it is the
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
  hence the fixture's `SingularWellKnownTypes` for the emulator write test and
  `BigQueryProtoRepeatedJsonITCase`, gated on `BQ_IT_PROJECT`, for the repeated half. This is a
  **breaking schema change** for any existing table (`STRUCT` → scalar): `SchemaUnifier` rejects the
  union rather than corrupting rows
- **BigQuery protobuf nullability** (#124 Part 1, with Part 3's `oneof` pin; Part 2 is the entry
  above): `ProtoToTableSchemaConverter` derives the mode from presence only under
  `ProtoSchemaOptions.Builder.deriveRequiredColumns()`, and the default stays **`NULLABLE`**.
  Reasons, in order of weight: proto3's presence-less form is the spelling you get by *not* thinking
  about nullability, so deriving `REQUIRED` from it by default would make nearly every scalar column
  of an auto-created table `REQUIRED` on the strength of a syntax default; and `REQUIRED` is the mode
  BigQuery cannot walk back. **This mapping is normative for every serializer** — every write path
  ends in a protobuf row (`STORAGE_API_*` directly; the Avro and JSON serializers via
  `BQTableSchemaToProtoDescriptor`; FILE_LOADS stages Avro only incidentally, and could stage
  Parquet) — so **#145 moved Avro onto this default and this method name**, rather than the reverse,
  and both serializers now take `deriveRequiredColumns()` with only the signal differing (a
  `["null", T]` union there, presence here). **Neither default is to be flipped per format again**:
  that is the whole point of the two agreeing.
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
  are pinned by tests so they read as known limitations. A **bare number in a `TIMESTAMP` column is
  epoch microseconds**, so epoch-seconds and epoch-millis documents are accepted and stored as some
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
