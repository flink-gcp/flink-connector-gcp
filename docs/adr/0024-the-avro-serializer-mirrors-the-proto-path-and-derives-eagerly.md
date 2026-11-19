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

# ADR-0024: The Avro serializer accepts `IndexedRecord`, derives eagerly, and rejects unstorable logical types

- Status: Accepted
- Date: 2026-07-26; revised by [#2071](https://github.com/flink-gcp/flink-connector-gcp/issues/2071) (2026-11-07: older-serialized-form fallback removed, ADR-0200), [#2179] (2026-11-17: the protobuf converter pairs by position) and [#2188] (2026-11-18: row descriptor names do not depend on the default locale; nested message types that would share a name are refused, [#2192])
- Issues: [#66] (Avro half); the shared holder extracted by [#828] (2026-08-18); the
  compatibility record corrected by [#998] (2026-08-22), [#2071](https://github.com/flink-gcp/flink-connector-gcp/issues/2071); the
  protobuf converter's field pairing revised by [#2179] (2026-11-17); the row descriptor's names
  made locale-independent by [#2188] (2026-11-18), which also refuses shared nested type names
  ([#2192])
- Modules: bigquery (`sink.serializer.avro`; `sink.serializer.proto` since [#2179]; `sink.serializer`
  since [#2188])
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Avro records,
  § Protobuf messages and § Column names

## Decision

`AvroRecordSerializationSchema` is `ProtoMessageSerializationSchema`'s shape with an Avro front end — the schema
is held as its **JSON text**, and the `TableSchema`/descriptor/row-converter triple is rebuilt
lazily. A parsed Avro `Schema` is also serializable, so the JSON field is not required by Flink's
job-graph serialization. It accepts **`IndexedRecord`**,
not `GenericRecord`, so `SpecificRecord` streams work; consequently each temporal and decimal
conversion accepts **both** the raw Avro value and the converted one (`Instant`, `LocalDate`,
`LocalTime`, `LocalDateTime`, `BigDecimal`, `UUID`), because a generated class with Avro's
conversions enabled carries the latter and assuming the former would be a per-row
`ClassCastException`.

- `AvroToTableSchemaConverter` is the inverse of the FILE_LOADS `TableSchemaToAvroConverter`,
  which is why `AvroSchemaRoundTripTest` pins the two against each other: an Avro serializer
  feeding FILE_LOADS goes Avro → `TableSchema` → Avro, so drift corrupts staged files instead of
  failing a build. The round-trip **identity** holds only under `deriveRequiredColumns()`, since
  `REQUIRED` is the only mode `TableSchemaToAvroConverter` maps back to a bare type; the
  default's `["null", T]` shape is pinned separately, a weaker guard, so the *values* on the
  union path are covered by `ProtoToAvroConverterTest` instead.
- Nullability is **`NULLABLE` by default with `AvroSchemaOptions.deriveRequiredColumns()` as the
  opt-in** (ADR-0026 has the shared reasoning); it touches schema derivation only, leaves
  `REPEATED` alone (a BigQuery `REPEATED` column cannot be `NULLABLE`) and recurses into nested
  structs and map entry columns.
- Avro `map<string,V>` → `REPEATED STRUCT<key,value>` rather than rejected as the Dataproc
  connector does, because the proto path already gives proto maps that shape.
- JSON and geography columns are marked by **dotted path only** (Avro has no field-option
  mechanism — a separate fact from Avro having no JSON logical type, which is why a marker is
  needed at all).
- The logical types BigQuery cannot store faithfully (`timestamp-nanos`,
  `local-timestamp-nanos`, `duration`, `big-decimal`, `uuid` on a `fixed`) are **rejected at job
  start** rather than silently falling back to the base type — literally at job start, because
  the schema is derived in `AvroRecordSerializationSchema.of(...)` rather than lazily: the lazy path
  first runs from `serialize()`, inside the writers' `FailureHandler` catch, where one
  misconfiguration would look like a poison record and a log-and-drop policy would swallow the
  whole stream.
- A `["null", array]` field is `REPEATED`, so a null array and an empty one are
  indistinguishable — BigQuery offers no way to keep them apart, and the alternative is
  rejecting the schema.
- The mirroring is a shared mechanism rather than a copied one (refined by [#828], 2026-08-18).
  `LazyDerivedState` holds what a serializer derives from its serializable fields and rebuilds it
  on the task manager, and `RowDescriptors.derive` wraps the checked validation failure
  `BQTableSchemaToProtoDescriptor` can raise. Both are `@Internal` in `sink.serializer`, the
  nearest package every caller can import — the format packages must not import each other
  (ADR-0055) and `RowDataSerializationSchema` is in `table.sink` besides. Neither is BigQuery-specific
  enough to belong there on its own, so a second module needing one is a reason to move it to
  `flink-connector-gcp-base`, not to copy it. `JsonDocumentSerializationSchema` and `RowDataSerializationSchema`
  hold their state through the same two. Each serializer keeps its own derivation, called from its
  own constructor, and its own private conversion-state class: the triple is where the formats
  differ, and the JSON serializer has no row converter to put in one (ADR-0025). The serialized
  form of the four changed — a `transient` field became a non-transient holder whose contents are
  transient — at unchanged `serialVersionUID`.
  A job graph written by an earlier build restores with nothing where the holder belongs and fails
  on first use.
  The reverse direction restores: Java deserialization ignores the holder field because the older
  local class has no serializable field by that name, and the older serializer rebuilds its own
  transient conversion state.
  No supported upgrade path reads a job graph written by another connector version (ADR-0200), so
  the old-to-new incompatibility needs no upgrade path.

## Evidence

Evidence caught in self-review and worth not re-deriving:

- The serialized-form directions above were measured on 2026-08-22 with the actual
  `JsonDocumentSerializer` (now `JsonDocumentSerializationSchema`) compiled at `bd7a76f5`
  (immediately before [#828]) and `113981fd`.
  Reading the old stream with the new class reached the first descriptor use and threw because
  `rowDescriptor` was null; reading the new stream with the old class restored and serialized a
  row successfully.
- BigQuery bounds a parameterized decimal by its **integer** digits (`NUMERIC(P,S)` needs
  `S ≤ 9` and `P - S ≤ 29`, `BIGNUMERIC` `S ≤ 38` and `P - S ≤ 38`), not by total precision, so
  `decimal(35,2)` is BIGNUMERIC and `decimal(77,38)` is rejected.
- `AvroRowConverter` pairs schema fields to descriptor fields **by position**: the descriptor's
  names are lower-cased, so a name lookup would have to repeat how, and position is exact precisely
  because the descriptor is always derived from the table schema this connector just produced. The
  first reason recorded here was the library's default-locale lower-casing (`ID` becoming `ıd`
  under `tr_TR`); the revision for #2188 below removed that from every derived descriptor.

## Revision for #2179: the protobuf converter pairs by position too

`ProtoRowConverter` paired each row descriptor field with its source field by exact name. The row
descriptor's field names are lower-cased, so a message with an upper-case letter in any field name,
such as `hasNext`, failed `ProtoMessageSerializationSchema`'s constructor. Every write method
failed with it, while the job graph was built.

The converter now pairs by position, as `AvroRowConverter` and `RowDataToProtoConverter` already
did. Position is exact for the same reason: the row descriptor is always derived from the table
schema `ProtoToTableSchemaConverter` produced from the same message, one column per field in
declaration order. A field count check and a name check guard the pairing. The name check uses
`String.equalsIgnoreCase`, which folds each character without a locale, so the dotless `ı` of a
`tr_TR` lower-casing still equals the `I` it came from.

- **The lookup the issue proposed was declined.** It keyed the source fields by their names
  lower-cased under `Locale.ROOT`, as `ProtoToAvroConverter` does. That finds `hasNext` under an
  English default locale. Under `tr_TR`, the client library gives the row field for `userId` the
  column name `userıd` instead, which no `Locale.ROOT` key matches; `ProtoRowConverterTest` asserts
  that premise before it converts.
- **The columns keep the field names as written.** The lower-cased row descriptor still writes the
  column `hasNext`, because BigQuery matches a column name case-insensitively.
- **`ProtoToAvroConverter` still pairs by `Locale.ROOT` name**, because FILE_LOADS reads any
  serializer's own descriptor and cannot assume its order. Under `tr_TR` it finds no field for a
  column with an upper-case `I` whenever the client library derived the serializer's descriptor,
  as it does for every built-in serializer, and the writer throws on the first record for that
  table rather than routing it to the failure handler. That predates this revision, and the
  revision for #2188 below removes it.

Verified against real BigQuery by `BigQueryProtoMixedCaseFieldNamesRealGcpITCase`. A table created
beforehand with the columns `id`, `hasNext`, `beforeUpdate STRUCT<innerValue, Count>`, `Items`,
`countsByName` and `userId` took a load job and a Storage Write API append. Each value read back
from the column of its field's name, and the table's schema was unchanged. A table whose columns
are the same names all lower-cased (`hasnext`, `beforeupdate.innervalue`, `items`) took both write
methods the same way, and kept its lower-case spelling. The two load jobs used the default
`WRITE_APPEND`; under `WRITE_TRUNCATE` a load replaces the schema with the serializer's anyway.

## Revision for #2188: row descriptor names do not depend on the default locale

`BQTableSchemaToProtoDescriptor` lower-cases each column name with the JVM's default locale. Under
`tr_TR` the column `userId` becomes `userıd`; that is not a protobuf-compatible name, so the library
names the field with a `col_…` placeholder and records `userıd` as its column name. Measured against
BigQuery under a Turkish default locale, with the code as #2179 left it, both write methods failed
for every column holding an upper-case `I`, for the protobuf and JSON serializers alike:

- The Storage Write API rejected the append with `INVALID_ARGUMENT: Input schema has more fields
  than BigQuery schema, extra fields: 'ıtems,userıd'`. The sink's message for that rejection advises
  enabling schema updates, which cannot help here.
- A load failed before staging anything, because `ProtoToAvroConverter` keys the descriptor by
  `Locale.ROOT` names and found no field for the column `Items`.

`RowDescriptors.derive` now hands the library every column name already lower-cased under
`Locale.ROOT`, at every depth. Lower-casing a lower-case name changes nothing in any locale, so the
descriptor is the same under every default locale, and a client and a task manager whose locales
differ derive the same one. Every serializer's row descriptor comes from this one method, so all
four serializers are covered.

- **Under an English locale only nested message type names change.** The fields are the ones the
  library derives itself, as `RowDescriptorsTest` asserts; a nested type is now named after the
  lower-cased column (`root__items` rather than `root__Items`). A row is encoded by field number,
  and none of the checkpoint serializers stores a descriptor. Round one's version lower-cased only
  schemas holding a name that differs, so that every other schema kept the library's own
  descriptor. The repair review found that this made the descriptor, and so the check below,
  depend on the default locale: a client under English accepted `a__B` beside `a.b` that a task
  manager under Turkish refused, inside the per-record failure handling. Lower-casing every schema
  removed that.
- **Two columns whose nested types would share a name with different fields are refused**
  ([#2192], closed by this change), for every write method. The library names a nested type after
  its column's path joined with `__`, so `a__b` beside `a.b` get one name, and so do `a__B` beside
  `a.b` once the names are lower-cased. Two types with one name do more than confuse the Storage
  Write API's writer schema, which keeps one type per name: measured on `d12527e7` (2026-11-18),
  appending through it swapped the values of `a.b` silently. They also make the library resolve a
  field to either type, so the descriptor itself gave `a.b` the other column's fields in 91 of 200
  derivations, and the positional converters and FILE_LOADS read that same descriptor. `derive`
  therefore collects every message type the library built, from the root file's dependencies
  transitively, and refuses two with one name and different fields, naming both columns. Types
  with identical fields are harmless whichever one a field resolves to, so they are accepted;
  that covers the type the library shares for identical column lists, and columns differing only
  in a description, a precision or the case of a name. The check's first version had two faults
  that the repair review found. It compared only the types the fields resolve to, which missed a
  collision whenever both fields resolved to the same type. It also compared instances rather than
  fields, which refused harmless pairs.
- **Position pairing stays.** The names the converters see no longer vary with the locale, but
  they are still lower-cased, so an exact lookup misses `hasNext`. Position does not depend on how
  the names were lower-cased.
- **Two columns differing only by case now collide under Turkish too.** Before, `ID` and `id`
  became `ıd` and `id` there and did not collide; no BigQuery table holds both.
- **The JSON serializer needed more than the descriptor**, because the client library matches its
  keys by name. ADR-0025's revision for #2188 records that half.
- **A serializer that caches its own descriptor** should cache what
  `BigQueryProtoSerializationSchema.getDescriptor` returns, as its Javadoc now says. Calling the
  library directly would bring the default-locale names back.

The library's own lower-casing is reported upstream as
[googleapis/google-cloud-java#15056](https://github.com/googleapis/google-cloud-java/issues/15056)
and tracked by [#2191].

Verified against BigQuery by `BigQueryTurkishLocaleRealGcpITCase`, which sets a Turkish default
locale for the whole class. With this change, the protobuf and JSON serializers, the latter asked to
normalize keys for the default locale (ADR-0025), each wrote a pre-created table through a load job and a Storage Write API append, every value read back from its
column, and the table's whole field list was unchanged (4/4). The same class failed all four cases
on the code before this change, with the two errors above.

[#66]: https://github.com/flink-gcp/flink-connector-gcp/issues/66
[#828]: https://github.com/flink-gcp/flink-connector-gcp/issues/828
[#998]: https://github.com/flink-gcp/flink-connector-gcp/issues/998
[#2179]: https://github.com/flink-gcp/flink-connector-gcp/issues/2179
[#2188]: https://github.com/flink-gcp/flink-connector-gcp/issues/2188
[#2191]: https://github.com/flink-gcp/flink-connector-gcp/issues/2191
[#2192]: https://github.com/flink-gcp/flink-connector-gcp/issues/2192
