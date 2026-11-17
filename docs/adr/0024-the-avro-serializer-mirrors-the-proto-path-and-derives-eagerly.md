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
- Date: 2026-07-26; revised by [#2071](https://github.com/flink-gcp/flink-connector-gcp/issues/2071) (2026-11-07: older-serialized-form fallback removed, ADR-0200) and [#2179] (2026-11-17: the protobuf converter pairs by position)
- Issues: [#66] (Avro half); the shared holder extracted by [#828] (2026-08-18); the
  compatibility record corrected by [#998] (2026-08-22), [#2071](https://github.com/flink-gcp/flink-connector-gcp/issues/2071); the
  protobuf converter's field pairing revised by [#2179] (2026-11-17)
- Modules: bigquery (`sink.serializer.avro`; `sink.serializer.proto` since [#2179])
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Avro records and
  § Protobuf messages

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
- `AvroRowConverter` pairs schema fields to descriptor fields **by position**, because
  `BQTableSchemaToProtoDescriptor` lowercases with the *default* locale — under `tr_TR` a column
  named `ID` becomes the proto field `ıd`, which no `Locale.ROOT` key matches. Position is exact
  here precisely because the descriptor is always derived from the table schema this connector
  just produced.

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
  table rather than routing it to the failure handler. That predates this revision.

Verified against real BigQuery by `BigQueryProtoMixedCaseFieldNamesRealGcpITCase`. A table created
beforehand with the columns `id`, `hasNext`, `beforeUpdate STRUCT<innerValue, Count>`, `Items`,
`countsByName` and `userId` took a load job and a Storage Write API append. Each value read back
from the column of its field's name, and the table's schema was unchanged. A table whose columns
are the same names all lower-cased (`hasnext`, `beforeupdate.innervalue`, `items`) took both write
methods the same way, and kept its lower-case spelling. The two load jobs used the default
`WRITE_APPEND`; under `WRITE_TRUNCATE` a load replaces the schema with the serializer's anyway.

[#66]: https://github.com/flink-gcp/flink-connector-gcp/issues/66
[#828]: https://github.com/flink-gcp/flink-connector-gcp/issues/828
[#998]: https://github.com/flink-gcp/flink-connector-gcp/issues/998
[#2179]: https://github.com/flink-gcp/flink-connector-gcp/issues/2179
