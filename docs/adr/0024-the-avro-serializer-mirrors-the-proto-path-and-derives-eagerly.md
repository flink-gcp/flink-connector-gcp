<!--
Copyright 2026 laughingman7743

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
- Date: 2026-07-26
- Issues: [#66] (Avro half)
- Modules: bigquery (`sink.serializer.avro`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Avro records

## Decision

`AvroRecordSerializer` is `ProtoMessageSerializer`'s shape with an Avro front end — the schema
is held as its **JSON text** (serializable, unlike a parsed `Schema`) and the
`TableSchema`/descriptor/row-converter triple is rebuilt lazily. It accepts **`IndexedRecord`**,
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
  the schema is derived in `AvroRecordSerializer.of(...)` rather than lazily: the lazy path
  first runs from `serialize()`, inside the writers' `FailureHandler` catch, where one
  misconfiguration would look like a poison record and a log-and-drop policy would swallow the
  whole stream.
- A `["null", array]` field is `REPEATED`, so a null array and an empty one are
  indistinguishable — BigQuery offers no way to keep them apart, and the alternative is
  rejecting the schema.

## Evidence

Two things caught in self-review and worth not re-deriving:

- BigQuery bounds a parameterized decimal by its **integer** digits (`NUMERIC(P,S)` needs
  `S ≤ 9` and `P - S ≤ 29`, `BIGNUMERIC` `S ≤ 38` and `P - S ≤ 38`), not by total precision, so
  `decimal(35,2)` is BIGNUMERIC and `decimal(77,38)` is rejected.
- `AvroRowConverter` pairs schema fields to descriptor fields **by position**, because
  `BQTableSchemaToProtoDescriptor` lowercases with the *default* locale — under `tr_TR` a column
  named `ID` becomes the proto field `ıd`, which no `Locale.ROOT` key matches. Position is exact
  here precisely because the descriptor is always derived from the table schema this connector
  just produced.

[#66]: https://github.com/laughingman7743/flink-connector-gcp/issues/66
