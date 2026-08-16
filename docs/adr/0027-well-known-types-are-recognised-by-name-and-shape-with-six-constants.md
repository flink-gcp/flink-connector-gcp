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

# ADR-0027: Protobuf well-known types are recognised by name *and* shape, with six constants

- Status: Accepted
- Date: 2026-07-27
- Issues: [#147] (which is [#124] Part 2)
- Modules: bigquery (`sink.serializer.proto`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Protobuf messages

## Decision

The vocabulary is **protobuf's, not this project's** — *well-known types* names the messages in
`google/protobuf/*.proto`, and the enum's grouping is Google's own (the [#121] spell-it-the-
vendor's-way rule; the javadoc says it is the vendor's word so nobody "improves" it into a local
coinage). Recognition lives in a package-private `ProtoWellKnownType` enum keyed on **full
name** — a descriptor built from a serialized `FileDescriptorSet` carries its own copy of
`wrappers.proto`, so identity comparison would miss every one.

- **The name is necessary but not sufficient**: `of()` also checks the message really has the
  sub-fields the conversions read (`seconds`+`nanos`, `paths`, `value`), and answers `NONE` when
  it does not, so the message expands as the ordinary `STRUCT` its author declared. Nothing
  reserves the `google.protobuf` package — `package google.protobuf; message Duration { int64
  millis = 1; }` is legal — and on the name alone that derived an `INT64` column and then threw
  a field-less `NullPointerException` on **every record**, from inside the writers'
  `FailureHandler` catch, where log-and-drop would swallow the stream. Measured, and it is the
  same rule ADR-0024 states: **a schema problem must not surface from `serialize()`**. Answering
  `NONE` rather than throwing is deliberate — there is nothing to reject, only a name that did
  not mean what it usually does. This could not be relocated with a `checkArgument`:
  `ProtoMessageSerializer` builds its state lazily, so on a task manager even plan construction
  happens inside that catch; the failure had to be *removed*, not moved.
- **Six constants, not sixteen**: the nine wrappers share one, because both the column type and
  the conversion kind come from the wrapper's `value` sub-field through the *same*
  `scalarType`/`scalarKind` functions a bare scalar goes through, so a `UInt64Value` inherits
  the `uint64` range check with no second table to keep in sync.
- Mappings: wrappers → the wrapped scalar; `Struct`/`Value`/`ListValue` → `JSON`; `Duration` →
  `INT64` micros; `FieldMask` → `STRING` of comma-joined **verbatim** paths
  (`FieldMaskUtil.toString`, not `toJsonString`, which lowerCamelCases them); `Any` →
  **nothing**, it stays `STRUCT<type_url, value>` because unpacking needs a `TypeRegistry` the
  connector cannot obtain — and marking it JSON is not a workaround, since the printer then
  fails per record. `INTERVAL` for `Duration` was rejected because `TableSchemaToAvroConverter`
  rejects it and would break the FILE_LOADS round trip, and `REPEATED STRING` for `FieldMask`
  because a *repeated* `FieldMask` cannot be flattened, so singular and repeated would map
  differently.
- Two placements are load-bearing. Auto-JSON is folded into the **existing marking branch** in
  `convertField` (the `marked` type `markedType` returns since [#126]): that way `modeOf`'s "a
  singular marked column is never REQUIRED" rule covers it with no new clause, the recursion
  guard is never reached (these types are mutually recursive and were rejected outright before),
  and **a configured JSON marking keeps winning** — the branch returns before the message type
  is inspected. And the WKT switch sits **before** the recursion guard, so two `Timestamp`s on
  one path are not a rejection. Modes need no new rule: these are message fields, so they have
  presence; the one deviation is deliberate — a proto2 `required` wrapper derives `REQUIRED`,
  and it is mandatory, so that is faithful.
- An out-of-range `Duration` is a **row-level** failure like uint64 overflow, rewrapped so the
  message names the field (protobuf's own names none); sub-microsecond truncation is silent, as
  it already is for `Timestamp`. `FieldPlan` moved to named static factories; only
  `Duration`/`FieldMask` need the `instanceof`-or-rebuild shape `toEpochMicros` established,
  because only they *construct* a well-known type to hand to `Durations`/`FieldMaskUtil` — a
  wrapper does not, so one `getField` serves a generated instance and a `DynamicMessage` alike.
- The test fixture `WellKnownTypes` is a **noun phrase like its sibling `AllTypes`** — the
  message is not itself well-known, it *contains* every well-known type; `WellKnown` alone was
  an adjective and was renamed for that reason.

## Evidence

- A zero-field message (`google.protobuf.Empty`) is rejected at schema derivation by a check
  stated about *columns* rather than about `Empty`, so it catches any user-written empty message
  too: the client library rejects such a column itself ("The RECORD field must have at least one
  sub-field") before a request is ever sent, with a message naming no field.
- `REPEATED JSON` works on **real** BigQuery but not on the goccy emulator, which rejects every
  insert into a table carrying an `ARRAY<JSON>` column — [#16] folded the original workaround
  fixtures into the gated `BigQuerySerializerFidelityITCase`, which writes the full fixture on
  the service, and the emulator class keeps only the schema half.

## Consequences

This is a **breaking schema change** for any existing table (`STRUCT` → scalar):
`SchemaUnifier` rejects the union rather than corrupting rows.

[#16]: https://github.com/laughingman7743/flink-connector-gcp/issues/16
[#121]: https://github.com/laughingman7743/flink-connector-gcp/issues/121
[#124]: https://github.com/laughingman7743/flink-connector-gcp/issues/124
[#126]: https://github.com/laughingman7743/flink-connector-gcp/issues/126
[#147]: https://github.com/laughingman7743/flink-connector-gcp/issues/147
