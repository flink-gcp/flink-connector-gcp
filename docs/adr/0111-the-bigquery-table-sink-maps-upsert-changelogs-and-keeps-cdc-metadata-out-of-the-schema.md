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

# ADR-0111: The BigQuery table sink maps upsert changelogs and keeps CDC metadata out of the schema

- Status: Accepted
- Date: 2026-08-14
- Issues: [#65](https://github.com/laughingman7743/flink-connector-gcp/issues/65),
  [#626](https://github.com/laughingman7743/flink-connector-gcp/issues/626)
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/table/bigquery.md#change-data-capture`

## Context

ADR-0110 added DataStream CDC primitives while leaving Flink SQL changelog conversion and
sequence extraction to the table layer.
Flink represents an upsert sink with `INSERT`, `UPDATE_AFTER`, and `DELETE` rows, while BigQuery
requires `UPSERT` or `DELETE` plus optional sequence metadata.
Flink also appends selected writable metadata fields after the physical row, so treating the
consumed runtime type as the BigQuery schema would create columns that do not belong to the table.

Delete rows present a second mismatch.
An upsert changelog may contain only the key on a delete, but ordinary protobuf validation requires
every physical field derived as `REQUIRED`.
The table sink therefore needs a delete representation that remains valid on the wire without
weakening validation for inserts and upserts.

## Decision

**Table CDC is explicit and stays on the Storage Write API default stream.**
`sink.cdc.enabled` defaults to `false` and requires both
`sink.write-method=storage-api-at-least-once` and a declared Flink primary key.
When enabled, the sink accepts an upsert changelog, maps `INSERT` and `UPDATE_AFTER` to `UPSERT`,
maps `DELETE` to `DELETE`, and rejects `UPDATE_BEFORE` at the serializer and change-type
boundaries.
A primary-key change is represented as a delete of the old key followed by an upsert of the new
key; interpreting every update-before row as a delete would also manufacture deletes for ordinary
updates.

**Delete serialization selects physical primary-key fields only.**
The selected converter retains the physical descriptor's original field numbers and builds a
partial protobuf message, which permits omitted required non-key fields on the wire.
The full-row converter continues to build a fully initialized protobuf message, so inserts and
upserts retain their existing required-field validation.

**Writable CDC metadata is separate from the physical row type.**
The sink exposes `change-sequence-number` as a nullable string and
`debezium-source-properties` as a nullable string map.
The planner may select no sequence source or exactly one; selecting either while CDC is disabled
or selecting both fails during planning.
The physical row type remains the sole input to BigQuery schema derivation, and the selected
metadata value is read at the physical arity after Flink appends it to the runtime row.

**The Debezium map is a stable hand-off to connector-specific profiles, not a timestamp
fallback.**
The source-owned key remains `value.source.properties`, while the BigQuery sink owns the writable
metadata key `debezium-source-properties`.
The map's `connector` value routes to a profile that selects ordering fields whose semantics are
defined by that connector.
An unavailable profile fails explicitly; `ts_ms` is not used alone because multiple changes in
one millisecond could then receive the same sequence and fall back to ingestion order.
PostgreSQL, MySQL GTID, and Spanner profiles refine this record through issues
[#629](https://github.com/laughingman7743/flink-connector-gcp/issues/629),
[#631](https://github.com/laughingman7743/flink-connector-gcp/issues/631), and
[#633](https://github.com/laughingman7743/flink-connector-gcp/issues/633).

**The changelog-mode compatibility seam is version-specific and internal.**
Flink 2.x receives an upsert mode that advertises key-only deletes, while Flink 1.20 receives the
older upsert mode whose API cannot express that bit.
Runtime serialization supports key-only deletes on every supported version.

## Evidence

- Planner coverage proves an upsert changelog retains deletes and projects a selected sequence
  field into writable metadata.
- Factory and sink tests cover the default, primary-key and write-method requirements, the
  cross-version changelog contract, metadata inventory, selection, copying, and CDC propagation.
- Serializer tests prove a primary-key change becomes a sequenced delete of the old key and upsert
  of the new key, a key-only delete omits a required non-key field, ordinary rows keep full
  validation, metadata stays outside the physical schema, and update-before rows fail.
- Adapter tests cover every accepted row kind, formatted sequence extraction, null propagation,
  and rejection of an unsupported Debezium connector without a timestamp fallback.

## Alternatives declined

- **Map `UPDATE_BEFORE` to `DELETE`.**
  This would emit a delete before every ordinary update, and a shared sequence could make the
  winner dependent on service tie-breaking.
- **Serialize every physical column for a delete.**
  Key-only deletes would fail when a non-key column is required, despite BigQuery needing only the
  primary key to identify the row.
- **Add writable metadata fields to the serializer's row type.**
  Table creation and schema reconciliation would then treat CDC control data as physical BigQuery
  columns.
- **Name the sink metadata `value.source.properties`.**
  That key belongs to the source value format and would make the sink's public surface depend on
  one enclosing source connector's metadata prefix.
- **Use `ts_ms` whenever no connector profile is known.**
  Timestamp collisions would silently discard the source system's stronger ordering information.

## Consequences

SQL and Table API jobs can preserve a Flink upsert changelog as BigQuery CDC mutations without
placing pseudocolumns or Debezium maps in the table schema.
Jobs must model a primary-key change as delete plus upsert and must not send update-before rows to
the runtime adapter.
The destination must have a BigQuery primary key.
ADR-0112 records how both CDC APIs can create it and apply optional maximum staleness without
treating a successful but lossy REST field as evidence.
The formatted-sequence route is usable immediately, while Debezium maps become operational one
connector profile at a time through #629, #631, and #633.
