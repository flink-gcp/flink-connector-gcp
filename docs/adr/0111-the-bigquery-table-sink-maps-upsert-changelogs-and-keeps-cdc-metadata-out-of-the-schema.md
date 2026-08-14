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
- Date: 2026-08-14; revised by [#629] (2026-08-15)
- Issues: [#65](https://github.com/laughingman7743/flink-connector-gcp/issues/65),
  [#626](https://github.com/laughingman7743/flink-connector-gcp/issues/626),
  [#629](https://github.com/laughingman7743/flink-connector-gcp/issues/629)
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
The `RowData` provider reads only the selected writable metadata position and delegates Debezium
maps to a separate connector resolver.
That resolver owns connector dispatch and profile-specific property extraction, so adding MySQL or
Spanner does not add source-specific branches to the `RowData` adapter.

Flink's `debezium-avro-confluent` format produces the physical-row changelog from `before`,
`after` and `op`, but does not retain the envelope's `source` record in that changelog.
The raw `avro-confluent` format can retain the complete envelope, but advertises an insert-only
changelog.
A projection that selects `before` or `after` with a `CASE` expression changes column values, not
the INSERT RowKind supplied by that source, so it cannot make a Debezium delete into a sink delete.
An Avro Kafka path that needs connector-specific ordering therefore keeps the complete envelope as
a `GenericRecord` and either calls the public
`DebeziumPostgreSqlCdcSequenceNumberProvider` from its DataStream CDC configuration or registers a
changelog DataStream carrying the source-properties map before entering SQL.
This adapter boundary is shared by the PostgreSQL, MySQL and Debezium Spanner profiles; the native
Spanner source instead exposes its own typed ordering metadata under #633.

**The PostgreSQL profile transcodes Debezium's two-part sequence into BigQuery sections.**
Only `connector=postgresql` selects this profile.
It parses Debezium's `sequence` as the nullable last committed LSN followed by the required current
LSN, represents a null last committed position as zero, and encodes both as fixed-width unsigned
64-bit hexadecimal sections.
A non-null `lsn` property must represent the same 64-bit value as the current sequence position.
Malformed or contradictory metadata fails the row instead of changing its ordering source.

This is not a parser for PostgreSQL's customary `X/Y` LSN display form.
PostgreSQL defines `pg_lsn` internally as an unsigned 64-bit byte position in the WAL; Debezium
exposes the same bits as signed decimal strings through `Lsn.asLong()`.
The encoder accepts both signed and unsigned decimal spellings, preserves their bit pattern, and
writes each value as one BigQuery hexadecimal section.
BigQuery compares those sections as unsigned numbers, first section before second.

The last-committed position is retained rather than using `source.lsn` alone.
PostgreSQL logical messages can share a current LSN across a transaction boundary, and Debezium's
resume logic separately records the last committed position for that reason.
The pair distinguishes progress into the next transaction even when its first event repeats the
previous commit's current LSN.
It remains possible for multiple events within one transaction to share both positions; Debezium
keeps an internal per-LSN event counter for precise restart recovery, but does not expose that
counter in `source.sequence`.
The pair is therefore deterministic and ordered for continuous tuple progress, but is not a unique
total-order identifier for every event.
If distinct events for the same BigQuery primary key share the pair, BigQuery falls back to system
ingestion time and this provider alone cannot make an out-of-order replay deterministic.
Applications that admit that case must provide a stable additional tie-breaker through the
formatted metadata route or a custom DataStream provider.

Replaying one source event reproduces its sequence without connector state.
That property also holds across a PostgreSQL primary failover when Debezium continues from the same
preserved and synchronized logical replication slot.
A recreated slot or another discontinuous LSN history is unsupported because the source map exposes
no timeline or ordering epoch that could disambiguate it.
The user documentation links to Debezium's PostgreSQL failover guidance rather than duplicating its
version-specific operational procedure.

MySQL GTID and Spanner profiles refine this record through issues
[#631](https://github.com/laughingman7743/flink-connector-gcp/issues/631) and
[#633](https://github.com/laughingman7743/flink-connector-gcp/issues/633).

**The changelog-mode compatibility seam is version-specific and internal.**
Flink 2.x receives an upsert mode that advertises key-only deletes, while Flink 1.20 receives the
older upsert mode whose API cannot express that bit.
Runtime serialization supports key-only deletes on every supported version.

## Evidence

- PostgreSQL documents `pg_lsn` as an internal 64-bit WAL byte position with the external `X/Y`
  hexadecimal form and standard comparison operators in the
  [`pg_lsn` type](https://www.postgresql.org/docs/current/datatype-pg-lsn.html).
- Debezium documents `source.sequence` as a JSON array containing the last committed and current
  LSN in that order in the
  [PostgreSQL connector reference](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).
  Its pinned
  [`SourceInfo.sequence()` implementation](https://github.com/debezium/debezium/blob/20a5d35af7ed00f984c15a543e6ee1ca1641630e/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/SourceInfo.java#L180-L197)
  serializes `Lsn.asLong()` as decimal strings.
- The introducing
  [DBZ-2911 commit](https://github.com/debezium/debezium/commit/06b0475f172155517908c256f579897783c60a95)
  describes the pair as PostgreSQL deduplication metadata.
  Debezium's pinned
  [per-LSN counter](https://github.com/debezium/debezium/blob/20a5d35af7ed00f984c15a543e6ee1ca1641630e/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresOffsetContext.java#L185-L209)
  and [DBZ-6204](https://github.com/debezium/debezium/pull/4357) show that multiple logical events
  can share one LSN.
- BigQuery documents one-to-four unsigned hexadecimal sections, lexicographic section comparison,
  and ingestion-time precedence for equal values in its
  [CDC ordering contract](https://cloud.google.com/bigquery/docs/change-data-capture).
- Flink 2.2.1's pinned
  [`RegistryAvroFormatFactory`](https://github.com/apache/flink/blob/release-2.2.1/flink-formats/flink-avro-confluent-registry/src/main/java/org/apache/flink/formats/avro/registry/confluent/RegistryAvroFormatFactory.java)
  advertises an insert-only changelog, while its
  [`DebeziumAvroFormatFactory`](https://github.com/apache/flink/blob/release-2.2.1/flink-formats/flink-avro-confluent-registry/src/main/java/org/apache/flink/formats/avro/registry/confluent/debezium/DebeziumAvroFormatFactory.java)
  advertises Debezium row kinds and its
  [`DebeziumAvroDeserializationSchema`](https://github.com/apache/flink/blob/release-2.2.1/flink-formats/flink-avro-confluent-registry/src/main/java/org/apache/flink/formats/avro/registry/confluent/debezium/DebeziumAvroDeserializationSchema.java)
  derives those row kinds from `before`, `after` and `op`.
  Flink's [dynamic-table model](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/concepts/dynamic_tables/)
  treats records from an insert-only stream as INSERT modifications, so projecting the raw
  envelope does not reinterpret its `op` field as a changelog operation.
- Planner coverage proves an upsert changelog retains deletes and projects a selected sequence
  field into writable metadata.
- Factory and sink tests cover the default, primary-key and write-method requirements, the
  cross-version changelog contract, metadata inventory, selection, copying, and CDC propagation.
- Serializer tests prove a primary-key change becomes a sequenced delete of the old key and upsert
  of the new key, a key-only delete omits a required non-key field, ordinary rows keep full
  validation, metadata stays outside the physical schema, and update-before rows fail.
- Adapter tests cover every accepted row kind, formatted sequence extraction, null propagation,
  PostgreSQL snapshot and streaming source shapes, replay and continuous-slot failover ordering,
  unsigned boundaries, malformed metadata, and rejection of unsupported Debezium connectors
  without a timestamp fallback.

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
- **Encode only `source.lsn`.**
  A current LSN can repeat across a commit, the next begin, and its first logical event; retaining
  Debezium's last-committed position preserves the transaction-progress component that a single
  position would discard.
- **Claim that the two values form a unique total order.**
  Debezium itself tracks an event count outside `source.sequence` because multiple events can share
  one LSN; the connector cannot reconstruct an absent tie-breaker.

## Consequences

SQL and Table API jobs can preserve a Flink upsert changelog as BigQuery CDC mutations without
placing pseudocolumns or Debezium maps in the table schema.
Jobs must model a primary-key change as delete plus upsert and must not send update-before rows to
the runtime adapter.
The destination must have a BigQuery primary key.
ADR-0112 records how both CDC APIs can create it and apply optional maximum staleness without
treating a successful but lossy REST field as evidence.
The formatted-sequence route and Debezium PostgreSQL profile are usable immediately.
DataStream jobs and the Table profile share the same strict PostgreSQL sequence encoder through
`DebeziumPostgreSqlCdcSequenceNumberProvider` rather than maintaining application-local parsers.
Debezium JSON can forward `value.source.properties` directly, while Debezium Avro requires the
complete-envelope adapter described above because Flink's standard changelog omits `source`.
Other Debezium maps become operational one connector profile at a time through
[#631](https://github.com/laughingman7743/flink-connector-gcp/issues/631) and
[#633](https://github.com/laughingman7743/flink-connector-gcp/issues/633).
