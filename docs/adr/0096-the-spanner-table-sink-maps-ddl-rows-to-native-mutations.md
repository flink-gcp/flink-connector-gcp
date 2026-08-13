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

# ADR-0096: The Spanner table connector maps DDL rows to native values

- Status: Accepted
- Date: 2026-08-11, revised 2026-08-13
- Issues: [#502](https://github.com/laughingman7743/flink-connector-gcp/issues/502), [#503](https://github.com/laughingman7743/flink-connector-gcp/issues/503), [#527](https://github.com/laughingman7743/flink-connector-gcp/issues/527), [#528](https://github.com/laughingman7743/flink-connector-gcp/issues/528), [#529](https://github.com/laughingman7743/flink-connector-gcp/issues/529), [#563](https://github.com/laughingman7743/flink-connector-gcp/issues/563) (under
  [#223](https://github.com/laughingman7743/flink-connector-gcp/issues/223)), [#573](https://github.com/laughingman7743/flink-connector-gcp/issues/573), [#544](https://github.com/laughingman7743/flink-connector-gcp/issues/544)
- Modules: spanner
- Current behavior: `docs/content/docs/connectors/table/spanner.md`

## Context

The DataStream sink accepts native Spanner `Mutation` objects through an application serializer.
SQL has no serializer callback, so the DDL must determine the mutation operation, native types, and primary-key encoding without weakening the DataStream sink's batching and retry contract.

## Decision

**The `spanner` table sink maps each physical `RowData` field directly onto one named Spanner column.**
The common lossless mappings cover `BOOL`, `INT64`, `FLOAT32`, `FLOAT64`, `STRING`, `BYTES`, `DATE`, `TIMESTAMP`, and one-dimensional arrays.
`DECIMAL(38, 9)` maps to GoogleSQL `NUMERIC`, while every Flink-supported `DECIMAL(p, s)` maps to PostgreSQL `numeric`; their client types and value factories are distinct.
PostgreSQL stores a wider physical domain than Flink can declare, so reads require exact representability in the declared Flink shape and fail rather than round or substitute null.
PostgreSQL `numeric` NaN also fails conversion because Flink `DECIMAL` has no NaN representation.
The connector rejects nested arrays and key types excluded by Spanner's [GoogleSQL](https://cloud.google.com/spanner/docs/reference/standard-sql/data-types) and [PostgreSQL](https://cloud.google.com/spanner/docs/reference/postgresql/data-types) type contracts before it creates a runtime provider.
Spanner UUID, JSON, PROTO, and ENUM cannot be distinguished from their Flink carrier types, so explicit field-path options mark them and provide native type names where Spanner requires one.
UUID fields use Flink `STRING` carriers in both dialects and require the complete 36-character hexadecimal form on writes and lookups.
Reads normalize UUID values to lowercase canonical strings.
The declared database dialect selects GoogleSQL `JSON` or PostgreSQL `jsonb`.
PROTO and ENUM markers are rejected for PostgreSQL databases because those named types are GoogleSQL-only.
Markers may mark an entire array of a special type.
Flink `ROW` is rejected because Spanner `STRUCT` is a query-result type, not a storable table column.

**A declared primary key selects idempotent upserts and enables deletes.**
`INSERT` and `UPDATE_AFTER` become `insertOrUpdate` mutations, while `DELETE` carries only the declared key columns.
A table without a primary key is insert-only because SQL cannot construct a Spanner delete key or make replayed writes idempotent without one.
`UPDATE_BEFORE` is rejected defensively; the planner does not send it to the advertised upsert sink.

**The table options are a mapping onto the existing builders.**
The destination fields assemble `SpannerDatabase`, the physical DDL supplies the serializer, and the eight `sink.*` options map one-for-one onto `SpannerWriterOptions`.
The table layer keeps the DataStream sink's fail-job constraint and failed-mutation policies because a DDL cannot carry a serializable failure-handler implementation.
The shared `service-account-key-file` option maps to the bounded source and sink builders and to synchronous and asynchronous lookup clients.
Only its path is serialized, and each JobManager or TaskManager component that owns a client reads the service-account JSON when that component opens.
Absent on a real-service path keeps ADC, and an emulator endpoint is mutually exclusive with the key path.

**The optional `schema` value qualifies every Table API data path.**
The sink mutations, bounded source reads, and lookup point reads use the same dialect-specific fully qualified table name.
When `schema` is set, the schema, table, and secondary-index options each contain one identifier component rather than a prequalified name.
PostgreSQL unquoted components fold to lower case and quoted components preserve case, while GoogleSQL schema-object components are case-insensitive.
Quoted values use the dialect's canonical delimiter escape: PostgreSQL doubles a double quote, and GoogleSQL backslash-escapes a backtick or backslash.
The connector checks only that each value is non-blank, structurally one component, and canonically quoted.
It decodes each component to its catalog spelling before assembling the fully qualified native data-API name; SQL delimiters are not part of that name.
It does not copy Spanner's evolving character, length, or keyword rules into the connector: bounded-scan catalog resolution uses `INFORMATION_SCHEMA`, and the native Mutation and lookup APIs remain authoritative for their names.
Leaving `schema` unset preserves the former empty GoogleSQL schema and PostgreSQL `public` behavior.

**The table source is a bounded `partitionRead` and pushes down top-level projection.**
The retained DDL columns become the read operation's column list and the converter emits the projected shape in planner order.
The same converter serves bounded scans and both lookup modes, including the exact decimal conversion rule.
When the planner retains no physical column, the first DDL column is a carrier for the read while conversion emits zero-field rows.
Nested projection is not advertised, and no range option is exposed because Spanner plans partitions from physical storage rather than from a user-selected column.
Partition hints, Data Boost, RPC priority, snapshot bounds, and source parallelism map directly onto the existing source builder.

**Bounded scans push the exact key subset and can select a secondary index.**
The source translates equality and ordered comparisons over consecutive key columns into native `KeySet` points and lexicographic ranges.
A complete primary-key equality and a leading equality prefix, optionally followed by one range column, are exact, so Flink need not evaluate them again.
Unsupported or non-leading predicates remain residual.
When `scan.index` is set, the source resolves schema existence, key order, direction, readiness, null filtering, and readable columns from the live `INFORMATION_SCHEMA` at the batch transaction's exact snapshot.
Secondary-index filtering is best effort, so every candidate remains a Flink residual.
The job fails when the named index is absent, not `READ_WRITE`, unsafe for nullable key rows, or cannot return the requested and residual columns; it never silently falls back to the base table.

## Evidence

Measured 2026-08-11 against the pom-pinned Flink 2.2.1 and Spanner emulator 1.5.56:

- The production factory planned and executed separate insert and upsert jobs against both GoogleSQL and PostgreSQL databases.
- The schema object crosses Flink's job serialization boundary; the integration test caught and now pins that requirement.
- Unit tests cover native scalar and composite mappings, special markers, primary-key deletes, insert-only tables, changelog modes, factory validation, and option parity with both DataStream builders.
- Source tests pin projection order, zero-column carrier reads, native-to-`RowData` conversion, mutually exclusive snapshot options, and both emulator dialects.
- Filter tests pin exact primary-key points and ranges, descending index bounds, residual ownership, null-filter safety, lookup gating, and deferred-read serialization.
- Named-schema tests pin dialect parsing, qualified sink and source paths, schema-aware metadata bindings, and distinct all-schema cell weights.
- Issue #563 adds PostgreSQL decimal coverage for multiple Flink shapes, exact scalar and array conversion, nulls, overflow, scale loss, NaN, and production sink-to-bounded-source round trips.
- Issue #527 adds native UUID scalar, array, null, primary-key, scan, and synchronous and asynchronous lookup coverage for both emulator dialects.

## Alternatives declined

- **Infer JSON, PROTO, and ENUM from carrier types**: `STRING`, `BYTES`, and `BIGINT` are also ordinary Spanner types, so inference would silently change schemas with the same Flink DDL.
- **Accept every form parsed by `UUID.fromString`**: Java also accepts shortened component forms, which would make the SQL carrier contract wider and less predictable than the documented Spanner spelling.
- **Use insert-or-update without a declared key**: Spanner tables always have physical keys, but their columns and order are not available in the DDL; an extra metadata read would make planning depend on the live destination and still leave Flink without an upsert key.
- **Expose failure handlers as strings**: the shared SPI accepts application code, not a closed enum, so a string surface would represent only an arbitrary subset and diverge from the builder.

## Consequences

A primary key is optional, but it changes the accepted changelog and replay behavior.
The DDL schema must match the destination's column names and native types; this slice does not read the live schema during planning.
Changing a physical `STRING` column to `UUID` requires a coordinated database migration and DDL option update; the connector neither validates existing rows in advance nor changes the live schema.
Because PostgreSQL DDL does not constrain a `numeric` column to the Flink declaration, a stored value outside that declaration fails the scan or lookup with the physical column name and declared decimal shape.
The lookup source builds on the same type mapping in [#504](https://github.com/laughingman7743/flink-connector-gcp/issues/504).
The table connector does not create schemas or qualify a table from a multipart `table` value when `schema` is set.
