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

# ADR-0176: The Spanner catalog maps an instance's databases and INFORMATION_SCHEMA to Flink tables

- Status: Accepted
- Date: 2026-10-03
- Issues: [#1214](https://github.com/flink-gcp/flink-connector-gcp/issues/1214), [#1212](https://github.com/flink-gcp/flink-connector-gcp/issues/1212)
- Modules: spanner
- Refines: ADR-0096's "the DDL must match the live schema and is not read at planning", which now holds for hand-written tables only, and its marker options, which gain `schema.generated-columns`; ADR-0105's column validation, which a change-stream table with a non-key generated column now meets at planning
- Current behavior: [Spanner catalog](../content/docs/connectors/table/spanner.md#catalog)

## Context

ADR-0168 fixes the shape every connector catalog shares, and `AbstractReadOnlyCatalog` holds its read-only half.
This record is what Spanner adds: what a database and a table name are, how `INFORMATION_SCHEMA` becomes a Flink schema in both dialects, and what the catalog leaves out.

Before this change the module had no Spanner-to-Flink type direction.
`SpannerTableSchemaConverter` maps a declared Flink schema to Spanner types, and the module read `INFORMATION_SCHEMA` only for index keys, cell weights and change-stream metadata, never `TABLES` or `COLUMNS`.
A Spanner table also has one more address level than a Flink catalog: a database, a schema and the table.

## Decision

### Scope

One catalog covers one instance.
Its databases are the instance's databases (`DatabaseAdminClient.listDatabases`), in either dialect, and a database's dialect comes from `getDatabase`.
A dialect is fixed when a database is created, so the catalog asks once per database and asks again only after a request finds the database dropped, which it then reports as missing; a missing database is never cached, so one created while the catalog is open is found.
A database dropped and created again under the same name in the other dialect, with no request finding it missing in between, keeps the first dialect cached, and the catalog answers it with that dialect's statements and table options until the catalog is created again.
Covering that case would cost a `getDatabase` on every statement, for a database replaced in place under a running catalog.
Its tables are each database's base tables outside the system schemas (`INFORMATION_SCHEMA.TABLES` with `TABLE_TYPE = 'BASE TABLE'`).
One `Spanner` handle, built from the project alone through `SpannerClients`, serves the admin client and every database's data client, and closes with the catalog.

The factory applies the table factory's credential and endpoint checks to the catalog's options, and checks `project`, `instance` and `default-database` as path components under their keys (ADR-0127).
`default-database` is also checked against Spanner's database-id grammar, `[a-z][a-z0-9_-]{0,28}[a-z0-9]`: a name outside it names no database, and the failure would otherwise be every unqualified table reporting "not found" without naming the option.
The catalog answers such a name in `databaseExists` without a request, for the reason ADR-0169 gives for datasets.

### Table names

A table in the default schema, the empty GoogleSQL schema or PostgreSQL `public`, is the Flink object name alone; a table in a named schema is `schema.table`.
Each part is rendered in the canonical quoting the connector's `schema` and `table` options already decode (ADR-0096): a GoogleSQL part outside the plain identifier grammar is backtick-quoted, a PostgreSQL part with an upper-case letter or a character outside the plain grammar is double-quoted.
`getTable` splits the name on dots outside quotes and decodes each part with the same decoder.
The rendering and the parse are inverses, so every name `listTables` returns resolves to the table it came from; a name that is not one or two canonical parts is `TableNotExistException`, never a `CatalogException`.
The lookup compares a GoogleSQL name case-insensitively and a PostgreSQL name exactly, the rule `SpannerTableName.catalogKey` applies, and the resolved table carries the native spelling `INFORMATION_SCHEMA` reports.

A default-schema table carries `table` alone, holding the native name, which the connector passes to Spanner without decoding.
A named-schema table carries `schema` and `table`, each in canonical quoting, which the connector decodes back to the native names.

### Type mapping

`INFORMATION_SCHEMA.COLUMNS.SPANNER_TYPE` is parsed into the inverse of the connector page's mapping, one Flink type per Spanner type:

| `SPANNER_TYPE`, GoogleSQL | `SPANNER_TYPE`, PostgreSQL | Flink | Marker |
|---|---|---|---|
| `BOOL` | `boolean` | `BOOLEAN` | |
| `INT64` | `bigint` | `BIGINT` | |
| `FLOAT32` | `real` | `FLOAT` | |
| `FLOAT64` | `double precision` | `DOUBLE` | |
| `NUMERIC` | `numeric` | `DECIMAL(38, 9)` | |
| `STRING(n)`, `STRING(MAX)` | `character varying`, `character varying(n)` | `STRING` | |
| `BYTES(n)`, `BYTES(MAX)` | `bytea` | `BYTES` | |
| `DATE` | `date` | `DATE` | |
| `TIMESTAMP` | `timestamp with time zone`, `spanner.commit_timestamp` | `TIMESTAMP_LTZ(9)` | |
| `JSON` | `jsonb` | `STRING` | `schema.json-field-paths` |
| `UUID` | `uuid` | `STRING` | `schema.uuid-field-paths` |
| `PROTO<pkg.Message>`, or `` `pkg.Message` `` on the emulator | | `BYTES` | `schema.proto-type-names` |
| `ENUM<pkg.Enum>`, or `` `pkg.Enum` `` on the emulator | | `BIGINT` | `schema.enum-type-names` |
| `ARRAY<T>`, with an optional `(vector_length=>n)` | `T[]`; a vector column as `ARRAY<T>(vector_length=>n)`, or `T[] vector length n` | `ARRAY<T>`, nullable elements | the element's, on the array field |

Spanner reports a PROTO or ENUM column with its kind, as `PROTO<pkg.Message>` or `ENUM<pkg.Enum>`.
The emulator reports only its fully qualified name in backticks, with nothing marking which of the two it is; for that spelling the catalog classifies the name from the proto descriptors `GetDatabaseDdl` returns, fetched only for a table that has such a column.
A PostgreSQL `numeric` has no fixed precision, so `DECIMAL(38, 9)` is the one Flink type the mapping can name; a stored value outside it fails the read, naming the column (ADR-0135).
A type the parser does not recognise, such as `INTERVAL`, fails `getTable` naming the table, the column and the raw `SPANNER_TYPE`; the column is never dropped (ADR-0168).
A column Spanner declares `NOT NULL` resolves as `NOT NULL`.

### Primary keys

The primary key comes from `INFORMATION_SCHEMA.INDEX_COLUMNS` with `INDEX_TYPE = 'PRIMARY_KEY'`, in ordinal order and in the native spelling, which is what `SpannerTableReadResolver` compares a declared key against.
Its columns are `NOT NULL` (ADR-0168), including a GoogleSQL key column Spanner allows to be `NULL`.
A key whose type the connector cannot key on, such as `FLOAT32`, PostgreSQL `numeric` or an ENUM, resolves, and the table factory refuses a statement over it as it refuses a hand-written table with that key.

### Generated and hidden columns

A stored generated column (`IS_GENERATED = 'ALWAYS'`, `IS_STORED = 'YES'`) resolves as an ordinary column and is listed in a new connector option, `schema.generated-columns`.
A generated column that is not stored is left out: Spanner's read API, which the table source and the lookup both go through, refuses to return one ("Cannot read generated column without STORED attribute"), so resolving it would leave a table no scan can read.
The sink leaves a listed column out of every insert and upsert, because Spanner refuses a mutation that sets a generated column, a generated key column included; an upsert without the key column lets Spanner compute the key, and a delete still names the row by it.
A generated key column is `NOT NULL`, as every key column is, so an `INSERT INTO` must supply it even though an upsert leaves the value out.
The value a row carries for a generated key column must therefore equal the one Spanner computes: the planner keys upserts by it and a delete addresses the row with it, so a statement that invents one writes a row it cannot later delete.
Checking the two against each other would need the generation expression evaluated in Flink, which the connector does not do.
A non-key generated column is nullable in the Flink schema whatever Spanner declares, so the planner's not-null enforcement does not refuse an `INSERT INTO` that leaves it out.
The option is public and applies to a hand-written table as well, so a table with generated columns is writable without the catalog.

Spanner change streams do not watch a generated column outside the primary key.
A change-stream table that lists such a column is therefore refused at planning, naming it, rather than failing on the first record as a missing declared column does (ADR-0105); a generated key column is watched and read.
The check sees only the key the DDL declares, which for a catalog table is Spanner's own, so a hand-written table that leaves the key undeclared is refused for a generated key column too, and the message asks for the key.
A hand-written table that declares a generated column without listing it still fails on the first record, and that failure names the constant reason the converter refused the record, here that the record omits a declared column, without quoting any value from it.

A hidden column (`IS_HIDDEN`) is left out, as Spanner's own `SELECT *` leaves it out; a hidden key column would be kept, since the primary key names it.
Those are the two exceptions to ADR-0168's "columns are never dropped": a hidden column is not part of the table's visible row, and the `TOKENLIST` columns a search index reads are always hidden and have no value a read could return; a generated column that is not stored is one the connector cannot read at all.

### Views, change streams and statistics

Views are not listed: the table source reads through Spanner's read API, which takes a table.
Change streams are not listed either; an `OPTIONS` hint turns a catalog table into its change-stream source.
Statistics are `UNKNOWN`: `INFORMATION_SCHEMA` holds no row count, and ADR-0168's reopen condition cannot be met.

### Tier

`SpannerCatalogOptions` is `@PublicEvolving`; the catalog and its factory are `@Internal` (ADR-0168).
`schema.generated-columns` is a `SpannerConnectorOptions` constant at that class's tier.

## Evidence

Measured on the Spanner emulator 1.5.57 on 2026-10-03, by a throwaway probe that created a table of every type in each dialect:

- `SPANNER_TYPE` spellings are the ones in the table above, with one exception: the PostgreSQL vector spelling `T[] vector length n` is the DDL's, accepted in case the service reports it, while the emulator reports the GoogleSQL spelling `ARRAY<FLOAT32>(vector_length=>3)`.
  A PostgreSQL `text` column reports `character varying`, and a PROTO or ENUM column reports `` `example.events.Event` `` with no kind.
- `GetDatabaseDdl` returns the proto bundle's `FileDescriptorSet`, from which messages and enums are told apart.
- GoogleSQL reports `IS_HIDDEN` as a BOOL and PostgreSQL as `YES` or `NO`; both report `IS_GENERATED` as `ALWAYS` or `NEVER`.
- The emulator refuses `INTERVAL` and `STRUCT` columns, and a `TOKENLIST` column that is not `HIDDEN`.
- A table or schema name cannot contain a dot in either dialect, while a quoted column name can contain `.` or `;`, which is why the marker values are quoted as Flink's own option serializer quotes them.
- The emulator treats PostgreSQL names that differ only in case as duplicates.
- A database id must match the grammar above.
- Writing a generated column fails with `FAILED_PRECONDITION` in both dialects; an upsert without a generated key column succeeds, and a delete by it succeeds.
- Reading through Spanner's read API returns a stored and a virtual generated column alike.
- A change stream's records omit stored and virtual non-key generated columns, and carry a generated key column.
  The [change streams overview](https://docs.cloud.google.com/spanner/docs/change-streams) states the same: "Change streams don't watch generated columns unless the column is part of the primary key."

`SpannerCatalogITCase` holds these through the planner in both dialects, and the unit tests hold the naming round trip for every string up to three characters over an alphabet of both quote characters, the escape, the dot, whitespace and mixed case.
Measured on the service on 2026-10-03 by `SpannerCatalogRealGcpITCase`, on a 100-processing-unit instance in `regional-us-central1`:

- Every `SPANNER_TYPE` in the table above, in both dialects, including `PROTO<example.events.Event>`, `ENUM<example.events.Event.Kind>`, `ARRAY<ENUM<example.events.Status>>`, `character varying[]` and `real[] vector length 3`, where the emulator's spellings differ.
- `IS_HIDDEN` is a BOOL in GoogleSQL and `YES`/`NO` in PostgreSQL, as on the emulator.
- A PostgreSQL database's schemata are `public`, `pg_catalog`, `information_schema`, `spanner_sys` and its own; `information_schema.index_columns` lists none of the system schemas.
- Listing excludes views, change streams and the system schemas in both dialects.
- A database id with no database answers "no such database" through `DatabaseNotFoundException`, as on the emulator.
- The read API refuses a generated column that is not stored, which the emulator returns.
- A catalog table with a stored generated column is written and read through application-default credentials.

Not measured: which permissions listing needs at the least, since the suite runs as the project owner; and what the service answers for a database the credentials cannot see, which the catalog reports as a `CatalogException` unless the client raises `DatabaseNotFoundException`.

## Alternatives declined

- **A Flink database per Spanner schema, one catalog per database**: GoogleSQL's default schema is the empty name, which cannot be a Flink database, and any stand-in such as `default` could collide with a real schema of that name.
- **Typing columns from a query's `ResultSetMetaData`**, as the JDBC connector's Spanner catalog does: it runs a query against the table, reports display sizes rather than lengths and a fixed `NUMERIC` precision, and needs a second `INFORMATION_SCHEMA` query for nullability; `SPANNER_TYPE` carries all of it.
- **`analyzeQuery` to classify PROTO and ENUM columns**: it plans a query against the table for a fact the proto bundle states directly.
- **`VARCHAR(n)` for `STRING(n)`**: the connector neither checks nor enforces a length, the reason ADR-0169 gives for BigQuery.
- **Leaving generated columns out**: reads would lose them, against ADR-0168's "columns are never dropped".
- **Emitting `NULL` for a non-key generated column in a change-stream read**: the column has a value Spanner did not record, and ADR-0105 declines to invent one; a hand-written change-stream table that leaves the column out reads the stream.
- **Listing views as tables**: the read API cannot read them, so every listed view would fail its first scan.

## Consequences

- A Spanner table is usable from SQL with no DDL, in either dialect and in any schema.
- `schema.generated-columns` makes a hand-written table with generated columns writable too.
- The catalog's credentials need the database admin permissions for listing and dialect, and on the emulator, whose `SPANNER_TYPE` omits a proto type's kind, for the proto bundle, beside the data permissions the tables' reads and writes already need.
