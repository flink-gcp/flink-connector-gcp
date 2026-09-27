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

# ADR-0169: The BigQuery catalog maps a project's datasets and REST schemas to Flink tables

- Status: Accepted
- Date: 2026-09-27
- Issues: [#1213](https://github.com/flink-gcp/flink-connector-gcp/issues/1213), [#1212](https://github.com/flink-gcp/flink-connector-gcp/issues/1212)
- Modules: bigquery
- Refines: ADR-0087's view opt-in, for catalog tables; ADR-0100's "planning does not fetch the live schema", which now holds for hand-written tables only
- Current behavior: [BigQuery catalog](../content/docs/connectors/table/bigquery.md#catalog)

## Context

ADR-0168 fixes the shape every connector catalog shares.
This record is what BigQuery adds: what a database is, how a REST table schema becomes a Flink schema, what a view resolves to, and what the catalog leaves out.

Before this change the module had no BigQuery-to-Flink type mapping.
The source reads the Storage Read API's Avro by physical name into whatever the DDL declares, and planning never fetched a schema (ADR-0100).
`BigQueryTableAdmin.getSchema` already read a live schema through the REST client the module ships, but only to reconcile a sink's schema.

## Decision

### Scope

One catalog covers one project.
Its databases are the project's datasets (`BigQuery.listDatasets`), and its tables are each dataset's tables of every type (`BigQuery.listTables`).
The options are the connector page's catalog option table.
The factory applies the table factory's own credential and endpoint checks to them, so a rejection reads the same at `CREATE CATALOG` as when a table is planned, and checks the path components under their keys (ADR-0127).
One check is the catalog's alone: the two emulator endpoints are set together or not at all.
The catalog's metadata requests use the REST endpoint and its tables' rows the gRPC one, so either alone sends one half to the service on ADC, and the REST-only half would read and write real rows under a schema the emulator answered.

A name outside the dataset-id grammar (letters, digits and underscores) is no database, answered without a request.
Calcite asks the current catalog whether a qualified name's first part is one of its databases, and that part may be another catalog's name, such as one with a hyphen.

The REST client comes from `BigQueryTableAdmin.restClient`, which the sink's table admin and the source's query runner now share; the catalog was the third copy of the emulator, key-file and application-default branches.
It now gives the client the configured project in every branch.
Every request through it names its project in full, so no request changes, but the builder no longer fails where the environment names no project: no `GOOGLE_CLOUD_PROJECT`, no gcloud configuration, and credentials that carry none, as an authorized user's or an external account's do.

### Type mapping

Each BigQuery type resolves to the widest Flink type the connector reads and writes it as:

| BigQuery | Flink |
|---|---|
| `STRING`, `JSON`, `GEOGRAPHY` | `STRING` |
| `BOOL` | `BOOLEAN` |
| `BYTES` | `BYTES` |
| `INT64` | `BIGINT` |
| `FLOAT64` | `DOUBLE` |
| `NUMERIC(p, s)` | `DECIMAL(p, s)`; unparameterized `DECIMAL(38, 9)` |
| `BIGNUMERIC(p, s)`, `p <= 38` | `DECIMAL(p, s)` |
| `DATE` | `DATE` |
| `TIME` | `TIME(3)` |
| `DATETIME` | `TIMESTAMP(6)` |
| `TIMESTAMP` | `TIMESTAMP_LTZ(6)` |
| `STRUCT` | `ROW`, recursively |
| `RANGE<DATE>`, `RANGE<DATETIME>`, `RANGE<TIMESTAMP>` | `` ROW<`start` T, `end` T> `` with `T` as above |
| mode `REQUIRED` | `NOT NULL` |
| mode `REPEATED` | a nullable `ARRAY<T NOT NULL>` |

A parameter-less precision or scale defaults as BigQuery defines it: `NUMERIC(p)` has scale 0.
Field and table descriptions become Flink comments.

`TIME` is `TIME(3)` because the source converts at most millisecond precision and rejects more.
Flink 1.20 and 2.2 plan it as `TIME(0)` (their `FlinkTypeFactory` drops the precision) while the connector's physical row type keeps `TIME(3)`; 2.3 keeps it throughout.
A repeated element is `NOT NULL` because BigQuery stores no null element and the sink rejects a nullable element declaration.
The array itself stays nullable, as a hand-written declaration would be.

`INTERVAL`, a `BIGNUMERIC` without parameters (up to 76 digits) or wider than 38 digits, a `TIMESTAMP` whose declared precision is finer than microseconds, and a type the client has no standard name for (`FOREIGN`, or one BigQuery adds later) have no Flink type the connector carries, and fail `getTable` naming the column path, the type and the reason.
An opt-in that exposed such a column as `STRING` was declined: the source reads Avro into the declared type, so it would need a conversion the connector does not have, for a case a hand-written query source that casts the column already covers.

The mapping is lossy in the direction the connector page's new column documents.
A table the sink created reads back with `TINYINT`, `SMALLINT` and `INT` as `BIGINT`, `CHAR` and `VARCHAR(n)` as `STRING`, `FLOAT` as `DOUBLE`, `MAP` and `MULTISET` as an array of key-value rows, a marked JSON `ROW` as `STRING`, and `NOT NULL` as nullable unless `sink.derive-required-columns` made the column `REQUIRED`.

### Primary keys

`TableInfo.getTableConstraints().getPrimaryKey()` becomes the Flink primary key, in the constraint's column order, with its columns made `NOT NULL` as ADR-0168 requires.
Key columns are matched to the schema ignoring case, since BigQuery column names are case-insensitive, and take the schema's spelling.
The connector's own CDC tables (ADR-0112) carry a primary key over `NULLABLE` columns unless `sink.derive-required-columns` is set, so this is what lets them resolve at all.
An upsert `INSERT INTO` a keyed catalog table takes ADR-0111's CDC path once the statement adds `'sink.cdc.enabled' = 'true'` as a hint; without it the planner refuses the upsert as it would for a hand-written table.

### Views

A logical or materialized view resolves as a `CatalogTable` carrying `scan.materialize-views = true`.
A read therefore runs a billed query job and reads its result, unless the statement opts into ADR-0089's reuse, whose job-name key and stale-result trade-off apply unchanged.
ADR-0087 made view reads opt-in because nothing tells a hand-written table's author that a view costs a query; through the catalog, naming the view is that opt-in, since the catalog knows the table is a view.
The alternative, carrying nothing, would hand the view to the Storage Read API, which refuses it (`non-table entities cannot be read with the storage API`, measured against the service on 2026-08-10 and recorded on the DataStream page) and leaves the user a service error rather than an option to set.
`INSERT INTO` a view plans and then fails when BigQuery refuses the write.
External and snapshot tables resolve by their schema, and reading or writing them does whatever the connector already does.

### Partitioning, clustering and statistics

Time and range partitioning and clustering never become Flink partition keys, because the table factory rejects `PARTITIONED BY` (ADR-0100).
The table comment is BigQuery's description, unchanged.

Statistics stay `UNKNOWN` (ADR-0168).
BigQuery's `numRows` omits rows still in the streaming buffer, so a table being written by streaming under-reports, and the row count is all the planner reads.
Reopen when a cached lookup makes the count free and the count includes buffered rows.

### Tier

`BigQueryCatalogOptions` is `@PublicEvolving`; `BigQueryCatalog`, `BigQueryCatalogFactory` and the converter are `@Internal` (ADR-0168).

## Evidence

Measured against goccy/bigquery-emulator 0.8.1 on 2026-09-27 by `BigQueryCatalogITCase`:

- A table created by the sink's DDL path with 27 columns resolved through the catalog to exactly the documented read-back types.
  The columns cover every row of the connector page's mapping the sink can create: each integer, floating-point, decimal, string, binary and temporal declaration, `ROW`, `ARRAY`, `MAP`, `MULTISET`, a JSON-marked string and `ROW`, a GEOGRAPHY-marked string, and a `NOT NULL` column.
  The sink never creates `RANGE` or `INTERVAL`, so those rows, like the refusals, are pinned by `BigQuerySchemaToFlinkConverterTest`.
  The sink created `DECIMAL(38, 20)` as `BIGNUMERIC` and the marked columns as `JSON` and `GEOGRAPHY`, asserted on the BigQuery schema, since their Flink answers cannot tell those types apart.
- The emulator kept a table's primary-key constraint and a view's schema, and the key resolved as a `NOT NULL` Flink key.
- The emulator's Storage Read serves a view directly, so a view's rows alone do not show materialization: a read with `scan.materialize-views = false` returned the same rows.
  A read through the catalog with `scan.query-result-dataset` named left the query job's `flink_bigquery_source_*` result table in that dataset, which is the evidence; with the catalog's option flipped to `false`, that test failed.
- A read through the catalog returned the same rows as a hand-written `CREATE TABLE` over the same table, and an `INSERT INTO` through the catalog landed its row.

## Alternatives declined

- **A catalog over several projects**: a Flink catalog has two levels below its name, and one project per catalog keeps `project` meaning one thing; a second project is a second `CREATE CATALOG`.
- **Failing `getTable` on a view, naming `scan.materialize-views`**: a hint cannot apply to a table that failed to resolve, so views would be unusable through the catalog.
- **`VARCHAR(n)` for a `STRING(n)` column**: the connector treats every string as `STRING`, and a length the sink ignores would only reject values the table accepts.
- **A non-null `ARRAY` for `REPEATED`**: truthful for reads, but it would turn a null array a statement inserts into a runtime failure a hand-written table does not have.

## Consequences

- `BigQuerySchemaToFlinkConverter`, the page's "Read back through the catalog as" column and `BigQueryCatalogITCase`'s round trip move together; a change to one is a change to all three.
- A new BigQuery type fails catalog lookups of tables that use it until the converter maps it.
