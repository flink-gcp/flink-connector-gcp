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

# ADR-0168: Connector catalogs are read-only views of the service schema

- Status: Accepted
- Date: 2026-09-27
- Issues: [#1212](https://github.com/flink-gcp/flink-connector-gcp/issues/1212), [#1213](https://github.com/flink-gcp/flink-connector-gcp/issues/1213)
- Modules: bigquery; each later connector catalog adds an adoption section here
- Current behavior: [BigQuery catalog](../content/docs/connectors/table/bigquery.md#catalog)

## Context

Every Table API use of the BigQuery, Spanner and Bigtable connectors starts with a hand-written `CREATE TABLE` that restates a schema the service already holds.
FLIP-93 described the same cost for relational databases, and the JDBC connector answered it with a read-only catalog that lists databases and tables and derives each table's schema and options from the live database.
[#1212](https://github.com/flink-gcp/flink-connector-gcp/issues/1212) adds one such catalog per schema-bearing connector, and this record is the shape they share, so the second catalog follows the first rather than negotiating it again.

Four facts about Flink decide most of that shape, measured against flink-table-common and flink-table-api-java 1.20.4 and 2.2.1 on 2026-09-27.

- `Catalog`, `CatalogFactory`, `CatalogTable.newBuilder()` and the catalog exceptions are `@PublicEvolving` and have the same signatures on both lines, so one source root serves both.
  The differences are additive: 1.20 declares `getTableFactory()` and `supportsManagedTable()` as deprecated defaults, 2.2 adds `listMaterializedTables(String)` (throwing by default) and the model methods, and `CatalogFactory.createCatalog(Context)` is a default on 1.20 and abstract on 2.2.
- `AbstractCatalog`, `CatalogDatabaseImpl` and `CommonCatalogOptions` are `@Internal`.
- `CREATE CATALOG` calls only the factory and `open()`, and `USE CATALOG` only `getDefaultDatabase()`.
  A query's name resolution calls `databaseExists`, then `getTable`, then `getTableStatistics` and `getTableColumnStatistics` for every permanent table, with no cache, and repeats the lookup within one statement.
  After `USE CATALOG`, every function name that is not built in is looked up with `getFunction`, and `CatalogManager` catches only `TableNotExistException`, `DatabaseNotExistException` and `FunctionNotExistException` on those paths.
- The planner creates a catalog table's source and sink through the catalog's `getFactory()`, merges `OPTIONS` hints over the table's options (`table.dynamic-table-options.enabled` is on by default on both lines), and rejects a primary key over a nullable column.
  Compiled-plan restore, `CREATE TABLE ... LIKE` and `SHOW CREATE TABLE` still discover the factory from the `connector` option.

## Decision

### Method set

A connector catalog implements `Catalog` directly, the JDBC catalog's read-only set.

- `listDatabases`, `getDatabase`, `databaseExists`, `listTables`, `getTable` and `tableExists` answer from the service.
  `listTables` names tables and views together, as `Catalog.listTables` specifies.
- `getTable` returns a `CatalogTable`, never a `CatalogView`.
  A service view is defined in the service's SQL, which Flink cannot expand, so `listViews` returns an empty list after confirming the database exists.
  How a connector reads a view is its adoption section's decision.
- Functions, procedures, partitions and `listMaterializedTables` answer empty, `false` or the matching not-exist exception without a request.
  `listMaterializedTables` is declared without `@Override`, because 1.20 has no such method; so is any other method only one supported line declares.
- Every mutating method, including the statistics setters, throws `UnsupportedOperationException`.
  Tables are created by the sinks' own creation paths and by the service.

A missing table is `TableNotExistException` and a missing database `DatabaseNotExistException`, the only two the planner handles; any other service failure is a `CatalogException` carrying the cause.

### Factory, options and tables

The catalog's `'type'` is the connector's factory identifier, as the JDBC connector uses `jdbc` for both; factory discovery tells the two apart by factory type.
`getFactory()` returns the connector's `DynamicTableFactory`.

The catalog's options are `ConfigOption`s on a `*CatalogOptions` class beside the connector's `*ConnectorOptions`.
They mirror the connector's identity, credential and emulator options under the same keys (ADR-0137), minus the table, plus a required `default-database` naming the service's container of tables.
The class declares its own keys rather than reusing the connector's constants, because their meaning differs (a catalog's `project` is the one listed, not one destination's owner) and `check-option-docs` reads key literals.
No catalog option has a default or restates one (ADR-0139).

`getTable` emits the connector's identity options, the table's location, and the catalog's credential and emulator options under the connector's keys, including `connector` for the paths that discover the factory from it.
Scan, lookup and sink tuning stays per statement, through `OPTIONS` hints, which also override a carried value.
A tuning key in `CREATE CATALOG` is rejected as unknown: a catalog-level default would configure statements that never mention it.

The table's schema is the inverse of the connector's documented type mapping, one Flink type per service type.
A service primary key becomes a `PRIMARY KEY NOT ENFORCED` whose columns are made `NOT NULL`, as a primary key in Flink DDL makes them; without that the resolver rejects any service key over a nullable column.
A column whose type the connector cannot carry fails `getTable` with a message naming the table, the column path and the service type.
The table never resolves with the column dropped, because a schema missing a column reads as a table that lacks it.

### No request before a lookup

The factory validates options only, and `open()` builds no client and loads no credentials; the client is built on the first metadata call.
An application-default-credentials lookup can probe the metadata server, so a catalog that resolved credentials at `open()` would make `CREATE CATALOG` depend on the network.
This is also what lets ADR-0144's documentation harness execute a `CREATE CATALOG` example with no GCP access.

### Statistics

Table and column statistics are `UNKNOWN`.
The planner reads only a row count from them, asks on every lookup, and a request per lookup would double the metadata calls of every statement.
A connector may fill them from metadata its `getTable` request already returned, if a cached lookup makes that free and the service's count is one a planner can trust; that is the reopen condition, recorded per connector.

### Tier

The `*CatalogOptions` class is `@PublicEvolving`: its keys are the user contract, as for the table options ADR-0124 kept at that tier, and ADR-0141's youth clause holds a surface that has not yet survived a release there.
The catalog and its factory are `@Internal`, as the table factories are.
SQL reaches them through `CREATE CATALOG`, and Java through `TableEnvironment.createCatalog` with a `CatalogDescriptor`, so neither class needs a public constructor contract.

### Shared code

No shared base class exists yet.
`AbstractCatalog` is `@Internal`, and `flink-connector-gcp-base` has no table-layer dependency (ADR-0138).
Each catalog carries its own small `CatalogDatabase` value.
When the second catalog lands, its author measures what the two copies share as protocol rather than as text, as ADR-0083 did, and records the answer here.

### Documentation

A Table connector page carries a `Catalog` section after its schema and type mapping, which the catalog inverts, with a source-backed `CREATE CATALOG` example that stops at the catalog boundary.
The type-mapping table gains a "Read back through the catalog as" column.
The catalog's `Option` table sits in that section, and its `[[config_options]]` entry names `heading = "Catalog"`: `check-option-docs` then holds that class to the section and the connector's class to the rest of the page (a refinement of ADR-0116).

## Evidence

- The Flink facts above were read from the 1.20.4 and 2.2.1 sources and bytecode on 2026-09-27, and are pinned by `BigQueryCatalogFactoryTest` (offline `CREATE CATALOG` and `USE CATALOG`), `BigQueryCatalogPlanTest` (a hint enabling CDC on a catalog table; the same upsert refused without it) and `BigQueryCatalogTest` (the nullable-key case).
- A query through a catalog table asked `databaseExists` before `getTable`: the plan test failed with `Object 'analytics' not found within 'bq'` until its stub answered the dataset.

## Alternatives declined

- **Extending `AbstractCatalog`**: it is `@Internal`, so `check-flink-api-tiers` would need an entry for a convenience the read-only set does not need.
- **Returning a `CatalogView` for a service view**: its expanded query must be Flink SQL, and no connector here can translate the service's SQL.
- **Catalog-level tuning defaults**: [#1212](https://github.com/flink-gcp/flink-connector-gcp/issues/1212) excluded them; hints cover tuning, and a default would be a second place a statement's configuration comes from.
- **Dropping the primary key when a key column is nullable**: safe for reads, but it would keep the connector's own CDC tables, whose key columns are nullable by default, off the CDC path through the catalog.
- **A separate page for catalog options**: the precedent (`bigtable-functions.md`) fits a separate surface, but a catalog is read beside its connector's type mapping and table options, and the heading scope costs one optional key in the checker.

## Consequences

- A catalog table behaves as the equivalent hand-written table, and a statement tunes it with hints.
- Every statement naming a catalog table makes metadata requests at planning; `CREATE CATALOG` and `USE CATALOG` make none.
- A new catalog follows this record and adds an adoption section; a deviation is a revision here.

## BigQuery adoption

[ADR-0169](0169-the-bigquery-catalog-maps-a-projects-datasets-and-rest-schemas-to-flink-tables.md) records the BigQuery catalog: one project, datasets as databases, the type mapping, views resolved as tables that materialize, and statistics declined.
