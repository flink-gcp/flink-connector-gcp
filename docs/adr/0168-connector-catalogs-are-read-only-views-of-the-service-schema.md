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
- Date: 2026-09-27; revised by [#1582](https://github.com/flink-gcp/flink-connector-gcp/issues/1582) and [#1214](https://github.com/flink-gcp/flink-connector-gcp/issues/1214) (2026-10-03)
- Issues: [#1212](https://github.com/flink-gcp/flink-connector-gcp/issues/1212), [#1213](https://github.com/flink-gcp/flink-connector-gcp/issues/1213), [#1582](https://github.com/flink-gcp/flink-connector-gcp/issues/1582), [#1214](https://github.com/flink-gcp/flink-connector-gcp/issues/1214)
- Modules: base, bigquery, spanner; each later connector catalog adds an adoption section here
- Current behavior: [BigQuery catalog](../content/docs/connectors/table/bigquery.md#catalog), [Spanner catalog](../content/docs/connectors/table/spanner.md#catalog)

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
  `listTables` names tables and views together, as `Catalog.listTables` specifies, where the connector can read a view; an adoption section records a catalog that lists tables only.
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

The read-only part of every connector catalog is one `@Internal` base class, `AbstractReadOnlyCatalog` in `flink-connector-gcp-base`'s `base.catalog` package, and `ReadOnlyCatalogDatabase` is the database value every catalog returns, since Flink's `CatalogDatabaseImpl` is `@Internal`.
The class holds the lifecycle and the lazily opened client, the empty view, partition, function and procedure answers, the `UNKNOWN` statistics, every refused mutation, and `listMaterializedTables` declared without `@Override`.
Those methods are `final`, so the contract above holds the same way in every connector; the two table-statistics getters are the exception, left open for the reopen condition above.
A subclass lists and resolves databases and tables, names its table factory, and keeps its service's database-name grammar.
The service's name, the read-only message, the client opener and the client closer are constructor arguments rather than overridable hooks, so the constructor calls nothing a subclass defines.
Flink 2.x's `dropModel` and `renameModel` are declared there without `@Override`, like `listMaterializedTables`; `createModel` and `alterModel` take a `CatalogModel`, a type 1.20 lacks, so the skeleton cannot declare them, and they still throw `UnsupportedOperationException` with Flink's own message.

The split follows a measurement taken when the second catalog, Spanner's ([#1214](https://github.com/flink-gcp/flink-connector-gcp/issues/1214)), was designed.
About 280 of `BigQueryCatalog`'s 590 lines were this part, and BigQuery appeared in it only as the read-only message and the service name in the client-opening failure, which became constructor arguments.
What the catalogs share is therefore the answer to Flink's `Catalog` contract, which is protocol rather than text in ADR-0083's sense, while listing, resolution, the type mapping and the name grammar differ per service and stay in each connector.
The type mappers stay per connector for the same reason: each reads a different representation of the service schema.

The class is base's first table-layer type.
ADR-0138 counted that as a cost against moving a per-module enum into base, where the shared type would have forbidden per-product values; nothing in the shared methods varies per product, and the dependency is `flink-table-common` at provided scope, which the BigQuery, Spanner and Bigtable modules already declare.
The SQL jars relocate base per product (ADR-0015).
The skeleton's signatures use only Flink and JDK types, so each jar carries its own copy and Flink sees a plain `Catalog`; `BigQuerySqlConnectorSmokeITCase` holds that by opening a catalog through the shaded factory.
The class moved into base ahead of its second consumer's merge, by [#1582](https://github.com/flink-gcp/flink-connector-gcp/issues/1582), so the Spanner catalog builds on it rather than on a copy.

### Documentation

A Table connector page carries a `Catalog` section after its schema and type mapping, which the catalog inverts, with a source-backed `CREATE CATALOG` example that stops at the catalog boundary.
The type-mapping table gains a "Read back through the catalog as" column.
The catalog's `Option` table sits in that section, and its `[[config_options]]` entry names `heading = "Catalog"`: `check-option-docs` then holds that class to the section and the connector's class to the rest of the page (a refinement of ADR-0116).

## Evidence

- The Flink facts above were read from the 1.20.4 and 2.2.1 sources and bytecode on 2026-09-27, and are pinned by `BigQueryCatalogFactoryTest` (offline `CREATE CATALOG` and `USE CATALOG`), `BigQueryCatalogPlanTest` (a hint enabling CDC on a catalog table; the same upsert refused without it) and `BigQueryCatalogTest` (the nullable-key case).
- A query through a catalog table asked `databaseExists` before `getTable`: the plan test failed with `Object 'analytics' not found within 'bq'` until its stub answered the dataset.
- `AbstractReadOnlyCatalogTest` pins the shared part: no client before the first metadata call, one client under concurrent first calls (the test fails when `client()` is not synchronized), the close-and-reopen cycle, every refused mutation, and the answers given without the client.

## Alternatives declined

- **Extending `AbstractCatalog`**: it is `@Internal`, so `check-flink-api-tiers` would need an entry for a convenience the read-only set does not need.
- **A copy of the read-only part per catalog until a third catalog lands**: two catalogs already shared it line for line, and the Bigtable and Datastore catalogs ([#1216](https://github.com/flink-gcp/flink-connector-gcp/issues/1216), [#1548](https://github.com/flink-gcp/flink-connector-gcp/issues/1548)) need the same methods, so each copy would only be a place for the contract to drift.
- **A table-layer module of its own**: it would keep `flink-table-common` out of base, at the price of a new artifact with its own shading, NOTICE and release entries for one abstract class and one value, where base already reaches every connector.
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

## Spanner adoption

[ADR-0176](0176-the-spanner-catalog-maps-an-instances-databases-and-information-schema-to-flink-tables.md) records the Spanner catalog: one instance, its databases in either dialect, a named schema's tables as canonical `schema.table` names, the `SPANNER_TYPE` mapping, generated columns kept and marked, hidden columns left out, and views and change streams not listed.
Two of its answers refine this record, and ADR-0176 gives the reason for each.
Its `listTables` names base tables only, since the connector reads through Spanner's read API, which cannot read a view; the method set's "names tables and views together" therefore holds for a catalog whose connector can read views.
It leaves out hidden columns and generated columns that are not stored, the two exceptions to "columns are never dropped".
