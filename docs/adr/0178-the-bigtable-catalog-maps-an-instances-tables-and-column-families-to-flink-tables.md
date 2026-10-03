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

# ADR-0178: The Bigtable catalog maps an instance's tables and column families to Flink tables

- Status: Accepted
- Date: 2026-10-03
- Issues: [#1216](https://github.com/flink-gcp/flink-connector-gcp/issues/1216), [#1212](https://github.com/flink-gcp/flink-connector-gcp/issues/1212)
- Modules: bigtable
- Refines: ADR-0168's "a required `default-database`" and "no catalog option has a default", for the reasons under Scope and Key type
- Current behavior: [Bigtable catalog](../content/docs/connectors/table/bigtable.md#catalog)

## Context

ADR-0168 fixes the shape every connector catalog shares, and `AbstractReadOnlyCatalog` holds its read-only half.
This record is what Bigtable adds: what the database and a table are, how a table's metadata becomes a Flink schema, and what the catalog leaves out.

Bigtable's metadata is thinner than BigQuery's or Spanner's.
`GetTable` names a table's column families and each family's value type, and nothing inside a family: a qualifier exists only in the rows that hold it, and a raw family's cells carry no type.
ADR-0086's `ROW` DDL needs both, so no `ROW` schema can be derived from metadata alone.
FLIP-117 proposed the same catalog for Flink's HBase connector; it is still under discussion, with "How do we map column (family:qualifier) to Flink Table columns?" among its open questions, and the HBase connector ships no catalog.
ADR-0172 added the `MAP` column family for this catalog: a top-level `MAP<key, value>` column holds a whole family, one entry per qualifier, which is the shape GoogleSQL for Bigtable presents.

## Decision

### Scope

One catalog covers one instance, and the instance is the catalog's one database, named by its id.
`listDatabases` and `databaseExists` answer from the `instance` option without a request, and any other name is no database.
So `bt.my-instance.orders` names table `orders`.

The catalog takes no `default-database`, which ADR-0168 requires of a catalog whose service has a container of tables below the catalog's scope.
Bigtable's container of tables is the instance, and the `instance` option already names it; a `default-database` whose only valid value is the instance id would be a second spelling of one fact.
Naming the database after the instance, rather than a fixed literal such as `default`, keeps every table name valid if a later catalog lists a project's instances as its databases.
This catalog stays at one instance because the emulator, which its integration tests and the documentation harness run against, has no instance admin API.

`listTables` is `BigtableTableAdminClient.listTables()`, every table of the instance.
A name outside the documented table-id grammar, `[_a-zA-Z0-9][-_.a-zA-Z0-9]*` up to 50 characters, is no table and is answered without a request, which also keeps a `/` out of the resource name.
The catalog does not check that the instance exists.
A table lookup maps the service's `NOT_FOUND` to a missing table, which on a missing instance is what `GetTable` answers; while validating a query the planner then lists the database's tables, and the listing's failure names the instance with the service's `NOT_FOUND`.
The first `SHOW TABLES` or query therefore reports the instance, which is why the catalog spends no request on the instance itself; `DESCRIBE`, which resolves through Flink's catalog manager without listing, reports the table as not existing.

### Schema

A table resolves as a `_key` row key column, then one `MAP` column per column family, named after it, in name order.
The family order is sorted because the admin API returns families in a protobuf map, whose order is no contract, and a schema whose column order changed between two lookups would change a statement's plan.

The row key column is `_key`, GoogleSQL for Bigtable's name.
`rowkey`, which Flink's HBase connector documents and BigQuery's Bigtable external tables use, was the other candidate.
`_key` won because, under the `string` key type below, GoogleSQL's own example query, `SELECT address['street'], address['city'] FROM myTable WHERE _key = 'user1'`, runs in Flink unchanged; the column name is the part of a GoogleSQL query a Flink schema can match.
`_key` is the `PRIMARY KEY ... NOT ENFORCED` and therefore `NOT NULL`, as ADR-0168 makes a service key; a Bigtable row key is unique and never null, and the declaration lets a delete carry the key alone.
A family named `_key` would need the row key's column name, so `getTable` fails naming the table and the family rather than renaming either; the emulator accepts such a family, and a hand-written table can name its row key otherwise.

A family's value type decides the map's value type:

- **`BIGINT`** for a `sum`, `min` or `max` aggregate whose input type, and state type when the service reports one, is a signed 64-bit integer in big-endian bytes; these aggregators keep their state in their input type. An `int64` with no encoding set counts as big-endian, as the sink's family-type check reads it, unless the encoding carries an unknown field: the pinned protobuf parses an encoding newer than itself that way, and the family then reads as `BYTES`. This is the codec's `BIGINT` layout, so the map reads aggregate state as ADR-0172 measured on real Bigtable.
- **`BYTES`** for every other family: a raw family, an HLL sketch, an `int64` in ordered-code bytes, and any aggregator or type this mapping does not know.
  An HLL family is `BYTES` whatever state type it reports. The pinned admin protobuf leaves HLL's state "TBD" and names `int64` as a "special state conversion" to the count estimate, which reading the cell does not perform; as `BIGINT`, the default `decode.trailing-bytes = ignore` would read each sketch's first eight bytes as a number without an error.

The second rule refines ADR-0168's "a column whose type the connector cannot carry fails `getTable`".
`BYTES` returns a cell's stored bytes and decodes nothing, so it cannot misread a family whose type is unknown; failing the table would make every family of it unreadable through the catalog to protect one that `BYTES` already reads faithfully.
The one conversion it keeps is the connector's null convention: an empty cell reads as `NULL`, as it does for every nullable `BYTES` value, which every catalog map value is.

### Key type

The catalog option `key-type`, `bytes` or `string`, types `_key` and every map key, the qualifier.
`bytes`, the default, keeps the stored bytes, which suits any key and matches GoogleSQL's and BigQuery's external-table default.
`string` reads them as UTF-8 without validating them, through the codec's `StringData.fromBytes`, so a key that is not UTF-8 keeps its bytes through a read and a write back.
Map values stay `BYTES` or `BIGINT` under either: a string value is one `CAST` away, and a typed value is what a hand-written table declares.

Flink does not coerce a string literal to `BYTES`, which GoogleSQL does.
Under `bytes`, `_key = 'user1'` and `address['street']` fail validation, and a query spells the bytes with `CAST('user1' AS BYTES)` or `x'...'`; the planner folds the cast to a literal, so the predicate still narrows the scan.
Bigtable's schema design guidance recommends human-readable string row keys, so many tables want `string`, and a schema is not something an `OPTIONS` hint can change.

The option therefore belongs to the catalog, and it has a default, which refines ADR-0168's "no catalog option has a default".
That rule keeps tuning out of the catalog, since a hint covers tuning per statement; `key-type` decides the schema itself, which no hint reaches.
Its default is the type that cannot misread a key.

### Options

A resolved table carries `connector`, `project`, `instance` and `table`, and the catalog's `service-account-key-file` and `emulator-endpoint` when set, under the connector's keys.
`key-type` is not carried: it shapes the schema, which the connector reads from the table's columns.
Every other option is per statement, as ADR-0168 decides.

### Change streams, views and aggregate writes

Change Streams tables are not listed.
A Change Streams table's schema is the fixed envelope (`BigtableChangeStreamEnvelopeSchema.DATA_TYPE`) or the selected-cell shape, and the factory rejects any other physical schema, so the hint that turns a Spanner catalog table into its change-stream source cannot turn this one.
A second listed name per table would invent a naming convention and choose the changelog mode, which ADR-0106 makes the user's explicit choice.
Authorized views and materialized views are not listed either: `listTables` does not return them, and the connector reads tables.

The catalog table reads aggregate state.
Contributing to an aggregate family takes an aggregate input table, which ADR-0156 keeps sink-only and which declares every physical family's aggregate type; the catalog emits no `sink.aggregate.column-family-types`, which the source path rejects.

### Requests and statistics

The client is a `BigtableTableAdminClient` with table-admin scopes only, opened on the first table request as ADR-0168 requires.
A table's families are read through its protobuf client with `SCHEMA_VIEW`, as the sink's table admin reads them, so a family type the client's model classes do not know still reaches the mapping.
Statistics are `UNKNOWN`: Bigtable's metadata carries no row count a planner can use without a scan.

### Tier

`BigtableCatalogOptions` and its `CatalogKeyType` enum are `@PublicEvolving`, as ADR-0168 decides for a catalog's options; the catalog, its factory and its client are `@Internal`.

## Evidence

Measured on 2026-10-04 against real Bigtable (project `flink-gcp`, one ephemeral one-node SSD instance in `us-central1-b`, Flink 2.2.1), by `BigtableCatalogRealGcpITCase` and `BigtableCatalogMissingInstanceRealGcpITCase`:

- The service reports a state type for every aggregate family it creates: `int64` in big-endian bytes for `int64-sum`, `int64-min` and `int64-max`, matching their input type, and raw `bytes` for `int64-hll`.
  The catalog's rule decides HLL by its aggregator all the same, since the proto names an `int64` state conversion for it.
- Through the catalog, `DESCRIBE` shows `MAP<STRING, BIGINT>` for the three integer families and `MAP<STRING, BYTES>` for the raw and HLL families under `string` keys, and contributions 3 and 5 read back as sum 8, min 3 and max 5, with the HLL state a 25-byte sketch rather than an eight-byte count.
- For an instance that does not exist, `ListTables` answers `NOT_FOUND: Failed to read: projects/{…}/instances/<id>`, and so does `GetTable`: a `SELECT` naming a table fails with the listing's message, because the planner lists the tables after the lookup finds none. `BigtableCatalogTest` holds this offline: `aQueryOnAMissingTableReportsAFailingListing`, `aDescribeOfAMissingTableReportsTheTableWithoutListing` (a single lookup, no listing), and `aMissingTableIsTableNotExist` (the catalog itself asks only for the table).

Measured on 2026-10-03 against Flink 2.2.1 and 1.20.4 and the Bigtable emulator:

- The emulator answers `ListInstances` with `UNIMPLEMENTED: unimplemented feature`, which is why the catalog stays at one instance. The status came from the emulator over a plaintext channel; the client does offer an emulator builder for instance admin (`BigtableInstanceAdminSettings.newBuilderForEmulator`), so the limit is the emulator's, not the client's.

- `BigtableCatalogITCase` lists and describes the instance's tables with `key-type` unset, reads a table through the catalog with `CAST` as the equivalent `ROW` DDL reads it, writes through a catalog table, plans a lookup join against one, runs GoogleSQL's overview query unchanged under `string`, and copies a row whose key is `0xFF 0x00 0x80 'k'` and whose qualifier is `0xC3 0x28 'q'` through `INSERT INTO ... SELECT` under `string` with both byte strings intact.
  It also holds the planner facts the docs state: `_key = CAST('user1' AS BYTES)` is pushed as a row-key filter, `_key = 'user1'` and `cf['name']` are refused on `BYTES` keys, and under `string` `_key = 'user1'` and `_key >= 'user' AND _key < 'uses'` are pushed while `_key LIKE 'user%'` is evaluated after a full scan.
  It refuses a table with a family named `_key`, and reads an empty cell as a `NULL` map value under a present qualifier key.
- A standalone planner probe, run once while deciding the key type, found `LIKE` on `BYTES` refused as `=` is, and `STARTS_WITH` and `SAFE_CONVERT_BYTES_TO_STRING` missing from Flink; no test holds these, since the docs only name the functions as GoogleSQL's.
- `BigtableCatalogSchemaTest` holds the value-type rules against the admin protobuf the client builds for each aggregate and against hand-built ones for an unset encoding, a reported state and an HLL family reporting an `int64` state, since the emulator cannot create an aggregate family.
- `BigtableCatalogFactoryTest` holds the offline `CREATE CATALOG` and `USE CATALOG`, which also answer `SHOW DATABASES`, and the rejection of `default-database` and tuning keys as unknown.

## Alternatives declined

- **`rowkey` as the row key column**: it matches the HBase connector's examples and BigQuery's external tables, but not the SQL Bigtable itself offers, and under `string` keys only `_key` lets a GoogleSQL query run unchanged.
- **`STRING` keys only, or `BYTES` keys only**: `STRING` misprints binary keys and lets a reader mistake them for text, and `BYTES` makes every predicate and entry access spell its bytes; the choice is per instance, so it is an option.
- **Failing `getTable` for an unknown family type**: `BYTES` reads such a family faithfully, as the Schema section argues.
- **Sampling rows to infer qualifiers**: declined by the issue's design note (2026-09-29). Qualifiers differ from row to row, so a sample would show only the sampled rows' qualifiers, and a raw cell's type could only be guessed from its bytes. A map column needs neither, since each row's qualifiers are its keys.
- **A fixed database name**, or **a `default-database` option**: see Scope.
- **Listing change streams as a second name per table**: see Change streams, views and aggregate writes.

## Consequences

- An instance's tables are usable from SQL without hand-written DDL, as maps; a query that wants typed qualifiers still declares a `ROW` or typed `MAP` table.
- A table whose rows hold a family named `_key` is reachable only through a hand-written table.
- Every statement naming a catalog table makes one `GetTable` request per lookup at planning; `CREATE CATALOG`, `USE CATALOG` and `SHOW DATABASES` make none.
