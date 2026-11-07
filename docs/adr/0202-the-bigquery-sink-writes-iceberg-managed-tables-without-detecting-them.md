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

# ADR-0202: The BigQuery sink writes Iceberg managed tables without detecting them

- Status: Accepted
- Date: 2026-11-07
- Issues: [#1994](https://github.com/flink-gcp/flink-connector-gcp/issues/1994),
  [#2089](https://github.com/flink-gcp/flink-connector-gcp/issues/2089)
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Iceberg managed tables

## Context

BigQuery's Iceberg managed tables (formerly BigLake tables for Apache Iceberg in BigQuery) keep
their data as Parquet files and Iceberg metadata in a Cloud Storage bucket of the user's, written
through a Cloud resource connection.
Google documents restrictions that meet several of this sink's paths: no copy jobs, no use as a
query-result destination, loads that only append and never update the schema, no `TRUNCATE`, and
no `JSON`, `RANGE`, `GEOGRAPHY`, `BIGNUMERIC` or `INTERVAL` columns.
It says nothing about which Storage Write API stream types reach these tables, and it lists change
data capture into them as a Preview.

A second kind of Iceberg table lives in the Lakehouse runtime catalog (formerly BigLake metastore)
and is served through the Apache Iceberg REST catalog.
Google's comparison table marks the Storage Write API as not supported for those tables and open
source engine writes as GA, so they are written through the Apache Iceberg Flink connector.

## Evidence

Measured on 2026-11-07 in `us-central1`, first with the raw clients against tables created through
the connection that [#2089](https://github.com/flink-gcp/flink-connector-gcp/issues/2089)
provisions, then through the connector by `BigQueryIcebergManagedTableRealGcpITCase`.

- Accepted: the default stream, `COMMITTED` and `PENDING` streams, a `BUFFERED` stream with
  `FlushRows`, a `WRITE_APPEND` load, `INSERT ... SELECT`, `tables.update` adding a column or
  relaxing `REQUIRED` to `NULLABLE`, a description and labels, and a Storage Read API session.
  Through the connector, `STORAGE_API_AT_LEAST_ONCE`, `STORAGE_API_EXACTLY_ONCE` and `FILE_LOADS`
  with `WRITE_APPEND` wrote rows that the connector's source read back.
- Refused, each with a message naming "BigQuery tables for Apache Iceberg": a load with schema
  update options, `WRITE_TRUNCATE`, `WRITE_TRUNCATE_DATA`, `WRITE_EMPTY` (even into an empty
  table), any copy job, a query with the table as its destination, `TRUNCATE TABLE`, and a `JSON`
  column at creation.
- Change data capture: the Storage Write API refused the default stream carrying `_CHANGE_TYPE`
  with "Create UPSERT stream is not supported for BigQuery tables for Apache Iceberg", although the
  table had a primary key and `max_staleness`; no row was written.
  The same rows into a standard table applied as documented, including a `DELETE` whose sequence
  number is higher than a later-arriving `UPSERT`.
- A load with `CREATE_IF_NEEDED` into a missing table created a standard table, not an Iceberg one.
- The dataset's default table expiration applies to an Iceberg managed table.

## Decision

**The sink treats an Iceberg managed table like any other existing table.**
It does not read the table's `biglakeConfiguration`, does not refuse a write method or disposition
for it, and does not create one.

- Every refusal above fails loudly with the service's own message, at the job that uses the path.
  The connector would only restate those messages, and a restated rule goes stale when the service
  widens what it accepts.
- The user creates the table.
  When it is missing, the sink's auto-creation makes a standard table without saying so, which is
  why the docs say to create it first.
- Change data capture is refused by the service today.
  The silent case Google documents for these tables, a `DELETE` arriving before its row being
  discarded because no delete retention window is kept, cannot happen while the stream is refused.
  A service change that admits the stream reopens it, and the gated case that asserts the refusal
  is what shows that change.
- Tables in the Lakehouse runtime catalog are outside this connector.

## Consequences

- The docs carry the measured table, dated, and the gated suite carries one case per accepted
  write method plus one per refusal the docs rely on (a truncating load and change data capture).
  A refusal case that starts failing means the service changed, and the docs change with it.
- A `FILE_LOADS` commit that overflows into temporary tables fails on its copy or query, which can
  happen first on a large commit after many small ones succeeded.
  The docs say so rather than the connector refusing every `FILE_LOADS` write.
- The E2E workflow passes two more variables, `BQ_IT_ICEBERG_CONNECTION` and
  `BQ_IT_ICEBERG_BUCKET`, naming the resources in `opentofu/flink-gcp/bigquery-iceberg.tf`.

## Alternatives declined

- **Detect the table at startup and refuse the paths that cannot work.** It costs a `tables.get`
  per destination and a rule list to keep in step with the service, for failures the service
  already reports clearly. Declined on the owner's direction that loudly failing service rules are
  the user's to adjust.
- **Create Iceberg managed tables.** Both kinds of Iceberg table were still gaining capabilities in
  2026; creation would add a connection, a storage URI and a table-format choice to every
  creation path for a table the user can create once with DDL.
- **Support Lakehouse runtime catalog tables.** The Storage Write API does not reach them.
