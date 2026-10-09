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

# ADR-0182: BigQuery table description and labels are creation options

- Status: Accepted
- Date: 2026-10-08
- Issues: [#1668](https://github.com/flink-gcp/flink-connector-gcp/issues/1668)
- Modules: bigquery, docs-validation
- Current behavior: [Table metadata](../content/docs/connectors/datastream/bigquery.md#table-metadata),
  [Table creation (SQL)](../content/docs/connectors/table/bigquery.md#table-creation)

## Context

`TableCreateOptions` covered partitioning, expiration and clustering, but not a table's description
or labels.
A pipeline that wanted described or labelled tables had to build its own BigQuery client and call
the Tables API itself, racing the sink's own creation.

[#1668] also asked for an opt-in that would add a missing description and labels to tables that
already exist, during "the schema reconciliation that already reads the table".
That premise holds for `FILE_LOADS` only, whose commit reads every destination table.
Both Storage Write API writers reconcile only when the serializer fingerprint changes or an append
reports a schema mismatch; the first write to a destination opens a stream without the connector
reading the table's metadata.

## Decision

`TableCreateOptions` gains `description(String)` and `labels(Map<String, String>)`.
Like the options already there, they apply only when the sink creates a table, under every write
method, and an existing table is never changed by them.
CDC creation merges the labels with its provisioning label rather than replacing them.
The SQL keys are `sink.table-create.description` and `sink.table-create.labels`, members of the
`sink.table-create.*` family, so beside an explicit `create-never` they are rejected like the rest.

Label keys and values get presence and blankness checks only, under
[ADR-0127](0127-a-configured-name-is-checked-for-what-this-project-will-do-with-it.md).
BigQuery's character, length and count rules are BigQuery's answer, given at creation with the
destination named.
The cost is when that answer arrives: as early as a missing table's first record and as late as the
next commit (the next checkpoint, or the end of a batch job), which for a destination resolver that
opens a new table later is only when that table first receives rows.
The owner chose that over a copy of BigQuery's rules that could go stale; the docs state the
timing.
The key `flink_gcp_cdc` is refused, because CDC table provisioning owns it and a configured value
would either be overwritten or break that protocol, the same reason ADR-0127 gives for the reserved
Cloud Tasks headers.

`TableCreateOptions` keeps `serialVersionUID = 1L`, and an instance serialized before the labels
existed reads them as empty rather than `null`, as `BufferedStreamOptions` does for its later field.

## Evidence

Measured on 2026-10-08 against real BigQuery with `BigQueryTableMetadataRealGcpITCase`: a table
created by the default-stream writer, by the exactly-once writer, and by a `FILE_LOADS` commit
before a direct load under `WRITE_APPEND` or `WRITE_TRUNCATE`, carried the configured description
and labels and the serializer's column description after the job.
So the truncating load, which replaces the table's schema, keeps the table's description and
labels.

Measured on 2026-10-09 with `BigQueryLoadJobRunnerRealGcpITCase`: the `CREATE_NEVER`/
`WRITE_TRUNCATE` copy that finishes a commit too large for one load job, from unpartitioned
temporary tables into a final table the admin created with a description and labels, replaced the
rows and kept both.
The same copy into a column-partitioned final table was refused ("Failed to copy Non partitioned
table to Column partitioned table: not supported"), and into a clustered one ("incompatible
clustering fields"), under `WRITE_APPEND` as well: the temporary tables carry neither setting.
That predates this decision and concerns `FILE_LOADS`' temporary-table path rather than the
metadata; it is recorded here because the measurement found it, and tracked in
[#1671](https://github.com/flink-gcp/flink-connector-gcp/issues/1671).

## Consequences

Column descriptions are not a creation option.
They travel with the serializer's `TableSchema` into created tables and appended columns, and
reconciliation keeps existing columns' descriptions, except under `FILE_LOADS` `WRITE_TRUNCATE`,
whose load replaces the schema wholesale; the protobuf, Avro and `RowData` serializers derive none.
SQL column `COMMENT`s do not become BigQuery column descriptions.

A pipeline that wants metadata on a table that already exists sets it outside the sink, as before.

## Alternatives declined

- **An opt-in that fills a missing description and labels on existing tables**, as [#1668] asked.
  It was built and reviewed (a field-limited, etag-conditioned `tables.patch` once per destination
  per writer) and then removed before merge, on the owner's decision that options named for
  creation apply at creation only. Its costs were what made that the better trade:
  - it needs `bigquery.tables.get` and `bigquery.tables.update` on every destination, where an
    append-only writer needs neither, so a least-privilege job would fail on a table it could write;
  - it adds one `tables.get` per destination per subtask on the Storage Write API paths, repeated
    on every restart, and every subtask races the others to patch the same table;
  - it gives "creation options" a second meaning that a reader of the name would not expect, and
    turns `create-never` from "no `sink.table-create.*` option applies" into a case-by-case rule.
- **Applying the description and labels to existing tables without an opt-in.** Every cost above,
  imposed on every job that configures them.
- **Copying BigQuery's label grammar.** Declined under ADR-0127: it would go stale in the direction
  that refuses a label BigQuery accepts.

[#1668]: https://github.com/flink-gcp/flink-connector-gcp/issues/1668
