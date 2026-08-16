---
title: Bigtable
type: docs
weight: 40
---

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

# Write to and read from a Bigtable table

Assumes the artifacts and credentials from the
[Quickstart]({{< relref "docs/quickstart" >}}) index, and the imports an IDE resolves from the
[Java API reference]({{< param ApiDocsURL >}}).

The examples use application-default credentials.
If a deployment cannot supply the intended identity through ADC, add
`serviceAccountKeyFile("/mounted/path/key.json")` to either builder.
The path is read when the job's runtime components start and must be mounted at the same absolute
path on every eligible TaskManager and, for either source, the JobManager.
Prefer an attached service account or Workload Identity over a long-lived key; the operational
requirements are in [Credential file deployment]({{< relref "docs/connectors/datastream/bigtable" >}}#credential-file-deployment).

## Write a stream of row mutations

**Create the table and its column family first.** By default the sink creates neither: a table's
schema is its column families and their garbage-collection policies, which is the part a sink
cannot guess. (Declaring that schema on the builder instead is
[table auto-creation]({{< relref "docs/connectors/datastream/bigtable" >}}#table-auto-creation);
the instance always has to exist.)

```sh
gcloud bigtable instances create my-instance \
    --display-name="my-instance" --cluster-config=id=my-cluster,zone=asia-northeast1-a,nodes=1
gcloud bigtable instances tables create orders --instance=my-instance \
    --column-families=cf
```

{{< java-snippet file="BigtableQuickstartWrite.java" tag="bigtable-quickstart-write" >}}

Read the rows back with the `cbt` CLI:

```sh
cbt -project my-project -instance my-instance read orders
```

Two things decided in that job rather than by the sink. The **row key** is the whole access pattern
in Bigtable — reads are by key or by key range — so `order#<id>` is a choice about how the data will
be read, not a formality. And the **cell timestamp** is what makes the replay of a record after a
failure an overwrite rather than a second version of the cell; leaving it out lets the server's
clock decide, and both versions then live until garbage collection removes one.

## Read a table back

The source reads the rows of a key range and finishes. It is bounded, which is not the same as
batch-only: this job runs in streaming mode and simply ends, which is also what lets a Bigtable
table be read and joined against an unbounded stream.

{{< java-snippet file="BigtableQuickstartRead.java" tag="bigtable-quickstart-read" >}}

The job needs `bigtable.tables.readRows` and `bigtable.tables.sampleRowKeys` — `roles/bigtable.reader`
covers both — and creates nothing.

**How many subtasks read is Bigtable's decision, not the job's.** Splits come from where the service
says the table's sections begin, so a small table is read by one subtask however high the parallelism
is set; the others finish immediately.

## The same thing in SQL

The `bigtable` table connector writes the same rows from a `CREATE TABLE`. The schema is the HBase
convention: one atomic column is the row key, and every `ROW<...>` column is a column family whose
fields are its qualifiers.

```sql
CREATE TABLE orders (
  rowkey STRING,
  cf ROW<order_id STRING, amount BIGINT>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project'   = 'my-project',
  'instance'  = 'my-instance',
  'table'     = 'orders',
  'sink.insert-only-input-mode' = 'insert-only'
);

INSERT INTO orders VALUES ('order#a-1', ROW('a-1', CAST(10 AS BIGINT)));
```

The sink is upsert-shaped, so an updating query works as it stands and a delete removes the whole
row. The example selects the table-local `insert-only` compatibility mode so its plain insert runs
unchanged on Flink 1.20, 2.2 and 2.3; the default `upsert` mode exposes Flink 2.3 conflict
strategies and may require an `ON CONFLICT` clause, which the SQL connector page below explains.
For the SQL client, put the `flink-sql-connector-gcp-bigtable` uber-jar in Flink's `lib/` —
it carries the connector and its whole runtime tree, relocated. The full `WITH` surface, the cell
encodings and the type mapping are on the
[Bigtable SQL connector]({{< relref "docs/connectors/table/bigtable" >}}) page — a `SELECT` over
the same DDL reads the table back, with the scan options that page carries.

## Next

[Bigtable examples]({{< relref "docs/examples/bigtable" >}}) — several mutations per record,
deletes, a table named per record, dropping bad rows instead of failing, reading a key range, and
running against the emulator.
