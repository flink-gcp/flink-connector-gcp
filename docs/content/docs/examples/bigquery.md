---
title: BigQuery
type: docs
weight: 10
---

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

# BigQuery examples

Starting from the [BigQuery quickstart]({{< relref "docs/quickstart/bigquery" >}}) job.

## Debezium PostgreSQL CDC from Kafka

Both examples consume Kafka values containing complete Debezium PostgreSQL Avro envelopes and write
the current row to a BigQuery table with a primary key.
They fail rather than use `source.ts_ms` when the PostgreSQL ordering metadata is absent or
contradictory.

### Kafka source

The examples use Flink's Kafka connector and
`ConfluentRegistryAvroDeserializationSchema.forGeneric(...)` to retain the complete Debezium
envelope, including `before`, `after`, `op`, and `source`.
Pass the Avro schema for that complete envelope as `debeziumEnvelopeSchema`:

{{< java-snippet file="BigQueryExamplesDebeziumPostgreSqlCdc.java" tag="bigquery-debezium-postgresql-cdc-kafka-source" >}}

For a Debezium tombstone, the Confluent Avro deserializer returns `null` and Kafka's value-only
wrapper emits no element.
Flink checkpoint restoration takes precedence over the configured initial offsets, so a restarted
job resumes the Kafka positions stored in its checkpoint.
Add a Kafka connector release compatible with the application's Flink version and
`flink-avro-confluent-registry` to the job artifact.
The compiled example uses Kafka connector `3.4.0-1.20` with Flink 1.20 and `5.0.0-2.2` with Flink
2.2 and 2.3; this repository does not ship either dependency in its connector artifacts.

### DataStream API

Pass the `KafkaSource<GenericRecord>` above as `kafkaSource`:

{{< java-snippet file="BigQueryExamplesDebeziumPostgreSqlCdc.java" tag="bigquery-debezium-postgresql-cdc-datastream" >}}

The adapter selects `after` for snapshot, create and update operations and `before` for a delete.
The serializer passes only that nested row to `AvroRecordSerializer`; the two CDC providers still
receive the complete envelope and derive the operation and sequence from it.
The example uses the default stream because BigQuery CDC is supported only by
`STORAGE_API_AT_LEAST_ONCE`.

### SQL sink through a DataStream bridge

**NOTE:** There is intentionally no Kafka source-table DDL in this example.
Flink's `debezium-avro-confluent` format creates the correct changelog row kinds but does not retain
`source.sequence` or `source.lsn` in its output.
The raw `avro-confluent` format retains the envelope but exposes it as insert-only rows, so SQL DDL
alone cannot produce both the Debezium changelog semantics and the source metadata required by this
BigQuery CDC profile.
Declaring that raw envelope as a source table does not close the gap.
A `CASE` expression can select `before` or `after` from its `op` value, but SQL expressions cannot
change an INSERT RowKind into the DELETE RowKind required for a Debezium delete.
[Issue #706](https://github.com/laughingman7743/flink-connector-gcp/issues/706) tracks the upstream
format improvement.

#### Source changelog view

Until that is available, pass the `KafkaSource<GenericRecord>` above to a DataStream adapter and
register its output as the `source_changes` changelog view on the `tableEnv` that will execute the
sink DDL and `INSERT` statement:

{{< java-snippet file="BigQueryExamplesDebeziumPostgreSqlCdc.java" tag="bigquery-debezium-postgresql-cdc-sql-bridge" >}}

The bridge emits the physical columns with Flink row kinds and a source-properties map.
This registered view is the source relation for the remaining SQL statements.

#### Sink table

Define the BigQuery sink table and map the source-properties column to writable metadata:

```sql
CREATE TABLE current_orders (
  id STRING NOT NULL,
  amount BIGINT,
  source_properties MAP<STRING, STRING>
    METADATA FROM 'debezium-source-properties',
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'dataset' = 'analytics',
  'table' = 'current_orders',
  'sink.cdc.enabled' = 'true',
  'sink.create-disposition' = 'create-if-needed',
  'sink.cdc.max-staleness' = '10 min',
  'sink.cdc.table-reconciliation' = 'reconcile'
);
```

#### Insert query

Forward the changelog rows and their ordering metadata from the source view into the sink table:

```sql
INSERT INTO current_orders
SELECT id, amount, source_properties FROM source_changes;
```

The job's checkpoint restores Kafka offsets after a Flink failure.
Records between the restored offset and the last successful BigQuery append can be replayed, but
the same Debezium event produces the same change sequence number.

The provider transcodes Debezium's decimal `sequence` pair into the hexadecimal sections required
by BigQuery; it does not parse PostgreSQL's customary `X/Y` display form.
PostgreSQL defines an LSN as a 64-bit WAL byte position, while Debezium defines `source.sequence`
as the last committed LSN followed by the current LSN.
Using both positions preserves transaction progress when adjacent operations share a current LSN.
It does not create a unique total order for multiple events that share both positions.
If distinct changes for the same BigQuery primary key can have an identical pair and can arrive out
of order, provide an application-specific stable tie-breaker through the formatted
`change-sequence-number` metadata or a custom `CdcSequenceNumberProvider`.
See the [PostgreSQL `pg_lsn` type](https://www.postgresql.org/docs/current/datatype-pg-lsn.html),
[Debezium PostgreSQL source metadata](https://debezium.io/documentation/reference/stable/connectors/postgresql.html),
and [BigQuery CDC ordering format](https://cloud.google.com/bigquery/docs/change-data-capture).

### Envelope adapter

Both examples use the following adapter helpers.
The DataStream path calls the connector's
`DebeziumPostgreSqlCdcSequenceNumberProvider`, so it applies the same strict sequence parser as the
SQL sink profile rather than maintaining a second implementation in the application.

{{< java-snippet file="BigQueryExamplesDebeziumPostgreSqlCdc.java" tag="bigquery-debezium-postgresql-cdc-adapter" >}}

This example serializes the complete `before` record for a delete, so set the PostgreSQL table to
`REPLICA IDENTITY FULL` to make that complete row image available.
A different adapter that serializes only the declared BigQuery primary key for deletes can use
PostgreSQL's default replica identity when the source table has that primary key.

Both APIs require the Debezium connector to continue the same preserved or synchronized logical
replication slot across a PostgreSQL primary failover.
A recreated or discontinuous slot starts another LSN history with no ordering epoch, which neither
example can make safe; see the
[BigQuery table CDC contract]({{< relref "docs/connectors/table/bigquery" >}}#change-data-capture).

## A table per day

The writer context carries the record's event timestamp, which makes time-based routing expressible without the record carrying the routing key.
The [dynamic destinations guide]({{< relref "docs/examples/dynamic-destinations" >}}#bigquery-tables) defines the shared resolver contract and compares its resource lifetime with the other sinks.
This resolver caches one destination per UTC day and falls back to the record's own timestamp when the writer context has none.

{{< java-snippet file="BigQueryExamplesTablePerDay.java" tag="bigquery-examples-table-per-day-resolver" >}}

{{< java-snippet file="BigQueryExamplesTablePerDay.java" tag="bigquery-examples-table-per-day-sink" >}}

Two things need planning when a resolver keeps producing new destinations.
The default-stream and buffered-stream methods hold one writer per active destination, so `DefaultStreamOptions` and `BufferedStreamOptions` expose `destinationIdleTimeout` (one hour by default) to bound that local state.
FILE_LOADS has no destination idle timeout and retains each destination's conversion state until the writer closes, although it finishes the open staging file at every commit preparation.
Every new table is also created on its first record under the default create disposition, so [table auto-creation](#table-auto-creation) applies to every day this produces, not only the first.

## Exactly-once

Two of BigQuery's three write methods are exactly-once, and they trade against each other rather
than one being better. (Pub/Sub and Cloud Tasks are at-least-once with no exactly-once path — those
services have no transactional publish.)

Both need streaming checkpointing in `CheckpointingMode.EXACTLY_ONCE`, which is Flink's default and
so needs no line in either job below — but a cluster setting `execution.checkpointing.mode` to
`AT_LEAST_ONCE` has the job rejected when the graph is built, rather than silently downgraded.

### Buffered streams

Rows are appended to one Storage Write API buffered stream per (subtask, destination) at explicit
offsets, invisible until a completed checkpoint makes exactly that checkpoint's rows visible.

{{< java-snippet file="BigQueryExamplesBufferedStreams.java" tag="bigquery-examples-buffered-streams" >}}

`bufferedStreamOptions(...)` is required for this write method and rejected for the others, and
every knob in it is defaulted — `builder().build()` is how to say "the defaults" out loud. The
checkpoint interval is the visibility latency: rows land when the checkpoint that named them
completes.
Each active destination uses a dedicated connection and contributes its own stream creation and
flush calls, so high-cardinality routing should use an idle timeout appropriate to its churn and
must account for the Storage Write API's stream-creation quota.

### File loads

Rows are staged as files on Cloud Storage — Avro by default — and loaded with BigQuery load jobs, which is free of
streaming-insert cost and exactly-once in both execution modes.

{{< java-snippet file="BigQueryExamplesFileLoads.java" tag="bigquery-examples-file-loads" >}}

Point `stagingPath` at a **dedicated bucket, separate from checkpoint and savepoint storage, with a
lifecycle rule** deleting objects after a few days, so files orphaned by a hard failure expire on
their own. Size the rule above the longest outage you intend to recover from: files a checkpoint
still references *are* the data, and a streaming job restored after the rule expired them leaves
its pending loads permanently failing.

Batch is the same builder with `RuntimeExecutionMode.BATCH` and no checkpointing — everything loads
at end of input.

### Redeploying an exactly-once job

**Never redeploy through discarded state.** The two-phase commit puts rows and source positions in
the same phase with no atomicity between them, so a writer restored with no state opens a loss
window of at most one checkpoint: rows appended but not flushed, and committables checkpointed but
not committed, stay invisible forever while the source may already have acked past them.

The sink cannot detect this — a writer restored with no state is indistinguishable from a new job —
so the guard belongs in deployment tooling:

```sh
flink stop --savepointPath gs://my-savepoints <job-id>
flink run -s gs://my-savepoints/savepoint-xxxx my-job.jar
```

With the Flink Kubernetes Operator that is `upgradeMode: savepoint` (or `last-state`), never
`stateless`. When state genuinely has to be dropped, rewind the source behind the last completed
checkpoint so a potential loss becomes a duplicate instead, and make duplicates harmless downstream
with an idempotent key plus `MERGE` or `QUALIFY ROW_NUMBER()`.

The at-least-once write method has the opposite profile — it keeps the sink strictly ahead of the
source, so discarding state can duplicate rows but cannot lose them. Neither method is uniformly
safer; the [BigQuery connector]({{< relref "docs/connectors/datastream/bigquery" >}}) page sets
their loss paths side by side.

## Table auto-creation

The default create disposition is `CREATE_IF_NEEDED`, so the first record for a missing table
creates it from the serializer's schema. `tableCreateOptions(...)` is what decides the rest of the
table's shape:

{{< java-snippet file="BigQueryExamplesTableAutoCreation.java" tag="bigquery-examples-table-auto-creation" >}}

**These apply at creation and never afterwards.** An existing table is never modified by them, so
adding partitioning to a running pipeline changes only the tables created from that point on. Use
`tableCreateOptionsProvider(...)` instead when the settings vary per destination — it receives the
`TableDestination` and returns the options for it.

Creation is idempotent across parallel subtasks (a 409 counts as success), so nothing needs
coordinating — and a subtask the per-table quota rate-limits instead of answering 409 retries the
creation within the recovery budget, so a wide parallelism costs a backoff rather than the job
(see [Losing the creation race]({{< relref "/docs/connectors/datastream/bigquery" >}}#losing-the-creation-race-costs-a-retry-not-the-job)).
The credentials need `bigquery.tables.create` on the dataset; `CREATE_NEVER` turns a
missing table into an immediate job failure instead, which is what to use when a missing table
means a routing bug.

Creation is also the **only** moment a `REQUIRED` column can appear — BigQuery cannot add one to an
existing table — so whichever column modes the serializer derives are decided here, durably.

## No emulator path

**There is no `emulatorEndpoint(...)` on the BigQuery sink**, and that is a decision rather than a
gap waiting to be filled. The module's own tests reach
[goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator) through a test-only appender
factory handed to a `@VisibleForTesting` overload, so the production factory never needed an
endpoint seam; adding one to the public API was considered under
[#54]({{< param BookRepo >}}/issues/54) and left unbuilt for want of a consumer. It would slot into
the production factory's constructor state cheaply — open an issue if you want it.

Develop against a real dataset meanwhile; a sandbox project with a short default table expiration
keeps it cheap. That is also less of a loss than it sounds, because of how much such a run could
never prove: the emulator supports neither `gs://` load jobs nor a Cloud Storage endpoint, so
`FILE_LOADS` could not run against it at all, and it reads `TIME`, `DATETIME`, `NUMERIC` and
`BIGNUMERIC` columns back as unrelated values.

## Reading one column of a large table

The two push-down knobs are applied by BigQuery when the read session is created, so what they
exclude never leaves it — and the columns you leave out are not scanned, which is what the read is
charged for.

{{< java-snippet file="BigQueryExamplesReadingOneColumn.java" tag="bigquery-examples-reading-one-column" >}}

The reader schema names only the column being read. A row's other columns are dropped by Avro's
schema resolution before the record is built — and here they never left BigQuery in the first place.

## Reading a public dataset

A read session belongs to a project, and that is the project it is billed to. Reading a table you do
not own — a public dataset, or another team's — means naming your own project as the payer:

{{< java-snippet file="BigQueryExamplesReadingPublicDataset.java" tag="bigquery-examples-reading-public-dataset" >}}

Without `parentProject` the session would be created in `bigquery-public-data`, where you have no
permission to create one.

## Reading a table as it was

`snapshotTime` reads the table as of an instant, from BigQuery's time-travel window. Two jobs given
the same instant read the same rows, whatever has been written since — which is what makes a
re-run reproducible rather than merely repeated.

{{< java-snippet file="BigQueryExamplesReadingSnapshot.java" tag="bigquery-examples-reading-snapshot" >}}

Note that a read session pins its own snapshot at creation regardless, so a job that does *not* set
this still reads one consistent view of the table — just whichever one existed when it started.

## Reading a view

A view cannot be read as a table — the Storage Read API reads storage, and a view has none. Run it
as a query instead, and the source reads the table its result lands in.

{{< java-snippet file="BigQueryExamplesReadingView.java" tag="bigquery-examples-reading-view" >}}

`parentProject` is required here rather than optional: no table is named, so nothing else says which
project runs the query job and is billed for it. By default the result goes to BigQuery's own
anonymous dataset, which expires it in about a day and charges no storage for it — nothing to create
and nothing to clean up.

Prune inside the query rather than with `selectedFields`: those are applied to the *result*, so they
cannot make the query itself cheaper, and a query source pays for both scans. The trade-offs and the
constraints of each landing place are under
[Reading a query or a view]({{< relref "docs/connectors/datastream/bigquery" >}}#reading-a-query-or-a-view).

## Reading a view without writing the query

If a job is pointed at names it does not control — a catalog where some are tables and some are
views — `materializeViews()` handles both without the job having to know which is which.

{{< java-snippet file="BigQueryExamplesMaterializingViews.java" tag="bigquery-examples-materializing-views" >}}

A view is materialized and read; an ordinary table is read directly, with nothing billed for a
query. It is off by default because it costs one metadata call per job to tell the two apart, and
because materializing bills a query nobody wrote. `selectedFields` is folded into the generated
`SELECT`, so a view is not scanned for columns that would only be discarded.

## Landing a query result in your own dataset

Name a dataset when the anonymous one will not do — because something outside the job has to read
the result, or because a cached results table is not a dependency you want to take.

{{< java-snippet file="BigQueryExamplesQueryResultDataset.java" tag="bigquery-examples-query-result-dataset" >}}

The dataset must already exist and be in the query's own location. The connector creates a table
there with a one-day expiration and does not delete it earlier: teardown also runs on a JobManager
failover, where the restored job is still reading the read session that table backs.

## Asking for more read streams

A read stream is read by one subtask at a time, and a subtask takes the next stream as soon as it
finishes one. Over-provisioning is therefore how the work spreads evenly: with as many streams as
subtasks, one slow stream leaves a subtask idle at the end.

{{< java-snippet file="BigQueryExamplesPreferredStreamCount.java" tag="bigquery-examples-preferred-stream-count" >}}

BigQuery decides the actual count and may give fewer — a small table is read by one stream however
many are asked for. The measured behaviour of both knobs is under
[Assignment and stream count]({{< relref "docs/connectors/datastream/bigquery" >}}#assignment-and-stream-count).
