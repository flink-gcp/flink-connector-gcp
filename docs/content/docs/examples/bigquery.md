---
title: BigQuery
type: docs
weight: 10
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

# BigQuery examples

Starting from the [BigQuery quickstart]({{< relref "docs/quickstart/bigquery" >}}) job.

## CDC examples by source connector

Each CDC source has its own ordering contract and example section:

| Source | Sequence profile | Example status |
|---|---|---|
| Debezium PostgreSQL | Last committed and current LSN | [Available below](#debezium-postgresql-cdc-from-kafka) |
| Debezium MySQL | Source UUID epoch, GTID transaction, binlog position, and row | [Available below](#debezium-mysql-cdc-from-kafka) |
| TiCDC Debezium protocol | TiDB commit TSO and cluster identity | [Available below](#ticdc-debezium-cdc-from-kafka) |
| Debezium Spanner and native Spanner Change Streams | Commit timestamp, record sequence, and mod number | [Available below](#spanner-cdc-from-either-route) |

The implemented sections use complete source envelopes and do not substitute source timestamps or
processing time when ordering metadata is absent.

## Debezium MySQL CDC from Kafka

Both examples consume Kafka values containing complete Debezium MySQL Avro envelopes and write the
current row to a BigQuery table with a primary key.
They forward `connector`, `snapshot`, `gtid`, `pos`, and `row` from the envelope's `source` record.

### MySQL Kafka source

Use Flink's Kafka connector and `ConfluentRegistryAvroDeserializationSchema.forGeneric(...)` to
retain the complete envelope.
Pass its Avro schema as `debeziumEnvelopeSchema`:

{{< java-snippet file="BigQueryExamplesDebeziumMySqlCdc.java" tag="bigquery-debezium-mysql-cdc-kafka-source" >}}

A Debezium tombstone produces no element through Kafka's value-only wrapper.
Flink checkpoint restoration resumes from the Kafka positions stored in the checkpoint.
Add a Kafka connector release compatible with the application's Flink version and
`flink-avro-confluent-registry` to the job artifact.

### MySQL DataStream API

Pass the `KafkaSource<GenericRecord>` above as `kafkaSource` and the Avro schema for one physical
row as `rowSchema`:

{{< java-snippet file="BigQueryExamplesDebeziumMySqlCdc.java" tag="bigquery-debezium-mysql-cdc-datastream" >}}

The sequence provider receives the complete source-properties map and applies the same strict GTID
profile as the Table sink.
The serializer selects `after` for snapshot, create, and update operations and `before` for a
delete.
The example uses the default stream because BigQuery CDC is supported only by
`STORAGE_API_AT_LEAST_ONCE`.

### MySQL SQL sink through a DataStream bridge

Flink's Debezium Avro format does not currently retain the source metadata required by this
profile, while the raw Avro format cannot produce the required delete row kinds through SQL alone.
[Issue #706]({{< param BookRepo >}}/issues/706) tracks that format
boundary.
Until it changes, register a changelog view from the complete envelope stream:

{{< java-snippet file="BigQueryExamplesDebeziumMySqlCdc.java" tag="bigquery-debezium-mysql-cdc-sql-bridge" >}}

Define the BigQuery sink table and map the source-properties column to writable metadata:

{{< sql-snippet file="flink/BigQueryExamples.sql" tag="mysql-sink-table" >}}

Insert the changelog rows and their source metadata:

{{< sql-snippet file="flink/BigQueryExamples.sql" tag="mysql-sink-insert" >}}

The source UUID option and the DataStream provider use the same append-only epoch list.
After a non-interleaved failover, append the new SID with Flink's semicolon delimiter, for example
`'24bc7850-2c16-11e6-a073-0242ac110002;3e11fa47-71ca-11e1-9e33-c80aa9429562'`.
Never edit or reorder an existing entry because the entry's position defines its ordering epoch.

### MySQL envelope adapter

The examples use these helpers to select the current row and retain only the MySQL ordering
properties:

{{< java-snippet file="BigQueryExamplesDebeziumMySqlCdc.java" tag="bigquery-debezium-mysql-cdc-adapter" >}}

This adapter serializes the complete `before` record for a delete.
Configure MySQL with `binlog_row_image=FULL`, or replace the adapter with one that emits only the
declared BigQuery primary key for deletes.

Initial snapshot rows target an empty BigQuery table.
Tagged GTIDs, multi-source GTID sets, incremental snapshots, interleaved source histories, Group
Replication primary changes, and Group Replication multi-primary histories are unsupported.
See the [MySQL topology support table]({{< relref "docs/connectors/table/bigquery" >}}#mysql-topology-support)
before selecting this profile.

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
The serializer passes only that nested row to `AvroRecordSerializationSchema`; the two CDC providers still
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
[Issue #706]({{< param BookRepo >}}/issues/706) tracks the upstream
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

{{< sql-snippet file="flink/BigQueryExamples.sql" tag="debezium-sink-table" >}}

#### Insert query

Forward the changelog rows and their ordering metadata from the source view into the sink table:

{{< sql-snippet file="flink/BigQueryExamples.sql" tag="debezium-sink-insert" >}}

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

## TiCDC Debezium CDC from Kafka

TiCDC replicates TiDB row changes to Kafka in a Debezium-compatible JSON envelope when the
changefeed sets `protocol=debezium`.
Its source object carries `connector=TiCDC`, the transaction's commit timestamp oracle value as
`commit_ts`, and the reporting TiCDC cluster's `cluster_id`; both fields are present from TiCDC
v8.0.0 onwards.
The connector's `TiCdcSequenceNumberProvider` writes that commit TSO as the BigQuery change
sequence, after checking that the event belongs to the configured cluster.
That identifier is TiCDC's own cluster ID, which defaults to `default`, so give each TiCDC cluster
its own before routing more than one of them into one table.

The commit TSO orders every transaction within one TiDB cluster and survives a TiCDC process or
node failover, so this profile configures no epoch list of the kind the MySQL GTID profile needs.
It never falls back to the source object's `ts_ms`, which truncates the same value to milliseconds
and so cannot order two transactions committed within one millisecond.

Both examples below cover row changes only.
Whether anything else reaches the topic depends on the TiCDC deployment.
Before TiDB v9.0.0, TiCDC's classic architecture sends row changes only in this protocol, so
nothing below has to be configured against other events, while its new architecture sends DDL and
watermark events from TiCDC v8.5.4-release.1.
From TiDB v9.0.0 that new architecture is the only one, so DDL events always reach the topic.
Watermark events additionally require the changefeed to set `enable-tidb-extension=true`.
Neither carries a row to write, and neither is a shape Flink's `debezium-json` format can
deserialize: a watermark's `op` value is unknown to it and a DDL event has no `op` field at all, so
either one fails the job rather than being skipped.
Where such a topic is unavoidable, set `'value.debezium-json.ignore-parse-errors' = 'true'` on the
source table, accepting that it also hides a genuinely malformed row change.
TiCDC provides no initial snapshot stream either, so load an already-populated table separately,
taking that load at the changefeed's start timestamp.

### TiCDC SQL

Unlike the Debezium Avro sections above, this path needs no DataStream bridge.
TiCDC's Debezium protocol is JSON only; its separate `avro` protocol carries TiCDC's own envelope
rather than a Debezium one, so `debezium-avro-confluent` does not read it.
Flink's `debezium-json` format produces the changelog row kinds and retains the source object as
`value.source.properties`:

{{< sql-snippet file="flink/BigQueryExamples.sql" tag="ticdc-source-table" >}}

Keep `value.debezium-json.schema-include` at `true` whatever the changefeed sets: TiCDC always
wraps the change in a `payload` object, which is what that option describes, while
`debezium-disable-schema=true` removes only the sibling `schema` object.
Reading a `payload`-wrapped message with `schema-include` set to `false` fails every record.

Define the BigQuery sink table with the cluster identity and the writable metadata column:

{{< sql-snippet file="flink/BigQueryExamples.sql" tag="ticdc-sink-and-insert" >}}

### TiCDC Kafka source

The DataStream example keeps the complete envelope by reading each message as text:

{{< java-snippet file="BigQueryExamplesTiCdc.java" tag="bigquery-ticdc-cdc-kafka-source" >}}

Add a Kafka connector release compatible with the application's Flink version to the job artifact;
this repository does not ship one.
The compiled example uses Kafka connector `3.4.0-1.20` with Flink 1.20 and `5.0.0-2.2` with Flink
2.2 and 2.3.

### TiCDC DataStream API

Pass the `KafkaSource<String>` above as `kafkaSource`, and the destination table's schema as
`rowSchema`, which is the Storage Write API's `TableSchema`:

{{< java-snippet file="BigQueryExamplesTiCdc.java" tag="bigquery-ticdc-cdc-datastream" >}}

The adapter selects `after` for create and update operations and `before` for a delete, and passes
only that nested row to `JsonDocumentSerializationSchema`.
JSON carries no schema, so that serializer takes the destination schema rather than deriving one.
The two CDC providers still receive the complete message and derive the operation and sequence from
it.
The example uses the default stream because BigQuery CDC is supported only by
`STORAGE_API_AT_LEAST_ONCE`.

A TiDB delete gives `before` the complete row image, so nothing corresponds to the PostgreSQL
example's `REPLICA IDENTITY FULL` for deletes.
An update is different: a changefeed with `sink.debezium.output-old-value=false` omits `before`
from updates, which the SQL path's `debezium-json` format rejects.

### TiCDC envelope adapter

The DataStream example uses the following adapter helpers:

{{< java-snippet file="BigQueryExamplesTiCdc.java" tag="bigquery-ticdc-cdc-adapter" >}}

The job's checkpoint restores Kafka offsets after a Flink failure.
Records between the restored offset and the last successful BigQuery append can be replayed, but
the same TiCDC event produces the same change sequence number.

Every row change of one transaction carries that transaction's commit TSO and therefore one
sequence, and a later transaction always supersedes an earlier one.

**NOTE:** One transaction can still produce two conflicting changes for one BigQuery primary key.
TiDB writes each key at most once per transaction, but TiCDC splits an UPDATE that modifies a
primary or unique key into a DELETE and an INSERT, which is the default for every sink except
MySQL.
A transaction that moves one key's value onto another key, such as the primary-key swap in
[TiCDC's UPDATE splitting behavior](https://docs.pingcap.com/tidb/stable/ticdc-split-update-behavior/),
emits both a DELETE and an INSERT for one key at one sequence, and BigQuery resolves that pair by
ingestion time rather than by TiCDC's emission order.
Where such transactions occur, supply an application tie-breaker through `change-sequence-number`
or a custom `CdcSequenceNumberProvider`; see the
[BigQuery table CDC contract]({{< relref "docs/connectors/table/bigquery" >}}#change-data-capture).

See the [TiCDC Debezium protocol](https://docs.pingcap.com/tidb/stable/ticdc-debezium/),
[TiDB timestamp oracle](https://docs.pingcap.com/tidb/stable/tso/),
and [BigQuery CDC ordering format](https://cloud.google.com/bigquery/docs/change-data-capture).

## Spanner CDC from either route

Spanner changes reach BigQuery either through a Debezium Spanner connector on Kafka or through this
repository's native Change Streams source.
Both routes encode the same three coordinates of one Spanner mod — the commit timestamp in
nanoseconds, the record sequence within the transaction, and the mod number within the record — so
the same change produces the same BigQuery sequence whichever route wrote it.

### Debezium Spanner Kafka source

Use Flink's Kafka connector and `ConfluentRegistryAvroDeserializationSchema.forGeneric(...)` to
retain the complete envelope.
Pass its Avro schema as `debeziumEnvelopeSchema`:

{{< java-snippet file="BigQueryExamplesDebeziumSpannerCdc.java" tag="bigquery-debezium-spanner-cdc-kafka-source" >}}

### Debezium Spanner DataStream API

`DebeziumSpannerCdcSequenceNumberProvider` reads `connector`, `ts_ns`, `sequence`, and `mod_number`
from the envelope's `source` record:

{{< java-snippet file="BigQueryExamplesDebeziumSpannerCdc.java" tag="bigquery-debezium-spanner-cdc-datastream" >}}

### Debezium Spanner SQL sink through a DataStream bridge

Registering the changelog as a view keeps the source properties available to SQL:

{{< java-snippet file="BigQueryExamplesDebeziumSpannerCdc.java" tag="bigquery-debezium-spanner-cdc-sql-bridge" >}}

{{< sql-snippet file="flink/BigQueryExamples.sql" tag="debezium-json-sink-and-insert" >}}

### Debezium Spanner envelope adapter

The adapter turns one envelope into the row to write and the source properties to order it by:

{{< java-snippet file="BigQueryExamplesDebeziumSpannerCdc.java" tag="bigquery-debezium-spanner-cdc-adapter" >}}

Copy the coordinates from the envelope's `source` record rather than from the payload.
Debezium writes `ts_ms`, `ts_us`, and `ts_ns` in both places: inside `source` they carry the Spanner
commit timestamp, while the siblings beside `source` carry the time the connector processed the
event on its own clock.

The adapter rejects every operation it does not recognize, which includes the `m` of the
low-watermark stamps Debezium writes into this same topic under
`gcp.spanner.low-watermark.enabled`.
Those stamps carry no row and no ordering coordinates, so there is nothing to write and nothing to
order them by; leave that option at its default `false` for a topic this example reads.

### Native Change Streams DataStream API

The native source hands the deserializer a typed `DataChangeRecord`, so the example emits one
element per mod and keeps the mod's position as its mod number:

{{< java-snippet file="BigQueryExamplesSpannerNativeCdc.java" tag="bigquery-spanner-native-cdc-deserializer" >}}

`SpannerCdcSequenceNumber.of(...)` then encodes those coordinates without going through a
Debezium-shaped map:

{{< java-snippet file="BigQueryExamplesSpannerNativeCdc.java" tag="bigquery-spanner-native-cdc-datastream" >}}

### Native Change Streams SQL

The native route needs no DataStream bridge.
The Debezium route needs one because Flink's Avro changelog drops the envelope's `source` record,
whereas the Spanner source of this repository is a Flink table in its own right and already exposes
its ordering coordinates as metadata columns.
Source DDL, sink DDL, and the `INSERT` are therefore all the SQL below.

Declare the change stream as a source table and expose its three ordering coordinates as metadata
columns:

{{< sql-snippet file="flink/BigQueryExamples.sql" tag="spanner-change-stream-source" >}}

Declare the BigQuery table and take the sequence as one row of writable metadata:

{{< sql-snippet file="flink/BigQueryExamples.sql" tag="spanner-change-stream-sink" >}}

Insert the changelog and build the sequence row from the three source metadata columns:

{{< sql-snippet file="flink/BigQueryExamples.sql" tag="spanner-change-stream-insert" >}}

Two details in that DDL are load-bearing.
The source runs in `upsert` changelog mode, because `full` also emits update-before rows, which the
BigQuery CDC sink rejects.
The commit-timestamp column is declared `TIMESTAMP_LTZ(9)` and carries no watermark: Flink permits
watermark columns only through precision 3, and truncating the commit timestamp to milliseconds
would let two changes of one key inside the same millisecond compare equal on their first section.

## A table per day

The writer context carries the record's event timestamp, which makes time-based routing expressible without the record carrying the routing key.
The [dynamic destinations guide]({{< relref "docs/examples/dynamic-destinations" >}}#bigquery-tables) defines the shared resolver contract and compares its resource lifetime with the other sinks.
This resolver caches one destination per UTC day and falls back to the record's own timestamp when the writer context has none.

{{< java-snippet file="BigQueryExamplesTablePerDay.java" tag="bigquery-examples-table-per-day-resolver" >}}

{{< java-snippet file="BigQueryExamplesTablePerDay.java" tag="bigquery-examples-table-per-day-sink" >}}

Two things need planning when a resolver keeps producing new destinations.
The default-stream and buffered-stream methods hold one writer per active destination, so `DefaultStreamOptions` and `BufferedStreamOptions` expose `destinationIdleTimeout` (one hour by default) to bound that local state.
FILE_LOADS bounds each writer subtask to `maxOpenDestinations` active files (16 by default), finishes the least recently used file when that capacity is reached, and also finishes a file after `destinationIdleTimeout` (one minute by default).
It retains at most `maxPendingFiles` finished and open files for the next commit (10,000 by
default), failing before another file is opened if churn reaches that bound.
A checkpoint finishes every remaining file and releases its conversion state.
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

## Pointing the sink at an emulator

The sink takes **two** emulator endpoints, one per transport, because BigQuery serves table metadata
over REST and the Storage Write API over gRPC — where every sibling connector needs one transport
and so exposes one endpoint. Against
[goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator), `emulatorEndpoint(...)` points
the Storage Write API traffic at it; `emulatorRestEndpoint(...)` points the table metadata traffic at
it, which means table creation under `CREATE_IF_NEEDED`, connector-driven schema updates and the CDC
table contract. A sink that only appends to a table that already exists needs the first alone. Any
other sink — one that creates a table, evolves a schema or manages a CDC table — needs both: given
only `emulatorEndpoint(...)`, its metadata half **still reaches real BigQuery, under ADC and without
saying so**, so a run meant for local development can create or alter production tables.

Both are rejected under `FILE_LOADS`, which stages files to Cloud Storage that no emulator here
stands in for: an endpoint could only be honored by the metadata half of that write method and
silently ignored by the half that moves the rows. Neither can be combined with
`serviceAccountKeyFile(...)`, because emulator connections are deliberately credential-free.

What such a run proves is bounded, and worth knowing before leaning on it. The emulator reads
`TIME`, `DATETIME`, `NUMERIC` and `BIGNUMERIC` columns back as unrelated values, and it keeps no
flush cursor — so the exactly-once guarantee is not observable there, which is why this module's
exactly-once integration tests run against a real dataset. A sandbox project with a short default
table expiration keeps that cheap.

## Reading a BigQuery table with SQL

Create the physical table used by the quickstart's read example and add one row:

```sh
bq query --use_legacy_sql=false \
  'CREATE TABLE IF NOT EXISTS `my-project.my_dataset.people` (id INT64, name STRING)'
bq query --use_legacy_sql=false \
  'INSERT `my-project.my_dataset.people` (id, name) VALUES (1001, "Ada")'
```

Register it as a bounded Flink table source:

{{< sql-snippet file="flink/BigQueryExamples.sql" tag="bounded-source" >}}

The source finishes after reading one BigQuery snapshot, so the table works in a batch job or as the bounded side of a streaming job.
Top-level projection is pushed into the Storage Read session, so the query above requests only `name`.
Flink predicates are evaluated after the source because the connector does not translate them into BigQuery filters; set `scan.row-restriction` in the DDL when a BigQuery-native server-side predicate is required.

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
