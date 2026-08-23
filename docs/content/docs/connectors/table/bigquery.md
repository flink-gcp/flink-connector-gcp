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

# BigQuery SQL Connector

The `bigquery` connector reads a bounded BigQuery table or query result and writes a table through
the module `flink-connector-gcp-bigquery`. It maps onto the DataStream source and sink documented in
[BigQuery]({{< relref "docs/connectors/datastream/bigquery" >}}) — that page carries the design,
the delivery guarantees and the error handling; this one carries the DDL surface. Per-feature
status is in the module README.

`scan.parallelism` and `sink.parallelism` come from Flink's own `FactoryUtil` rather than from this
connector. There is no `format` option: a BigQuery row is structured and the DDL schema *is* the
schema, so the connector supplies its own `RowData` converter and serializer.

```sql
CREATE TABLE events (
  id STRING,
  amount BIGINT,
  event_ts TIMESTAMP_LTZ(6),
  attributes ROW<source STRING, version INT>
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'dataset' = 'analytics',
  'table' = 'events'
);

INSERT INTO events
SELECT id, amount, event_ts, ROW(source, version) FROM staged_events;

SELECT id, amount FROM events WHERE amount > 0;
```

A query source needs a billing project but no `dataset` or `table`:

```sql
CREATE TABLE recent_events (
  id STRING,
  amount BIGINT
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'scan.query' = 'SELECT id, amount FROM `analytics.events` WHERE event_date = CURRENT_DATE()',
  'scan.query-location' = 'US'
);
```

## Getting the connector onto the classpath

Use `flink-sql-connector-gcp-bigquery`, an uber-jar built for exactly this: put it in Flink's `lib/`
directory, or add it with `ADD JAR` in the SQL client. It bundles `flink-connector-gcp-bigquery`
together with its whole runtime tree — the Storage Write API and REST clients, the Cloud Storage
client, gRPC, protobuf, Avro, Guava, the Google auth and HTTP clients — which is 110 artifacts, not
a dependency list anyone wants to assemble by hand.

The plain `flink-connector-gcp-bigquery` jar works too, where the deployment already resolves
transitive dependencies. That is the right choice for a DataStream job built with Maven or Gradle —
and for one using the Avro serializer it is the only choice: `avro` is relocated inside the
uber-jar, so the `AvroRecordSerializer` in it takes a relocated `IndexedRecord` that an ordinary job
cannot supply. None of that applies to SQL, where the connector supplies its own `RowData`
serializer.

### Everything bundled is relocated

Every bundled *third-party* package moves under `io.github.flink.gcp.connector.bigquery.shaded.`,
so the versions of gRPC, protobuf, Guava, Jackson and Avro this connector needs cannot collide with
the ones a job, another connector, or Flink itself brings. That is the point of the artifact:
without it, a BigQuery job that also touches any other Google Cloud library becomes a
version-alignment exercise. The connector's own `io.github.flink.gcp.connector.bigquery` stays
where it is — it is this jar's public surface, and it is what the DDL's `connector` option resolves
through.

Six third-party packages are deliberately *not* relocated, and none of them can collide in a way
that matters: `org.conscrypt`, which gRPC picks up reflectively as an optional TLS provider and
does without when it is unusable; and the annotation-only `javax.annotation`, `org.jspecify`,
`org.checkerframework`, `org.codehaus.mojo.animal_sniffer` and `android.annotation`, where a
duplicate class is inert because nothing ever invokes it. `javax.annotation` here is jsr305's
classes only — `javax.annotation-api`, the other artifact publishing into that package, is not
bundled ([#352]({{< param BookRepo >}}/issues/352)).

Two packages the jar references are not in it. `org.slf4j` is excluded deliberately: Flink's own
distribution provides it, and bundling it would be wrong either way round — relocated, the
connector's logging would bind to a copy no Flink log configuration reaches and go silent;
unrelocated, the jar would put a second `slf4j-api` on a classpath that already has one.
`org.apache.commons.logging` is absent because nothing in the tree brings it; the Apache HTTP
transport it belongs to is not the one the Google clients use by default, and it is left
unrelocated precisely so that a deployment that does reach it can supply commons-logging in `lib/`
in the ordinary way.

`io.grpc:grpc-netty-shaded` *is* relocated, which takes some care: gRPC ships it already relocated
once, having renamed its `META-INF/native/` libraries to match, because netty derives the native
library name from its own package at load time. Relocating those classes a second time therefore
means renaming the library files again in step. Leaving it alone was the obvious alternative and is
wrong — the jar would then be unable to share a classpath with `flink-sql-connector-gcp-pubsub`,
which bundles gRPC too. Sharing a `lib/` with it does work: of the 501 file entries the two jars
have in common (directory entries excluded), 497 are byte-identical, and the four that differ are
per-jar metadata Flink reads through `ServiceLoader`, which enumerates every copy — the manifest,
the `NOTICE`, and the two service files (measured 2026-08-08, one build of each jar). **Merging the two into one fat
jar is the case that does not work** — one connector's factory registration and one jar's `NOTICE`
would be shadowed, silently. Put them in `lib/`, or add each with its own `ADD JAR`. One consequence of relocating an already-relocated gRPC: netty's **system
property names** move with it, so a `-D` spelled `io.grpc.netty.shaded.io.netty.maxDirectMemory`
has no effect here — it has to carry the shaded prefix above.

The jar is about 64 MB (measured 2026-08-06). The source uses the Storage Read API's Avro wire
format, but `google-cloud-bigquerystorage` also brings Apache Arrow and flatbuffers for the declined
Arrow path; together with netty those three are 3.2 MB, about 5% of the jar. They remain bundled
because the bundle is defined as "the runtime classpath" rather than as a hand-maintained list, and
a list is how a dependency gets silently dropped from a jar instead of failing a build.

### Credentials

The connector uses **application default credentials** when `service-account-key-file` is absent.
Set that option to a service-account JSON key-file path to use an explicit identity for every
BigQuery client and, under `file-loads`, for GCS staging too.

Only the path enters the job graph.
The file is read when runtime clients open, so it must exist at the same path on every JobManager
and TaskManager that can plan a read or run a source reader, sink writer or committer, including
after failover or rescaling.
Every copy at that path must contain the same service-account key; a query result created by one
identity might not be readable by another.
Only service-account JSON is accepted, and failures do not include the path or parser cause.
The option is rejected with either emulator endpoint because emulator connections are
credential-free.

### Licensing

`META-INF/NOTICE` inside the jar lists every bundled artifact grouped by licence, and
`META-INF/licenses/` carries the full text of each non-Apache-2.0 one — protobuf, gax, the Google
auth library, the ThreeTen backport and ThreeTen-Extra, RE2/J, animal-sniffer, the Checker
Framework qualifiers, the Stax2 API and JSON-java.

The prose of the NOTICE is human-written, in the module's `NOTICE.template`; the artifact lists are
generated into it from what Maven actually resolves, so a wrong licence grouping or a stale version
cannot be written at all. Each licence text has a pinned source — the artifact's own jar where one
ships a text, otherwise a curated URL matched to the bundled version — recorded with its sha256, so
a text that changes upstream fails the build instead of being shipped unreviewed. `just
update-notice <module>` regenerates both after a dependency change; `just check-notice <module>`
verifies, offline, that what is checked in still matches the bundle and the pins. Generic
licence-name URLs (`opensource.org`, `spdx.org`) are rejected as sources: they serve HTML pages or
bare templates, and the copyright holder is part of a BSD or MIT text.

## Options

Every connector-owned runtime option maps onto the DataStream API, which stays the source of
truth; destination components are assembled together, and `scan.parent-project` overrides the
`project` fallback passed to `parentProject(...)`.
Flink's `scan.parallelism` and `sink.parallelism` configure the corresponding runtime provider
instead.
Except for the documented `project` fallback into `scan.parent-project`, leaving an optional
runtime option out of the DDL leaves its setter uncalled, so its default is whatever the connector
or SDK already uses. The full list of defaults is in the
[configuration reference]({{< relref "docs/reference/bigquery" >}}).

### Destination

| Option | Type | Maps to |
|---|---|---|
| `project` | String | The project part of `table(...)`; also the source billing project unless `scan.parent-project` overrides it. Required for sinks and direct sources; a query source may instead set `scan.parent-project` |
| `dataset` | String | The dataset part of `table(...)`; required for a sink or direct table source, unused by a query source |
| `table` | String | The table part of `table(...)`; required for a sink or direct table source. One SQL table writes to one BigQuery table: per-record routing has no SQL surface and stays on the DataStream API |
| `emulator-endpoint` | String | `emulatorEndpoint(...)`, the Storage Read or Write API's gRPC endpoint as `host:port`. Parsed when the statement is planned, so a malformed value fails on the client in either direction, and the rejection names `emulator-endpoint` — the key written in the DDL. Refused outright under `file-loads`, before its shape is looked at |
| `emulator-rest-endpoint` | String | `emulatorRestEndpoint(...)`, used for source query/view materialization and sink table metadata. Separate because BigQuery serves the two transports on different ports. Parsed and refused on the same terms as `emulator-endpoint`, under its own name. A direct table source leaves it unused — one `WITH` clause serves both directions — but a malformed value is still rejected |
| `service-account-key-file` | String | `serviceAccountKeyFile(...)`; a service-account JSON key-file path loaded on JobManagers and TaskManagers at runtime. Absent uses ADC; rejected with either emulator endpoint |

### Source

The source is bounded and insert-only. A direct source names `project`, `dataset`, and `table`.
`scan.query` switches to the query-result path, where either `project` or
`scan.parent-project` is required. Projection
pushdown sends the planner's retained top-level column names to the Storage Read session. It prunes
the direct table read and the generated SQL used to materialize a named view. A user-supplied query
is left unchanged, so projection prunes only its result-table read and does not reduce what the
query itself scans. A plan needing no output column retains the first physical column as a carrier;
the planner discards it above the source. Nested projection and SQL filter pushdown are not
advertised; use `scan.row-restriction` when a BigQuery-native server-side predicate is needed.

| Option | Type | Maps to |
|---|---|---|
| `scan.parent-project` | String | `parentProject(...)`; the project that owns and is billed for the Storage Read session. *Unset ⇒ `project`*. Set it independently when the table belongs to another project, such as a public dataset |
| `scan.query` | String | `query(...)`; reads the query's result instead of the configured table |
| `scan.materialize-views` | Boolean | `materializeViews()`; checks a configured table name and materializes it when it is a logical or materialized view. Cannot be combined with `scan.query` |
| `scan.query-location` | String | `queryLocation(...)`; query or view materialization only |
| `scan.query-result-dataset` | String | `queryResultDataset(...)`; query or view materialization only. Absent uses BigQuery's anonymous dataset |
| `scan.reuse-query-result-within` | Duration | `reuseQueryResultWithin(...)`; requires `scan.query-location` |
| `scan.row-restriction` | String | `rowRestriction(...)`; a BigQuery filter expression without the `WHERE` keyword |
| `scan.snapshot-time` | String | `snapshotTime(...)`; an ISO-8601 instant for a direct table read, incompatible with query or view materialization |
| `scan.max-stream-count` | Integer | `maxStreamCount(...)` |
| `scan.preferred-min-stream-count` | Integer | `preferredMinStreamCount(...)` |
| `scan.max-records-per-fetch` | Integer | `maxRecordsPerFetch(...)` |
| `scan.retry.max-attempts` | Integer | `retryMaxAttempts(...)` |
| `scan.parallelism` | Integer | The bounded source's parallelism (Flink's own option) |

### Sink

| Option | Type | Maps to |
|---|---|---|
| `sink.write-method` | Enum | `writeMethod(...)` — `storage-api-at-least-once`, `storage-api-exactly-once` or `file-loads`. Each carries its own tuning family below, and a key of a family this option does not select is rejected rather than ignored |
| `sink.cdc.enabled` | Boolean | Enables BigQuery CDC mutations through `cdcOptions(...)`. Defaults to `false`; requires `storage-api-at-least-once` and a declared primary key |
| `sink.cdc.debezium-mysql.source-uuids` | List&lt;String&gt; | Ordered MySQL GTID source UUIDs. The append-only order assigns failover epochs for the Debezium MySQL sequence profile |
| `sink.cdc.ticdc.cluster-id` | String | The TiCDC cluster ID whose commit timestamp oracle values order this table, as the changefeed reports it in `cluster_id`. A row change reporting another cluster is rejected rather than ordered against an unrelated oracle. TiCDC defaults this identifier to `default` |
| `sink.cdc.max-staleness` | Duration | `CdcTableOptions.maxStaleness(...)`. Absent means that the property is unmanaged |
| `sink.cdc.clear-max-staleness` | Boolean | `CdcTableOptions.clearMaxStaleness()`. Explicitly removes a previous value and is mutually exclusive with `sink.cdc.max-staleness` |
| `sink.cdc.table-reconciliation` | Enum | `cdcTableReconciliationPolicy(...)` — `verify-only` (default) or `reconcile`. Selects whether an existing table is only checked or has managed mutable CDC properties converged |
| `sink.create-disposition` | Enum | `createDisposition(...)` — `create-if-needed` or `create-never`. This controls only whether a missing table may be created; existing-table handling is independent |
| `sink.location` | String | `location(...)` |
| `sink.schema-update.allow-new-fields` | Boolean | `SchemaUpdateOptions.allowNewFields()`. Accepted under every write method; their reconciliation boundaries differ as described under [Schema evolution](#schema-evolution) |
| `sink.schema-update.allow-field-relaxation` | Boolean | `SchemaUpdateOptions.allowFieldRelaxation()`. Accepted under every write method; BigQuery's native load/query option applies to `write-append` and `write-truncate-data`, while the connector also reconciles `file-loads` tables before `write-empty` jobs |
| `sink.derive-required-columns` | Boolean | Derives a `REQUIRED` column from a `NOT NULL` one; off, every derived column is `NULLABLE` |
| `sink.json-field-paths` | List&lt;String&gt; | Derives the named columns as BigQuery `JSON` |
| `sink.geography-field-paths` | List&lt;String&gt; | Derives the named columns as BigQuery `GEOGRAPHY` |
| `sink.parallelism` | Integer | The sink's parallelism (Flink's own option) |

### Change data capture

Set `sink.cdc.enabled` to `true` to consume a Flink upsert changelog and write BigQuery `UPSERT`
and `DELETE` mutations through the Storage Write API default stream.
The option defaults to `false`, is rejected with `storage-api-exactly-once` or `file-loads`, and
requires the sink DDL to declare a `PRIMARY KEY ... NOT ENFORCED`.
Under the default `create-if-needed`, the sink creates a missing table's schema and BigQuery
primary key through the Tables API.
Set `sink.cdc.max-staleness` when the sink should also manage that CDC policy, or set
`sink.cdc.clear-max-staleness=true` to remove a previous value.
Leaving both unset means that maximum staleness is unmanaged.
`INFORMATION_SCHEMA.TABLE_OPTIONS` exposes a never-set value as absent and a value cleared with
`NULL` as a zero interval; the sink accepts both as disabled.
BigQuery silently dropped `maxStaleness` through all three measured Tables API write methods on
2026-08-14, so the sink applies a configured value through `ALTER TABLE` and verifies it through
`INFORMATION_SCHEMA.TABLE_OPTIONS` before opening the first writer.
That optional path needs `bigquery.jobs.create` and table-update permission and submits metadata
query jobs; leaving both maximum-staleness options unset submits none.
The default `sink.cdc.table-reconciliation=verify-only` checks existing tables without modifying
an unlabeled table.
Select `reconcile` to adopt an existing table and converge maximum staleness plus the connector
provisioning label.
Managing that label requires table-update permission even when maximum staleness is unmanaged;
the label-only path submits no query job.
Reconciliation never changes the primary key, partitioning, clustering, or schema.
`sink.create-disposition=create-never` still permits verification or reconciliation of an existing
table; it only denies creation when the table is missing.

The two options combine as follows:

| `sink.create-disposition` | `verify-only` (default) | `reconcile` |
|---|---|---|
| `create-if-needed` | Create a missing table; verify an existing table | Create a missing table; converge an existing table |
| `create-never` | Fail if the table is missing; verify an existing table | Fail if the table is missing; converge an existing table |

`verify-only` does not start adoption or drift repair.
If a table carries the matching connector-owned pending label, either policy resumes that partial
attempt, which may apply the required maximum-staleness DDL before completing the label.

A job restored from a job graph serialized before CDC auto-creation existed keeps that pre-created
behavior until it is redeployed from a new plan.
See
[ADR-0112]({{< param BookRepo >}}/blob/main/docs/adr/0112-bigquery-cdc-auto-creation-combines-the-tables-api-with-verified-ddl.md)
for the service evidence, recovery protocol, and ownership boundary.

`INSERT` and `UPDATE_AFTER` become `UPSERT`, while `DELETE` becomes `DELETE`.
`UPDATE_BEFORE` is rejected instead of being guessed to be a delete, because an ordinary update
also carries an update-before row.
A primary-key change must therefore reach the sink as `DELETE` for the old key followed by
`INSERT` or `UPDATE_AFTER` for the new key.
Delete rows serialize only the declared primary-key fields, so a key-only delete does not need
values for `REQUIRED` non-key BigQuery columns.

Sequence metadata is optional.
When selected, exactly one of these writable metadata columns may appear in the sink DDL:

| Metadata key | Data type | Meaning |
|---|---|---|
| `change-sequence-number` | `STRING` | An already formatted BigQuery `_CHANGE_SEQUENCE_NUMBER`: one to four slash-separated hexadecimal sections, each at most 16 digits |
| `debezium-source-properties` | `MAP<STRING, STRING>` | The Debezium format's source metadata map, normally forwarded from a source column declared `METADATA FROM 'value.source.properties' VIRTUAL`; built-in profiles accept `connector=postgresql`, `connector=mysql`, `connector=TiCDC`, and `connector=spanner` |
| `spanner-change-sequence` | `ROW<commit_timestamp TIMESTAMP_LTZ(9), record_sequence STRING, mod_number INT>` | The typed ordering metadata of one Spanner change-stream mod, normally forwarded from the native Spanner source's `commit-timestamp`, `sequence`, and `mod-number` metadata columns |

Writable metadata is appended to the runtime row by Flink and is not part of the physical
BigQuery schema.
Selecting more than one sequence source fails during planning.
Once a sequence metadata column is selected, every emitted row must supply a non-null valid value;
missing or invalid values enter the configured row-failure path.
Without any of them, BigQuery resolves colliding mutations for a primary key by arrival order.

#### What the sequence value looks like

Whichever route supplies it, the sink writes one string into BigQuery's `_CHANGE_SEQUENCE_NUMBER`
pseudocolumn, and every built-in profile writes each section as exactly 16 uppercase hexadecimal
digits:

```text
17306D33FB84D440/0000000000000001/0000000000000000
```

BigQuery accepts one to four sections separated by `/`, each at most 16 hexadecimal characters in
either case, spanning `0/0/0/0` to
`FFFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF`.
It compares the first section, and the next one only when the previous sections are equal.
Each section is compared as an unsigned number rather than as text, so a shorter section is the
smaller value: `FFF/B` precedes `FFF/ABC`.
The profiles pad to a fixed width anyway, which makes the strings sort identically whether a reader,
a log search, or BigQuery compares them.
Two rows whose sequences are equal are applied in BigQuery ingestion order, and mixing rows that
carry a sequence with rows that do not leaves the order of that mix undefined.

The pseudocolumn is write-only.
It travels with the append request and never becomes a table column, so no query reads it back;
project any ordering value a reader needs into an ordinary column as well.

The formatted route is immediately usable when the query can construct or forward a valid
sequence:

```sql
CREATE TABLE current_orders (
  id STRING NOT NULL,
  amount BIGINT,
  sequence STRING METADATA FROM 'change-sequence-number',
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

INSERT INTO current_orders
SELECT id, amount, formatted_sequence FROM ordered_changes;
```

Flink's Debezium JSON format exposes connector-specific ordering fields through
`value.source.properties` when it is used as a value format, while its Debezium Avro format does
not retain those fields in the produced changelog.
The [Kafka-to-BigQuery CDC examples]({{< relref "docs/examples/bigquery" >}}#cdc-examples-by-source-connector)
separate PostgreSQL, MySQL, TiCDC, and Spanner ordering contracts.
The PostgreSQL and MySQL sections show how to retain a complete Avro envelope in DataStream and
hand it to either API, while the TiCDC section reads its JSON envelope through `debezium-json`,
which retains `value.source.properties` and needs no such bridge.

This page shows Kafka only to make Flink's `value.source.properties` hand-off explicit; this
repository does not ship a Kafka connector.
The BigQuery sink owns the public `debezium-source-properties` name because it is writable sink
metadata, while `value.source.properties` remains owned by the source format.
Connector profiles choose ordering fields from that map instead of falling back to `ts_ms`, which
can collide for changes in the same millisecond.

The PostgreSQL profile parses Debezium's `sequence` JSON array as the nullable last committed LSN
and required current LSN.
It writes both positions as two fixed-width unsigned 64-bit hexadecimal sections and verifies the
current position against `lsn` when that property is non-null.
A `sequence` of `["16","17"]` becomes `0000000000000010/0000000000000011`, and a snapshot row,
whose last committed position is null, becomes `0000000000000000/0000000000000010`.
Missing, malformed, or contradictory values fail the row through the configured failure handler.
DataStream applications can apply the same parser to a source-properties map with
`DebeziumPostgreSqlCdcSequenceNumberProvider`.
A replay of the same source event produces the same sequence, while a later position from the same
logical replication slot produces a larger sequence.

PostgreSQL primary failover therefore requires Debezium to continue from the same preserved and
synchronized logical replication slot.
Follow the
[Debezium PostgreSQL failover guidance](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
when configuring that slot.
A recreated slot or any transition that loses LSN continuity is unsupported because the source
metadata carries no ordering epoch that could distinguish the new LSN history.

The MySQL profile encodes four fixed-width unsigned hexadecimal sections: the configured source
UUID epoch, GTID transaction id, binlog event position, and zero-based row within that event.
Configure `sink.cdc.debezium-mysql.source-uuids` with the initial server UUID and append each
non-interleaved failover UUID in causal order.
Flink list options use semicolons, so a failover list is written as
`'sink.cdc.debezium-mysql.source-uuids' = '24bc7850-2c16-11e6-a073-0242ac110002;3e11fa47-71ca-11e1-9e33-c80aa9429562'`.
Never edit or reorder existing entries, because their positions define stable ordering epochs.
Initial snapshot rows use epoch zero and are safe only when loading an empty destination; ad hoc
snapshots into a populated target are unsupported.
A snapshot row is therefore `0000000000000000/0000000000000000/0000000000000000/0000000000000000`,
while the first configured UUID's GTID transaction 16 at binlog position 1081, row 2, becomes
`0000000000000001/0000000000000010/0000000000000439/0000000000000002`.
Streaming rows require an untagged single-source `UUID:transaction_id` GTID plus `pos` and `row`.
DataStream applications can use `DebeziumMySqlCdcSequenceNumberProvider` with the same ordered
UUID list.

#### MySQL topology support

The profile has these topology boundaries:

| MySQL topology | Status | Ordering requirement |
|---|---|---|
| One source UUID | Supported | The UUID remains the only configured epoch |
| Non-interleaved failover to a new source UUID | Supported | Append the new UUID before consuming its events; no event from an earlier UUID may follow |
| Interleaved or multi-source GTID history | Unsupported | The source map has no causal epoch that can order concurrent or returning SIDs |
| Group Replication primary change | Unsupported | Group transactions retain one group UUID, so the source map does not identify a primary epoch |
| Group Replication multi-primary | Unsupported | Members reserve separate GTID transaction-number blocks, which do not provide a group-wide ordering coordinate |

Tagged GTIDs, multi-source GTID sets, incremental snapshots, unknown UUIDs, and malformed
coordinates fail explicitly.
Interleaved histories and Group Replication cannot be detected from every individual source map,
so applications must not send those topologies to this profile.
Group Replication can become supportable if MySQL and Debezium can expose a durable group-wide
ordering coordinate in source metadata.
Until then, use `change-sequence-number` with an application-provided total order for that topology.

#### TiCDC commit timestamp oracle values

TiCDC emits TiDB row changes in a Debezium-compatible envelope whose source map carries
`connector=TiCDC`, the numeric `commit_ts`, and `cluster_id`.
Both TiDB fields are present from TiCDC v8.0.0 onwards.
The profile writes the commit timestamp oracle value as one fixed-width unsigned 64-bit
hexadecimal section, and rejects a missing, non-numeric, zero, or overflowing value.
A `commit_ts` of `449574614268182531` becomes `063D35BACF7D0003`; the next logical counter within
that same millisecond becomes `063D35BACF7D0004`, and the next physical millisecond
`063D35BAD0410001`.
The single section is what makes those two neighbours distinguishable at all.
It never substitutes the source object's `ts_ms`, which truncates that same timestamp oracle value
to milliseconds and so cannot order two transactions committed within one millisecond.
DataStream applications can apply the same encoding to a source-properties map with
`TiCdcSequenceNumberProvider`.

A commit TSO orders every transaction within one TiDB cluster, and it is read from the change log
rather than assigned by the process encoding it, so it survives a TiCDC process or node failover.
This profile therefore needs no failover epoch and no equivalent of the MySQL source-UUID list.
Configure `sink.cdc.ticdc.cluster-id` with the `cluster_id` the changefeed reports, and a row
change from any other cluster fails through the configured failure handler rather than being
ordered against an unrelated oracle.
That identifier is TiCDC's own cluster ID rather than TiDB's: it defaults to `default` and follows
`^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$`, so give each TiCDC cluster its own before routing more than one
of them into one table.
Row changes of one transaction share its commit TSO and therefore its sequence, so the commit TSO
is not a unique total order over events.
TiDB writes each key at most once per transaction, but TiCDC splits an UPDATE that modifies a
primary or unique key into a DELETE and an INSERT, which is the default for every sink except
MySQL.
A transaction that moves one key's value onto another key, such as a primary-key swap, therefore
emits both a DELETE and an INSERT for one BigQuery primary key at one sequence, and BigQuery
resolves that pair by ingestion time rather than by TiCDC's emission order.
Applications that admit such transactions must supply their own tie-breaker through
`change-sequence-number` or a custom `CdcSequenceNumberProvider`.
See [TiCDC's UPDATE splitting behavior](https://docs.pingcap.com/tidb/stable/ticdc-split-update-behavior/)
for the transactions that produce a split.

This profile covers row changes only.
Which other events reach the topic depends on the TiCDC deployment rather than on this connector.
Before TiDB v9.0.0, TiCDC's classic architecture sends row changes only in this protocol, ignoring
DDL and watermark events, while its new architecture sends both from TiCDC v8.5.4-release.1.
From TiDB v9.0.0 that new architecture is the only one, so DDL events always reach the topic.
Watermark events additionally require the changefeed to set `enable-tidb-extension=true`.
Neither carries a row to write, and neither is a shape Flink's `debezium-json` format can
deserialize, so a topic carrying them needs `value.debezium-json.ignore-parse-errors` or a
changefeed that does not produce them.
Before v9.0.0 the two architectures are separate deployments, so which one is running is a
question for whoever operates the changefeed rather than something the topic reveals.
TiCDC provides no initial snapshot stream, so a populated destination needs a separate initial
load, taken at the changefeed's start timestamp.

#### Spanner commit timestamps, record sequences, and mod numbers

Spanner reaches this sink through two routes that share one ordering contract.
Both encode three fixed-width unsigned 64-bit hexadecimal sections: the commit timestamp in
nanoseconds since the epoch, the record sequence within the transaction, and the mod number within
the change record.
A change committed at `2022-12-13T18:18:51.785Z`, first in its transaction and first in its record,
becomes `17306D33FB84D440/0000000000000001/0000000000000000`; the record after it in that
transaction becomes `…/0000000000000002/0000000000000000`, and the second mod of the first record
`…/0000000000000001/0000000000000001`.

Debezium's Spanner connector supplies those coordinates as `ts_ns`, `sequence`, and `mod_number`
under `connector=spanner`.
`ts_ns` inside the `source` object is Debezium's rendering of the Spanner commit timestamp, and it
keeps the nanoseconds `ts_ms` would truncate.
The identically named field beside `source` in the payload is the connector's own processing time
and never reaches a source-properties map.
DataStream applications can apply the same parser with `DebeziumSpannerCdcSequenceNumberProvider`.

The native Change Streams source of this repository already exposes those coordinates as typed
metadata, so it uses `spanner-change-sequence` instead and needs no Debezium-shaped map.
Forward the source's `commit-timestamp`, `sequence`, and `mod-number` metadata columns into that
row.
Declare the commit-timestamp column as `TIMESTAMP_LTZ(9)`: a watermark declaration forces precision
3, and a millisecond-truncated commit timestamp makes two changes of one key within one millisecond
compare equal on their first section.
DataStream applications reading the native source call `SpannerCdcSequenceNumber.of(...)` with the
same three values.
Both routes produce the same sequence for the same change; a record sequence is compared
numerically, so Spanner's zero-padded form and Debezium's unpadded form agree.

Missing, negative, non-numeric, and overflowing values fail through the configured failure handler.
The profile never substitutes a processing timestamp.
That matters because Debezium's Spanner connector writes low-watermark stamps into the same data
change topic when `gcp.spanner.low-watermark.enabled` is set: they carry `op` `m`, no row, and a
`source` object whose record sequence and mod number are absent and whose timestamps Debezium fills
from the connector's own clock.
This profile rejects such a record rather than ordering rows against that clock, and the example
adapter below rejects it one step earlier on its unsupported operation.
Leave that option at its default `false`, or give the reading job a failure policy for those
records, because Flink's `debezium-json` format cannot deserialize a row-less envelope either.
The profile needs no configuration, because Spanner assigns commit timestamps itself and a
change-stream reader inherits that order without a failover epoch.

The three sections order every record of one transaction, and order two transactions whose commit
timestamps differ.
Spanner assigns [distinct commit timestamps to transactions that write overlapping sets of
fields](https://cloud.google.com/spanner/docs/commit-timestamp), so repeated changes to one set of
columns are ordered by the first section alone.
Two transactions that write *disjoint* fields may share a commit timestamp, and a record sequence
counts within its own transaction rather than across transactions, so a pair like that can encode
to one sequence.
BigQuery then resolves it by ingestion order, in the same way it resolves the PostgreSQL profile's
shared LSN and the TiCDC profile's shared commit TSO.
A workload that updates one row from two transactions touching different columns, and that needs
those two ordered, must supply its own total order through `change-sequence-number` or a custom
`CdcSequenceNumberProvider`.

The `snapshot` property is ignored: the Spanner connector streams a change stream and has no
snapshot phase whose value could select a different ordering.

### Table creation

Setting any one of these builds a `TableCreateOptions`; the rest stay at the connector's defaults.
Partitioning and clustering apply **only when the sink creates the table** — an existing table is
never repartitioned or reclustered by them, whatever the DDL says.

They do not *authorize* creation: `sink.create-disposition` does, and it defaults to
`create-if-needed`, so the settings alone configure the table an unconfigured DDL already creates.
Setting any of them beside an explicit `create-never` is rejected.

A column BigQuery could not use is rejected at plan time rather than at the first record: one the
table does not declare, a partitioning column that is not `TIMESTAMP`, `TIMESTAMP_LTZ` or `DATE`,
an `hour` granularity over a `DATE` column (a `DATE` column has day, month and year granularity
only), and a repeated or nested clustering column — BigQuery clusters on top-level, non-repeated
columns of a scalar type, which an array, map, multiset or row column is not. Which *scalar* types
are clusterable is left to the service: that list has grown before, and a stale copy here would
refuse a table BigQuery would have created.

| Option | Type | Maps to |
|---|---|---|
| `sink.table-create.time-partitioning.type` | Enum | `TableCreateOptions.timePartitioning(...)` — `hour`, `day`, `month` or `year` |
| `sink.table-create.time-partitioning.field` | String | The `TIMESTAMP`, `TIMESTAMP_LTZ` or `DATE` column to partition on; a `DATE` column takes no `hour` granularity. Left out, the table is partitioned on **ingestion time** — the case `PARTITIONED BY` could not express. Requires the granularity above |
| `sink.table-create.time-partitioning.expiration` | Duration | `TableCreateOptions.timePartitioningExpiration(...)`. Requires the granularity above |
| `sink.table-create.clustered-fields` | List&lt;String&gt; | `TableCreateOptions.clusteredFields(...)`, in precedence order; BigQuery takes at most four top-level columns |

### Sink tuning — `storage-api-at-least-once`

Setting any one of these builds a `DefaultStreamOptions`; the rest stay at the connector's
defaults.

| Option | Type | Maps to |
|---|---|---|
| `sink.default-stream.max-append-request-bytes` | MemorySize | `maxAppendRequestBytes(...)` |
| `sink.default-stream.recovery.initial-backoff` | Duration | `recoveryInitialBackoff(...)` |
| `sink.default-stream.recovery.max-backoff` | Duration | `recoveryMaxBackoff(...)` |
| `sink.default-stream.recovery.max-attempts` | Integer | `recoveryMaxAttempts(...)` |
| `sink.default-stream.retry.initial-delay` | Duration | `retryInitialDelay(...)` |
| `sink.default-stream.retry.delay-multiplier` | Double | `retryDelayMultiplier(...)` |
| `sink.default-stream.retry.max-delay` | Duration | `retryMaxDelay(...)` |
| `sink.default-stream.retry.max-attempts` | Integer | `retryMaxAttempts(...)` |
| `sink.default-stream.retry.max-duration` | Duration | `maxRetryDuration(...)` |
| `sink.default-stream.max-inflight-requests` | Integer | `maxInflightRequests(...)` |
| `sink.default-stream.max-inflight-bytes` | MemorySize | `maxInflightBytes(...)` |
| `sink.default-stream.min-connections-per-region` | Integer | `minConnectionsPerRegion(...)` |
| `sink.default-stream.max-connections-per-region` | Integer | `maxConnectionsPerRegion(...)` |
| `sink.default-stream.destination-idle-timeout` | Duration | `destinationIdleTimeout(...)` |
| `sink.default-stream.flush-interval` | Duration | `flushInterval(...)` |
| `sink.default-stream.metrics.per-destination` | Boolean | `perDestinationMetrics(...)` |

### Sink tuning — `storage-api-exactly-once`

The connector's own `BufferedStreamOptions`, which this write method **requires**. Unlike the
family above, a DDL that selects the write method and sets none of these still gets one, with every
knob at its default — so there is no key to remember beside `sink.write-method`.

Worth knowing before the first run of a job that auto-creates its table: each writer subtask creates
the stream itself, and on a missing table each one attempts the creation. Against BigQuery's
per-table metadata quota that is a race — measured at parallelism 10, and again by racing sixteen
creations directly, where the service answered HTTP 403 `rateLimitExceeded`, *"Exceeded rate limits:
too many table update operations for this table"*. The connector retries the creation itself within
`sink.buffered-stream.recovery.*`, so losing the race costs a backoff rather than the job. The
budget is still finite, so on a very large cluster the cheaper answer is to skip the race: create
the table first and use `'sink.create-disposition' = 'create-never'`, or cap `sink.parallelism` for
the run that creates it.

There is no connection-pool group here: unlike the default-stream path, these appenders never enter
the SDK's connection pool, so there is nothing for its sizing knobs to size.

| Option | Type | Maps to |
|---|---|---|
| `sink.buffered-stream.max-append-request-bytes` | MemorySize | `maxAppendRequestBytes(...)` |
| `sink.buffered-stream.destination-idle-timeout` | Duration | `destinationIdleTimeout(...)` |
| `sink.buffered-stream.recovery.initial-backoff` | Duration | `recoveryInitialBackoff(...)` |
| `sink.buffered-stream.recovery.max-backoff` | Duration | `recoveryMaxBackoff(...)` |
| `sink.buffered-stream.recovery.max-attempts` | Integer | `recoveryMaxAttempts(...)` |
| `sink.buffered-stream.retry.initial-delay` | Duration | `retryInitialDelay(...)` |
| `sink.buffered-stream.retry.delay-multiplier` | Double | `retryDelayMultiplier(...)` |
| `sink.buffered-stream.retry.max-delay` | Duration | `retryMaxDelay(...)` |
| `sink.buffered-stream.retry.max-attempts` | Integer | `retryMaxAttempts(...)` |
| `sink.buffered-stream.retry.max-duration` | Duration | `maxRetryDuration(...)` |

### Sink tuning — `file-loads`

The connector's own `FileLoadsOptions`, which this write method **requires**. So is
`sink.file-loads.staging-path`: it is the one *conditionally* required key on this page — no
default, and required by the write method rather than by the connector — and leaving it out under
`file-loads` is rejected when the plan is built, naming the key.

| Option | Type | Maps to |
|---|---|---|
| `sink.file-loads.staging-path` | String | `stagingPath(...)` — `gs://bucket` or `gs://bucket/prefix`. **Required** under this write method |
| `sink.file-loads.temp-dataset` | String | `tempDataset(...)`, holding leaf, intermediate and aggregate temporary tables when a load is too large for one job or replacement rows span staging formats. It must share the final destination's BigQuery location. Absent, each destination table's own dataset |
| `sink.file-loads.write-disposition` | Enum | `writeDisposition(...)` — `write-append`, `write-truncate`, `write-truncate-data` or `write-empty`. `write-truncate-data` preserves the existing table schema and constraints. Streaming execution accepts `write-append` only, since every checkpoint commits its own staged files |
| `sink.file-loads.min-checkpoint-interval` | Duration | `minCheckpointInterval(...)`, the smallest checkpoint interval streaming execution accepts. Lowering it is an explicit opt-in: each checkpoint consumes at least one destination-table modification, a direct load or an overflow copy, against BigQuery's daily limits |
| `sink.file-loads.max-staging-file-bytes` | MemorySize | `maxStagingFileBytes(...)`, the size at which an open staging file is finished and the next one opened. The default is measured — see [File loads]({{< relref "docs/connectors/datastream/bigquery" >}}#file-loads) — and raising it matters mainly for a very large volume to one destination, since the 10,000-URI per-load-job cap is a file count |
| `sink.file-loads.staging-format` | Enum | `stagingFormat(...)` — `avro` (default) or `parquet`. Parquet needs `parquet-avro`, and a Hadoop runtime unless the compression is `none`, on the cluster's classpath; a destination whose schema has a `JSON` column stages Avro whatever this says |
| `sink.file-loads.parquet-compression` | Enum | `parquetCompression(...)` — `zstd` (default) or `none`. Rejected when the staging format is `avro` |
| `sink.file-loads.load-job-poll.initial-backoff` | Duration | `loadJobPollInitialBackoff(...)` |
| `sink.file-loads.load-job-poll.max-backoff` | Duration | `loadJobPollMaxBackoff(...)` |
| `sink.file-loads.schema-reconcile.initial-backoff` | Duration | `schemaReconcileInitialBackoff(...)` |
| `sink.file-loads.schema-reconcile.max-backoff` | Duration | `schemaReconcileMaxBackoff(...)` |
| `sink.file-loads.schema-reconcile.max-attempts` | Integer | `schemaReconcileMaxAttempts(...)` |
| `sink.file-loads.metrics.per-destination` | Boolean | `perDestinationMetrics(...)` |

## Type mapping

A sink derives the BigQuery column type from the SQL declaration and uses that schema when it
creates a missing table.
A source reads the Storage Read API's Avro field by physical name and converts it to the declared
SQL type.
The DDL must therefore agree with the source table or query result because planning does not fetch
the live schema.

| Flink declaration | BigQuery source column | BigQuery sink column |
|---|---|---|
| `CHAR`, `VARCHAR`, `STRING` | `STRING`, `JSON` or `GEOGRAPHY` | `STRING`, or `JSON` / `GEOGRAPHY` when marked |
| `BOOLEAN` | `BOOL` | `BOOL` |
| `BINARY`, `VARBINARY`, `BYTES` | `BYTES` | `BYTES` |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | `INT64` | `INT64` |
| `FLOAT`, `DOUBLE` | `FLOAT64` | `FLOAT64` |
| `DECIMAL(p, s)` | A fitting `NUMERIC` or `BIGNUMERIC` value | `NUMERIC` when `s <= 9` and `p - s <= 29`; otherwise `BIGNUMERIC` |
| `DATE` | `DATE` | `DATE` |
| `TIME(p)` | `TIME`; Flink 1.20 and 2.2 resolve SQL `p` to `0`, while 2.3 retains it through `3` | `TIME`; the same Flink-version boundary applies |
| `TIMESTAMP(p)` | `DATETIME`; `p > 6` is rejected | `DATETIME`; `p > 6` is rejected |
| `TIMESTAMP_LTZ(p)` | `TIMESTAMP`; `p > 6` is rejected | `TIMESTAMP`; `p > 6` is rejected |
| `ROW` | `STRUCT`, recursively | `STRUCT`, recursively, or `JSON` when marked |
| `` ROW<`start` DATE, `end` DATE> `` | `RANGE<DATE>`; source only | `STRUCT<start DATE, end DATE>` |
| `` ROW<`start` TIMESTAMP(p), `end` TIMESTAMP(p)> `` | `RANGE<DATETIME>`; source only | `STRUCT<start DATETIME, end DATETIME>` |
| `` ROW<`start` TIMESTAMP_LTZ(p), `end` TIMESTAMP_LTZ(p)> `` | `RANGE<TIMESTAMP>`; source only | `STRUCT<start TIMESTAMP, end TIMESTAMP>` |
| `ARRAY<T>` | `REPEATED T`; declarations may use nullable elements although BigQuery values contain none | `REPEATED T`; nested arrays and nullable element declarations are rejected |
| `MAP<K, V>`, `MULTISET<T>` | `REPEATED STRUCT<key, value>` | `REPEATED STRUCT<key, value>` |
| `TIMESTAMP WITH TIME ZONE`, `INTERVAL`, `RAW`, `NULL`, structured and distinct types | rejected | rejected |

Unsupported Flink logical types are rejected on the client when the job graph is built.
A DDL that disguises BigQuery `INTERVAL` as a `ROW` is rejected from the live writer schema before
the first value is converted.
A source decimal that does not fit the declared precision fails the read rather than becoming
`NULL`.

### `RANGE` is source-only

The Storage Read API represents `RANGE<T>` as a record with nullable `start` and `end` fields.
Declare that record as one of the three `ROW` shapes in the table above.
A null endpoint means that side is unbounded, while a null `ROW` means the range value itself is
null.

The sink does not infer `RANGE` from a two-field `ROW`.
The same declaration sent to a sink creates a BigQuery `STRUCT`, which keeps source and sink schema
derivation deterministic instead of guessing from field names.

### `INTERVAL` remains unsupported

BigQuery returned `INTERVAL` as an undocumented Avro record containing months, days and
microseconds in a real-service measurement on 2026-08-13.
Flink divides intervals into a year-month value and a day-time value, so neither type can preserve
all three components.
Flink's internal day-time value is also measured in milliseconds and cannot preserve the observed
microseconds.

The source therefore rejects both Flink interval types and rejects declaring BigQuery `INTERVAL`
as a raw `ROW<months INT, days INT, microseconds BIGINT>`.
If the textual representation is sufficient, use a query source that casts the BigQuery interval
to `STRING` and declare that result column as Flink `STRING`.
The sink continues to reject interval declarations because it has no lossless BigQuery mapping.

### `TIMESTAMP` is civil and `TIMESTAMP_LTZ` is an instant

`TIMESTAMP` is a wall-clock type, so it becomes BigQuery's `DATETIME`; `TIMESTAMP_LTZ` is an
instant, so it becomes `TIMESTAMP`. The GoogleCloudDataproc connector maps these the other way
round, which stores a wall-clock value as an instant and an instant as a wall-clock value. If you
are migrating from it, this is the row to check.

### SQL `TIME` precision depends on the Flink version

BigQuery Storage Read returns `TIME` as Avro `time-micros`. Flink 1.20 and 2.2 resolve a SQL
declaration such as `TIME(3)` to `TIME(0)` before the connector sees the schema, so source and sink
SQL paths on those versions carry only whole seconds. Flink 2.3 and newer retain the declared
precision and the connector preserves milliseconds through `TIME(3)`. Use a query source that casts
the value to `STRING` when fractional text must be preserved on the earlier versions.

A catalog schema constructed programmatically can carry `TIME(1)` through `TIME(3)` to the
connector on every supported version, whose internal value stores milliseconds. The connector
rejects a programmatic precision above `3` rather than claiming it can preserve BigQuery's
microseconds.

### Marked columns

`sink.json-field-paths` and `sink.geography-field-paths` name columns by dotted path — `payload`,
`event.body`, `the_map.value`. A map's key cannot be marked.

A marked `STRING` is passed through **verbatim and unvalidated**: malformed JSON or an invalid
geometry is a row-level error BigQuery reports, exactly as on the other write paths. A marked `ROW`
is different — it is rendered as JSON text rather than expanded into a `STRUCT`, so its columns
become object members. Strings are escaped, `BYTES` becomes base64, `DECIMAL` an unquoted number,
and the temporal types ISO-8601 strings. A `MULTISET` has no JSON form and is rejected, as is a map
with non-string keys. `GEOGRAPHY` may only mark a `STRING`: no structured value means a geometry to
BigQuery.

## Schema evolution

The two `sink.schema-update.*` options are accepted under every write method.
Whenever the connector reconciles rather than replaces a table schema, they select the same additive union rules: existing fields are not removed, reordered or retyped, new fields are added as `NULLABLE`, and `REPEATED` modes do not change.

`sink.derive-required-columns` and `sink.schema-update.allow-field-relaxation` operate at different stages.
The former changes the desired schema derived from the DDL: when enabled, a `NOT NULL` column becomes `REQUIRED`; when disabled, every non-repeated derived column is `NULLABLE`.
The latter permits the connector to relax an existing table column from `REQUIRED` to `NULLABLE` when the DDL-derived schema is nullable.
During additive reconciliation it never tightens a `NULLABLE` table column to `REQUIRED`, and a new `REQUIRED`-derived column is added to an existing table as `NULLABLE` because BigQuery cannot add required fields.
Changing `sink.derive-required-columns` alone therefore does not rewrite an existing table.

The selected write method decides when that union is applied.
`storage-api-at-least-once` rebuilds its default-stream writer after reconciling the table.
`storage-api-exactly-once` drains old-schema rows and reconnects the same buffered stream at the same next offset.
`file-loads` reconciles once per destination before loading each batch run or streaming checkpoint, then puts the reconciled schema on every load job.
For `file-loads`, `write-append` and `write-truncate-data` jobs also carry BigQuery's native schema-update options, `write-empty` relies on the connector's pre-load reconciliation, `write-truncate-data` preserves the live schema and constraints, and `write-truncate` replaces the table schema instead of reconciling it.

See [Schema evolution]({{< relref "docs/connectors/datastream/bigquery" >}}#schema-evolution) on the DataStream page for the nullability result table, failure behavior, propagation waits and serializer compatibility rules.

## Delivery guarantees

`sink.write-method` decides them. **In streaming execution all three need checkpointing enabled**
(`execution.checkpointing.interval`), since a checkpoint is what makes rows durable, visible or
loaded; batch execution needs none, because the sink flushes or loads at end of input. See
[Delivery guarantees]({{< relref "docs/connectors/datastream/bigquery" >}}#delivery-guarantees-and-state)
on the DataStream page for the full statement; what a SQL user needs is:

- **`storage-api-at-least-once`** (the default) — rows are durable once a checkpoint completes, and
  a restart may re-append rows the previous attempt had already written.
- **`storage-api-exactly-once`** — rows become visible only when a completed checkpoint commits
  them, so a restart **from a checkpoint or savepoint** neither loses nor duplicates. A *stateless*
  redeploy is the exception, and it loses: rows appended but not yet flushed stay invisible forever.
  The checkpoint interval is therefore the visibility latency as well. A schema change drains rows
  encoded with the old descriptor, widens the table when a `sink.schema-update.*` option permits it,
  and reconnects the same buffered stream at its next offset. It needs Flink's `EXACTLY_ONCE`
  checkpointing mode with checkpoints-after-tasks-finish enabled. Both are already Flink's defaults;
  a cluster that overrides either has the job refused when the graph is built.
- **`file-loads`** — always exactly-once, by staging rows as files on Cloud Storage and importing
  them with load jobs. In streaming execution each checkpoint issues at least one load job per
  table, against BigQuery's quota of **1,500 per table per day**, so the checkpoint interval has a
  floor — two minutes — that `sink.file-loads.min-checkpoint-interval` lowers only as an explicit
  opt-in; and each checkpoint appends, so `write-truncate`, `write-truncate-data` and `write-empty`
  are refused there.
  Both refusals arrive when the plan is built, and each names the `sink.file-loads.*` key you
  would change. The quota is per *table* while the floor is checked per *job*, so two jobs writing
  one table, or two `INSERT INTO` statements in one `StatementSet`, each pass the check and
  together double the cadence.
  Batch execution, `SET 'execution.runtime-mode' = 'batch'`, loads everything at end of input and
  takes any disposition.

The [worked examples]({{< relref "docs/examples/bigquery" >}}) carry what neither of those bullets
can: how to redeploy an exactly-once job without losing the rows a discarded checkpoint was holding,
and why a FILE_LOADS staging bucket wants to be a dedicated one with a lifecycle rule sized above
the longest outage the job must recover from.

### Inserts only, unless CDC is enabled

Without `sink.cdc.enabled`, the changelog mode is insert-only. An updating query — an aggregation
without a window, a non-windowed join — is rejected when the plan is built, because BigQuery's
append-only write paths cannot express a retraction and appending the `-U` and `-D` rows as ordinary
ones would corrupt the table silently.

Setting `sink.cdc.enabled` to `true` makes the sink answer an updating query with an upsert changelog
instead, so the same query plans; an insert-only query still gets insert-only. That path augments
each row with BigQuery's CDC pseudocolumns, and it carries its own restrictions — it runs on
`storage-api-at-least-once` and no other write method. See
[Change data capture](#change-data-capture).

## Design decisions

**A row that fails to convert fails the job.** The failed-row handler is not exposed from SQL: there
is no way to name a dead-letter queue in a `WITH` clause, and a dropping policy is a decision to
make against a concrete need rather than in advance.

**`PARTITIONED BY` is rejected rather than consumed.** Flink's clause models Hive-style value
partitioning, which BigQuery time partitioning is not, and ingestion-time partitioning has no column
to name at all — so the clause could never cover the whole feature. The sink does not implement
`SupportsPartitioning`, which makes a partition spec fail at plan time instead of being silently
ignored. `INSERT OVERWRITE` is refused for the same reason. Partitioning and clustering are
configured by [`sink.table-create.*`](#table-creation) instead.

**A partitioning or clustering column BigQuery could not use fails at plan time.** The service
refuses such a table at creation, but the sink only gets there at the first record, from inside a
task — and the emulator accepts every one of these without complaint, so a test suite alone would
not notice. In SQL the DDL *is* the created table's schema, so the mistake is visible while the job
graph is being built, and that is where it is reported. The DataStream API makes no such check: its
schema comes from the serializer, per destination, and is not in hand when the options are
configured. Names are matched case-insensitively, and the value reaches BigQuery exactly as
written.

What is checked is the column's *shape*, never a list of types that could grow: existence, the
three types time-unit partitioning is defined over, the `DATE`-has-no-hour rule, and "top-level,
non-repeated, scalar" for clustering. A clustering column of a scalar type BigQuery happens not to
accept today — `DOUBLE`, `TIME` — still reaches the service, deliberately: encoding that list here
would buy an earlier failure at the risk of refusing a table a later BigQuery would create.

**A tuning key of a write method you did not select is rejected, not ignored.** Each write method
owns one options object on the DataStream API, and the builder already refuses a mismatched pair —
but its message names `bufferedStreamOptions(...)`, a method a SQL user never called and cannot
call. The connector therefore restates the rule in option keys, naming the offending ones.
The emulator endpoints remain a separate incompatibility under `file-loads`, which stages to Cloud
Storage that no emulator provides.

**The two required families are built from the write method, not from key presence.** Selecting
`storage-api-exactly-once` or `file-loads` and tuning nothing is a complete configuration: every
knob of `BufferedStreamOptions` has a default, and `FileLoadsOptions` needs only its staging path.
`sink.default-stream.*` is the one family whose absence means absence, because its write method is
chosen by not choosing.

**Metadata columns are CDC-only and write-only.** A BigQuery row has no envelope around it, so the
source exposes no readable metadata.
The sink exposes only the sequence metadata documented under
[Change data capture](#change-data-capture), and keeps it outside the physical BigQuery schema.

**One table per SQL table.** Per-record routing and per-destination creation options stay on the
DataStream API; a SQL `INSERT INTO` names one table, and a table-name pattern is deferred until a
concrete need appears.

**Enum options carry their DDL spelling in `toString()`**, so `sink.write-method` takes
`storage-api-at-least-once` rather than the Java constant. Flink resolves an enum-valued option by
matching `toString()` case-insensitively and normalizing nothing else.

## Testing

The unit tests drive the factory without a planner and cover the Storage Read Avro-to-`RowData`
mapping with synthetic writer schemas and records. The integration tests run `CREATE TABLE`,
bounded `SELECT`, and `INSERT INTO` through the planner against the goccy/bigquery-emulator
container, with the two emulator endpoints interpolated into the `WITH` clause — so they exercise
the production factory rather than a test seam. That Table source round trip covers `INT64` and
`STRING`; the underlying DataStream source has gated real-GCP coverage, but it returns
`GenericRecord` and therefore does not independently prove the Table source's conversion of
emulator-unsupported types.

`sink.table-create.*` needs both levels, and for a reason the emulator states by omission: it
stores a create request's partitioning and clustering verbatim and **validates nothing**, so
`BigQueryTableCreateOptionsITCase` can show the settings survive the mapper but not that BigQuery
would accept them. The gated `BigQueryTableCreationFidelityITCase` is what measures that, against
the real service.

**Neither write method beyond the default has a table-level round trip the emulator can carry**, so
both are gated: `BigQueryTableExactlyOnceITCase` and `BigQueryTableFileLoadsITCase`, against real
BigQuery and real Cloud Storage. `file-loads` stages to Cloud Storage, which nothing here
stands in for. `storage-api-exactly-once` was tried and dropped — the emulator assigns its own
append offsets instead of honoring the requested one, and keeps no flush cursor, so the writer's
offset check fails on the first append. What the emulator suite still covers is the plan-time
refusals, in the planner where a SQL user meets them.

The uber-jar is covered separately, in `flink-sql-connector-gcp-bigquery`.

- `BigQuerySqlConnectorPackagingITCase` reads the built jar: the factory SPI file SQL discovers the
  connector through, that every artifact on the runtime classpath contributed its classes, that no
  class outside the shaded prefix is missing from a short documented allow-list — and that no entry
  on that allow-list is dead, since an exemption matching nothing silently covers whatever arrives
  under it later — that the netty native libraries were renamed to match their relocated package,
  that the relocated gRPC service file names the relocated provider, that no service file hands a
  relocated implementation to an interface this jar does not own — bar one documented exemption,
  netty's BlockHound integration, which is inert unless that test-time agent is present — that
  every licence text checked in reached the jar, and that the `NOTICE` claims no Apache provenance.
- `BigQuerySqlConnectorSmokeITCase` runs a SQL `INSERT` and bounded `SELECT` against the emulator
  **through the shaded classes** — the module's surefire configuration drops the connector artifact from the test
  classpath and adds the uber-jar, and the test asserts the factory really did load from there,
  because a regression in that setup would leave every other assertion about the wrong code. This
  is the only test that exercises relocation at runtime, and it lets the sink create its own table,
  so both transports are driven: the relocated REST client for the metadata half and the relocated
  gRPC one for the rows. The harness drives the emulator with the *stock*, unrelocated BigQuery
  client, so the two coexisting on one classpath is itself part of what is asserted.
- `BundledDependenciesNoticeTest` diffs `META-INF/NOTICE` against the runtime dependency tree
  recorded during the build, in both directions. The bundle is the whole runtime classpath, so a
  new transitive is bundled automatically; this test is what makes it fail the build until the
  NOTICE is regenerated to record it.

All three are shared with `flink-sql-connector-gcp-pubsub` rather than copied: the checks live in
`flink-connector-gcp-test-utils`, and each module contributes its artifact id, its shaded prefix,
its factory and its own package root.
