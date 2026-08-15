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

# ADR-0111: The BigQuery table sink maps upsert changelogs and keeps CDC metadata out of the schema

- Status: Accepted
- Date: 2026-08-14; revised by [#629], [#631] and [#717] (2026-08-15)
- Issues: [#65](https://github.com/laughingman7743/flink-connector-gcp/issues/65),
  [#626](https://github.com/laughingman7743/flink-connector-gcp/issues/626),
  [#629](https://github.com/laughingman7743/flink-connector-gcp/issues/629),
  [#631](https://github.com/laughingman7743/flink-connector-gcp/issues/631),
  [#717](https://github.com/laughingman7743/flink-connector-gcp/issues/717)
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/table/bigquery.md#change-data-capture`

## Context

ADR-0110 added DataStream CDC primitives while leaving Flink SQL changelog conversion and
sequence extraction to the table layer.
Flink represents an upsert sink with `INSERT`, `UPDATE_AFTER`, and `DELETE` rows, while BigQuery
requires `UPSERT` or `DELETE` plus optional sequence metadata.
Flink also appends selected writable metadata fields after the physical row, so treating the
consumed runtime type as the BigQuery schema would create columns that do not belong to the table.

Delete rows present a second mismatch.
An upsert changelog may contain only the key on a delete, but ordinary protobuf validation requires
every physical field derived as `REQUIRED`.
The table sink therefore needs a delete representation that remains valid on the wire without
weakening validation for inserts and upserts.

## Decision

**Table CDC is explicit and stays on the Storage Write API default stream.**
`sink.cdc.enabled` defaults to `false` and requires both
`sink.write-method=storage-api-at-least-once` and a declared Flink primary key.
When enabled, the sink accepts an upsert changelog, maps `INSERT` and `UPDATE_AFTER` to `UPSERT`,
maps `DELETE` to `DELETE`, and rejects `UPDATE_BEFORE` at the serializer and change-type
boundaries.
A primary-key change is represented as a delete of the old key followed by an upsert of the new
key; interpreting every update-before row as a delete would also manufacture deletes for ordinary
updates.

**Delete serialization selects physical primary-key fields only.**
The selected converter retains the physical descriptor's original field numbers and builds a
partial protobuf message, which permits omitted required non-key fields on the wire.
The full-row converter continues to build a fully initialized protobuf message, so inserts and
upserts retain their existing required-field validation.

**Writable CDC metadata is separate from the physical row type.**
The sink exposes `change-sequence-number` as a nullable string, `debezium-source-properties` as a
nullable string map, and `spanner-change-sequence` as a nullable row.
The planner may select no sequence source or exactly one; selecting one while CDC is disabled or
selecting more than one fails during planning.
The physical row type remains the sole input to BigQuery schema derivation, and the selected
metadata value is read at the physical arity after Flink appends it to the runtime row.

**The Debezium map is a stable hand-off to connector-specific profiles, not a timestamp
fallback.**
The source-owned key remains `value.source.properties`, while the BigQuery sink owns the writable
metadata key `debezium-source-properties`.
The map's `connector` value routes to a profile that selects ordering fields whose semantics are
defined by that connector.
An unavailable profile fails explicitly; `ts_ms` is not used alone because multiple changes in
one millisecond could then receive the same sequence and fall back to ingestion order.
The `RowData` provider reads only the selected writable metadata position and delegates Debezium
maps to a separate connector resolver.
That resolver owns connector dispatch and profile-specific property extraction, so adding MySQL or
Spanner does not add source-specific branches to the `RowData` adapter.

Flink's `debezium-avro-confluent` format produces the physical-row changelog from `before`,
`after` and `op`, but does not retain the envelope's `source` record in that changelog.
The raw `avro-confluent` format can retain the complete envelope, but advertises an insert-only
changelog.
A projection that selects `before` or `after` with a `CASE` expression changes column values, not
the INSERT RowKind supplied by that source, so it cannot make a Debezium delete into a sink delete.
An Avro Kafka path that needs connector-specific ordering therefore keeps the complete envelope as
a `GenericRecord` and either calls the public
`DebeziumPostgreSqlCdcSequenceNumberProvider` from its DataStream CDC configuration or registers a
changelog DataStream carrying the source-properties map before entering SQL.
This adapter boundary is shared by the PostgreSQL, MySQL and Debezium Spanner profiles; the native
Spanner source instead exposes its own typed ordering metadata through the `spanner-change-sequence`
column recorded below.

**The PostgreSQL profile transcodes Debezium's two-part sequence into BigQuery sections.**
Only `connector=postgresql` selects this profile.
It parses Debezium's `sequence` as the nullable last committed LSN followed by the required current
LSN, represents a null last committed position as zero, and encodes both as fixed-width unsigned
64-bit hexadecimal sections.
A non-null `lsn` property must represent the same 64-bit value as the current sequence position.
Malformed or contradictory metadata fails the row instead of changing its ordering source.

This is not a parser for PostgreSQL's customary `X/Y` LSN display form.
PostgreSQL defines `pg_lsn` internally as an unsigned 64-bit byte position in the WAL; Debezium
exposes the same bits as signed decimal strings through `Lsn.asLong()`.
The encoder accepts both signed and unsigned decimal spellings, preserves their bit pattern, and
writes each value as one BigQuery hexadecimal section.
BigQuery compares those sections as unsigned numbers, first section before second.

The last-committed position is retained rather than using `source.lsn` alone.
PostgreSQL logical messages can share a current LSN across a transaction boundary, and Debezium's
resume logic separately records the last committed position for that reason.
The pair distinguishes progress into the next transaction even when its first event repeats the
previous commit's current LSN.
It remains possible for multiple events within one transaction to share both positions; Debezium
keeps an internal per-LSN event counter for precise restart recovery, but does not expose that
counter in `source.sequence`.
The pair is therefore deterministic and ordered for continuous tuple progress, but is not a unique
total-order identifier for every event.
If distinct events for the same BigQuery primary key share the pair, BigQuery falls back to system
ingestion time and this provider alone cannot make an out-of-order replay deterministic.
Applications that admit that case must provide a stable additional tie-breaker through the
formatted metadata route or a custom DataStream provider.

Replaying one source event reproduces its sequence without connector state.
That property also holds across a PostgreSQL primary failover when Debezium continues from the same
preserved and synchronized logical replication slot.
A recreated slot or another discontinuous LSN history is unsupported because the source map exposes
no timeline or ordering epoch that could disambiguate it.
The user documentation links to Debezium's PostgreSQL failover guidance rather than duplicating its
version-specific operational procedure.

**The MySQL profile assigns append-only source UUID epochs to GTID coordinates.**
It encodes the epoch, transaction id, binlog position, and row as four unsigned 64-bit sections.
Snapshot rows use the all-zero epoch and require an empty target.
Streaming accepts one untagged `UUID:transaction_id`; tagged or multi-source values fail.
Appending a failover UUID makes every event from it sort after every coordinate from an earlier
UUID, so editing, reordering, and interleaving histories are unsafe.
Group Replication does not fit that epoch model.
Transactions originating in one replication group use the fixed group UUID, while each member
reserves its own block of GTID transaction numbers.
The Debezium source map therefore cannot identify a primary epoch or provide a group-wide ordering
coordinate across primary changes or multi-primary histories.
MySQL's GTID log event also contains logical-clock and commit-timestamp fields, and Debezium's
binlog reader parses them.
The logical sequence restarts in each local binlog file, however, and commit timestamps are not a
unique total order; the MySQL connector currently retains only the GTID when handling that event.
Those topologies remain unsupported until MySQL and Debezium can expose a durable group-wide
ordering coordinate in source metadata.

**The TiCDC profile encodes the commit timestamp oracle value as one section.**
Only `connector=TiCDC` selects this profile, which reads the `commit_ts` and `cluster_id` fields
TiCDC adds to its Debezium-compatible envelope from v8.0.0 onwards.
`cluster_id` is the reporting TiCDC cluster's own identifier rather than TiDB's, and it defaults to
`default`, so it separates configured changefeeds rather than proving a database identity.
A commit TSO is a 46-bit physical millisecond timestamp with an 18-bit logical counter, allocated
by one cluster's Placement Driver and therefore already a total order over that cluster's
transactions.
One unsigned 64-bit section carries it without loss, and the oracle survives a TiCDC process or
node failover, so this profile needs neither the MySQL epoch model nor a second coordinate.

The configured `sink.cdc.ticdc.cluster-id` is required and validated per event rather than trusted.
Two TiDB clusters advance independent oracles, so ordering their events in one BigQuery table
would interleave unrelated timestamps; rejecting the foreign event states that at the row instead
of silently producing an ordering that means nothing.
The check is only as strong as the deployment's identifiers: two TiCDC clusters that both keep the
default `default` are indistinguishable in the envelope, which the option's documentation states.

A zero `commit_ts` is rejected. Placement Driver timestamps are strictly positive, while zero is
what the protocol's unused MySQL-inherited coordinates carry, so accepting it would let a
misconfigured or non-TiCDC producer sort ahead of every real change.

Row changes of one transaction share its commit TSO and therefore its sequence, so the commit TSO
orders transactions rather than individual events.
TiDB writes each key at most once per transaction, but TiCDC splits an UPDATE that modifies a
primary or unique key into a DELETE and an INSERT, which is the default for every sink except
MySQL.
A transaction that moves one key's value onto another key, such as a primary-key swap, therefore
emits both a DELETE and an INSERT for one BigQuery primary key at one sequence, and BigQuery
resolves that pair by ingestion time.
This profile cannot close that gap from source metadata: the protocol's `row` and `pos` are
placeholder zeros, and its `ts_ms` is the same commit timestamp oracle value truncated to
milliseconds. Like the PostgreSQL pair, it is deterministic
and ordered for transaction progress without being a unique total order, and applications that
admit such transactions supply a tie-breaker through the formatted metadata route or a custom
DataStream provider.

The source object's `ts_ms` is never a fallback: it truncates the same commit timestamp oracle
value to milliseconds, so it cannot order two transactions committed within one millisecond, which
is the collision the formatted-metadata route exists for.

The profile covers row changes only.
Before TiDB v9.0.0, TiCDC's classic architecture emits nothing else in this protocol while its new
architecture adds DDL and watermark events from v8.5.4-release.1; from TiDB v9.0.0 that new
architecture is the only one, so DDL events always arrive, and watermark events additionally
require `enable-tidb-extension`.
Neither carries a row to write, and neither is a shape Flink's `debezium-json` format can
deserialize, so such a topic needs `ignore-parse-errors` or a changefeed that does not produce
them.
The protocol provides no initial snapshot stream, so it has no counterpart to the MySQL profile's
zero epoch.

**The Spanner profile encodes one contract for two routes.**
A Spanner change is ordered by three fixed-width unsigned 64-bit sections: the commit timestamp in
nanoseconds since the epoch, the record sequence within the transaction, and the mod number within
the change record.
Both routes call one internal encoder over those three values, so equivalence between them holds by
construction rather than by parallel implementations agreeing.
Debezium supplies them as `ts_ns`, `sequence` and `mod_number` under `connector=spanner`; the native
Change Streams source supplies them through the typed `spanner-change-sequence` row and never
converts to a Debezium-shaped map.
A record sequence is parsed numerically, which makes Spanner's zero-padded rendering and Debezium's
unpadded `Long.toString` rendering the same section.

The commit-timestamp section rejects negative values instead of preserving their bit pattern as the
PostgreSQL profile does for LSNs.
An LSN is an opaque 64-bit position whose signed decimal spelling is a rendering artifact, while a
commit timestamp is a physical instant: a negative value would mean a pre-1970 commit, and BigQuery
compares sections as unsigned, so accepting one would sort it after every real timestamp.
The section is therefore bounded at `Long.MAX_VALUE` on both routes, which is also the largest
instant either `TimestampData` or `Instant` can express in nanoseconds.
`snapshot` is ignored: the connector streams a change stream with no snapshot phase, so no value of
that property could select a different ordering.
Nothing falls back to a processing timestamp, which is why a Debezium record without a record
sequence or mod number, such as a low-watermark stamp, is rejected rather than ordered by the
connector's clock — Debezium substitutes its own clock for a missing source timestamp, and that
substitution is not detectable downstream.

The profile needs no configuration.
Spanner assigns commit timestamps itself and a change-stream reader inherits that order, so there is
no counterpart to the MySQL source-UUID epoch list or the TiCDC cluster identity.

The sections are not a total order over every pair of changes to one primary key, and the profile
does not claim to be.
Spanner's uniqueness guarantee is stated over sets of fields rather than over rows: transactions
writing overlapping fields receive distinct commit timestamps, while transactions writing disjoint
fields may share one.
A record sequence is unique and monotonic within one transaction — across that transaction's
partitions, not within a partition — so it does not compare two different transactions.
Two transactions that update disjoint columns of one row at one commit timestamp therefore encode
to the same sequence, and BigQuery resolves them by ingestion order.
That is the same residual tie the PostgreSQL profile carries for a shared LSN and the TiCDC profile
carries for a split primary-key update at one commit TSO, and it is documented rather than papered
over: `server_transaction_id` distinguishes such transactions but is not an order, so no fourth
section could break the tie correctly.

**The changelog-mode compatibility seam is version-specific and internal.**
Flink 2.x receives an upsert mode that advertises key-only deletes, while Flink 1.20 receives the
older upsert mode whose API cannot express that bit.
Runtime serialization supports key-only deletes on every supported version.

## Evidence

- PostgreSQL documents `pg_lsn` as an internal 64-bit WAL byte position with the external `X/Y`
  hexadecimal form and standard comparison operators in the
  [`pg_lsn` type](https://www.postgresql.org/docs/current/datatype-pg-lsn.html).
- Debezium documents `source.sequence` as a JSON array containing the last committed and current
  LSN in that order in the
  [PostgreSQL connector reference](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).
  Its pinned
  [`SourceInfo.sequence()` implementation](https://github.com/debezium/debezium/blob/20a5d35af7ed00f984c15a543e6ee1ca1641630e/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/SourceInfo.java#L180-L197)
  serializes `Lsn.asLong()` as decimal strings.
- The introducing
  [DBZ-2911 commit](https://github.com/debezium/debezium/commit/06b0475f172155517908c256f579897783c60a95)
  describes the pair as PostgreSQL deduplication metadata.
  Debezium's pinned
  [per-LSN counter](https://github.com/debezium/debezium/blob/20a5d35af7ed00f984c15a543e6ee1ca1641630e/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresOffsetContext.java#L185-L209)
  and [DBZ-6204](https://github.com/debezium/debezium/pull/4357) show that multiple logical events
  can share one LSN.
- BigQuery documents one-to-four unsigned hexadecimal sections, lexicographic section comparison,
  and ingestion-time precedence for equal values in its
  [CDC ordering contract](https://cloud.google.com/bigquery/docs/change-data-capture).
- MySQL documents that Group Replication transactions use the fixed group UUID while each member
  reserves and consumes a separate block of consecutive GTIDs in
  [GTIDs and Group Replication](https://dev.mysql.com/doc/refman/8.4/en/group-replication-gtids.html).
- MySQL documents the GTID event's `last_committed`, `sequence_number`, and commit-timestamp fields
  in its
  [binary format](https://dev.mysql.com/doc/dev/mysql-server/8.4.9/classmysql_1_1binlog_1_1event_1_1Gtid__event.html),
  and documents that `sequence_number` restarts in each binary log file in
  [Binary Logging Options and Variables](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html).
- Debezium's binlog dependency parses those fields in
  [`GtidEventDataDeserializer`](https://github.com/debezium/mysql-binlog-connector-java/blob/main/src/main/java/com/github/shyiko/mysql/binlog/event/deserialization/GtidEventDataDeserializer.java),
  while the MySQL connector's
  [`handleGtidEvent`](https://github.com/debezium/debezium/blob/main/debezium-connector-mysql/src/main/java/io/debezium/connector/mysql/MySqlStreamingChangeEventSource.java)
  retains the GTID rather than exposing the additional fields as source metadata.
- Flink 2.2.1's pinned
  [`RegistryAvroFormatFactory`](https://github.com/apache/flink/blob/release-2.2.1/flink-formats/flink-avro-confluent-registry/src/main/java/org/apache/flink/formats/avro/registry/confluent/RegistryAvroFormatFactory.java)
  advertises an insert-only changelog, while its
  [`DebeziumAvroFormatFactory`](https://github.com/apache/flink/blob/release-2.2.1/flink-formats/flink-avro-confluent-registry/src/main/java/org/apache/flink/formats/avro/registry/confluent/debezium/DebeziumAvroFormatFactory.java)
  advertises Debezium row kinds and its
  [`DebeziumAvroDeserializationSchema`](https://github.com/apache/flink/blob/release-2.2.1/flink-formats/flink-avro-confluent-registry/src/main/java/org/apache/flink/formats/avro/registry/confluent/debezium/DebeziumAvroDeserializationSchema.java)
  derives those row kinds from `before`, `after` and `op`.
  Flink's [dynamic-table model](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/concepts/dynamic_tables/)
  treats records from an insert-only stream as INSERT modifications, so projecting the raw
  envelope does not reinterpret its `op` field as a changelog operation.
- Planner coverage proves an upsert changelog retains deletes and projects a selected sequence
  field into writable metadata.
- Factory and sink tests cover the default, primary-key and write-method requirements, the
  cross-version changelog contract, metadata inventory, selection, copying, and CDC propagation.
- Serializer tests prove a primary-key change becomes a sequenced delete of the old key and upsert
  of the new key, a key-only delete omits a required non-key field, ordinary rows keep full
  validation, metadata stays outside the physical schema, and update-before rows fail.
- Adapter tests cover every accepted row kind, formatted sequence extraction, null propagation,
  PostgreSQL snapshot and streaming source shapes, replay and continuous-slot failover ordering,
  unsigned boundaries, malformed metadata, and rejection of unsupported Debezium connectors
  without a timestamp fallback.
- MySQL fixtures cover initial snapshots, create/update/delete streaming shapes, multi-row events,
  replay, appended failover epochs, unsigned boundaries, and invalid topology metadata.
- PingCAP documents TiCDC's Debezium protocol, its `connector` value, and the added `commit_ts` and
  `cluster_id` source fields in
  [TiCDC Debezium Protocol](https://docs.pingcap.com/tidb/stable/ticdc-debezium/) — which also
  states that the classic architecture "only supports Row Changed events and directly ignores DDL
  events and WATERMARK events", while the new architecture sends both from v8.5.4-release.1, a
  split its
  [development version](https://docs.pingcap.com/tidb/dev/ticdc-debezium/) drops in favour of
  "starting from v9.0.0, TiCDC supports DDL events and WATERMARK events" — and documents the
  46-bit physical and 18-bit logical composition of a Placement Driver timestamp in
  [TimeStamp Oracle](https://docs.pingcap.com/tidb/stable/tso/).
- PingCAP documents that for every sink except MySQL, TiCDC splits an UPDATE modifying a primary or
  non-null unique key into a DELETE followed by an INSERT, by default since v6.5.10, v7.1.6,
  v7.5.3, and v8.1.1, and gives a primary-key swap whose one transaction deletes and rewrites the
  same keys, in
  [TiCDC behavior in splitting UPDATE events](https://docs.pingcap.com/tidb/stable/ticdc-split-update-behavior/).
- TiCDC fixtures cover the documented envelope, its physical and logical timestamp components,
  replay after a reversed arrival order, one transaction's row events sharing a sequence that a
  later transaction supersedes, unsigned boundaries, and rejection of malformed, missing, zero,
  overflowing, snapshot-state and foreign-cluster metadata without a timestamp fallback, including
  through the table layer's metadata map for a pre-v8.0.0 envelope.

## Alternatives declined

- **Map `UPDATE_BEFORE` to `DELETE`.**
  This would emit a delete before every ordinary update, and a shared sequence could make the
  winner dependent on service tie-breaking.
- **Serialize every physical column for a delete.**
  Key-only deletes would fail when a non-key column is required, despite BigQuery needing only the
  primary key to identify the row.
- **Add writable metadata fields to the serializer's row type.**
  Table creation and schema reconciliation would then treat CDC control data as physical BigQuery
  columns.
- **Name the sink metadata `value.source.properties`.**
  That key belongs to the source value format and would make the sink's public surface depend on
  one enclosing source connector's metadata prefix.
- **Use `ts_ms` whenever no connector profile is known.**
  Timestamp collisions would silently discard the source system's stronger ordering information.
- **Encode only `source.lsn`.**
  A current LSN can repeat across a commit, the next begin, and its first logical event; retaining
  Debezium's last-committed position preserves the transaction-progress component that a single
  position would discard.
- **Claim that the two values form a unique total order.**
  Debezium itself tracks an event count outside `source.sequence` because multiple events can share
  one LSN; the connector cannot reconstruct an absent tie-breaker.
- **Give TiCDC an ordered cluster-ID list like the MySQL source UUIDs.**
  A failover within one TiDB cluster keeps the same Placement Driver oracle and cluster ID, so the
  list would have exactly one entry; accepting several would ask the connector to order the
  independent oracles of separate clusters, which no source field lets it do.
- **Add the source object's `ts_ms` as a TiCDC tie-breaker below `commit_ts`.**
  It is the same timestamp oracle value truncated to milliseconds, so it adds no ordering
  information the leading section does not already carry.
- **Accept `commit_ts` of zero as an ordinary early timestamp.**
  Placement Driver timestamps are strictly positive, and zero is the value the protocol's unused
  MySQL-inherited coordinates carry, so accepting it would sort a malformed or non-TiCDC envelope
  ahead of every real change.

## Consequences

SQL and Table API jobs can preserve a Flink upsert changelog as BigQuery CDC mutations without
placing pseudocolumns or Debezium maps in the table schema.
Jobs must model a primary-key change as delete plus upsert and must not send update-before rows to
the runtime adapter.
The destination must have a BigQuery primary key.
ADR-0112 records how both CDC APIs can create it and apply optional maximum staleness without
treating a successful but lossy REST field as evidence.
The formatted-sequence route and Debezium PostgreSQL profile are usable immediately.
DataStream jobs and the Table profile share the same strict PostgreSQL sequence encoder through
`DebeziumPostgreSqlCdcSequenceNumberProvider` rather than maintaining application-local parsers.
Debezium JSON can forward `value.source.properties` directly, while Debezium Avro requires the
complete-envelope adapter described above because Flink's standard changelog omits `source`.
MySQL maps use the same adapter boundary with an explicit append-only source UUID list.
TiCDC needs neither, because Flink's `debezium-json` format retains its JSON envelope's `source`
object and the commit TSO orders one cluster without configured epochs; its configuration is the
single cluster identity every event is checked against.
Spanner needs no configuration either, and it is the one source whose native connector in this
repository can bypass the Debezium map entirely through typed metadata.
Any further Debezium map becomes operational one connector profile at a time.
