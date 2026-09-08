# flink-sql-connector-gcp-bigquery

The BigQuery connector packaged for SQL users supports bounded table and query reads, all three sink write methods, and experimental CDC upsert/delete ingestion with a declared primary key on the at-least-once default stream.
It is one jar to drop into Flink's `lib/`, bundling
`flink-connector-gcp-bigquery` and its whole runtime tree — the BigQuery Storage Read, Storage Write and REST
clients, the Cloud Storage client, gRPC, protobuf, Avro, Guava and the rest — with every bundled
package relocated so it cannot collide with anything else on the classpath.

There are no production Java sources here. The connector, its options and its behaviour are
documented with the connector itself:

- [`flink-connector-gcp-bigquery`](../flink-connector-gcp-bigquery/README.md) — the module, and the
  implementation-status tables
- [SQL connector documentation](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/table/bigquery/) —
  bounded sources, sink write methods, CDC ingestion, the DDL option surface, the type mapping, and how to put this jar on the classpath
- [DataStream connector documentation](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/bigquery/) —
  bounded reads, write methods, CDC ingestion, delivery guarantees, tuning and error handling

```sql
CREATE TABLE orders (
  order_id STRING,
  amount   BIGINT,
  event_ts TIMESTAMP_LTZ(6)
) WITH (
  'connector' = 'bigquery',
  'project'   = 'my-project',
  'dataset'   = 'analytics',
  'table'     = 'orders'
);
```

This jar is for SQL. A DataStream job should depend on `flink-connector-gcp-bigquery` instead: the
uber-jar relocates Avro, so the `AvroRecordSerializationSchema` inside it takes a relocated `IndexedRecord`
that an ordinary job cannot supply.

## Bundled dependencies

The jar redistributes third-party binaries. `META-INF/NOTICE` inside it enumerates every bundled
artifact grouped by licence, and `META-INF/licenses/` carries the text of each non-Apache-2.0
licence. Both are checked in under `src/main/resources/` and both are held to the build: the
NOTICE's artifact lists are generated from what Maven actually resolves (the prose lives in
`NOTICE.template`), each licence text is pinned by sha256 to a recorded source, and CI fails on any
drift — a dependency added, removed or re-licensed upstream cannot ship unrecorded.

## Provenance and attribution

No code has been copied into this module.
Its Java sources support packaging, NOTICE, and smoke verification only.
The build follows the shape of Apache Flink's own SQL
connector modules — `flink-sql-connector-kafka` for the overall module layout and the `shade-flink`
execution inherited from `flink-connector-parent` — and, more directly, the sibling
`flink-sql-connector-gcp-pubsub` in this repository, which established the shading and licensing
decisions this module reuses. Those are **design references only**; the shade configuration here
was written for this project.
