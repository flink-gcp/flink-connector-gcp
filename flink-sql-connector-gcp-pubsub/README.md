# flink-sql-connector-gcp-pubsub

The Cloud Pub/Sub connector packaged for SQL users: one jar to drop into Flink's `lib/`, bundling
`flink-connector-gcp-pubsub` and its whole runtime tree — the Pub/Sub client, gRPC, protobuf, Guava
and the rest — with every bundled package relocated so it cannot collide with anything else on the
classpath.

There is no code here. The connector, its options and its behaviour are documented with the
connector itself:

- [`flink-connector-gcp-pubsub`](../flink-connector-gcp-pubsub/README.md) — the module, and the
  implementation-status tables
- [SQL connector documentation](../docs/content/docs/connectors/table/pubsub.md) — the DDL option
  surface, metadata columns, and how to put this jar on the classpath
- [DataStream connector documentation](../docs/content/docs/connectors/datastream/pubsub.md) —
  delivery guarantees, tuning and error handling

```sql
CREATE TABLE orders (
  order_id STRING,
  amount   INT,
  attrs    MAP<STRING, STRING> METADATA FROM 'attributes'
) WITH (
  'connector' = 'pubsub',
  'project'   = 'my-project',
  'topic'     = 'orders',
  'format'    = 'json'
);
```

This jar is for SQL. A DataStream job should depend on `flink-connector-gcp-pubsub` instead: the
uber-jar relocates the Pub/Sub client, so `PubSubSerializationSchema` inside it returns a relocated
`PubsubMessage` that an ordinary job cannot consume.

## Bundled dependencies

The jar redistributes third-party binaries. `META-INF/NOTICE` inside it enumerates every bundled
artifact grouped by licence, and `META-INF/licenses/` carries the text of each non-Apache-2.0
licence. Both are checked in under `src/main/resources/` and both are held to the build: the
NOTICE's artifact lists are generated from what Maven actually resolves (the prose lives in
`NOTICE.template`), each licence text is pinned by sha256 to a recorded source, and CI fails on any
drift — a dependency added, removed or re-licensed upstream cannot ship unrecorded.

## Provenance and attribution

No code has been copied into this module; it has none. The build follows the shape of Apache
Flink's own SQL connector modules — `flink-sql-connector-kafka` for the overall module layout and
the `shade-flink` execution inherited from `flink-connector-parent`, and
`flink-sql-connector-aws-kinesis-streams` for the NOTICE grouping convention. Those are **design
references only**; the shade configuration here was written for this project.
