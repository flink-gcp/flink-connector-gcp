# flink-sql-connector-gcp-firestore

The Firestore connector packaged for SQL users: one jar to drop into Flink's `lib/`, bundling
`flink-connector-gcp-firestore` and the runtime tree it needs — the Firestore and Datastore
clients, gRPC, protobuf, Guava, OpenTelemetry and the rest. Java dependency packages linked by
the connector move under its connector-specific prefix; documented annotation-only packages and
optional native-backed Conscrypt remain unrelocated.

There are no production Java sources here. The connector, its options and its behaviour are
documented with the connector itself:

- [`flink-connector-gcp-firestore`](../flink-connector-gcp-firestore/README.md) — the module, and the
  implementation-status tables
- [SQL connector documentation](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/table/firestore/) —
  the DDL option surface, the type mapping, and how to put this jar on the classpath
- [DataStream connector documentation](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/firestore/) —
  delivery guarantees, tuning and error handling

```sql
CREATE TABLE orders (
  order_id STRING NOT NULL,
  status   STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector'  = 'firestore',
  'project'    = 'my-project',
  'collection' = 'orders'
);
```

This jar is for SQL. A DataStream job should depend on `flink-connector-gcp-firestore` instead:
the uber-jar relocates the Firestore and Datastore clients, including the `GeoPoint`, `Blob` and
`Entity` types the serialization SPIs use, so its Java surface is not a substitute for the
ordinary connector dependency.

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
execution inherited from `flink-connector-parent`, and
`flink-sql-connector-aws-kinesis-streams` for the NOTICE grouping convention. Those are **design
references only**; the shade configuration here was written for this project.
