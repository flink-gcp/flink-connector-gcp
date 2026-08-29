# flink-sql-connector-gcp-bigtable

The Bigtable connector packaged for SQL users: one jar to drop into Flink's `lib/`, bundling
`flink-connector-gcp-bigtable` and its whole runtime tree — the Bigtable client, gRPC, protobuf,
Guava and the rest — with every bundled package relocated so it cannot collide with anything else
on the classpath.

There are no production Java sources here. The connector, its options and its behaviour are
documented with the connector itself:

- [`flink-connector-gcp-bigtable`](../flink-connector-gcp-bigtable/README.md) — the module, and the
  implementation-status tables
- [SQL connector documentation](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/table/bigtable/) —
  the DDL model, the option surface, and how to put this jar on the classpath
- [DataStream connector documentation](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/bigtable/) —
  delivery guarantees, tuning and error handling

```sql
CREATE TABLE page_views (
  rowkey STRING,
  stats  ROW<views BIGINT, last_visit TIMESTAMP(3)>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project'   = 'my-project',
  'instance'  = 'my-instance',
  'table'     = 'page_views'
);
```

This jar is for SQL. A DataStream job should depend on `flink-connector-gcp-bigtable` instead: the
uber-jar relocates the Bigtable client, so `BigtableSerializationSchema` inside it expects a
relocated `RowMutationEntry` that an ordinary job cannot construct.

## Bundled dependencies

The jar redistributes third-party binaries. `META-INF/NOTICE` inside it enumerates every bundled
artifact grouped by licence, and `META-INF/licenses/` carries the text of each non-Apache-2.0
licence. Both are checked in under `src/main/resources/` and both are held to the build: the
NOTICE's artifact lists are generated from what Maven actually resolves (the prose lives in
`NOTICE.template`), each licence text is pinned by sha256 to a recorded source, and CI fails on any
drift — a dependency added, removed or re-licensed upstream cannot ship unrecorded.

## Provenance and attribution

No code has been copied into this module.
Its Java sources support packaging, NOTICE, planning, and smoke verification only.
The build follows the shape of Apache Flink's own
SQL connector modules — `flink-sql-connector-kafka` for the overall module layout and the
`shade-flink` execution inherited from `flink-connector-parent`, and
`flink-sql-connector-aws-kinesis-streams` for the NOTICE grouping convention. Those are **design
references only**; the shade configuration here was written for this project.
