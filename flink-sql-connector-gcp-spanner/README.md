# flink-sql-connector-gcp-spanner

The Spanner connector packaged for SQL users: one jar to drop into Flink's `lib/`, bundling
`flink-connector-gcp-spanner` and the runtime dependencies selected by the repository's SQL
artifact rules — the Spanner client, gRPC, protobuf, Guava and the rest — with behavior-bearing
packages relocated so they cannot collide with anything else on the classpath.

There is no code here. The connector, its options and its behaviour are documented with the
connector itself:

- [`flink-connector-gcp-spanner`](../flink-connector-gcp-spanner/README.md) — the module, and the
  implementation-status tables
- [SQL connector documentation](../docs/content/docs/connectors/table/spanner.md) — the DDL model,
  the option surface, and how to put this jar on the classpath
- [DataStream connector documentation](../docs/content/docs/connectors/datastream/spanner.md) —
  delivery guarantees, tuning and error handling

```sql
CREATE TABLE page_views (
  id BIGINT,
  name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project'   = 'my-project',
  'instance'  = 'my-instance',
  'database'  = 'my-database',
  'table'     = 'page_views'
);
```

This jar is for SQL. A DataStream job should depend on `flink-connector-gcp-spanner` instead;
the uber-jar's relocated Google Cloud types are intentionally isolated from application code.

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
