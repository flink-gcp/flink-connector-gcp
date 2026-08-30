# flink-connector-gcp-spanner

Cloud Spanner connector for Apache Flink. The sink applies one `Mutation` per record through
`batchWriteAtLeastOnce` into the tables of one database; the bounded source reads a database at one
snapshot, on partitions the service planned; and the unbounded source reads a Change Stream.
Both dialects, GoogleSQL and PostgreSQL.

| Feature | Status |
|---|---|
| SinkV2 at-least-once sink over `batchWriteAtLeastOnce`; `Mutation` serialization SPI | Implemented ([#220](https://github.com/flink-gcp/flink-connector-gcp/issues/220)) |
| Per-mutation failure policy (the shared `FailureHandler` SPI) | Implemented ([#220](https://github.com/flink-gcp/flink-connector-gcp/issues/220)) |
| Index-aware mutation-cell batching, read from `INFORMATION_SCHEMA` | Implemented ([#220](https://github.com/flink-gcp/flink-connector-gcp/issues/220)) |
| Emulator integration tests, both dialects | Implemented ([#220](https://github.com/flink-gcp/flink-connector-gcp/issues/220)) |
| DataStream bounded batch source (`PartitionQuery`, Data Boost) | Implemented ([#221](https://github.com/flink-gcp/flink-connector-gcp/issues/221)) |
| DataStream Change Streams source, both dialects, including real-GCP recovery acceptance | Implemented ([#534](https://github.com/flink-gcp/flink-connector-gcp/issues/534), [#536](https://github.com/flink-gcp/flink-connector-gcp/issues/536), [#535](https://github.com/flink-gcp/flink-connector-gcp/issues/535)) |
| Table API / SQL sink, native type mapping, and DDL factory | Implemented ([#502](https://github.com/flink-gcp/flink-connector-gcp/issues/502)) |
| Table API / SQL bounded scan and projection | Implemented ([#503](https://github.com/flink-gcp/flink-connector-gcp/issues/503)) |
| Table API / SQL lookup source | Implemented ([#504](https://github.com/flink-gcp/flink-connector-gcp/issues/504)) |
| Relocated SQL uber-jar | Implemented ([#505](https://github.com/flink-gcp/flink-connector-gcp/issues/505)) |
| Gated real-GCP integration tests | Implemented ([#224](https://github.com/flink-gcp/flink-connector-gcp/issues/224)) |
| Change-stream CDC changelog scan, readable metadata, and source watermarks in the Table API and SQL | Implemented ([#582](https://github.com/flink-gcp/flink-connector-gcp/issues/582), [#583](https://github.com/flink-gcp/flink-connector-gcp/issues/583)) |

<!-- readme-example file="SpannerReadmeOverview.java" tag="spanner-readme-overview" -->
```java
Sink<OrderEvent> sink =
        SpannerSink.<OrderEvent>builder()
                .database(DatabaseDestination.of("my-project", "my-instance", "orders-db"))
                .serializer(
                        (event, context) ->
                                Mutation.newInsertOrUpdateBuilder("Orders")
                                        .set("OrderId")
                                        .to(event.id())
                                        .set("Total")
                                        .to(event.total())
                                        .build())
                .build();
```

## Documentation

The connector documentation — why the destination is a database rather than a table, the
serialization SPI, what a replay does and which mutation operations make it harmless, why the
retry loop belongs to the sink, how a batch is weighed and which limit each batch knob defends,
which refusals reach the failure handler and which do not, how the source splits a read and what a
recovery re-reads, and where the emulator differs from the service — is in the
[Spanner connector guide](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/spanner/).

A complete runnable job is in
[Quickstart](https://flink-gcp.github.io/flink-connector-gcp/docs/quickstart/spanner/).
DataStream multi-table writes, deletes, refusal handling and bounded reads; Table scans, upserts
and lookup joins; Change Streams modes and materialization; and emulator-backed local runs are
worked through in
[Examples](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/spanner/).
Every option the sink and the source take, with its default, is in the
[configuration reference](https://flink-gcp.github.io/flink-connector-gcp/docs/reference/spanner/).
The SQL DDL, type mapping, primary-key behavior, and Table API options are in the
[Spanner SQL connector guide](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/table/spanner/).

## Provenance and attribution

This module is an original implementation. Apache Beam's `SpannerIO` (Apache-2.0) is a **design
reference** only: its batch limits (1 MiB / 5,000 mutated cells / 500 rows), its way of counting a
mutation's cells (one per written column plus one per covering secondary index, read from
`INFORMATION_SCHEMA`), its decision not to sort batches by primary key for unbounded input, and the
shape of its mutation size estimator were all read and followed. Where this connector departs from
it is the write RPC: Beam predates `BatchWrite` and uses `writeAtLeastOnceWithOptions` with a
retry-and-bisect scheme, while this sink uses `batchWriteAtLeastOnce` and the per-group statuses it
reports — which is also why Beam's limits, values it chose for a `Commit`, are a starting point here
rather than figures the service is documented to enforce on this RPC.
The [Spanner connector for Apache Spark](https://github.com/GoogleCloudDataproc/spark-spanner-connector)
(Apache-2.0) was read as a design reference for the batch source; the comparison and no-copied-code
boundary are recorded in
[ADR-0085](../docs/adr/0085-the-spanner-batch-source-splits-by-server-planned-partition.md).
The Table Change Streams source's transaction and mod metadata vocabulary follows
[debezium-connector-spanner](https://github.com/debezium/debezium-connector-spanner) (Apache-2.0)
where the same Spanner fields exist; [ADR-0105](../docs/adr/0105-the-spanner-table-change-stream-source-emits-full-or-keyed-changelogs.md)
records the exact mapping.
The Spanner sink's retry handling was designed by comparison with this repository's Cloud Tasks and Bigtable sinks.
It follows Cloud Tasks' connector-owned loop rather than Bigtable's SDK-batcher adapter.
[ADR-0075](../docs/adr/0075-the-spanner-sink-batch-writes-and-owns-the-whole-retry-loop.md) records the resulting Spanner-specific retry boundary.

No source code has been copied into this module.
