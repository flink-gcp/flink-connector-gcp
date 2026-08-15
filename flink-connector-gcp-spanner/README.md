# flink-connector-gcp-spanner

Cloud Spanner connector for Apache Flink. The sink applies one `Mutation` per record through
`batchWriteAtLeastOnce` into the tables of one database; the bounded source reads a database at one
snapshot, on partitions the service planned; and the unbounded source reads a Change Stream.
Both dialects, GoogleSQL and PostgreSQL.

| Feature | Status |
|---|---|
| SinkV2 at-least-once sink over `batchWriteAtLeastOnce`; `Mutation` serialization SPI | Implemented ([#220](https://github.com/laughingman7743/flink-connector-gcp/issues/220)) |
| Per-mutation failure policy (the shared `FailureHandler` SPI) | Implemented ([#220](https://github.com/laughingman7743/flink-connector-gcp/issues/220)) |
| Index-aware mutation-cell batching, read from `INFORMATION_SCHEMA` | Implemented ([#220](https://github.com/laughingman7743/flink-connector-gcp/issues/220)) |
| Emulator integration tests, both dialects | Implemented ([#220](https://github.com/laughingman7743/flink-connector-gcp/issues/220)) |
| DataStream bounded batch source (`PartitionQuery`, Data Boost) | Implemented ([#221](https://github.com/laughingman7743/flink-connector-gcp/issues/221)) |
| DataStream Change Streams source, both dialects, including real-GCP recovery acceptance | Implemented ([#534](https://github.com/laughingman7743/flink-connector-gcp/issues/534), [#536](https://github.com/laughingman7743/flink-connector-gcp/issues/536), [#535](https://github.com/laughingman7743/flink-connector-gcp/issues/535)) |
| Table API / SQL sink, native type mapping, and DDL factory | Implemented ([#502](https://github.com/laughingman7743/flink-connector-gcp/issues/502)) |
| Table API / SQL bounded scan and projection | Implemented ([#503](https://github.com/laughingman7743/flink-connector-gcp/issues/503)) |
| Table API / SQL lookup source | Implemented ([#504](https://github.com/laughingman7743/flink-connector-gcp/issues/504)) |
| Relocated SQL uber-jar | Implemented ([#505](https://github.com/laughingman7743/flink-connector-gcp/issues/505)) |
| Gated real-GCP integration tests | Implemented ([#224](https://github.com/laughingman7743/flink-connector-gcp/issues/224)) |
| Change-stream CDC changelog scan, readable metadata, and source watermarks in the Table API and SQL | Implemented ([#582](https://github.com/laughingman7743/flink-connector-gcp/issues/582), [#583](https://github.com/laughingman7743/flink-connector-gcp/issues/583)) |

<!-- readme-example file="SpannerReadmeOverview.java" tag="spanner-readme-overview" -->
```java
Sink<OrderEvent> sink =
        SpannerSink.<OrderEvent>builder()
                .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
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
recovery re-reads, and where the emulator differs from the service — is in
[docs/content/docs/connectors/datastream/spanner.md](../docs/content/docs/connectors/datastream/spanner.md)
(rendered on the documentation site once it is published).

A complete runnable job is in
[Quickstart](../docs/content/docs/quickstart/spanner.md); writing to several tables from one
stream, deletes, dropping refused mutations, reading a key range and emulator-backed local runs are
worked through in
[Examples](../docs/content/docs/examples/spanner.md). Every option the sink and the source take,
with its default, is in the [configuration reference](../docs/content/docs/reference/spanner.md).
The SQL DDL, type mapping, primary-key behavior, and Table API options are in the
[Spanner SQL connector guide](../docs/content/docs/connectors/table/spanner.md).

## Provenance and attribution

This module is an original implementation. Apache Beam's `SpannerIO` (Apache-2.0) is a **design
reference** only: its batch limits (1 MiB / 5,000 mutated cells / 500 rows), its way of counting a
mutation's cells (one per written column plus one per covering secondary index, read from
`INFORMATION_SCHEMA`), its decision not to sort batches by primary key for unbounded input, and the
shape of its mutation size estimator were all read and followed. Where this connector departs from
it is the write RPC: Beam predates `BatchWrite` and uses `writeAtLeastOnceWithOptions` with a
retry-and-bisect scheme, while this sink uses `batchWriteAtLeastOnce` and the per-group statuses it
reports — which is also why Beam's limits, values it chose for a `Commit`, are a starting point here
rather than figures the service is documented to enforce on this RPC. The
[Spanner connector for Apache Spark](https://github.com/GoogleCloudDataproc/spark-spanner-connector)
and [debezium-connector-spanner](https://github.com/debezium/debezium-connector-spanner) (both
Apache-2.0) were read as further design references for the batch source and Change Streams work.
The Table source's transaction and mod metadata vocabulary follows Debezium's Spanner source
metadata where the same Spanner fields exist.
The implemented Change Streams source design otherwise
follows the Cloud Tasks sink in this repository, whose writer likewise owns its retry loop.

No source code has been copied into this module.
