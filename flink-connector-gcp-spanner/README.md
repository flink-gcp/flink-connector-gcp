# flink-connector-gcp-spanner

Cloud Spanner connector for Apache Flink. The sink applies one `Mutation` per record through
`batchWriteAtLeastOnce` into the tables of one database; the bounded source reads a database at one
snapshot, on partitions the service planned. Both dialects, GoogleSQL and PostgreSQL.

| Feature | Status |
|---|---|
| SinkV2 at-least-once sink over `batchWriteAtLeastOnce`; `Mutation` serialization SPI | Implemented ([#220](https://github.com/laughingman7743/flink-connector-gcp/issues/220)) |
| Per-mutation failure policy (the shared `FailureHandler` SPI) | Implemented ([#220](https://github.com/laughingman7743/flink-connector-gcp/issues/220)) |
| Index-aware mutation-cell batching, read from `INFORMATION_SCHEMA` | Implemented ([#220](https://github.com/laughingman7743/flink-connector-gcp/issues/220)) |
| Emulator integration tests, both dialects | Implemented ([#220](https://github.com/laughingman7743/flink-connector-gcp/issues/220)) |
| DataStream bounded batch source (`PartitionQuery`, Data Boost) | Implemented ([#221](https://github.com/laughingman7743/flink-connector-gcp/issues/221)) |
| Change streams source | Planned ([#222](https://github.com/laughingman7743/flink-connector-gcp/issues/222)) |
| Table API / SQL support | Planned ([#223](https://github.com/laughingman7743/flink-connector-gcp/issues/223)) |
| Gated real-GCP integration tests | Planned ([#224](https://github.com/laughingman7743/flink-connector-gcp/issues/224)) |
| Change-stream CDC in SQL | Planned ([#225](https://github.com/laughingman7743/flink-connector-gcp/issues/225)) |

```java
Sink<OrderEvent> sink =
        SpannerSink.<OrderEvent>builder()
                .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
                .serializer(
                        (event, context) ->
                                Mutation.newInsertOrUpdateBuilder("Orders")
                                        .set("OrderId").to(event.id())
                                        .set("Total").to(event.total())
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
Apache-2.0) were read as further design references for the batch source and the planned
change-stream source. The design otherwise
follows the Cloud Tasks sink in this repository, whose writer likewise owns its retry loop.

No source code has been copied into this module.
