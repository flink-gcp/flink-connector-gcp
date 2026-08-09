# flink-connector-gcp-bigtable

Cloud Bigtable connector for Apache Flink. The sink applies one row mutation per record through
the client's bulk `MutateRows` batcher, into a fixed table or one the record names; the source reads
a table's rows, splitting it by the row-key boundaries the service samples.

| Feature | Status |
|---|---|
| SinkV2 at-least-once sink over the bulk mutation batcher; `RowMutationEntry` serialization SPI | Implemented ([#33](https://github.com/laughingman7743/flink-connector-gcp/issues/33)) |
| Per-mutation failure policy (the shared `FailureHandler` SPI) | Implemented ([#33](https://github.com/laughingman7743/flink-connector-gcp/issues/33)) |
| Emulator integration tests | Implemented ([#33](https://github.com/laughingman7743/flink-connector-gcp/issues/33)) |
| DataStream bounded scan source | Implemented ([#216](https://github.com/laughingman7743/flink-connector-gcp/issues/216)) |
| Table API / SQL support | Planned ([#217](https://github.com/laughingman7743/flink-connector-gcp/issues/217)) |
| Gated real-GCP integration tests | Implemented ([#218](https://github.com/laughingman7743/flink-connector-gcp/issues/218)) |
| Table and column-family auto-creation (`CREATE_IF_NEEDED`) | Implemented ([#233](https://github.com/laughingman7743/flink-connector-gcp/issues/233)) |
| Per-record table destinations (`destinationResolver`) | Implemented ([#232](https://github.com/laughingman7743/flink-connector-gcp/issues/232)) |
| Change streams source | Planned ([#35](https://github.com/laughingman7743/flink-connector-gcp/issues/35)) |

```java
Sink<OrderEvent> sink =
        BigtableSink.<OrderEvent>builder()
                .table(TableDestination.of("my-project", "my-instance", "orders"))
                .serializer(
                        (event, context) ->
                                RowMutationEntry.create("order#" + event.id())
                                        .setCell("cf", "payload", event.timestampMicros(),
                                                event.body()))
                .build();
```

```java
Source<OrderEvent, ?, ?> source =
        BigtableSource.<OrderEvent>builder()
                .table(TableDestination.of("my-project", "my-instance", "orders"))
                // Zero or more records per row, so the schema declares its produced type too.
                .deserializer(new OrderEventRows())
                .prefix("order#")
                .build();
```

## Documentation

The connector documentation — what the connector is for, the serialization SPI, why the table is
resolved per record, delivery guarantees and what a cell timestamp decides about replays, why retries
belong to the client, tuning and the client's own flow controller, how failures are classified, how
the source splits a table by sampled row-key range and resumes by truncating it, and where the
emulator differs from the service — is in
[docs/content/docs/connectors/datastream/bigtable.md](../docs/content/docs/connectors/datastream/bigtable.md)
(rendered on the documentation site once it is published).

Complete runnable jobs, one per direction, are in
[Quickstart](../docs/content/docs/quickstart/bigtable.md); several mutations per record, dropping
bad rows, reading a key range, filtering on the server and emulator-backed local runs are worked
through in [Examples](../docs/content/docs/examples/bigtable.md). Every option the sink and source
take, with its default, is in the
[configuration reference](../docs/content/docs/reference/bigtable.md).

## Provenance and attribution

This module is an original implementation.
[google/flink-connector-gcp](https://github.com/google/flink-connector-gcp)
(Apache-2.0) is a **design reference** only: the serialization SPI's shape — returning a
`RowMutationEntry`, with `null` meaning skip — is shared with its `BaseRowMutationSerializer` so
that its users can migrate mechanically. Depending on that artifact, and vendoring its sources, were
both evaluated and rejected; the grounds are recorded on
[#33](https://github.com/laughingman7743/flink-connector-gcp/issues/33). Apache Beam's `BigtableIO`
(Apache-2.0) was read as a second design reference for driving the bulk mutation batcher, and the
design otherwise follows the Pub/Sub sink in this repository (the mailbox-based in-flight bounds,
the flush-on-checkpoint stateless writer).

No source code has been copied into this module.
