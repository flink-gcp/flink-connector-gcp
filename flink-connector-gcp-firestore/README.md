# flink-connector-gcp-firestore

Cloud Firestore connector for Apache Flink. The sink applies one document write per record to a
Firestore database in Native mode through the client library's `BulkWriter`, at-least-once, into
the collections each write names. The module is also the home of the Datastore-mode surface, under
a second package root.

| Feature | Status |
|---|---|
| SinkV2 at-least-once sink over `BulkWriter`; `FirestoreWrite` serialization SPI | Implemented ([#1540](https://github.com/flink-gcp/flink-connector-gcp/issues/1540)) |
| Per-write failure policy (the shared `FailureHandler` SPI), with solo confirmation of `INVALID_ARGUMENT` and opt-in routing of failed preconditions | Implemented ([#1540](https://github.com/flink-gcp/flink-connector-gcp/issues/1540)) |
| Emulator integration tests | Implemented ([#1540](https://github.com/flink-gcp/flink-connector-gcp/issues/1540)) |
| DataStream bounded source (`PartitionQuery` cursor ranges at one read time) | Planned ([#1541](https://github.com/flink-gcp/flink-connector-gcp/issues/1541)) |
| Datastore-mode sink | Planned ([#1542](https://github.com/flink-gcp/flink-connector-gcp/issues/1542)) |
| Datastore-mode bounded source | Planned ([#1543](https://github.com/flink-gcp/flink-connector-gcp/issues/1543)) |
| Table API / SQL and the relocated SQL uber-jar | Planned ([#1544](https://github.com/flink-gcp/flink-connector-gcp/issues/1544), [#1545](https://github.com/flink-gcp/flink-connector-gcp/issues/1545)) |
| Gated real-GCP integration tests | Planned ([#1546](https://github.com/flink-gcp/flink-connector-gcp/issues/1546)) |
| Opt-in per-document submission order | Planned ([#1556](https://github.com/flink-gcp/flink-connector-gcp/issues/1556)) |
| Change-stream source (the Firestore APIs have no change-stream read; reopen conditions recorded on the issue) | Declined ([#355](https://github.com/flink-gcp/flink-connector-gcp/issues/355)) |

<!-- readme-example file="FirestoreReadmeOverview.java" tag="firestore-readme-overview" -->
```java
Sink<OrderEvent> sink =
        FirestoreSink.<OrderEvent>builder()
                .database(DatabaseDestination.of("my-project", "orders-db"))
                .serializer(
                        (event, context) ->
                                FirestoreWrite.set(
                                        "orders/" + event.getId(),
                                        Map.of("total", event.getTotal())))
                .build();
```

## Documentation

The connector documentation — why the destination is a database rather than a collection, the
closed value vocabulary and why it is closed, what a replay does to each operation, which refusals
reach the failure handler and why an invalid write is confirmed alone first, how the client
library throttles and retries, the two client-library defects the writer works around, and where
the emulator differs from the service — is in the
[Firestore connector guide](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/firestore/).

A complete runnable job is in
[Quickstart](https://flink-gcp.github.io/flink-connector-gcp/docs/quickstart/firestore/).
Merges, deletes, conditional updates, dropping refused writes and emulator-backed local runs are
worked through in
[Examples](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/firestore/).
Every option the sink takes, with its default, is in the
[configuration reference](https://flink-gcp.github.io/flink-connector-gcp/docs/reference/firestore/).

## Provenance and attribution

This module is an original implementation. Apache Beam's `FirestoreIO` (`FirestoreV1` and its
`RpcQosOptions`, Apache-2.0) was read as a **design reference** only, for its write batching and
ramp-up throttling; this sink uses the client library's own `BulkWriter` instead of a hand-rolled
batcher, and the comparison is recorded in
[ADR-0171](../docs/adr/0171-the-firestore-sink-writes-through-bulkwriter-and-confirms-invalid-writes-alone.md).
The client library's `BulkWriter` is called, not copied.

No source code has been copied into this module.
