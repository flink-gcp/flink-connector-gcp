# flink-connector-gcp-firestore

Cloud Firestore connector for Apache Flink. A bounded source reads a Firestore database in Native
mode at one snapshot time, either a whole collection group cut into service-planned partitions or
one query of your own. The sink applies one document write per record through the client library's
`BulkWriter`, at-least-once, into the collections each write names. A second sink, under the
module's second package root, writes a database in Datastore mode in non-transactional commits.

| Feature | Status |
|---|---|
| SinkV2 at-least-once sink over `BulkWriter`; `FirestoreWrite` serialization SPI | Implemented ([#1540](https://github.com/flink-gcp/flink-connector-gcp/issues/1540)) |
| Per-write failure policy (the shared `FailureHandler` SPI), with solo confirmation of `INVALID_ARGUMENT` and opt-in routing of failed preconditions | Implemented ([#1540](https://github.com/flink-gcp/flink-connector-gcp/issues/1540)) |
| Emulator integration tests | Implemented ([#1540](https://github.com/flink-gcp/flink-connector-gcp/issues/1540), [#1541](https://github.com/flink-gcp/flink-connector-gcp/issues/1541), [#1542](https://github.com/flink-gcp/flink-connector-gcp/issues/1542)) |
| DataStream bounded source (`PartitionQuery` cursor ranges or one query, at one read time; resume by cursor) | Implemented ([#1541](https://github.com/flink-gcp/flink-connector-gcp/issues/1541)) |
| Datastore-mode at-least-once sink over non-transactional commits; `DatastoreMutation` serialization SPI; ramp-up throttling | Implemented ([#1542](https://github.com/flink-gcp/flink-connector-gcp/issues/1542)) |
| Datastore-mode per-write failure policy, confirming a refused commit one write at a time | Implemented ([#1542](https://github.com/flink-gcp/flink-connector-gcp/issues/1542)) |
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

The connector documentation — how the source partitions a scan, holds one snapshot and resumes a
split after its last document, why the sink's destination is a database rather than a collection, the
closed value vocabulary and why it is closed, what a replay does to each operation, which refusals
reach the failure handler and why an invalid write is confirmed alone first, how the client
library throttles and retries, the two client-library defects the writer works around, how the
Datastore-mode sink batches, confirms a refused commit and paces its ramp-up, and where the
emulator differs from the service — is in the
[Firestore connector guide](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/firestore/).

A complete runnable job is in
[Quickstart](https://flink-gcp.github.io/flink-connector-gcp/docs/quickstart/firestore/).
Merges, deletes, conditional updates, dropping refused writes and emulator-backed local runs are
worked through in
[Examples](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/firestore/).
Every option the source and the sinks take, with its default, is in the
[configuration reference](https://flink-gcp.github.io/flink-connector-gcp/docs/reference/firestore/).

## Provenance and attribution

This module is an original implementation. Apache Beam's `FirestoreIO` (`FirestoreV1` and its
`RpcQosOptions`, Apache-2.0) was read as a **design reference** only, for its write batching and
ramp-up throttling and for its `PartitionQuery`-then-`RunQuery` read; this sink uses the client
library's own `BulkWriter` instead of a hand-rolled batcher, and the comparison is recorded in
[ADR-0171](../docs/adr/0171-the-firestore-sink-writes-through-bulkwriter-and-confirms-invalid-writes-alone.md).
The source's read design is recorded in
[ADR-0173](../docs/adr/0173-the-firestore-source-reads-partition-cursor-ranges-at-one-read-time.md).
The client library's `BulkWriter`, partitioning and queries are called, not copied. Apache Beam's
`DatastoreIO` (`DatastoreV1` and its `RampupThrottlingFn`, Apache-2.0) was likewise read as a
design reference only, for the Datastore-mode sink's batch limits, its flush before a repeated key
and its ramp-up throttle; the Datastore-mode sink's design is recorded in
[ADR-0175](../docs/adr/0175-the-datastore-sink-commits-batches-and-confirms-a-refusal-one-write-at-a-time.md).

No source code has been copied into this module.
