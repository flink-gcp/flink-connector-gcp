# flink-connector-gcp-firestore

Cloud Firestore connector for Apache Flink. A bounded source reads a Firestore database in Native
mode at one snapshot time, either a whole collection group cut into service-planned partitions or
one query of your own. The sink applies one document write per record through the client library's
`BulkWriter`, at-least-once, into the collections each write names. A second source and sink,
under the module's second package root, read a database in Datastore mode in key ranges cut by
the client library's query splitter and write one in non-transactional commits.

| Feature | Status |
|---|---|
| SinkV2 at-least-once sink over `BulkWriter`; `FirestoreWrite` serialization SPI | Implemented ([#1540](https://github.com/flink-gcp/flink-connector-gcp/issues/1540)) |
| Per-write failure policy (the shared `FailureHandler` SPI), with solo confirmation of `INVALID_ARGUMENT` and opt-in routing of failed preconditions | Implemented ([#1540](https://github.com/flink-gcp/flink-connector-gcp/issues/1540)) |
| Emulator integration tests | Implemented ([#1540](https://github.com/flink-gcp/flink-connector-gcp/issues/1540), [#1541](https://github.com/flink-gcp/flink-connector-gcp/issues/1541), [#1542](https://github.com/flink-gcp/flink-connector-gcp/issues/1542), [#1543](https://github.com/flink-gcp/flink-connector-gcp/issues/1543)) |
| DataStream bounded source (`PartitionQuery` cursor ranges or one query, at one read time; resume by cursor) | Implemented ([#1541](https://github.com/flink-gcp/flink-connector-gcp/issues/1541)) |
| Datastore-mode at-least-once sink over non-transactional commits; `DatastoreMutation` serialization SPI; ramp-up throttling | Implemented ([#1542](https://github.com/flink-gcp/flink-connector-gcp/issues/1542)) |
| Datastore-mode per-write failure policy, confirming a refused commit one write at a time | Implemented ([#1542](https://github.com/flink-gcp/flink-connector-gcp/issues/1542)) |
| Datastore-mode bounded source (a kind, a query or a GQL query; `QuerySplitter` key ranges at one read time; resume by cursor) | Implemented ([#1543](https://github.com/flink-gcp/flink-connector-gcp/issues/1543)) |
| Table API / SQL sink for Native mode (one collection keyed by document id; `set`, `merge` and `update` write modes; geo-point and reference markers) and the relocated SQL uber-jar | Implemented ([#1607](https://github.com/flink-gcp/flink-connector-gcp/issues/1607)) |
| Table API / SQL bounded scan for Native mode (one collection or a collection group; projection pushdown; readable metadata; `type-mismatch-policy`) | Implemented ([#1608](https://github.com/flink-gcp/flink-connector-gcp/issues/1608)) |
| Table API / SQL lookup for Native mode (by document id; blocking or asynchronous; `NONE` or `PARTIAL` cache) | Implemented ([#1609](https://github.com/flink-gcp/flink-connector-gcp/issues/1609)) |
| Table API / SQL for Datastore mode | Planned ([#1545](https://github.com/flink-gcp/flink-connector-gcp/issues/1545)) |
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
Datastore-mode source cuts a query into key ranges and the sink batches, confirms a refused commit
and paces its ramp-up, and where the
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
and its ramp-up throttle, and for the source's statistics-based split count, its GQL parse and
its paging; the Datastore-mode sink's design is recorded in
[ADR-0175](../docs/adr/0175-the-datastore-sink-commits-batches-and-confirms-a-refusal-one-write-at-a-time.md)
and the source's in
[ADR-0177](../docs/adr/0177-the-datastore-source-reads-key-ranges-the-client-librarys-splitter-cuts.md).
The `datastore-v1-proto-client` library's `QuerySplitter` is called, not copied.

No source code has been copied into this module.
