# flink-connector-gcp-bigquery

BigQuery sink for Apache Flink with a unified, `BigQueryIO`-style write API.

One builder dispatches to a write-method implementation at job-graph construction time:

| Write method | Semantics | Status |
|---|---|---|
| `STORAGE_API_AT_LEAST_ONCE` | Storage Write API default stream; dynamic per-record table destinations; connection multiplexing delegated to the client's connection pool | API only (writer: #10) |
| `STORAGE_API_EXACTLY_ONCE` | Storage Write API buffered streams + two-phase commit | Not buildable yet — `build()` rejects (#30) |
| `FILE_LOADS` | GCS-staged files + BigQuery load jobs; batch only | Not buildable yet — `build()` rejects (#14) |

```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                .destinationResolver(
                        (e, ctx) -> TableDestination.of("my-project", "my_dataset", e.tableName()))
                .serializer(new MyEventProtoSerializer())
                .build();
```

API notes:

- `BigQueryProtoSerializer` is an abstract class exposing `getDescriptor(TableDestination)` in
  addition to `serialize`, so the sink can derive table/stream schemas *before* the first record
  of a destination (table auto-creation, write-stream and load-job schemas). Protobuf
  `Descriptor`s are not Java-serializable — obtain them statically or lazily, don't store them in
  instance fields.
- `DestinationResolver.resolve(element, context)` receives the writer context (event timestamp,
  watermark) so time-based routing such as daily tables is expressible. Resolvers run per record:
  cache and reuse `TableDestination` instances.
- `TableDestination` is pure table identity (`equals`/`hashCode` over project/dataset/table);
  per-destination creation metadata (partitioning, clustering) will be supplied through separate
  hooks so destination identity stays stable as a cache/connection key.

## Provenance and attribution

This module is an original implementation. Its design references the following Apache-2.0
projects; when code is adapted from them, the fact is recorded here and in the repository-level
`NOTICE` file, keeping the original license headers where applicable:

- [Apache Beam](https://github.com/apache/beam) `BigQueryIO` — the unified write-method API shape
  and the dynamic destinations concept
- [GoogleCloudDataproc/flink-bigquery-connector](https://github.com/GoogleCloudDataproc/flink-bigquery-connector)
  — reference for Storage Write API sink internals and the serializer contract
  (descriptor accessor + `ByteString` rows)
- [googleapis/java-bigquerystorage](https://github.com/googleapis/java-bigquerystorage)
  (`JsonToProtoMessage`, `BQTableSchemaToProtoDescriptor`) — reference for proto/schema conversion

No source code has been copied into this module so far.
