# flink-connector-gcp-bigquery

BigQuery sink for Apache Flink with a unified, `BigQueryIO`-style write API.

One builder dispatches to a write-method implementation at job-graph construction time:

| Write method | Semantics | Status |
|---|---|---|
| `STORAGE_API_AT_LEAST_ONCE` | Storage Write API default stream; dynamic per-record table destinations; connection multiplexing delegated to the client's connection pool | API only (writer: #10) |
| `STORAGE_API_EXACTLY_ONCE` | Storage Write API buffered streams + two-phase commit | API only (writer: #30) |
| `FILE_LOADS` | GCS-staged files + BigQuery load jobs; batch only | API only (writer: #14) |

```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                .destinationResolver(e -> TableDestination.of("my-project", "my_dataset", e.tableName()))
                .serializer(new MyEventProtoSerializer())
                .build();
```

## Provenance and attribution

This module is an original implementation. Its design references the following Apache-2.0
projects; when code is adapted from them, the fact is recorded here and in the repository-level
`NOTICE` file, keeping the original license headers where applicable:

- [Apache Beam](https://github.com/apache/beam) `BigQueryIO` — the unified write-method API shape
  and the dynamic destinations concept
- [GoogleCloudDataproc/flink-bigquery-connector](https://github.com/GoogleCloudDataproc/flink-bigquery-connector)
  — reference for Storage Write API sink internals
- [googleapis/java-bigquerystorage](https://github.com/googleapis/java-bigquerystorage)
  (`JsonToProtoMessage`, `BQTableSchemaToProtoDescriptor`) — reference for proto/schema conversion

No source code has been copied into this module so far.
