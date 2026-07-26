# flink-connector-gcp-bigquery

BigQuery sink for Apache Flink with a unified, `BigQueryIO`-style write API.

One builder dispatches to a write-method implementation at job-graph construction time:

| Write method | Status |
|---|---|
| `STORAGE_API_AT_LEAST_ONCE` | Writer implemented, incl. table auto-creation with create dispositions, error classification/routing and schema evolution (full emulator IT suite: [#15](https://github.com/laughingman7743/flink-connector-gcp/issues/15)) |
| `STORAGE_API_EXACTLY_ONCE` | Implemented ([#30](https://github.com/laughingman7743/flink-connector-gcp/issues/30)) |
| `FILE_LOADS` | Implemented ([#14](https://github.com/laughingman7743/flink-connector-gcp/issues/14) batch, [#69](https://github.com/laughingman7743/flink-connector-gcp/issues/69) streaming) |

```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                .destinationResolver(
                        (e, ctx) -> TableDestination.of("my-project", "my_dataset", e.tableName()))
                .serializer(new MyEventProtoSerializer())
                .build();
```

## Documentation

Design, delivery guarantees, schema evolution, error handling, tuning and the testing strategy
are documented in
[docs/content/docs/connectors/datastream/bigquery.md](../docs/content/docs/connectors/datastream/bigquery.md)
(rendered on the documentation site once it is published).

## Provenance and attribution

This module is an original implementation. Its design references the following Apache-2.0
projects; when code is adapted from them, the fact is recorded here and in the repository-level
`NOTICE` file, keeping the original license headers where applicable:

- [Apache Beam](https://github.com/apache/beam) `BigQueryIO` — the unified write-method API shape
  and the dynamic destinations concept; for FILE_LOADS, the
  `BatchLoads`/`WritePartition`/`WriteTables`/`WriteRename` design (per-destination bin-packing
  against load-job limits, temp-table + copy-job overflow path, updating the final table schema
  before the copy since copy jobs support no schema update options, and GC of staged files only
  after load completion), and the streaming FILE_LOADS `triggeringFrequency` model (the Flink
  checkpoint takes the trigger role); for STORAGE_API_EXACTLY_ONCE, the
  `StorageApiFlushAndFinalizeDoFn` flush semantics (`ALREADY_EXISTS` on `FlushRows` means the
  offset was already flushed and is success — the idempotent-re-commit foundation)
- [Apache Flink](https://github.com/apache/flink) sink runtime — design reference for the
  committer-based load stage ([#69](https://github.com/laughingman7743/flink-connector-gcp/issues/69)): the SinkV2 committer/committable machinery
  (`CommitterOperator`, `GlobalCommitterOperator`) and the `FileSink` pre-commit-topology
  precedent were studied to establish that streaming commits must run in the committer (records
  emitted to a post-commit topology during job shutdown are not guaranteed to be processed); the
  stamper and committer here are independent implementations over the public SinkV2 API
- [GoogleCloudDataproc/flink-bigquery-connector](https://github.com/GoogleCloudDataproc/flink-bigquery-connector)
  — reference for Storage Write API sink internals and the serializer contract
  (descriptor accessor + `ByteString` rows); for FILE_LOADS, the `BigQueryIndirectSink`/
  `BigQueryLoadJobOperator` design (SinkV2 post-commit topology on a single non-parallel
  operator, deterministic BigQuery job ids with get-then-submit re-attach for exactly-once
  retries, 1.5 GiB size-based file rolling, best-effort cleanup); for
  STORAGE_API_EXACTLY_ONCE, the `BigQueryBufferedWriter`/`BigQueryCommitter` design (one
  buffered stream per subtask reused across checkpoints and tracked in writer state, the
  restore-time validation append that adopts or abandons the restored stream, inclusive flush
  offsets in the committable) — `BigQueryBufferedStreamWriter` and `BufferedStreamCommitter`
  are independent implementations of that protocol over Flink 2.x `SupportsWriterState`/
  `SupportsCommitter`, deliberately diverging on finalization (streams are never finalized
  here: real BigQuery rejects `FlushRows` on a finalized stream, which would break restored
  pending commits)
- [googleapis/java-bigquerystorage](https://github.com/googleapis/java-bigquerystorage)
  (`JsonToProtoMessage`, `BQTableSchemaToProtoDescriptor`, `BqToBqStorageSchemaConverter`) —
  reference for proto/schema conversion (`StorageSchemaConverter` and
  `BigQuerySchemaConverter` are independent implementations of the two directions)
- [Aiven-Open/bigquery-connector-for-apache-kafka](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka)
  (the maintained continuation of
  [wepay/kafka-connect-bigquery](https://github.com/wepay/kafka-connect-bigquery)) — design
  reference for the schema-evolution mechanics: the schema union rules and their gating flags,
  the update-on-error flow with a bounded jittered wait for schema propagation, and the
  coordinator-free concurrent-update retry pattern (`SchemaUnifier`,
  `BigQueryDefaultStreamWriter`'s reconciliation and `BigQueryTableAdmin`'s lost-race handling
  are independent reimplementations; the full design research is recorded on issue [#12](https://github.com/laughingman7743/flink-connector-gcp/issues/12))

No source code has been copied into this module so far.
