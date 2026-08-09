# flink-connector-gcp-bigquery

BigQuery connectors for Apache Flink: a sink with a unified, `BigQueryIO`-style write API, and a
bounded source over the Storage Read API.

One builder dispatches to a write-method implementation at job-graph construction time:

| Write method | Status |
|---|---|
| `STORAGE_API_AT_LEAST_ONCE` | Writer implemented, incl. table auto-creation with create dispositions, error classification/routing and schema evolution (full emulator IT suite: [#15](https://github.com/laughingman7743/flink-connector-gcp/issues/15)); tuning knobs, cold-destination eviction and `flushInterval` ([#54](https://github.com/laughingman7743/flink-connector-gcp/issues/54)) |
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

Ready-made serializers cover the common input shapes: `ProtoMessageSerializer.of(MyMessage.class)`
for records that already are protobuf messages, `AvroRecordSerializer.of(schema)` for Avro
`GenericRecord` and `SpecificRecord` streams, and `JsonDocumentSerializer.of(schema)` for JSON documents
as text.

## Source

| Read feature | Status |
|---|---|
| Bounded `BigQuerySource` over the Storage Read API: one split per read stream with offset-resumed recovery, pull assignment, column projection, row restriction and point-in-time reads, Avro rows | Implemented ([#390](https://github.com/laughingman7743/flink-connector-gcp/issues/390)) |
| Read resilience and real-GCP restore coverage | Planned ([#391](https://github.com/laughingman7743/flink-connector-gcp/issues/391)) |
| Reading the result of a query rather than a table | Planned ([#392](https://github.com/laughingman7743/flink-connector-gcp/issues/392)) |
| Arrow wire format | Planned ([#393](https://github.com/laughingman7743/flink-connector-gcp/issues/393)) |

```java
Source<GenericRecord, ?, ?> source =
        BigQuerySource.<GenericRecord>builder()
                .table(TableDestination.of("my-project", "my_dataset", "my_table"))
                .deserializer(BigQueryRowDeserializer.genericRecord(readerSchema))
                .selectedFields("id", "name")
                .build();
```

A read is charged for the bytes it scans, so `selectedFields` is what keeps a wide table cheap.
Using the shipped `GenericRecord` deserializer needs `flink-avro` on the job's classpath.

## Table API / SQL

| Table API / SQL feature | Status |
|---|---|
| `bigquery` table connector; `DynamicTableSink` over `STORAGE_API_AT_LEAST_ONCE`, with the `sink.default-stream.*` tuning family and the `RowData` serializer | Implemented ([#287](https://github.com/laughingman7743/flink-connector-gcp/issues/287)) |
| The remaining write methods from SQL (`sink.buffered-stream.*`, `sink.file-loads.*`) | Implemented ([#288](https://github.com/laughingman7743/flink-connector-gcp/issues/288)) |
| Table-creation options (`sink.table-create.*`): time partitioning and clustering | Implemented ([#289](https://github.com/laughingman7743/flink-connector-gcp/issues/289)) |
| `flink-sql-connector-gcp-bigquery` shaded uber-jar | Implemented ([#290](https://github.com/laughingman7743/flink-connector-gcp/issues/290)) |

## Documentation

Design, delivery guarantees, schema evolution, error handling, tuning and the testing strategy
are documented in
[docs/content/docs/connectors/datastream/bigquery.md](../docs/content/docs/connectors/datastream/bigquery.md)
(rendered on the documentation site once it is published).

A complete runnable job is in
[Quickstart](../docs/content/docs/quickstart/bigquery.md); dynamic destinations, the exactly-once
write methods and table auto-creation are worked through in
[Examples](../docs/content/docs/examples/bigquery.md), which also work through reading: projection
and restriction, a public dataset, a point-in-time read and the stream-count knobs. Every option the
sink and the source take, with its default, is in the
[configuration reference](../docs/content/docs/reference/bigquery.md).

The Table API / SQL option surface is documented in
[docs/content/docs/connectors/table/bigquery.md](../docs/content/docs/connectors/table/bigquery.md).

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
  retries, 1.5 GB size-based file rolling, best-effort cleanup); for
  STORAGE_API_EXACTLY_ONCE, the `BigQueryBufferedWriter`/`BigQueryCommitter` design (one
  buffered stream per subtask reused across checkpoints and tracked in writer state, the
  restore-time validation append that adopts or abandons the restored stream, inclusive flush
  offsets in the committable) — `BigQueryBufferedStreamWriter` and `BufferedStreamCommitter`
  are independent implementations of that protocol over Flink 2.x `SupportsWriterState`/
  `SupportsCommitter`, deliberately diverging on finalization (streams are never finalized
  here: real BigQuery rejects `FlushRows` on a finalized stream, which would break restored
  pending commits). For the Avro serializer ([#66](https://github.com/laughingman7743/flink-connector-gcp/issues/66)) it is the `AvroToProtoSerializer` type
  matrix that was the reference — which logical types map where, and the `["null", T]`-only union
  rule. `AvroToTableSchemaConverter` and `AvroRowConverter` are independent implementations, and
  deliberately diverge on Avro maps, which are mapped to `REPEATED STRUCT<key, value>` rather than
  rejected
- [googleapis/java-bigquerystorage](https://github.com/googleapis/java-bigquerystorage) —
  `BQTableSchemaToProtoDescriptor`, `CivilTimeEncoder` and `BigDecimalByteStringEncoder` are
  **called** by every serializer here, and `JsonToProtoMessage` is the whole of
  `JsonDocumentSerializer`'s conversion, so this is a runtime dependency rather than only a
  reference. `BqToBqStorageSchemaConverter` is the design reference for the schema conversion
  `StorageSchemaConverter` and `BigQuerySchemaConverter` implement independently in both
  directions
- [Aiven-Open/bigquery-connector-for-apache-kafka](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka)
  (the maintained continuation of
  [wepay/kafka-connect-bigquery](https://github.com/wepay/kafka-connect-bigquery)) — design
  reference for the schema-evolution mechanics: the schema union rules and their gating flags,
  the update-on-error flow with a bounded jittered wait for schema propagation, and the
  coordinator-free concurrent-update retry pattern (`SchemaUnifier`,
  `BigQueryDefaultStreamWriter`'s reconciliation and `BigQueryTableAdmin`'s lost-race handling
  are independent reimplementations; the full design research is recorded on issue [#12](https://github.com/laughingman7743/flink-connector-gcp/issues/12))

No source code has been copied into this module so far.
