# flink-connector-gcp-bigquery

BigQuery connectors for Apache Flink: a sink with a unified, `BigQueryIO`-style write API, and a
bounded source over the Storage Read API.

One builder dispatches to a write-method implementation at job-graph construction time:

| Write method | Status |
|---|---|
| `STORAGE_API_AT_LEAST_ONCE` | Writer implemented, incl. table auto-creation with create dispositions, error classification/routing and schema evolution (full emulator IT suite: [#15](https://github.com/flink-gcp/flink-connector-gcp/issues/15)); tuning knobs, cold-destination eviction and `flushInterval` ([#54](https://github.com/flink-gcp/flink-connector-gcp/issues/54)) |
| `STORAGE_API_EXACTLY_ONCE` | Implemented, including dynamic destinations and mid-stream schema evolution ([#30](https://github.com/flink-gcp/flink-connector-gcp/issues/30), [#76](https://github.com/flink-gcp/flink-connector-gcp/issues/76), [#77](https://github.com/flink-gcp/flink-connector-gcp/issues/77)) |
| `FILE_LOADS` | Implemented ([#14](https://github.com/flink-gcp/flink-connector-gcp/issues/14) batch, [#69](https://github.com/flink-gcp/flink-connector-gcp/issues/69) streaming, [#646](https://github.com/flink-gcp/flink-connector-gcp/issues/646) metadata-preserving batch replacement) |

<!-- readme-example file="BigQueryConnectorOverview.java" tag="bigquery-connector-overview" -->
```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                .destinationResolver(
                        (e, ctx) ->
                                TableDestination.of(
                                        "my-project", "my_dataset", e.tableName()))
                .serializer(new MyEventProtoSerializer())
                .build();
```

Ready-made serializers cover the common input shapes: `ProtoMessageSerializationSchema.of(MyMessage.class)`
for records that already are protobuf messages, `AvroRecordSerializationSchema.of(schema)` for Avro
`GenericRecord` and `SpecificRecord` streams, and `JsonDocumentSerializationSchema.of(schema)` for JSON documents
as text.

## Source

| Read feature | Status |
|---|---|
| Bounded `BigQuerySource` over the Storage Read API: one split per read stream with offset-resumed recovery, pull assignment, column projection, row restriction and point-in-time reads, Avro rows | Implemented ([#390](https://github.com/flink-gcp/flink-connector-gcp/issues/390)) |
| Read resilience: a bounded `ReadRows` retry, a retry counter, an explained session expiry, and multi-stream restore coverage against real BigQuery | Implemented ([#391](https://github.com/flink-gcp/flink-connector-gcp/issues/391)) |
| Reading the result of a query rather than a table, which is the only way to read a view: a query job first, then its result table, landing either in BigQuery's anonymous dataset or in one you name — plus opt-in `materializeViews()` for a name that turns out to be a view | Implemented ([#392](https://github.com/flink-gcp/flink-connector-gcp/issues/392)) |
| Arrow wire format, measured against the Avro path: faster only for a reader that never asks for a row, and Flink asks for one | Declined ([#393](https://github.com/flink-gcp/flink-connector-gcp/issues/393)) |

<!-- readme-example file="BigQueryReadmeSource.java" tag="bigquery-readme-source" -->
```java
Source<GenericRecord, ?, ?> source =
        BigQuerySource.<GenericRecord>builder()
                .table(TableDestination.of("my-project", "my_dataset", "my_table"))
                .deserializer(BigQueryRowDeserializationSchema.genericRecord(readerSchema))
                .selectedFields("id", "name")
                .build();
```

A read is charged for the bytes it scans, so `selectedFields` is what keeps a wide table cheap.
Using the shipped `GenericRecord` deserializer needs `flink-avro` on the job's classpath.

## Table API / SQL

| Table API / SQL feature | Status |
|---|---|
| `bigquery` table connector; `DynamicTableSink` over `STORAGE_API_AT_LEAST_ONCE`, with the `sink.default-stream.*` tuning family and the `RowData` serializer | Implemented ([#287](https://github.com/flink-gcp/flink-connector-gcp/issues/287)) |
| The remaining write methods from SQL (`sink.buffered-stream.*`, `sink.file-loads.*`) | Implemented ([#288](https://github.com/flink-gcp/flink-connector-gcp/issues/288)) |
| Table-creation options (`sink.table-create.*`): time partitioning and clustering | Implemented ([#289](https://github.com/flink-gcp/flink-connector-gcp/issues/289)) |
| Bounded `DynamicTableSource` over table, query, and view-materialization reads, with top-level projection pushdown | Implemented ([#542](https://github.com/flink-gcp/flink-connector-gcp/issues/542)) |
| `flink-sql-connector-gcp-bigquery` shaded uber-jar | Implemented ([#290](https://github.com/flink-gcp/flink-connector-gcp/issues/290)) |

## Documentation

Design, delivery guarantees, schema evolution, error handling, tuning and the testing strategy
are documented in the
[BigQuery connector guide](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/bigquery/).

A complete runnable job is in
[Quickstart](https://flink-gcp.github.io/flink-connector-gcp/docs/quickstart/bigquery/); dynamic
destinations, the exactly-once write methods and table auto-creation are worked through in
[Examples](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/bigquery/), which also work
through reading: projection and restriction, a public dataset, a point-in-time read and the
stream-count knobs. Every option the sink and the source take, with its default, is in the
[configuration reference](https://flink-gcp.github.io/flink-connector-gcp/docs/reference/bigquery/).

The Table API / SQL option surface is documented in the
[BigQuery SQL connector guide](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/table/bigquery/).

## Provenance and attribution

This module is an original implementation. Its design references the following Apache-2.0
projects; when code is adapted from them, the fact is recorded here and in the repository-level
`NOTICE` file, keeping the original license headers where applicable. The detail of what each
reference contributed lives in the decision records linked from each entry:

- [Apache Beam](https://github.com/apache/beam) `BigQueryIO` — the unified write-method API
  shape ([ADR-0016](../docs/adr/0016-the-bigquery-sink-is-a-bigqueryio-style-facade.md)) and
  the dynamic destinations concept, the FILE_LOADS design
  ([ADR-0018 § Design references](../docs/adr/0018-file-loads-commits-deterministic-load-jobs-in-the-committer.md#design-references)),
  and the flush semantics behind idempotent re-commits
  ([ADR-0022](../docs/adr/0022-exactly-once-uses-buffered-streams-reused-across-checkpoints-never-finalized.md))
- [Apache Flink](https://github.com/apache/flink) sink runtime — the SinkV2 committer machinery
  and the `FileSink` precedent studied for the committer-based load stage
  ([ADR-0018 § Design references](../docs/adr/0018-file-loads-commits-deterministic-load-jobs-in-the-committer.md#design-references));
  the stamper and committer here are independent implementations over the public SinkV2 API
- [GoogleCloudDataproc/flink-bigquery-connector](https://github.com/GoogleCloudDataproc/flink-bigquery-connector)
  — Storage Write API sink internals and the serializer contract, the FILE_LOADS job design
  ([ADR-0018 § Design references](../docs/adr/0018-file-loads-commits-deterministic-load-jobs-in-the-committer.md#design-references)),
  the buffered-stream exactly-once protocol — `BigQueryBufferedStreamWriter` and
  `BufferedStreamCommitter` are independent implementations of it, deliberately diverged on
  finalization
  ([ADR-0022](../docs/adr/0022-exactly-once-uses-buffered-streams-reused-across-checkpoints-never-finalized.md))
  — the Avro serializer's type matrix, deliberately diverged from on maps
  ([ADR-0024](../docs/adr/0024-the-avro-serializer-mirrors-the-proto-path-and-derives-eagerly.md)),
  and a source design reference
  ([ADR-0079](../docs/adr/0079-the-bigquery-source-splits-by-read-stream-and-its-enumerator-keeps-no-ledger.md))
- [googleapis/java-bigquerystorage](https://github.com/googleapis/java-bigquerystorage) —
  `BQTableSchemaToProtoDescriptor`, `CivilTimeEncoder` and `BigDecimalByteStringEncoder` are
  **called** by every serializer here, and `JsonToProtoMessage` is the whole of
  `JsonDocumentSerializationSchema`'s conversion
  ([ADR-0025](../docs/adr/0025-the-json-serializer-delegates-conversion-to-the-client-library.md)),
  so this is a runtime dependency rather than only a reference.
  `BqToBqStorageSchemaConverter` is the design reference for the schema conversions
  `StorageSchemaConverter` and `BigQuerySchemaConverter` implement independently in both
  directions
- [Aiven-Open/bigquery-connector-for-apache-kafka](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka)
  (the maintained continuation of
  [wepay/kafka-connect-bigquery](https://github.com/wepay/kafka-connect-bigquery)) — design
  reference for the schema-evolution mechanics: the schema union rules and their gating flags,
  the update-on-error flow with a bounded jittered wait, and the coordinator-free
  concurrent-update retry (`SchemaUnifier`, `StorageWriteSchemaReconciler` and
  `BigQueryTableAdmin`'s lost-race handling are independent reimplementations; the option
  vocabulary is credited in
  [ADR-0026](../docs/adr/0026-the-protobuf-mapping-is-normative-and-nullable-is-the-default-mode.md),
  the lost-race decision is
  [ADR-0071](../docs/adr/0071-a-lost-table-creation-race-is-retried-by-a-wrapped-table-admin.md),
  and the full design research is recorded on issue
  [#12](https://github.com/flink-gcp/flink-connector-gcp/issues/12))

No source code has been copied into this module so far.
