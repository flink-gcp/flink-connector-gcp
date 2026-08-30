/*
 * Copyright 2026 The flink-gcp authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.UnroutableRecord;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.failure.BigQueryFailure;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.ParquetCompression;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalField;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldNullPolicy;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldType;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFields;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileLoadsWriter}, staging into an {@link InMemoryStagingStorage}. */
class FileLoadsWriterTest {

    private static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("name")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.REQUIRED))
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("value")
                                    .setType(TableFieldSchema.Type.INT64)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    // A column whose Avro conversion can fail on the value, which INT64 and STRING
                    // cannot: the writer routes those failures differently from parse failures.
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("amount")
                                    .setType(TableFieldSchema.Type.NUMERIC)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();

    /** The same shape plus a REPEATED column, whose Parquet encoding is the silent failure. */
    private static final TableSchema SCHEMA_WITH_REPEATED =
            SCHEMA.toBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("tags")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.REPEATED))
                    .build();

    /** The same shape plus a JSON column, which a Parquet load job refuses outright. */
    private static final TableSchema SCHEMA_WITH_JSON =
            SCHEMA.toBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("doc")
                                    .setType(TableFieldSchema.Type.JSON)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    /** A record routed to a table, optionally failing serialization or producing bad bytes. */
    static final class TestRow {
        private final String table;
        private final String name;
        private final Long value;
        private final boolean failSerialization;
        private final boolean produceGarbageBytes;

        TestRow(String table, String name, Long value) {
            this(table, name, value, false, false);
        }

        TestRow(
                String table,
                String name,
                Long value,
                boolean failSerialization,
                boolean produceGarbageBytes) {
            this.table = table;
            this.name = name;
            this.value = value;
            this.failSerialization = failSerialization;
            this.produceGarbageBytes = produceGarbageBytes;
        }
    }

    /** Serializes {@link TestRow}s into the wire form of the schema-derived descriptor. */
    private static final class TestRowSerializer extends BigQueryProtoSerializationSchema<TestRow> {
        private static final long serialVersionUID = 1L;

        private final TableSchema schema;
        private transient Descriptors.Descriptor descriptor;
        private int invocations;

        TestRowSerializer(TableSchema schema) {
            this.schema = schema;
        }

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return schema;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return descriptor();
        }

        private Descriptors.Descriptor descriptor() {
            if (descriptor == null) {
                try {
                    descriptor =
                            BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                                    schema);
                } catch (Descriptors.DescriptorValidationException e) {
                    throw new IllegalStateException(e);
                }
            }
            return descriptor;
        }

        @Override
        public ByteString serialize(TestRow element) throws IOException {
            invocations++;
            if (element.failSerialization) {
                throw new IOException("boom");
            }
            if ("runtime-boom".equals(element.name)) {
                throw new IllegalStateException("unchecked boom");
            }
            if ("skip-me".equals(element.name)) {
                return null;
            }
            if (element.produceGarbageBytes) {
                // Missing the REQUIRED name field; parsing rejects it.
                return ByteString.EMPTY;
            }
            DynamicMessage.Builder row = DynamicMessage.newBuilder(descriptor());
            row.setField(descriptor().findFieldByName("name"), element.name);
            if (element.value != null) {
                row.setField(descriptor().findFieldByName("value"), element.value);
            }
            if ("unconvertible".equals(element.name)) {
                // A NUMERIC too wide for the column's decimal precision: the row parses, and then
                // fails on the value — the writer's other row-level failure path.
                Descriptors.FieldDescriptor amount = descriptor().findFieldByName("amount");
                byte[] huge = new byte[32];
                Arrays.fill(huge, (byte) 0x7F);
                row.setField(
                        amount,
                        amount.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING
                                ? new BigInteger(huge).toString()
                                : ByteString.copyFrom(huge));
            }
            return row.build().toByteString();
        }
    }

    /** Collects failed rows instead of failing the job. */
    static final class CollectingHandler implements FailureHandler<BigQueryFailure> {
        private static final long serialVersionUID = 1L;

        private final List<BigQueryFailure> rows = new ArrayList<>();

        /** "handle"/"flush" in invocation order, pinning that flush follows the routed rows. */
        private final List<String> events = new ArrayList<>();

        private boolean closed;

        @Override
        public void handle(BigQueryFailure row) {
            rows.add(row);
            events.add("handle");
        }

        @Override
        public void flush() {
            events.add("flush");
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    static BigQuerySinkConfig<TestRow> config(FailureHandler<BigQueryFailure> handler) {
        return config(handler, SCHEMA);
    }

    static BigQuerySinkConfig<TestRow> config(
            FailureHandler<BigQueryFailure> handler, TableSchema schema) {
        return config(handler, schema, null);
    }

    static BigQuerySinkConfig<TestRow> config(
            FailureHandler<BigQueryFailure> handler,
            TableSchema schema,
            AdditionalFields<TestRow> additionalFields) {
        var builder =
                BigQuerySink.<TestRow>builder()
                        .writeMethod(WriteMethod.FILE_LOADS)
                        .destinationResolver(
                                (element, context) -> TableDestination.of("p", "d", element.table))
                        .serializer(new TestRowSerializer(schema))
                        .failureHandler(handler)
                        .fileLoadsOptions(
                                FileLoadsOptions.builder()
                                        .stagingPath("gs://bucket/prefix")
                                        .build());
        if (additionalFields != null) {
            builder.additionalFields(additionalFields);
        }
        BigQueryFileLoadsSink<TestRow> sink = (BigQueryFileLoadsSink<TestRow>) builder.build();
        return sink.getConfig();
    }

    private static FileLoadsWriter<TestRow> writer(
            BigQuerySinkConfig<TestRow> config, StagingStorage storage, FileLoadsOptions options) {
        return writer(
                config,
                storage,
                options,
                new ManualProcessingTimeService(),
                TestSinkWriterMetricGroup.create());
    }

    private static FileLoadsWriter<TestRow> writer(
            BigQuerySinkConfig<TestRow> config,
            StagingStorage storage,
            FileLoadsOptions options,
            ProcessingTimeService timers,
            TestSinkWriterMetricGroup metrics) {
        return new FileLoadsWriter<>(
                config,
                options,
                storage,
                metrics,
                "0123456789abcdef0123456789abcdef",
                3,
                1,
                timers);
    }

    private static FileLoadsWriter<TestRow> writer(
            BigQuerySinkConfig<TestRow> config, StagingStorage storage, long maxStagingFileBytes) {
        return new FileLoadsWriter<>(
                config,
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxStagingFileBytes(maxStagingFileBytes)
                        .build(),
                storage,
                TestSinkWriterMetricGroup.create(),
                "0123456789abcdef0123456789abcdef",
                3,
                1,
                new ManualProcessingTimeService());
    }

    private static List<GenericRecord> readAvro(byte[] bytes) throws IOException {
        List<GenericRecord> records = new ArrayList<>();
        try (DataFileReader<GenericRecord> reader =
                new DataFileReader<>(
                        new SeekableByteArrayInput(bytes), new GenericDatumReader<>())) {
            reader.forEach(records::add);
        }
        return records;
    }

    @Test
    void rejectsAMissingProcessingTimeService() {
        assertThatThrownBy(
                        () ->
                                new FileLoadsWriter<>(
                                        config(FailureHandler.failJob()),
                                        FileLoadsOptions.builder()
                                                .stagingPath("gs://bucket/prefix")
                                                .build(),
                                        new InMemoryStagingStorage(),
                                        TestSinkWriterMetricGroup.create(),
                                        "0123456789abcdef0123456789abcdef",
                                        3,
                                        1,
                                        null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("timerService");
    }

    @Test
    void routesAnExplicitResolutionFailureWithoutOpeningAStagingFile() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        CollectingHandler handler = new CollectingHandler();
        TestRowSerializer serializer = new TestRowSerializer(SCHEMA);
        UnroutableRecord unroutable =
                UnroutableRecord.of(ByteString.copyFromUtf8("original"), "unknown tenant");
        BigQuerySinkConfig<TestRow> config =
                ((BigQueryFileLoadsSink<TestRow>)
                                BigQuerySink.<TestRow>builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .destinationResolver((element, context) -> unroutable)
                                        .serializer(serializer)
                                        .failureHandler(handler)
                                        .fileLoadsOptions(
                                                FileLoadsOptions.builder()
                                                        .stagingPath("gs://bucket/prefix")
                                                        .build())
                                        .build())
                        .getConfig();
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        FileLoadsWriter<TestRow> writer =
                new FileLoadsWriter<>(
                        config,
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        storage,
                        metrics,
                        "0123456789abcdef0123456789abcdef",
                        3,
                        1,
                        new ManualProcessingTimeService());

        writer.write(new TestRow("ignored", "record", 1L), CONTEXT);

        assertThat(handler.rows).containsExactly(unroutable);
        assertThat(serializer.invocations).isZero();
        assertThat(writer.prepareCommit()).isEmpty();
        assertThat(storage.getObjects()).isEmpty();
        assertThat(metrics.counterValue("numRecordsSendErrors")).isEqualTo(1);
        assertThat(metrics.hasMetric("destination", "unresolved", "sendErrors")).isFalse();
    }

    @Test
    void explicitResolutionFailureFailsBeforeOpeningAStagingFileUnderTheDefaultPolicy()
            throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        TestRowSerializer serializer = new TestRowSerializer(SCHEMA);
        UnroutableRecord unroutable =
                UnroutableRecord.of(ByteString.copyFromUtf8("original"), "unknown tenant");
        BigQuerySinkConfig<TestRow> config =
                ((BigQueryFileLoadsSink<TestRow>)
                                BigQuerySink.<TestRow>builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .destinationResolver((element, context) -> unroutable)
                                        .serializer(serializer)
                                        .fileLoadsOptions(
                                                FileLoadsOptions.builder()
                                                        .stagingPath("gs://bucket/prefix")
                                                        .build())
                                        .build())
                        .getConfig();
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        FileLoadsWriter<TestRow> writer =
                new FileLoadsWriter<>(
                        config,
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        storage,
                        metrics,
                        "0123456789abcdef0123456789abcdef",
                        3,
                        1,
                        new ManualProcessingTimeService());

        assertThatThrownBy(() -> writer.write(new TestRow("ignored", "record", 1L), CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("destination unresolved")
                .hasMessageContaining("unknown tenant");
        assertThat(serializer.invocations).isZero();
        assertThat(writer.prepareCommit()).isEmpty();
        assertThat(storage.getObjects()).isEmpty();
        assertThat(metrics.counterValue("numRecordsSendErrors")).isEqualTo(1);
        assertThat(metrics.hasMetric("destination", "unresolved", "sendErrors")).isFalse();
    }

    @Test
    void treatsANullResolutionAsFatalBeforeTheFailurePolicy() {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        CollectingHandler handler = new CollectingHandler();
        TestRowSerializer serializer = new TestRowSerializer(SCHEMA);
        BigQuerySinkConfig<TestRow> config =
                ((BigQueryFileLoadsSink<TestRow>)
                                BigQuerySink.<TestRow>builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .destinationResolver((element, context) -> null)
                                        .serializer(serializer)
                                        .failureHandler(handler)
                                        .fileLoadsOptions(
                                                FileLoadsOptions.builder()
                                                        .stagingPath("gs://bucket/prefix")
                                                        .build())
                                        .build())
                        .getConfig();
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        FileLoadsWriter<TestRow> writer =
                new FileLoadsWriter<>(
                        config,
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        storage,
                        metrics,
                        "0123456789abcdef0123456789abcdef",
                        3,
                        1,
                        new ManualProcessingTimeService());

        assertThatThrownBy(() -> writer.write(new TestRow("ignored", "record", 1L), CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessage("The destination resolver returned null for a record.");
        assertThat(handler.rows).isEmpty();
        assertThat(serializer.invocations).isZero();
        assertThat(storage.getObjects()).isEmpty();
        assertThat(metrics.counterValue("numRecordsSendErrors")).isZero();
    }

    @Test
    void keepsUnexpectedResolverExceptionsFatal() {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        CollectingHandler handler = new CollectingHandler();
        TestRowSerializer serializer = new TestRowSerializer(SCHEMA);
        BigQuerySinkConfig<TestRow> config =
                ((BigQueryFileLoadsSink<TestRow>)
                                BigQuerySink.<TestRow>builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .destinationResolver(
                                                (element, context) -> {
                                                    throw new IllegalStateException("resolver bug");
                                                })
                                        .serializer(serializer)
                                        .failureHandler(handler)
                                        .fileLoadsOptions(
                                                FileLoadsOptions.builder()
                                                        .stagingPath("gs://bucket/prefix")
                                                        .build())
                                        .build())
                        .getConfig();
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        FileLoadsWriter<TestRow> writer =
                new FileLoadsWriter<>(
                        config,
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        storage,
                        metrics,
                        "0123456789abcdef0123456789abcdef",
                        3,
                        1,
                        new ManualProcessingTimeService());

        assertThatThrownBy(() -> writer.write(new TestRow("ignored", "record", 1L), CONTEXT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("resolver bug");
        assertThat(handler.rows).isEmpty();
        assertThat(serializer.invocations).isZero();
        assertThat(storage.getObjects()).isEmpty();
        assertThat(metrics.counterValue("numRecordsSendErrors")).isZero();
    }

    @Test
    void stagesParquetWhenTheFormatSelectsIt() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        parquetOptions(ParquetCompression.ZSTD));

        writer.write(new TestRow("t", "a", 1L), CONTEXT);
        writer.write(new TestRow("t", "b", 2L), CONTEXT);
        FileLoadsCommittable committable = writer.prepareCommit().iterator().next();
        writer.close();

        assertThat(committable.getFormat()).isEqualTo(StagingFormat.PARQUET);
        assertThat(committable.getUri()).endsWith(".parquet");
        assertThat(committable.getRowCount()).isEqualTo(2);
        // The container's own magic at both ends. Asserting the committable alone would pass on a
        // writer that stamped PARQUET and wrote an Avro file; the load job would then fail on the
        // service, which is the slowest possible place to find out.
        byte[] staged = storage.getObjects().get(committable.getUri());
        assertThat(new String(staged, 0, 4, StandardCharsets.US_ASCII)).isEqualTo("PAR1");
        assertThat(codecOf(staged)).isEqualTo(CompressionCodecName.ZSTD);
        assertThat(committable.getByteCount()).isEqualTo(staged.length);
    }

    @Test
    void stagesUncompressedParquetWhenTheCodecSelectsIt() throws Exception {
        // The Hadoop-free configuration. It has to be exercised because it takes a different
        // branch through parquet-hadoop's CodecFactory — the one that does not resolve a codec.
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        parquetOptions(ParquetCompression.NONE));

        writer.write(new TestRow("t", "a", 1L), CONTEXT);
        FileLoadsCommittable committable = writer.prepareCommit().iterator().next();
        writer.close();

        assertThat(committable.getFormat()).isEqualTo(StagingFormat.PARQUET);
        // The codec out of the footer, not the magic bytes: every codec writes PAR1, so magic
        // alone would pass on a writer that quietly compressed — which is the difference between
        // needing a Hadoop runtime and not.
        assertThat(codecOf(storage.getObjects().get(committable.getUri())))
                .isEqualTo(CompressionCodecName.UNCOMPRESSED);
    }

    @Test
    void stagesAvroForADestinationWhoseSchemaNamesAJsonColumn() throws Exception {
        // Not a preference: a PARQUET load job is refused whenever the provided schema names a
        // JSON column, whatever the file holds, so a Parquet file here could never be loaded.
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                new FileLoadsWriter<>(
                        config(FailureHandler.failJob(), SCHEMA_WITH_JSON),
                        parquetOptions(ParquetCompression.ZSTD),
                        storage,
                        TestSinkWriterMetricGroup.create(),
                        "0123456789abcdef0123456789abcdef",
                        3,
                        1,
                        new ManualProcessingTimeService());

        writer.write(new TestRow("t", "a", 1L), CONTEXT);
        FileLoadsCommittable committable = writer.prepareCommit().iterator().next();
        writer.close();

        assertThat(committable.getFormat()).isEqualTo(StagingFormat.AVRO);
        assertThat(committable.getUri()).endsWith(".avro");
    }

    @Test
    void jsonFallbackWarningIsBoundedAcrossDestinationReactivation() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                new FileLoadsWriter<>(
                        config(FailureHandler.failJob(), SCHEMA_WITH_JSON),
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .stagingFormat(StagingFormat.PARQUET)
                                .parquetCompression(ParquetCompression.ZSTD)
                                .maxOpenDestinations(1)
                                .build(),
                        storage,
                        TestSinkWriterMetricGroup.create(),
                        "0123456789abcdef0123456789abcdef",
                        3,
                        1,
                        new ManualProcessingTimeService());

        try (LogCapture capture = LogCapture.of(FileLoadsWriter.class);
                writer) {
            writer.write(new TestRow("a", "row", 1L), CONTEXT);
            writer.write(new TestRow("b", "row", 2L), CONTEXT);
            writer.write(new TestRow("a", "row", 3L), CONTEXT);

            assertThat(capture.getMessages())
                    .filteredOn(message -> message.contains("later fallback destinations"))
                    .singleElement()
                    .asString()
                    .contains("p.d.a");
        }
    }

    @Test
    void parquetCarriesAThreeLevelListSoRepeatedColumnsSurviveTheLoad() throws Exception {
        // The one silent failure this format has. parquet-avro's legacy default is a two-level
        // list, which BigQuery's enableListInference does not recognise — a REPEATED column then
        // loads as an empty array and the job reports success. Read back rather than assumed.
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                new FileLoadsWriter<>(
                        config(FailureHandler.failJob(), SCHEMA_WITH_REPEATED),
                        parquetOptions(ParquetCompression.ZSTD),
                        storage,
                        TestSinkWriterMetricGroup.create(),
                        "0123456789abcdef0123456789abcdef",
                        3,
                        1,
                        new ManualProcessingTimeService());

        writer.write(new TestRow("t", "a", 1L), CONTEXT);
        FileLoadsCommittable committable = writer.prepareCommit().iterator().next();
        writer.close();

        byte[] staged = storage.getObjects().get(committable.getUri());
        try (ParquetFileReader reader = ParquetFileReader.open(new BytesInputFile(staged))) {
            String schema = reader.getFooter().getFileMetaData().getSchema().toString();
            assertThat(schema).contains("group tags (LIST)").contains("repeated group list");
        }
    }

    @Test
    void parquetRollsAtTheThresholdRatherThanRunningToEndOfInput() throws Exception {
        // Parquet buffers a whole row group before anything reaches the stream, so the row-group
        // size has to come from the roll threshold. Left at Parquet's own 128 MiB default the
        // written byte count stays at zero and this produces one file however much is written.
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .stagingFormat(StagingFormat.PARQUET)
                                // Not 1: Parquet writes its magic on open, so a 1-byte threshold
                                // rolls immediately whatever the row-group size is, and would pass
                                // on a writer that never flushed a row group at all.
                                .maxStagingFileBytes(8 * 1024)
                                .build());

        for (int i = 0; i < 20_000; i++) {
            writer.write(new TestRow("t", "row-" + i, (long) i), CONTEXT);
        }
        Collection<FileLoadsCommittable> committables = writer.prepareCommit();
        writer.close();

        assertThat(committables).hasSizeGreaterThan(1);
        assertThat(committables).allSatisfy(c -> assertThat(c.getUri()).endsWith(".parquet"));
    }

    private static CompressionCodecName codecOf(byte[] staged) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new BytesInputFile(staged))) {
            return reader.getFooter().getBlocks().get(0).getColumns().get(0).getCodec();
        }
    }

    /** Parquet input over a byte array: the read side of the no-filesystem argument. */
    private static final class BytesInputFile implements InputFile {

        private final byte[] bytes;

        BytesInputFile(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public long getLength() {
            return bytes.length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new DelegatingSeekableInputStream(new ByteArrayInputStream(bytes)) {
                private long pos;

                @Override
                public long getPos() {
                    return pos;
                }

                @Override
                public void seek(long newPos) throws IOException {
                    getStream().reset();
                    long skipped = getStream().skip(newPos);
                    if (skipped != newPos) {
                        throw new IOException("Short seek: " + skipped + " of " + newPos);
                    }
                    pos = newPos;
                }

                @Override
                public int read() throws IOException {
                    int b = getStream().read();
                    if (b >= 0) {
                        pos++;
                    }
                    return b;
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    int n = getStream().read(b, off, len);
                    if (n > 0) {
                        pos += n;
                    }
                    return n;
                }
            };
        }
    }

    private static FileLoadsOptions parquetOptions(ParquetCompression compression) {
        return FileLoadsOptions.builder()
                .stagingPath("gs://bucket/prefix")
                .stagingFormat(StagingFormat.PARQUET)
                .parquetCompression(compression)
                .build();
    }

    @Test
    void stagesWithTheZstandardCodec() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);

        writer.write(new TestRow("t", "a", 1L), CONTEXT);
        FileLoadsCommittable committable = writer.prepareCommit().iterator().next();
        writer.close();

        byte[] staged = storage.getObjects().get(committable.getUri());
        try (DataFileReader<GenericRecord> reader =
                new DataFileReader<>(
                        new SeekableByteArrayInput(staged), new GenericDatumReader<>())) {
            // Read out of the container's own metadata, so this fails when the codec changes rather
            // than only when it stops working. Asserting the bytes decode would not do: every codec
            // decodes, so a silent switch back to deflate would cost 3.6x the write CPU (#283) with
            // nothing to report it.
            assertThat(reader.getMetaString("avro.codec")).isEqualTo("zstandard");
            assertThat(reader.next().get("name")).hasToString("a");
        }
    }

    @Test
    void stagesOneFilePerDestination() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        BigQuerySinkConfig<TestRow> config = config(FailureHandler.failJob());
        FileLoadsWriter<TestRow> writer =
                writer(config, storage, FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);

        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        writer.write(new TestRow("t1", "b", null), CONTEXT);
        writer.write(new TestRow("t2", "c", 3L), CONTEXT);
        assertThat(storage.getObjects()).isEmpty(); // Nothing visible before prepareCommit.

        Collection<FileLoadsCommittable> committables = writer.prepareCommit();
        writer.close();

        assertThat(committables).hasSize(2);
        FileLoadsCommittable t1 =
                committables.stream()
                        .filter(c -> c.getDestination().getTable().equals("t1"))
                        .findFirst()
                        .orElseThrow();
        assertThat(t1.getRowCount()).isEqualTo(2);
        assertThat(t1.getByteCount()).isPositive();
        assertThat(t1.getUri())
                .startsWith("gs://bucket/prefix/0123456789abcdef0123456789abcdef/p.d.t1/3-1-")
                .endsWith(".avro");
        assertThat(storage.getObjects()).containsKey(t1.getUri());
        assertThat(t1.getByteCount()).isEqualTo(storage.getObjects().get(t1.getUri()).length);

        List<GenericRecord> records = readAvro(storage.getObjects().get(t1.getUri()));
        assertThat(records).hasSize(2);
        assertThat(records.get(0).get("name")).hasToString("a");
        assertThat(records.get(0).get("value")).isEqualTo(1L);
        assertThat(records.get(1).get("name")).hasToString("b");
        assertThat(records.get(1).get("value")).isNull();
    }

    @Test
    void stagesConfiguredPhysicalFieldsThroughTheFileLoadsWriter() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        AdditionalFields<TestRow> additionalFields =
                AdditionalFields.<TestRow>builder()
                        .field(
                                AdditionalField.of(
                                        "source",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.REQUIRED,
                                        row -> "computed-" + row.name))
                        .build();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob(), SCHEMA, additionalFields),
                        storage,
                        FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);

        writer.write(new TestRow("t", "alpha", 1L), CONTEXT);
        FileLoadsCommittable committable = writer.prepareCommit().iterator().next();
        writer.close();

        List<GenericRecord> records = readAvro(storage.getObjects().get(committable.getUri()));
        assertThat(records)
                .singleElement()
                .satisfies(
                        record -> {
                            assertThat(record.get("name")).hasToString("alpha");
                            assertThat(record.get("source")).hasToString("computed-alpha");
                        });
    }

    @Test
    void rollsFilesAtMaxSize() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer = writer(config(FailureHandler.failJob()), storage, 1);

        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        writer.write(new TestRow("t1", "b", 2L), CONTEXT);
        writer.write(new TestRow("t1", "c", 3L), CONTEXT);
        Collection<FileLoadsCommittable> committables = writer.prepareCommit();
        writer.close();

        assertThat(committables).hasSize(3);
        assertThat(committables.stream().map(FileLoadsCommittable::getUri).distinct()).hasSize(3);
        assertThat(committables).allSatisfy(c -> assertThat(c.getRowCount()).isEqualTo(1));
    }

    @Test
    void routesSerializationFailuresToHandler() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        CollectingHandler handler = new CollectingHandler();
        FileLoadsWriter<TestRow> writer =
                writer(config(handler), storage, FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);

        writer.write(new TestRow("t1", "a", 1L, true, false), CONTEXT);
        Collection<FileLoadsCommittable> committables = writer.prepareCommit();
        writer.close();

        assertThat(committables).isEmpty();
        assertThat(handler.rows).hasSize(1);
        assertThat(handler.rows.get(0).getPayloadBytes()).isNull();
        assertThat(handler.rows.get(0).describeDestination()).isEqualTo("p.d.t1");
    }

    @Test
    void routesSerializerRuntimeExceptionsToHandler() throws Exception {
        // A poison record must reach the handler no matter how the serializer fails.
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        CollectingHandler handler = new CollectingHandler();
        FileLoadsWriter<TestRow> writer =
                writer(config(handler), storage, FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);

        writer.write(new TestRow("t1", "runtime-boom", 1L), CONTEXT);
        Collection<FileLoadsCommittable> committables = writer.prepareCommit();
        writer.close();

        assertThat(committables).isEmpty();
        assertThat(handler.rows).hasSize(1);
        assertThat(handler.rows.get(0).getCause()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void routesUnparseableRowsToHandler() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        CollectingHandler handler = new CollectingHandler();
        FileLoadsWriter<TestRow> writer =
                writer(config(handler), storage, FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);

        writer.write(new TestRow("t1", "a", 1L, false, true), CONTEXT);
        writer.write(new TestRow("t1", "b", 2L), CONTEXT);
        Collection<FileLoadsCommittable> committables = writer.prepareCommit();
        writer.close();

        assertThat(handler.rows).hasSize(1);
        assertThat(handler.rows.get(0).getPayloadBytes()).isNotNull();
        assertThat(committables)
                .singleElement()
                .satisfies(c -> assertThat(c.getRowCount()).isEqualTo(1));
    }

    @Test
    void serializationFailureFailsJobUnderDefaultPolicy() {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);

        assertThatThrownBy(() -> writer.write(new TestRow("t1", "a", 1L, true, false), CONTEXT))
                .isInstanceOf(IOException.class);
    }

    @Test
    void flushLeavesStagedFilesAloneInBothModes() throws Exception {
        // A pre-end-of-input flush is a checkpoint — the streaming trigger; prepareCommit(),
        // which follows every flush, does the actual file finishing. flush() only tells the
        // failure handler to persist what was routed to it.
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        new InMemoryStagingStorage(),
                        FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);

        writer.flush(false);
        writer.flush(true);
    }

    @Test
    void handlerFlushRunsAtEveryWriterFlush() throws Exception {
        CollectingHandler handler = new CollectingHandler();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(handler),
                        new InMemoryStagingStorage(),
                        FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);

        writer.write(new TestRow("t1", "a", 1L, true, false), CONTEXT);
        writer.flush(false);
        writer.flush(true);

        // The routed row is handled before the first flush(), so a buffering handler has
        // everything when it persists; end of input flushes the handler too.
        assertThat(handler.events).containsExactly("handle", "flush", "flush");
    }

    @Test
    void closeClosesTheHandler() throws Exception {
        CollectingHandler handler = new CollectingHandler();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(handler),
                        new InMemoryStagingStorage(),
                        FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);

        writer.close();

        assertThat(handler.closed).isTrue();
    }

    @Test
    void closeStillClosesTheHandlerWhenAbortingAStagedFileThrowsAnError() throws Exception {
        // #276: the handler is last after every open staged file, and Flink's IOUtils.closeAll
        // rethrew an Error from inside its loop, leaving it open. StagedFileWriter.abort() swallows
        // an IOException or a RuntimeException by design, so an Error is the only failure this list
        // can carry at all — which is what makes the failure path pinnable here.
        CollectingHandler handler = new CollectingHandler();
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(config(handler), storage, FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);
        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        storage.closeFailure = new NoClassDefFoundError("staged file close blew up");

        assertThatThrownBy(writer::close)
                .isInstanceOf(NoClassDefFoundError.class)
                .hasMessage("staged file close blew up");
        assertThat(handler.closed).isTrue();
    }

    @Test
    void closeClosesTheStagingStorage() throws Exception {
        // #820: GcsStagingStorage holds a Storage client for the life of the task and no close()
        // path reached it. The writer owns its own deserialized copy of the sink's storage, so it
        // is the writer's close() that has to release it.
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(new CollectingHandler()),
                        storage,
                        FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);

        writer.close();

        assertThat(storage.getCloseCount()).isEqualTo(1);
    }

    @Test
    void aStagingStorageCloseFailureIsSuppressedOntoTheOneAlreadyBeingReported() throws Exception {
        // The staging client is last in the Closers.closeAll list, which reports the *first*
        // failure and suppresses the rest onto it. So a client this writer has finished with can
        // never displace what an abort or the handler had to say — and the handler still closes.
        CollectingHandler handler = new CollectingHandler();
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(config(handler), storage, FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);
        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        storage.closeFailure = new NoClassDefFoundError("staged file close blew up");
        IllegalStateException clientTeardown =
                new IllegalStateException("staging client teardown blew up");
        storage.failOnClose(clientTeardown);

        assertThatThrownBy(writer::close)
                .isInstanceOf(NoClassDefFoundError.class)
                .hasMessage("staged file close blew up")
                .satisfies(e -> assertThat(e.getSuppressed()).containsExactly(clientTeardown));
        assertThat(handler.closed).isTrue();
        assertThat(storage.getCloseCount()).isEqualTo(1);
    }

    @Test
    void capacityFinishesTheLeastRecentlyUsedDestinationAndReopenUsesANewUri() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .maxOpenDestinations(2)
                                .build(),
                        new ManualProcessingTimeService(),
                        metrics);

        writer.write(new TestRow("a", "a1", 1L), CONTEXT);
        writer.write(new TestRow("b", "b1", 1L), CONTEXT);
        writer.write(new TestRow("a", "a2", 2L), CONTEXT);
        writer.write(new TestRow("c", "c1", 1L), CONTEXT);

        assertThat(metrics.<Integer>gaugeValue("openDestinations")).isEqualTo(2);
        assertThat(metrics.counterValue("capacityEvictions")).isEqualTo(1);
        assertThat(storage.getObjects().keySet()).singleElement().asString().contains("/p.d.b/");

        writer.write(new TestRow("b", "b2", 2L), CONTEXT);
        Collection<FileLoadsCommittable> committables = writer.prepareCommit();

        assertThat(committables).hasSize(4);
        assertThat(committables).extracting(FileLoadsCommittable::getUri).doesNotHaveDuplicates();
        assertThat(metrics.<Integer>gaugeValue("openDestinations")).isZero();
        assertThat(metrics.counterValue("destinationActivations")).isEqualTo(4);
        assertThat(metrics.counterValue("capacityEvictions")).isEqualTo(2);
    }

    @Test
    void invalidNewDestinationDoesNotEvictAHealthyActiveDestination() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.logAndDrop()),
                        storage,
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .maxOpenDestinations(1)
                                .build(),
                        new ManualProcessingTimeService(),
                        metrics);

        writer.write(new TestRow("a", "valid", 1L), CONTEXT);
        writer.write(new TestRow("b", "invalid", 1L, false, true), CONTEXT);

        assertThat(metrics.<Integer>gaugeValue("openDestinations")).isEqualTo(1);
        assertThat(metrics.counterValue("capacityEvictions")).isZero();
        assertThat(storage.getObjects()).isEmpty();
        assertThat(writer.prepareCommit())
                .singleElement()
                .extracting(FileLoadsCommittable::getDestination)
                .isEqualTo(TableDestination.of("p", "d", "a"));
    }

    @Test
    void defaultCapacityBoundsTheActiveDestinationGauge() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        new ManualProcessingTimeService(),
                        metrics);

        for (int i = 0; i < 17; i++) {
            writer.write(new TestRow("t" + i, "row", (long) i), CONTEXT);
        }

        assertThat(metrics.<Integer>gaugeValue("openDestinations")).isEqualTo(16);
        assertThat(metrics.counterValue("capacityEvictions")).isEqualTo(1);
        assertThat(writer.prepareCommit()).hasSize(17);
        assertThat(metrics.<Integer>gaugeValue("openDestinations")).isZero();
    }

    @Test
    void pendingFileLimitFailsBeforeDestinationChurnCanRetainAnotherFile() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .maxOpenDestinations(2)
                                .maxPendingFiles(3)
                                .build(),
                        new ManualProcessingTimeService(),
                        metrics);

        writer.write(new TestRow("a", "row", 1L), CONTEXT);
        writer.write(new TestRow("b", "row", 2L), CONTEXT);
        writer.write(new TestRow("c", "row", 3L), CONTEXT);

        assertThatThrownBy(() -> writer.write(new TestRow("d", "row", 4L), CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("reached maxPendingFiles=3");
        assertThat(metrics.<Integer>gaugeValue("pendingFiles")).isEqualTo(3);
        assertThat(metrics.<Integer>gaugeValue("openDestinations")).isEqualTo(2);
        assertThat(metrics.counterValue("capacityEvictions")).isEqualTo(1);
        assertThat(writer.prepareCommit()).hasSize(3);
        assertThat(metrics.<Integer>gaugeValue("pendingFiles")).isZero();
    }

    @Test
    void idleTimeoutFinishesAtTheBoundaryAndRearmsUntilClose() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        ManualProcessingTimeService timers = new ManualProcessingTimeService();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .destinationIdleTimeout(java.time.Duration.ofMinutes(1))
                                .build(),
                        timers,
                        metrics);

        writer.write(new TestRow("a", "a1", 1L), CONTEXT);
        timers.advanceTo(59_999);
        assertThat(metrics.<Integer>gaugeValue("openDestinations")).isEqualTo(1);

        timers.advanceTo(60_000);
        assertThat(metrics.<Integer>gaugeValue("openDestinations")).isZero();
        assertThat(metrics.counterValue("idleEvictions")).isEqualTo(1);
        assertThat(storage.getObjects()).hasSize(1);

        writer.write(new TestRow("b", "b1", 1L), CONTEXT);
        timers.advanceTo(120_000);
        assertThat(metrics.counterValue("idleEvictions")).isEqualTo(2);
        writer.close();
        timers.advanceTo(180_000);
        assertThat(metrics.counterValue("idleEvictions")).isEqualTo(2);
    }

    @Test
    void aRecentWriteRefreshesTheIdleDeadline() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        ManualProcessingTimeService timers = new ManualProcessingTimeService();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .destinationIdleTimeout(java.time.Duration.ofMinutes(1))
                                .build(),
                        timers,
                        metrics);

        writer.write(new TestRow("a", "a1", 1L), CONTEXT);
        timers.advanceTo(30_000);
        writer.write(new TestRow("a", "a2", 2L), CONTEXT);
        timers.advanceTo(60_000);

        assertThat(metrics.<Integer>gaugeValue("openDestinations")).isEqualTo(1);
        assertThat(metrics.counterValue("idleEvictions")).isZero();

        timers.advanceTo(90_000);
        assertThat(metrics.<Integer>gaugeValue("openDestinations")).isZero();
        assertThat(metrics.counterValue("idleEvictions")).isEqualTo(1);
    }

    @Test
    void maximumIdleTimeoutCannotOverflowTheTimerDeadline() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        ManualProcessingTimeService timers = new ManualProcessingTimeService();
        timers.advanceTo(1);
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .destinationIdleTimeout(java.time.Duration.ofMillis(Long.MAX_VALUE))
                                .build(),
                        timers,
                        metrics);

        writer.write(new TestRow("a", "a1", 1L), CONTEXT);

        assertThat(timers.registeredTimerCount()).isZero();
        assertThat(metrics.<Integer>gaugeValue("openDestinations")).isEqualTo(1);
    }

    @Test
    void serializedRowLimitAcceptsTheBoundaryAndRoutesTheNextByte() throws Exception {
        TestRow row = new TestRow("a", "row", 1L);
        int serializedSize = new TestRowSerializer(SCHEMA).serialize(row).size();

        InMemoryStagingStorage acceptedStorage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> accepted =
                writer(
                        config(FailureHandler.failJob()),
                        acceptedStorage,
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .maxSerializedRowBytes(serializedSize)
                                .build());
        accepted.write(row, CONTEXT);
        assertThat(accepted.prepareCommit()).hasSize(1);

        CollectingHandler handler = new CollectingHandler();
        InMemoryStagingStorage rejectedStorage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> rejected =
                writer(
                        config(handler),
                        rejectedStorage,
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .maxSerializedRowBytes(serializedSize - 1L)
                                .build());
        rejected.write(row, CONTEXT);

        assertThat(handler.rows)
                .singleElement()
                .extracting(BigQueryFailure::getErrorMessage)
                .asString()
                .contains("exceeding the configured")
                .contains((serializedSize - 1L) + "-byte");
        assertThat(rejected.prepareCommit()).isEmpty();
        assertThat(rejectedStorage.getObjects()).isEmpty();
    }

    @Test
    void checkpointFinalizationUsesTheConfiguredConcurrencyBound() throws Exception {
        BlockingCloseStagingStorage storage = new BlockingCloseStagingStorage();
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .maxOpenDestinations(3)
                                .maxConcurrentCheckpointFinalizations(3)
                                .build(),
                        new ManualProcessingTimeService(),
                        metrics);
        writer.write(new TestRow("a", "row", 1L), CONTEXT);
        writer.write(new TestRow("b", "row", 2L), CONTEXT);
        writer.write(new TestRow("c", "row", 3L), CONTEXT);

        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<Collection<FileLoadsCommittable>> finalization =
                    caller.submit(writer::prepareCommit);

            assertThat(storage.awaitAllCloses()).isTrue();
            assertThat(storage.maximumActiveCloses()).isEqualTo(3);
            // Ownership moved to the finalizer before its workers started. A fatal worker can
            // return while a peer still runs, so writer.close() must not see that peer as open.
            assertThat(metrics.<Integer>gaugeValue("openDestinations")).isZero();
            storage.releaseCloses();

            assertThat(finalization.get(5, TimeUnit.SECONDS)).hasSize(3);
            assertThat(storage.maximumActiveCloses()).isEqualTo(3);
        } finally {
            storage.releaseCloses();
            caller.shutdownNow();
        }
    }

    @Test
    void checkpointFinalizationDefaultsToTheCallingThread() throws Exception {
        CloseThreadStagingStorage storage = new CloseThreadStagingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .maxOpenDestinations(2)
                                .build());
        writer.write(new TestRow("a", "row", 1L), CONTEXT);
        writer.write(new TestRow("b", "row", 2L), CONTEXT);
        Thread caller = Thread.currentThread();

        assertThat(writer.prepareCommit()).hasSize(2);

        assertThat(storage.closeThreads()).containsExactly(caller, caller);
    }

    @Test
    void singletonCheckpointFinalizationStaysOnTheCallingThreadAtHigherConcurrency()
            throws Exception {
        CloseThreadStagingStorage storage = new CloseThreadStagingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .maxConcurrentCheckpointFinalizations(3)
                                .build());
        writer.write(new TestRow("a", "row", 1L), CONTEXT);
        Thread caller = Thread.currentThread();

        assertThat(writer.prepareCommit()).hasSize(1);

        assertThat(storage.closeThreads()).containsExactly(caller);
    }

    @Test
    void multiplePrepareCommitCyclesYieldDistinctUris() throws Exception {
        // Streaming execution calls prepareCommit once per checkpoint; the per-destination file
        // sequence must keep growing so a later checkpoint's file never reuses an earlier URI.
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);

        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        writer.flush(false);
        Collection<FileLoadsCommittable> first = writer.prepareCommit();

        writer.write(new TestRow("t1", "b", 2L), CONTEXT);
        writer.flush(false);
        Collection<FileLoadsCommittable> second = writer.prepareCommit();
        writer.close();

        assertThat(first).hasSize(1);
        assertThat(second).hasSize(1);
        String firstUri = first.iterator().next().getUri();
        String secondUri = second.iterator().next().getUri();
        assertThat(secondUri).isNotEqualTo(firstUri);
        assertThat(storage.getObjects()).containsKeys(firstUri, secondUri);
        assertThat(readAvro(storage.getObjects().get(secondUri)))
                .singleElement()
                .satisfies(record -> assertThat(record.get("name")).hasToString("b"));
    }

    @Test
    void closeWithoutPrepareCommitLeavesNoCommittables() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailureHandler.failJob()),
                        storage,
                        FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);

        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        writer.close();

        // The aborted file may or may not have finalized an object; either way no committable
        // references it, which is what keeps failed attempts out of load jobs.
        assertThat(writer.prepareCommit()).isEmpty();
    }

    private static final class BlockingCloseStagingStorage implements StagingStorage {
        private static final long serialVersionUID = 1L;

        private final CountDownLatch allCloses = new CountDownLatch(3);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger activeCloses = new AtomicInteger();
        private final AtomicInteger maximumActiveCloses = new AtomicInteger();

        @Override
        public OutputStream createObject(String gcsUri) {
            return new ByteArrayOutputStream() {
                private final AtomicBoolean closed = new AtomicBoolean();

                @Override
                public void close() throws IOException {
                    if (!closed.compareAndSet(false, true)) {
                        return;
                    }
                    int active = activeCloses.incrementAndGet();
                    maximumActiveCloses.accumulateAndGet(active, Math::max);
                    allCloses.countDown();
                    try {
                        if (!release.await(5, TimeUnit.SECONDS)) {
                            throw new IOException("Timed out waiting to release staged files");
                        }
                        super.close();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Staging close interrupted", e);
                    } finally {
                        activeCloses.decrementAndGet();
                    }
                }
            };
        }

        @Override
        public void deleteObjects(List<String> gcsUris) {}

        @Override
        public void close() {}

        private boolean awaitAllCloses() throws InterruptedException {
            return allCloses.await(5, TimeUnit.SECONDS);
        }

        private void releaseCloses() {
            release.countDown();
        }

        private int maximumActiveCloses() {
            return maximumActiveCloses.get();
        }
    }

    private static final class CloseThreadStagingStorage implements StagingStorage {
        private static final long serialVersionUID = 1L;

        private final List<Thread> closeThreads = new ArrayList<>();

        @Override
        public OutputStream createObject(String gcsUri) {
            return new ByteArrayOutputStream() {
                @Override
                public void close() throws IOException {
                    closeThreads.add(Thread.currentThread());
                    super.close();
                }
            };
        }

        @Override
        public void deleteObjects(List<String> gcsUris) {}

        @Override
        public void close() {}

        private List<Thread> closeThreads() {
            return closeThreads;
        }
    }
}
