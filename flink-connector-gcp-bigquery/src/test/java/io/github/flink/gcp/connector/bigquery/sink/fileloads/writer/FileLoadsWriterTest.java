/*
 * Copyright 2026 laughingman7743
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

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRow;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRowHandler;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
                    .build();

    private static final SinkWriter.Context CONTEXT =
            new SinkWriter.Context() {
                @Override
                public long currentWatermark() {
                    return 0;
                }

                @Override
                public Long timestamp() {
                    return null;
                }
            };

    /** A record routed to a table, optionally failing serialization or producing bad bytes. */
    private static final class TestRow {
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
    private static final class TestRowSerializer extends BigQueryProtoSerializer<TestRow> {
        private static final long serialVersionUID = 1L;

        private transient Descriptors.Descriptor descriptor;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return SCHEMA;
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
                                    SCHEMA);
                } catch (Descriptors.DescriptorValidationException e) {
                    throw new IllegalStateException(e);
                }
            }
            return descriptor;
        }

        @Override
        public ByteString serialize(TestRow element) throws IOException {
            if (element.failSerialization) {
                throw new IOException("boom");
            }
            if ("runtime-boom".equals(element.name)) {
                throw new IllegalStateException("unchecked boom");
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
            return row.build().toByteString();
        }
    }

    /** Collects failed rows instead of failing the job. */
    private static final class CollectingHandler implements FailedRowHandler {
        private static final long serialVersionUID = 1L;

        private final List<FailedRow> rows = new ArrayList<>();

        @Override
        public void handle(FailedRow row) {
            rows.add(row);
        }
    }

    private static BigQuerySinkConfig<TestRow> config(FailedRowHandler handler) {
        BigQueryFileLoadsSink<TestRow> sink =
                (BigQueryFileLoadsSink<TestRow>)
                        BigQuerySink.<TestRow>builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                .destinationResolver(
                                        (element, context) ->
                                                TableDestination.of("p", "d", element.table))
                                .serializer(new TestRowSerializer())
                                .failedRowHandler(handler)
                                .fileLoadsOptions(
                                        FileLoadsOptions.builder()
                                                .stagingPath("gs://bucket/prefix")
                                                .build())
                                .build();
        return sink.getConfig();
    }

    private static FileLoadsWriter<TestRow> writer(
            BigQuerySinkConfig<TestRow> config, StagingStorage storage, long maxFileBytes) {
        return new FileLoadsWriter<>(
                config,
                FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                storage,
                "0123456789abcdef0123456789abcdef",
                3,
                1,
                maxFileBytes);
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
    void stagesOneFilePerDestination() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        BigQuerySinkConfig<TestRow> config = config(FailedRowHandler.failJob());
        FileLoadsWriter<TestRow> writer =
                writer(config, storage, FileLoadsWriter.DEFAULT_MAX_FILE_BYTES);

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
    void rollsFilesAtMaxSize() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer = writer(config(FailedRowHandler.failJob()), storage, 1);

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
                writer(config(handler), storage, FileLoadsWriter.DEFAULT_MAX_FILE_BYTES);

        writer.write(new TestRow("t1", "a", 1L, true, false), CONTEXT);
        Collection<FileLoadsCommittable> committables = writer.prepareCommit();
        writer.close();

        assertThat(committables).isEmpty();
        assertThat(handler.rows).hasSize(1);
        assertThat(handler.rows.get(0).getRowBytes()).isNull();
        assertThat(handler.rows.get(0).getDestination())
                .isEqualTo(TableDestination.of("p", "d", "t1"));
    }

    @Test
    void routesSerializerRuntimeExceptionsToHandler() throws Exception {
        // A poison record must reach the handler no matter how the serializer fails.
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        CollectingHandler handler = new CollectingHandler();
        FileLoadsWriter<TestRow> writer =
                writer(config(handler), storage, FileLoadsWriter.DEFAULT_MAX_FILE_BYTES);

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
                writer(config(handler), storage, FileLoadsWriter.DEFAULT_MAX_FILE_BYTES);

        writer.write(new TestRow("t1", "a", 1L, false, true), CONTEXT);
        writer.write(new TestRow("t1", "b", 2L), CONTEXT);
        Collection<FileLoadsCommittable> committables = writer.prepareCommit();
        writer.close();

        assertThat(handler.rows).hasSize(1);
        assertThat(handler.rows.get(0).getRowBytes()).isNotNull();
        assertThat(committables)
                .singleElement()
                .satisfies(c -> assertThat(c.getRowCount()).isEqualTo(1));
    }

    @Test
    void serializationFailureFailsJobUnderDefaultPolicy() {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailedRowHandler.failJob()),
                        storage,
                        FileLoadsWriter.DEFAULT_MAX_FILE_BYTES);

        assertThatThrownBy(() -> writer.write(new TestRow("t1", "a", 1L, true, false), CONTEXT))
                .isInstanceOf(IOException.class);
    }

    @Test
    void flushBeforeEndOfInputRejectsStreamingExecution() {
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailedRowHandler.failJob()),
                        new InMemoryStagingStorage(),
                        FileLoadsWriter.DEFAULT_MAX_FILE_BYTES);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("batch");
        writer.flush(true); // End-of-input flush is fine.
    }

    @Test
    void closeWithoutPrepareCommitLeavesNoCommittables() throws Exception {
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(
                        config(FailedRowHandler.failJob()),
                        storage,
                        FileLoadsWriter.DEFAULT_MAX_FILE_BYTES);

        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        writer.close();

        // The aborted file may or may not have finalized an object; either way no committable
        // references it, which is what keeps failed attempts out of load jobs.
        assertThat(writer.prepareCommit()).isEmpty();
    }
}
