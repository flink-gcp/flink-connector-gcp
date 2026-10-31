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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeTypeProvider;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcOptions;
import io.github.flink.gcp.connector.bigquery.sink.failure.BigQueryFailure;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalField;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldNullPolicy;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldType;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFields;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.SchemaDerivationException;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Tests that a row serialized before the serializer schema evolves keeps its appended fields — the
 * CDC pseudocolumns and the configured additional fields — when the rebuilt stream writer appends
 * it under the evolved descriptor (#1950). The row's bytes are never re-encoded, so the appended
 * fields must keep their field numbers across the evolution.
 */
class BigQueryDefaultStreamWriterAugmentedEvolutionTest {

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    /** Field number the appended fields count down from (ProtoDescriptorAugmenter). */
    private static final int CEILING = (1 << 18) - 1;

    /** Key under which a decoded row records the schema version of the descriptor that read it. */
    private static final String READ_WITH = "@readWith";

    /**
     * Serializer writing a record {@code "key:value"} as the STRING columns {@code k} and {@code
     * v}. {@link #addColumn(int)} appends a STRING column {@code n} after them and advances the
     * fingerprint; field number 3 is the evolution the serializer contract asks for.
     */
    private static final class GrowingSerializer extends BigQueryProtoSerializationSchema<String> {
        private static final long serialVersionUID = 1L;

        private TableSchema schema =
                TableSchema.newBuilder().addFields(column("k")).addFields(column("v")).build();
        private int newColumnNumber;
        private int pendingColumnNumber;
        private int version;
        private transient Descriptors.Descriptor descriptor;

        private static TableFieldSchema column(String name) {
            return TableFieldSchema.newBuilder()
                    .setName(name)
                    .setType(TableFieldSchema.Type.STRING)
                    .setMode(TableFieldSchema.Mode.NULLABLE)
                    .build();
        }

        void addColumn(int fieldNumber) {
            schema = schema.toBuilder().addFields(column("n")).build();
            newColumnNumber = fieldNumber;
            version++;
            descriptor = null;
        }

        /** Makes the next {@link #serialize(String)} call {@link #addColumn(int)} first. */
        void addColumnOnNextSerialize(int fieldNumber) {
            pendingColumnNumber = fieldNumber;
        }

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return schema;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            if (descriptor == null) {
                try {
                    Descriptors.Descriptor derived =
                            BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                                    schema);
                    descriptor =
                            derived.findFieldByName("n") == null
                                    ? derived
                                    : renumber(derived, "n", newColumnNumber);
                } catch (Descriptors.DescriptorValidationException e) {
                    throw new AssertionError(e);
                }
            }
            return descriptor;
        }

        private static Descriptors.Descriptor renumber(
                Descriptors.Descriptor descriptor, String field, int number)
                throws Descriptors.DescriptorValidationException {
            FileDescriptorProto.Builder file = descriptor.getFile().toProto().toBuilder();
            DescriptorProto.Builder message = file.getMessageTypeBuilder(descriptor.getIndex());
            for (FieldDescriptorProto.Builder candidate : message.getFieldBuilderList()) {
                if (candidate.getName().equals(field)) {
                    candidate.setNumber(number);
                }
            }
            return Descriptors.FileDescriptor.buildFrom(
                            file.build(),
                            descriptor
                                    .getFile()
                                    .getDependencies()
                                    .toArray(new Descriptors.FileDescriptor[0]))
                    .findMessageTypeByName(descriptor.getName());
        }

        @Override
        public Object getSchemaFingerprint(TableDestination destination) {
            return version;
        }

        @Override
        public ByteString serialize(String element) {
            if (pendingColumnNumber != 0) {
                addColumn(pendingColumnNumber);
                pendingColumnNumber = 0;
            }
            String[] parts = element.split(":", 2);
            Descriptors.Descriptor d = getDescriptor(DESTINATION);
            return DynamicMessage.newBuilder(d)
                    .setField(d.findFieldByName("k"), parts[0])
                    .setField(d.findFieldByName("v"), parts[1])
                    .build()
                    .toByteString();
        }
    }

    /**
     * Appender factory decoding every appended row with the descriptor its stream writer was built
     * with, as the service does, and recording under {@link #READ_WITH} whether that descriptor was
     * the original ({@code "v0"}) or the evolved one ({@code "v1"}). The first request fails once
     * when {@link #failFirst} is set; {@link #failed} records the descriptor it was sent with.
     */
    private static final class DecodingFactory implements RowAppenderFactory {
        private static final long serialVersionUID = 1L;

        private final List<Map<String, Object>> rows = new ArrayList<>();
        private final List<String> failed = new ArrayList<>();
        private boolean failFirst;

        @Override
        public RowAppender create(
                TableDestination destination,
                Descriptors.Descriptor rowDescriptor,
                String location) {
            String version = rowDescriptor.findFieldByName("n") == null ? "v0" : "v1";
            return new RowAppender() {
                @Override
                public ApiFuture<AppendRowsResponse> append(ProtoRows request) {
                    if (failFirst) {
                        failFirst = false;
                        failed.add(version);
                        return ApiFutures.immediateFailedFuture(
                                new StatusRuntimeException(Status.UNAVAILABLE));
                    }
                    for (ByteString row : request.getSerializedRowsList()) {
                        rows.add(decode(row, rowDescriptor, version));
                    }
                    return ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance());
                }

                @Override
                public void close() {}
            };
        }

        private static Map<String, Object> decode(
                ByteString row, Descriptors.Descriptor d, String version) {
            try {
                DynamicMessage message = DynamicMessage.parseFrom(d, row);
                Map<String, Object> fields = new LinkedHashMap<>();
                message.getAllFields()
                        .forEach((field, value) -> fields.put(field.getName(), value));
                fields.put(READ_WITH, version);
                return fields;
            } catch (InvalidProtocolBufferException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static SinkWriter<String> writer(
            GrowingSerializer serializer,
            DecodingFactory factory,
            FailureHandler<BigQueryFailure> failureHandler,
            Consumer<BigQuerySinkBuilder<String>> augmentation) {
        BigQuerySinkBuilder<String> builder =
                BigQuerySink.<String>builder()
                        .table(DESTINATION)
                        .serializer(serializer)
                        .failureHandler(failureHandler)
                        .defaultStreamOptions(
                                DefaultStreamOptions.builder()
                                        // One row per request: a row is sent when the next one
                                        // arrives, unless a rebuild or a flush sends it first.
                                        .maxAppendRequestBytes(1)
                                        .build());
        augmentation.accept(builder);
        return ((BigQueryDefaultStreamSink<String>) builder.build())
                .createWriter(
                        factory,
                        new BigQueryDefaultStreamWriterTest.NoopTableAdmin(),
                        TestSinkWriterMetricGroup.create());
    }

    private static void cdc(BigQuerySinkBuilder<String> builder) {
        builder.cdcOptions(CdcOptions.<String>builder(CdcChangeTypeProvider.upsertOnly()).build())
                .cdcTableOptions(CdcTableOptions.builder().primaryKeyColumns(List.of("k")).build());
    }

    private static void additionalField(BigQuerySinkBuilder<String> builder) {
        builder.additionalFields(
                AdditionalFields.<String>builder()
                        .field(
                                AdditionalField.<String>of(
                                        "source",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.REQUIRED,
                                        record -> "src-" + record.charAt(0)))
                        .build());
    }

    /**
     * Writes {@code a:1}, which is still pending when a column is added, then {@code b:2}: the
     * fingerprint refresh moves {@code a:1} into the rebuilt state, whose stream writer sends it.
     */
    private static List<Map<String, Object>> pendingAcrossEvolution(
            Consumer<BigQuerySinkBuilder<String>> augmentation) throws Exception {
        GrowingSerializer serializer = new GrowingSerializer();
        DecodingFactory factory = new DecodingFactory();
        SinkWriter<String> writer =
                writer(serializer, factory, FailureHandler.failJob(), augmentation);
        writer.write("a:1", CONTEXT);
        serializer.addColumn(3);
        writer.write("b:2", CONTEXT);
        writer.flush(false);
        return factory.rows;
    }

    /**
     * Writes {@code a:1} and {@code x:0}, which sends {@code a:1} under the original descriptor and
     * fails; then adds a column and writes {@code b:2}, whose repair rebuilds the stream writer
     * with the evolved descriptor and re-sends {@code a:1} through it.
     */
    private static List<Map<String, Object>> repairedAcrossEvolution(
            Consumer<BigQuerySinkBuilder<String>> augmentation) throws Exception {
        GrowingSerializer serializer = new GrowingSerializer();
        DecodingFactory factory = new DecodingFactory();
        factory.failFirst = true;
        SinkWriter<String> writer =
                writer(serializer, factory, FailureHandler.failJob(), augmentation);
        writer.write("a:1", CONTEXT);
        writer.write("x:0", CONTEXT);
        assertThat(factory.failed).containsExactly("v0");
        serializer.addColumn(3);
        writer.write("b:2", CONTEXT);
        writer.flush(false);
        return factory.rows;
    }

    @Test
    void aCdcRowPendingAcrossEvolutionKeepsItsChangeType() throws Exception {
        assertThat(pendingAcrossEvolution(BigQueryDefaultStreamWriterAugmentedEvolutionTest::cdc))
                .containsExactlyInAnyOrder(
                        Map.of("k", "a", "v", "1", "_change_type", "UPSERT", READ_WITH, "v1"),
                        Map.of("k", "b", "v", "2", "_change_type", "UPSERT", READ_WITH, "v1"));
    }

    @Test
    void aCdcRowARepairResendsAfterEvolutionKeepsItsChangeType() throws Exception {
        assertThat(repairedAcrossEvolution(BigQueryDefaultStreamWriterAugmentedEvolutionTest::cdc))
                .containsExactlyInAnyOrder(
                        Map.of("k", "a", "v", "1", "_change_type", "UPSERT", READ_WITH, "v1"),
                        Map.of("k", "x", "v", "0", "_change_type", "UPSERT", READ_WITH, "v1"),
                        Map.of("k", "b", "v", "2", "_change_type", "UPSERT", READ_WITH, "v1"));
    }

    @Test
    void anAdditionalFieldPendingAcrossEvolutionKeepsItsValue() throws Exception {
        assertThat(
                        pendingAcrossEvolution(
                                BigQueryDefaultStreamWriterAugmentedEvolutionTest::additionalField))
                .containsExactlyInAnyOrder(
                        Map.of("k", "a", "v", "1", "source", "src-a", READ_WITH, "v1"),
                        Map.of("k", "b", "v", "2", "source", "src-b", READ_WITH, "v1"));
    }

    @Test
    void anAdditionalFieldARepairResendsAfterEvolutionKeepsItsValue() throws Exception {
        assertThat(
                        repairedAcrossEvolution(
                                BigQueryDefaultStreamWriterAugmentedEvolutionTest::additionalField))
                .containsExactlyInAnyOrder(
                        Map.of("k", "a", "v", "1", "source", "src-a", READ_WITH, "v1"),
                        Map.of("k", "x", "v", "0", "source", "src-x", READ_WITH, "v1"),
                        Map.of("k", "b", "v", "2", "source", "src-b", READ_WITH, "v1"));
    }

    /**
     * An evolution whose new column takes the number an appended field holds fails the write, even
     * under a failure policy that drops failed rows, because the schema is derived before the
     * row-failure boundary.
     */
    @Test
    void anEvolutionTakingAnAppendedFieldNumberFailsTheWrite() throws Exception {
        GrowingSerializer serializer = new GrowingSerializer();
        DecodingFactory factory = new DecodingFactory();
        SinkWriter<String> writer =
                writer(
                        serializer,
                        factory,
                        FailureHandler.logAndDrop(),
                        BigQueryDefaultStreamWriterAugmentedEvolutionTest::cdc);
        writer.write("a:1", CONTEXT);
        serializer.addColumn(CEILING);

        assertThatThrownBy(() -> writer.write("b:2", CONTEXT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "moves BigQuery CDC pseudocolumn _change_type from field number "
                                + CEILING
                                + " to "
                                + (CEILING - 1));
        assertThat(factory.rows).isEmpty();
    }

    /**
     * The same evolution, made by the serializer while it serializes a record, is derived inside
     * the row-failure boundary, and still fails the write rather than dropping the record (#1954).
     */
    @Test
    void anEvolutionTakingAnAppendedFieldNumberWhileSerializingFailsTheWrite() throws Exception {
        GrowingSerializer serializer = new GrowingSerializer();
        DecodingFactory factory = new DecodingFactory();
        BigQueryBufferedStreamWriterTest.RecordingHandler handler =
                new BigQueryBufferedStreamWriterTest.RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        serializer,
                        factory,
                        handler,
                        BigQueryDefaultStreamWriterAugmentedEvolutionTest::cdc);
        writer.write("a:1", CONTEXT);
        serializer.addColumnOnNextSerialize(CEILING);

        Throwable thrown = catchThrowable(() -> writer.write("b:2", CONTEXT));

        // The handler first: had the record reached it, its failure says why.
        assertThat(handler.rows).isEmpty();
        assertThat(thrown)
                .isInstanceOf(SchemaDerivationException.class)
                .rootCause()
                .hasMessageContaining(
                        "moves BigQuery CDC pseudocolumn _change_type from field number "
                                + CEILING
                                + " to "
                                + (CEILING - 1));
    }
}
