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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;
import com.google.rpc.Status;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptionsProvider;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeTypeProvider;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalField;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldNullPolicy;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldType;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFields;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.tables.RetriableTableAdminException;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableSchemaSnapshot;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigQueryDefaultStreamWriter}. */
class BigQueryDefaultStreamWriterTest {

    static final TableAdmin NOOP_ADMIN = new NoopTableAdmin();

    /**
     * Admin whose tables always "exist" implicitly: creation is a no-op, reads find nothing.
     *
     * <p>{@code create} keeps the SPI's {@code throws IOException} although it never throws, so
     * subclasses can script a failing creation — which is otherwise impossible, since an override
     * may not widen the checked exceptions of the method it overrides.
     */
    static class NoopTableAdmin implements TableAdmin {
        @Override
        public void create(
                TableDestination destination,
                TableSchema schema,
                io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions options)
                throws IOException {}

        @Override
        public boolean ensureCdcTable(
                TableDestination destination,
                TableSchema schema,
                TableCreateOptionsProvider createOptionsProvider,
                CdcTableOptions cdcOptions,
                CreateDisposition createDisposition,
                CdcTableReconciliationPolicy reconciliationPolicy)
                throws IOException {
            create(destination, schema, createOptionsProvider.optionsFor(destination));
            return true;
        }

        @Override
        public TableSchemaSnapshot getSchema(TableDestination destination) {
            return null;
        }

        @Override
        public boolean updateSchema(
                TableDestination destination, TableSchemaSnapshot base, TableSchema proposed) {
            return true;
        }
    }

    /** A fast retry schedule for tests: 1 ms backoffs, the given attempt budget. */
    static RetrySchedule fastSchedule(int maxAttempts) {
        return new RetrySchedule(1, 1, maxAttempts, 0);
    }

    /**
     * What a subtask that lost the table-creation race to the per-table metadata-update quota is
     * answered, as {@code BigQueryTableAdmin} types it. Measured 2026-08-08 by racing sixteen
     * concurrent creations of one missing table: five came back HTTP 403 / {@code
     * rateLimitExceeded} rather than the 409 the connector treats as success (#383).
     *
     * <p>Here rather than in one test class because all three writer test classes drive it.
     */
    static RetriableTableAdminException rateLimited(TableDestination destination) {
        return new RetriableTableAdminException(
                "Failed to create BigQuery table " + destination,
                new BigQueryException(
                        403,
                        "Exceeded rate limits: too many table update operations for this table.",
                        new BigQueryError("rateLimitExceeded", null, "Exceeded rate limits")));
    }

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    /** Serializer writing the record string bytes; descriptor is irrelevant for the fake. */
    private static class StringSerializer extends BigQueryProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        private final List<TableDestination> descriptorRequests = new ArrayList<>();

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("f")
                                    .setType(TableFieldSchema.Type.INT64)
                                    .setMode(TableFieldSchema.Mode.NULLABLE)
                                    .build())
                    .build();
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            descriptorRequests.add(destination);
            return Empty.getDescriptor();
        }

        @Override
        public ByteString serialize(String element) {
            return ByteString.copyFromUtf8(element);
        }
    }

    /** Serializer emitting one oversized row. */
    private static class OversizedSerializer extends BigQueryProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return TableSchema.getDefaultInstance();
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return Empty.getDescriptor();
        }

        @Override
        public ByteString serialize(String element) {
            return ByteString.copyFrom(new byte[BigQueryDefaultStreamWriter.MAX_ROW_BYTES + 1]);
        }
    }

    private static class CdcStringSerializer extends BigQueryProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        private static final TableSchema SCHEMA =
                TableSchema.newBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("value")
                                        .setType(TableFieldSchema.Type.STRING)
                                        .setMode(TableFieldSchema.Mode.NULLABLE))
                        .build();
        private static final Descriptors.Descriptor DESCRIPTOR = descriptor();

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return SCHEMA;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return DESCRIPTOR;
        }

        @Override
        public ByteString serialize(String element) {
            return DynamicMessage.newBuilder(DESCRIPTOR)
                    .setField(DESCRIPTOR.findFieldByName("value"), element)
                    .build()
                    .toByteString();
        }

        private static Descriptors.Descriptor descriptor() {
            try {
                return BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(SCHEMA);
            } catch (Descriptors.DescriptorValidationException e) {
                throw new AssertionError(e);
            }
        }
    }

    /** Serializer whose physical field collides case-insensitively with a CDC pseudocolumn. */
    private static class ConflictingCdcSerializer extends BigQueryProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        private static final TableSchema SCHEMA =
                TableSchema.newBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("_CHANGE_TYPE")
                                        .setType(TableFieldSchema.Type.STRING)
                                        .setMode(TableFieldSchema.Mode.NULLABLE))
                        .build();
        private static final Descriptors.Descriptor DESCRIPTOR = descriptor();

        private final AtomicInteger serializations = new AtomicInteger();

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return SCHEMA;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return DESCRIPTOR;
        }

        @Override
        public ByteString serialize(String element) {
            serializations.incrementAndGet();
            return ByteString.EMPTY;
        }

        private static Descriptors.Descriptor descriptor() {
            try {
                return BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(SCHEMA);
            } catch (Descriptors.DescriptorValidationException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static class FakeAppenderFactory implements RowAppenderFactory {
        private static final long serialVersionUID = 1L;

        private final Map<TableDestination, FakeAppender> appenders = new LinkedHashMap<>();
        private final Map<TableDestination, Descriptors.Descriptor> descriptors =
                new LinkedHashMap<>();

        /** Shared script: consumed globally in append order across all appenders. */
        private final List<ApiFuture<AppendRowsResponse>> scriptedResults = new ArrayList<>();

        @Override
        public RowAppender create(
                TableDestination destination,
                Descriptors.Descriptor rowDescriptor,
                String location) {
            FakeAppender appender = new FakeAppender();
            appenders.put(destination, appender);
            descriptors.put(destination, rowDescriptor);
            return appender;
        }

        private class FakeAppender implements RowAppender {
            private final List<ProtoRows> appends = new ArrayList<>();
            private boolean closed;

            @Override
            public ApiFuture<AppendRowsResponse> append(ProtoRows rows) {
                appends.add(rows);
                if (scriptedResults.isEmpty()) {
                    return ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance());
                }
                return scriptedResults.remove(0);
            }

            @Override
            public void close() {
                closed = true;
            }
        }
    }

    private static final class RecordingCdcTableAdmin extends NoopTableAdmin {
        private final List<TableDestination> ensured = new ArrayList<>();
        private TableSchema schema;
        private CdcTableOptions options;
        private boolean ready;
        private boolean creationRequested = true;

        @Override
        public boolean ensureCdcTable(
                TableDestination destination,
                TableSchema schema,
                TableCreateOptionsProvider createOptionsProvider,
                CdcTableOptions options,
                CreateDisposition createDisposition,
                CdcTableReconciliationPolicy reconciliationPolicy) {
            ensured.add(destination);
            this.schema = schema;
            this.options = options;
            ready = true;
            return creationRequested;
        }
    }

    private static BigQuerySinkConfig<String> config(
            DestinationResolver<? super String> resolver,
            BigQueryProtoSerializer<? super String> serializer) {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destinationResolver(resolver)
                                .serializer(serializer)
                                .build();
        return sink.getConfig();
    }

    private static List<String> rowsOf(ProtoRows rows) {
        List<String> values = new ArrayList<>();
        rows.getSerializedRowsList().forEach(b -> values.add(b.toStringUtf8()));
        return values;
    }

    @Test
    void routesRowsToPerDestinationAppenders() throws Exception {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(
                                (element, context) ->
                                        TableDestination.of("p", "d", element.substring(0, 1)),
                                new StringSerializer()),
                        factory,
                        NOOP_ADMIN,
                        TestSinkWriterMetricGroup.create());

        writer.write("a1", CONTEXT);
        writer.write("b1", CONTEXT);
        writer.write("a2", CONTEXT);
        writer.flush(false);

        assertThat(factory.appenders.keySet())
                .containsExactly(
                        TableDestination.of("p", "d", "a"), TableDestination.of("p", "d", "b"));
        FakeAppenderFactory.FakeAppender a =
                factory.appenders.get(TableDestination.of("p", "d", "a"));
        FakeAppenderFactory.FakeAppender b =
                factory.appenders.get(TableDestination.of("p", "d", "b"));
        assertThat(a.appends).hasSize(1);
        assertThat(rowsOf(a.appends.get(0))).containsExactly("a1", "a2");
        assertThat(b.appends).hasSize(1);
        assertThat(rowsOf(b.appends.get(0))).containsExactly("b1");
    }

    @Test
    void appendsCdcMetadataWithTheAugmentedDescriptorForDynamicDestinations() throws Exception {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destinationResolver(
                                        (element, context) ->
                                                TableDestination.of(
                                                        "p", "d", element.substring(0, 1)))
                                .serializer(new CdcStringSerializer())
                                .createDisposition(CreateDisposition.CREATE_NEVER)
                                .cdcOptions(
                                        CdcOptions.<String>builder(
                                                        CdcChangeTypeProvider.upsertOnly())
                                                .sequenceNumberProvider(
                                                        element ->
                                                                Integer.toHexString(
                                                                        element.length()))
                                                .build())
                                .build();
        SinkWriter<String> writer =
                sink.createWriter(factory, NOOP_ADMIN, TestSinkWriterMetricGroup.create());

        writer.write("abcdefghij", CONTEXT);
        writer.write("beta", CONTEXT);
        writer.flush(false);

        for (Map.Entry<TableDestination, FakeAppenderFactory.FakeAppender> entry :
                factory.appenders.entrySet()) {
            Descriptors.Descriptor descriptor = factory.descriptors.get(entry.getKey());
            DynamicMessage row =
                    DynamicMessage.parseFrom(
                            descriptor, entry.getValue().appends.get(0).getSerializedRows(0));
            assertThat(row.getField(descriptor.findFieldByName("_change_type")))
                    .isEqualTo("UPSERT");
            String value = (String) row.getField(descriptor.findFieldByName("value"));
            assertThat(row.getField(descriptor.findFieldByName("_change_sequence_number")))
                    .isEqualTo(value.length() == 10 ? "A" : "4");
        }
        assertThat(factory.descriptors).hasSize(2);
    }

    @Test
    void appendsConfiguredPhysicalFieldsThroughTheDefaultStreamWriter() throws Exception {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        TableDestination destination = TableDestination.of("p", "d", "t");
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(destination)
                                .serializer(new CdcStringSerializer())
                                .additionalFields(
                                        AdditionalFields.<String>builder()
                                                .field(
                                                        AdditionalField.of(
                                                                "source",
                                                                AdditionalFieldType.STRING,
                                                                AdditionalFieldNullPolicy.REQUIRED,
                                                                value -> "computed-" + value))
                                                .build())
                                .build();
        SinkWriter<String> writer =
                sink.createWriter(factory, NOOP_ADMIN, TestSinkWriterMetricGroup.create());

        writer.write("alpha", CONTEXT);
        writer.flush(false);

        Descriptors.Descriptor descriptor = factory.descriptors.get(destination);
        DynamicMessage row =
                DynamicMessage.parseFrom(
                        descriptor,
                        factory.appenders.get(destination).appends.get(0).getSerializedRows(0));
        assertThat(row.getField(descriptor.findFieldByName("value"))).isEqualTo("alpha");
        assertThat(row.getField(descriptor.findFieldByName("source"))).isEqualTo("computed-alpha");
    }

    @Test
    void provisionsCdcTableBeforeOpeningTheFirstAppender() throws Exception {
        RecordingCdcTableAdmin admin = new RecordingCdcTableAdmin();
        FakeAppenderFactory factory =
                new FakeAppenderFactory() {
                    @Override
                    public RowAppender create(
                            TableDestination destination,
                            Descriptors.Descriptor rowDescriptor,
                            String location) {
                        assertThat(admin.ready).isTrue();
                        return super.create(destination, rowDescriptor, location);
                    }
                };
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(TableDestination.of("p", "d", "t"))
                                .serializer(new CdcStringSerializer())
                                .additionalFields(
                                        AdditionalFields.<String>builder()
                                                .field(
                                                        AdditionalField.of(
                                                                "source",
                                                                AdditionalFieldType.STRING,
                                                                AdditionalFieldNullPolicy.REQUIRED,
                                                                value -> "computed-" + value))
                                                .build())
                                .cdcOptions(
                                        CdcOptions.<String>builder(
                                                        CdcChangeTypeProvider.upsertOnly())
                                                .build())
                                .cdcTableOptions(
                                        CdcTableOptions.builder()
                                                .primaryKeyColumns(
                                                        java.util.Collections.singletonList(
                                                                "value"))
                                                .maxStaleness(java.time.Duration.ofMinutes(10))
                                                .build())
                                .build();
        SinkWriter<String> writer =
                sink.createWriter(factory, admin, TestSinkWriterMetricGroup.create());

        writer.write("first", CONTEXT);
        writer.write("second", CONTEXT);
        writer.flush(false);

        assertThat(admin.ensured).containsExactly(TableDestination.of("p", "d", "t"));
        assertThat(admin.schema.getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("value", "source");
        assertThat(admin.options.getPrimaryKeyColumns()).containsExactly("value");
        assertThat(admin.options.getMaxStaleness()).isEqualTo(java.time.Duration.ofMinutes(10));
    }

    @Test
    void existingCdcTableVerificationDoesNotResolveCreationOptions() throws Exception {
        RecordingCdcTableAdmin admin = new RecordingCdcTableAdmin();
        admin.creationRequested = false;
        AtomicInteger creationOptionsCalls = new AtomicInteger();
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(TableDestination.of("p", "d", "t"))
                                .serializer(new CdcStringSerializer())
                                .cdcOptions(
                                        CdcOptions.<String>builder(
                                                        CdcChangeTypeProvider.upsertOnly())
                                                .build())
                                .cdcTableOptions(
                                        CdcTableOptions.builder()
                                                .primaryKeyColumns(
                                                        java.util.Collections.singletonList(
                                                                "value"))
                                                .build())
                                .tableCreateOptionsProvider(
                                        destination -> {
                                            creationOptionsCalls.incrementAndGet();
                                            return TableCreateOptions.builder().build();
                                        })
                                .build();
        SinkWriter<String> writer =
                sink.createWriter(
                        new FakeAppenderFactory(), admin, TestSinkWriterMetricGroup.create());

        writer.write("value", CONTEXT);

        assertThat(creationOptionsCalls).hasValue(0);
        assertThat(admin.ensured).containsExactly(TableDestination.of("p", "d", "t"));
    }

    @Test
    void nullCdcTableOptionsIdentifyTheProviderAndDestination() {
        TableDestination destination = TableDestination.of("p", "d", "t");
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(destination)
                                .serializer(new CdcStringSerializer())
                                .cdcOptions(
                                        CdcOptions.<String>builder(
                                                        CdcChangeTypeProvider.upsertOnly())
                                                .build())
                                .cdcTableOptionsProvider(ignored -> null)
                                .build();
        SinkWriter<String> writer =
                sink.createWriter(
                        new FakeAppenderFactory(),
                        new RecordingCdcTableAdmin(),
                        TestSinkWriterMetricGroup.create());

        assertThatThrownBy(() -> writer.write("value", CONTEXT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("CdcTableOptionsProvider returned null")
                .hasMessageContaining(destination.toString());
    }

    @Test
    void oldJobGraphsKeepTheirPreCreatedCdcTableBehavior() throws Exception {
        RecordingCdcTableAdmin admin = new RecordingCdcTableAdmin();
        FakeAppenderFactory factory = new FakeAppenderFactory();
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(TableDestination.of("p", "d", "t"))
                                .serializer(new CdcStringSerializer())
                                .cdcOptions(
                                        CdcOptions.<String>builder(
                                                        CdcChangeTypeProvider.upsertOnly())
                                                .build())
                                .cdcTableOptions(
                                        CdcTableOptions.builder()
                                                .primaryKeyColumns(
                                                        java.util.Collections.singletonList(
                                                                "value"))
                                                .build())
                                .build();
        BigQuerySinkConfig<String> config = sink.getConfig();
        java.lang.reflect.Field field =
                BigQuerySinkConfig.class.getDeclaredField("manageCdcTableCreation");
        field.setAccessible(true);
        field.setBoolean(config, false);
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config,
                        factory,
                        admin,
                        TestSinkWriterMetricGroup.create(),
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        fastSchedule(3),
                        fastSchedule(3));

        writer.write("value", CONTEXT);

        assertThat(admin.ensured).isEmpty();
        assertThat(factory.appenders).hasSize(1);
    }

    @Test
    void rejectsCdcDescriptorConflictsBeforeRowFailureHandling() {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        ConflictingCdcSerializer serializer = new ConflictingCdcSerializer();
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(TableDestination.of("p", "d", "t"))
                                .serializer(serializer)
                                .createDisposition(CreateDisposition.CREATE_NEVER)
                                .cdcOptions(
                                        CdcOptions.<String>builder(
                                                        CdcChangeTypeProvider.upsertOnly())
                                                .build())
                                .build();
        SinkWriter<String> writer =
                sink.createWriter(factory, NOOP_ADMIN, TestSinkWriterMetricGroup.create());

        assertThatThrownBy(() -> writer.write("value", CONTEXT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pseudocolumn");
        assertThat(serializer.serializations).hasValue(0);
        assertThat(factory.appenders).isEmpty();
    }

    @Test
    void appendsWhenBatchSizeThresholdIsReached() throws Exception {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(
                                (element, context) -> TableDestination.of("p", "d", "t"),
                                new StringSerializer()),
                        factory,
                        NOOP_ADMIN,
                        TestSinkWriterMetricGroup.create(),
                        4,
                        fastSchedule(1),
                        fastSchedule(1));

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);
        writer.write("cc", CONTEXT);
        writer.flush(false);

        FakeAppenderFactory.FakeAppender appender = factory.appenders.values().iterator().next();
        assertThat(appender.appends).hasSize(2);
        assertThat(rowsOf(appender.appends.get(0))).containsExactly("aa", "bb");
        assertThat(rowsOf(appender.appends.get(1))).containsExactly("cc");
    }

    /** The options constructor must plumb the batching cap through to the append path. */
    @Test
    void optionsConstructorAppliesMaxAppendRequestBytes() throws Exception {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(
                                (element, context) -> TableDestination.of("p", "d", "t"),
                                new StringSerializer()),
                        factory,
                        NOOP_ADMIN,
                        TestSinkWriterMetricGroup.create(),
                        DefaultStreamOptions.builder().maxAppendRequestBytes(4).build());

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);
        writer.write("cc", CONTEXT);
        writer.flush(false);

        FakeAppenderFactory.FakeAppender appender = factory.appenders.values().iterator().next();
        assertThat(appender.appends).hasSize(2);
        assertThat(rowsOf(appender.appends.get(0))).containsExactly("aa", "bb");
        assertThat(rowsOf(appender.appends.get(1))).containsExactly("cc");
    }

    @Test
    void rejectsOversizedRows() {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(
                                (element, context) -> TableDestination.of("p", "d", "t"),
                                new OversizedSerializer()),
                        factory,
                        NOOP_ADMIN,
                        TestSinkWriterMetricGroup.create());

        assertThatThrownBy(() -> writer.write("big", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("per-row limit");
    }

    @Test
    void asyncAppendFailureFailsSubsequentWrite() throws Exception {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        SettableApiFuture<AppendRowsResponse> failing = SettableApiFuture.create();
        factory.scriptedResults.add(failing);
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(
                                (element, context) -> TableDestination.of("p", "d", "t"),
                                new StringSerializer()),
                        factory,
                        NOOP_ADMIN,
                        TestSinkWriterMetricGroup.create(),
                        1,
                        fastSchedule(1),
                        fastSchedule(1));

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT); // triggers async append of "aa"
        failing.setException(new RuntimeException("boom"));

        assertThatThrownBy(() -> writer.write("cc", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage("boom");
    }

    @Test
    void flushSurfacesFailedAppends() throws Exception {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        SettableApiFuture<AppendRowsResponse> failing = SettableApiFuture.create();
        failing.setException(new RuntimeException("append failed"));
        factory.scriptedResults.add(failing);
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(
                                (element, context) -> TableDestination.of("p", "d", "t"),
                                new StringSerializer()),
                        factory,
                        NOOP_ADMIN,
                        TestSinkWriterMetricGroup.create());

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage("append failed");
    }

    @Test
    void responseLevelErrorsFailTheFlush() throws Exception {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        factory.scriptedResults.add(
                ApiFutures.immediateFuture(
                        AppendRowsResponse.newBuilder()
                                .setError(Status.newBuilder().setMessage("schema mismatch"))
                                .build()));
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(
                                (element, context) -> TableDestination.of("p", "d", "t"),
                                new StringSerializer()),
                        factory,
                        NOOP_ADMIN,
                        TestSinkWriterMetricGroup.create());

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("schema mismatch");
    }

    @Test
    void flushInspectsResponsesCompletedWhileWaiting() throws Exception {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        SettableApiFuture<AppendRowsResponse> pending = SettableApiFuture.create();
        factory.scriptedResults.add(pending);
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(
                                (element, context) -> TableDestination.of("p", "d", "t"),
                                new StringSerializer()),
                        factory,
                        NOOP_ADMIN,
                        TestSinkWriterMetricGroup.create());

        writer.write("aa", CONTEXT);

        // Complete the append with an errored response from another thread while flush() is
        // blocked in get(): flush must fail based on the response itself, independent of
        // completion-callback scheduling.
        CountDownLatch started = new CountDownLatch(1);
        Thread completer =
                new Thread(
                        () -> {
                            try {
                                started.await();
                                Thread.sleep(100);
                            } catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();
                            }
                            pending.set(
                                    AppendRowsResponse.newBuilder()
                                            .setError(Status.newBuilder().setMessage("late error"))
                                            .build());
                        });
        completer.start();
        started.countDown();

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("late error");
        completer.join();
    }

    @Test
    void requestsDescriptorPerDestination() throws Exception {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        StringSerializer serializer = new StringSerializer();
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(
                                (element, context) -> TableDestination.of("p", "d", element),
                                serializer),
                        factory,
                        NOOP_ADMIN,
                        TestSinkWriterMetricGroup.create());

        writer.write("t1", CONTEXT);
        writer.write("t2", CONTEXT);
        writer.write("t1", CONTEXT);
        writer.flush(false);

        assertThat(serializer.descriptorRequests)
                .containsExactly(
                        TableDestination.of("p", "d", "t1"), TableDestination.of("p", "d", "t2"));
    }

    @Test
    void closeClosesAllAppenders() throws Exception {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(
                                (element, context) -> TableDestination.of("p", "d", element),
                                new StringSerializer()),
                        factory,
                        NOOP_ADMIN,
                        TestSinkWriterMetricGroup.create());

        writer.write("t1", CONTEXT);
        writer.write("t2", CONTEXT);
        writer.flush(false);
        writer.close();

        assertThat(factory.appenders.values()).allSatisfy(a -> assertThat(a.closed).isTrue());
    }

    @Test
    void sinkCreatesFunctionalWriterThroughFactorySeam() throws Exception {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(TableDestination.of("p", "d", "t"))
                                .serializer(new StringSerializer())
                                .build();

        SinkWriter<String> writer =
                sink.createWriter(factory, NOOP_ADMIN, TestSinkWriterMetricGroup.create());
        writer.write("row", CONTEXT);
        writer.flush(false);
        writer.close();

        assertThat(writer).isInstanceOf(BigQueryDefaultStreamWriter.class);
        FakeAppenderFactory.FakeAppender appender =
                factory.appenders.get(TableDestination.of("p", "d", "t"));
        assertThat(rowsOf(appender.appends.get(0))).containsExactly("row");
    }
}
