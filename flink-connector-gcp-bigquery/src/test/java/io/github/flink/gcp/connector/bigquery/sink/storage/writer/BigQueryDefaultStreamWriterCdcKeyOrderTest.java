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
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.OrderingMode;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptionsProvider;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeTypeProvider;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcOptions;
import io.github.flink.gcp.connector.bigquery.sink.failure.BigQueryFailure;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.tables.PrimaryKeyColumn;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link OrderingMode#PER_TABLE} keeps one CDC primary key out of an append request
 * twice (ADR-0192). A test that expects a split runs {@link OrderingMode#NONE} over the same input
 * as its control, so that it can fail; the tests that expect no split are held by the split tests
 * and by the primary-key reads they count.
 */
class BigQueryDefaultStreamWriterCdcKeyOrderTest {

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    /** A record whose serialized row carries its key in the wrong wire type. */
    private static final String UNPARSEABLE = "unparseable";

    private static final List<PrimaryKeyColumn> STRING_KEY =
            Collections.singletonList(new PrimaryKeyColumn("k", StandardSQLTypeName.STRING, null));

    /**
     * Serializer writing a record {@code "key:value"} as the two STRING columns {@code k} and
     * {@code v}, with a schema fingerprint a test can advance.
     */
    private static class KeyValueSerializer extends BigQueryProtoSerializationSchema<String> {
        private static final long serialVersionUID = 1L;

        private static final TableSchema SCHEMA =
                TableSchema.newBuilder().addFields(column("k")).addFields(column("v")).build();

        private static final Descriptors.Descriptor DESCRIPTOR = descriptor();

        private int version;

        private static TableFieldSchema column(String name) {
            return TableFieldSchema.newBuilder()
                    .setName(name)
                    .setType(TableFieldSchema.Type.STRING)
                    .setMode(TableFieldSchema.Mode.NULLABLE)
                    .build();
        }

        private static Descriptors.Descriptor descriptor() {
            try {
                return BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(SCHEMA);
            } catch (Descriptors.DescriptorValidationException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return SCHEMA;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return DESCRIPTOR;
        }

        @Override
        public Object getSchemaFingerprint(TableDestination destination) {
            return version;
        }

        @Override
        public ByteString serialize(String element) {
            if (element.equals(UNPARSEABLE)) {
                // The STRING key k (field 1) written as a varint: the row parses, with the value
                // kept as an unknown field, but its key cannot be read.
                return ByteString.copyFrom(new byte[] {0x08, 0x01});
            }
            String[] parts = element.split(":", 2);
            return DynamicMessage.newBuilder(DESCRIPTOR)
                    .setField(DESCRIPTOR.findFieldByName("k"), parts[0])
                    .setField(DESCRIPTOR.findFieldByName("v"), parts[1])
                    .build()
                    .toByteString();
        }

        static String recordOf(ByteString row, Descriptors.Descriptor writeDescriptor) {
            if (writeDescriptor.findFieldByName("k") == null) {
                // Another serializer's row, which only its request size describes.
                return "?";
            }
            try {
                DynamicMessage message = DynamicMessage.parseFrom(writeDescriptor, row);
                if (!message.hasField(writeDescriptor.findFieldByName("k"))) {
                    // The mistyped key is kept as an unknown field, so k reads as unset.
                    return UNPARSEABLE;
                }
                return message.getField(writeDescriptor.findFieldByName("k"))
                        + ":"
                        + message.getField(writeDescriptor.findFieldByName("v"));
            } catch (InvalidProtocolBufferException e) {
                throw new AssertionError(e);
            }
        }
    }

    /**
     * Appender factory answering every append at once and recording each request's records. A
     * request whose first record was named by {@link #failingOnce} fails the first time it is sent.
     */
    private static class RecordingFactory implements RowAppenderFactory {
        private static final long serialVersionUID = 1L;

        private final List<List<String>> requests = new ArrayList<>();
        private final Set<String> failOnce = new HashSet<>();

        RecordingFactory failingOnce(String firstRecord) {
            failOnce.add(firstRecord);
            return this;
        }

        @Override
        public RowAppender create(
                TableDestination destination,
                Descriptors.Descriptor rowDescriptor,
                String location) {
            return new RowAppender() {
                @Override
                public ApiFuture<AppendRowsResponse> append(ProtoRows rows) {
                    List<String> request = new ArrayList<>();
                    for (ByteString row : rows.getSerializedRowsList()) {
                        request.add(KeyValueSerializer.recordOf(row, rowDescriptor));
                    }
                    requests.add(request);
                    if (failOnce.remove(request.get(0))) {
                        return ApiFutures.immediateFailedFuture(
                                new StatusRuntimeException(Status.UNAVAILABLE));
                    }
                    return ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance());
                }

                @Override
                public void close() {}
            };
        }
    }

    /** Admin answering a fixed primary key and recording the CDC calls in order. */
    private static class KeyedAdmin extends BigQueryDefaultStreamWriterTest.NoopTableAdmin {
        private final List<PrimaryKeyColumn> key;
        private final List<String> calls = new ArrayList<>();

        KeyedAdmin(List<PrimaryKeyColumn> key) {
            this.key = key;
        }

        @Override
        public boolean ensureCdcTable(
                TableDestination destination,
                TableSchema schema,
                TableCreateOptionsProvider createOptionsProvider,
                CdcTableOptions cdcOptions,
                CreateDisposition createDisposition,
                CdcTableReconciliationPolicy reconciliationPolicy)
                throws IOException {
            calls.add("ensure");
            return super.ensureCdcTable(
                    destination,
                    schema,
                    createOptionsProvider,
                    cdcOptions,
                    createDisposition,
                    reconciliationPolicy);
        }

        @Override
        public List<PrimaryKeyColumn> getPrimaryKey(TableDestination destination) {
            calls.add("read");
            return key;
        }

        long reads() {
            return calls.stream().filter("read"::equals).count();
        }
    }

    private static BigQueryDefaultStreamSink<String> sink(
            KeyValueSerializer serializer,
            OrderingMode orderingMode,
            @Nullable CdcOptions<String> cdcOptions) {
        return sink(
                serializer,
                DefaultStreamOptions.builder().orderingMode(orderingMode).build(),
                cdcOptions);
    }

    private static BigQueryDefaultStreamSink<String> sink(
            KeyValueSerializer serializer,
            DefaultStreamOptions options,
            @Nullable CdcOptions<String> cdcOptions) {
        BigQuerySinkBuilder<String> builder =
                BigQuerySink.<String>builder()
                        .table(DESTINATION)
                        .serializer(serializer)
                        .failureHandler(FailureHandler.<BigQueryFailure>logAndDrop())
                        .defaultStreamOptions(options);
        if (cdcOptions != null) {
            builder.cdcOptions(cdcOptions)
                    .cdcTableOptions(
                            CdcTableOptions.builder()
                                    .primaryKeyColumns(Collections.singletonList("k"))
                                    .build());
        }
        return (BigQueryDefaultStreamSink<String>) builder.build();
    }

    /** Upserts whose sequences differ per record, as a sequence that never repeats does. */
    private static CdcOptions<String> upserts() {
        return CdcOptions.<String>builder(CdcChangeTypeProvider.upsertOnly())
                .sequenceNumberProvider(record -> Integer.toHexString(record.hashCode()))
                .build();
    }

    /** Writes the records through the sink's production path, then flushes. */
    private static List<List<String>> requests(
            OrderingMode orderingMode,
            @Nullable CdcOptions<String> cdcOptions,
            KeyedAdmin admin,
            String... records)
            throws Exception {
        return requests(new RecordingFactory(), orderingMode, cdcOptions, admin, records);
    }

    private static List<List<String>> requests(
            RecordingFactory factory,
            OrderingMode orderingMode,
            @Nullable CdcOptions<String> cdcOptions,
            KeyedAdmin admin,
            String... records)
            throws Exception {
        SinkWriter<String> writer =
                sink(new KeyValueSerializer(), orderingMode, cdcOptions)
                        .createWriter(factory, admin, TestSinkWriterMetricGroup.create());
        for (String record : records) {
            writer.write(record, CONTEXT);
        }
        writer.flush(false);
        return factory.requests;
    }

    @Test
    void aRepeatedKeyStartsANewRequestUnderPerTable() throws Exception {
        // b:1 and a:2 are in different requests, so b:2 only has to stay out of the one holding
        // b:1.
        String[] records = {"a:1", "b:1", "a:2", "c:1", "b:2", "a:3"};

        assertThat(requests(OrderingMode.NONE, upserts(), new KeyedAdmin(STRING_KEY), records))
                .containsExactly(List.of("a:1", "b:1", "a:2", "c:1", "b:2", "a:3"));

        KeyedAdmin admin = new KeyedAdmin(STRING_KEY);
        assertThat(requests(OrderingMode.PER_TABLE, upserts(), admin, records))
                .containsExactly(
                        List.of("a:1", "b:1"), List.of("a:2", "c:1", "b:2"), List.of("a:3"));
        // The key is read once, after the table contract has made sure the table exists.
        assertThat(admin.calls).containsExactly("ensure", "read");
    }

    /**
     * Serializer writing a record {@code "key:value"} to the columns {@code user-id} and {@code v}.
     * The first is not a valid proto field name, so the descriptor writes it under a placeholder
     * field that carries the name in its {@code column_name} annotation.
     */
    private static class FlexibleNameSerializer extends BigQueryProtoSerializationSchema<String> {
        private static final long serialVersionUID = 1L;

        private static final TableSchema SCHEMA =
                TableSchema.newBuilder()
                        .addFields(KeyValueSerializer.column("user-id"))
                        .addFields(KeyValueSerializer.column("v"))
                        .build();

        private static final Descriptors.Descriptor DESCRIPTOR = descriptor();

        private static Descriptors.Descriptor descriptor() {
            try {
                return BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(SCHEMA);
            } catch (Descriptors.DescriptorValidationException e) {
                throw new AssertionError(e);
            }
        }

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
            String[] parts = element.split(":", 2);
            // The key's placeholder field is the first, its value the second.
            return DynamicMessage.newBuilder(DESCRIPTOR)
                    .setField(DESCRIPTOR.getFields().get(0), parts[0])
                    .setField(DESCRIPTOR.findFieldByName("v"), parts[1])
                    .build()
                    .toByteString();
        }
    }

    private static List<Integer> requestSizesForAFlexibleKeyName(OrderingMode mode)
            throws Exception {
        RecordingFactory factory = new RecordingFactory();
        SinkWriter<String> writer =
                ((BigQueryDefaultStreamSink<String>)
                                BigQuerySink.<String>builder()
                                        .table(DESTINATION)
                                        .serializer(new FlexibleNameSerializer())
                                        .defaultStreamOptions(
                                                DefaultStreamOptions.builder()
                                                        .orderingMode(mode)
                                                        .build())
                                        .cdcOptions(upserts())
                                        .cdcTableOptions(
                                                CdcTableOptions.builder()
                                                        .primaryKeyColumns(
                                                                Collections.singletonList(
                                                                        "user-id"))
                                                        .build())
                                        .build())
                        .createWriter(
                                factory,
                                new KeyedAdmin(
                                        Collections.singletonList(
                                                new PrimaryKeyColumn(
                                                        "user-id",
                                                        StandardSQLTypeName.STRING,
                                                        null))),
                                TestSinkWriterMetricGroup.create());
        for (String record : new String[] {"a:1", "b:1", "a:2"}) {
            writer.write(record, CONTEXT);
        }
        writer.flush(false);
        List<Integer> sizes = new ArrayList<>();
        factory.requests.forEach(request -> sizes.add(request.size()));
        return sizes;
    }

    @Test
    void aKeyColumnWithAPlaceholderFieldNameIsFoundByItsColumnName() throws Exception {
        // Read by the placeholder's own name, the key would be absent in every row, and every
        // row would be its own request: 1, 1, 1.
        assertThat(requestSizesForAFlexibleKeyName(OrderingMode.NONE)).containsExactly(3);
        assertThat(requestSizesForAFlexibleKeyName(OrderingMode.PER_TABLE)).containsExactly(2, 1);
    }

    @Test
    void aTiedSequenceStillStartsANewRequestUnderPerTable() throws Exception {
        // Equal sequences fall back to ingestion order, so under per-table the split keeps them
        // in separate requests, which commit in arrival order (ADR-0193).
        String[] records = {"a:1", "a:2", "b:1"};

        assertThat(requests(OrderingMode.NONE, tiedSequence(), new KeyedAdmin(STRING_KEY), records))
                .containsExactly(List.of("a:1", "a:2", "b:1"));

        KeyedAdmin admin = new KeyedAdmin(STRING_KEY);
        assertThat(requests(OrderingMode.PER_TABLE, tiedSequence(), admin, records))
                .containsExactly(List.of("a:1"), List.of("a:2", "b:1"));
        assertThat(admin.reads()).isEqualTo(1);
    }

    private static CdcOptions<String> tiedSequence() {
        return CdcOptions.<String>builder(CdcChangeTypeProvider.upsertOnly())
                .sequenceNumberProvider(record -> "1")
                .build();
    }

    @Test
    void anAppendOnlyTableIsNotSplitByAnyColumn() throws Exception {
        KeyedAdmin admin = new KeyedAdmin(STRING_KEY);

        assertThat(requests(OrderingMode.PER_TABLE, null, admin, "a:1", "a:2", "a:3"))
                .containsExactly(List.of("a:1", "a:2", "a:3"));
        assertThat(admin.reads()).isZero();
    }

    @Test
    void aKeyWhoseBytesNameNoValueSendsEveryRowAlone() throws Exception {
        // A TIMESTAMP written as text has several spellings of one instant, so no two rows can be
        // told apart and every row is its own request.
        List<PrimaryKeyColumn> textTimestamp =
                Collections.singletonList(
                        new PrimaryKeyColumn("k", StandardSQLTypeName.TIMESTAMP, null));

        assertThat(
                        requests(
                                OrderingMode.NONE,
                                upserts(),
                                new KeyedAdmin(textTimestamp),
                                "a:1",
                                "b:1",
                                "c:1"))
                .containsExactly(List.of("a:1", "b:1", "c:1"));

        assertThat(
                        requests(
                                OrderingMode.PER_TABLE,
                                upserts(),
                                new KeyedAdmin(textTimestamp),
                                "a:1",
                                "b:1",
                                "c:1"))
                .containsExactly(List.of("a:1"), List.of("b:1"), List.of("c:1"));
    }

    @Test
    void aRowWhoseKeyCannotBeReadSharesARequestWithNoOtherRow() throws Exception {
        // Its key matches every key, so it ends the request before it and the one after it.
        String[] records = {"a:1", UNPARSEABLE, "b:1", "c:1"};

        assertThat(requests(OrderingMode.NONE, upserts(), new KeyedAdmin(STRING_KEY), records))
                .containsExactly(List.of("a:1", UNPARSEABLE, "b:1", "c:1"));

        assertThat(requests(OrderingMode.PER_TABLE, upserts(), new KeyedAdmin(STRING_KEY), records))
                .containsExactly(List.of("a:1"), List.of(UNPARSEABLE), List.of("b:1", "c:1"));
    }

    @Test
    void theKeysOfPendingRowsSurviveARebuild() throws Exception {
        // A schema refresh rebuilds the state and moves the pending row a:1 to it. Its key must
        // move with it, or a:2 joins the request; and the rebuilt state must still read keys, or
        // b:1 is split off too.
        assertThat(writeAcrossASchemaRefresh(OrderingMode.NONE))
                .containsExactly(List.of("a:1", "b:1", "a:2"));
        assertThat(writeAcrossASchemaRefresh(OrderingMode.PER_TABLE))
                .containsExactly(List.of("a:1", "b:1"), List.of("a:2"));
    }

    private static List<List<String>> writeAcrossASchemaRefresh(OrderingMode mode)
            throws Exception {
        KeyValueSerializer serializer = new KeyValueSerializer();
        RecordingFactory factory = new RecordingFactory();
        SinkWriter<String> writer =
                sink(serializer, mode, upserts())
                        .createWriter(
                                factory,
                                new KeyedAdmin(STRING_KEY),
                                TestSinkWriterMetricGroup.create());

        writer.write("a:1", CONTEXT);
        serializer.version++;
        writer.write("b:1", CONTEXT);
        writer.write("a:2", CONTEXT);
        writer.flush(false);
        return factory.requests;
    }

    @Test
    void aRepairKeepsTheRequestsTheKeysWereSplitInto() throws Exception {
        // [a:1, b:1] fails once and is re-sent on a rebuilt stream writer, which takes over the
        // pending a:2 with its key; nothing may merge the re-sent rows with the pending ones.
        String[] records = {"a:1", "b:1", "a:2", "c:1", "a:3"};

        assertThat(
                        requests(
                                new RecordingFactory().failingOnce("a:1"),
                                OrderingMode.NONE,
                                upserts(),
                                new KeyedAdmin(STRING_KEY),
                                records))
                .containsExactly(
                        List.of("a:1", "b:1", "a:2", "c:1", "a:3"),
                        List.of("a:1", "b:1", "a:2", "c:1", "a:3"));

        assertThat(
                        requests(
                                new RecordingFactory().failingOnce("a:1"),
                                OrderingMode.PER_TABLE,
                                upserts(),
                                new KeyedAdmin(STRING_KEY),
                                records))
                .containsExactly(
                        List.of("a:1", "b:1"),
                        List.of("a:1", "b:1"),
                        List.of("a:2", "c:1"),
                        List.of("a:3"));
    }

    @Test
    void anUnreadableKeyIsReportedOnceWhenTheTableOpens() throws Exception {
        List<PrimaryKeyColumn> textTimestamp =
                Collections.singletonList(
                        new PrimaryKeyColumn("k", StandardSQLTypeName.TIMESTAMP, null));
        KeyValueSerializer serializer = new KeyValueSerializer();

        try (LogCapture capture = LogCapture.of(BigQueryDefaultStreamWriter.class)) {
            SinkWriter<String> writer =
                    sink(serializer, OrderingMode.PER_TABLE, upserts())
                            .createWriter(
                                    new RecordingFactory(),
                                    new KeyedAdmin(textTimestamp),
                                    TestSinkWriterMetricGroup.create());
            writer.write("a:1", CONTEXT);
            // A rebuild that leaves the reason unchanged does not repeat the warning.
            serializer.version++;
            writer.write("b:1", CONTEXT);
            writer.flush(false);

            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains("p.d.t")
                    .contains("per-table")
                    .contains("k TIMESTAMP");
        }

        try (LogCapture capture = LogCapture.of(BigQueryDefaultStreamWriter.class)) {
            requests(OrderingMode.PER_TABLE, upserts(), new KeyedAdmin(STRING_KEY), "a:1");

            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void anEvictedTableReadsItsPrimaryKeyAgainWhenItReturns() throws Exception {
        KeyedAdmin admin = new KeyedAdmin(STRING_KEY);
        SinkWriter<String> writer =
                sink(
                                new KeyValueSerializer(),
                                DefaultStreamOptions.builder()
                                        .orderingMode(OrderingMode.PER_TABLE)
                                        .destinationIdleTimeout(Duration.ofNanos(1))
                                        .build(),
                                upserts())
                        .createWriter(
                                new RecordingFactory(), admin, TestSinkWriterMetricGroup.create());

        writer.write("a:1", CONTEXT);
        writer.flush(false); // evicts the table, idle for longer than a nanosecond
        writer.write("a:2", CONTEXT);
        writer.flush(false);

        assertThat(admin.calls).containsExactly("ensure", "read", "ensure", "read");
    }

    @Test
    void theRequestByteCapStillSplitsBetweenDistinctKeys() throws Exception {
        // Each record serializes to more bytes than the whole cap, so each is a request of its own
        // whatever its key; the key split must leave that check in place.
        RecordingFactory factory = new RecordingFactory();
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new KeyValueSerializer())
                                .defaultStreamOptions(
                                        DefaultStreamOptions.builder()
                                                .orderingMode(OrderingMode.PER_TABLE)
                                                .maxAppendRequestBytes(8)
                                                .build())
                                .cdcOptions(upserts())
                                .cdcTableOptions(
                                        CdcTableOptions.builder()
                                                .primaryKeyColumns(Collections.singletonList("k"))
                                                .build())
                                .build();
        SinkWriter<String> writer =
                sink.createWriter(
                        factory, new KeyedAdmin(STRING_KEY), TestSinkWriterMetricGroup.create());

        writer.write("a:1", CONTEXT);
        writer.write("b:1", CONTEXT);
        writer.flush(false);

        assertThat(factory.requests).containsExactly(List.of("a:1"), List.of("b:1"));
    }
}
