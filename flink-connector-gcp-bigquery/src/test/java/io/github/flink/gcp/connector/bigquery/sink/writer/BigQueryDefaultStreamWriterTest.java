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

package io.github.flink.gcp.connector.bigquery.sink.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import com.google.rpc.Status;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigQueryDefaultStreamWriter}. */
class BigQueryDefaultStreamWriterTest {

    static final TableAdmin NOOP_ADMIN = new NoopTableAdmin();

    /** Admin whose tables always "exist" implicitly: creation is a no-op, reads find nothing. */
    static class NoopTableAdmin implements TableAdmin {
        @Override
        public void create(
                TableDestination destination,
                TableSchema schema,
                io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions options) {}

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

    private static class FakeAppenderFactory implements RowAppenderFactory {
        private static final long serialVersionUID = 1L;

        private final Map<TableDestination, FakeAppender> appenders = new LinkedHashMap<>();

        /** Shared script: consumed globally in append order across all appenders. */
        private final List<ApiFuture<AppendRowsResponse>> scriptedResults = new ArrayList<>();

        @Override
        public RowAppender create(
                TableDestination destination,
                Descriptors.Descriptor rowDescriptor,
                String location) {
            FakeAppender appender = new FakeAppender();
            appenders.put(destination, appender);
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
                        NOOP_ADMIN);

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
    void appendsWhenBatchSizeThresholdIsReached() throws Exception {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(
                                (element, context) -> TableDestination.of("p", "d", "t"),
                                new StringSerializer()),
                        factory,
                        NOOP_ADMIN,
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

    @Test
    void rejectsOversizedRows() {
        FakeAppenderFactory factory = new FakeAppenderFactory();
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(
                                (element, context) -> TableDestination.of("p", "d", "t"),
                                new OversizedSerializer()),
                        factory,
                        NOOP_ADMIN);

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
                        NOOP_ADMIN);

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
                        NOOP_ADMIN);

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
                        NOOP_ADMIN);

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
                        NOOP_ADMIN);

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
                        NOOP_ADMIN);

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

        SinkWriter<String> writer = sink.createWriter(factory, NOOP_ADMIN);
        writer.write("row", CONTEXT);
        writer.flush(false);
        writer.close();

        assertThat(writer).isInstanceOf(BigQueryDefaultStreamWriter.class);
        FakeAppenderFactory.FakeAppender appender =
                factory.appenders.get(TableDestination.of("p", "d", "t"));
        assertThat(rowsOf(appender.appends.get(0))).containsExactly("row");
    }
}
