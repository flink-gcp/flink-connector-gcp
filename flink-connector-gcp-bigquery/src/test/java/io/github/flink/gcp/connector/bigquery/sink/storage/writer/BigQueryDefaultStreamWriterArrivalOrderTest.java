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

import com.google.api.core.AbstractApiFuture;
import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigquery.sink.OrderingMode;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.failure.BigQueryFailure;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the order in which {@link BigQueryDefaultStreamWriter} commits a table's rows, with and
 * without {@link OrderingMode#PER_TABLE}.
 *
 * <p>By default the fake service answers an append when it is awaited, or, failing that, when the
 * next append to the same table is sent: a service that is slower to answer than the writer is to
 * send. Rows land in the order the successful answers happen, and the rows of one answer in their
 * order within the request, which the service does not promise; the mode orders requests, so only
 * the row-level test puts more than one row in a request. Each test that asserts the order under
 * {@code PER_TABLE} against that service also asserts that {@code NONE} lands the same input out of
 * order, so the test can fail.
 */
class BigQueryDefaultStreamWriterArrivalOrderTest {

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    private static final StatusRuntimeException UNAVAILABLE =
            new StatusRuntimeException(Status.UNAVAILABLE);

    private static final StatusRuntimeException INVALID_ARGUMENT =
            new StatusRuntimeException(Status.INVALID_ARGUMENT);

    /** Serializer writing the record string bytes, with a schema fingerprint a test can advance. */
    private static class StringSerializer extends BigQueryProtoSerializationSchema<String> {
        private static final long serialVersionUID = 1L;

        private int version;

        @Override
        public ByteString serialize(String element) {
            return ByteString.copyFromUtf8(element);
        }

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("f")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.NULLABLE)
                                    .build())
                    .build();
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return Empty.getDescriptor();
        }

        @Override
        public Object getSchemaFingerprint(TableDestination destination) {
            return version;
        }
    }

    /** When the fake service answers an append. */
    private enum Answering {
        /** When awaited, or when the next append to the same table is sent. */
        LATE,
        /** Only when awaited, as a service slower still would. */
        WHEN_AWAITED,
        /** Before the append call returns, as a service faster than the next record would. */
        AT_ONCE
    }

    /**
     * Appender factory over the fake service. Failures are scripted per batch, by the batch's first
     * row: each attempt to append such a batch takes the next scripted failure, and an attempt with
     * none left succeeds.
     */
    private static class FakeServiceFactory implements RowAppenderFactory {
        private static final long serialVersionUID = 1L;

        private final Answering answering;
        private final Map<String, Deque<Throwable>> failures = new HashMap<>();
        private final List<String> sent = new ArrayList<>();
        private final List<String> landed = new ArrayList<>();
        private final Map<TableDestination, List<String>> landedByTable = new HashMap<>();

        FakeServiceFactory(Answering answering) {
            this.answering = answering;
        }

        FakeServiceFactory failing(String firstRow, Throwable... attempts) {
            failures.put(firstRow, new ArrayDeque<>(Arrays.asList(attempts)));
            return this;
        }

        @Override
        public RowAppender create(
                TableDestination destination,
                Descriptors.Descriptor rowDescriptor,
                String location) {
            return new RowAppender() {
                @Nullable private Answer unanswered;

                @Override
                public ApiFuture<AppendRowsResponse> append(ProtoRows rows) {
                    if (unanswered != null && answering == Answering.LATE) {
                        unanswered.answer();
                    }
                    String firstRow = rows.getSerializedRows(0).toStringUtf8();
                    rows.getSerializedRowsList().forEach(row -> sent.add(row.toStringUtf8()));
                    Deque<Throwable> scripted = failures.get(firstRow);
                    Throwable failure =
                            scripted == null || scripted.isEmpty() ? null : scripted.poll();
                    Answer answer = new Answer(destination, rows, failure);
                    unanswered = answer;
                    if (answering == Answering.AT_ONCE) {
                        answer.answer();
                    }
                    return answer;
                }

                @Override
                public void close() {}
            };
        }

        /** An append's future, completed with its scripted outcome on first demand. */
        private class Answer extends AbstractApiFuture<AppendRowsResponse> {
            private final TableDestination destination;
            private final ProtoRows rows;
            @Nullable private final Throwable failure;
            private boolean answered;

            private Answer(
                    TableDestination destination, ProtoRows rows, @Nullable Throwable failure) {
                this.destination = destination;
                this.rows = rows;
                this.failure = failure;
            }

            private void answer() {
                if (answered) {
                    return;
                }
                answered = true;
                if (failure != null) {
                    setException(failure);
                    return;
                }
                for (ByteString row : rows.getSerializedRowsList()) {
                    landed.add(row.toStringUtf8());
                    landedByTable
                            .computeIfAbsent(destination, d -> new ArrayList<>())
                            .add(row.toStringUtf8());
                }
                set(AppendRowsResponse.getDefaultInstance());
            }

            @Override
            public AppendRowsResponse get() throws InterruptedException, ExecutionException {
                answer();
                return super.get();
            }

            @Override
            public AppendRowsResponse get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                answer();
                return super.get(timeout, unit);
            }
        }
    }

    private static BigQuerySinkConfig<String> config(
            StringSerializer serializer, @Nullable DestinationResolver<String> resolver) {
        return sink(serializer, resolver, null).getConfig();
    }

    private static BigQueryDefaultStreamSink<String> sink(
            StringSerializer serializer,
            @Nullable DestinationResolver<String> resolver,
            @Nullable DefaultStreamOptions options) {
        BigQuerySinkBuilder<String> builder =
                BigQuerySink.<String>builder()
                        .serializer(serializer)
                        .failureHandler(FailureHandler.<BigQueryFailure>logAndDrop());
        if (resolver == null) {
            builder.table(DESTINATION);
        } else {
            builder.destinationResolver(resolver);
        }
        if (options != null) {
            builder.defaultStreamOptions(options);
        }
        return (BigQueryDefaultStreamSink<String>) builder.build();
    }

    private static BigQueryDefaultStreamWriter<String> writer(
            FakeServiceFactory factory,
            OrderingMode orderingMode,
            long maxAppendRequestBytes,
            BigQuerySinkConfig<String> config) {
        return new BigQueryDefaultStreamWriter<>(
                config,
                factory,
                BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                new DefaultStreamWriterMetrics(TestSinkWriterMetricGroup.create(), false),
                maxAppendRequestBytes,
                BigQueryDefaultStreamWriterTest.fastSchedule(3),
                BigQueryDefaultStreamWriterTest.fastSchedule(3),
                DefaultStreamOptions.DEFAULT_DESTINATION_IDLE_TIMEOUT,
                null,
                null,
                System::nanoTime,
                orderingMode);
    }

    /** Writes the records with a one-byte cap, so each record is its own append, then flushes. */
    private static List<String> writeOneRowPerAppend(
            FakeServiceFactory factory, OrderingMode orderingMode, String... records)
            throws IOException {
        BigQueryDefaultStreamWriter<String> writer =
                writer(factory, orderingMode, 1, config(new StringSerializer(), null));
        for (String record : records) {
            writer.write(record, CONTEXT);
        }
        writer.flush(false);
        return factory.landed;
    }

    private static FakeServiceFactory late() {
        return new FakeServiceFactory(Answering.LATE);
    }

    @Test
    void aFailedAppendIsReSentBeforeTheNextOneUnderPerTable() throws Exception {
        assertThat(
                        writeOneRowPerAppend(
                                late().failing("a", UNAVAILABLE), OrderingMode.NONE, "a", "b", "c"))
                .containsExactly("b", "c", "a");

        assertThat(
                        writeOneRowPerAppend(
                                late().failing("a", UNAVAILABLE),
                                OrderingMode.PER_TABLE,
                                "a",
                                "b",
                                "c"))
                .containsExactly("a", "b", "c");
    }

    @Test
    void aReSendThatFailsAgainIsRetriedBeforeTheNextAppendUnderPerTable() throws Exception {
        assertThat(
                        writeOneRowPerAppend(
                                late().failing("a", UNAVAILABLE, UNAVAILABLE),
                                OrderingMode.NONE,
                                "a",
                                "b",
                                "c"))
                .containsExactly("b", "c", "a");

        assertThat(
                        writeOneRowPerAppend(
                                late().failing("a", UNAVAILABLE, UNAVAILABLE),
                                OrderingMode.PER_TABLE,
                                "a",
                                "b",
                                "c"))
                .containsExactly("a", "b", "c");
    }

    @Test
    void theSurvivorsOfARowLevelFailureLandBeforeTheNextAppendUnderPerTable() throws Exception {
        Throwable rowError =
                new Exceptions.AppendSerializtionError(
                        Status.Code.INVALID_ARGUMENT.value(),
                        "bad rows",
                        "stream",
                        Map.of(1, "a2 is broken"));
        // A four-byte cap holds two two-byte records per append.
        String[] records = {"a1", "a2", "b1", "b2", "c1"};

        FakeServiceFactory none = late().failing("a1", rowError);
        BigQueryDefaultStreamWriter<String> unordered =
                writer(none, OrderingMode.NONE, 4, config(new StringSerializer(), null));
        for (String record : records) {
            unordered.write(record, CONTEXT);
        }
        unordered.flush(false);
        assertThat(none.landed).containsExactly("b1", "b2", "c1", "a1");

        FakeServiceFactory perTable = late().failing("a1", rowError);
        BigQueryDefaultStreamWriter<String> ordered =
                writer(perTable, OrderingMode.PER_TABLE, 4, config(new StringSerializer(), null));
        for (String record : records) {
            ordered.write(record, CONTEXT);
        }
        ordered.flush(false);
        assertThat(perTable.landed).containsExactly("a1", "b1", "b2", "c1");
    }

    @Test
    void manyFailedAppendsAreReSentInArrivalOrderUnderPerTable() throws Exception {
        String[] records = new String[21];
        for (int i = 0; i < records.length; i++) {
            records[i] = String.format("r%02d", i);
        }

        // Without the mode, every batch is in flight when the flush sends the last one, and the
        // repair re-sends the failed ones in the order of a hash map, behind that last batch.
        FakeServiceFactory none = new FakeServiceFactory(Answering.WHEN_AWAITED);
        for (int i = 0; i < records.length - 1; i++) {
            none.failing(records[i], UNAVAILABLE);
        }
        assertThat(writeOneRowPerAppend(none, OrderingMode.NONE, records))
                .containsExactlyInAnyOrder(records)
                .startsWith("r20");

        // Under the mode each batch is sent only once the previous one has landed, so every first
        // attempt fails on its own and is re-sent before the next batch goes out.
        FakeServiceFactory perTable = new FakeServiceFactory(Answering.WHEN_AWAITED);
        for (String record : records) {
            perTable.failing(record, UNAVAILABLE);
        }
        assertThat(writeOneRowPerAppend(perTable, OrderingMode.PER_TABLE, records))
                .containsExactly(records);
    }

    @Test
    void answersThatArriveBeforeTheNextRecordKeepArrivalOrderUnderPerTable() throws Exception {
        // The failure is already known when the next record arrives, so the sweep at the start of
        // write() repairs it and the await finds a rebuilt state with nothing to wait for. This
        // path keeps the order without the mode too; the test holds that the mode does not break
        // it.
        assertThat(
                        writeOneRowPerAppend(
                                new FakeServiceFactory(Answering.AT_ONCE).failing("a", UNAVAILABLE),
                                OrderingMode.PER_TABLE,
                                "a",
                                "b",
                                "c"))
                .containsExactly("a", "b", "c");
    }

    @Test
    void aTerminalFailureStopsTheNextAppendUnderPerTable() throws Exception {
        FakeServiceFactory factory = late().failing("a", INVALID_ARGUMENT);
        BigQueryDefaultStreamWriter<String> writer =
                writer(factory, OrderingMode.PER_TABLE, 1, config(new StringSerializer(), null));

        writer.write("a", CONTEXT);
        writer.write("b", CONTEXT); // appends [a]

        assertThatThrownBy(() -> writer.write("c", CONTEXT)) // would append [b]
                .isInstanceOf(IOException.class)
                .hasMessageContaining("An append to BigQuery table p.d.t failed");
        assertThat(factory.sent).containsExactly("a");
        assertThat(factory.landed).isEmpty();
    }

    @Test
    void anExhaustedRepairStopsTheNextAppendUnderPerTable() throws Exception {
        // fastSchedule(3) allows three attempts in all; the fourth failure is never reached.
        FakeServiceFactory factory =
                late().failing("a", UNAVAILABLE, UNAVAILABLE, UNAVAILABLE, UNAVAILABLE);
        BigQueryDefaultStreamWriter<String> writer =
                writer(factory, OrderingMode.PER_TABLE, 1, config(new StringSerializer(), null));

        writer.write("a", CONTEXT);
        writer.write("b", CONTEXT); // appends [a]

        assertThatThrownBy(() -> writer.write("c", CONTEXT)) // would append [b]
                .isInstanceOf(IOException.class);
        assertThat(factory.sent).doesNotContain("b");
        assertThat(factory.landed).isEmpty();
    }

    @Test
    void aTerminalFailureSeenByASchemaRefreshStopsTheNextAppendUnderPerTable() throws Exception {
        // The refresh awaits [a] and rebuilds the state, and the failure's completion callback
        // takes the batch first, so the refresh sees nothing to repair and the rebuilt state has no
        // previous append. The await before [b] must still surface the captured failure.
        FakeServiceFactory factory = late().failing("a", INVALID_ARGUMENT);
        StringSerializer serializer = new StringSerializer();
        BigQueryDefaultStreamWriter<String> writer =
                writer(factory, OrderingMode.PER_TABLE, 1, config(serializer, null));

        writer.write("a", CONTEXT);
        writer.write("b", CONTEXT); // appends [a]
        serializer.version++;

        assertThatThrownBy(() -> writer.write("c", CONTEXT)) // refreshes, then would append [b]
                .isInstanceOf(IOException.class)
                .hasMessageContaining("An append to BigQuery table p.d.t failed");
        assertThat(factory.sent).containsExactly("a");
    }

    @Test
    void perTableKeepsTheAppendsOfDifferentTablesInFlightTogether() throws Exception {
        FakeServiceFactory factory = late().failing("a2", UNAVAILABLE);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        factory,
                        OrderingMode.PER_TABLE,
                        1,
                        config(
                                new StringSerializer(),
                                (element, context) ->
                                        TableDestination.of("p", "d", element.substring(0, 1))));

        writer.write("a1", CONTEXT);
        writer.write("b1", CONTEXT);
        writer.write("a2", CONTEXT); // appends [a1]
        writer.write("b2", CONTEXT); // appends [b1], with [a1] still unanswered

        assertThat(factory.sent).containsExactly("a1", "b1");
        assertThat(factory.landed).isEmpty();

        writer.write("a3", CONTEXT); // awaits [a1], appends [a2], which will fail
        writer.write("b3", CONTEXT); // awaits [b1], appends [b2]
        // The flush awaits [a2] while it walks both tables, repairs it, and only then sends [a3].
        writer.flush(false);
        assertThat(factory.landedByTable.get(TableDestination.of("p", "d", "a")))
                .containsExactly("a1", "a2", "a3");
        assertThat(factory.landedByTable.get(TableDestination.of("p", "d", "b")))
                .containsExactly("b1", "b2", "b3");
    }

    @Test
    void theOptionsReachTheWriterThroughTheSink() throws Exception {
        // The tests above construct the writer with the mode; this one takes the production path,
        // from DefaultStreamOptions through the sink to the writer.
        FakeServiceFactory factory = late().failing("a", UNAVAILABLE);
        SinkWriter<String> writer =
                sink(
                                new StringSerializer(),
                                null,
                                DefaultStreamOptions.builder()
                                        .maxAppendRequestBytes(1)
                                        .recoveryInitialBackoff(Duration.ofMillis(1))
                                        .recoveryMaxBackoff(Duration.ofMillis(1))
                                        .orderingMode(OrderingMode.PER_TABLE)
                                        .build())
                        .createWriter(
                                factory,
                                BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                                TestSinkWriterMetricGroup.create());

        writer.write("a", CONTEXT);
        writer.write("b", CONTEXT);
        writer.write("c", CONTEXT);
        writer.flush(false);

        assertThat(factory.landed).containsExactly("a", "b", "c");
    }
}
