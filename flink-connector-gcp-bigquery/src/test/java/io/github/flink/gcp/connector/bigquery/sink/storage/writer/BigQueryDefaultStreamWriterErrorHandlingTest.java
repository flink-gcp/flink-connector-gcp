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
import org.apache.flink.util.ExceptionUtils;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.failure.BigQueryFailure;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the error classification, retry and {@link FailureHandler} routing behavior of {@link
 * BigQueryDefaultStreamWriter}. Table auto-creation ({@code NOT_FOUND}) recovery is covered by
 * {@link BigQueryDefaultStreamWriterAutoCreationTest}.
 */
class BigQueryDefaultStreamWriterErrorHandlingTest {

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    private static ApiFuture<AppendRowsResponse> failedWith(Status status) {
        return ApiFutures.immediateFailedFuture(new StatusRuntimeException(status));
    }

    private static ApiFuture<AppendRowsResponse> rowLevelError(Map<Integer, String> rowErrors) {
        return ApiFutures.immediateFailedFuture(
                new Exceptions.AppendSerializtionError(
                        Status.Code.INVALID_ARGUMENT.value(), "bad rows", "stream", rowErrors));
    }

    /** Serializer writing the record string bytes; records starting with "unserializable" fail. */
    private static class StringSerializer extends BigQueryProtoSerializerStub {
        private static final long serialVersionUID = 1L;

        @Override
        public ByteString serialize(String element) throws IOException {
            if (element.startsWith("unserializable")) {
                throw new IOException("cannot serialize " + element);
            }
            return ByteString.copyFromUtf8(element);
        }
    }

    /** Serializer emitting one oversized row. */
    private static class OversizedSerializer extends BigQueryProtoSerializerStub {
        private static final long serialVersionUID = 1L;

        @Override
        public ByteString serialize(String element) {
            return ByteString.copyFrom(new byte[BigQueryDefaultStreamWriter.MAX_ROW_BYTES + 1]);
        }
    }

    /** Serializer failing with an unchecked exception for every record. */
    private static class UncheckedFailingSerializer extends BigQueryProtoSerializerStub {
        private static final long serialVersionUID = 1L;

        @Override
        public ByteString serialize(String element) {
            throw new IllegalStateException("unchecked serializer failure");
        }
    }

    private abstract static class BigQueryProtoSerializerStub
            extends io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer<
                    String> {
        private static final long serialVersionUID = 1L;

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
    }

    /** Handler recording every routed row and dropping it. */
    private static class RecordingFailedRowHandler implements FailureHandler<BigQueryFailure> {
        private static final long serialVersionUID = 1L;

        private final transient List<BigQueryFailure> rows = new ArrayList<>();

        /** "handle"/"flush" in invocation order, pinning that flush runs after the drain. */
        private final transient List<String> events = new ArrayList<>();

        private transient boolean closed;

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

    /**
     * Appender factory with a global append-result script (consumed across all appenders in append
     * order; empty script means success).
     */
    private static class ScriptedAppenderFactory implements RowAppenderFactory {
        private static final long serialVersionUID = 1L;

        private final List<FakeAppender> created = new ArrayList<>();
        private final Deque<ApiFuture<AppendRowsResponse>> scriptedResults = new ArrayDeque<>();

        /**
         * When set, every appender throws it on close. Typed {@code Throwable} so a test can script
         * an {@code Error}, which is thrown as itself.
         */
        private Throwable closeFailure;

        @Override
        public RowAppender create(
                TableDestination destination,
                Descriptors.Descriptor rowDescriptor,
                String location) {
            FakeAppender appender = new FakeAppender();
            created.add(appender);
            return appender;
        }

        private List<String> allAppendedRows() {
            List<String> rows = new ArrayList<>();
            for (FakeAppender appender : created) {
                for (ProtoRows batch : appender.appends) {
                    batch.getSerializedRowsList().forEach(b -> rows.add(b.toStringUtf8()));
                }
            }
            return rows;
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
                return scriptedResults.poll();
            }

            @Override
            public void close() {
                closed = true;
                if (closeFailure != null) {
                    ExceptionUtils.rethrow(closeFailure);
                }
            }
        }
    }

    private static BigQuerySinkConfig<String> config(
            io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer<
                            ? super String>
                    serializer,
            FailureHandler<BigQueryFailure> failureHandler) {
        return ((BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(DESTINATION)
                                .serializer(serializer)
                                .failureHandler(failureHandler)
                                .build())
                .getConfig();
    }

    private static BigQueryDefaultStreamWriter<String> writer(
            BigQuerySinkConfig<String> config,
            ScriptedAppenderFactory factory,
            long maxAppendRequestBytes,
            int recoveryMaxAttempts) {
        return new BigQueryDefaultStreamWriter<>(
                config,
                factory,
                BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                TestSinkWriterMetricGroup.create(),
                maxAppendRequestBytes,
                BigQueryDefaultStreamWriterTest.fastSchedule(recoveryMaxAttempts),
                BigQueryDefaultStreamWriterTest.fastSchedule(recoveryMaxAttempts));
    }

    private static List<String> rowsOf(ProtoRows rows) {
        List<String> values = new ArrayList<>();
        rows.getSerializedRowsList().forEach(b -> values.add(b.toStringUtf8()));
        return values;
    }

    // --- terminal errors: fail the write or checkpoint, no retry ---

    @Test
    void terminalFailureFailsTheFlush() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(failedWith(Status.INVALID_ARGUMENT));
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), FailureHandler.failJob()),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("An append to BigQuery table p.d.t failed")
                .hasCauseInstanceOf(StatusRuntimeException.class);
        // No writer-side retry for terminal failures.
        assertThat(factory.created).hasSize(1);
        assertThat(factory.created.get(0).appends).hasSize(1);
    }

    @Test
    void terminalAsyncFailureFailsTheNextWrite() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        SettableApiFuture<AppendRowsResponse> pending = SettableApiFuture.create();
        factory.scriptedResults.add(pending);
        BigQueryDefaultStreamWriter<String> writer =
                writer(config(new StringSerializer(), FailureHandler.failJob()), factory, 1, 3);

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT); // triggers the async append of [aa]
        // INVALID_ARGUMENT rather than PERMISSION_DENIED, which used to be the example here: the
        // service masks a missing table behind PERMISSION_DENIED, so under the default
        // CREATE_IF_NEEDED that code now routes to table creation instead of being terminal (see
        // AppendErrorClassifier#isMissingTable). An INVALID_ARGUMENT naming no rows still is.
        pending.setException(new StatusRuntimeException(Status.INVALID_ARGUMENT));

        assertThatThrownBy(() -> writer.write("cc", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("An append to BigQuery table p.d.t failed");
    }

    // --- transient errors: retried by the writer, do not surface ---

    @Test
    void transientFailureIsRetriedAndDoesNotSurfaceInFlush() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(failedWith(Status.UNAVAILABLE));
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), FailureHandler.failJob()),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);

        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
        // The batch was re-appended verbatim on a rebuilt appender.
        assertThat(factory.created).hasSize(2);
        assertThat(factory.created.get(0).closed).isTrue();
        assertThat(rowsOf(factory.created.get(1).appends.get(0))).containsExactly("aa");
    }

    @Test
    void asyncTransientFailureIsRepairedOnTheNextWrite() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(failedWith(Status.ABORTED));
        BigQueryDefaultStreamWriter<String> writer =
                writer(config(new StringSerializer(), FailureHandler.failJob()), factory, 1, 3);

        writer.write("aa", CONTEXT); // buffered
        writer.write("bb", CONTEXT); // appends [aa], which fails asynchronously with ABORTED
        writer.write("cc", CONTEXT); // sweeps the failure and re-appends [aa]
        writer.flush(false);

        assertThat(factory.created.get(0).closed).isTrue();
        assertThat(factory.allAppendedRows()).containsExactly("aa", "aa", "bb", "cc");
    }

    @Test
    void transientRetryBudgetExhaustionIsTerminal() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(failedWith(Status.UNAVAILABLE)); // initial append
        factory.scriptedResults.add(failedWith(Status.UNAVAILABLE)); // retry attempt 1
        factory.scriptedResults.add(failedWith(Status.UNAVAILABLE)); // retry attempt 2
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), FailureHandler.failJob()),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        2);

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("retry budget is exhausted")
                .hasMessageContaining("2 attempt(s)");
    }

    @Test
    void transientFailureDuringTableCreationRecoveryKeepsRetrying() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(failedWith(Status.NOT_FOUND)); // initial append
        factory.scriptedResults.add(failedWith(Status.UNAVAILABLE)); // re-append after creation
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), FailureHandler.failJob()),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);

        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
        assertThat(factory.allAppendedRows()).containsExactly("aa", "aa", "aa");
    }

    // --- row-level errors: routed to the failed-row handler ---

    @Test
    void rowLevelFailureWithDefaultPolicyFailsTheFlush() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(rowLevelError(Map.of(1, "row 1 is broken")));
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), FailureHandler.failJob()),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("A record for bigquery destination p.d.t failed terminally")
                .hasMessageContaining("row 1 is broken");
    }

    @Test
    void aTransientCodedRowDetailedFailureIsRetriedWholeAndNeverRouted() throws Exception {
        // An outage-shaped failure must not become dead letters even when it arrives with row
        // details: the SDK stamps the response's own status code onto AppendSerializationError.
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(
                ApiFutures.immediateFailedFuture(
                        new Exceptions.AppendSerializtionError(
                                Status.Code.UNAVAILABLE.value(),
                                "backend unavailable",
                                "stream",
                                Map.of(0, "phantom row error"))));
        RecordingFailedRowHandler handler = new RecordingFailedRowHandler();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);
        writer.flush(false);

        assertThat(handler.rows).isEmpty();
        // The whole batch was re-appended on the rebuilt appender, nothing dropped.
        assertThat(rowsOf(factory.created.get(1).appends.get(0))).containsExactly("aa", "bb");
    }

    @Test
    void rowLevelFailureDropsOnlyTheFailedRowsAndReappendsTheRest() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(rowLevelError(Map.of(1, "row 1 is broken")));
        RecordingFailedRowHandler handler = new RecordingFailedRowHandler();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);
        writer.write("cc", CONTEXT);
        writer.flush(false);

        assertThat(handler.rows).hasSize(1);
        assertThat(handler.rows.get(0).describeDestination()).isEqualTo(DESTINATION.toString());
        assertThat(handler.rows.get(0).getPayloadBytes().toStringUtf8()).isEqualTo("bb");
        assertThat(handler.rows.get(0).getErrorMessage()).isEqualTo("row 1 is broken");
        // The surviving rows were re-appended on a rebuilt appender.
        assertThat(factory.created).hasSize(2);
        assertThat(rowsOf(factory.created.get(1).appends.get(0))).containsExactly("aa", "cc");
    }

    @Test
    void rowLevelFailureOfAllRowsReappendsNothing() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(rowLevelError(Map.of(0, "broken", 1, "also broken")));
        RecordingFailedRowHandler handler = new RecordingFailedRowHandler();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);
        writer.flush(false);

        assertThat(handler.rows).hasSize(2);
        assertThat(factory.created.get(1).appends).isEmpty();
    }

    @Test
    void responseEmbeddedRowErrorsAreRoutedToTheHandler() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(
                ApiFutures.immediateFuture(
                        AppendRowsResponse.newBuilder()
                                .addRowErrors(
                                        RowError.newBuilder()
                                                .setIndex(0)
                                                .setMessage("bad response row"))
                                .build()));
        RecordingFailedRowHandler handler = new RecordingFailedRowHandler();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);
        writer.flush(false);

        assertThat(handler.rows).hasSize(1);
        assertThat(handler.rows.get(0).getPayloadBytes().toStringUtf8()).isEqualTo("aa");
        assertThat(handler.rows.get(0).getErrorMessage()).isEqualTo("bad response row");
        assertThat(rowsOf(factory.created.get(1).appends.get(0))).containsExactly("bb");
    }

    @Test
    void rowLevelFailureThrowingCustomHandlerFailsTheFlush() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(rowLevelError(Map.of(0, "broken")));
        FailureHandler<BigQueryFailure> throwingHandler =
                row -> {
                    throw new IOException("handler rejected the row");
                };
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), throwingHandler),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessage("handler rejected the row");
    }

    // --- handler lifecycle: flush at every writer flush, after the drain ---

    @Test
    void handlerFlushRunsAtEveryWriterFlushAfterRoutedRowsAreHandled() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(rowLevelError(Map.of(0, "broken")));
        RecordingFailedRowHandler handler = new RecordingFailedRowHandler();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);
        writer.flush(false);
        writer.flush(true);

        // The routed row is handled before the first flush(), so a buffering handler has
        // everything when it persists; end of input flushes the handler too.
        assertThat(handler.events).containsExactly("handle", "flush", "flush");
    }

    @Test
    void handlerFlushFailureFailsTheFlush() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        FailureHandler<BigQueryFailure> unflushableHandler =
                new FailureHandler<BigQueryFailure>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void handle(BigQueryFailure row) {}

                    @Override
                    public void flush() throws IOException {
                        throw new IOException("dead letters not persisted");
                    }
                };
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), unflushableHandler),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessage("dead letters not persisted");
    }

    // --- write()-time row-level failures: serialization and size limit ---

    @Test
    void serializationFailureIsRoutedToTheHandler() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        RecordingFailedRowHandler handler = new RecordingFailedRowHandler();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("ok", CONTEXT);
        writer.write("unserializable", CONTEXT);
        writer.flush(false);

        assertThat(handler.rows).hasSize(1);
        assertThat(handler.rows.get(0).getPayloadBytes()).isNull();
        assertThat(handler.rows.get(0).getErrorMessage())
                .contains("cannot serialize unserializable");
        assertThat(handler.rows.get(0).getCause()).isInstanceOf(IOException.class);
        assertThat(factory.allAppendedRows()).containsExactly("ok");
    }

    @Test
    void serializationFailureWithDefaultPolicyFailsTheWrite() {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), FailureHandler.failJob()),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        assertThatThrownBy(() -> writer.write("unserializable", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("cannot serialize unserializable");
    }

    @Test
    void oversizedRowIsRoutedToTheHandler() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        RecordingFailedRowHandler handler = new RecordingFailedRowHandler();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new OversizedSerializer(), handler),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("big", CONTEXT);
        writer.flush(false);

        assertThat(handler.rows).hasSize(1);
        assertThat(handler.rows.get(0).getPayloadBytes()).isNotNull();
        assertThat(handler.rows.get(0).getErrorMessage()).contains("per-row limit");
        assertThat(factory.allAppendedRows()).isEmpty();
    }

    @Test
    void uncheckedSerializationFailureIsRoutedToTheHandler() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        RecordingFailedRowHandler handler = new RecordingFailedRowHandler();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new UncheckedFailingSerializer(), handler),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("poison", CONTEXT);
        writer.flush(false);

        assertThat(handler.rows).hasSize(1);
        assertThat(handler.rows.get(0).getPayloadBytes()).isNull();
        assertThat(handler.rows.get(0).getCause()).isInstanceOf(IllegalStateException.class);
        assertThat(factory.allAppendedRows()).isEmpty();
    }

    @Test
    void rowErrorsMatchingNoRowOfTheBatchAreTerminal() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        // Both the initial append and the re-append report an index outside the two-row batch:
        // nothing can be dropped, so retrying could never make progress.
        factory.scriptedResults.add(rowLevelError(Map.of(5, "stale index")));
        factory.scriptedResults.add(rowLevelError(Map.of(5, "stale index")));
        RecordingFailedRowHandler handler = new RecordingFailedRowHandler();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("row errors matching none");
        assertThat(handler.rows).isEmpty();
    }

    @Test
    void transientResponseErrorWithRowErrorsIsRetriedNotRouted() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(
                ApiFutures.immediateFuture(
                        AppendRowsResponse.newBuilder()
                                .setError(
                                        com.google.rpc.Status.newBuilder()
                                                .setCode(Status.Code.UNAVAILABLE.value())
                                                .setMessage("backend busy"))
                                .addRowErrors(
                                        RowError.newBuilder().setIndex(0).setMessage("row noise"))
                                .build()));
        RecordingFailedRowHandler handler = new RecordingFailedRowHandler();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);
        writer.flush(false);

        // The transient request error takes precedence: the whole batch is retried, no row is
        // dropped or routed to the handler.
        assertThat(handler.rows).isEmpty();
        assertThat(factory.allAppendedRows()).containsExactly("aa", "aa");
    }

    // --- handler lifecycle ---

    @Test
    void closeClosesTheHandler() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        RecordingFailedRowHandler handler = new RecordingFailedRowHandler();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);
        writer.flush(false);
        writer.close();

        assertThat(handler.closed).isTrue();
    }

    @Test
    void closeStillClosesTheHandlerWhenAnAppenderCloseThrowsAnError() throws Exception {
        // #276: the handler is last after every destination's appender, and Flink's
        // IOUtils.closeAll rethrew an Error from inside its loop, leaving it open. That the Error
        // reaches the caller as an Error is the other half — Flink halts the JVM on a fatal one,
        // and only if it arrives unwrapped.
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        RecordingFailedRowHandler handler = new RecordingFailedRowHandler();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler),
                        factory,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);
        writer.write("aa", CONTEXT);
        writer.flush(false);
        factory.closeFailure = new NoClassDefFoundError("appender close blew up");

        assertThatThrownBy(writer::close)
                .isInstanceOf(NoClassDefFoundError.class)
                .hasMessage("appender close blew up");
        assertThat(handler.closed).isTrue();
    }
}
