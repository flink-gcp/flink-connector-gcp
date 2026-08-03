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

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRow;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamCommittable;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigQueryBufferedStreamWriter} (happy path and stream lifecycle). */
class BigQueryBufferedStreamWriterTest {

    static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");

    static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    /** Serializer writing the record string bytes; descriptor is irrelevant for the fake. */
    static class StringSerializer extends BigQueryProtoSerializer<String> {
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

        @Override
        public ByteString serialize(String element) {
            if (element.startsWith("poison")) {
                throw new IllegalStateException("cannot serialize " + element);
            }
            return ByteString.copyFromUtf8(element);
        }
    }

    /** Handler recording every failed row. */
    static class RecordingHandler implements FailureHandler<FailedRow> {
        private static final long serialVersionUID = 1L;

        final List<FailedRow> rows = new ArrayList<>();

        /** "handle"/"flush" in invocation order, pinning that flush runs after the drain. */
        final List<String> events = new ArrayList<>();

        boolean closed;

        @Override
        public void handle(FailedRow row) {
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

    /** Options with a fast retry schedule for tests. */
    static BufferedStreamOptions fastOptions(int maxAttempts) {
        return BufferedStreamOptions.builder()
                .recoveryInitialBackoff(Duration.ofMillis(1))
                .recoveryMaxBackoff(Duration.ofMillis(1))
                .recoveryMaxAttempts(maxAttempts)
                .build();
    }

    static BigQuerySinkConfig<String> config() {
        return config(new StringSerializer(), null, null);
    }

    static BigQuerySinkConfig<String> config(
            BigQueryProtoSerializer<? super String> serializer,
            FailureHandler<FailedRow> handler,
            CreateDisposition createDisposition) {
        var builder = BigQuerySink.<String>builder().destination(DESTINATION);
        builder.serializer(serializer);
        if (handler != null) {
            builder.failedRowHandler(handler);
        }
        if (createDisposition != null) {
            builder.createDisposition(createDisposition);
        }
        return ((BigQueryDefaultStreamSink<String>) builder.build()).getConfig();
    }

    static BigQueryBufferedStreamWriter<String> writer(
            BigQuerySinkConfig<String> config,
            BufferedStreamOptions options,
            FakeBufferedStreamService service,
            TableAdmin tableAdmin,
            BufferedStreamWriterState... restoredStates) {
        return new BigQueryBufferedStreamWriter<>(
                config,
                options,
                service.asFactory(),
                tableAdmin,
                TestSinkWriterMetricGroup.create(),
                0,
                List.of(restoredStates));
    }

    static List<String> rowsOf(ProtoRows rows) {
        List<String> values = new ArrayList<>();
        rows.getSerializedRowsList().forEach(b -> values.add(b.toStringUtf8()));
        return values;
    }

    static BufferedStreamCommittable onlyCommittable(
            Collection<BufferedStreamCommittable> committables) {
        assertThat(committables).hasSize(1);
        return committables.iterator().next();
    }

    @Test
    void createsNoStreamWithoutRecords() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        writer.flush(false);

        assertThat(writer.prepareCommit()).isEmpty();
        assertThat(service.createdStreams).isEmpty();
        List<BufferedStreamWriterState> states = writer.snapshotState(1);
        assertThat(states)
                .containsExactly(
                        new BufferedStreamWriterState(BufferedStreamWriterState.NO_STREAM, 0, 1));
        writer.close();
    }

    @Test
    void handlerFlushRunsAtEveryWriterFlushAfterRoutedRowsAreHandled() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        // Rejected inside flush()'s drain, not at write() time, so the events order pins that
        // the handler flushes only after the drain routed the row.
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        new Exceptions.AppendSerializtionError(
                                Status.Code.INVALID_ARGUMENT.value(),
                                "bad rows",
                                "stream",
                                Map.of(0, "bad row"))));
        RecordingHandler handler = new RecordingHandler();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler, null),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        writer.write("a", CONTEXT);
        writer.flush(false);
        writer.flush(true);

        // The routed row is handled before the first flush(), so a buffering handler has
        // everything when it persists; end of input flushes the handler too.
        assertThat(handler.events).containsExactly("handle", "flush", "flush");
        writer.close();
    }

    @Test
    void closeClosesTheHandler() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        RecordingHandler handler = new RecordingHandler();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler, null),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        writer.close();

        assertThat(handler.closed).isTrue();
    }

    @Test
    void closeStillClosesTheHandlerWhenTheServiceCloseThrows() throws Exception {
        // The lifecycle contract promises close on the failure path too; sequential closes would
        // skip the handler when the service close throws.
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.closeFailure = new IllegalStateException("service close failure");
        RecordingHandler handler = new RecordingHandler();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler, null),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);
        writer.write("a", CONTEXT);
        writer.flush(false);

        assertThatThrownBy(writer::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("service close failure");
        assertThat(handler.closed).isTrue();
    }

    @Test
    void appendsAndEmitsOneCommittablePerCheckpoint() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        writer.write("a", CONTEXT);
        writer.write("b", CONTEXT);
        writer.flush(false);

        assertThat(service.createdStreams).hasSize(1);
        String stream = service.createdStreams.get(0);
        assertThat(service.appends).hasSize(1);
        assertThat(service.appends.get(0).offset).isEqualTo(0);
        assertThat(rowsOf(service.appends.get(0).rows)).containsExactly("a", "b");

        BufferedStreamCommittable committable = onlyCommittable(writer.prepareCommit());
        assertThat(committable.getStreamName()).isEqualTo(stream);
        assertThat(committable.getFlushOffset()).isEqualTo(1);
        assertThat(committable.getSubtaskId()).isEqualTo(0);

        assertThat(writer.snapshotState(7))
                .containsExactly(new BufferedStreamWriterState(stream, 2, 7));

        // A quiet checkpoint emits nothing.
        writer.flush(false);
        assertThat(writer.prepareCommit()).isEmpty();

        // Progress on the same stream emits the advanced offset; no new stream is created.
        writer.write("c", CONTEXT);
        writer.flush(false);
        BufferedStreamCommittable next = onlyCommittable(writer.prepareCommit());
        assertThat(next.getStreamName()).isEqualTo(stream);
        assertThat(next.getFlushOffset()).isEqualTo(2);
        assertThat(service.createdStreams).hasSize(1);
        assertThat(service.appends.get(1).offset).isEqualTo(2);
    }

    @Test
    void rollsBatchesAtMaxAppendRequestBytes() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        BufferedStreamOptions options =
                BufferedStreamOptions.builder().maxAppendRequestBytes(4).build();
        BigQueryBufferedStreamWriter<String> writer =
                writer(config(), options, service, BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);
        writer.write("cc", CONTEXT);
        writer.flush(false);

        assertThat(service.appends).hasSize(2);
        assertThat(service.appends.get(0).offset).isEqualTo(0);
        assertThat(rowsOf(service.appends.get(0).rows)).containsExactly("aa", "bb");
        assertThat(service.appends.get(1).offset).isEqualTo(2);
        assertThat(rowsOf(service.appends.get(1).rows)).containsExactly("cc");
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isEqualTo(2);
    }

    @Test
    void routesSerializationFailuresWithoutCreatingAStream() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        RecordingHandler handler = new RecordingHandler();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler, null),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        writer.write("poison1", CONTEXT);
        writer.flush(false);

        assertThat(handler.rows).hasSize(1);
        assertThat(handler.rows.get(0).getErrorMessage()).contains("Failed to serialize");
        assertThat(service.createdStreams).isEmpty();
        assertThat(writer.prepareCommit()).isEmpty();
    }

    @Test
    void routesOversizedRowsWithoutAppending() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        RecordingHandler handler = new RecordingHandler();
        BigQueryProtoSerializer<String> oversized =
                new StringSerializer() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public ByteString serialize(String element) {
                        return ByteString.copyFrom(
                                new byte[BigQueryDefaultStreamWriter.MAX_ROW_BYTES + 1]);
                    }
                };
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(oversized, handler, null),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        writer.write("big", CONTEXT);
        writer.flush(false);

        assertThat(handler.rows).hasSize(1);
        assertThat(handler.rows.get(0).getErrorMessage()).contains("per-row limit");
        assertThat(service.createdStreams).isEmpty();
    }

    @Test
    void createsTheTableOnNotFoundUnderCreateIfNeeded() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.createFailures.add(
                ApiExceptionFactory.createException(
                        null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false));
        List<TableDestination> created = new ArrayList<>();
        TableAdmin admin =
                new BigQueryDefaultStreamWriterTest.NoopTableAdmin() {
                    @Override
                    public void create(
                            TableDestination destination,
                            TableSchema schema,
                            TableCreateOptions options) {
                        created.add(destination);
                    }
                };
        BigQueryBufferedStreamWriter<String> writer =
                writer(config(), fastOptions(3), service, admin);

        writer.write("a", CONTEXT);
        writer.flush(false);

        assertThat(created).containsExactly(DESTINATION);
        assertThat(service.createdStreams).hasSize(1);
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isEqualTo(0);
    }

    @Test
    void failsOnNotFoundUnderCreateNever() {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.createFailures.add(
                ApiExceptionFactory.createException(
                        null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false));
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), null, CreateDisposition.CREATE_NEVER),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        assertThatThrownBy(
                        () -> {
                            writer.write("a", CONTEXT);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to create a BigQuery buffered stream");
    }

    @Test
    void closeNeverFinalizesTheStream() throws Exception {
        // BigQuery rejects FlushRows on a finalized stream, and in batch execution (and after a
        // crash) committables are committed after the writer closed — the stream must stay open.
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        writer.write("a", CONTEXT);
        writer.flush(false);
        writer.prepareCommit();
        writer.close();

        assertThat(service.closed).isTrue();
        // The committable emitted before close must still be flushable.
        assertThat(service.flushRows(service.createdStreams.get(0), 0)).isEqualTo(0);
    }

    @Test
    void emitsNothingWhenEveryRowWasDropped() throws Exception {
        // All rows routed to the handler: no stream progress, no committable with offset -1.
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        RecordingHandler handler = new RecordingHandler();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler, null),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        writer.write("poison1", CONTEXT);
        writer.write("poison2", CONTEXT);
        writer.flush(false);

        assertThat(handler.rows).hasSize(2);
        assertThat(writer.prepareCommit()).isEmpty();
        assertThat(writer.snapshotState(1))
                .containsExactly(
                        new BufferedStreamWriterState(BufferedStreamWriterState.NO_STREAM, 0, 1));
    }

    @Test
    void statelessRestoreBehavesLikeAFreshWriter() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        BigQueryBufferedStreamWriter<String> writer =
                new BigQueryBufferedStreamWriter<>(
                        config(),
                        fastOptions(3),
                        service.asFactory(),
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                        TestSinkWriterMetricGroup.create(),
                        0,
                        Collections.emptyList());

        writer.write("a", CONTEXT);
        writer.flush(false);

        assertThat(service.createdStreams).hasSize(1);
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isEqualTo(0);
    }

    /**
     * The SPI takes the options so the SDK {@code retry*} knobs reach the appenders. A service
     * built with anything other than the writer's own options — defaults, say — would drop them
     * silently, since the appender is the only thing that reads them.
     */
    @Test
    void theWriterBuildsItsServiceWithItsOwnOptions() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        BufferedStreamOptions options =
                BufferedStreamOptions.builder()
                        .recoveryInitialBackoff(Duration.ofMillis(1))
                        .recoveryMaxBackoff(Duration.ofMillis(1))
                        .retryMaxAttempts(9)
                        .build();
        BigQueryBufferedStreamWriter<String> writer =
                writer(config(), options, service, BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        writer.write("row-1", CONTEXT);
        writer.flush(false);

        assertThat(service.createdWith).isSameAs(options);
        writer.close();
    }
}
