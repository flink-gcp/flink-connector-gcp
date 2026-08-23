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

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryDefaultStreamWriterTest.NOOP_ADMIN;
import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryDefaultStreamWriterTest.fastSchedule;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BigQueryDefaultStreamWriter}'s cold-destination eviction and periodic
 * (processing-time) flush.
 */
class BigQueryDefaultStreamWriterEvictionTest {

    private static final Duration IDLE_TIMEOUT = Duration.ofMinutes(10);
    private static final Duration FLUSH_INTERVAL = Duration.ofSeconds(5);

    private static final TableDestination A = TableDestination.of("p", "d", "a");
    private static final TableDestination B = TableDestination.of("p", "d", "b");

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    private final CountingAppenderFactory factory = new CountingAppenderFactory();
    private final AtomicLong nanos = new AtomicLong();
    private final ManualProcessingTimeService timers = new ManualProcessingTimeService();

    /** Serializer writing the record string bytes; the descriptor is irrelevant for the fake. */
    private static final class StringSerializer extends BigQueryProtoSerializationSchema<String> {
        private static final long serialVersionUID = 1L;

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
            return Empty.getDescriptor();
        }

        @Override
        public ByteString serialize(String element) {
            return ByteString.copyFromUtf8(element);
        }
    }

    /** Appender factory recording every appender ever created, in creation order. */
    private static final class CountingAppenderFactory implements RowAppenderFactory {
        private static final long serialVersionUID = 1L;

        private final Map<TableDestination, List<FakeAppender>> appenders = new LinkedHashMap<>();
        private final List<ApiFuture<AppendRowsResponse>> scriptedResults = new ArrayList<>();
        private boolean closeThrows;

        @Override
        public RowAppender create(
                TableDestination destination,
                Descriptors.Descriptor rowDescriptor,
                String location) {
            FakeAppender appender = new FakeAppender();
            appenders.computeIfAbsent(destination, d -> new ArrayList<>()).add(appender);
            return appender;
        }

        private List<FakeAppender> of(TableDestination destination) {
            return appenders.getOrDefault(destination, List.of());
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
                if (closeThrows) {
                    throw new IllegalStateException("close failed");
                }
            }
        }
    }

    /** Timers fire when the manually advanced clock passes them; callbacks may re-register. */
    private static final class ManualProcessingTimeService implements ProcessingTimeService {
        private long now;
        private final List<Long> timestamps = new ArrayList<>();
        private final List<ProcessingTimeCallback> callbacks = new ArrayList<>();

        @Override
        public long getCurrentProcessingTime() {
            return now;
        }

        @Override
        public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback) {
            timestamps.add(timestamp);
            callbacks.add(callback);
            return null;
        }

        int registrations() {
            return timestamps.size();
        }

        void advanceTo(long time) throws Exception {
            now = time;
            for (int i = 0; i < callbacks.size(); i++) {
                if (timestamps.get(i) != null && timestamps.get(i) <= now) {
                    ProcessingTimeCallback callback = callbacks.get(i);
                    timestamps.set(i, null);
                    callback.onProcessingTime(now);
                }
            }
        }
    }

    private static BigQuerySinkConfig<String> config() {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destinationResolver(
                                        (element, context) ->
                                                TableDestination.of(
                                                        "p", "d", element.substring(0, 1)))
                                .serializer(new StringSerializer())
                                .build();
        return sink.getConfig();
    }

    private BigQueryDefaultStreamWriter<String> writer(@Nullable Duration flushInterval) {
        return new BigQueryDefaultStreamWriter<>(
                config(),
                factory,
                NOOP_ADMIN,
                new DefaultStreamWriterMetrics(TestSinkWriterMetricGroup.create(), false),
                DefaultStreamOptions.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                fastSchedule(1),
                fastSchedule(1),
                IDLE_TIMEOUT,
                flushInterval,
                flushInterval != null ? timers : null,
                nanos::get);
    }

    @Test
    void coldDestinationIsEvictedWhileActiveSiblingSurvives() throws Exception {
        BigQueryDefaultStreamWriter<String> writer = writer(null);

        writer.write("a1", CONTEXT);
        nanos.addAndGet(IDLE_TIMEOUT.toNanos());
        writer.write("b1", CONTEXT);
        nanos.addAndGet(1);
        writer.flush(false);

        assertThat(factory.of(A).get(0).closed).isTrue();
        assertThat(factory.of(B).get(0).closed).isFalse();
        assertThat(factory.of(B)).hasSize(1);
    }

    @Test
    void idleExactlyAtTheTimeoutIsKept() throws Exception {
        BigQueryDefaultStreamWriter<String> writer = writer(null);

        writer.write("a1", CONTEXT);
        nanos.addAndGet(IDLE_TIMEOUT.toNanos());
        writer.flush(false);

        assertThat(factory.of(A).get(0).closed).isFalse();

        nanos.addAndGet(1);
        writer.flush(false);

        assertThat(factory.of(A).get(0).closed).isTrue();
    }

    /** Writing must refresh idleness: total age past the timeout is not what evicts. */
    @Test
    void writingRefreshesTheIdleClock() throws Exception {
        BigQueryDefaultStreamWriter<String> writer = writer(null);

        writer.write("a1", CONTEXT);
        nanos.addAndGet(IDLE_TIMEOUT.toNanos() / 2);
        writer.write("a2", CONTEXT);
        nanos.addAndGet(IDLE_TIMEOUT.toNanos() / 2 + 1);
        writer.flush(false);

        assertThat(factory.of(A).get(0).closed).isFalse();
    }

    @Test
    void evictedDestinationRecoversOnTheNextWrite() throws Exception {
        BigQueryDefaultStreamWriter<String> writer = writer(null);

        writer.write("a1", CONTEXT);
        nanos.addAndGet(IDLE_TIMEOUT.toNanos() + 1);
        writer.flush(false);
        writer.write("a2", CONTEXT);
        writer.flush(false);

        assertThat(factory.of(A)).hasSize(2);
        assertThat(factory.of(A).get(1).appends).hasSize(1);
        assertThat(factory.of(A).get(1).appends.get(0).getSerializedRows(0).toStringUtf8())
                .isEqualTo("a2");
    }

    @Test
    void endOfInputFlushDoesNotEvict() throws Exception {
        BigQueryDefaultStreamWriter<String> writer = writer(null);

        writer.write("a1", CONTEXT);
        nanos.addAndGet(IDLE_TIMEOUT.toNanos() + 1);
        writer.flush(true);

        assertThat(factory.of(A).get(0).closed).isFalse();
    }

    /** Hygiene must never fail a checkpoint: a failing close is logged and the entry dropped. */
    @Test
    void failingAppenderCloseDoesNotFailTheFlush() throws Exception {
        BigQueryDefaultStreamWriter<String> writer = writer(null);

        writer.write("a1", CONTEXT);
        nanos.addAndGet(IDLE_TIMEOUT.toNanos() + 1);
        factory.closeThrows = true;
        writer.flush(false);
        factory.closeThrows = false;
        writer.write("a2", CONTEXT);

        assertThat(factory.of(A)).hasSize(2);
    }

    /** The public options constructor must plumb the idle timeout through. */
    @Test
    void optionsConstructorAppliesTheIdleTimeout() throws Exception {
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(),
                        factory,
                        NOOP_ADMIN,
                        TestSinkWriterMetricGroup.create(),
                        DefaultStreamOptions.builder()
                                .destinationIdleTimeout(Duration.ofNanos(1))
                                .build(),
                        null);

        writer.write("a1", CONTEXT);
        writer.flush(false);

        assertThat(factory.of(A).get(0).closed).isTrue();
    }

    @Test
    void noTimerIsRegisteredWithoutAFlushInterval() {
        writer(null);

        assertThat(timers.registrations()).isZero();
    }

    /** The public options constructor must plumb the flush interval through. */
    @Test
    void optionsConstructorArmsTheTimer() {
        new BigQueryDefaultStreamWriter<>(
                config(),
                factory,
                NOOP_ADMIN,
                TestSinkWriterMetricGroup.create(),
                DefaultStreamOptions.builder().flushInterval(FLUSH_INTERVAL).build(),
                timers);

        assertThat(timers.registrations()).isEqualTo(1);
        assertThat(timers.timestamps.get(0)).isEqualTo(FLUSH_INTERVAL.toMillis());
    }

    @Test
    void timerFlushesPendingRowsAndRearms() throws Exception {
        BigQueryDefaultStreamWriter<String> writer = writer(FLUSH_INTERVAL);

        writer.write("a1", CONTEXT);
        timers.advanceTo(FLUSH_INTERVAL.toMillis());

        assertThat(factory.of(A).get(0).appends).hasSize(1);
        assertThat(timers.registrations()).isEqualTo(2);

        writer.write("a2", CONTEXT);
        timers.advanceTo(2 * FLUSH_INTERVAL.toMillis());

        assertThat(factory.of(A).get(0).appends).hasSize(2);
        assertThat(timers.registrations()).isEqualTo(3);
    }

    /** The timer path is a full flush, so idle eviction runs from it too. */
    @Test
    void timerFlushEvictsIdleDestinations() throws Exception {
        BigQueryDefaultStreamWriter<String> writer = writer(FLUSH_INTERVAL);

        writer.write("a1", CONTEXT);
        nanos.addAndGet(IDLE_TIMEOUT.toNanos() + 1);
        timers.advanceTo(FLUSH_INTERVAL.toMillis());
        timers.advanceTo(2 * FLUSH_INTERVAL.toMillis());

        assertThat(factory.of(A).get(0).closed).isTrue();
    }

    @Test
    void timerStopsAfterClose() throws Exception {
        BigQueryDefaultStreamWriter<String> writer = writer(FLUSH_INTERVAL);

        writer.write("a1", CONTEXT);
        writer.close();
        timers.advanceTo(FLUSH_INTERVAL.toMillis());

        // The pending timer fired into a closed writer: no flush, no re-arm.
        assertThat(factory.of(A).get(0).appends).isEmpty();
        assertThat(timers.registrations()).isEqualTo(1);
    }

    @Test
    void timerFlushFailurePropagates() throws Exception {
        BigQueryDefaultStreamWriter<String> writer = writer(FLUSH_INTERVAL);
        factory.scriptedResults.add(
                ApiFutures.immediateFailedFuture(
                        Status.INVALID_ARGUMENT
                                .withDescription("bad request")
                                .asRuntimeException()));

        writer.write("a1", CONTEXT);

        assertThatThrownBy(() -> timers.advanceTo(FLUSH_INTERVAL.toMillis()))
                .isInstanceOf(IOException.class);
    }
}
