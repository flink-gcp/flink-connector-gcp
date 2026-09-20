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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BufferedStreamService;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.OffsetRowAppender;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.RowAppender;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.RowAppenderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Per-writer, best-effort logs of connector-to-appender calls, without retaining rows or futures.
 */
@Internal
final class AppenderObservations {
    private static final Logger LOG = LoggerFactory.getLogger(AppenderObservations.class);
    private final String identity;
    private final LongSupplier clock;
    private final Consumer<Event> output;
    private final long started;
    private long nextAppender;
    private long sequence;
    private long openAppenders;
    private long lostEvents;

    static AppenderObservations logging(RecoveryOptions options, WriterInitContext context) {
        String identity =
                "runId="
                        + options.runId
                        + " phase="
                        + options.phase
                        + " mode="
                        + options.mode
                        + " subtask="
                        + context.getTaskInfo().getIndexOfThisSubtask()
                        + " attempt="
                        + context.getTaskInfo().getAttemptNumber()
                        + " writer="
                        + UUID.randomUUID();
        return new AppenderObservations(
                identity,
                System::nanoTime,
                event -> LOG.info("bigquery-appender v=1 {}", event.format()));
    }

    AppenderObservations(String identity, LongSupplier clock, Consumer<Event> output) {
        this.identity = identity;
        this.clock = clock;
        this.output = output;
        this.started = clock.getAsLong();
        emit(null, "writer", "created", null, -1, started, null);
    }

    RowAppenderFactory observe(RowAppenderFactory factory) {
        return (destination, descriptor, location) -> {
            Handle handle = handle(destination.toTablePath() + "/streams/_default");
            long before = clock.getAsLong();
            RowAppender delegate;
            try {
                delegate = factory.create(destination, descriptor, location);
            } catch (IOException | RuntimeException | Error failure) {
                emit(handle, "open", "threw", null, -1, before, failure);
                throw failure;
            }
            emit(handle, "open", "returned", null, -1, before, null);
            return new RowAppender() {
                @Override
                public ApiFuture<AppendRowsResponse> append(ProtoRows rows) {
                    return send(handle, rows, -1, () -> delegate.append(rows));
                }

                @Override
                public void close() {
                    closeAppender(handle, delegate::close);
                }
            };
        };
    }

    BufferedStreamService observe(BufferedStreamService service) {
        return new BufferedStreamService() {
            @Override
            public String createBufferedStream(TableDestination destination) throws IOException {
                return service.createBufferedStream(destination);
            }

            @Override
            public OffsetRowAppender openAppender(String stream, Descriptors.Descriptor descriptor)
                    throws IOException {
                Handle handle = handle(stream);
                long before = clock.getAsLong();
                OffsetRowAppender delegate;
                try {
                    delegate = service.openAppender(stream, descriptor);
                } catch (IOException | RuntimeException | Error failure) {
                    emit(handle, "open", "threw", null, -1, before, failure);
                    throw failure;
                }
                emit(handle, "open", "returned", null, -1, before, null);
                return new OffsetRowAppender() {
                    @Override
                    public ApiFuture<AppendRowsResponse> append(ProtoRows rows, long offset) {
                        return send(handle, rows, offset, () -> delegate.append(rows, offset));
                    }

                    @Override
                    public void close() {
                        closeAppender(handle, delegate::close);
                    }
                };
            }

            @Override
            public long flushRows(String stream, long offset) throws IOException {
                return service.flushRows(stream, offset);
            }

            @Override
            public void close() {
                service.close();
            }
        };
    }

    private ApiFuture<AppendRowsResponse> send(
            Handle handle,
            ProtoRows rows,
            long offset,
            Supplier<ApiFuture<AppendRowsResponse>> call) {
        long before = clock.getAsLong();
        ApiFuture<AppendRowsResponse> result;
        try {
            result = call.get();
        } catch (RuntimeException | Error failure) {
            emit(handle, "append", "threw", rows, offset, before, failure);
            throw failure;
        }
        emit(handle, "append", "returned", rows, offset, before, null);
        // Return the identical future: no callback, wait, retry or cancellation wrapper.
        return result;
    }

    private void closeAppender(Handle handle, Runnable close) {
        long before = clock.getAsLong();
        try {
            close.run();
        } catch (RuntimeException | Error failure) {
            emit(handle, "close", "threw", null, -1, before, failure);
            throw failure;
        }
        emit(handle, "close", "returned", null, -1, before, null);
    }

    private synchronized Handle handle(String stream) {
        return new Handle(++nextAppender, stream);
    }

    private synchronized void emit(
            Handle handle,
            String operation,
            String outcome,
            ProtoRows rows,
            long offset,
            long before,
            Throwable failure) {
        long now = clock.getAsLong();
        if (operation.equals("open") && failure == null) {
            openAppenders++;
        } else if (operation.equals("close") && failure == null && !handle.closed) {
            handle.closed = true;
            openAppenders--;
        }
        Event event =
                new Event(
                        identity,
                        ++sequence,
                        handle == null ? 0 : handle.id,
                        handle == null ? "none" : handle.stream,
                        operation,
                        outcome,
                        rows == null ? 0 : rows.getSerializedRowsCount(),
                        rows == null ? 0 : rows.getSerializedSize(),
                        offset,
                        now - before,
                        now - started,
                        openAppenders,
                        lostEvents,
                        failure == null ? "none" : failure.getClass().getName());
        try {
            output.accept(event);
        } catch (RuntimeException unavailable) {
            // Observation failure must not turn an accepted append into a synchronous rejection.
            // A later event reports the gap and cumulative loss; an abrupt ending stays incomplete.
            lostEvents++;
        }
    }

    private static final class Handle {
        private final long id;
        private final String stream;
        private boolean closed;

        private Handle(long id, String stream) {
            this.id = id;
            this.stream = stream;
        }
    }

    record Event(
            String identity,
            long sequence,
            long appender,
            String stream,
            String operation,
            String outcome,
            int rows,
            int protoRowsBytes,
            long offset,
            long callNanos,
            long elapsedNanos,
            long openAppenders,
            long lostEvents,
            String failure) {
        String format() {
            return identity
                    + " sequence="
                    + sequence
                    + " appender="
                    + appender
                    + " stream="
                    + stream
                    + " operation="
                    + operation
                    + " outcome="
                    + outcome
                    + " rows="
                    + rows
                    + " protoRowsBytes="
                    + protoRowsBytes
                    + " offset="
                    + offset
                    + " callNanos="
                    + callNanos
                    + " elapsedNanos="
                    + elapsedNanos
                    + " openAppenders="
                    + openAppenders
                    + " lostEvents="
                    + lostEvents
                    + " failure="
                    + failure;
        }
    }
}
