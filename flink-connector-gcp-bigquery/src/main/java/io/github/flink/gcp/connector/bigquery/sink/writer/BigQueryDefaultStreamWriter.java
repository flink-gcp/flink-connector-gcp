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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * At-least-once {@link SinkWriter} appending to Storage Write API default streams with dynamic
 * per-record table destinations.
 *
 * <p>Per destination, rows are buffered into append batches (bounded by {@link
 * #DEFAULT_MAX_APPEND_REQUEST_BYTES}) and appended asynchronously; backpressure is provided by the
 * stream writer's own in-flight limits. {@link #flush(boolean)} appends all pending batches and
 * awaits every in-flight append, inspecting each response directly, so records never pass a
 * checkpoint barrier unacknowledged — this is what makes the sink at-least-once. Asynchronous
 * append failures are additionally captured by completion callbacks and rethrown on the next {@link
 * #write} or {@link #flush} call.
 *
 * <p>The writer is <em>stateless</em>: it stores nothing in Flink state, so discarding operator
 * state can never lose sink-buffered data (the {@code AsyncSinkWriter}-style alternative of
 * persisting unflushed buffers into writer state was deliberately rejected for exactly that failure
 * mode). Checkpointing must be enabled for the at-least-once guarantee in streaming jobs; without
 * it {@code flush()} is only invoked at end of input.
 *
 * <p>In-flight batches are retained together with their destination until acknowledged (the
 * groundwork for table auto-creation (#11) and schema-evolution rebuilds (#12), which re-append
 * failed batches).
 *
 * <p>Table auto-creation ({@code CREATE_IF_NEEDED}) is tracked in issue #11; until it lands,
 * destination tables must exist.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class BigQueryDefaultStreamWriter<T> implements SinkWriter<T> {

    /**
     * Maximum serialized-row bytes buffered per destination before an append request is issued.
     * Well below the Storage Write API's 10 MB request limit; larger batches amortize request
     * overhead, smaller ones bound memory and latency.
     */
    static final long DEFAULT_MAX_APPEND_REQUEST_BYTES = 512 * 1024;

    /**
     * Hard per-row limit, kept under the Storage Write API's 10 MB AppendRows request cap (with
     * headroom for request framing). Rejected client-side so the offending record is identified
     * immediately instead of poisoning the pipeline through replayed server-side rejections.
     */
    static final int MAX_ROW_BYTES = 9 * 1024 * 1024;

    private final BigQuerySinkConfig<T> config;
    private final RowAppenderFactory appenderFactory;
    private final long maxAppendRequestBytes;

    /** Accessed only from the task thread. */
    private final Map<TableDestination, DestinationState> states = new HashMap<>();

    /** Completed entries are removed by gRPC callback threads. */
    private final Map<ApiFuture<AppendRowsResponse>, TableDestination> inFlight =
            new ConcurrentHashMap<>();

    private final AtomicReference<Throwable> asyncError = new AtomicReference<>();

    /**
     * Creates a writer.
     *
     * @param config the sink configuration
     * @param appenderFactory the appender factory
     */
    public BigQueryDefaultStreamWriter(
            BigQuerySinkConfig<T> config, RowAppenderFactory appenderFactory) {
        this(config, appenderFactory, DEFAULT_MAX_APPEND_REQUEST_BYTES);
    }

    BigQueryDefaultStreamWriter(
            BigQuerySinkConfig<T> config,
            RowAppenderFactory appenderFactory,
            long maxAppendRequestBytes) {
        this.config = Preconditions.checkNotNull(config, "config must not be null");
        this.appenderFactory =
                Preconditions.checkNotNull(appenderFactory, "appenderFactory must not be null");
        this.maxAppendRequestBytes = maxAppendRequestBytes;
    }

    @Override
    public void write(T element, Context context) throws IOException {
        checkAsyncError();
        TableDestination destination = config.getDestinationResolver().resolve(element, context);
        ByteString row = config.getSerializer().serialize(element);
        if (row.size() > MAX_ROW_BYTES) {
            throw new IOException(
                    "A row for "
                            + destination
                            + " is "
                            + row.size()
                            + " bytes, exceeding the "
                            + MAX_ROW_BYTES
                            + "-byte per-row limit of the BigQuery Storage Write API");
        }
        DestinationState state = states.get(destination);
        if (state == null) {
            state = createState(destination);
            states.put(destination, state);
        }
        if (state.pendingCount() > 0 && state.pendingBytes + row.size() > maxAppendRequestBytes) {
            appendPending(destination, state);
        }
        state.add(row);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        checkAsyncError();
        for (Map.Entry<TableDestination, DestinationState> entry : states.entrySet()) {
            if (entry.getValue().pendingCount() > 0) {
                appendPending(entry.getKey(), entry.getValue());
            }
        }
        // Inspect every in-flight response directly: waiters can be released before completion
        // callbacks have run, so relying on the callbacks alone could let a checkpoint succeed
        // ahead of a captured failure.
        for (Map.Entry<ApiFuture<AppendRowsResponse>, TableDestination> entry :
                inFlight.entrySet()) {
            try {
                checkResponse(entry.getValue(), entry.getKey().get());
            } catch (ExecutionException e) {
                throw new IOException(
                        "An append to BigQuery table " + entry.getValue() + " failed",
                        e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while flushing appends to BigQuery", e);
            }
        }
        checkAsyncError();
    }

    @Override
    public void close() throws Exception {
        List<AutoCloseable> appenders = new ArrayList<>(states.size());
        for (DestinationState state : states.values()) {
            appenders.add(state.appender);
        }
        states.clear();
        IOUtils.closeAll(appenders);
    }

    private DestinationState createState(TableDestination destination) throws IOException {
        RowAppender appender =
                appenderFactory.create(
                        destination,
                        config.getSerializer().getDescriptor(destination),
                        config.getLocation());
        return new DestinationState(appender);
    }

    private void appendPending(TableDestination destination, DestinationState state) {
        ProtoRows rows = state.take();
        ApiFuture<AppendRowsResponse> future = state.appender.append(rows);
        inFlight.put(future, destination);
        ApiFutures.addCallback(
                future,
                new ApiFutureCallback<AppendRowsResponse>() {
                    @Override
                    public void onSuccess(AppendRowsResponse response) {
                        try {
                            checkResponse(destination, response);
                        } catch (IOException e) {
                            asyncError.compareAndSet(null, e);
                        }
                        inFlight.remove(future);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        asyncError.compareAndSet(
                                null,
                                new IOException(
                                        "An append to BigQuery table " + destination + " failed",
                                        t));
                        inFlight.remove(future);
                    }
                },
                Runnable::run);
    }

    private static void checkResponse(TableDestination destination, AppendRowsResponse response)
            throws IOException {
        if (response.hasError()) {
            throw new IOException(
                    "An append to BigQuery table "
                            + destination
                            + " completed with an error: "
                            + response.getError().getMessage());
        }
        if (response.getRowErrorsCount() > 0) {
            throw new IOException(
                    "An append to BigQuery table "
                            + destination
                            + " completed with "
                            + response.getRowErrorsCount()
                            + " row error(s), first: "
                            + response.getRowErrors(0).getMessage());
        }
    }

    private void checkAsyncError() throws IOException {
        Throwable error = asyncError.get();
        if (error != null) {
            if (error instanceof IOException) {
                throw (IOException) error;
            }
            throw new IOException("An append to BigQuery failed", error);
        }
    }

    private static final class DestinationState {
        private final RowAppender appender;
        private ProtoRows.Builder rows = ProtoRows.newBuilder();
        private long pendingBytes;

        DestinationState(RowAppender appender) {
            this.appender = appender;
        }

        void add(ByteString row) {
            rows.addSerializedRows(row);
            pendingBytes += row.size();
        }

        int pendingCount() {
            return rows.getSerializedRowsCount();
        }

        ProtoRows take() {
            ProtoRows batch = rows.build();
            rows = ProtoRows.newBuilder();
            pendingBytes = 0;
            return batch;
        }
    }
}
