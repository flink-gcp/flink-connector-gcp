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
import java.util.Set;
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
 * awaits every in-flight append, so records never pass a checkpoint barrier unacknowledged — this
 * is what makes the sink at-least-once. Asynchronous append failures are captured and rethrown on
 * the next {@link #write} or {@link #flush} call.
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

    private final BigQuerySinkConfig<T> config;
    private final RowAppenderFactory appenderFactory;
    private final long maxAppendRequestBytes;

    /** Accessed only from the task thread. */
    private final Map<TableDestination, DestinationState> states = new HashMap<>();

    /** Completed entries are removed by gRPC callback threads. */
    private final Set<ApiFuture<AppendRowsResponse>> inFlight = ConcurrentHashMap.newKeySet();

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
        DestinationState state = states.get(destination);
        if (state == null) {
            state = createState(destination);
            states.put(destination, state);
        }
        if (!state.rows.isEmpty() && state.pendingBytes + row.size() > maxAppendRequestBytes) {
            appendPending(state);
        }
        state.rows.add(row);
        state.pendingBytes += row.size();
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        checkAsyncError();
        for (DestinationState state : states.values()) {
            if (!state.rows.isEmpty()) {
                appendPending(state);
            }
        }
        for (ApiFuture<AppendRowsResponse> future : inFlight.toArray(new ApiFuture[0])) {
            try {
                future.get();
            } catch (ExecutionException e) {
                throw new IOException("An append to BigQuery failed", e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while flushing appends to BigQuery", e);
            }
        }
        checkAsyncError();
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        for (DestinationState state : states.values()) {
            try {
                state.appender.close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        states.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private DestinationState createState(TableDestination destination) throws IOException {
        RowAppender appender =
                appenderFactory.create(
                        destination,
                        config.getSerializer().getDescriptor(destination),
                        config.getLocation());
        return new DestinationState(appender);
    }

    private void appendPending(DestinationState state) {
        ProtoRows rows = ProtoRows.newBuilder().addAllSerializedRows(state.rows).build();
        state.rows = new ArrayList<>();
        state.pendingBytes = 0;
        ApiFuture<AppendRowsResponse> future = state.appender.append(rows);
        inFlight.add(future);
        ApiFutures.addCallback(
                future,
                new ApiFutureCallback<AppendRowsResponse>() {
                    @Override
                    public void onSuccess(AppendRowsResponse response) {
                        if (response.hasError() || response.getRowErrorsCount() > 0) {
                            asyncError.compareAndSet(
                                    null,
                                    new IOException(
                                            "BigQuery append completed with errors: "
                                                    + (response.hasError()
                                                            ? response.getError().getMessage()
                                                            : response.getRowErrors(0)
                                                                    .getMessage())));
                        }
                        inFlight.remove(future);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        asyncError.compareAndSet(null, t);
                        inFlight.remove(future);
                    }
                },
                Runnable::run);
    }

    private void checkAsyncError() throws IOException {
        Throwable error = asyncError.get();
        if (error != null) {
            throw new IOException("An append to BigQuery failed", error);
        }
    }

    private static final class DestinationState {
        private final RowAppender appender;
        private List<ByteString> rows = new ArrayList<>();
        private long pendingBytes;

        DestinationState(RowAppender appender) {
            this.appender = appender;
        }
    }
}
