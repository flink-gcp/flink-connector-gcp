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
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * <p>Under {@link CreateDisposition#CREATE_IF_NEEDED}, appends failing with {@code NOT_FOUND} are
 * recovered on the task thread: the destination table is created via the {@link TableCreator}
 * (schema from the serializer, partitioning/clustering from the configured options provider), the
 * destination's stream writer is rebuilt, and the failed batch is re-appended with backoff while
 * table metadata propagates to the Storage Write API backend. In-flight batches are retained
 * together with their destination until acknowledged so they can be re-appended (this also is the
 * groundwork for schema-evolution rebuilds, #12). Under {@link CreateDisposition#CREATE_NEVER},
 * {@code NOT_FOUND} fails the write or checkpoint immediately.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class BigQueryDefaultStreamWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryDefaultStreamWriter.class);

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

    /**
     * Recovery retry schedule for {@code NOT_FOUND} after creating a table: metadata propagation to
     * the Storage Write API backend is usually seconds but can take considerably longer. The
     * defaults (500 ms initial, doubled up to 10 s, 10 attempts) allow roughly a minute in total.
     */
    static final long DEFAULT_RECOVERY_INITIAL_BACKOFF_MS = 500;

    static final long DEFAULT_RECOVERY_MAX_BACKOFF_MS = 10_000;

    static final int DEFAULT_RECOVERY_MAX_ATTEMPTS = 10;

    private final BigQuerySinkConfig<T> config;
    private final RowAppenderFactory appenderFactory;
    private final TableCreator tableCreator;
    private final long maxAppendRequestBytes;
    private final long recoveryInitialBackoffMs;
    private final long recoveryMaxBackoffMs;
    private final int recoveryMaxAttempts;

    /** Accessed only from the task thread. */
    private final Map<TableDestination, DestinationState> states = new HashMap<>();

    /** Destinations already created (or found existing) by this writer. Task thread only. */
    private final Set<TableDestination> ensuredTables = new HashSet<>();

    /** Completed entries are removed by gRPC callback threads (except recoverable failures). */
    private final Map<ApiFuture<AppendRowsResponse>, InFlightBatch> inFlight =
            new ConcurrentHashMap<>();

    private final AtomicReference<Throwable> asyncError = new AtomicReference<>();

    /**
     * Set by completion callbacks when an append failed with a recoverable {@code NOT_FOUND}; the
     * task thread then sweeps {@link #inFlight} for failed batches and recovers them.
     */
    private final AtomicBoolean recoveryNeeded = new AtomicBoolean();

    /**
     * Creates a writer.
     *
     * @param config the sink configuration
     * @param appenderFactory the appender factory
     * @param tableCreator the creator for missing destination tables
     */
    public BigQueryDefaultStreamWriter(
            BigQuerySinkConfig<T> config,
            RowAppenderFactory appenderFactory,
            TableCreator tableCreator) {
        this(
                config,
                appenderFactory,
                tableCreator,
                DEFAULT_MAX_APPEND_REQUEST_BYTES,
                DEFAULT_RECOVERY_INITIAL_BACKOFF_MS,
                DEFAULT_RECOVERY_MAX_BACKOFF_MS,
                DEFAULT_RECOVERY_MAX_ATTEMPTS);
    }

    BigQueryDefaultStreamWriter(
            BigQuerySinkConfig<T> config,
            RowAppenderFactory appenderFactory,
            TableCreator tableCreator,
            long maxAppendRequestBytes,
            long recoveryInitialBackoffMs,
            long recoveryMaxBackoffMs,
            int recoveryMaxAttempts) {
        this.config = Preconditions.checkNotNull(config, "config must not be null");
        this.appenderFactory =
                Preconditions.checkNotNull(appenderFactory, "appenderFactory must not be null");
        this.tableCreator =
                Preconditions.checkNotNull(tableCreator, "tableCreator must not be null");
        this.maxAppendRequestBytes = maxAppendRequestBytes;
        this.recoveryInitialBackoffMs = recoveryInitialBackoffMs;
        this.recoveryMaxBackoffMs = recoveryMaxBackoffMs;
        this.recoveryMaxAttempts = recoveryMaxAttempts;
    }

    @Override
    public void write(T element, Context context) throws IOException {
        checkAsyncError();
        if (recoveryNeeded.getAndSet(false)) {
            recoverFailedInFlight();
        }
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
        DestinationState state = ensureState(destination);
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
        for (Map.Entry<ApiFuture<AppendRowsResponse>, InFlightBatch> entry : inFlight.entrySet()) {
            try {
                checkResponse(entry.getValue().destination, entry.getKey().get());
            } catch (ExecutionException e) {
                handleFailedAppend(entry.getKey(), e.getCause());
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

    /**
     * Returns the destination's state, creating it if absent. When creating the appender itself
     * fails with {@code NOT_FOUND} (the SDK looks up the table's location when none is configured)
     * and the disposition allows it, the table is created and the appender creation retried.
     */
    private DestinationState ensureState(TableDestination destination) throws IOException {
        DestinationState state = states.get(destination);
        if (state != null) {
            return state;
        }
        try {
            state = createState(destination);
        } catch (IOException | RuntimeException e) {
            if (!isRecoverableNotFound(e)) {
                throw wrapFailure("Failed to open a BigQuery write stream to " + destination, e);
            }
            LOG.info(
                    "Destination table {} does not exist, creating it (CREATE_IF_NEEDED)",
                    destination);
            ensureTableExists(destination);
            state = createStateWithRetry(destination);
        }
        states.put(destination, state);
        return state;
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
        inFlight.put(future, new InFlightBatch(destination, rows));
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
                        if (isRecoverableNotFound(t)) {
                            // Leave the batch in inFlight; the task thread recovers it on the
                            // next write() or flush().
                            recoveryNeeded.set(true);
                        } else {
                            asyncError.compareAndSet(null, wrapAppendFailure(destination, t));
                            inFlight.remove(future);
                        }
                    }
                },
                Runnable::run);
    }

    /**
     * Sweeps {@link #inFlight} for batches that failed with a recoverable {@code NOT_FOUND} and
     * recovers them. Called on the task thread between checkpoints; {@link #flush(boolean)} reaches
     * the same recovery through its own response inspection.
     */
    private void recoverFailedInFlight() throws IOException {
        for (Map.Entry<ApiFuture<AppendRowsResponse>, InFlightBatch> entry : inFlight.entrySet()) {
            if (!entry.getKey().isDone()) {
                continue;
            }
            try {
                entry.getKey().get();
            } catch (ExecutionException e) {
                if (isRecoverableNotFound(e.getCause())) {
                    // Successful and non-recoverable completions are owned by the callbacks;
                    // recoverable failures are left in the map for exactly this sweep.
                    handleFailedAppend(entry.getKey(), e.getCause());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while recovering appends to BigQuery", e);
            }
        }
    }

    /**
     * Handles a completed-with-failure append on the task thread: recoverable {@code NOT_FOUND}
     * batches are re-appended after creating the table, anything else fails the writer. The {@link
     * #inFlight} removal arbitrates ownership against the completion callbacks.
     */
    private void handleFailedAppend(ApiFuture<AppendRowsResponse> future, Throwable cause)
            throws IOException {
        InFlightBatch batch = inFlight.remove(future);
        if (batch == null) {
            // The completion callback owned this failure; it is surfaced via checkAsyncError.
            return;
        }
        if (isRecoverableNotFound(cause)) {
            LOG.info(
                    "An append to {} failed because the table does not exist, creating it"
                            + " (CREATE_IF_NEEDED)",
                    batch.destination);
            ensureTableExists(batch.destination);
            appendWithRetry(batch.destination, batch.rows);
        } else {
            throw wrapAppendFailure(batch.destination, cause);
        }
    }

    /** Creates the destination table once per writer lifetime; idempotent across subtasks. */
    private void ensureTableExists(TableDestination destination) throws IOException {
        if (!ensuredTables.add(destination)) {
            return;
        }
        tableCreator.create(
                destination,
                config.getSerializer().getTableSchema(destination),
                config.getTableCreateOptionsProvider().optionsFor(destination));
    }

    /**
     * Re-appends a batch to a just-created table: rebuilds the destination's stream writer and
     * retries with backoff while the append keeps failing with {@code NOT_FOUND} (table metadata
     * has not propagated to the Storage Write API backend yet).
     */
    private void appendWithRetry(TableDestination destination, ProtoRows rows) throws IOException {
        long backoffMs = recoveryInitialBackoffMs;
        for (int attempt = 1; ; attempt++) {
            DestinationState state = null;
            try {
                state = rebuildState(destination);
            } catch (IOException | RuntimeException e) {
                if (!isRecoverableNotFound(e) || attempt >= recoveryMaxAttempts) {
                    throw wrapFailure(
                            "Failed to open a BigQuery write stream to "
                                    + destination
                                    + " after creating the table ("
                                    + attempt
                                    + " attempt(s))",
                            e);
                }
            }
            if (state != null) {
                try {
                    checkResponse(destination, state.appender.append(rows).get());
                    return;
                } catch (ExecutionException e) {
                    if (!isRecoverableNotFound(e.getCause()) || attempt >= recoveryMaxAttempts) {
                        throw wrapFailure(
                                "A re-append to BigQuery table "
                                        + destination
                                        + " failed after creating the table ("
                                        + attempt
                                        + " attempt(s))",
                                e.getCause());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Interrupted while re-appending to BigQuery table " + destination, e);
                }
            }
            LOG.info(
                    "BigQuery table {} is not readable by the Storage Write API yet"
                            + " (attempt {}/{}), backing off {} ms",
                    destination,
                    attempt,
                    recoveryMaxAttempts,
                    backoffMs);
            sleep(backoffMs);
            backoffMs = Math.min(backoffMs * 2, recoveryMaxBackoffMs);
        }
    }

    /** Retries appender creation with backoff while it fails with {@code NOT_FOUND}. */
    private DestinationState createStateWithRetry(TableDestination destination) throws IOException {
        long backoffMs = recoveryInitialBackoffMs;
        for (int attempt = 1; ; attempt++) {
            try {
                return createState(destination);
            } catch (IOException | RuntimeException e) {
                if (!isRecoverableNotFound(e) || attempt >= recoveryMaxAttempts) {
                    throw wrapFailure(
                            "Failed to open a BigQuery write stream to "
                                    + destination
                                    + " after creating the table ("
                                    + attempt
                                    + " attempt(s))",
                            e);
                }
            }
            sleep(backoffMs);
            backoffMs = Math.min(backoffMs * 2, recoveryMaxBackoffMs);
        }
    }

    /**
     * Replaces the destination's state with one backed by a fresh appender, carrying over any
     * buffered-but-not-yet-appended rows. The new state is created before the old one is torn down,
     * so a failed rebuild leaves the previous state (and its buffered rows) untouched.
     */
    private DestinationState rebuildState(TableDestination destination) throws IOException {
        DestinationState fresh = createState(destination);
        DestinationState old = states.remove(destination);
        if (old != null) {
            if (old.pendingCount() > 0) {
                for (ByteString row : old.take().getSerializedRowsList()) {
                    fresh.add(row);
                }
            }
            old.appender.close();
        }
        states.put(destination, fresh);
        return fresh;
    }

    private boolean isRecoverableNotFound(Throwable t) {
        return config.getCreateDisposition() == CreateDisposition.CREATE_IF_NEEDED && isNotFound(t);
    }

    /** Walks the cause chain for a gax or gRPC {@code NOT_FOUND}. */
    private static boolean isNotFound(Throwable t) {
        int depth = 0;
        for (Throwable cause = t; cause != null && depth < 20; cause = cause.getCause(), depth++) {
            if (cause instanceof ApiException
                    && ((ApiException) cause).getStatusCode().getCode()
                            == StatusCode.Code.NOT_FOUND) {
                return true;
            }
            if (cause instanceof StatusRuntimeException
                    && ((StatusRuntimeException) cause).getStatus().getCode()
                            == Status.Code.NOT_FOUND) {
                return true;
            }
        }
        return false;
    }

    private IOException wrapAppendFailure(TableDestination destination, Throwable cause) {
        return wrapFailure("An append to BigQuery table " + destination + " failed", cause);
    }

    private IOException wrapFailure(String message, Throwable cause) {
        if (isNotFound(cause) && config.getCreateDisposition() == CreateDisposition.CREATE_NEVER) {
            message += " because the table does not exist and createDisposition is CREATE_NEVER";
        }
        return new IOException(message, cause);
    }

    private static void sleep(long millis) throws IOException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for a BigQuery table to appear", e);
        }
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

    /** An unacknowledged append batch, retained so failed batches can be re-appended. */
    private static final class InFlightBatch {
        private final TableDestination destination;
        private final ProtoRows rows;

        InFlightBatch(TableDestination destination, ProtoRows rows) {
            this.destination = destination;
            this.rows = rows;
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
