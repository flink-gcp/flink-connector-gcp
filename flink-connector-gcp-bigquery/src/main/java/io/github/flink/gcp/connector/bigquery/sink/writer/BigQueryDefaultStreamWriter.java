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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.FailedRow;
import io.github.flink.gcp.connector.bigquery.sink.FailedRowHandler;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
 * append failures are additionally captured by completion callbacks and handled on the next {@link
 * #write} or {@link #flush} call.
 *
 * <p>The writer is <em>stateless</em>: it stores nothing in Flink state, so discarding operator
 * state can never lose sink-buffered data (the {@code AsyncSinkWriter}-style alternative of
 * persisting unflushed buffers into writer state was deliberately rejected for exactly that failure
 * mode). Checkpointing must be enabled for the at-least-once guarantee in streaming jobs; without
 * it {@code flush()} is only invoked at end of input.
 *
 * <p>Append failures are routed by {@link AppendErrorClassifier} on the task thread. Transient
 * failures that surface past the SDK's own in-stream retries are re-appended on a rebuilt stream
 * writer with backoff within a bounded retry budget; exhausting the budget is terminal. Row-level
 * failures (rows rejected with per-row error details) are routed row by row to the configured
 * {@link FailedRowHandler} — which fails the job, drops the row, or forwards it to a dead-letter
 * queue — and the surviving rows of the batch are re-appended. Everything else (for example {@code
 * INVALID_ARGUMENT} or {@code PERMISSION_DENIED}) is terminal and fails the ongoing write or
 * checkpoint. In-flight batches are retained together with their destination until acknowledged so
 * they can be re-appended (this also is the groundwork for schema-evolution rebuilds, #12).
 *
 * <p>Under {@link CreateDisposition#CREATE_IF_NEEDED}, appends failing with {@code NOT_FOUND} are
 * recovered on the task thread: the destination table is created via the {@link TableCreator}
 * (schema from the serializer, partitioning/clustering from the configured options provider), the
 * destination's stream writer is rebuilt, and the failed batch is re-appended with backoff while
 * table metadata propagates to the Storage Write API backend. Under {@link
 * CreateDisposition#CREATE_NEVER}, {@code NOT_FOUND} fails the write or checkpoint immediately.
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
     * Retry schedule for re-appends on the task thread, shared by {@code NOT_FOUND} recovery after
     * creating a table (metadata propagation to the Storage Write API backend is usually seconds
     * but can take considerably longer) and by transient append failures that surfaced past the
     * SDK's own retries. The defaults (500 ms initial, doubled up to 10 s, 10 attempts) allow
     * roughly a minute in total.
     */
    static final long DEFAULT_RECOVERY_INITIAL_BACKOFF_MS = 500;

    static final long DEFAULT_RECOVERY_MAX_BACKOFF_MS = 10_000;

    static final int DEFAULT_RECOVERY_MAX_ATTEMPTS = 10;

    private final BigQuerySinkConfig<T> config;
    private final RowAppenderFactory appenderFactory;
    private final TableCreator tableCreator;
    private final FailedRowHandler failedRowHandler;
    private final long maxAppendRequestBytes;
    private final long recoveryInitialBackoffMs;
    private final long recoveryMaxBackoffMs;
    private final int recoveryMaxAttempts;

    /** Accessed only from the task thread. */
    private final Map<TableDestination, DestinationState> states = new HashMap<>();

    /** Completed entries are removed by gRPC callback threads (except repairable failures). */
    private final Map<ApiFuture<AppendRowsResponse>, InFlightBatch> inFlight =
            new ConcurrentHashMap<>();

    private final AtomicReference<Throwable> asyncError = new AtomicReference<>();

    /**
     * Set by completion callbacks when an append failed in a way the task thread can repair
     * (recoverable {@code NOT_FOUND}, transient failures, row-level failures); the task thread then
     * sweeps {@link #inFlight} for failed batches and repairs them.
     */
    private final AtomicBoolean repairNeeded = new AtomicBoolean();

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
        this.failedRowHandler = config.getFailedRowHandler();
        this.maxAppendRequestBytes = maxAppendRequestBytes;
        this.recoveryInitialBackoffMs = recoveryInitialBackoffMs;
        this.recoveryMaxBackoffMs = recoveryMaxBackoffMs;
        this.recoveryMaxAttempts = recoveryMaxAttempts;
    }

    @Override
    public void write(T element, Context context) throws IOException {
        checkAsyncError();
        // Plain volatile read on the per-record fast path; the atomic clear runs only when a
        // repair is actually pending.
        if (repairNeeded.get() && repairNeeded.getAndSet(false)) {
            repairFailedInFlight();
        }
        TableDestination destination = config.getDestinationResolver().resolve(element, context);
        ByteString row;
        try {
            row = config.getSerializer().serialize(element);
        } catch (IOException e) {
            failedRowHandler.handle(
                    FailedRow.of(
                            destination,
                            null,
                            "Failed to serialize a record for " + destination + ": " + e,
                            e));
            return;
        }
        if (row.size() > MAX_ROW_BYTES) {
            failedRowHandler.handle(
                    FailedRow.of(
                            destination,
                            row,
                            "A row for "
                                    + destination
                                    + " is "
                                    + row.size()
                                    + " bytes, exceeding the "
                                    + MAX_ROW_BYTES
                                    + "-byte per-row limit of the BigQuery Storage Write API",
                            null));
            return;
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
            Throwable failure;
            try {
                failure = responseToThrowable(entry.getValue().destination, entry.getKey().get());
            } catch (ExecutionException e) {
                failure = e.getCause();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while flushing appends to BigQuery", e);
            }
            if (failure != null) {
                handleFailedAppend(entry.getKey(), failure);
            }
        }
        checkAsyncError();
    }

    @Override
    public void close() throws Exception {
        List<AutoCloseable> closeables = new ArrayList<>(states.size() + 1);
        for (DestinationState state : states.values()) {
            closeables.add(state.appender);
        }
        states.clear();
        closeables.add(failedRowHandler::close);
        IOUtils.closeAll(closeables);
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
            recoverDestination(destination, Collections.emptyList());
            return states.get(destination);
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
                        Throwable failure = responseToThrowable(destination, response);
                        if (failure == null) {
                            inFlight.remove(future);
                        } else {
                            park(failure);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        park(t);
                    }

                    /**
                     * Leaves repairable failures in {@link #inFlight} for the task thread to repair
                     * on the next {@code write()} or {@code flush()}; terminal ones are captured
                     * immediately.
                     */
                    private void park(Throwable failure) {
                        if (isTaskThreadRepairable(failure)) {
                            repairNeeded.set(true);
                        } else {
                            asyncError.compareAndSet(
                                    null,
                                    failure instanceof IOException
                                            ? failure
                                            : wrapAppendFailure(destination, failure));
                            inFlight.remove(future);
                        }
                    }
                },
                Runnable::run);
    }

    /** Whether the failure is one the task thread repairs instead of failing the job outright. */
    private boolean isTaskThreadRepairable(Throwable t) {
        return isRecoverableNotFound(t)
                || AppendErrorClassifier.classify(t) != AppendErrorClassifier.Kind.TERMINAL;
    }

    /**
     * Sweeps {@link #inFlight} for batches whose append failed and repairs them. Called on the task
     * thread between checkpoints; {@link #flush(boolean)} reaches the same repair through its own
     * response inspection.
     */
    private void repairFailedInFlight() throws IOException {
        for (Map.Entry<ApiFuture<AppendRowsResponse>, InFlightBatch> entry : inFlight.entrySet()) {
            if (!entry.getKey().isDone()) {
                continue;
            }
            Throwable failure;
            try {
                failure = responseToThrowable(entry.getValue().destination, entry.getKey().get());
            } catch (ExecutionException e) {
                // Successful completions are owned by the callbacks; repairable failures are
                // left in the map for exactly this sweep, and a terminal failure whose callback
                // has not run yet is surfaced here directly.
                failure = e.getCause();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while recovering appends to BigQuery", e);
            }
            if (failure != null) {
                handleFailedAppend(entry.getKey(), failure);
            }
        }
    }

    /**
     * Handles a completed-with-failure append on the task thread: recoverable {@code NOT_FOUND}
     * batches are re-appended after creating the table, transient failures are re-appended within
     * the retry budget, row-level failures are routed to the {@link FailedRowHandler} (surviving
     * rows are re-appended), and anything else fails the writer. The {@link #inFlight} removal
     * arbitrates ownership against the completion callbacks.
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
            List<ProtoRows> batches = new ArrayList<>();
            batches.add(batch.rows);
            collectFailedSiblings(batch.destination, batches);
            recoverDestination(batch.destination, batches);
            return;
        }
        Optional<Exceptions.AppendSerializtionError> rowLevel =
                AppendErrorClassifier.findRowLevel(cause);
        if (rowLevel.isPresent()) {
            List<ProtoRows> batches = new ArrayList<>();
            ProtoRows survivors = routeRowLevel(batch.destination, batch.rows, rowLevel.get());
            if (survivors.getSerializedRowsCount() > 0) {
                batches.add(survivors);
            }
            collectFailedSiblings(batch.destination, batches);
            retryBatches(batch.destination, batches, false);
            return;
        }
        if (AppendErrorClassifier.classify(cause) == AppendErrorClassifier.Kind.TRANSIENT) {
            LOG.info(
                    "An append to {} failed transiently past the SDK retries, re-appending"
                            + " (cause: {})",
                    batch.destination,
                    cause.toString());
            List<ProtoRows> batches = new ArrayList<>();
            batches.add(batch.rows);
            collectFailedSiblings(batch.destination, batches);
            retryBatches(batch.destination, batches, false);
            return;
        }
        throw terminalFailure(batch.destination, cause);
    }

    /**
     * Awaits every other in-flight append of the destination and collects those that failed in a
     * repairable way. Repair tears the destination's appender down; awaiting the siblings first
     * guarantees no live append is cancelled by that close, and grouping the failed batches lets
     * them share one rebuilt appender. Row-level sibling failures are routed to the handler here,
     * with only the surviving rows collected; terminal sibling failures fail the writer.
     */
    private void collectFailedSiblings(TableDestination destination, List<ProtoRows> batches)
            throws IOException {
        for (Map.Entry<ApiFuture<AppendRowsResponse>, InFlightBatch> entry : inFlight.entrySet()) {
            if (!destination.equals(entry.getValue().destination)) {
                continue;
            }
            Throwable failure;
            try {
                failure = responseToThrowable(destination, entry.getKey().get());
            } catch (ExecutionException e) {
                failure = e.getCause();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while recovering appends to BigQuery", e);
            }
            if (failure == null) {
                continue;
            }
            InFlightBatch sibling = inFlight.remove(entry.getKey());
            if (sibling == null) {
                continue;
            }
            Optional<Exceptions.AppendSerializtionError> rowLevel =
                    AppendErrorClassifier.findRowLevel(failure);
            if (rowLevel.isPresent()) {
                ProtoRows survivors = routeRowLevel(destination, sibling.rows, rowLevel.get());
                if (survivors.getSerializedRowsCount() > 0) {
                    batches.add(survivors);
                }
            } else if (isRecoverableNotFound(failure)
                    || AppendErrorClassifier.classify(failure)
                            == AppendErrorClassifier.Kind.TRANSIENT) {
                batches.add(sibling.rows);
            } else {
                throw terminalFailure(destination, failure);
            }
        }
    }

    /**
     * Routes a row-level append failure to the {@link FailedRowHandler} row by row and returns the
     * surviving rows. The handler decides per row: returning normally drops the row, throwing fails
     * the writer.
     */
    private ProtoRows routeRowLevel(
            TableDestination destination,
            ProtoRows rows,
            Exceptions.AppendSerializtionError rowLevel)
            throws IOException {
        Map<Integer, String> rowErrors = rowLevel.getRowIndexToErrorMessage();
        ProtoRows.Builder survivors = ProtoRows.newBuilder();
        for (int i = 0; i < rows.getSerializedRowsCount(); i++) {
            String errorMessage = rowErrors.get(i);
            if (errorMessage == null) {
                survivors.addSerializedRows(rows.getSerializedRows(i));
            } else {
                failedRowHandler.handle(
                        FailedRow.of(
                                destination, rows.getSerializedRows(i), errorMessage, rowLevel));
            }
        }
        return survivors.build();
    }

    /**
     * Creates the destination table (idempotent across parallel subtasks) and re-appends the given
     * failed batches while table metadata propagates to the Storage Write API backend. With no
     * batches this reduces to rebuilding the appender of a just-created table.
     */
    private void recoverDestination(TableDestination destination, List<ProtoRows> batches)
            throws IOException {
        createTable(destination);
        retryBatches(destination, batches, true);
    }

    private void createTable(TableDestination destination) throws IOException {
        tableCreator.create(
                destination,
                config.getSerializer().getTableSchema(destination),
                config.getTableCreateOptionsProvider().optionsFor(destination));
    }

    /**
     * Re-appends the given batches on a rebuilt appender, retrying with backoff within the retry
     * budget as long as failures stay repairable: {@code NOT_FOUND} while table metadata has not
     * propagated yet (creating the table first if it has not been created during this repair and
     * the disposition allows it), transient failures, and row-level failures (which are routed to
     * the {@link FailedRowHandler}, shrinking the batch to the surviving rows). Terminal failures
     * and retry-budget exhaustion fail the writer.
     *
     * @param destination the destination whose appender is rebuilt
     * @param batches the batches to re-append
     * @param tableCreated whether the destination table was just created for this repair
     */
    private void retryBatches(
            TableDestination destination, List<ProtoRows> batches, boolean tableCreated)
            throws IOException {
        List<ProtoRows> remaining = new ArrayList<>(batches);
        long backoffMs = recoveryInitialBackoffMs;
        for (int attempt = 1; ; attempt++) {
            DestinationState state = null;
            try {
                state = rebuildState(destination);
            } catch (IOException | RuntimeException e) {
                tableCreated = maybeCreateMissingTable(destination, e, tableCreated);
                if (!isRetriable(e, tableCreated) || attempt >= recoveryMaxAttempts) {
                    throw wrapFailure(
                            retryFailureMessage(
                                    "Failed to open a BigQuery write stream to " + destination,
                                    e,
                                    tableCreated,
                                    attempt),
                            e);
                }
            }
            boolean backOff = state == null;
            while (state != null && !remaining.isEmpty() && !backOff) {
                ProtoRows head = remaining.get(0);
                Throwable failure;
                try {
                    failure = responseToThrowable(destination, state.appender.append(head).get());
                } catch (ExecutionException e) {
                    failure = e.getCause();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Interrupted while re-appending to BigQuery table " + destination, e);
                }
                if (failure == null) {
                    remaining.remove(0);
                    continue;
                }
                Optional<Exceptions.AppendSerializtionError> rowLevel =
                        AppendErrorClassifier.findRowLevel(failure);
                if (rowLevel.isPresent()) {
                    // Shrink the batch to the surviving rows and stay in the same attempt; each
                    // pass drops at least one row, so this terminates.
                    remaining.remove(0);
                    ProtoRows survivors = routeRowLevel(destination, head, rowLevel.get());
                    if (survivors.getSerializedRowsCount() > 0) {
                        remaining.add(0, survivors);
                    }
                    continue;
                }
                tableCreated = maybeCreateMissingTable(destination, failure, tableCreated);
                if (!isRetriable(failure, tableCreated) || attempt >= recoveryMaxAttempts) {
                    throw wrapFailure(
                            retryFailureMessage(
                                    "A re-append to BigQuery table " + destination + " failed",
                                    failure,
                                    tableCreated,
                                    attempt),
                            failure);
                }
                backOff = true;
            }
            if (!backOff) {
                return;
            }
            LOG.info(
                    "Re-appending to BigQuery table {} is not possible yet"
                            + " (attempt {}/{}), backing off {} ms",
                    destination,
                    attempt,
                    recoveryMaxAttempts,
                    backoffMs);
            sleep(backoffMs);
            backoffMs = Math.min(backoffMs * 2, recoveryMaxBackoffMs);
        }
    }

    /**
     * Creates the destination table when a repair-time failure is a recoverable {@code NOT_FOUND}
     * and the table has not been created during this repair yet (a transient repair can discover a
     * missing table). Returns the updated created-flag.
     */
    private boolean maybeCreateMissingTable(
            TableDestination destination, Throwable failure, boolean tableCreated)
            throws IOException {
        if (!tableCreated && isRecoverableNotFound(failure)) {
            LOG.info(
                    "The table behind {} does not exist, creating it (CREATE_IF_NEEDED)",
                    destination);
            createTable(destination);
            return true;
        }
        return tableCreated;
    }

    /**
     * Whether a repair-time failure warrants another attempt: {@code NOT_FOUND} while created table
     * metadata propagates, or a transient failure.
     */
    private static boolean isRetriable(Throwable failure, boolean tableCreated) {
        return (tableCreated && isNotFound(failure))
                || AppendErrorClassifier.classify(failure) == AppendErrorClassifier.Kind.TRANSIENT;
    }

    private String retryFailureMessage(
            String base, Throwable failure, boolean tableCreated, int attempt) {
        StringBuilder message = new StringBuilder(base);
        if (tableCreated) {
            message.append(" after creating the table");
        }
        if (isRetriable(failure, tableCreated) && attempt >= recoveryMaxAttempts) {
            message.append(", the retry budget is exhausted");
        }
        return message.append(" (").append(attempt).append(" attempt(s))").toString();
    }

    /**
     * Replaces the destination's state with one backed by a fresh appender, carrying over any
     * buffered-but-not-yet-appended rows. The new state is created and registered before the old
     * one is torn down, so a failure at any point never orphans buffered rows.
     */
    private DestinationState rebuildState(TableDestination destination) throws IOException {
        DestinationState fresh = createState(destination);
        DestinationState old = states.put(destination, fresh);
        if (old != null) {
            if (old.pendingCount() > 0) {
                for (ByteString row : old.take().getSerializedRowsList()) {
                    fresh.add(row);
                }
            }
            old.appender.close();
        }
        return fresh;
    }

    private boolean isRecoverableNotFound(Throwable t) {
        return config.getCreateDisposition() == CreateDisposition.CREATE_IF_NEEDED && isNotFound(t);
    }

    /** Walks the cause chain for a gax or gRPC {@code NOT_FOUND}. */
    private static boolean isNotFound(Throwable t) {
        return ExceptionUtils.findThrowable(
                        t,
                        cause ->
                                (cause instanceof ApiException
                                                && ((ApiException) cause).getStatusCode().getCode()
                                                        == StatusCode.Code.NOT_FOUND)
                                        || (cause instanceof StatusRuntimeException
                                                && ((StatusRuntimeException) cause)
                                                                .getStatus()
                                                                .getCode()
                                                        == Status.Code.NOT_FOUND))
                .isPresent();
    }

    /**
     * Turns a terminal append failure into the {@link IOException} failing the writer. Failures
     * synthesized from an errored response already carry the full message and are thrown as-is.
     */
    private IOException terminalFailure(TableDestination destination, Throwable cause) {
        if (cause instanceof IOException) {
            return (IOException) cause;
        }
        return wrapAppendFailure(destination, cause);
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
            throw new IOException("Interrupted while waiting to retry appends to BigQuery", e);
        }
    }

    /**
     * Maps a completed append response to the failure it carries, or {@code null} for a clean
     * response. Row errors become a synthesized row-level error (the same shape the SDK raises), an
     * error with a transient status code becomes a synthesized {@link StatusRuntimeException} so
     * classification treats it uniformly, and any other error is terminal.
     */
    private static Throwable responseToThrowable(
            TableDestination destination, AppendRowsResponse response) {
        if (response.getRowErrorsCount() > 0) {
            Map<Integer, String> rowErrors = new HashMap<>();
            for (RowError rowError : response.getRowErrorsList()) {
                rowErrors.put((int) rowError.getIndex(), rowError.getMessage());
            }
            return new Exceptions.AppendSerializtionError(
                    Status.Code.INVALID_ARGUMENT.value(),
                    "An append to BigQuery table "
                            + destination
                            + " completed with "
                            + response.getRowErrorsCount()
                            + " row error(s)",
                    response.getWriteStream(),
                    rowErrors);
        }
        if (response.hasError()) {
            if (AppendErrorClassifier.isTransientCode(response.getError().getCode())) {
                return Status.fromCodeValue(response.getError().getCode())
                        .withDescription(response.getError().getMessage())
                        .asRuntimeException();
            }
            return new IOException(
                    "An append to BigQuery table "
                            + destination
                            + " completed with an error: "
                            + response.getError().getMessage());
        }
        return null;
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
