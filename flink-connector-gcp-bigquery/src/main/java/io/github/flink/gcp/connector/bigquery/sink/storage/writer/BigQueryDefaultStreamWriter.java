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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRow;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRowHandler;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.tables.SchemaUnifier;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableSchemaSnapshot;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * At-least-once {@link SinkWriter} appending to Storage Write API default streams with dynamic
 * per-record table destinations.
 *
 * <p>Per destination, rows are buffered into append batches (bounded by {@code
 * DefaultStreamOptions#maxAppendRequestBytes}, default {@link #DEFAULT_MAX_APPEND_REQUEST_BYTES})
 * and appended asynchronously; backpressure is provided by the stream writer's own in-flight
 * limits. {@link #flush(boolean)} appends all pending batches and awaits every in-flight append,
 * inspecting each response directly, so records never pass a checkpoint barrier unacknowledged —
 * this is what makes the sink at-least-once. Asynchronous append failures are additionally captured
 * by completion callbacks and handled on the next {@link #write} or {@link #flush} call.
 *
 * <p>The writer is <em>stateless</em>: it stores nothing in Flink state, so discarding operator
 * state can never lose sink-buffered data (the {@code AsyncSinkWriter}-style alternative of
 * persisting unflushed buffers into writer state was deliberately rejected for exactly that failure
 * mode). Checkpointing must be enabled for the at-least-once guarantee in streaming jobs; without
 * it {@code flush()} is only invoked at end of input.
 *
 * <p>Append failures are routed by {@link AppendErrorClassifier} on the task thread. Transient
 * failures that surface past the SDK's own in-stream retries — and failures reporting the stream
 * writer itself as stale (finalized, unknown, closed) — are re-appended on a rebuilt stream writer
 * with backoff within a bounded retry budget; exhausting the budget is terminal. Row-level failures
 * (rows rejected with per-row error details) are routed row by row to the configured {@link
 * FailedRowHandler} — which fails the job, drops the row, or forwards it to a dead-letter queue —
 * and the surviving rows of the batch are re-appended. Everything else (for example {@code
 * INVALID_ARGUMENT} or {@code PERMISSION_DENIED}) is terminal and fails the ongoing write or
 * checkpoint. In-flight batches are retained together with their destination until acknowledged so
 * they can be re-appended.
 *
 * <p>Under {@link CreateDisposition#CREATE_IF_NEEDED}, appends failing with {@code NOT_FOUND} are
 * recovered on the task thread: the destination table is created via the {@link TableAdmin} (schema
 * from the serializer, partitioning/clustering from the configured options provider), the
 * destination's stream writer is rebuilt, and the failed batch is re-appended with backoff while
 * table metadata propagates to the Storage Write API backend. Under {@link
 * CreateDisposition#CREATE_NEVER}, {@code NOT_FOUND} fails the write or checkpoint immediately.
 *
 * <h2>Schema evolution</h2>
 *
 * <p>Schema changes are handled without a job restart, on three paths that all converge on
 * rebuilding the destination's stream writer with a fresh serializer descriptor:
 *
 * <ul>
 *   <li><em>Server-pushed schema updates:</em> when an append response carries {@code
 *       updated_schema} (the table's schema changed, for example through DDL), the destination's
 *       writer is rebuilt on the task thread — a raw {@code StreamWriter} never refreshes its
 *       schema by itself, also not under connection-pool multiplexing.
 *   <li><em>Serializer schema changes:</em> when {@code getSchemaFingerprint} reports a change, the
 *       writer is rebuilt <em>before</em> rows serialized under the new schema are appended — and,
 *       when schema updates are enabled, the destination table's schema is reconciled first (fresh
 *       read, union with the serializer schema, etag-conditioned update; concurrent subtasks
 *       converge because unions are additive and lost races re-read), so the first append under the
 *       new schema does not have to fail.
 *   <li><em>Schema-mismatch append failures:</em> with schema updates enabled, the table schema is
 *       reconciled the same way and the failed batches are re-appended within a long jittered retry
 *       budget while the update propagates to the Storage Write API backend (typically minutes; the
 *       budget allows roughly fifteen — a schema repair can therefore block a checkpoint longer
 *       than Flink's default checkpoint timeout of ten minutes, which may need raising). With
 *       updates disabled, schema mismatches stay terminal.
 * </ul>
 *
 * <p>Retained batches are serialized bytes and are never re-encoded; rebuilding writers relies on
 * the serializer evolving additively (see {@code BigQueryProtoSerializer#getSchemaFingerprint}),
 * which keeps previously serialized bytes valid under the new descriptor.
 *
 * <p>(The schema-evolution mechanics — update-on-error with a bounded jittered wait for schema
 * propagation, the proactive local pre-check, and the coordinator-free concurrent updates — are
 * independent reimplementations informed by the design of the Aiven/kafka-connect-bigquery
 * connector; see the module README's provenance section.)
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
     * but can take considerably longer), transient append failures that surfaced past the SDK's own
     * retries, and stale-stream-writer refreshes. The defaults (500 ms initial, doubled up to 10 s,
     * 10 attempts) allow roughly a minute in total.
     */
    static final RetrySchedule DEFAULT_RECOVERY_SCHEDULE = new RetrySchedule(500, 10_000, 10, 0);

    /**
     * Retry schedule for re-appends after a table schema update, while the update propagates to the
     * Storage Write API backend — which takes minutes, considerably longer than table-creation
     * propagation. Flat 30 s waits with ±25% jitter (de-synchronizing parallel subtasks), 30
     * attempts: a ceiling of roughly fifteen minutes.
     */
    static final RetrySchedule DEFAULT_SCHEMA_WAIT_SCHEDULE =
            new RetrySchedule(30_000, 30_000, 30, 0.25);

    /**
     * Attempts at applying a schema update before giving up; each attempt is a fresh read, union
     * and etag-conditioned update, so only concurrent updates (or the per-table metadata quota)
     * consume attempts. Concurrent unions converge, so a handful suffices.
     */
    static final int SCHEMA_UPDATE_MAX_ATTEMPTS = 5;

    /**
     * Upper bound of the random sleep before reading and updating a table's schema, spreading
     * parallel subtasks that discovered the same schema change at the same time across the
     * per-table metadata-update quota (about five updates per ten seconds).
     */
    static final long SCHEMA_UPDATE_MAX_JITTER_MS = 500;

    private final BigQuerySinkConfig<T> config;
    private final RowAppenderFactory appenderFactory;
    private final TableAdmin tableAdmin;
    private final FailedRowHandler failedRowHandler;
    private final long maxAppendRequestBytes;
    private final RetrySchedule recoverySchedule;
    private final RetrySchedule schemaWaitSchedule;

    /** Accessed only from the task thread. */
    private final Map<TableDestination, DestinationState> states = new HashMap<>();

    /** Completed entries are removed by gRPC callback threads (except repairable failures). */
    private final Map<ApiFuture<AppendRowsResponse>, InFlightBatch> inFlight =
            new ConcurrentHashMap<>();

    private final AtomicReference<Throwable> asyncError = new AtomicReference<>();

    /**
     * Set by completion callbacks when an append failed in a way the task thread can repair
     * (recoverable {@code NOT_FOUND}, schema mismatches, transient failures, row-level failures);
     * the task thread then sweeps {@link #inFlight} for failed batches and repairs them.
     */
    private final AtomicBoolean repairNeeded = new AtomicBoolean();

    /**
     * Destinations whose append responses carried {@code updated_schema}, recorded by completion
     * callbacks and drained on the task thread, which refreshes their stream writers (the rebuilt
     * writer's schema always comes from the serializer, so destinations whose serializer
     * fingerprint is unchanged are skipped).
     */
    private final Set<TableDestination> pushedSchemaRefreshes = ConcurrentHashMap.newKeySet();

    /**
     * Creates a writer with default options.
     *
     * @param config the sink configuration
     * @param appenderFactory the appender factory
     * @param tableAdmin the admin for creating and updating destination tables
     */
    public BigQueryDefaultStreamWriter(
            BigQuerySinkConfig<T> config,
            RowAppenderFactory appenderFactory,
            TableAdmin tableAdmin) {
        this(
                config,
                appenderFactory,
                tableAdmin,
                DEFAULT_MAX_APPEND_REQUEST_BYTES,
                DEFAULT_RECOVERY_SCHEDULE,
                DEFAULT_SCHEMA_WAIT_SCHEDULE);
    }

    /**
     * Creates a writer, taking the batching cap and the connector-driven recovery schedule from the
     * given options (same mapping as the buffered-stream writer; the schedule is jitter-free,
     * matching {@link #DEFAULT_RECOVERY_SCHEDULE}). The schema-wait schedule is not configurable —
     * it paces BigQuery metadata propagation, a service property rather than a workload property.
     *
     * @param config the sink configuration
     * @param appenderFactory the appender factory
     * @param tableAdmin the admin for creating and updating destination tables
     * @param options the default-stream options
     */
    public BigQueryDefaultStreamWriter(
            BigQuerySinkConfig<T> config,
            RowAppenderFactory appenderFactory,
            TableAdmin tableAdmin,
            DefaultStreamOptions options) {
        this(
                config,
                appenderFactory,
                tableAdmin,
                options.getMaxAppendRequestBytes(),
                new RetrySchedule(
                        options.getRetryInitialBackoff().toMillis(),
                        options.getRetryMaxBackoff().toMillis(),
                        options.getRetryMaxAttempts(),
                        0),
                DEFAULT_SCHEMA_WAIT_SCHEDULE);
    }

    BigQueryDefaultStreamWriter(
            BigQuerySinkConfig<T> config,
            RowAppenderFactory appenderFactory,
            TableAdmin tableAdmin,
            long maxAppendRequestBytes,
            RetrySchedule recoverySchedule,
            RetrySchedule schemaWaitSchedule) {
        this.config = Preconditions.checkNotNull(config, "config must not be null");
        this.appenderFactory =
                Preconditions.checkNotNull(appenderFactory, "appenderFactory must not be null");
        this.tableAdmin = Preconditions.checkNotNull(tableAdmin, "tableAdmin must not be null");
        this.failedRowHandler = config.getFailedRowHandler();
        this.maxAppendRequestBytes = maxAppendRequestBytes;
        this.recoverySchedule =
                Preconditions.checkNotNull(recoverySchedule, "recoverySchedule must not be null");
        this.schemaWaitSchedule =
                Preconditions.checkNotNull(
                        schemaWaitSchedule, "schemaWaitSchedule must not be null");
    }

    @Override
    public void write(T element, Context context) throws IOException {
        checkAsyncError();
        // Plain volatile reads on the per-record fast path; the atomic clears run only when a
        // repair or refresh is actually pending.
        if (repairNeeded.get() && repairNeeded.getAndSet(false)) {
            repairFailedInFlight();
            // A terminal failure may have been captured by a completion callback while the
            // repair awaited its futures; surface it before accepting the record.
            checkAsyncError();
        }
        if (!pushedSchemaRefreshes.isEmpty()) {
            refreshPushedSchemas();
            checkAsyncError();
        }
        TableDestination destination = config.getDestinationResolver().resolve(element, context);
        ByteString row;
        try {
            // Serialized before any per-destination state exists: a poison record must reach the
            // handler no matter how the serializer fails, without opening a write stream (or
            // auto-creating a table) for a destination that may never receive a row. Resolver
            // failures, by contrast, are configuration errors and propagate.
            row = config.getSerializer().serialize(element);
        } catch (IOException | RuntimeException e) {
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
        // The fingerprint check runs after serialize(), so even a serializer that advances its
        // schema while serializing this very record gets the stream refreshed before the record's
        // bytes are appended.
        state = refreshOnFingerprintChange(destination, state);
        if (state.pendingCount() > 0 && state.pendingBytes + row.size() > maxAppendRequestBytes) {
            appendPending(destination, state);
        }
        state.add(row);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        checkAsyncError();
        if (!pushedSchemaRefreshes.isEmpty()) {
            refreshPushedSchemas();
            checkAsyncError();
        }
        for (Map.Entry<TableDestination, DestinationState> entry : states.entrySet()) {
            if (entry.getValue().pendingCount() > 0) {
                appendPending(entry.getKey(), entry.getValue());
            }
        }
        // Inspect every in-flight response directly: waiters can be released before completion
        // callbacks have run, so relying on the callbacks alone could let a checkpoint succeed
        // ahead of a captured failure.
        for (Map.Entry<ApiFuture<AppendRowsResponse>, InFlightBatch> entry : inFlight.entrySet()) {
            Throwable failure =
                    awaitFailure(
                            entry.getValue().destination,
                            entry.getKey(),
                            "Interrupted while flushing appends to BigQuery");
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
        // The fingerprint is captured before the descriptor: if the serializer schema evolves
        // between the two calls, the stale fingerprint triggers a redundant-but-harmless refresh
        // on the next record, whereas the opposite order could miss a change.
        Object fingerprint = config.getSerializer().getSchemaFingerprint(destination);
        RowAppender appender =
                appenderFactory.create(
                        destination,
                        config.getSerializer().getDescriptor(destination),
                        config.getLocation());
        return new DestinationState(appender, fingerprint);
    }

    /**
     * Rebuilds the destination's stream writer when the serializer reports a schema fingerprint
     * different from the one its current writer was built with — <em>before</em> any row serialized
     * under the changed schema is appended, so the first append does not have to fail. When schema
     * updates are enabled, the destination table's schema is reconciled first, proactively.
     */
    private DestinationState refreshOnFingerprintChange(
            TableDestination destination, DestinationState state) throws IOException {
        if (state.schemaFingerprint == null) {
            // A null fingerprint at stream-open time means the schema never changes; skip the
            // per-record serializer call entirely.
            return state;
        }
        Object fingerprint = config.getSerializer().getSchemaFingerprint(destination);
        if (Objects.equals(fingerprint, state.schemaFingerprint)) {
            return state;
        }
        LOG.info("The serializer schema for {} changed, refreshing the write stream", destination);
        boolean reconciled =
                config.getSchemaUpdateOptions().isEnabled() && reconcileSchema(destination);
        refreshDestination(destination, reconciled);
        return states.get(destination);
    }

    /**
     * Refreshes the stream writers of destinations for which append responses carried {@code
     * updated_schema}: the table's schema changed, and a stream writer never picks that up by
     * itself. The rebuilt writer's schema comes from the serializer, so destinations whose
     * serializer fingerprint is unchanged (including static-schema serializers) are skipped — a
     * rebuild would install the identical schema and only churn the stream. Called on the task
     * thread.
     */
    private void refreshPushedSchemas() throws IOException {
        for (TableDestination destination : new ArrayList<>(pushedSchemaRefreshes)) {
            pushedSchemaRefreshes.remove(destination);
            DestinationState state = states.get(destination);
            if (state == null) {
                continue;
            }
            Object fingerprint = config.getSerializer().getSchemaFingerprint(destination);
            if (Objects.equals(fingerprint, state.schemaFingerprint)) {
                LOG.debug(
                        "BigQuery reported an updated schema for {} but the serializer schema is"
                                + " unchanged, not refreshing the write stream",
                        destination);
                continue;
            }
            LOG.info(
                    "BigQuery reported an updated schema for {}, refreshing the write stream",
                    destination);
            refreshDestination(destination, false);
        }
    }

    /**
     * Replaces the destination's stream writer with one built from the serializer's current schema.
     * In-flight appends are awaited first (the rebuild would cancel them); those that failed
     * repairably are re-appended on the rebuilt writer, and the rebuild itself runs under {@link
     * #retryBatches}' guarded budget so a table missing or still propagating is recovered rather
     * than surfacing as a raw failure.
     *
     * @param destination the destination to refresh
     * @param schemaUpdated whether the table schema was just reconciled for this refresh
     */
    private void refreshDestination(TableDestination destination, boolean schemaUpdated)
            throws IOException {
        List<ProtoRows> failedBatches = new ArrayList<>();
        collectFailedSiblings(destination, failedBatches);
        retryBatches(
                destination,
                failedBatches,
                false,
                schemaUpdated,
                schemaUpdated ? schemaWaitSchedule : recoverySchedule);
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
                            if (response.hasUpdatedSchema()) {
                                pushedSchemaRefreshes.add(destination);
                            }
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
                        if (repairActionFor(failure) != RepairAction.FAIL) {
                            repairNeeded.set(true);
                        } else {
                            asyncError.compareAndSet(null, terminalFailure(destination, failure));
                            inFlight.remove(future);
                        }
                    }
                },
                Runnable::run);
    }

    /** How a failed append is repaired (or not) on the task thread. */
    private enum RepairAction {
        /** Recoverable {@code NOT_FOUND}: create the table, then re-append. */
        CREATE_TABLE,
        /** Schema mismatch with updates enabled: reconcile the table schema, then re-append. */
        UPDATE_SCHEMA,
        /** Row-level failure: route rows to the handler, re-append the survivors. */
        ROUTE_ROWS,
        /** Transient or stale-writer failure: re-append within the retry budget. */
        RETRY,
        /** Terminal failure: fail the writer. */
        FAIL
    }

    /**
     * The single authority for repair routing; every failure-handling site dispatches on this so
     * the callback-thread park decision and the task-thread repairs can never diverge.
     */
    private RepairAction repairActionFor(Throwable cause) {
        if (isRecoverableNotFound(cause)) {
            return RepairAction.CREATE_TABLE;
        }
        if (AppendErrorClassifier.isSchemaMismatch(cause)
                && config.getSchemaUpdateOptions().isEnabled()) {
            return RepairAction.UPDATE_SCHEMA;
        }
        if (AppendErrorClassifier.findRowLevel(cause).isPresent()) {
            return RepairAction.ROUTE_ROWS;
        }
        if (AppendErrorClassifier.classify(cause) == AppendErrorClassifier.Kind.TRANSIENT
                || AppendErrorClassifier.requiresWriterRefresh(cause)) {
            return RepairAction.RETRY;
        }
        return RepairAction.FAIL;
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
            // Successful completions are owned by the callbacks; repairable failures are left
            // in the map for exactly this sweep, and a terminal failure whose callback has not
            // run yet is surfaced here directly.
            Throwable failure =
                    awaitFailure(
                            entry.getValue().destination,
                            entry.getKey(),
                            "Interrupted while recovering appends to BigQuery");
            if (failure != null) {
                handleFailedAppend(entry.getKey(), failure);
            }
        }
    }

    /**
     * Handles a completed-with-failure append on the task thread: recoverable {@code NOT_FOUND}
     * batches are re-appended after creating the table, schema mismatches are re-appended after
     * reconciling the table schema (when updates are enabled), transient and stale-writer failures
     * are re-appended within the retry budget, row-level failures are routed to the {@link
     * FailedRowHandler} (surviving rows are re-appended), and anything else fails the writer. The
     * {@link #inFlight} removal arbitrates ownership against the completion callbacks.
     */
    private void handleFailedAppend(ApiFuture<AppendRowsResponse> future, Throwable cause)
            throws IOException {
        InFlightBatch batch = inFlight.remove(future);
        if (batch == null) {
            // The completion callback owned this failure; it is surfaced via checkAsyncError.
            return;
        }
        RepairAction action = repairActionFor(cause);
        if (action == RepairAction.FAIL) {
            throw terminalFailure(batch.destination, cause);
        }
        List<ProtoRows> batches = new ArrayList<>();
        if (action == RepairAction.ROUTE_ROWS) {
            ProtoRows survivors =
                    routeRowLevel(
                            batch.destination,
                            batch.rows,
                            AppendErrorClassifier.findRowLevel(cause).get());
            if (survivors.getSerializedRowsCount() > 0) {
                batches.add(survivors);
            }
        } else {
            logRepair(action, batch.destination, cause);
            batches.add(batch.rows);
        }
        collectFailedSiblings(batch.destination, batches);
        switch (action) {
            case CREATE_TABLE:
                recoverDestination(batch.destination, batches);
                break;
            case UPDATE_SCHEMA:
                reconcileSchema(batch.destination);
                retryBatches(batch.destination, batches, false, true, schemaWaitSchedule);
                break;
            default:
                retryBatches(batch.destination, batches, false, false, recoverySchedule);
                break;
        }
    }

    private static void logRepair(
            RepairAction action, TableDestination destination, Throwable cause) {
        String reason;
        switch (action) {
            case CREATE_TABLE:
                reason =
                        "An append to {} failed because the table does not exist, creating it"
                                + " (CREATE_IF_NEEDED)";
                break;
            case UPDATE_SCHEMA:
                reason =
                        "An append to {} failed with a schema mismatch, reconciling the table"
                                + " schema (cause: {})";
                break;
            default:
                reason =
                        "An append to {} failed transiently past the SDK retries, re-appending"
                                + " (cause: {})";
                break;
        }
        LOG.info(reason, destination, cause.toString());
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
            Throwable failure =
                    awaitFailure(
                            destination,
                            entry.getKey(),
                            "Interrupted while recovering appends to BigQuery");
            if (failure == null) {
                continue;
            }
            InFlightBatch sibling = inFlight.remove(entry.getKey());
            if (sibling == null) {
                continue;
            }
            switch (repairActionFor(failure)) {
                case ROUTE_ROWS:
                    ProtoRows survivors =
                            routeRowLevel(
                                    destination,
                                    sibling.rows,
                                    AppendErrorClassifier.findRowLevel(failure).get());
                    if (survivors.getSerializedRowsCount() > 0) {
                        batches.add(survivors);
                    }
                    break;
                case FAIL:
                    throw terminalFailure(destination, failure);
                default:
                    batches.add(sibling.rows);
                    break;
            }
        }
    }

    /**
     * Awaits an append future and returns the failure it completed with — unwrapped from the
     * execution exception, or derived from the response — or {@code null} on success.
     */
    private Throwable awaitFailure(
            TableDestination destination,
            ApiFuture<AppendRowsResponse> future,
            String interruptMessage)
            throws IOException {
        try {
            return responseToThrowable(destination, future.get());
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(interruptMessage, e);
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
        retryBatches(destination, batches, true, false, recoverySchedule);
    }

    private void createTable(TableDestination destination) throws IOException {
        tableAdmin.create(
                destination,
                config.getSerializer().getTableSchema(destination),
                config.getTableCreateOptionsProvider().optionsFor(destination));
    }

    /**
     * Reconciles the destination table's schema with the serializer's: fresh read of the live
     * schema, union with the serializer schema under the configured {@code SchemaUpdateOptions},
     * and — only when the union differs — an etag-conditioned update. Lost races (a parallel
     * subtask updated the table concurrently, or the per-table metadata-update quota was exceeded)
     * re-read and re-union after a jittered sleep; because unions are additive, concurrent
     * reconciliations converge and usually end in the no-change short-circuit, so no up-front
     * jitter is needed — the etag makes optimistic first attempts safe. An impermissible union
     * (dropped field, changed type, gated change) throws and is terminal.
     *
     * @param destination the destination to reconcile
     * @return whether the table was changed (schema updated, or created after disappearing)
     */
    private boolean reconcileSchema(TableDestination destination) throws IOException {
        TableSchema desired = config.getSerializer().getTableSchema(destination);
        for (int attempt = 1; attempt <= SCHEMA_UPDATE_MAX_ATTEMPTS; attempt++) {
            TableSchemaSnapshot live = tableAdmin.getSchema(destination);
            if (live == null) {
                // Degenerate: the table has meanwhile disappeared. Creation applies the full
                // serializer schema, so there is nothing left to reconcile — but it stays gated
                // by the disposition like every other create path.
                if (config.getCreateDisposition() != CreateDisposition.CREATE_IF_NEEDED) {
                    throw new IOException(
                            "Cannot update the schema of BigQuery table "
                                    + destination
                                    + " because the table does not exist and createDisposition"
                                    + " is CREATE_NEVER");
                }
                LOG.info(
                        "The table behind {} does not exist, creating it instead of updating its"
                                + " schema (CREATE_IF_NEEDED)",
                        destination);
                createTable(destination);
                return true;
            }
            SchemaUnifier.UnionResult union =
                    SchemaUnifier.union(live.getSchema(), desired, config.getSchemaUpdateOptions());
            if (!union.isChanged()) {
                // The table already covers the serializer schema (possibly thanks to a
                // concurrent subtask).
                return false;
            }
            if (tableAdmin.updateSchema(destination, live, union.getSchema())) {
                LOG.info("Updated the schema of {} to cover the serializer schema", destination);
                return true;
            }
            sleepJitter();
        }
        throw new IOException(
                "Failed to update the schema of BigQuery table "
                        + destination
                        + ": lost a concurrent-update race "
                        + SCHEMA_UPDATE_MAX_ATTEMPTS
                        + " times");
    }

    /**
     * Re-appends the given batches on a rebuilt appender, retrying with backoff within the given
     * schedule's budget as long as failures stay repairable: {@code NOT_FOUND} while table metadata
     * has not propagated yet (creating the table first if it has not been created during this
     * repair and the disposition allows it), schema mismatches while a schema update propagates
     * (reconciling first if the mismatch is discovered during this repair and updates are enabled),
     * stale-writer and transient failures, and row-level failures (which are routed to the {@link
     * FailedRowHandler}, shrinking the batch to the surviving rows). Terminal failures and
     * retry-budget exhaustion fail the writer.
     *
     * @param destination the destination whose appender is rebuilt
     * @param batches the batches to re-append
     * @param tableCreated whether the destination table was just created for this repair
     * @param schemaUpdated whether the table schema was just reconciled for this repair
     * @param schedule the retry schedule bounding this repair
     */
    private void retryBatches(
            TableDestination destination,
            List<ProtoRows> batches,
            boolean tableCreated,
            boolean schemaUpdated,
            RetrySchedule schedule)
            throws IOException {
        List<ProtoRows> remaining = new ArrayList<>(batches);
        for (int attempt = 1; ; attempt++) {
            DestinationState state = null;
            try {
                state = rebuildState(destination);
            } catch (IOException | RuntimeException e) {
                tableCreated = maybeCreateMissingTable(destination, e, tableCreated);
                boolean retriable = isRetriable(e, tableCreated, schemaUpdated);
                if (!retriable || attempt >= schedule.maxAttempts()) {
                    throw wrapFailure(
                            retryFailureMessage(
                                    "Failed to open a BigQuery write stream to " + destination,
                                    retriable,
                                    tableCreated,
                                    schemaUpdated,
                                    attempt,
                                    schedule),
                            e);
                }
            }
            boolean backOff = state == null;
            while (state != null && !remaining.isEmpty() && !backOff) {
                ProtoRows head = remaining.get(0);
                Throwable failure =
                        awaitFailure(
                                destination,
                                state.appender.append(head),
                                "Interrupted while re-appending to BigQuery table " + destination);
                if (failure == null) {
                    remaining.remove(0);
                    continue;
                }
                // Schema-mismatch reconciliation is checked before row-level routing, mirroring
                // repairActionFor: a mismatch fails the batch as a whole, and a schema update can
                // save rows that per-row routing would drop.
                if (maybeReconcileSchema(destination, failure, schemaUpdated)) {
                    // The remaining re-appends now wait on schema propagation, which needs the
                    // longer budget.
                    schemaUpdated = true;
                    schedule = schemaWaitSchedule;
                } else if (AppendErrorClassifier.findRowLevel(failure).isPresent()) {
                    Exceptions.AppendSerializtionError rowLevel =
                            AppendErrorClassifier.findRowLevel(failure).get();
                    // Shrink the batch to the surviving rows and stay in the same attempt.
                    ProtoRows survivors = routeRowLevel(destination, head, rowLevel);
                    if (survivors.getSerializedRowsCount() >= head.getSerializedRowsCount()) {
                        // No row matched the reported indices, so nothing was dropped;
                        // re-appending the identical batch could never make progress.
                        throw wrapFailure(
                                "A re-append to BigQuery table "
                                        + destination
                                        + " failed with row errors matching none of the batch's"
                                        + " rows ("
                                        + attempt
                                        + " attempt(s))",
                                failure);
                    }
                    remaining.remove(0);
                    if (survivors.getSerializedRowsCount() > 0) {
                        remaining.add(0, survivors);
                    }
                    continue;
                }
                tableCreated = maybeCreateMissingTable(destination, failure, tableCreated);
                boolean retriable = isRetriable(failure, tableCreated, schemaUpdated);
                if (!retriable || attempt >= schedule.maxAttempts()) {
                    throw wrapFailure(
                            retryFailureMessage(
                                    "A re-append to BigQuery table " + destination + " failed",
                                    retriable,
                                    tableCreated,
                                    schemaUpdated,
                                    attempt,
                                    schedule),
                            failure);
                }
                backOff = true;
            }
            if (!backOff) {
                return;
            }
            long backoffMs = schedule.backoffMs(attempt);
            LOG.info(
                    "Re-appending to BigQuery table {} is not possible yet"
                            + " (attempt {}/{}), backing off {} ms",
                    destination,
                    attempt,
                    schedule.maxAttempts(),
                    backoffMs);
            sleep(backoffMs);
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
     * Reconciles the destination table's schema when a repair-time failure is a schema mismatch,
     * updates are enabled, and no reconciliation has run during this repair yet (a transient repair
     * can discover a schema mismatch). Returns whether a reconciliation ran just now.
     */
    private boolean maybeReconcileSchema(
            TableDestination destination, Throwable failure, boolean schemaUpdated)
            throws IOException {
        if (schemaUpdated
                || !config.getSchemaUpdateOptions().isEnabled()
                || !AppendErrorClassifier.isSchemaMismatch(failure)) {
            return false;
        }
        LOG.info(
                "A re-append to {} failed with a schema mismatch, reconciling the table schema",
                destination);
        reconcileSchema(destination);
        return true;
    }

    /**
     * Whether a repair-time failure warrants another attempt: {@code NOT_FOUND} while created table
     * metadata propagates, a schema mismatch while a schema update propagates, a stale-writer
     * failure, or a transient failure.
     */
    private static boolean isRetriable(
            Throwable failure, boolean tableCreated, boolean schemaUpdated) {
        return (tableCreated && isNotFound(failure))
                || (schemaUpdated && AppendErrorClassifier.isSchemaMismatch(failure))
                || AppendErrorClassifier.requiresWriterRefresh(failure)
                || AppendErrorClassifier.classify(failure) == AppendErrorClassifier.Kind.TRANSIENT;
    }

    private String retryFailureMessage(
            String base,
            boolean retriable,
            boolean tableCreated,
            boolean schemaUpdated,
            int attempt,
            RetrySchedule schedule) {
        StringBuilder message = new StringBuilder(base);
        if (tableCreated) {
            message.append(" after creating the table");
        }
        if (schemaUpdated) {
            message.append(" after reconciling the table schema");
        }
        if (retriable && attempt >= schedule.maxAttempts()) {
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
        return AppendErrorClassifier.hasCode(t, Status.Code.NOT_FOUND);
    }

    /**
     * Turns a terminal append failure into the {@link IOException} failing the writer. Failures
     * synthesized from an errored response already carry the full message and are thrown as-is;
     * everything else — including foreign {@link IOException}s — is wrapped with the destination
     * context.
     */
    private IOException terminalFailure(TableDestination destination, Throwable cause) {
        if (cause instanceof ResponseErrorException) {
            return (ResponseErrorException) cause;
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
        if (AppendErrorClassifier.isSchemaMismatch(cause)
                && !config.getSchemaUpdateOptions().isEnabled()) {
            message +=
                    " because the rows carry fields the table does not have; update the table"
                            + " schema, or enable schemaUpdateOptions(...) to let the sink update"
                            + " it";
        }
        return new IOException(message, cause);
    }

    /** Sleeps a random duration up to {@link #SCHEMA_UPDATE_MAX_JITTER_MS}. */
    private static void sleepJitter() throws IOException {
        sleep(ThreadLocalRandom.current().nextLong(SCHEMA_UPDATE_MAX_JITTER_MS + 1));
    }

    private static void sleep(long millis) throws IOException {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting to retry appends to BigQuery", e);
        }
    }

    /**
     * Maps a completed append response to the failure it carries, or {@code null} for a clean
     * response. An error with a transient status code becomes a synthesized {@link
     * StatusRuntimeException} so the whole batch is retried (checked before row errors: a transient
     * request must not drop rows). An error carrying a storage error identifying a stale stream
     * writer — or, with schema updates enabled, a schema mismatch — is surfaced as a typed failure
     * (the SDK's typed exception where it has one, otherwise a synthesized status exception with
     * the storage error in its trailers) so those responses are repaired like the equivalent
     * append-future failures instead of dropping every row. With schema updates disabled, a schema
     * mismatch accompanied by row errors falls through to row-level routing so the configured
     * {@link FailedRowHandler} policy still applies. Remaining row errors become a synthesized
     * row-level error (the same shape the SDK raises), and any other error is terminal.
     */
    private Throwable responseToThrowable(
            TableDestination destination, AppendRowsResponse response) {
        if (response.hasError()) {
            if (AppendErrorClassifier.isTransientCode(response.getError().getCode())) {
                return Status.fromCodeValue(response.getError().getCode())
                        .withDescription(response.getError().getMessage())
                        .asRuntimeException();
            }
            // The SDK types only some storage errors (SCHEMA_MISMATCH_EXTRA_FIELDS,
            // STREAM_FINALIZED, STREAM_NOT_FOUND, offsets); the synthesized fallback keeps the
            // storage error detectable in the status trailers for the rest (INVALID_STREAM_STATE).
            Throwable storageError = AppendErrorClassifier.toStorageException(response.getError());
            if (storageError == null) {
                storageError = StatusProto.toStatusRuntimeException(response.getError());
            }
            if (AppendErrorClassifier.requiresWriterRefresh(storageError)) {
                return storageError;
            }
            if (AppendErrorClassifier.isSchemaMismatch(storageError)
                    && (config.getSchemaUpdateOptions().isEnabled()
                            || response.getRowErrorsCount() == 0)) {
                return storageError;
            }
        }
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
            return new ResponseErrorException(
                    "An append to BigQuery table "
                            + destination
                            + " completed with an error: "
                            + response.getError().getMessage());
        }
        return null;
    }

    /**
     * Terminal failure synthesized from an errored append response. A dedicated type marks the
     * message as already carrying the full destination context, so it is surfaced as-is while
     * foreign {@link IOException}s still get wrapped.
     */
    private static final class ResponseErrorException extends IOException {
        private static final long serialVersionUID = 1L;

        ResponseErrorException(String message) {
            super(message);
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

        /** The serializer's schema fingerprint captured when the appender was built. */
        private final Object schemaFingerprint;

        private ProtoRows.Builder rows = ProtoRows.newBuilder();
        private long pendingBytes;

        DestinationState(RowAppender appender, Object schemaFingerprint) {
            this.appender = appender;
            this.schemaFingerprint = schemaFingerprint;
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
