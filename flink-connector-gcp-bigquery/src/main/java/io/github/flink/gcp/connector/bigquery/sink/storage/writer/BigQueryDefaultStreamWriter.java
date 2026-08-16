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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.metrics.DestinationMetrics;
import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolution;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolutionDispatcher;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.UnroutableRecord;
import io.github.flink.gcp.connector.bigquery.sink.failure.BigQueryFailure;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRow;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdminException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * At-least-once {@link SinkWriter} appending to Storage Write API default streams with dynamic
 * per-record table destinations.
 *
 * <p>Per destination, rows are buffered into append batches (bounded by {@code
 * DefaultStreamOptions#maxAppendRequestBytes}, default {@link #DEFAULT_MAX_APPEND_REQUEST_BYTES})
 * and appended asynchronously; backpressure is provided by the stream writer's own in-flight
 * limits. {@link #flush(boolean)} appends all pending batches and awaits every in-flight append,
 * inspecting each response directly, so records never pass a checkpoint barrier unacknowledged —
 * this is what makes the sink at-least-once. Records the serializer skips by returning {@code null}
 * are written nowhere and so are outside that claim. Asynchronous append failures are additionally
 * captured by completion callbacks and handled on the next {@link #write} or {@link #flush} call.
 *
 * <p>The writer is <em>stateless</em>: it stores nothing in Flink state, so discarding operator
 * state can never lose sink-buffered data (the {@code AsyncSinkWriter}-style alternative of
 * persisting unflushed buffers into writer state was deliberately rejected for exactly that failure
 * mode). Checkpointing must be enabled for the at-least-once guarantee in streaming jobs; without
 * it {@code flush()} is only invoked at end of input.
 *
 * <p>That guarantee assumes the default {@code failJob()} policy. Under {@code logAndDrop()} or
 * {@code sendToDeadLetterQueue(...)} a successful checkpoint means every row up to the barrier was
 * either acknowledged by BigQuery, skipped by the serializer, or handed to the {@link
 * FailureHandler}; the append-failure routing below says which failures reach it.
 *
 * <p>Append failures are routed by {@link AppendErrorClassifier} on the task thread. Transient
 * failures that surface past the SDK's own in-stream retries — and failures reporting the stream
 * writer itself as stale (finalized, unknown, closed) — are re-appended on a rebuilt stream writer
 * with backoff within a bounded retry budget; exhausting the budget is terminal. Row-level failures
 * (rows rejected with per-row error details) are routed row by row to the configured {@link
 * FailureHandler} — which fails the job, drops the row, or forwards it to a dead-letter queue — and
 * the surviving rows of the batch are re-appended. Everything else (for example {@code
 * INVALID_ARGUMENT}) is terminal and fails the ongoing write or checkpoint. In-flight batches are
 * retained together with their destination until acknowledged so they can be re-appended.
 *
 * <p>Under {@link CreateDisposition#CREATE_IF_NEEDED}, appends failing with a missing-table verdict
 * ({@link AppendErrorClassifier#isMissingTable} — {@code NOT_FOUND}, or the {@code
 * PERMISSION_DENIED} the service masks a missing table behind) are recovered on the task thread:
 * the destination table is created via the {@link TableAdmin} (schema from the serializer,
 * partitioning/clustering from the configured options provider), the destination's stream writer is
 * rebuilt, and the failed batch is re-appended with backoff while table metadata propagates to the
 * Storage Write API backend — a window that masks the same way. Under {@link
 * CreateDisposition#CREATE_NEVER}, both codes fail the write or checkpoint immediately.
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
public class BigQueryDefaultStreamWriter<T>
        implements SinkWriter<T>, DestinationResolutionDispatcher.Visitor<T> {

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
     * Retry schedule for re-appends after a table schema update, while the update propagates to the
     * Storage Write API backend — which takes minutes, considerably longer than table-creation
     * propagation. Flat 30 s waits, jittered (de-synchronizing parallel subtasks), 30 attempts: a
     * ceiling of roughly fifteen minutes.
     */
    private final BigQuerySinkConfig<T> config;

    private final RowAppenderFactory appenderFactory;
    private final TableAdmin tableAdmin;
    private final FailureHandler<? super BigQueryFailure> failureHandler;
    private final long maxAppendRequestBytes;
    private final RetrySchedule recoverySchedule;
    private final RetrySchedule schemaWaitSchedule;
    private final long destinationIdleTimeoutNanos;
    @Nullable private final Duration flushInterval;
    @Nullable private final ProcessingTimeService timerService;
    private final LongSupplier nanoClock;
    private final DefaultStreamWriterMetrics metrics;
    private final StorageWriteSchemaReconciler<T> schemaReconciler;

    /**
     * Stops the periodic-flush timer from re-arming (and from flushing closed appenders) once the
     * writer is closed. Written and read on the task thread only, like {@link #states}: processing
     * time timer callbacks run on the mailbox.
     */
    private boolean closed;

    /** Accessed only from the task thread. */
    private final Map<TableDestination, DestinationState> states = new HashMap<>();

    /** Completed entries are removed by gRPC callback threads (except repairable failures). */
    private final Map<ApiFuture<AppendRowsResponse>, InFlightBatch> inFlight =
            new ConcurrentHashMap<>();

    private final AtomicReference<Throwable> asyncError = new AtomicReference<>();

    /**
     * Whether the captured terminal failure has been counted under its error class. Read and
     * written on the task thread only, by {@link #checkAsyncError()}: the capture happens on a
     * callback thread, which counts nothing, and every later call would otherwise re-count the same
     * failure while the task is torn down.
     */
    private boolean asyncErrorCounted;

    /**
     * Set by completion callbacks when an append failed in a way the task thread can repair (a
     * recoverable missing-table verdict, schema mismatches, transient failures, row-level
     * failures); the task thread then sweeps {@link #inFlight} for failed batches and repairs them.
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
     * Creates a writer with default options and no periodic flush.
     *
     * @param config the sink configuration
     * @param appenderFactory the appender factory
     * @param tableAdmin the admin for creating and updating destination tables
     * @param metricGroup the writer's metric group
     */
    public BigQueryDefaultStreamWriter(
            BigQuerySinkConfig<T> config,
            RowAppenderFactory appenderFactory,
            TableAdmin tableAdmin,
            SinkWriterMetricGroup metricGroup) {
        this(
                config,
                appenderFactory,
                tableAdmin,
                metricGroup,
                DefaultStreamOptions.builder().build(),
                null);
    }

    /**
     * Creates a writer with no periodic flush (there is no timer service to drive one).
     *
     * @param config the sink configuration
     * @param appenderFactory the appender factory
     * @param tableAdmin the admin for creating and updating destination tables
     * @param metricGroup the writer's metric group
     * @param options the default-stream options
     */
    public BigQueryDefaultStreamWriter(
            BigQuerySinkConfig<T> config,
            RowAppenderFactory appenderFactory,
            TableAdmin tableAdmin,
            SinkWriterMetricGroup metricGroup,
            DefaultStreamOptions options) {
        this(config, appenderFactory, tableAdmin, metricGroup, options, null);
    }

    /**
     * Creates a writer, taking the batching cap and the connector-driven recovery schedule from the
     * given options. That schedule covers missing-table recovery after creating a table (metadata
     * propagation to the Storage Write API backend is usually seconds but can take considerably
     * longer), transient append failures that surfaced past the SDK's own retries, and
     * stale-stream-writer refreshes; its defaults allow roughly a minute in total. The schema-wait
     * schedule is not configurable — it paces BigQuery metadata propagation, a service property
     * rather than a workload property.
     *
     * <p>When the options carry a {@code flushInterval} and a timer service is given, the writer
     * registers a recurring processing-time flush; without a timer service the interval is inert.
     *
     * @param config the sink configuration
     * @param appenderFactory the appender factory
     * @param tableAdmin the admin for creating and updating destination tables
     * @param metricGroup the writer's metric group
     * @param options the default-stream options
     * @param timerService the processing-time service driving the periodic flush, or {@code null}
     */
    public BigQueryDefaultStreamWriter(
            BigQuerySinkConfig<T> config,
            RowAppenderFactory appenderFactory,
            TableAdmin tableAdmin,
            SinkWriterMetricGroup metricGroup,
            DefaultStreamOptions options,
            @Nullable ProcessingTimeService timerService) {
        this(
                config,
                appenderFactory,
                tableAdmin,
                new DefaultStreamWriterMetrics(metricGroup, options.isPerDestinationMetrics()),
                options.getMaxAppendRequestBytes(),
                options.toRecoverySchedule(),
                StorageWriteSchemaReconciler.DEFAULT_SCHEMA_WAIT_SCHEDULE,
                options.getDestinationIdleTimeout(),
                options.getFlushInterval(),
                timerService,
                System::nanoTime);
    }

    BigQueryDefaultStreamWriter(
            BigQuerySinkConfig<T> config,
            RowAppenderFactory appenderFactory,
            TableAdmin tableAdmin,
            SinkWriterMetricGroup metricGroup,
            long maxAppendRequestBytes,
            RetrySchedule recoverySchedule,
            RetrySchedule schemaWaitSchedule) {
        this(
                config,
                appenderFactory,
                tableAdmin,
                new DefaultStreamWriterMetrics(metricGroup, false),
                maxAppendRequestBytes,
                recoverySchedule,
                schemaWaitSchedule,
                DefaultStreamOptions.DEFAULT_DESTINATION_IDLE_TIMEOUT,
                null,
                null,
                System::nanoTime);
    }

    BigQueryDefaultStreamWriter(
            BigQuerySinkConfig<T> config,
            RowAppenderFactory appenderFactory,
            TableAdmin tableAdmin,
            DefaultStreamWriterMetrics metrics,
            long maxAppendRequestBytes,
            RetrySchedule recoverySchedule,
            RetrySchedule schemaWaitSchedule,
            Duration destinationIdleTimeout,
            @Nullable Duration flushInterval,
            @Nullable ProcessingTimeService timerService,
            LongSupplier nanoClock) {
        this.config = Preconditions.checkNotNull(config, "config must not be null");
        this.appenderFactory =
                Preconditions.checkNotNull(appenderFactory, "appenderFactory must not be null");
        this.tableAdmin = Preconditions.checkNotNull(tableAdmin, "tableAdmin must not be null");
        this.failureHandler = config.getFailureHandler();
        this.maxAppendRequestBytes = maxAppendRequestBytes;
        this.recoverySchedule =
                Preconditions.checkNotNull(recoverySchedule, "recoverySchedule must not be null");
        this.schemaWaitSchedule =
                Preconditions.checkNotNull(
                        schemaWaitSchedule, "schemaWaitSchedule must not be null");
        this.destinationIdleTimeoutNanos =
                Preconditions.checkNotNull(
                                destinationIdleTimeout, "destinationIdleTimeout must not be null")
                        .toNanos();
        this.flushInterval = flushInterval;
        this.timerService = timerService;
        this.nanoClock = Preconditions.checkNotNull(nanoClock, "nanoClock must not be null");
        this.metrics = Preconditions.checkNotNull(metrics, "metrics must not be null");
        this.schemaReconciler = new StorageWriteSchemaReconciler<>(config, tableAdmin);
        // Both gauges read collections the task thread owns; a reporter thread sampling them can
        // see a size mid-update, which is what "best-effort" means for a gauge over a live map.
        this.metrics.bindWriterState(
                (Gauge<Integer>) inFlight::size, (Gauge<Integer>) states::size);
        if (flushInterval != null && timerService != null) {
            scheduleFlush();
        }
    }

    /**
     * Arms the next periodic flush. The callback runs on the mailbox (task) thread, which is what
     * makes calling {@link #flush(boolean)} — and touching {@link #states} — safe from it.
     */
    private void scheduleFlush() {
        timerService.registerTimer(
                timerService.getCurrentProcessingTime() + flushInterval.toMillis(),
                timestamp -> {
                    if (closed) {
                        return;
                    }
                    flush(false);
                    scheduleFlush();
                });
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
        DestinationResolution resolution =
                config.getDestinationResolver().resolve(element, context);
        DestinationResolutionDispatcher.dispatch(resolution, element, context, this);
    }

    @Override
    public void visit(UnroutableRecord failure, T element, Context context) throws IOException {
        metrics.recordFailedWithoutDestination();
        failureHandler.handle(failure);
    }

    @Override
    public void visit(TableDestination destination, T element, Context context) throws IOException {
        // Augmented schema conflicts are configuration failures, not poison rows. Derive both
        // schema surfaces before entering the row-failure boundary so log-and-drop or dead-letter
        // policies cannot hide a destination-wide conflict.
        config.prepareWriteSchema(destination);
        ByteString row;
        try {
            // Serialized before any per-destination state exists: a poison record must reach the
            // handler no matter how the serializer fails, without opening a write stream (or
            // auto-creating a table) for a destination that may never receive a row. Resolver
            // failures, by contrast, are configuration errors and propagate.
            row = config.serialize(element, destination);
        } catch (IOException | RuntimeException e) {
            metrics.rowFailed(metrics.forTable(destination));
            failureHandler.handle(
                    FailedRow.of(
                            destination,
                            null,
                            "Failed to serialize a record for " + destination + ": " + e,
                            e));
            return;
        }
        if (row == null) {
            // Skip by contract, not a failure. Like a rejected record it costs no per-destination
            // state. Counted, because nothing else reports it: a serializer skipping every record
            // leaves an empty table under a green job.
            metrics.recordSkipped();
            return;
        }
        if (row.size() > MAX_ROW_BYTES) {
            metrics.rowFailed(metrics.forTable(destination));
            failureHandler.handle(
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
        state.lastAccessNanos = nanoClock.getAsLong();
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
        // After the drain: every row-level failure this flush routed has been handled, so the
        // handler can persist them before the checkpoint completes.
        failureHandler.flush();
        if (!endOfInput) {
            evictIdleDestinations();
        }
    }

    /**
     * Closes and drops the per-destination state of destinations idle beyond the configured timeout
     * — memory hygiene for long-lived jobs with dynamic destinations (for example date-suffixed
     * tables), whose {@link #states} map otherwise grows without bound. Runs at the end of a
     * successful flush, when every destination's pending batch is empty and every in-flight append
     * has been awaited, so closing an appender here cannot cancel a live append (the invariant the
     * repair path maintains with {@code collectFailedSiblings}). The pending check is defensive: a
     * row-level failure routed to a dropping {@code FailureHandler} can leave re-appended rows
     * pending past the await loop. Correctness is unaffected either way — an evicted destination
     * that receives a record again rebuilds its stream writer transparently.
     */
    private void evictIdleDestinations() {
        long now = nanoClock.getAsLong();
        Iterator<Map.Entry<TableDestination, DestinationState>> iterator =
                states.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TableDestination, DestinationState> entry = iterator.next();
            DestinationState state = entry.getValue();
            if (state.pendingCount() > 0
                    || now - state.lastAccessNanos <= destinationIdleTimeoutNanos) {
                continue;
            }
            iterator.remove();
            try {
                state.appender.close();
            } catch (RuntimeException e) {
                // Hygiene must never fail a checkpoint; the stream writer is abandoned either way.
                LOG.warn(
                        "Failed to close the stream writer of idle destination {}",
                        entry.getKey(),
                        e);
            }
            LOG.info(
                    "Evicted destination {} after {} without records",
                    entry.getKey(),
                    Duration.ofNanos(now - state.lastAccessNanos));
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        List<AutoCloseable> closeables = new ArrayList<>(states.size() + 1);
        for (DestinationState state : states.values()) {
            closeables.add(state.appender);
        }
        states.clear();
        // Both maps back gauges a reporter may still sample between this call and the metric
        // group's own close, so a writer torn down mid-flight must not keep reporting appends it
        // will never wait for again. Nothing re-adds an entry: the completion callbacks only
        // remove. Same reason PubSubWriter.close() zeroes its parked count.
        inFlight.clear();
        // Closers.closeAll, not sequential closes: the handler must be closed on the failure path
        // too, even when closing an appender throws.
        closeables.add(failureHandler::close);
        Closers.closeAll(closeables);
    }

    /**
     * Returns the destination's state, creating it if absent. A CDC destination under {@code
     * CREATE_IF_NEEDED} is provisioned and verified before the appender opens. For other writes,
     * when creating the appender itself fails with a missing-table verdict (the SDK looks up the
     * table's location when none is configured) and the disposition allows it, the table is created
     * and the appender creation retried. A recovery that fails in turn carries that first verdict
     * as a suppressed exception, so the failure a reader acts on still says what made this writer
     * try to create a table.
     */
    private DestinationState ensureState(TableDestination destination) throws IOException {
        DestinationState state = states.get(destination);
        if (state != null) {
            return state;
        }
        if (managesCdcTableContract()) {
            ensureCdcTable(destination);
        }
        try {
            state = createState(destination);
        } catch (IOException | RuntimeException e) {
            if (!isRecoverableMissingTable(e)) {
                throw wrapFailure("Failed to open a BigQuery write stream to " + destination, e);
            }
            LOG.info(
                    "Destination table {} may not exist, creating it (CREATE_IF_NEEDED)"
                            + " (cause: {})",
                    destination,
                    e.toString());
            try {
                recoverDestination(destination, Collections.emptyList());
            } catch (IOException | RuntimeException recoveryFailure) {
                // The verdict that sent us here is what tells the two readings of a masked
                // PERMISSION_DENIED apart — a table that is not there, or an existing one these
                // credentials cannot write to. Without it a reader of "cannot create the table"
                // has no way to see that the real problem was the second. Suppressed rather than
                // chained: the recovery failure is the one to act on. Same shape as
                // BigQueryLoadJobRunner.create's conflict lookup.
                recoveryFailure.addSuppressed(e);
                throw recoveryFailure;
            }
            return states.get(destination);
        }
        states.put(destination, state);
        return state;
    }

    private DestinationState createState(TableDestination destination) throws IOException {
        // The fingerprint is captured before the descriptor: if the serializer schema evolves
        // between the two calls, the stale fingerprint triggers a redundant-but-harmless refresh
        // on the next record, whereas the opposite order could miss a change.
        Object fingerprint = config.getSchemaFingerprint(destination);
        RowAppender appender =
                appenderFactory.create(
                        destination, config.getWriteDescriptor(destination), config.getLocation());
        return new DestinationState(appender, fingerprint, nanoClock.getAsLong());
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
        Object fingerprint = config.getSchemaFingerprint(destination);
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
            Object fingerprint = config.getSchemaFingerprint(destination);
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
        long rowBytes = state.pendingBytes;
        ProtoRows rows = state.take();
        ApiFuture<AppendRowsResponse> future = state.appender.append(rows);
        // Counted here and nowhere else: this is the append that first hands the batch to the
        // client library, and it is counted after the call, so a synchronous rejection (which
        // registers no callback, and reaches BigQuery not at all) is not reported as sent. The
        // re-appends of a repair go through retryBatches, which counts appendRetries instead.
        metrics.batchAppended(
                metrics.forTable(destination), rows.getSerializedRowsCount(), rowBytes);
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
        /** A recoverable missing-table verdict: create the table, then re-append. */
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
        if (isRecoverableMissingTable(cause)) {
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
     * Handles a completed-with-failure append on the task thread: batches under a recoverable
     * missing-table verdict are re-appended after creating the table, schema mismatches after
     * reconciling the table schema (when updates are enabled), transient and stale-writer failures
     * are re-appended within the retry budget, row-level failures are routed to the {@link
     * FailureHandler} (surviving rows are re-appended), and anything else fails the writer. The
     * {@link #inFlight} removal arbitrates ownership against the completion callbacks.
     */
    private void handleFailedAppend(ApiFuture<AppendRowsResponse> future, Throwable cause)
            throws IOException {
        InFlightBatch batch = inFlight.remove(future);
        if (batch == null) {
            // The completion callback owned this failure; it is surfaced — and counted — via
            // checkAsyncError.
            return;
        }
        metrics.appendFailed(AppendErrorClassifier.statusCode(cause));
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
                // The trailing placeholder is not decoration: slf4j drops an extra argument that
                // is not a Throwable, so without it the cause this method is handed goes nowhere
                // — and a masked PERMISSION_DENIED is exactly the cause a reader needs to see.
                reason =
                        "An append to {} failed because the table may not exist, creating it"
                                + " (CREATE_IF_NEEDED) (cause: {})";
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
            metrics.appendFailed(AppendErrorClassifier.statusCode(failure));
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
     * Routes a row-level append failure to the {@link FailureHandler} row by row and returns the
     * surviving rows. The handler decides per row: returning normally drops the row, throwing fails
     * the writer.
     */
    private ProtoRows routeRowLevel(
            TableDestination destination,
            ProtoRows rows,
            Exceptions.AppendSerializtionError rowLevel)
            throws IOException {
        Map<Integer, String> rowErrors = rowLevel.getRowIndexToErrorMessage();
        DestinationMetrics.Counters table = metrics.forTable(destination);
        ProtoRows.Builder survivors = ProtoRows.newBuilder();
        for (int i = 0; i < rows.getSerializedRowsCount(); i++) {
            String errorMessage = rowErrors.get(i);
            if (errorMessage == null) {
                survivors.addSerializedRows(rows.getSerializedRows(i));
            } else {
                metrics.rowFailed(table);
                failureHandler.handle(
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

    /**
     * Creates the destination table, which every repair path that discovers a missing table
     * reaches. A creation the service rate-limits is repeated by the {@link TableAdmin} the sink
     * wired ({@code RetryingTableAdmin}, on the recovery schedule), so this call blocks rather than
     * failing — and nothing here chooses a budget, which is what keeps a creation off the
     * fifteen-minute schema one however the repair around it got here. {@link #scheduleFor} keeps
     * the missing-table verdict itself off that budget for the same reason.
     */
    private void createTable(TableDestination destination) throws IOException {
        if (managesCdcTableContract()) {
            ensureCdcTable(destination);
            return;
        }
        tableAdmin.create(
                destination,
                config.getTableSchema(destination),
                config.getTableCreateOptionsProvider().optionsFor(destination));
        // The single creation site, so the counter needs no guard against the repair paths that
        // reach it. Creation is idempotent across subtasks, so this counts what this subtask
        // asked for, not what BigQuery had to do — and a creation the admin had to repeat counts
        // once, since the repeats happen before this line is reached.
        metrics.tableCreated();
    }

    private boolean managesCdcTableContract() {
        return config.managesCdcTableContract();
    }

    private void ensureCdcTable(TableDestination destination) throws IOException {
        CdcTableOptions cdcTableOptions =
                Preconditions.checkNotNull(
                        config.getCdcTableOptionsProvider().optionsFor(destination),
                        "CdcTableOptionsProvider returned null for %s",
                        destination);
        try {
            if (tableAdmin.ensureCdcTable(
                    destination,
                    config.getTableSchema(destination),
                    config.getTableCreateOptionsProvider(),
                    cdcTableOptions,
                    config.getCreateDisposition(),
                    config.getCdcTableReconciliationPolicy())) {
                countCdcTableCreationRequest();
            }
        } catch (TableAdminException e) {
            if (e.wasCreationRequested()) {
                countCdcTableCreationRequest();
            }
            throw e;
        }
    }

    private void countCdcTableCreationRequest() {
        // Eager CDC verification of a table that already exists must not look like another
        // creation after every restart or destination reactivation. Once creation was requested,
        // count it even when later provisioning fails: the table still exists for the operator.
        metrics.tableCreated();
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
        StorageWriteSchemaReconciler.Outcome outcome = schemaReconciler.reconcile(destination);
        if (outcome == StorageWriteSchemaReconciler.Outcome.CREATED) {
            metrics.tableCreated();
        } else if (outcome == StorageWriteSchemaReconciler.Outcome.UPDATED) {
            metrics.schemaReconciled();
        }
        return outcome != StorageWriteSchemaReconciler.Outcome.UNCHANGED;
    }

    /**
     * Re-appends the given batches on a rebuilt appender, retrying with backoff within the given
     * schedule's budget as long as failures stay repairable: a missing-table verdict while table
     * metadata has not propagated yet (creating the table first if it has not been created during
     * this repair and the disposition allows it), schema mismatches while a schema update
     * propagates (reconciling first if the mismatch is discovered during this repair and updates
     * are enabled), stale-writer and transient failures, and row-level failures (which are routed
     * to the {@link FailureHandler}, shrinking the batch to the surviving rows). Terminal failures
     * and retry-budget exhaustion fail the writer.
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
                tableCreated = createTableIfMissing(destination, e, tableCreated);
                schedule = scheduleFor(e, tableCreated, schemaUpdated, schedule);
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
                ApiFuture<AppendRowsResponse> reappend = state.appender.append(head);
                // Re-appends are counted here rather than as sends: the rows they carry were
                // counted when appendPending first handed them over.
                metrics.appendRetried();
                Throwable failure =
                        awaitFailure(
                                destination,
                                reappend,
                                "Interrupted while re-appending to BigQuery table " + destination);
                if (failure == null) {
                    remaining.remove(0);
                    continue;
                }
                metrics.appendFailed(AppendErrorClassifier.statusCode(failure));
                // Schema-mismatch reconciliation is checked before row-level routing, mirroring
                // repairActionFor: a mismatch fails the batch as a whole, and a schema update can
                // save rows that per-row routing would drop.
                if (reconcileSchemaIfMismatched(destination, failure, schemaUpdated)) {
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
                tableCreated = createTableIfMissing(destination, failure, tableCreated);
                schedule = scheduleFor(failure, tableCreated, schemaUpdated, schedule);
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
     * Creates the destination table when a repair-time failure is a recoverable missing-table
     * verdict and the table has not been created during this repair yet (a transient repair can
     * discover a missing table).
     *
     * @return the updated created-flag: {@code true} once the table has been created, whether just
     *     now or on an earlier attempt of this repair
     */
    private boolean createTableIfMissing(
            TableDestination destination, Throwable failure, boolean tableCreated)
            throws IOException {
        if (!tableCreated && isRecoverableMissingTable(failure)) {
            LOG.info(
                    "The table behind {} may not exist, creating it (CREATE_IF_NEEDED)",
                    destination);
            createTable(destination);
            return true;
        }
        return tableCreated;
    }

    /**
     * Reconciles the destination table's schema when a repair-time failure is a schema mismatch,
     * updates are enabled, and no reconciliation has run during this repair yet (a transient repair
     * can discover a schema mismatch).
     *
     * @return whether a reconciliation ran just now — not the accumulated flag its sibling {@code
     *     createTableIfMissing} returns; the caller switches to the schema-wait schedule only on a
     *     fresh reconciliation
     */
    private boolean reconcileSchemaIfMismatched(
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
     * Picks the budget the <em>current</em> failure deserves, whichever one the repair happens to
     * be running on.
     *
     * <p>Two moves, and they have to be two because the repair's schedule is a loop variable rather
     * than a property of the failure in hand:
     *
     * <ul>
     *   <li><b>Down to the recovery schedule for a missing-table verdict.</b> Table-creation
     *       metadata propagates in seconds, but {@code createTableIfMissing} is reached from schema
     *       repairs too, which run on the fifteen-minute {@link #schemaWaitSchedule}. Without this,
     *       a masked {@code PERMISSION_DENIED} that is a <em>genuine</em> denial — an existing
     *       table the credentials cannot write to, where the creation attempt returns HTTP 409 and
     *       is swallowed as success — would inherit that budget and turn a failure that used to be
     *       immediate and well named into a checkpoint timeout with no cause attached. The service
     *       masks existence, so this writer cannot tell that case from a real propagation window;
     *       what it can do is not spend a schema budget on a question that is not about schemas.
     *   <li><b>Back up for a schema mismatch.</b> The escalation at the reconcile branch fires only
     *       on the reconciliation itself, which happens once per repair — so once a missing-table
     *       verdict has bounded the schedule, a mismatch arriving afterwards would wait out schema
     *       propagation on the one-minute budget instead of the fifteen it is sized for, and fail a
     *       repair that was progressing. Reachable when a table is dropped and re-created
     *       mid-repair, and newly reachable at all because the masked code brings missing-table
     *       verdicts into schema repairs.
     * </ul>
     *
     * <p>Deliberately missing-table and schema-mismatch only. A transient or stale-writer failure
     * during a schema repair keeps the long budget: unlike a possibly-permanent denial those really
     * are retriable, and shortening their wait would fail repairs that would have succeeded.
     */
    private RetrySchedule scheduleFor(
            Throwable failure,
            boolean tableCreated,
            boolean schemaUpdated,
            RetrySchedule schedule) {
        if (schemaUpdated && AppendErrorClassifier.isSchemaMismatch(failure)) {
            return schemaWaitSchedule;
        }
        if (tableCreated
                && schedule == schemaWaitSchedule
                && AppendErrorClassifier.isMissingTable(failure)) {
            return recoverySchedule;
        }
        return schedule;
    }

    /**
     * Whether a repair-time failure warrants another attempt: a missing-table verdict while created
     * table metadata propagates, a schema mismatch while a schema update propagates, a stale-writer
     * failure, or a transient failure.
     *
     * <p>The first clause takes the wide {@link AppendErrorClassifier#isMissingTable} rather than
     * {@code NOT_FOUND} alone: the service masks a table it cannot see yet as {@code
     * PERMISSION_DENIED}, so a propagation window right after this writer created the table looks
     * exactly like the failure that made it create the table. {@link #scheduleFor} is what keeps
     * that allowance from costing a schema budget.
     */
    private static boolean isRetriable(
            Throwable failure, boolean tableCreated, boolean schemaUpdated) {
        return (tableCreated && AppendErrorClassifier.isMissingTable(failure))
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
            message.append(" after a table-creation attempt");
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

    private boolean isRecoverableMissingTable(Throwable t) {
        return config.getCreateDisposition() == CreateDisposition.CREATE_IF_NEEDED
                && AppendErrorClassifier.isMissingTable(t);
    }

    /**
     * Walks the cause chain for a gax or gRPC {@code NOT_FOUND} — the table is definitely not
     * there, as opposed to the wider {@link AppendErrorClassifier#isMissingTable}, which also
     * accepts the {@code PERMISSION_DENIED} the real service masks a missing table behind. Used
     * only where the message asserts nonexistence to the reader, which the masked code cannot.
     */
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
                            + " schema and wait for Storage Write API propagation, or enable"
                            + " schemaUpdateOptions(...) to let the sink update it (Table API:"
                            + " set 'sink.schema-update.allow-new-fields' = 'true')";
        }
        return new IOException(message, cause);
    }

    private static void sleep(long millis) throws IOException {
        Retries.sleep(millis, "Interrupted while waiting to retry appends to BigQuery");
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
     * {@link FailureHandler} policy still applies. Remaining row errors become a synthesized
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
            if (!asyncErrorCounted) {
                // The one failure class a completion callback owns outright, counted here because
                // this is where the task thread first sees it — and counted once, since the
                // captured error is never cleared and this method runs on every write and flush.
                asyncErrorCounted = true;
                metrics.appendFailed(AppendErrorClassifier.statusCode(error));
            }
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

        /**
         * When the destination last received a record ({@code nanoClock} time), for idle eviction.
         * Initialized to creation time so a state rebuilt outside {@code write()} (a repair) is not
         * instantly idle.
         */
        private long lastAccessNanos;

        DestinationState(RowAppender appender, Object schemaFingerprint, long createdNanos) {
            this.appender = appender;
            this.schemaFingerprint = schemaFingerprint;
            this.lastAccessNanos = createdNanos;
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
