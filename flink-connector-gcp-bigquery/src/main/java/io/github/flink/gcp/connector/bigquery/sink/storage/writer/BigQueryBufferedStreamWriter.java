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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRow;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamCommittable;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;

/**
 * Exactly-once {@code SinkWriter} appending rows to one application-created BUFFERED Storage Write
 * API stream per destination at explicit offsets. Rows stay invisible until the committer flushes
 * them ({@code FlushRows}) as part of a completed checkpoint's commit — the two-phase-commit
 * contract.
 *
 * <p>That contract assumes the default {@code failJob()} policy. Under {@code logAndDrop()} or
 * {@code sendToDeadLetterQueue(...)} a completed checkpoint's commit makes every row up to the
 * barrier visible except those handed to the {@link FailureHandler}, which are never appended and
 * so never become visible at all; the error handling below says which failures reach it. A record
 * the serializer skips by returning {@code null} is never appended either, under any policy.
 *
 * <p><b>Stream lifecycle.</b> Each subtask owns one buffered stream per active destination, created
 * lazily on its first append and <em>reused across checkpoints</em> (frequent {@code
 * CreateWriteStream} churn is explicitly not intended usage of the API). Each stream name and next
 * append offset are Flink writer state; {@link #prepareCommit()} emits one committable per changed
 * destination naming the offset a completed checkpoint may flush up to. A clean destination that
 * stays idle past {@link BufferedStreamOptions#getDestinationIdleTimeout()} is evicted after a
 * successful non-end-of-input flush; a later row creates a new stream for it.
 *
 * <p><b>Restore.</b> A restored writer probes its stream with the first replayed batch,
 * synchronously, at the restored offset: success adopts the stream; {@code OFFSET_ALREADY_EXISTS}
 * (the pre-crash attempt appended past the restored offset), {@code OFFSET_OUT_OF_RANGE}, a
 * finalized/unknown stream, or a failure to open the appender abandon it — a fresh stream starts at
 * offset zero. Nothing appended past the restored offset was ever named by a committable, so it can
 * never be flushed and never becomes visible. Abandoned streams are deliberately <em>never
 * finalized</em> (neither here nor in {@link #close()}): BigQuery rejects {@code FlushRows} on a
 * finalized stream (verified against the real service), so finalizing could permanently break a
 * restored-but-uncommitted committable that still has to flush the old stream; leaving the stream
 * open keeps its unflushed tail invisible either way. Whether BigQuery bills that buffered storage
 * has not been established.
 *
 * <p><b>Error handling.</b> Serialization failures and oversized rows are routed to the {@link
 * FailureHandler} before any stream exists. Transient append failures surfacing past the SDK's
 * in-stream retries are re-appended at their original offset within a bounded budget ({@code
 * OFFSET_ALREADY_EXISTS} then means the original landed — success). A row-level rejection discards
 * nothing silently: the rejected batch's failing rows go to the handler and the surviving rows,
 * plus every batch appended behind the rejected one, are replayed with recomputed offsets (an
 * append request is rejected atomically, so the offset never advanced). During such a replay {@code
 * OFFSET_ALREADY_EXISTS} is terminal — offsets have shifted, and rows already present there would
 * silently diverge from what the writer believes was appended. A serializer-fingerprint change
 * drains old-schema rows, reconciles the table when enabled, and reconnects the same remote stream
 * with the new descriptor. A schema-mismatch response performs the same reconciliation and
 * same-offset retry; without enabled schema updates it remains terminal with configuration
 * guidance.
 *
 * <p><b>A missing table is {@link #createStream}'s business alone.</b> Every append-side recovery
 * decision here is transient-only on purpose: the propagation window after this writer creates a
 * table has not been observed to reach an append. Measured over 140 trials, in which the {@code
 * FlushRows} taken on the same table immediately after each append was denied eleven times and no
 * append was denied once (see {@code docs/adr/0030}, which also names the four sites and what a
 * single contrary observation would earn). Adding the verdict here would spend the recovery budget
 * on a denial that fails at once today — including the one case that is terminal by design, a table
 * dropped while the job runs, which no amount of waiting repairs.
 *
 * <p>Backpressure comes from the SDK: {@code StreamWriter} bounds its in-flight requests and blocks
 * further appends until the server acknowledges.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class BigQueryBufferedStreamWriter<T>
        implements CommittingSinkWriter<T, BufferedStreamCommittable>,
                StatefulSinkWriter<T, BufferedStreamWriterState> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryBufferedStreamWriter.class);

    private final BigQuerySinkConfig<T> config;
    private final BufferedStreamServiceFactory serviceFactory;
    private final TableAdmin tableAdmin;
    private final FailureHandler<? super FailedRow> failedRowHandler;
    private final int subtaskId;
    private final long maxAppendRequestBytes;
    private final long destinationIdleTimeoutNanos;
    private final RetrySchedule retrySchedule;
    private final RetrySchedule schemaWaitSchedule;
    private final BufferedStreamOptions options;
    private final BufferedStreamWriterMetrics metrics;
    private final StorageWriteSchemaReconciler<T> schemaReconciler;
    private final LongSupplier nanoClock;

    /** Active destinations in first-seen order; accessed only by the task thread. */
    private final Map<TableDestination, DestinationState> destinations = new LinkedHashMap<>();

    /** Reporter-safe scalar backing the aggregate in-flight gauge. */
    private volatile int inFlightCount;

    @Nullable private BufferedStreamService service;

    /**
     * Creates a writer, fresh or restored.
     *
     * @param config the sink configuration
     * @param options the buffered-stream options
     * @param serviceFactory the Storage Write API service factory
     * @param tableAdmin the admin for creating the destination table
     * @param metricGroup the writer's metric group
     * @param subtaskId the subtask index (diagnostics and committable attribution)
     * @param restoredStates the restored writer states; empty for a fresh writer
     */
    public BigQueryBufferedStreamWriter(
            BigQuerySinkConfig<T> config,
            BufferedStreamOptions options,
            BufferedStreamServiceFactory serviceFactory,
            TableAdmin tableAdmin,
            SinkWriterMetricGroup metricGroup,
            int subtaskId,
            Collection<BufferedStreamWriterState> restoredStates) {
        this(
                config,
                options,
                serviceFactory,
                tableAdmin,
                metricGroup,
                subtaskId,
                restoredStates,
                StorageWriteSchemaReconciler.DEFAULT_SCHEMA_WAIT_SCHEDULE,
                System::nanoTime);
    }

    @VisibleForTesting
    BigQueryBufferedStreamWriter(
            BigQuerySinkConfig<T> config,
            BufferedStreamOptions options,
            BufferedStreamServiceFactory serviceFactory,
            TableAdmin tableAdmin,
            SinkWriterMetricGroup metricGroup,
            int subtaskId,
            Collection<BufferedStreamWriterState> restoredStates,
            LongSupplier nanoClock) {
        this(
                config,
                options,
                serviceFactory,
                tableAdmin,
                metricGroup,
                subtaskId,
                restoredStates,
                StorageWriteSchemaReconciler.DEFAULT_SCHEMA_WAIT_SCHEDULE,
                nanoClock);
    }

    @VisibleForTesting
    BigQueryBufferedStreamWriter(
            BigQuerySinkConfig<T> config,
            BufferedStreamOptions options,
            BufferedStreamServiceFactory serviceFactory,
            TableAdmin tableAdmin,
            SinkWriterMetricGroup metricGroup,
            int subtaskId,
            Collection<BufferedStreamWriterState> restoredStates,
            RetrySchedule schemaWaitSchedule,
            LongSupplier nanoClock) {
        this.config = Preconditions.checkNotNull(config, "config must not be null");
        Preconditions.checkNotNull(options, "options must not be null");
        this.serviceFactory =
                Preconditions.checkNotNull(serviceFactory, "serviceFactory must not be null");
        this.tableAdmin = Preconditions.checkNotNull(tableAdmin, "tableAdmin must not be null");
        this.failedRowHandler = config.getFailedRowHandler();
        this.subtaskId = subtaskId;
        this.maxAppendRequestBytes = options.getMaxAppendRequestBytes();
        this.destinationIdleTimeoutNanos = options.getDestinationIdleTimeout().toNanos();
        this.retrySchedule = options.toRecoverySchedule();
        this.schemaWaitSchedule =
                Preconditions.checkNotNull(
                        schemaWaitSchedule, "schemaWaitSchedule must not be null");
        this.options = options;
        this.nanoClock = Preconditions.checkNotNull(nanoClock, "nanoClock must not be null");
        this.metrics =
                new BufferedStreamWriterMetrics(
                        Preconditions.checkNotNull(metricGroup, "metricGroup must not be null"));
        this.schemaReconciler = new StorageWriteSchemaReconciler<>(config, tableAdmin);
        this.metrics.bindWriterState((Gauge<Integer>) () -> inFlightCount);

        Map<TableDestination, BufferedStreamWriterState> adoptedByDestination =
                new LinkedHashMap<>();
        for (BufferedStreamWriterState state : restoredStates) {
            if (state.getStreamName().equals(BufferedStreamWriterState.NO_STREAM)) {
                continue;
            }
            BufferedStreamWriterState adopted = adoptedByDestination.get(state.getDestination());
            if (adopted == null
                    || state.getCheckpointId() > adopted.getCheckpointId()
                    || (state.getCheckpointId() == adopted.getCheckpointId()
                            && state.getStreamName().compareTo(adopted.getStreamName()) < 0)) {
                adoptedByDestination.put(state.getDestination(), state);
            }
        }
        long now = nanoClock.getAsLong();
        for (BufferedStreamWriterState adopted : adoptedByDestination.values()) {
            TableDestination destination = adopted.getDestination();
            Object fingerprint = config.getSchemaFingerprint(destination);
            Descriptors.Descriptor descriptor = config.getWriteDescriptor(destination);
            destinations.put(
                    destination, DestinationState.restored(adopted, fingerprint, descriptor, now));
            LOG.info(
                    "Restored subtask {} destination {} with stream {} at offset {}",
                    subtaskId,
                    adopted.getDestination(),
                    adopted.getStreamName(),
                    adopted.getNextOffset());
        }
        int dropped = restoredStates.size() - adoptedByDestination.size();
        if (dropped > 0) {
            LOG.info(
                    "Restored subtask {} dropped {} superseded or empty destination state(s)",
                    subtaskId,
                    dropped);
        }
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        TableDestination destination = config.getDestinationResolver().resolve(element, context);
        DestinationState state = destinations.get(destination);
        if (state != null) {
            // Keep the per-record cost independent of the number of active destinations. An
            // inactive destination's bounded SDK queue is drained at the next record for that
            // destination or at the checkpoint barrier.
            drainInFlight(state, true);
        }
        config.prepareWriteSchema(destination);
        ByteString row;
        try {
            // A poison record must reach the handler no matter how the serializer fails,
            // without creating a stream (or auto-creating a table) it may never need.
            row = config.serialize(element, destination);
        } catch (IOException | RuntimeException e) {
            metrics.rowFailed();
            failedRowHandler.handle(
                    FailedRow.of(
                            destination,
                            null,
                            "Failed to serialize a record for " + destination + ": " + e,
                            e));
            return;
        }
        if (row == null) {
            // Skip by contract, not a failure. Like a rejected record it creates no stream.
            // Counted, because nothing else reports it: a serializer skipping every record leaves
            // an empty table under a green job.
            metrics.recordSkipped();
            return;
        }
        if (row.size() > BigQueryDefaultStreamWriter.MAX_ROW_BYTES) {
            metrics.rowFailed();
            failedRowHandler.handle(
                    FailedRow.of(
                            destination,
                            row,
                            "A row for "
                                    + destination
                                    + " is "
                                    + row.size()
                                    + " bytes, exceeding the "
                                    + BigQueryDefaultStreamWriter.MAX_ROW_BYTES
                                    + "-byte per-row limit of the BigQuery Storage Write API",
                            null));
            return;
        }
        if (state == null) {
            state = createState(destination, nanoClock.getAsLong());
            destinations.put(destination, state);
        } else {
            state.lastAccessNanos = nanoClock.getAsLong();
            refreshOnFingerprintChange(state);
        }
        if (state.pending.getSerializedRowsCount() > 0
                && state.pendingBytes + row.size() > maxAppendRequestBytes) {
            sendAppend(state);
        }
        state.pending.addSerializedRows(row);
        state.pendingBytes += row.size();
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        for (DestinationState state : destinations.values()) {
            sendAppend(state);
        }
        for (DestinationState state : destinations.values()) {
            drainInFlight(state, false);
        }
        // After the drain: every row-level failure this flush routed has been handled, so the
        // handler can persist them before the checkpoint completes.
        failedRowHandler.flush();
        if (!endOfInput) {
            evictIdleDestinations();
        }
    }

    @Override
    public Collection<BufferedStreamCommittable> prepareCommit() {
        List<BufferedStreamCommittable> committables = new ArrayList<>();
        for (DestinationState state : destinations.values()) {
            if (state.streamName.equals(BufferedStreamWriterState.NO_STREAM)
                    || state.nextOffset == 0
                    || (state.streamName.equals(state.lastSnapshotStreamName)
                            && state.nextOffset == state.lastSnapshotOffset)) {
                continue;
            }
            // FlushRows offsets are inclusive: nextOffset - 1 is the last appended row.
            committables.add(
                    new BufferedStreamCommittable(
                            state.streamName, state.nextOffset - 1, subtaskId));
        }
        return committables;
    }

    @Override
    public List<BufferedStreamWriterState> snapshotState(long checkpointId) {
        List<BufferedStreamWriterState> snapshots = new ArrayList<>();
        for (DestinationState state : destinations.values()) {
            state.lastSnapshotStreamName = state.streamName;
            state.lastSnapshotOffset = state.nextOffset;
            snapshots.add(
                    new BufferedStreamWriterState(
                            state.destination, state.streamName, state.nextOffset, checkpointId));
        }
        return snapshots;
    }

    @Override
    public void close() throws Exception {
        // The stream is left open deliberately, whatever its state: committables emitted by
        // prepareCommit() may still be uncommitted (batch execution commits after the writer
        // closes; a crash leaves restored committables behind), and BigQuery rejects FlushRows
        // on a finalized stream — finalizing here could make those commits permanently fail.
        // An unflushable tail past the last snapshot stays invisible without any cleanup.
        // Closers.closeAll, not sequential closes: the handler must be closed on the failure path
        // too, even when closing the appender or service throws.
        List<AutoCloseable> closeables = new ArrayList<>();
        for (DestinationState state : destinations.values()) {
            if (state.appender != null) {
                closeables.add(state.appender);
                state.appender = null;
            }
            state.inFlight.clear();
        }
        if (service != null) {
            closeables.add(service);
            service = null;
        }
        // The scalar backs the inFlightAppends gauge, which a reporter may still sample between
        // this call and the metric group's own close; a writer torn down mid-flight must not keep
        // reporting appends after it has given resource ownership to the close sequence below.
        inFlightCount = 0;
        destinations.clear();
        closeables.add(failedRowHandler::close);
        Closers.closeAll(closeables);
    }

    // ------------------------------------------------------------------
    // Append pipeline
    // ------------------------------------------------------------------

    /** An append issued but not yet acknowledged; rows are retained for re-appends. */
    private static final class InFlightAppend {
        final ApiFuture<AppendRowsResponse> future;
        final long expectedOffset;
        final ProtoRows rows;

        InFlightAppend(ApiFuture<AppendRowsResponse> future, long expectedOffset, ProtoRows rows) {
            this.future = future;
            this.expectedOffset = expectedOffset;
            this.rows = rows;
        }
    }

    /** All task-thread state belonging to one dynamic destination. */
    private static final class DestinationState {
        final TableDestination destination;
        @Nullable OffsetRowAppender appender;
        @Nullable Object schemaFingerprint;
        Descriptors.Descriptor descriptor;
        String streamName;
        long nextOffset;
        String lastSnapshotStreamName;
        long lastSnapshotOffset;
        boolean probePending;
        ProtoRows.Builder pending = ProtoRows.newBuilder();
        long pendingBytes;
        final ArrayDeque<InFlightAppend> inFlight = new ArrayDeque<>();
        long lastAccessNanos;

        private DestinationState(
                TableDestination destination,
                String streamName,
                long nextOffset,
                boolean probePending,
                @Nullable Object schemaFingerprint,
                Descriptors.Descriptor descriptor,
                long lastAccessNanos) {
            this.destination = destination;
            this.streamName = streamName;
            this.nextOffset = nextOffset;
            this.lastSnapshotStreamName = streamName;
            this.lastSnapshotOffset = nextOffset;
            this.probePending = probePending;
            this.schemaFingerprint = schemaFingerprint;
            this.descriptor = descriptor;
            this.lastAccessNanos = lastAccessNanos;
        }

        static DestinationState fresh(
                TableDestination destination,
                @Nullable Object schemaFingerprint,
                Descriptors.Descriptor descriptor,
                long now) {
            return new DestinationState(
                    destination,
                    BufferedStreamWriterState.NO_STREAM,
                    0,
                    false,
                    schemaFingerprint,
                    descriptor,
                    now);
        }

        static DestinationState restored(
                BufferedStreamWriterState restored,
                @Nullable Object schemaFingerprint,
                Descriptors.Descriptor descriptor,
                long now) {
            return new DestinationState(
                    restored.getDestination(),
                    restored.getStreamName(),
                    restored.getNextOffset(),
                    true,
                    schemaFingerprint,
                    descriptor,
                    now);
        }
    }

    private DestinationState createState(TableDestination destination, long now) {
        // Capture the fingerprint first: if the serializer evolves between these calls, the next
        // record observes the stale fingerprint and performs a redundant-but-safe refresh. The
        // reverse order could miss a change and append rows under the wrong descriptor.
        Object fingerprint = config.getSchemaFingerprint(destination);
        Descriptors.Descriptor descriptor = config.getWriteDescriptor(destination);
        return DestinationState.fresh(destination, fingerprint, descriptor, now);
    }

    /** Refreshes one destination's connection before a row encoded under a new schema is queued. */
    private void refreshOnFingerprintChange(DestinationState state) throws IOException {
        if (state.schemaFingerprint == null) {
            return;
        }
        Object fingerprint = config.getSchemaFingerprint(state.destination);
        if (Objects.equals(fingerprint, state.schemaFingerprint)) {
            return;
        }

        // Rows already serialized under the old descriptor must reach the stream before the local
        // connection changes schema. This also fixes nextOffset before any retry under the new
        // descriptor begins.
        sendAppend(state);
        drainInFlight(state, false);

        if (config.getSchemaUpdateOptions().isEnabled()) {
            reconcileSchema(state.destination);
        }

        Descriptors.Descriptor descriptor = config.getWriteDescriptor(state.destination);
        LOG.info(
                "The serializer schema for {} changed, reopening buffered stream {} at offset {}",
                state.destination,
                state.streamName,
                state.nextOffset);
        closeAppender(state);
        state.schemaFingerprint = fingerprint;
        state.descriptor = descriptor;
        if (!state.streamName.equals(BufferedStreamWriterState.NO_STREAM)) {
            openAppender(state, "Failed to reopen BigQuery stream after a schema change");
        }
    }

    /** Sends the pending batch: the probe synchronously on a restored stream, pipelined after. */
    private void sendAppend(DestinationState state) throws IOException {
        if (state.pending.getSerializedRowsCount() == 0) {
            return;
        }
        ProtoRows batch = state.pending.build();
        state.pending = ProtoRows.newBuilder();
        state.pendingBytes = 0;
        if (state.probePending) {
            probeRestoredStream(state, batch);
            return;
        }
        ensureStream(state);
        try {
            ApiFuture<AppendRowsResponse> future = state.appender.append(batch, state.nextOffset);
            // Counted after the hand-off, so a batch the client rejects synchronously — reaching
            // BigQuery not at all — is not reported as sent. Everything re-appended later counts
            // as an appendRetries instead.
            metrics.batchAppended(batch);
            state.inFlight.addLast(new InFlightAppend(future, state.nextOffset, batch));
            inFlightCount++;
            state.nextOffset += batch.getSerializedRowsCount();
        } catch (RuntimeException e) {
            throw wrapFailure("Failed to append to BigQuery stream " + state.streamName, e);
        }
    }

    /**
     * Validates acknowledged appends in order, recovering failed ones. With {@code onlyCompleted}
     * the drain stops at the first unfinished future (the opportunistic per-record check);
     * otherwise every in-flight append is awaited (the checkpoint barrier).
     */
    private void drainInFlight(DestinationState state, boolean onlyCompleted) throws IOException {
        while (!state.inFlight.isEmpty()) {
            InFlightAppend head = state.inFlight.peekFirst();
            if (onlyCompleted && !head.future.isDone()) {
                return;
            }
            Throwable failure = awaitFailure(state, head.future, head.expectedOffset);
            state.inFlight.removeFirst();
            inFlightCount--;
            if (failure == null || AppendErrorClassifier.isOffsetAlreadyExists(failure)) {
                // OFFSET_ALREADY_EXISTS outside a replay means a retry of an append that had
                // already landed (the SDK's in-stream retries can race the acknowledgement), so
                // it is a success and is counted as neither a failure nor a send.
                continue;
            }
            metrics.appendFailed(AppendErrorClassifier.statusCode(failure));
            recover(state, head, failure);
        }
    }

    /**
     * Recovers the failed head append. Transient failures — {@code OFFSET_OUT_OF_RANGE} cascades
     * from an earlier, since-repaired failure, and a client-side closed stream writer — are
     * re-appended at the original offset. Row-level rejections escalate to {@link
     * #recoverRowLevel}. Everything else is terminal.
     */
    private void recover(DestinationState state, InFlightAppend failed, Throwable failure)
            throws IOException {
        boolean schemaUpdated = false;
        RetrySchedule schedule = retrySchedule;
        if (AppendErrorClassifier.isSchemaMismatch(failure)
                && config.getSchemaUpdateOptions().isEnabled()) {
            LOG.info(
                    "An append to {} failed with a schema mismatch, reconciling the table schema",
                    state.destination);
            reconcileSchema(state.destination);
            refreshAppenderSchema(state);
            schemaUpdated = true;
            schedule = schemaWaitSchedule;
        }
        Exceptions.AppendSerializtionError rowLevel =
                AppendErrorClassifier.findRowLevel(failure).orElse(null);
        if (rowLevel == null || schemaUpdated) {
            if (AppendErrorClassifier.classify(failure) != AppendErrorClassifier.Kind.TRANSIENT
                    && !AppendErrorClassifier.isOffsetOutOfRange(failure)
                    && !AppendErrorClassifier.isWriterClosed(failure)
                    && !schemaUpdated) {
                throw wrapFailure(
                        "An append to BigQuery stream "
                                + state.streamName
                                + " at offset "
                                + failed.expectedOffset
                                + " failed",
                        failure);
            }
            if (AppendErrorClassifier.isWriterClosed(failure)) {
                reopenAppender(state);
            }
            rowLevel =
                    resendAtSameOffset(
                            state,
                            failed.rows,
                            failed.expectedOffset,
                            failure,
                            schedule,
                            schemaUpdated);
            if (rowLevel == null) {
                return;
            }
        }
        recoverRowLevel(state, failed.rows, failed.expectedOffset, rowLevel);
    }

    /**
     * Synchronously re-appends a batch at its original offset within the retry budget. {@code
     * OFFSET_ALREADY_EXISTS} means the original append landed — success. A row-level rejection is
     * returned to the caller for offset-shifting recovery.
     */
    @Nullable
    private Exceptions.AppendSerializtionError resendAtSameOffset(
            DestinationState state,
            ProtoRows rows,
            long offset,
            Throwable initialFailure,
            RetrySchedule initialSchedule,
            boolean schemaUpdated)
            throws IOException {
        LOG.info(
                "Re-appending {} row(s) to {} at offset {} after: {}",
                rows.getSerializedRowsCount(),
                state.streamName,
                offset,
                initialFailure.toString());
        RetrySchedule schedule = initialSchedule;
        for (int attempt = 1; attempt <= schedule.maxAttempts(); attempt++) {
            Throwable failure = syncAppend(state, rows, offset, false);
            if (failure == null || AppendErrorClassifier.isOffsetAlreadyExists(failure)) {
                return null;
            }
            metrics.appendFailed(AppendErrorClassifier.statusCode(failure));
            boolean schemaMismatch = AppendErrorClassifier.isSchemaMismatch(failure);
            if (schemaMismatch && config.getSchemaUpdateOptions().isEnabled()) {
                if (!schemaUpdated) {
                    LOG.info(
                            "A re-append to {} failed with a schema mismatch, reconciling the"
                                    + " table schema",
                            state.destination);
                    reconcileSchema(state.destination);
                    schemaUpdated = true;
                    schedule = schemaWaitSchedule;
                    attempt = 0;
                    refreshAppenderSchema(state);
                    continue;
                }
                refreshAppenderSchema(state);
            }
            Exceptions.AppendSerializtionError rowLevel =
                    AppendErrorClassifier.findRowLevel(failure).orElse(null);
            if (rowLevel != null) {
                return rowLevel;
            }
            boolean retriable =
                    AppendErrorClassifier.classify(failure) == AppendErrorClassifier.Kind.TRANSIENT
                            || AppendErrorClassifier.isOffsetOutOfRange(failure)
                            || AppendErrorClassifier.isWriterClosed(failure)
                            || (schemaUpdated && schemaMismatch);
            if (!retriable || attempt >= schedule.maxAttempts()) {
                throw wrapFailure(
                        "Re-appending to BigQuery stream "
                                + state.streamName
                                + " at offset "
                                + offset
                                + " failed"
                                + (retriable ? ", the retry budget is exhausted" : "")
                                + " ("
                                + attempt
                                + " attempt(s))",
                        failure);
            }
            if (AppendErrorClassifier.isWriterClosed(failure)) {
                reopenAppender(state);
            }
            sleep(schedule.backoffMs(attempt));
        }
        throw new IllegalStateException("unreachable");
    }

    /**
     * Recovers from a row-level rejection: the rejected request never advanced the stream offset,
     * so its failing rows are routed to the {@link FailureHandler} and the surviving rows — plus
     * every batch appended behind the rejected one, whose pre-assigned offsets are now stale — are
     * replayed with recomputed offsets.
     */
    private void recoverRowLevel(
            DestinationState state,
            ProtoRows rows,
            long offset,
            Exceptions.AppendSerializtionError rowLevel)
            throws IOException {
        ArrayDeque<ProtoRows> replay = new ArrayDeque<>();
        replay.addLast(routeRowLevel(state, rows, rowLevel));
        while (!state.inFlight.isEmpty()) {
            InFlightAppend entry = state.inFlight.removeFirst();
            inFlightCount--;
            // Deliberately not counted under an error class: every append behind a rejected offset
            // fails *because* of that rejection, which is itself counted, so counting these would
            // multiply one incident by the depth of the pipeline. Same rule as the Pub/Sub sink's
            // cascade cancellations.
            Throwable failure = awaitFailure(state, entry.future, entry.expectedOffset);
            if (failure == null || AppendErrorClassifier.isOffsetAlreadyExists(failure)) {
                // Nothing can land beyond a rejected request's offset; an acknowledged later
                // append contradicts that and leaves the stream content unknowable.
                throw new IOException(
                        "An append to BigQuery stream "
                                + state.streamName
                                + " at offset "
                                + entry.expectedOffset
                                + " was acknowledged although the append at offset "
                                + offset
                                + " was rejected; the stream state is inconsistent");
            }
            replay.addLast(entry.rows);
        }
        LOG.info(
                "Replaying {} batch(es) to {} from offset {} after a row-level rejection",
                replay.size(),
                state.streamName,
                offset);
        state.nextOffset = offset;
        replayBatches(state, replay);
    }

    /**
     * Synchronously appends the batches at recomputed offsets starting at the destination's next
     * offset. Row-level rejections along the way route more rows to the handler and continue with
     * the survivors. {@code OFFSET_ALREADY_EXISTS} is terminal here: offsets have shifted, so rows
     * already present would diverge from what this writer appended.
     */
    private void replayBatches(DestinationState state, ArrayDeque<ProtoRows> replay)
            throws IOException {
        int attempt = 0;
        boolean schemaUpdated = false;
        RetrySchedule schedule = retrySchedule;
        while (!replay.isEmpty()) {
            ProtoRows batch = replay.peekFirst();
            if (batch.getSerializedRowsCount() == 0) {
                replay.removeFirst();
                continue;
            }
            Throwable failure = syncAppend(state, batch, state.nextOffset, false);
            if (failure == null) {
                replay.removeFirst();
                state.nextOffset += batch.getSerializedRowsCount();
                attempt = 0;
                continue;
            }
            metrics.appendFailed(AppendErrorClassifier.statusCode(failure));
            boolean schemaMismatch = AppendErrorClassifier.isSchemaMismatch(failure);
            if (schemaMismatch && config.getSchemaUpdateOptions().isEnabled()) {
                if (!schemaUpdated) {
                    LOG.info(
                            "A replayed append to {} failed with a schema mismatch, reconciling"
                                    + " the table schema",
                            state.destination);
                    reconcileSchema(state.destination);
                    schemaUpdated = true;
                    schedule = schemaWaitSchedule;
                    attempt = 0;
                    refreshAppenderSchema(state);
                    continue;
                }
                refreshAppenderSchema(state);
            }
            Exceptions.AppendSerializtionError rowLevel =
                    AppendErrorClassifier.findRowLevel(failure).orElse(null);
            if (rowLevel != null && !schemaMismatch) {
                ProtoRows survivors = routeRowLevel(state, batch, rowLevel);
                if (survivors.getSerializedRowsCount() >= batch.getSerializedRowsCount()) {
                    // No row matched the reported indices, so nothing was dropped; re-appending
                    // the identical batch could never make progress (mirrors the default-stream
                    // writer's guard in retryBatches).
                    throw wrapFailure(
                            "A replayed append to BigQuery stream "
                                    + state.streamName
                                    + " failed with row errors matching none of the batch's rows"
                                    + " ("
                                    + attempt
                                    + " attempt(s))",
                            failure);
                }
                replay.removeFirst();
                replay.addFirst(survivors);
                attempt = 0;
                continue;
            }
            attempt++;
            boolean retriable =
                    AppendErrorClassifier.classify(failure) == AppendErrorClassifier.Kind.TRANSIENT
                            || AppendErrorClassifier.isWriterClosed(failure)
                            || (schemaUpdated && schemaMismatch);
            if (!retriable || attempt >= schedule.maxAttempts()) {
                throw wrapFailure(
                        "Replaying an append to BigQuery stream "
                                + state.streamName
                                + " at offset "
                                + state.nextOffset
                                + " failed"
                                + (retriable ? ", the retry budget is exhausted" : "")
                                + " ("
                                + attempt
                                + " attempt(s))",
                        failure);
            }
            if (AppendErrorClassifier.isWriterClosed(failure)) {
                reopenAppender(state);
            }
            sleep(schedule.backoffMs(attempt));
        }
    }

    /**
     * Replaces a client-side-closed appender with a fresh one on the same stream — the SDK's {@code
     * StreamWriter} poisons itself after a connection-level failure, but the stream is unaffected
     * and re-appends at the same offsets stay valid ({@code OFFSET_ALREADY_EXISTS} still means the
     * original landed).
     */
    private void reopenAppender(DestinationState state) throws IOException {
        LOG.info("Reopening the writer of stream {} after a client-side close", state.streamName);
        closeAppender(state);
        openAppender(state, "Failed to reopen BigQuery stream");
    }

    /** Reopens the same remote stream with the serializer's current descriptor. */
    private void refreshAppenderSchema(DestinationState state) throws IOException {
        Object fingerprint = config.getSchemaFingerprint(state.destination);
        Descriptors.Descriptor descriptor = config.getWriteDescriptor(state.destination);
        closeAppender(state);
        state.schemaFingerprint = fingerprint;
        state.descriptor = descriptor;
        openAppender(state, "Failed to reopen BigQuery stream after reconciling its schema");
    }

    private void closeAppender(DestinationState state) {
        if (state.appender == null) {
            return;
        }
        state.appender.close();
        state.appender = null;
    }

    private void openAppender(DestinationState state, String failureMessage) throws IOException {
        ensureService();
        try {
            state.appender = service.openAppender(state.streamName, state.descriptor);
        } catch (IOException | RuntimeException e) {
            throw wrapFailure(failureMessage + " " + state.streamName, e);
        }
    }

    private StorageWriteSchemaReconciler.Outcome reconcileSchema(TableDestination destination)
            throws IOException {
        StorageWriteSchemaReconciler.Outcome outcome = schemaReconciler.reconcile(destination);
        if (outcome == StorageWriteSchemaReconciler.Outcome.UPDATED) {
            metrics.schemaReconciled();
        }
        return outcome;
    }

    /**
     * Routes a row-level append failure to the {@link FailureHandler} row by row and returns the
     * surviving rows. The handler decides per row: returning normally drops the row, throwing fails
     * the writer.
     */
    private ProtoRows routeRowLevel(
            DestinationState state, ProtoRows rows, Exceptions.AppendSerializtionError rowLevel)
            throws IOException {
        Map<Integer, String> rowErrors = rowLevel.getRowIndexToErrorMessage();
        ProtoRows.Builder survivors = ProtoRows.newBuilder();
        for (int i = 0; i < rows.getSerializedRowsCount(); i++) {
            String errorMessage = rowErrors.get(i);
            if (errorMessage == null) {
                survivors.addSerializedRows(rows.getSerializedRows(i));
            } else {
                metrics.rowFailed();
                failedRowHandler.handle(
                        FailedRow.of(
                                state.destination,
                                rows.getSerializedRows(i),
                                errorMessage,
                                rowLevel));
            }
        }
        return survivors.build();
    }

    // ------------------------------------------------------------------
    // Stream lifecycle
    // ------------------------------------------------------------------

    /**
     * Validates the restored stream with a synchronous probe append of the first replayed batch at
     * the restored offset. Success adopts the stream; offset conflicts, a dead stream or a failure
     * to open the appender abandon it for a fresh one; transient failures retry within the budget.
     */
    private void probeRestoredStream(DestinationState state, ProtoRows batch) throws IOException {
        ensureService();
        RetrySchedule schedule = retrySchedule;
        boolean schemaUpdated = false;
        for (int attempt = 1; attempt <= schedule.maxAttempts(); attempt++) {
            if (state.appender == null) {
                try {
                    state.appender = service.openAppender(state.streamName, state.descriptor);
                } catch (IOException | RuntimeException e) {
                    LOG.warn(
                            "Failed to reopen restored stream {}, abandoning it",
                            state.streamName,
                            e);
                    abandonRestoredStream(state, batch);
                    return;
                }
            }
            // The probe carries the batch sendAppend handed over, so its first attempt is that
            // batch's hand-off; later attempts (and the replay onto a fresh stream, if the
            // restored one is abandoned) re-append rows already counted.
            Throwable failure = syncAppend(state, batch, state.nextOffset, attempt == 1);
            if (failure == null) {
                LOG.info(
                        "Restored stream {} accepted the probe append at offset {}, reusing it",
                        state.streamName,
                        state.nextOffset);
                state.nextOffset += batch.getSerializedRowsCount();
                state.probePending = false;
                return;
            }
            metrics.appendFailed(AppendErrorClassifier.statusCode(failure));
            boolean schemaMismatch = AppendErrorClassifier.isSchemaMismatch(failure);
            if (schemaMismatch && config.getSchemaUpdateOptions().isEnabled()) {
                if (!schemaUpdated) {
                    LOG.info(
                            "The restored stream {} rejected the current serializer schema,"
                                    + " reconciling {}",
                            state.streamName,
                            state.destination);
                    reconcileSchema(state.destination);
                    schemaUpdated = true;
                    schedule = schemaWaitSchedule;
                    attempt = 0;
                    refreshAppenderSchema(state);
                    continue;
                }
                if (attempt >= schedule.maxAttempts()) {
                    throw wrapFailure(
                            "Probing restored BigQuery stream "
                                    + state.streamName
                                    + " at offset "
                                    + state.nextOffset
                                    + " failed after reconciling the table schema; the retry"
                                    + " budget is exhausted ("
                                    + attempt
                                    + " attempt(s))",
                            failure);
                }
                refreshAppenderSchema(state);
                sleep(schedule.backoffMs(attempt));
                continue;
            }
            if (AppendErrorClassifier.isWriterClosed(failure)) {
                if (attempt >= schedule.maxAttempts()) {
                    throw wrapFailure(
                            "Probing restored BigQuery stream "
                                    + state.streamName
                                    + " at offset "
                                    + state.nextOffset
                                    + " failed because the writer stayed closed ("
                                    + attempt
                                    + " attempt(s))",
                            failure);
                }
                reopenAppender(state);
                sleep(schedule.backoffMs(attempt));
                continue;
            }
            if (AppendErrorClassifier.isOffsetAlreadyExists(failure)
                    || AppendErrorClassifier.isOffsetOutOfRange(failure)
                    || AppendErrorClassifier.requiresWriterRefresh(failure)) {
                LOG.info(
                        "Restored stream {} rejected the probe append at offset {} ({}),"
                                + " abandoning it",
                        state.streamName,
                        state.nextOffset,
                        failure.toString());
                abandonRestoredStream(state, batch);
                return;
            }
            Exceptions.AppendSerializtionError rowLevel =
                    AppendErrorClassifier.findRowLevel(failure).orElse(null);
            if (rowLevel != null) {
                // The stream itself is usable; recover the batch's rows on it.
                state.probePending = false;
                recoverRowLevel(state, batch, state.nextOffset, rowLevel);
                return;
            }
            if (AppendErrorClassifier.classify(failure) != AppendErrorClassifier.Kind.TRANSIENT
                    || attempt >= schedule.maxAttempts()) {
                throw wrapFailure(
                        "Probing restored BigQuery stream "
                                + state.streamName
                                + " at offset "
                                + state.nextOffset
                                + " failed ("
                                + attempt
                                + " attempt(s))",
                        failure);
            }
            sleep(schedule.backoffMs(attempt));
        }
    }

    /**
     * Abandons the restored stream and replays the probe batch onto a fresh stream from offset
     * zero. The old stream is left open: a restored-but-uncommitted committable may still have to
     * flush it (BigQuery rejects {@code FlushRows} on a finalized stream), and its tail past the
     * restored offset is never named by a committable, so it stays invisible without cleanup.
     */
    private void abandonRestoredStream(DestinationState state, ProtoRows batch) throws IOException {
        closeAppender(state);
        state.streamName = BufferedStreamWriterState.NO_STREAM;
        state.nextOffset = 0;
        state.probePending = false;
        ensureStream(state);
        ArrayDeque<ProtoRows> replay = new ArrayDeque<>();
        replay.addLast(batch);
        replayBatches(state, replay);
    }

    /** Creates the stream and opens its appender if not yet open. */
    private void ensureStream(DestinationState state) throws IOException {
        if (state.appender != null) {
            return;
        }
        ensureService();
        if (state.streamName.equals(BufferedStreamWriterState.NO_STREAM)) {
            state.streamName = createStream(state);
            state.nextOffset = 0;
            LOG.info(
                    "Created buffered stream {} for destination {} in subtask {}",
                    state.streamName,
                    state.destination,
                    subtaskId);
        }
        openAppender(state, "Failed to open BigQuery stream");
    }

    /**
     * Creates a buffered stream on the destination table. A missing table is created under {@code
     * CREATE_IF_NEEDED} and the stream creation retried while the table metadata propagates to the
     * Storage Write API backend. The creation itself is repeated by the {@link TableAdmin} the sink
     * wired ({@code RetryingTableAdmin}, on this method's own recovery schedule), so a subtask that
     * loses the creation race to the per-table quota rather than to an HTTP 409 waits its turn
     * instead of failing the write.
     */
    private String createStream(DestinationState state) throws IOException {
        boolean tableCreated = false;
        for (int attempt = 1; attempt <= retrySchedule.maxAttempts(); attempt++) {
            Throwable failure;
            try {
                return service.createBufferedStream(state.destination);
            } catch (IOException | RuntimeException e) {
                failure = e;
            }
            // The wide missing-table verdict, not NOT_FOUND alone: the real service masks a table
            // that is not there as PERMISSION_DENIED (see AppendErrorClassifier#isMissingTable),
            // so NOT_FOUND alone never fires outside the emulator.
            boolean notFound = AppendErrorClassifier.isMissingTable(failure);
            if (notFound
                    && !tableCreated
                    && config.getCreateDisposition() == CreateDisposition.CREATE_IF_NEEDED) {
                LOG.info(
                        "Destination table {} may not exist, creating it (CREATE_IF_NEEDED)",
                        state.destination);
                tableAdmin.create(
                        state.destination,
                        config.getTableSchema(state.destination),
                        config.getTableCreateOptionsProvider().optionsFor(state.destination));
                tableCreated = true;
            } else if (!(notFound && tableCreated)
                    && AppendErrorClassifier.classify(failure)
                            != AppendErrorClassifier.Kind.TRANSIENT) {
                throw wrapFailure(
                        "Failed to create a BigQuery buffered stream on " + state.destination,
                        failure);
            }
            if (attempt >= retrySchedule.maxAttempts()) {
                throw wrapFailure(
                        "Failed to create a BigQuery buffered stream on "
                                + state.destination
                                + (tableCreated ? " after a table-creation attempt" : "")
                                + ", the retry budget is exhausted ("
                                + attempt
                                + " attempt(s))",
                        failure);
            }
            sleep(retrySchedule.backoffMs(attempt));
        }
        throw new IllegalStateException("unreachable");
    }

    /**
     * Releases clean destination-local resources after a successful non-end-of-input flush. The
     * remote buffered stream is deliberately left unfinalized: a restored committable may still
     * need to flush it, and BigQuery removes unfinalized streams under its retention policy.
     */
    private void evictIdleDestinations() {
        long now = nanoClock.getAsLong();
        Iterator<Map.Entry<TableDestination, DestinationState>> iterator =
                destinations.entrySet().iterator();
        while (iterator.hasNext()) {
            DestinationState state = iterator.next().getValue();
            boolean clean =
                    state.streamName.equals(state.lastSnapshotStreamName)
                            && state.nextOffset == state.lastSnapshotOffset;
            boolean idle = now - state.lastAccessNanos > destinationIdleTimeoutNanos;
            if (!idle
                    || !clean
                    || state.pending.getSerializedRowsCount() != 0
                    || !state.inFlight.isEmpty()) {
                continue;
            }
            iterator.remove();
            if (state.appender != null) {
                try {
                    state.appender.close();
                } catch (RuntimeException e) {
                    LOG.warn(
                            "Failed to close the local writer for idle destination {} and stream"
                                    + " {}; the destination was evicted and the remote stream was"
                                    + " left unfinalized",
                            state.destination,
                            state.streamName,
                            e);
                }
                state.appender = null;
            }
            LOG.info(
                    "Evicted idle destination {} and left buffered stream {} unfinalized",
                    state.destination,
                    state.streamName);
        }
    }

    private void ensureService() throws IOException {
        if (service == null) {
            service = serviceFactory.create(config.getLocation(), options);
        }
    }

    // ------------------------------------------------------------------
    // Append plumbing
    // ------------------------------------------------------------------

    /** Appends synchronously and returns the failure, or {@code null} on success. */
    @Nullable
    private Throwable syncAppend(
            DestinationState state, ProtoRows rows, long offset, boolean firstAttempt)
            throws IOException {
        ApiFuture<AppendRowsResponse> future;
        try {
            future = state.appender.append(rows, offset);
        } catch (RuntimeException e) {
            return e;
        }
        // The flag carries the metric's whole contract: exactly one caller hands a batch over for
        // the first time (the restored-stream probe), and every other caller here is re-appending
        // rows sendAppend or that probe already counted.
        if (firstAttempt) {
            metrics.batchAppended(rows);
        } else {
            metrics.appendRetried();
        }
        return awaitFailure(state, future, offset);
    }

    /**
     * Awaits an append future and returns the failure it completed with — unwrapped from the
     * execution exception, or derived from the response — or {@code null} on success. A successful
     * response acknowledging a different offset than requested is a failure: the writer's offset
     * accounting and the stream have diverged.
     */
    @Nullable
    private Throwable awaitFailure(
            DestinationState state, ApiFuture<AppendRowsResponse> future, long expectedOffset)
            throws IOException {
        AppendRowsResponse response;
        try {
            response = future.get();
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while awaiting appends to BigQuery", e);
        }
        Throwable failure = responseToThrowable(state, response);
        if (failure != null) {
            return failure;
        }
        if (response.hasAppendResult() && response.getAppendResult().hasOffset()) {
            long acknowledged = response.getAppendResult().getOffset().getValue();
            if (acknowledged != expectedOffset) {
                return new IOException(
                        "BigQuery acknowledged an append to "
                                + state.streamName
                                + " at offset "
                                + acknowledged
                                + " although offset "
                                + expectedOffset
                                + " was requested; the stream state is inconsistent");
            }
        }
        return null;
    }

    /**
     * Maps a completed append response to the failure it carries, or {@code null} for a clean
     * response. Mirrors the default-stream writer's mapping, minus the schema-update repairs this
     * write method does not perform: transient codes become synthesized status exceptions (whole
     * batch retried), typed storage errors (offsets, stream state) surface as such, row errors
     * become a synthesized row-level error, anything else is terminal.
     */
    @Nullable
    private Throwable responseToThrowable(DestinationState state, AppendRowsResponse response) {
        if (response.hasError()) {
            if (AppendErrorClassifier.isTransientCode(response.getError().getCode())) {
                return Status.fromCodeValue(response.getError().getCode())
                        .withDescription(response.getError().getMessage())
                        .asRuntimeException();
            }
            Throwable storageError = AppendErrorClassifier.toStorageException(response.getError());
            if (storageError == null) {
                storageError = StatusProto.toStatusRuntimeException(response.getError());
            }
            if (response.getRowErrorsCount() == 0) {
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
                    "An append to BigQuery stream "
                            + state.streamName
                            + " completed with "
                            + response.getRowErrorsCount()
                            + " row error(s)",
                    response.getWriteStream(),
                    rowErrors);
        }
        return null;
    }

    /** Wraps unconditionally: the call-site context (stream, offset, attempts) must survive. */
    private IOException wrapFailure(String message, Throwable cause) {
        if (AppendErrorClassifier.isSchemaMismatch(cause)) {
            if (config.getSchemaUpdateOptions().isEnabled()) {
                message +=
                        " because the rows remain incompatible after reconciling the table schema";
            } else {
                message +=
                        " because the rows carry fields the table does not have; update the table"
                                + " schema before changing the serializer and wait for Storage"
                                + " Write API propagation, or enable schemaUpdateOptions(...)"
                                + " (Table API: set 'sink.schema-update.allow-new-fields' ="
                                + " 'true')";
            }
        }
        return new IOException(message, cause);
    }

    private static void sleep(long millis) throws IOException {
        Retries.sleep(millis, "Interrupted while waiting to retry appends to BigQuery");
    }
}
