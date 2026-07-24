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

package io.github.flink.gcp.connector.bigquery.sink.storageapi.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.FixedDestinationResolver;
import io.github.flink.gcp.connector.bigquery.sink.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRow;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRowHandler;
import io.github.flink.gcp.connector.bigquery.sink.storageapi.BufferedStreamCommittable;
import io.github.flink.gcp.connector.bigquery.sink.storageapi.BufferedStreamOptions;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Exactly-once {@code SinkWriter} appending rows to one application-created BUFFERED Storage Write
 * API stream at explicit offsets. Rows stay invisible until the committer flushes them ({@code
 * FlushRows}) as part of a completed checkpoint's commit — the two-phase-commit contract.
 *
 * <p><b>Stream lifecycle.</b> Each subtask owns one buffered stream, created lazily on the first
 * append and <em>reused across checkpoints</em> (frequent {@code CreateWriteStream} churn is
 * explicitly not intended usage of the API). The stream name and the next append offset are Flink
 * writer state; {@link #prepareCommit()} emits one committable naming the offset a completed
 * checkpoint may flush up to. On a clean run a stream is created once per writer lifetime.
 *
 * <p><b>Restore.</b> A restored writer probes its stream with the first replayed batch,
 * synchronously, at the restored offset: success adopts the stream; {@code OFFSET_ALREADY_EXISTS}
 * (the pre-crash attempt appended past the restored offset), {@code OFFSET_OUT_OF_RANGE}, a
 * finalized/unknown stream, or a failure to open the appender abandon it — the old stream is
 * best-effort finalized and a fresh one starts at offset zero. Nothing appended past the restored
 * offset was ever named by a committable, so it can never be flushed and never becomes visible; a
 * restored-but-uncommitted committable of the old stream still flushes fine, because finalizing
 * blocks appends only.
 *
 * <p><b>Error handling.</b> Serialization failures and oversized rows are routed to the {@link
 * FailedRowHandler} before any stream exists. Transient append failures surfacing past the SDK's
 * in-stream retries are re-appended at their original offset within a bounded budget ({@code
 * OFFSET_ALREADY_EXISTS} then means the original landed — success). A row-level rejection discards
 * nothing silently: the rejected batch's failing rows go to the handler and the surviving rows,
 * plus every batch appended behind the rejected one, are replayed with recomputed offsets (an
 * append request is rejected atomically, so the offset never advanced). During such a replay {@code
 * OFFSET_ALREADY_EXISTS} is terminal — offsets have shifted, and rows already present there would
 * silently diverge from what the writer believes was appended. Schema mismatches are terminal: a
 * buffered stream's schema is pinned at creation, and mid-stream schema evolution is out of scope
 * for this write method.
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
    private final FailedRowHandler failedRowHandler;
    private final TableDestination destination;
    private final int subtaskId;
    private final long maxAppendRequestBytes;
    private final RetrySchedule retrySchedule;

    /** Streams of unadopted restored states (scale-down), finalized best-effort on first use. */
    private final List<String> abandonedRestoredStreams = new ArrayList<>();

    @Nullable private BufferedStreamService service;
    @Nullable private OffsetRowAppender appender;
    @Nullable private Descriptors.Descriptor descriptor;

    private String streamName;
    private long nextOffset;
    private String lastSnapshotStreamName;
    private long lastSnapshotOffset;

    /** Whether the restored stream still has to be validated by the synchronous probe append. */
    private boolean probePending;

    private ProtoRows.Builder pending = ProtoRows.newBuilder();
    private long pendingBytes;

    /** Appends issued but not yet acknowledged, in append order (task thread only). */
    private final ArrayDeque<InFlightAppend> inFlight = new ArrayDeque<>();

    /**
     * Creates a writer, fresh or restored.
     *
     * @param config the sink configuration (must carry a fixed destination)
     * @param options the buffered-stream options
     * @param serviceFactory the Storage Write API service factory
     * @param tableAdmin the admin for creating the destination table
     * @param subtaskId the subtask index (diagnostics and committable attribution)
     * @param restoredStates the restored writer states; empty for a fresh writer
     */
    public BigQueryBufferedStreamWriter(
            BigQuerySinkConfig<T> config,
            BufferedStreamOptions options,
            BufferedStreamServiceFactory serviceFactory,
            TableAdmin tableAdmin,
            int subtaskId,
            Collection<BufferedStreamWriterState> restoredStates) {
        this.config = Preconditions.checkNotNull(config, "config must not be null");
        Preconditions.checkNotNull(options, "options must not be null");
        this.serviceFactory =
                Preconditions.checkNotNull(serviceFactory, "serviceFactory must not be null");
        this.tableAdmin = Preconditions.checkNotNull(tableAdmin, "tableAdmin must not be null");
        this.failedRowHandler = config.getFailedRowHandler();
        Preconditions.checkArgument(
                config.getDestinationResolver() instanceof FixedDestinationResolver,
                "The buffered-stream writer requires a fixed destination");
        this.destination =
                ((FixedDestinationResolver) config.getDestinationResolver()).getDestination();
        this.subtaskId = subtaskId;
        this.maxAppendRequestBytes = options.getMaxAppendRequestBytes();
        this.retrySchedule =
                new RetrySchedule(
                        options.getRetryInitialBackoff().toMillis(),
                        options.getRetryMaxBackoff().toMillis(),
                        options.getRetryMaxAttempts(),
                        0);

        BufferedStreamWriterState adopted = null;
        for (BufferedStreamWriterState state : restoredStates) {
            if (adopted == null || state.getCheckpointId() > adopted.getCheckpointId()) {
                adopted = state;
            }
        }
        for (BufferedStreamWriterState state : restoredStates) {
            if (state != adopted
                    && !state.getStreamName().equals(BufferedStreamWriterState.NO_STREAM)) {
                abandonedRestoredStreams.add(state.getStreamName());
            }
        }
        this.streamName =
                adopted == null ? BufferedStreamWriterState.NO_STREAM : adopted.getStreamName();
        this.nextOffset = adopted == null ? 0 : adopted.getNextOffset();
        this.lastSnapshotStreamName = streamName;
        this.lastSnapshotOffset = nextOffset;
        this.probePending = !streamName.equals(BufferedStreamWriterState.NO_STREAM);
        if (adopted != null) {
            LOG.info(
                    "Restored subtask {} with stream {} at offset {} ({} sibling state(s)"
                            + " abandoned)",
                    subtaskId,
                    streamName,
                    nextOffset,
                    abandonedRestoredStreams.size());
        }
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        drainInFlight(true);
        ByteString row;
        try {
            // A poison record must reach the handler no matter how the serializer fails,
            // without creating a stream (or auto-creating a table) it may never need.
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
        if (row.size() > BigQueryDefaultStreamWriter.MAX_ROW_BYTES) {
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
        if (pending.getSerializedRowsCount() > 0
                && pendingBytes + row.size() > maxAppendRequestBytes) {
            sendAppend();
        }
        pending.addSerializedRows(row);
        pendingBytes += row.size();
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        sendAppend();
        drainInFlight(false);
    }

    @Override
    public Collection<BufferedStreamCommittable> prepareCommit() {
        if (streamName.equals(BufferedStreamWriterState.NO_STREAM)
                || nextOffset == 0
                || (streamName.equals(lastSnapshotStreamName)
                        && nextOffset == lastSnapshotOffset)) {
            return Collections.emptyList();
        }
        // FlushRows offsets are inclusive: nextOffset - 1 is the last appended row.
        return Collections.singletonList(
                new BufferedStreamCommittable(streamName, nextOffset - 1, subtaskId));
    }

    @Override
    public List<BufferedStreamWriterState> snapshotState(long checkpointId) {
        lastSnapshotStreamName = streamName;
        lastSnapshotOffset = nextOffset;
        return Collections.singletonList(
                new BufferedStreamWriterState(streamName, nextOffset, checkpointId));
    }

    @Override
    public void close() throws Exception {
        try {
            boolean uncommittedTail =
                    !streamName.equals(BufferedStreamWriterState.NO_STREAM)
                            && (!streamName.equals(lastSnapshotStreamName)
                                    || nextOffset != lastSnapshotOffset);
            if (uncommittedTail && service != null) {
                // Rows appended past the last snapshot can never be flushed; finalizing marks the
                // stream dead so a later restore abandons it immediately instead of probing into
                // it. Best-effort: a failure here only costs the restore a probe round-trip.
                finalizeBestEffort(streamName);
            }
        } finally {
            if (appender != null) {
                appender.close();
                appender = null;
            }
            if (service != null) {
                service.close();
                service = null;
            }
            failedRowHandler.close();
        }
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

    /** Sends the pending batch: the probe synchronously on a restored stream, pipelined after. */
    private void sendAppend() throws IOException {
        if (pending.getSerializedRowsCount() == 0) {
            return;
        }
        ProtoRows batch = pending.build();
        pending = ProtoRows.newBuilder();
        pendingBytes = 0;
        if (probePending) {
            probeRestoredStream(batch);
            return;
        }
        ensureStream();
        try {
            ApiFuture<AppendRowsResponse> future = appender.append(batch, nextOffset);
            inFlight.addLast(new InFlightAppend(future, nextOffset, batch));
            nextOffset += batch.getSerializedRowsCount();
        } catch (RuntimeException e) {
            throw wrapFailure("Failed to append to BigQuery stream " + streamName, e);
        }
    }

    /**
     * Validates acknowledged appends in order, recovering failed ones. With {@code onlyCompleted}
     * the drain stops at the first unfinished future (the opportunistic per-record check);
     * otherwise every in-flight append is awaited (the checkpoint barrier).
     */
    private void drainInFlight(boolean onlyCompleted) throws IOException {
        while (!inFlight.isEmpty()) {
            InFlightAppend head = inFlight.peekFirst();
            if (onlyCompleted && !head.future.isDone()) {
                return;
            }
            Throwable failure = awaitFailure(head.future, head.expectedOffset);
            inFlight.removeFirst();
            if (failure == null || AppendErrorClassifier.isOffsetAlreadyExists(failure)) {
                // OFFSET_ALREADY_EXISTS outside a replay means a retry of an append that had
                // already landed (the SDK's in-stream retries can race the acknowledgement).
                continue;
            }
            recover(head, failure);
        }
    }

    /**
     * Recovers the failed head append. Transient failures — and {@code OFFSET_OUT_OF_RANGE}
     * cascades from an earlier, since-repaired failure — are re-appended at the original offset.
     * Row-level rejections escalate to {@link #recoverRowLevel}. Everything else is terminal.
     */
    private void recover(InFlightAppend failed, Throwable failure) throws IOException {
        Exceptions.AppendSerializtionError rowLevel =
                AppendErrorClassifier.findRowLevel(failure).orElse(null);
        if (rowLevel == null) {
            if (AppendErrorClassifier.classify(failure) != AppendErrorClassifier.Kind.TRANSIENT
                    && !AppendErrorClassifier.isOffsetOutOfRange(failure)) {
                throw wrapFailure(
                        "An append to BigQuery stream "
                                + streamName
                                + " at offset "
                                + failed.expectedOffset
                                + " failed",
                        failure);
            }
            rowLevel = resendAtSameOffset(failed.rows, failed.expectedOffset, failure);
            if (rowLevel == null) {
                return;
            }
        }
        recoverRowLevel(failed.rows, failed.expectedOffset, rowLevel);
    }

    /**
     * Synchronously re-appends a batch at its original offset within the retry budget. {@code
     * OFFSET_ALREADY_EXISTS} means the original append landed — success. A row-level rejection is
     * returned to the caller for offset-shifting recovery.
     */
    @Nullable
    private Exceptions.AppendSerializtionError resendAtSameOffset(
            ProtoRows rows, long offset, Throwable initialFailure) throws IOException {
        LOG.info(
                "Re-appending {} row(s) to {} at offset {} after: {}",
                rows.getSerializedRowsCount(),
                streamName,
                offset,
                initialFailure.toString());
        for (int attempt = 1; attempt <= retrySchedule.maxAttempts(); attempt++) {
            Throwable failure = syncAppend(rows, offset);
            if (failure == null || AppendErrorClassifier.isOffsetAlreadyExists(failure)) {
                return null;
            }
            Exceptions.AppendSerializtionError rowLevel =
                    AppendErrorClassifier.findRowLevel(failure).orElse(null);
            if (rowLevel != null) {
                return rowLevel;
            }
            boolean retriable =
                    AppendErrorClassifier.classify(failure) == AppendErrorClassifier.Kind.TRANSIENT
                            || AppendErrorClassifier.isOffsetOutOfRange(failure);
            if (!retriable || attempt >= retrySchedule.maxAttempts()) {
                throw wrapFailure(
                        "Re-appending to BigQuery stream "
                                + streamName
                                + " at offset "
                                + offset
                                + " failed"
                                + (retriable ? ", the retry budget is exhausted" : "")
                                + " ("
                                + attempt
                                + " attempt(s))",
                        failure);
            }
            sleep(retrySchedule.backoffMs(attempt));
        }
        throw new IllegalStateException("unreachable");
    }

    /**
     * Recovers from a row-level rejection: the rejected request never advanced the stream offset,
     * so its failing rows are routed to the {@link FailedRowHandler} and the surviving rows — plus
     * every batch appended behind the rejected one, whose pre-assigned offsets are now stale — are
     * replayed with recomputed offsets.
     */
    private void recoverRowLevel(
            ProtoRows rows, long offset, Exceptions.AppendSerializtionError rowLevel)
            throws IOException {
        ArrayDeque<ProtoRows> replay = new ArrayDeque<>();
        replay.addLast(routeRowLevel(rows, rowLevel));
        while (!inFlight.isEmpty()) {
            InFlightAppend entry = inFlight.removeFirst();
            Throwable failure = awaitFailure(entry.future, entry.expectedOffset);
            if (failure == null || AppendErrorClassifier.isOffsetAlreadyExists(failure)) {
                // Nothing can land beyond a rejected request's offset; an acknowledged later
                // append contradicts that and leaves the stream content unknowable.
                throw new IOException(
                        "An append to BigQuery stream "
                                + streamName
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
                streamName,
                offset);
        nextOffset = offset;
        replayBatches(replay);
    }

    /**
     * Synchronously appends the batches at recomputed offsets starting at {@link #nextOffset}.
     * Row-level rejections along the way route more rows to the handler and continue with the
     * survivors. {@code OFFSET_ALREADY_EXISTS} is terminal here: offsets have shifted, so rows
     * already present would diverge from what this writer appended.
     */
    private void replayBatches(ArrayDeque<ProtoRows> replay) throws IOException {
        int attempt = 0;
        while (!replay.isEmpty()) {
            ProtoRows batch = replay.peekFirst();
            if (batch.getSerializedRowsCount() == 0) {
                replay.removeFirst();
                continue;
            }
            Throwable failure = syncAppend(batch, nextOffset);
            if (failure == null) {
                replay.removeFirst();
                nextOffset += batch.getSerializedRowsCount();
                attempt = 0;
                continue;
            }
            Exceptions.AppendSerializtionError rowLevel =
                    AppendErrorClassifier.findRowLevel(failure).orElse(null);
            if (rowLevel != null) {
                replay.removeFirst();
                replay.addFirst(routeRowLevel(batch, rowLevel));
                attempt = 0;
                continue;
            }
            attempt++;
            boolean retriable =
                    AppendErrorClassifier.classify(failure) == AppendErrorClassifier.Kind.TRANSIENT;
            if (!retriable || attempt >= retrySchedule.maxAttempts()) {
                throw wrapFailure(
                        "Replaying an append to BigQuery stream "
                                + streamName
                                + " at offset "
                                + nextOffset
                                + " failed"
                                + (retriable ? ", the retry budget is exhausted" : "")
                                + " ("
                                + attempt
                                + " attempt(s))",
                        failure);
            }
            sleep(retrySchedule.backoffMs(attempt));
        }
    }

    /**
     * Routes a row-level append failure to the {@link FailedRowHandler} row by row and returns the
     * surviving rows. The handler decides per row: returning normally drops the row, throwing fails
     * the writer.
     */
    private ProtoRows routeRowLevel(ProtoRows rows, Exceptions.AppendSerializtionError rowLevel)
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

    // ------------------------------------------------------------------
    // Stream lifecycle
    // ------------------------------------------------------------------

    /**
     * Validates the restored stream with a synchronous probe append of the first replayed batch at
     * the restored offset. Success adopts the stream; offset conflicts, a dead stream or a failure
     * to open the appender abandon it for a fresh one; transient failures retry within the budget.
     */
    private void probeRestoredStream(ProtoRows batch) throws IOException {
        ensureService();
        for (int attempt = 1; attempt <= retrySchedule.maxAttempts(); attempt++) {
            if (appender == null) {
                try {
                    appender = service.openAppender(streamName, descriptor());
                } catch (IOException | RuntimeException e) {
                    LOG.warn("Failed to reopen restored stream {}, abandoning it", streamName, e);
                    abandonRestoredStream(batch);
                    return;
                }
            }
            Throwable failure = syncAppend(batch, nextOffset);
            if (failure == null) {
                LOG.info(
                        "Restored stream {} accepted the probe append at offset {}, reusing it",
                        streamName,
                        nextOffset);
                nextOffset += batch.getSerializedRowsCount();
                probePending = false;
                return;
            }
            if (AppendErrorClassifier.isOffsetAlreadyExists(failure)
                    || AppendErrorClassifier.isOffsetOutOfRange(failure)
                    || AppendErrorClassifier.requiresWriterRefresh(failure)) {
                LOG.info(
                        "Restored stream {} rejected the probe append at offset {} ({}),"
                                + " abandoning it",
                        streamName,
                        nextOffset,
                        failure.toString());
                abandonRestoredStream(batch);
                return;
            }
            Exceptions.AppendSerializtionError rowLevel =
                    AppendErrorClassifier.findRowLevel(failure).orElse(null);
            if (rowLevel != null) {
                // The stream itself is usable; recover the batch's rows on it.
                probePending = false;
                recoverRowLevel(batch, nextOffset, rowLevel);
                return;
            }
            if (AppendErrorClassifier.classify(failure) != AppendErrorClassifier.Kind.TRANSIENT
                    || attempt >= retrySchedule.maxAttempts()) {
                throw wrapFailure(
                        "Probing restored BigQuery stream "
                                + streamName
                                + " at offset "
                                + nextOffset
                                + " failed ("
                                + attempt
                                + " attempt(s))",
                        failure);
            }
            sleep(retrySchedule.backoffMs(attempt));
        }
    }

    /**
     * Abandons the restored stream — nothing past the restored offset was ever committable, so
     * finalizing it (best-effort) loses nothing and keeps restored-but-uncommitted flushes of it
     * working — and replays the probe batch onto a fresh stream from offset zero.
     */
    private void abandonRestoredStream(ProtoRows batch) throws IOException {
        if (appender != null) {
            appender.close();
            appender = null;
        }
        finalizeBestEffort(streamName);
        streamName = BufferedStreamWriterState.NO_STREAM;
        nextOffset = 0;
        probePending = false;
        ensureStream();
        ArrayDeque<ProtoRows> replay = new ArrayDeque<>();
        replay.addLast(batch);
        replayBatches(replay);
    }

    /** Creates the stream and opens its appender if not yet open. */
    private void ensureStream() throws IOException {
        if (appender != null) {
            return;
        }
        ensureService();
        if (streamName.equals(BufferedStreamWriterState.NO_STREAM)) {
            streamName = createStream();
            nextOffset = 0;
            LOG.info("Created buffered stream {} for subtask {}", streamName, subtaskId);
        }
        try {
            appender = service.openAppender(streamName, descriptor());
        } catch (IOException | RuntimeException e) {
            throw wrapFailure("Failed to open BigQuery stream " + streamName, e);
        }
    }

    /**
     * Creates a buffered stream on the destination table. A missing table is created under {@code
     * CREATE_IF_NEEDED} and the stream creation retried while the table metadata propagates to the
     * Storage Write API backend.
     */
    private String createStream() throws IOException {
        boolean tableCreated = false;
        for (int attempt = 1; attempt <= retrySchedule.maxAttempts(); attempt++) {
            Throwable failure;
            try {
                return service.createBufferedStream(destination);
            } catch (IOException | RuntimeException e) {
                failure = e;
            }
            boolean notFound = AppendErrorClassifier.hasCode(failure, Status.Code.NOT_FOUND);
            if (notFound
                    && !tableCreated
                    && config.getCreateDisposition() == CreateDisposition.CREATE_IF_NEEDED) {
                LOG.info(
                        "Destination table {} does not exist, creating it (CREATE_IF_NEEDED)",
                        destination);
                tableAdmin.create(
                        destination,
                        config.getSerializer().getTableSchema(destination),
                        config.getTableCreateOptionsProvider().optionsFor(destination));
                tableCreated = true;
            } else if (!(notFound && tableCreated)
                    && AppendErrorClassifier.classify(failure)
                            != AppendErrorClassifier.Kind.TRANSIENT) {
                throw wrapFailure(
                        "Failed to create a BigQuery buffered stream on " + destination, failure);
            }
            if (attempt >= retrySchedule.maxAttempts()) {
                throw wrapFailure(
                        "Failed to create a BigQuery buffered stream on "
                                + destination
                                + (tableCreated ? " after creating the table" : "")
                                + ", the retry budget is exhausted ("
                                + attempt
                                + " attempt(s))",
                        failure);
            }
            sleep(retrySchedule.backoffMs(attempt));
        }
        throw new IllegalStateException("unreachable");
    }

    private void ensureService() throws IOException {
        if (service == null) {
            service = serviceFactory.create(config.getLocation());
        }
        if (!abandonedRestoredStreams.isEmpty()) {
            for (String abandoned : new ArrayList<>(abandonedRestoredStreams)) {
                finalizeBestEffort(abandoned);
            }
            abandonedRestoredStreams.clear();
        }
    }

    private void finalizeBestEffort(String stream) {
        try {
            service.finalizeStream(stream);
            LOG.info("Finalized abandoned buffered stream {}", stream);
        } catch (IOException | RuntimeException e) {
            LOG.warn(
                    "Failed to finalize abandoned buffered stream {} (its unflushed rows stay"
                            + " invisible regardless)",
                    stream,
                    e);
        }
    }

    private Descriptors.Descriptor descriptor() {
        if (descriptor == null) {
            descriptor = config.getSerializer().getDescriptor(destination);
        }
        return descriptor;
    }

    // ------------------------------------------------------------------
    // Append plumbing
    // ------------------------------------------------------------------

    /** Appends synchronously and returns the failure, or {@code null} on success. */
    @Nullable
    private Throwable syncAppend(ProtoRows rows, long offset) throws IOException {
        ApiFuture<AppendRowsResponse> future;
        try {
            future = appender.append(rows, offset);
        } catch (RuntimeException e) {
            return e;
        }
        return awaitFailure(future, offset);
    }

    /**
     * Awaits an append future and returns the failure it completed with — unwrapped from the
     * execution exception, or derived from the response — or {@code null} on success. A successful
     * response acknowledging a different offset than requested is a failure: the writer's offset
     * accounting and the stream have diverged.
     */
    @Nullable
    private Throwable awaitFailure(ApiFuture<AppendRowsResponse> future, long expectedOffset)
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
        Throwable failure = responseToThrowable(response);
        if (failure != null) {
            return failure;
        }
        if (response.hasAppendResult() && response.getAppendResult().hasOffset()) {
            long acknowledged = response.getAppendResult().getOffset().getValue();
            if (acknowledged != expectedOffset) {
                return new IOException(
                        "BigQuery acknowledged an append to "
                                + streamName
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
    private Throwable responseToThrowable(AppendRowsResponse response) {
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
                            + streamName
                            + " completed with "
                            + response.getRowErrorsCount()
                            + " row error(s)",
                    response.getWriteStream(),
                    rowErrors);
        }
        return null;
    }

    private IOException wrapFailure(String message, Throwable cause) {
        if (cause instanceof IOException && cause.getMessage() != null) {
            return (IOException) cause;
        }
        if (AppendErrorClassifier.isSchemaMismatch(cause)) {
            message +=
                    " because the rows carry fields the table does not have; schema evolution is"
                            + " not supported by WriteMethod.STORAGE_API_EXACTLY_ONCE — update the"
                            + " table schema and restart the job";
        }
        return new IOException(message, cause);
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
}
