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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolution;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolutionDispatcher;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.UnroutableRecord;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRow;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.AvroCompression;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.ParquetCompression;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.bigquery.sink.serializer.SchemaDerivationException;
import io.github.flink.gcp.connector.bigquery.sink.serializer.SerializedRow;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * The FILE_LOADS {@link org.apache.flink.api.connector.sink2.SinkWriter}: encodes records in the
 * configured {@link FileLoadsOptions#getStagingFormat() staging format} and streams them to
 * per-destination staging files on Cloud Storage, emitting one {@link FileLoadsCommittable} per
 * finalized file from {@link #prepareCommit()} — once at end of input in batch execution, once per
 * checkpoint in streaming execution.
 *
 * <p>Rows never accumulate on the heap: a record the serializer converts is appended to a file
 * writer that streams into a GCS resumable upload. Bulk row data is therefore proportional to the
 * number of concurrently open destinations rather than to the data volume or job length, while the
 * much smaller file metadata retained until the next commit is separately bounded by {@link
 * FileLoadsOptions#getMaxPendingFiles()}. What one open destination costs depends on the format:
 * Avro holds one upload chunk plus one unflushed block, while Parquet buffers a whole row group,
 * sized from {@code maxStagingFileBytes} for the correctness reason {@code ParquetStagedFileWriter}
 * records — so a task manager is sized for the format in use. In streaming execution the
 * inter-checkpoint bulk-data buffer therefore <em>is</em> GCS. With {@link
 * FileLoadsOptions#isBackgroundStagingUploadEnabled()} on, full chunks and the finalization of
 * rolled, evicted and idle files run on a {@code StagingUploader} instead of this thread. Files are
 * rolled at {@link FileLoadsOptions#getMaxStagingFileBytes()}, which is sized for load throughput
 * and bounded by the per-load-job URI cap — see that option's default for both.
 *
 * <p>Staging object names include the Flink job id, subtask index, attempt number and a random
 * component, so files written by failed attempts can neither collide with live ones nor be loaded:
 * load jobs only ever reference the URIs carried by committables, which are emitted only from
 * {@link #prepareCommit()}. A writer-global monotonic sequence makes every object name unique even
 * when a destination is evicted and reopened or a checkpoint clears all active state.
 *
 * <p>Serialization and row-conversion failures are row-level and routed to the configured {@link
 * io.github.flink.gcp.connector.base.failure.FailureHandler}; staging I/O failures fail the job.
 * There is no row-level policy at load time — a BigQuery load job is all-or-nothing. A record the
 * serializer skips by returning {@code null} is neither: it reaches no staging file and no handler.
 *
 * <p>Only destinations with an open staging file retain their descriptor, Avro schema and
 * converter. The least recently used file is finished before the configured active-destination
 * limit is exceeded; inactive files are also finished after the configured timeout. A destination
 * that returns later rebuilds that state from the serializer's current schema.
 *
 * @param <T> type of the records written
 */
@Internal
public final class FileLoadsWriter<T>
        implements CommittingSinkWriter<T, FileLoadsCommittable>,
                DestinationResolutionDispatcher.Visitor<T> {

    private static final Logger LOG = LoggerFactory.getLogger(FileLoadsWriter.class);

    private final BigQuerySinkConfig<T> config;
    private final StagingStorage storage;
    private final String flinkJobId;
    private final String pathPrefix;
    private final String filePrefix;
    private final long maxStagingFileBytes;
    private final int maxOpenDestinations;
    private final int maxPendingFiles;
    private final int maxConcurrentCheckpointFinalizations;
    private final int stagingUploadChunkBytes;
    private final long destinationIdleTimeoutMillis;
    private final long maxSerializedRowBytes;
    private final StagingFormat stagingFormat;
    private final ParquetCompression parquetCompression;
    private final AvroCompression avroCompression;
    private final int zstdLevel;
    private final ProcessingTimeService timerService;
    private final FileLoadsWriterMetrics metrics;

    /** Active destinations in least-recently-used to most-recently-used order. */
    private final Map<TableDestination, DestinationState> destinations =
            new LinkedHashMap<>(16, 0.75f, true);

    private final List<FileLoadsCommittable> finishedFiles = new ArrayList<>();

    /**
     * Files closed on the staging uploader whose finalization has not been collected yet, in close
     * order. A committable moves to {@code finishedFiles} only once its object is finalized.
     */
    private final ArrayDeque<PendingFinalization> pendingFinalizations = new ArrayDeque<>();

    /** The background staging uploader, or {@code null} when uploads run on the task thread. */
    @Nullable private final StagingUploader uploader;

    private long nextFileSequence;
    private long nextIdleSweepTimestamp = Long.MAX_VALUE;
    private boolean capacityWarningLogged;
    private boolean formatFallbackWarningLogged;
    private boolean closed;

    /**
     * Creates a writer.
     *
     * @param config the sink configuration
     * @param options the FILE_LOADS options
     * @param storage the staging storage
     * @param metricGroup the writer's metric group
     * @param flinkJobId the Flink job id (hex), scoping this run's staging directory
     * @param subtaskIndex the subtask index
     * @param attemptNumber the attempt number
     * @param timerService the processing-time service that drives idle-destination eviction
     */
    public FileLoadsWriter(
            BigQuerySinkConfig<T> config,
            FileLoadsOptions options,
            StagingStorage storage,
            SinkWriterMetricGroup metricGroup,
            String flinkJobId,
            int subtaskIndex,
            int attemptNumber,
            ProcessingTimeService timerService) {
        this.config = config;
        this.storage = storage;
        this.flinkJobId = flinkJobId;
        this.pathPrefix = options.getStagingPath() + "/" + flinkJobId;
        this.filePrefix =
                subtaskIndex
                        + "-"
                        + attemptNumber
                        + "-"
                        + UUID.randomUUID().toString().substring(0, 8);
        this.maxStagingFileBytes = options.getMaxStagingFileBytes();
        this.maxOpenDestinations = options.getMaxOpenDestinations();
        this.maxPendingFiles = options.getMaxPendingFiles();
        this.maxConcurrentCheckpointFinalizations =
                options.getMaxConcurrentCheckpointFinalizations();
        // FileLoadsOptions bounds the chunk to what an int can express.
        this.stagingUploadChunkBytes = Math.toIntExact(options.getStagingUploadChunkBytes());
        this.uploader =
                options.isBackgroundStagingUploadEnabled()
                        ? new StagingUploader(
                                stagingUploadChunkBytes,
                                options.getMaxInFlightUploadOperations(),
                                "FileLoadsWriter staging uploader " + filePrefix)
                        : null;
        // Without the uploader no upload operation queues, so none counts towards the warning; the
        // open files' chunks count either way.
        warnIfUploadBuffersExceedHeapShare(
                uploader == null ? 0 : options.getMaxInFlightUploadOperations(),
                Runtime.getRuntime().maxMemory());
        this.destinationIdleTimeoutMillis = options.getDestinationIdleTimeout().toMillis();
        this.maxSerializedRowBytes = options.getMaxSerializedRowBytes();
        this.stagingFormat = options.getStagingFormat();
        this.parquetCompression = options.getParquetCompression();
        this.avroCompression = options.getAvroCompression();
        this.zstdLevel = options.getZstdLevel();
        this.timerService =
                Preconditions.checkNotNull(timerService, "timerService must not be null");
        this.metrics = new FileLoadsWriterMetrics(metricGroup, options.isPerDestinationMetrics());
        // The map is the task thread's; a reporter thread sampling it can see a size mid-update,
        // which is what "best-effort" means for a gauge over live writer state.
        this.metrics.bindWriterState(
                (Gauge<Integer>) destinations::size, (Gauge<Integer>) this::pendingFileCount);
    }

    /**
     * The upload buffers this writer may hold at once: one chunk per open destination file plus one
     * per upload operation the uploader may hold, saturating rather than overflowing. Parquet row
     * groups, Avro blocks and the client's copy of a closing file's last chunk come on top.
     */
    static long plannedUploadBufferBytes(
            int maxOpenDestinations, int maxInFlightOperations, long chunkBytes) {
        long chunks = (long) maxOpenDestinations + maxInFlightOperations;
        return chunks > Long.MAX_VALUE / chunkBytes ? Long.MAX_VALUE : chunks * chunkBytes;
    }

    /**
     * Whether one writer's planned upload buffers exceed a quarter of the maximum heap. The limits
     * are valid one by one, so this warns rather than fails: only the TaskManager knows its heap,
     * and the share left for the rest of the task is the operator's call.
     *
     * <p>The quarter is a round heuristic, not a measurement. It leaves room for the format
     * buffers, the clients and the rest of the task, and it is per writer: every FILE_LOADS writer
     * subtask in the TaskManager holds its own buffers, so several slots can exhaust the heap
     * without any one of them crossing it.
     *
     * @param maxHeapBytes {@link Runtime#maxMemory()}, which is {@link Long#MAX_VALUE} when the JVM
     *     has no limit
     */
    static boolean uploadBuffersExceedHeapShare(long plannedBytes, long maxHeapBytes) {
        return maxHeapBytes != Long.MAX_VALUE && plannedBytes > maxHeapBytes / 4;
    }

    private void warnIfUploadBuffersExceedHeapShare(int maxInFlightOperations, long maxHeapBytes) {
        long plannedBytes =
                plannedUploadBufferBytes(
                        maxOpenDestinations, maxInFlightOperations, stagingUploadChunkBytes);
        if (!uploadBuffersExceedHeapShare(plannedBytes, maxHeapBytes)) {
            return;
        }
        LOG.warn(
                "This FILE_LOADS writer alone may hold {} bytes of staging upload buffers, more"
                        + " than a quarter of the JVM's {}-byte maximum heap: {}"
                        + " maxOpenDestinations plus {} maxInFlightUploadOperations, each up to one"
                        + " {}-byte stagingUploadChunkBytes chunk. Every FILE_LOADS writer subtask"
                        + " in this TaskManager holds its own. Lower one of them or give the"
                        + " TaskManager more heap; Parquet staging also buffers a row group per open"
                        + " file.",
                plannedBytes,
                maxHeapBytes,
                maxOpenDestinations,
                maxInFlightOperations,
                stagingUploadChunkBytes);
    }

    /** Finds the next active destination deadline after an idle sweep changed the active set. */
    private void scheduleNextIdleSweep() {
        long earliestDeadline = Long.MAX_VALUE;
        for (DestinationState state : destinations.values()) {
            earliestDeadline = Math.min(earliestDeadline, idleDeadline(state.lastAccessMillis));
        }
        scheduleIdleSweep(earliestDeadline);
    }

    /** Arms one idle deadline on Flink's mailbox task thread if it precedes the current timer. */
    private void scheduleIdleSweep(long deadline) {
        if (deadline >= nextIdleSweepTimestamp) {
            return;
        }
        nextIdleSweepTimestamp = deadline;
        timerService.registerTimer(
                deadline,
                timestamp -> {
                    if (closed) {
                        return;
                    }
                    nextIdleSweepTimestamp = Long.MAX_VALUE;
                    evictIdleDestinations(timestamp);
                    scheduleNextIdleSweep();
                });
    }

    private long idleDeadline(long lastAccessMillis) {
        return lastAccessMillis > Long.MAX_VALUE - destinationIdleTimeoutMillis
                ? Long.MAX_VALUE
                : lastAccessMillis + destinationIdleTimeoutMillis;
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        DestinationResolution resolution =
                config.getDestinationResolver().resolve(element, context);
        DestinationResolutionDispatcher.dispatch(resolution, element, context, this);
    }

    @Override
    public void visit(UnroutableRecord failure, T element, Context context) throws IOException {
        metrics.recordFailedWithoutDestination();
        config.getFailureHandler().handle(failure);
    }

    @Override
    public void visit(TableDestination destination, T element, Context context) throws IOException {
        config.prepareWriteSchema(destination);
        SerializedRow row;
        try {
            // A poison record must reach the handler no matter how the serializer fails,
            // matching the streaming writer's contract.
            row = config.serializeRow(element, destination);
        } catch (SchemaDerivationException e) {
            // The destination's schema failure, not this record's; see SchemaDerivationException.
            throw e;
        } catch (IOException | RuntimeException e) {
            metrics.recordFailed(metrics.forTable(destination));
            config.getFailureHandler()
                    .handle(
                            FailedRow.of(
                                    destination,
                                    null,
                                    "Failed to serialize a record for " + destination + ": " + e,
                                    e));
            return;
        }
        if (row == null) {
            // Skip by contract, not a failure. Ahead of stateFor(...), so a record written nowhere
            // opens no file — and ahead of every metrics.forTable(...), which registers a
            // destination's counters on first use and can never unregister them, so a table only
            // skipped records resolve to gains no counters reading zero. Counted here, because
            // nothing else reports it: a serializer skipping every record leaves an empty table
            // under a green job.
            metrics.recordSkipped();
            return;
        }
        // The size of the bytes the serializer would have returned, computed without encoding
        // them; the bytes themselves are encoded only for a row routed to the failure handler.
        long rowSize = row.getSerializedSize();
        // A row past Integer.MAX_VALUE bytes has no encoding, so it fails under any configured
        // limit, Long.MAX_VALUE included; see getSerializedSize.
        boolean encodable = rowSize <= Integer.MAX_VALUE;
        if (!encodable || rowSize > maxSerializedRowBytes) {
            metrics.recordFailed(metrics.forTable(destination));
            config.getFailureHandler()
                    .handle(
                            FailedRow.of(
                                    destination,
                                    encodable ? row.toByteString() : null,
                                    encodable
                                            ? "A serialized row for "
                                                    + destination
                                                    + " is "
                                                    + rowSize
                                                    + " bytes, exceeding the configured "
                                                    + maxSerializedRowBytes
                                                    + "-byte FILE_LOADS limit"
                                            : "A serialized row for "
                                                    + destination
                                                    + " is more than "
                                                    + Integer.MAX_VALUE
                                                    + " bytes, which no protobuf encoding can"
                                                    + " hold",
                                    null));
            return;
        }
        DestinationState state = stateForConversion(destination);
        boolean newlyActive = !destinations.containsKey(destination);
        GenericRecord record;
        try {
            // A serializer that hands over its message is not parsed at all; one that returns
            // bytes is parsed once, against its own descriptor rather than the augmented one.
            record =
                    state.converter.convert(
                            row.getBase(state.descriptor), row.getTail(state.writeDescriptor));
        } catch (InvalidProtocolBufferException e) {
            metrics.recordFailed(metrics.forTable(destination));
            config.getFailureHandler()
                    .handle(
                            FailedRow.of(
                                    destination,
                                    row.toByteString(),
                                    "A row for "
                                            + destination
                                            + " does not conform to the serializer's descriptor: "
                                            + e,
                                    e));
            return;
        } catch (IOException e) {
            metrics.recordFailed(metrics.forTable(destination));
            config.getFailureHandler()
                    .handle(
                            FailedRow.of(
                                    destination,
                                    row.toByteString(),
                                    "Failed to convert a row for " + destination + " to Avro: " + e,
                                    e));
            return;
        }
        // From here on, failures are staging I/O errors and fail the job.
        collectFinalizedFiles();
        if (newlyActive) {
            ensurePendingFileCapacity();
            evictForCapacity();
            state.file = openFile(destination, state);
            state.lastAccessMillis = currentProcessingTime();
            destinations.put(destination, state);
            metrics.destinationActivated();
        }
        state.file.append(record);
        state.lastAccessMillis = currentProcessingTime();
        scheduleIdleSweep(idleDeadline(state.lastAccessMillis));
        // The staging file is this write path's hand-off, so the record counts here — the bytes
        // cannot, since a record has no encoded size of its own: it is compressed into an Avro
        // block or a Parquet row group, and only the finished file has a size to count.
        metrics.recordStaged(metrics.forTable(destination));
        if (state.file.isFull(maxStagingFileBytes)) {
            finishFile(state);
            destinations.remove(destination);
            collectFinalizedFiles();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        // The staged files need nothing here: prepareCommit(), which follows every flush,
        // finishes the open ones. A pre-end-of-input flush is a checkpoint — the streaming
        // trigger for FILE_LOADS — and the failure handler persists the rows routed to it
        // before that checkpoint completes.
        config.getFailureHandler().flush();
    }

    @Override
    public Collection<FileLoadsCommittable> prepareCommit()
            throws IOException, InterruptedException {
        int activeDestinations = destinations.size();
        if (uploader != null) {
            // Every upload operation submitted so far runs first, so the committables keep their
            // close order and each remaining open file's channel is idle before it is detached and
            // finalized below, serially or by the checkpoint finalizer, exactly as without an
            // uploader.
            uploader.drain();
            collectFinalizedFiles();
            for (DestinationState state : destinations.values()) {
                state.detachStream();
            }
        }
        if (activeDestinations < 2 || maxConcurrentCheckpointFinalizations == 1) {
            Iterator<DestinationState> iterator = destinations.values().iterator();
            while (iterator.hasNext()) {
                finishFile(iterator.next());
                iterator.remove();
            }
        } else {
            List<StagedFileWriter> files = new ArrayList<>(activeDestinations);
            for (DestinationState state : destinations.values()) {
                files.add(state.file);
            }
            // The finalizer owns this snapshot from here. Detach it before starting workers because
            // a JVM-fatal failure may return after cancelling a peer that ignores interruption;
            // close() must not concurrently abort a file while that peer is still inside finish().
            destinations.clear();
            StagedFileFinalizer.finish(
                    files, maxConcurrentCheckpointFinalizations, this::recordFinishedFile);
        }
        List<FileLoadsCommittable> committables = new ArrayList<>(finishedFiles);
        finishedFiles.clear();
        LOG.info("Staged {} files for {} destinations", committables.size(), activeDestinations);
        return committables;
    }

    @Override
    public void close() throws Exception {
        closed = true;
        // First, so that no uploader task finalizes a file this method then aborts. Waits for
        // nothing.
        if (uploader != null) {
            uploader.close();
        }
        pendingFinalizations.clear();
        // Closers.closeAll, not sequential closes: the handler must be closed on the failure path
        // too. Aborting a staged file closes nothing and does not fail, so the handler is the
        // first entry that can.
        List<AutoCloseable> closeables = new ArrayList<>();
        for (DestinationState state : destinations.values()) {
            StagedFileWriter file = state.file;
            state.file = null;
            state.stream = null;
            // Null when a finish failed after handing the file over; nothing is left to abort.
            if (file != null) {
                closeables.add(file::abort);
            }
        }
        // The map backs the openDestinations gauge, which a reporter may still sample between this
        // call and the metric group's own close; the conversion state is dead once the files are
        // aborted. Cleared after the loop above has taken every open file.
        destinations.clear();
        finishedFiles.clear();
        closeables.add(config.getFailureHandler()::close);
        // The staging client goes last because Closers.closeAll reports the *first* failure and
        // suppresses the rest onto it: whatever the handler has to say outranks a teardown failure
        // from a client this writer is finished with.
        closeables.add(storage::close);
        Closers.closeAll(closeables);
    }

    /**
     * Finishes the destination's open file and collects its committable once the object is
     * finalized: here, or later from the staging uploader, in close order either way.
     */
    private void finishFile(DestinationState state) throws IOException {
        FileLoadsCommittable committable = state.file.finish();
        state.file = null;
        StagingUploader.UploadOutputStream stream = state.stream;
        state.stream = null;
        if (stream == null) {
            recordFinishedFile(committable);
            return;
        }
        // Collected by the caller once the destination is out of the map: an uploader failure that
        // surfaces here must not leave a destination whose file is already gone.
        pendingFinalizations.add(new PendingFinalization(committable, stream.finalized()));
    }

    /**
     * Records, in close order, every uploader finalization that has completed, and reports an
     * uploader failure. Stops at the first one still running so that a later file never overtakes
     * it.
     */
    private void collectFinalizedFiles() throws IOException {
        if (uploader == null) {
            return;
        }
        uploader.checkFailure();
        while (!pendingFinalizations.isEmpty()) {
            PendingFinalization next = pendingFinalizations.peekFirst();
            if (!next.finalized.isDone()) {
                return;
            }
            if (next.finalized.isCompletedExceptionally()) {
                // The uploader records its failure before completing the file, so this reports it.
                uploader.checkFailure();
                throw new IOException("A staging file was not finalized on the staging uploader");
            }
            pendingFinalizations.removeFirst();
            recordFinishedFile(next.committable);
        }
    }

    /**
     * Records a finalization result on the task thread, preserving metric and committable order.
     */
    private void recordFinishedFile(FileLoadsCommittable committable) {
        finishedFiles.add(committable);
        metrics.fileFinished(committable.getByteCount());
    }

    /** Returns active conversion state or builds detached state that retains nothing on failure. */
    private DestinationState stateForConversion(TableDestination destination) {
        DestinationState state = destinations.get(destination);
        if (state == null) {
            TableSchema tableSchema = config.getTableSchema(destination);
            Descriptors.Descriptor descriptor = config.getSerializerDescriptor(destination);
            // Only a row with added fields has a tail to read them from; without any, a second
            // descriptor from a serializer that derives one per call would give the plan nothing
            // to read but could still disagree with the first.
            Descriptors.Descriptor writeDescriptor =
                    config.addsRowFields() ? config.getWriteDescriptor(destination) : null;
            Schema avroSchema = TableSchemaToAvroConverter.convert(tableSchema);
            state =
                    new DestinationState(
                            descriptor,
                            writeDescriptor,
                            avroSchema,
                            new ProtoToAvroConverter(
                                    tableSchema, descriptor, writeDescriptor, avroSchema),
                            formatFor(destination, tableSchema));
        }
        return state;
    }

    /** Finishes and removes the least recently used destination before opening another one. */
    private void evictForCapacity() throws IOException {
        if (destinations.size() < maxOpenDestinations) {
            return;
        }
        Iterator<Map.Entry<TableDestination, DestinationState>> iterator =
                destinations.entrySet().iterator();
        Map.Entry<TableDestination, DestinationState> eldest = iterator.next();
        finishFile(eldest.getValue());
        iterator.remove();
        collectFinalizedFiles();
        metrics.capacityEvicted();
        if (!capacityWarningLogged) {
            capacityWarningLogged = true;
            LOG.warn(
                    "FILE_LOADS writer reached maxOpenDestinations={}; least-recently-used"
                            + " destination files will be finished. Increase the limit only after"
                            + " accounting for each active file's upload and format buffers.",
                    maxOpenDestinations);
        }
    }

    /** Finishes destinations that have received no record for at least the configured timeout. */
    private void evictIdleDestinations(long now) throws IOException {
        Iterator<Map.Entry<TableDestination, DestinationState>> iterator =
                destinations.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TableDestination, DestinationState> entry = iterator.next();
            DestinationState state = entry.getValue();
            if (now - state.lastAccessMillis < destinationIdleTimeoutMillis) {
                continue;
            }
            finishFile(state);
            iterator.remove();
            metrics.idleEvicted();
        }
        collectFinalizedFiles();
    }

    private long currentProcessingTime() {
        return timerService.getCurrentProcessingTime();
    }

    private int pendingFileCount() {
        return Math.addExact(
                Math.addExact(finishedFiles.size(), pendingFinalizations.size()),
                destinations.size());
    }

    /**
     * Refuses the next file before changing active state, keeping the next commit's retained file
     * descriptors bounded even when destination churn repeatedly triggers LRU eviction.
     */
    private void ensurePendingFileCapacity() throws IOException {
        if (pendingFileCount() < maxPendingFiles) {
            return;
        }
        throw new IOException(
                "FILE_LOADS writer reached maxPendingFiles="
                        + maxPendingFiles
                        + " before the next commit; reduce destination churn, commit more often,"
                        + " or raise the limit only after accounting for retained file metadata");
    }

    private StagedFileWriter openFile(TableDestination destination, DestinationState state)
            throws IOException {
        final long sequence;
        try {
            sequence = nextFileSequence;
            nextFileSequence = Math.incrementExact(nextFileSequence);
        } catch (ArithmeticException e) {
            throw new IOException("FILE_LOADS staging-file sequence exhausted", e);
        }
        String uri =
                pathPrefix
                        + "/"
                        + destination
                        + "/"
                        + filePrefix
                        + "-"
                        + sequence
                        + state.format.getExtension();
        WritableByteChannel channel = storage.createObject(uri, stagingUploadChunkBytes);
        StagingUploader.UploadOutputStream stream =
                uploader == null ? null : uploader.open(channel);
        StagedFileWriter file =
                StagedFileWriter.open(
                        state.format,
                        parquetCompression,
                        avroCompression,
                        zstdLevel,
                        flinkJobId,
                        destination,
                        uri,
                        state.avroSchema,
                        stream == null ? Channels.newOutputStream(channel) : stream,
                        maxStagingFileBytes);
        state.stream = stream;
        return file;
    }

    /**
     * The format this destination stages in: the configured one, unless its schema names a {@code
     * JSON} column.
     *
     * <p>The fallback is a correctness override rather than a preference. A {@code PARQUET} load is
     * refused at <em>job-configuration</em> level whenever the provided schema names a {@code JSON}
     * column — whatever the file holds — so a Parquet file for such a destination could never be
     * loaded at all. Deciding here, where the destination's schema is first resolved, is the only
     * place that works: with a per-record destination resolver the full set of schemas is not known
     * when the job graph is built.
     *
     * <p>Logged once per writer, naming the first destination encountered. Remembering every
     * destination ever seen would reintroduce unbounded state, while logging every reactivation
     * under capacity churn can flood the TaskManager log.
     */
    private StagingFormat formatFor(TableDestination destination, TableSchema tableSchema) {
        if (stagingFormat != StagingFormat.PARQUET || !hasJsonColumn(tableSchema.getFieldsList())) {
            return stagingFormat;
        }
        if (!formatFallbackWarningLogged) {
            formatFallbackWarningLogged = true;
            LOG.warn(
                    "Staging {} as Avro rather than Parquet because its schema names a JSON column;"
                            + " later fallback destinations are not logged by this writer.",
                    destination);
        }
        return StagingFormat.AVRO;
    }

    private static boolean hasJsonColumn(List<TableFieldSchema> fields) {
        for (TableFieldSchema field : fields) {
            if (field.getType() == TableFieldSchema.Type.JSON) {
                return true;
            }
            if (hasJsonColumn(field.getFieldsList())) {
                return true;
            }
        }
        return false;
    }

    /** Per-destination conversion state and the currently open file, if any. */
    private static final class DestinationState {

        /** The serializer's own descriptor, which a row's base has. */
        private final Descriptors.Descriptor descriptor;

        /** The descriptor with the sink's added fields, which a row's tail has, or null. */
        @Nullable private final Descriptors.Descriptor writeDescriptor;

        private final Schema avroSchema;
        private final ProtoToAvroConverter converter;
        private final StagingFormat format;
        private StagedFileWriter file;

        /**
         * The open file's uploader stream, or {@code null} without an uploader or once it is
         * finished.
         */
        @Nullable private StagingUploader.UploadOutputStream stream;

        private long lastAccessMillis;

        DestinationState(
                Descriptors.Descriptor descriptor,
                @Nullable Descriptors.Descriptor writeDescriptor,
                Schema avroSchema,
                ProtoToAvroConverter converter,
                StagingFormat format) {
            this.descriptor = descriptor;
            this.writeDescriptor = writeDescriptor;
            this.avroSchema = avroSchema;
            this.converter = converter;
            this.format = format;
        }

        /** Moves the open file off the drained uploader so it can be finalized on any thread. */
        void detachStream() {
            if (stream != null) {
                stream.detach();
                stream = null;
            }
        }
    }

    /** A file closed on the staging uploader, with the committable its finalization releases. */
    private static final class PendingFinalization {
        private final FileLoadsCommittable committable;
        private final CompletableFuture<Void> finalized;

        PendingFinalization(FileLoadsCommittable committable, CompletableFuture<Void> finalized) {
            this.committable = committable;
            this.finalized = finalized;
        }
    }
}
