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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRow;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The FILE_LOADS {@link org.apache.flink.api.connector.sink2.SinkWriter}: encodes records to Avro
 * and streams them to per-destination staging files on Cloud Storage, emitting one {@link
 * FileLoadsCommittable} per finalized file from {@link #prepareCommit()} — once at end of input in
 * batch execution, once per checkpoint in streaming execution.
 *
 * <p>Rows never accumulate on the heap: a record the serializer converts is appended to an Avro
 * writer that streams into a GCS resumable upload, so memory use is proportional to the number of
 * concurrently open destinations (one upload chunk plus one Avro block each), not to the data
 * volume or job length. In streaming execution the inter-checkpoint buffer therefore <em>is</em>
 * GCS. Files are rolled at {@link FileLoadsOptions#getMaxStagingFileBytes()}, which is sized for
 * load throughput and bounded by the per-load-job URI cap — see that option's default for both.
 *
 * <p>Staging object names include the Flink job id, subtask index, attempt number and a random
 * component, so files written by failed attempts can neither collide with live ones nor be loaded:
 * load jobs only ever reference the URIs carried by committables, which are emitted only from
 * {@link #prepareCommit()}. The per-destination state (and with it the monotonic file sequence) is
 * kept for the writer's lifetime — never reset between checkpoints — so a later checkpoint's file
 * cannot reuse an earlier checkpoint's URI; the cost is a few KB of conversion state per distinct
 * destination.
 *
 * <p>Serialization and Avro-conversion failures are row-level and routed to the configured {@link
 * io.github.flink.gcp.connector.base.failure.FailureHandler}; staging I/O failures fail the job.
 * There is no row-level policy at load time — a BigQuery load job is all-or-nothing. A record the
 * serializer skips by returning {@code null} is neither: it reaches no staging file and no handler.
 *
 * <p>The schema per destination is captured when its first record arrives and cached for the
 * writer's lifetime; mid-run serializer schema changes are only picked up after a restart.
 *
 * @param <T> type of the records written
 */
@Internal
public final class FileLoadsWriter<T> implements CommittingSinkWriter<T, FileLoadsCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(FileLoadsWriter.class);

    private final BigQuerySinkConfig<T> config;
    private final StagingStorage storage;
    private final String flinkJobId;
    private final String pathPrefix;
    private final String filePrefix;
    private final long maxStagingFileBytes;
    private final FileLoadsWriterMetrics metrics;

    private final Map<TableDestination, DestinationState> destinations = new HashMap<>();
    private final List<FileLoadsCommittable> finishedFiles = new ArrayList<>();

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
     */
    public FileLoadsWriter(
            BigQuerySinkConfig<T> config,
            FileLoadsOptions options,
            StagingStorage storage,
            SinkWriterMetricGroup metricGroup,
            String flinkJobId,
            int subtaskIndex,
            int attemptNumber) {
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
        this.metrics = new FileLoadsWriterMetrics(metricGroup, options.isPerDestinationMetrics());
        // The map is the task thread's; a reporter thread sampling it can see a size mid-update,
        // which is what "best-effort" means for a gauge over live writer state.
        this.metrics.bindWriterState((Gauge<Integer>) destinations::size);
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        TableDestination destination = config.getDestinationResolver().resolve(element, context);
        ByteString rowBytes;
        try {
            // A poison record must reach the handler no matter how the serializer fails,
            // matching the streaming writer's contract.
            rowBytes = config.getSerializer().serialize(element);
        } catch (IOException | RuntimeException e) {
            metrics.recordFailed(metrics.forTable(destination));
            config.getFailedRowHandler()
                    .handle(
                            FailedRow.of(
                                    destination,
                                    null,
                                    "Failed to serialize a record for " + destination + ": " + e,
                                    e));
            return;
        }
        if (rowBytes == null) {
            // Skip by contract, not a failure. Ahead of stateFor(...), so a record written nowhere
            // opens no file — and ahead of every metrics.forTable(...), which registers a
            // destination's counters on first use and can never unregister them, so a table only
            // skipped records resolve to gains no counters reading zero. Counted here, because
            // nothing else reports it: a serializer skipping every record leaves an empty table
            // under a green job.
            metrics.recordSkipped();
            return;
        }
        DestinationState state = stateFor(destination);
        GenericRecord record;
        try {
            DynamicMessage row = DynamicMessage.parseFrom(state.descriptor, rowBytes);
            record = state.converter.convert(row);
        } catch (InvalidProtocolBufferException e) {
            metrics.recordFailed(metrics.forTable(destination));
            config.getFailedRowHandler()
                    .handle(
                            FailedRow.of(
                                    destination,
                                    rowBytes,
                                    "A row for "
                                            + destination
                                            + " does not conform to the serializer's descriptor: "
                                            + e,
                                    e));
            return;
        } catch (IOException e) {
            metrics.recordFailed(metrics.forTable(destination));
            config.getFailedRowHandler()
                    .handle(
                            FailedRow.of(
                                    destination,
                                    rowBytes,
                                    "Failed to convert a row for " + destination + " to Avro: " + e,
                                    e));
            return;
        }
        // From here on, failures are staging I/O errors and fail the job.
        if (state.file == null) {
            state.file = openFile(destination, state);
        }
        state.file.append(record);
        // The staging file is this write path's hand-off, so the record counts here — the bytes
        // cannot, since an Avro block's encoded size is only known once the file is finished.
        metrics.recordStaged(metrics.forTable(destination));
        if (state.file.bytesWritten() >= maxStagingFileBytes) {
            finishFile(state);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        // The staged files need nothing here: prepareCommit(), which follows every flush,
        // finishes the open ones. A pre-end-of-input flush is a checkpoint — the streaming
        // trigger for FILE_LOADS — and the failure handler persists the rows routed to it
        // before that checkpoint completes.
        config.getFailedRowHandler().flush();
    }

    @Override
    public Collection<FileLoadsCommittable> prepareCommit() throws IOException {
        for (DestinationState state : destinations.values()) {
            if (state.file != null) {
                finishFile(state);
            }
        }
        List<FileLoadsCommittable> committables = new ArrayList<>(finishedFiles);
        finishedFiles.clear();
        LOG.info("Staged {} files for {} destinations", committables.size(), destinations.size());
        return committables;
    }

    @Override
    public void close() throws Exception {
        // Closers.closeAll, not sequential closes: the handler must be closed on the failure path
        // too, even when aborting a staged file throws.
        List<AutoCloseable> closeables = new ArrayList<>();
        for (DestinationState state : destinations.values()) {
            if (state.file != null) {
                StagedFileWriter file = state.file;
                state.file = null;
                closeables.add(file::abort);
            }
        }
        // The map backs the openDestinations gauge, which a reporter may still sample between this
        // call and the metric group's own close; the conversion state is dead once the files are
        // aborted. Cleared after the loop above has taken every open file.
        destinations.clear();
        closeables.add(config.getFailedRowHandler()::close);
        Closers.closeAll(closeables);
    }

    /** Finalizes the destination's open file, collecting its committable and counting its bytes. */
    private void finishFile(DestinationState state) throws IOException {
        FileLoadsCommittable committable = state.file.finish();
        state.file = null;
        finishedFiles.add(committable);
        metrics.fileFinished(committable.getByteCount());
    }

    private DestinationState stateFor(TableDestination destination) {
        DestinationState state = destinations.get(destination);
        if (state == null) {
            TableSchema tableSchema = config.getSerializer().getTableSchema(destination);
            Descriptors.Descriptor descriptor = config.getSerializer().getDescriptor(destination);
            Schema avroSchema = TableSchemaToAvroConverter.convert(tableSchema);
            state =
                    new DestinationState(
                            descriptor,
                            avroSchema,
                            new ProtoToAvroConverter(tableSchema, descriptor, avroSchema));
            destinations.put(destination, state);
        }
        return state;
    }

    private StagedFileWriter openFile(TableDestination destination, DestinationState state)
            throws IOException {
        String uri =
                pathPrefix
                        + "/"
                        + destination
                        + "/"
                        + filePrefix
                        + "-"
                        + state.fileSequence++
                        + ".avro";
        return new StagedFileWriter(
                flinkJobId, destination, uri, state.avroSchema, storage.createObject(uri));
    }

    /** Per-destination conversion state and the currently open file, if any. */
    private static final class DestinationState {

        private final Descriptors.Descriptor descriptor;
        private final Schema avroSchema;
        private final ProtoToAvroConverter converter;
        private StagedFileWriter file;
        private int fileSequence;

        DestinationState(
                Descriptors.Descriptor descriptor,
                Schema avroSchema,
                ProtoToAvroConverter converter) {
            this.descriptor = descriptor;
            this.avroSchema = avroSchema;
            this.converter = converter;
        }
    }
}
