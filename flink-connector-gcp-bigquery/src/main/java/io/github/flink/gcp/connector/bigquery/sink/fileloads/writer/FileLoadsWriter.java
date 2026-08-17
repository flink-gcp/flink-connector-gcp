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
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolution;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolutionDispatcher;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.UnroutableRecord;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRow;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.ParquetCompression;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
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
 * The FILE_LOADS {@link org.apache.flink.api.connector.sink2.SinkWriter}: encodes records in the
 * configured {@link FileLoadsOptions#getStagingFormat() staging format} and streams them to
 * per-destination staging files on Cloud Storage, emitting one {@link FileLoadsCommittable} per
 * finalized file from {@link #prepareCommit()} — once at end of input in batch execution, once per
 * checkpoint in streaming execution.
 *
 * <p>Rows never accumulate on the heap: a record the serializer converts is appended to a file
 * writer that streams into a GCS resumable upload, so memory use is proportional to the number of
 * concurrently open destinations rather than to the data volume or job length. What one open
 * destination costs depends on the format: Avro holds one upload chunk plus one unflushed block,
 * while Parquet buffers a whole row group, sized from {@code maxStagingFileBytes} for the
 * correctness reason {@code ParquetStagedFileWriter} records — so a task manager is sized for the
 * format in use. In streaming execution the inter-checkpoint buffer therefore <em>is</em> GCS.
 * Files are rolled at {@link FileLoadsOptions#getMaxStagingFileBytes()}, which is sized for load
 * throughput and bounded by the per-load-job URI cap — see that option's default for both.
 *
 * <p>Staging object names include the Flink job id, subtask index, attempt number and a random
 * component, so files written by failed attempts can neither collide with live ones nor be loaded:
 * load jobs only ever reference the URIs carried by committables, which are emitted only from
 * {@link #prepareCommit()}. The per-destination state (and with it the monotonic file sequence) is
 * kept for the writer's lifetime — never reset between checkpoints — so a later checkpoint's file
 * cannot reuse an earlier checkpoint's URI; the cost is a few KB of conversion state per distinct
 * destination.
 *
 * <p>Serialization and row-conversion failures are row-level and routed to the configured {@link
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
    private final StagingFormat stagingFormat;
    private final ParquetCompression parquetCompression;
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
        this.stagingFormat = options.getStagingFormat();
        this.parquetCompression = options.getParquetCompression();
        this.metrics = new FileLoadsWriterMetrics(metricGroup, options.isPerDestinationMetrics());
        // The map is the task thread's; a reporter thread sampling it can see a size mid-update,
        // which is what "best-effort" means for a gauge over live writer state.
        this.metrics.bindWriterState((Gauge<Integer>) destinations::size);
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
        ByteString rowBytes;
        try {
            // A poison record must reach the handler no matter how the serializer fails,
            // matching the streaming writer's contract.
            rowBytes = config.serialize(element, destination);
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
            config.getFailureHandler()
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
            config.getFailureHandler()
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
        // cannot, since a record has no encoded size of its own: it is compressed into an Avro
        // block or a Parquet row group, and only the finished file has a size to count.
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
        config.getFailureHandler().flush();
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
        closeables.add(config.getFailureHandler()::close);
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
            TableSchema tableSchema = config.getTableSchema(destination);
            Descriptors.Descriptor descriptor = config.getWriteDescriptor(destination);
            Schema avroSchema = TableSchemaToAvroConverter.convert(tableSchema);
            state =
                    new DestinationState(
                            descriptor,
                            avroSchema,
                            new ProtoToAvroConverter(tableSchema, descriptor, avroSchema),
                            formatFor(destination, tableSchema));
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
                        + state.format.getExtension();
        return StagedFileWriter.open(
                state.format,
                parquetCompression,
                flinkJobId,
                destination,
                uri,
                state.avroSchema,
                storage.createObject(uri),
                maxStagingFileBytes);
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
     * <p>Logged once per destination, because a user who asked for Parquet and silently got Avro
     * for one table has no other way to find out.
     */
    private StagingFormat formatFor(TableDestination destination, TableSchema tableSchema) {
        if (stagingFormat != StagingFormat.PARQUET || !hasJsonColumn(tableSchema.getFieldsList())) {
            return stagingFormat;
        }
        LOG.info(
                "Staging {} as Avro rather than Parquet: its schema names a JSON column, which a"
                        + " Parquet load job rejects whatever the file contains.",
                destination);
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

        private final Descriptors.Descriptor descriptor;
        private final Schema avroSchema;
        private final ProtoToAvroConverter converter;
        private final StagingFormat format;
        private StagedFileWriter file;
        private int fileSequence;

        DestinationState(
                Descriptors.Descriptor descriptor,
                Schema avroSchema,
                ProtoToAvroConverter converter,
                StagingFormat format) {
            this.descriptor = descriptor;
            this.avroSchema = avroSchema;
            this.converter = converter;
            this.format = format;
        }
    }
}
