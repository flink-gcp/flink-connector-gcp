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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.util.IOUtils;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
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
 * <p>Rows never accumulate on the heap: each record is appended to an Avro writer that streams into
 * a GCS resumable upload, so memory use is proportional to the number of concurrently open
 * destinations (one upload chunk plus one Avro block each), not to the data volume or job length.
 * In streaming execution the inter-checkpoint buffer therefore <em>is</em> GCS. Files are rolled at
 * {@link #DEFAULT_MAX_FILE_BYTES} so the common case stays within a single direct load job (10,000
 * files x 1.5 GiB well exceeds typical batches).
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
 * There is no row-level policy at load time — a BigQuery load job is all-or-nothing.
 *
 * <p>The schema per destination is captured when its first record arrives and cached for the
 * writer's lifetime; mid-run serializer schema changes are only picked up after a restart.
 *
 * @param <T> type of the records written
 */
@Internal
public final class FileLoadsWriter<T> implements CommittingSinkWriter<T, FileLoadsCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(FileLoadsWriter.class);

    /**
     * Size at which a staging file is rolled. 1.5 GiB keeps 10,000 files (the per-load-job URI
     * limit) at ~15 TB, aligning the fast single-load path with BigQuery's per-job byte limit.
     */
    static final long DEFAULT_MAX_FILE_BYTES = 1_610_612_736L;

    private final BigQuerySinkConfig<T> config;
    private final StagingStorage storage;
    private final String flinkJobId;
    private final String pathPrefix;
    private final String filePrefix;
    private final long maxFileBytes;

    private final Map<TableDestination, DestinationState> destinations = new HashMap<>();
    private final List<FileLoadsCommittable> finishedFiles = new ArrayList<>();

    /**
     * Creates a writer.
     *
     * @param config the sink configuration
     * @param options the FILE_LOADS options
     * @param storage the staging storage
     * @param flinkJobId the Flink job id (hex), scoping this run's staging directory
     * @param subtaskIndex the subtask index
     * @param attemptNumber the attempt number
     */
    public FileLoadsWriter(
            BigQuerySinkConfig<T> config,
            FileLoadsOptions options,
            StagingStorage storage,
            String flinkJobId,
            int subtaskIndex,
            int attemptNumber) {
        this(
                config,
                options,
                storage,
                flinkJobId,
                subtaskIndex,
                attemptNumber,
                DEFAULT_MAX_FILE_BYTES);
    }

    @VisibleForTesting
    FileLoadsWriter(
            BigQuerySinkConfig<T> config,
            FileLoadsOptions options,
            StagingStorage storage,
            String flinkJobId,
            int subtaskIndex,
            int attemptNumber,
            long maxFileBytes) {
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
        this.maxFileBytes = maxFileBytes;
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
            config.getFailedRowHandler()
                    .handle(
                            FailedRow.of(
                                    destination,
                                    null,
                                    "Failed to serialize a record for " + destination + ": " + e,
                                    e));
            return;
        }
        DestinationState state = stateFor(destination);
        GenericRecord record;
        try {
            DynamicMessage row = DynamicMessage.parseFrom(state.descriptor, rowBytes);
            record = state.converter.convert(row);
        } catch (InvalidProtocolBufferException e) {
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
        if (state.file.bytesWritten() >= maxFileBytes) {
            finishedFiles.add(state.file.finish());
            state.file = null;
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
                finishedFiles.add(state.file.finish());
                state.file = null;
            }
        }
        List<FileLoadsCommittable> committables = new ArrayList<>(finishedFiles);
        finishedFiles.clear();
        LOG.info("Staged {} files for {} destinations", committables.size(), destinations.size());
        return committables;
    }

    @Override
    public void close() throws Exception {
        // closeAll, not sequential closes: the handler must be closed on the failure path too,
        // even when aborting a staged file throws.
        List<AutoCloseable> closeables = new ArrayList<>();
        for (DestinationState state : destinations.values()) {
            if (state.file != null) {
                StagedFileWriter file = state.file;
                state.file = null;
                closeables.add(file::abort);
            }
        }
        closeables.add(config.getFailedRowHandler()::close);
        IOUtils.closeAll(closeables);
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
