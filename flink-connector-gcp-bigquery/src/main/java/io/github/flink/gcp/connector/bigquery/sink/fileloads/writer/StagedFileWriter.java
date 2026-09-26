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

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.ParquetCompression;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.OutputStream;

/**
 * One open staging file, streaming into the Cloud Storage upload channel it was opened over.
 *
 * <p>Implementations track the row count and a byte count used for size-based rolling and load-job
 * partitioning. That count trails the appended data — by an unflushed Avro block, or by a whole
 * Parquet row group — so a rolled file lands at or slightly above the threshold rather than exactly
 * on it.
 */
@Internal
interface StagedFileWriter {

    /**
     * Opens a writer for the given format.
     *
     * <p>{@link ParquetStagedFileWriter} is referenced only from the {@code PARQUET} branch, so a
     * deployment that never selects Parquet never loads it — which is what lets {@code
     * parquet-avro} be a {@code provided} dependency rather than one this connector ships.
     *
     * @param maxStagingFileBytes the roll threshold, which sizes Parquet's row group; see that
     *     implementation for why it cannot be ignored there
     */
    static StagedFileWriter open(
            StagingFormat format,
            ParquetCompression compression,
            String flinkJobId,
            TableDestination destination,
            String uri,
            Schema schema,
            OutputStream stream,
            long maxStagingFileBytes)
            throws IOException {
        switch (format) {
            case AVRO:
                return new AvroStagedFileWriter(flinkJobId, destination, uri, schema, stream);
            case PARQUET:
                return new ParquetStagedFileWriter(
                        flinkJobId,
                        destination,
                        uri,
                        schema,
                        stream,
                        compression,
                        maxStagingFileBytes);
            default:
                throw new IllegalStateException("Unhandled staging format: " + format);
        }
    }

    /** Appends one converted row. */
    void append(GenericRecord record) throws IOException;

    /** Returns the bytes handed to the staging stream so far, trailing the appended data. */
    long bytesWritten();

    /**
     * Closes the file — finalizing the staging object — and returns its committable.
     *
     * @return the committable describing the finalized object
     * @throws IOException if the file cannot be finalized
     */
    FileLoadsCommittable finish() throws IOException;

    /**
     * Discards the file without finalizing its object: nothing is encoded, uploaded or closed, and
     * the method does not fail. The staging upload is left unfinalized, which creates no object;
     * Cloud Storage expires an unfinalized resumable upload on its own. The writer's buffers are
     * heap memory and go with the writer. Callers still tolerate an implementation that fails.
     *
     * <p>Not closing is safe only while three conditions hold. The staging stream must be the Cloud
     * Storage HTTP upload channel that {@code GcsStagingStorage} opens, which holds no connection
     * or thread between chunk requests. The format writer's codec must hold no resource that only
     * {@code close()} releases. Parquet's buffers must come from its default heap allocator, since
     * {@code close()} is what releases an allocator's buffers. Revisit this method before any of
     * them changes, for example before switching the storage client to the gRPC transport, adding a
     * Parquet codec or giving the Parquet builder an allocator.
     */
    void abort();
}
