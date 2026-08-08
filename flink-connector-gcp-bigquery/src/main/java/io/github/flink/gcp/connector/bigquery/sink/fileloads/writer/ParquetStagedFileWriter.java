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

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.ParquetCompression;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A Parquet file, written through {@code parquet-avro} over the same {@link Schema} the Avro path
 * uses — so the two formats share {@code TableSchemaToAvroConverter} and {@code
 * ProtoToAvroConverter} rather than each carrying its own mapping.
 *
 * <p>Loaded only when the staging format is {@code PARQUET}, which is what lets {@code
 * parquet-avro} be a {@code provided} dependency: a deployment that never selects Parquet never
 * resolves this class.
 */
@Internal
final class ParquetStagedFileWriter implements StagedFileWriter {

    private final String flinkJobId;
    private final TableDestination destination;
    private final String uri;
    private final CountingOutputStream countingStream;
    private final ParquetWriter<GenericRecord> parquetWriter;
    private long rowCount;

    ParquetStagedFileWriter(
            String flinkJobId,
            TableDestination destination,
            String uri,
            Schema schema,
            OutputStream stream,
            ParquetCompression compression,
            long maxStagingFileBytes)
            throws IOException {
        this.flinkJobId = flinkJobId;
        this.destination = destination;
        this.uri = uri;
        this.countingStream = new CountingOutputStream(stream);
        try {
            this.parquetWriter =
                    AvroParquetWriter.<GenericRecord>builder(new StreamOutputFile(countingStream))
                            // Before any config(...) call: the builder allocates a
                            // HadoopParquetConfiguration when none is set, and that instantiates
                            // Hadoop's Configuration — which parses core-default.xml off the
                            // classpath and defeats the point of the NONE codec being Hadoop-free.
                            .withConf(new PlainParquetConfiguration())
                            .withSchema(schema)
                            // Explicit, for the same reason: AvroWriteSupport.init() reaches
                            // getDataModel() only when no model was supplied, and that converts the
                            // configuration back into a Hadoop one.
                            .withDataModel(GenericData.get())
                            .withCompressionCodec(codecOf(compression))
                            .withRowGroupSize(rowGroupSize(maxStagingFileBytes))
                            // Three-level LIST, not parquet-avro's legacy two-level default:
                            // BigQuery's enableListInference reads the standard annotation, and a
                            // two-level list would load as an empty array without an error.
                            .config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false")
                            .build();
        } catch (IOException | RuntimeException | LinkageError e) {
            // The builder opened the stream through StreamOutputFile.create(), so nothing else
            // will close it if construction failed. LinkageError is caught deliberately: the
            // Parquet and Hadoop classes are `provided`, and a classpath missing them fails here
            // rather than at the builder's own checks.
            try {
                stream.close();
            } catch (IOException | RuntimeException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /**
     * Sizes the row group from the roll threshold, which is not a tuning choice but a correctness
     * one.
     *
     * <p>Parquet buffers a whole row group before anything reaches the stream, so with its own
     * default of 128 MiB the written byte count that {@link #bytesWritten()} reports — and that the
     * writer rolls on — would stay at zero until the file is closed. A 16 MiB threshold would then
     * never fire and every file would run to end of input.
     *
     * <p>The threshold counts compressed output while a row group is measured against buffered,
     * uncompressed data, so using it directly makes the row group hold rather less compressed data
     * than the threshold and a file therefore closes after two or three of them. That is
     * deliberately coarser than Avro's 64,000-byte block, and it is affordable because row-group
     * count was measured not to affect load duration: 1, 3, 5 and 11 groups per 32 MiB file loaded
     * in 7.5-8.0 s (#285).
     */
    private static long rowGroupSize(long maxStagingFileBytes) {
        return maxStagingFileBytes;
    }

    private static CompressionCodecName codecOf(ParquetCompression compression) {
        switch (compression) {
            case ZSTD:
                return CompressionCodecName.ZSTD;
            case NONE:
                return CompressionCodecName.UNCOMPRESSED;
            default:
                throw new IllegalStateException("Unhandled Parquet compression: " + compression);
        }
    }

    @Override
    public void append(GenericRecord record) throws IOException {
        parquetWriter.write(record);
        rowCount++;
    }

    @Override
    public long bytesWritten() {
        return countingStream.getCount();
    }

    @Override
    public FileLoadsCommittable finish() throws IOException {
        parquetWriter.close();
        return new FileLoadsCommittable(
                flinkJobId,
                destination,
                uri,
                countingStream.getCount(),
                rowCount,
                StagingFormat.PARQUET);
    }

    @Override
    public void abort() {
        try {
            parquetWriter.close();
        } catch (IOException | RuntimeException | LinkageError e) {
            // The object is unreferenced garbage either way; nothing to do. LinkageError is
            // included for the same reason the constructor catches it: these classes are
            // `provided`, so a classpath that satisfied the client's probe and not the
            // TaskManager's can fail here — and abort() runs on the writer's own close path,
            // where an escaping Error would mask whatever is already failing.
        }
    }

    /**
     * Parquet's output abstraction over the staging stream.
     *
     * <p>{@link PositionOutputStream} needs only a running byte position, never a seek, so a Cloud
     * Storage resumable upload satisfies it directly — no Hadoop {@code FileSystem} and no local
     * spill file, which is what keeps this path's memory profile the same as the Avro one's.
     */
    private static final class StreamOutputFile implements OutputFile {

        private final CountingOutputStream counting;

        StreamOutputFile(CountingOutputStream counting) {
            this.counting = counting;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new PositionOutputStream() {

                @Override
                public long getPos() {
                    return counting.getCount();
                }

                @Override
                public void write(int b) throws IOException {
                    counting.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    counting.write(b, off, len);
                }

                @Override
                public void flush() throws IOException {
                    counting.flush();
                }

                @Override
                public void close() throws IOException {
                    counting.close();
                }
            };
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return ParquetWriter.DEFAULT_BLOCK_SIZE;
        }
    }
}
