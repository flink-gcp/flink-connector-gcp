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
    private long lastRowGroupBytes;

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
        // A failure here leaves the staging stream unclosed, as abort() does: closing it would
        // finalize an object holding a partial file. The builder keeps Parquet's default heap
        // allocator on purpose; abort() never closes the writer, so an allocator whose buffers
        // need an explicit release would leak them.
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
    }

    /**
     * Sizes the row group from the roll threshold, which is not a tuning choice but a correctness
     * one.
     *
     * <p>Parquet buffers a whole row group before anything reaches the stream, and {@link
     * #isFull(long)} decides only at row-group boundaries, so with Parquet's own default of 128 MiB
     * no row group would flush before the file is closed. A 16 MiB threshold would then never fire
     * and every file would run to end of input.
     *
     * <p>A row group is coarser than Avro's 64,000-byte block, which is affordable because
     * row-group count was measured not to affect load duration: 1, 3, 5 and 11 groups per 32 MiB
     * file loaded in 7.5-8.0 s (#285).
     */
    private static long rowGroupSize(long maxStagingFileBytes) {
        return maxStagingFileBytes;
    }

    /**
     * Maps the option to a Parquet codec. A codec added here must hold no resource that only {@code
     * close()} releases, because {@link #abort()} never closes the writer; a compressor borrowed
     * from Hadoop's {@code CodecPool}, for example, would never be returned.
     */
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
        long before = countingStream.getCount();
        parquetWriter.write(record);
        // Nothing but a row-group flush reaches the stream after the magic written on open.
        long flushed = countingStream.getCount() - before;
        if (flushed > 0) {
            lastRowGroupBytes = flushed;
        }
        rowCount++;
    }

    /**
     * Full at the row-group boundary expected to be nearest the threshold: once the bytes written
     * are within half the last row group of it, so that another group of that size would land
     * further away. Only an expectation: a next group larger than the last, from rows that compress
     * worse, can carry the file further past the threshold.
     *
     * <p>The byte count alone does not work. Parquet flushes a row group when its estimate of the
     * buffered size nears the threshold, preferring under to over, and the estimate counts each
     * column's open page uncompressed. One incompressible column therefore usually flushes groups
     * just under the threshold, which a byte-count roll does not fire on, so files took a second
     * group and closed at about twice the threshold (#1704); many compressible columns flush groups
     * a fraction of it, which only accumulate towards it.
     */
    @Override
    public boolean isFull(long maxStagingFileBytes) {
        return lastRowGroupBytes > 0
                && countingStream.getCount() + lastRowGroupBytes / 2 >= maxStagingFileBytes;
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
        // Closing parquetWriter would encode, compress and upload the whole buffered row group
        // and finalize the object. Both configured codecs hold no native resource between pages:
        // UNCOMPRESSED has no compressor, and Parquet's ZstandardCodec creates none, opening and
        // closing a compression stream per page instead.
    }

    /**
     * Parquet's output abstraction over the staging stream.
     *
     * <p>{@link PositionOutputStream} needs only a running byte position, never a seek, so a Cloud
     * Storage resumable upload satisfies it directly — no Hadoop {@code FileSystem} and no local
     * spill file. That is what keeps the <em>streaming</em> shape the same as the Avro path's; the
     * buffered row group above is where the two paths' memory genuinely differs.
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
