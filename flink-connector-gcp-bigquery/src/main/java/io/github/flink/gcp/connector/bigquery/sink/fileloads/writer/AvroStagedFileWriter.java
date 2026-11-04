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
import io.github.flink.gcp.connector.bigquery.sink.fileloads.AvroCompression;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An Avro container file: the default staging format, and the only one with no extra dependency.
 */
@Internal
final class AvroStagedFileWriter implements StagedFileWriter {

    /**
     * Maps the options to an Avro codec: zstandard at the configured level, or none.
     *
     * <p><b>The zstandard default is chosen for CPU, not for size.</b> Compression runs on the task
     * thread, the same one streaming into the GCS upload while the job processes records, so its
     * cost is subtracted from throughput directly. Measured 2026-08-08 with this writer, 2,000,000
     * rows of a realistic mix, five passes after a warm-up, single-threaded on OpenJDK 21
     * (aarch64): deflate 11,436 ms, zstandard level 3 3,182 ms, and 2,134 ms with no codec at all,
     * so the compression itself costs 9,302 ms against 1,048 ms, a factor of 8.9. Size is a wash
     * against deflate there (zstandard 1.8% larger), so it must not be quoted as a reason.
     *
     * <p>Deflate and snappy are not offered (#2022). On a nested row of about 9.5 KB, zstandard at
     * level -3 matched snappy's CPU with 4% fewer bytes, and deflate was slower and larger than
     * zstandard at every level measured; snappy would also add a native library to the shaded
     * distribution.
     *
     * <p>The single-argument overload is the one taken deliberately: it selects {@code
     * ZstandardCodec.Option(level, false, false)}, with no per-block checksum and no recycling
     * buffer pool. A checksum would duplicate what the Avro container and GCS already provide, and
     * the pool is a memory/throughput trade this writer has no measurement for. Note the two- and
     * three-argument overloads take {@code useChecksum} before {@code useBufferPool}, which is easy
     * to transpose.
     *
     * <p>A {@code CodecFactory} is an immutable descriptor: {@code DataFileWriter.setCodec} calls
     * {@code createInstance()} to build a per-writer {@code Codec}.
     *
     * <p>The zstandard library is {@code com.github.luben:zstd-jni}, which Avro declares {@code
     * optional}; this module declares it at runtime scope, and a shaded distribution has to keep
     * its native libraries reachable.
     *
     * <p>{@link #abort()} never closes the writer, which is safe because the zstandard codec opens
     * and closes its compression stream per block and the null codec holds nothing. Revisit that
     * method before adding a codec.
     *
     * @param compression the configured codec
     * @param zstdLevel the zstandard level, ignored for {@link AvroCompression#NONE}
     * @return the codec factory
     */
    private static CodecFactory codecOf(AvroCompression compression, int zstdLevel) {
        switch (compression) {
            case ZSTD:
                return CodecFactory.zstandardCodec(zstdLevel);
            case NONE:
                return CodecFactory.nullCodec();
            default:
                throw new IllegalStateException("Unhandled Avro compression: " + compression);
        }
    }

    private final String flinkJobId;
    private final TableDestination destination;
    private final String uri;
    private final CountingOutputStream countingStream;
    private final DataFileWriter<GenericRecord> avroWriter;
    private long rowCount;

    AvroStagedFileWriter(
            String flinkJobId,
            TableDestination destination,
            String uri,
            Schema schema,
            OutputStream stream,
            AvroCompression compression,
            int zstdLevel)
            throws IOException {
        this.flinkJobId = flinkJobId;
        this.destination = destination;
        this.uri = uri;
        this.countingStream = new CountingOutputStream(stream);
        DataFileWriter<GenericRecord> writer =
                new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema))
                        .setCodec(codecOf(compression, zstdLevel));
        // A failure here leaves the staging stream unclosed, as abort() does: closing it would
        // finalize an object holding a partial header.
        this.avroWriter = writer.create(schema, countingStream);
    }

    @Override
    public void append(GenericRecord record) throws IOException {
        avroWriter.append(record);
        rowCount++;
    }

    /** Full once the bytes written reach the threshold; they trail the data by one Avro block. */
    @Override
    public boolean isFull(long maxStagingFileBytes) {
        return countingStream.getCount() >= maxStagingFileBytes;
    }

    @Override
    public FileLoadsCommittable finish() throws IOException {
        avroWriter.close();
        return new FileLoadsCommittable(
                flinkJobId,
                destination,
                uri,
                countingStream.getCount(),
                rowCount,
                StagingFormat.AVRO);
    }

    @Override
    public void abort() {
        // Closing avroWriter would compress the pending block and finalize the object. Neither
        // codec holds a native resource that waits for close(): zstandard opens and closes its
        // compression stream per block, and the null codec has none.
    }
}
