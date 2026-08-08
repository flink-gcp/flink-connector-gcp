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
     * The staging codec: zstandard at Avro's own {@code DEFAULT_ZSTANDARD_LEVEL} (3).
     *
     * <p><b>Chosen for CPU, not for size.</b> Compression runs on the task thread — the same one
     * streaming into the GCS upload while the job processes records — so its cost is subtracted
     * from throughput directly. Measured 2026-08-08 with this writer, 2,000,000 rows of a realistic
     * mix, five passes after a warm-up, single-threaded on OpenJDK 21 (aarch64): deflate 11,436 ms,
     * zstandard 3,182 ms, and 2,134 ms with no codec at all — so the compression itself costs 9,302
     * ms against 1,048 ms, a factor of 8.9.
     *
     * <p>Size is a wash and must not be quoted as a reason: 201,708,003 bytes against deflate's
     * 198,227,825, so zstandard is 1.8% <em>larger</em> here, matching the 1.01-1.02x measured at a
     * million rows on #283. A 17% win appears only on files of a few thousand rows, where the
     * dictionary dominates, and does not generalise.
     *
     * <p>Level 3 rather than a lower one: level 1 measured slower (3,459 ms) <em>and</em> 5%
     * larger, so there is nothing below to trade for.
     *
     * <p>The single-argument overload is the one taken deliberately: it selects {@code
     * ZstandardCodec.Option(level, false, false)} — no per-block checksum and no recycling buffer
     * pool. A checksum would duplicate what the Avro container and GCS already provide, and the
     * pool is a memory/throughput trade this writer has no measurement for. Note the two- and
     * three-argument overloads take {@code useChecksum} before {@code useBufferPool}, which is easy
     * to transpose.
     *
     * <p>Shared as a static because a {@code CodecFactory} is an immutable descriptor — {@code
     * DataFileWriter.setCodec} calls {@code createInstance()} to build a per-writer {@code Codec} —
     * so one instance is safe across the writers of a TaskManager. Avro keeps its own built-ins in
     * a static registry the same way.
     *
     * <p>The library is {@code com.github.luben:zstd-jni}, which Avro declares {@code optional} —
     * this module declares it at runtime scope, and a shaded distribution has to keep its native
     * libraries reachable.
     */
    private static final CodecFactory STAGING_CODEC =
            CodecFactory.zstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL);

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
            OutputStream stream)
            throws IOException {
        this.flinkJobId = flinkJobId;
        this.destination = destination;
        this.uri = uri;
        this.countingStream = new CountingOutputStream(stream);
        DataFileWriter<GenericRecord> writer =
                new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema))
                        .setCodec(STAGING_CODEC);
        try {
            this.avroWriter = writer.create(schema, countingStream);
        } catch (IOException | RuntimeException e) {
            // DataFileWriter.close() is a no-op before create() succeeds; close the staging
            // stream directly so the upload channel does not leak.
            try {
                stream.close();
            } catch (IOException | RuntimeException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    @Override
    public void append(GenericRecord record) throws IOException {
        avroWriter.append(record);
        rowCount++;
    }

    @Override
    public long bytesWritten() {
        return countingStream.getCount();
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
        try {
            avroWriter.close();
        } catch (IOException | RuntimeException e) {
            // The object is unreferenced garbage either way; nothing to do.
        }
    }
}
