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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.OutputStream;

/**
 * One open Avro staging file: an Avro container writer over a byte-counting staging stream,
 * tracking the row count and (approximate, trailing by up to one unflushed Avro block) byte count
 * used for size-based rolling and load-job partitioning.
 */
@Internal
final class StagedFileWriter {

    private final TableDestination destination;
    private final String uri;
    private final CountingOutputStream countingStream;
    private final DataFileWriter<GenericRecord> avroWriter;
    private long rowCount;

    StagedFileWriter(TableDestination destination, String uri, Schema schema, OutputStream stream)
            throws IOException {
        this.destination = destination;
        this.uri = uri;
        this.countingStream = new CountingOutputStream(stream);
        DataFileWriter<GenericRecord> writer =
                new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema))
                        .setCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL));
        try {
            this.avroWriter = writer.create(schema, countingStream);
        } catch (IOException e) {
            writer.close();
            throw e;
        }
    }

    void append(GenericRecord record) throws IOException {
        avroWriter.append(record);
        rowCount++;
    }

    /** Returns the bytes written so far, trailing the appended data by one unflushed block. */
    long bytesWritten() {
        return countingStream.getCount();
    }

    /**
     * Closes the file — finalizing the staging object — and returns its committable.
     *
     * @return the committable describing the finalized object
     * @throws IOException if the file cannot be finalized
     */
    FileLoadsCommittable finish() throws IOException {
        avroWriter.close();
        return new FileLoadsCommittable(destination, uri, countingStream.getCount(), rowCount);
    }

    /** Closes the file discarding errors; the object (finalized or not) is never referenced. */
    void abort() {
        try {
            avroWriter.close();
        } catch (IOException | RuntimeException e) {
            // The object is unreferenced garbage either way; nothing to do.
        }
    }

    /** Plain byte-counting stream wrapper (kept local to avoid a Guava dependency). */
    private static final class CountingOutputStream extends OutputStream {

        private final OutputStream delegate;
        private long count;

        CountingOutputStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        long getCount() {
            return count;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            count++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            count += len;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
