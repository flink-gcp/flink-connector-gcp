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

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.ParquetCompression;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.OutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StagedFileWriterTest {

    private static final Schema SCHEMA =
            SchemaBuilder.record("Row").fields().requiredString("name").endRecord();

    @ParameterizedTest
    @EnumSource(StagingFormat.class)
    void aFailedOpenLeavesTheStagingStreamUnclosed(StagingFormat format) {
        // Closing the stream would finalize an object holding a partial file, as abort() would.
        IOException writeFailure = new IOException("header write failed");
        FailingStream stream = new FailingStream(writeFailure);

        assertThatThrownBy(
                        () ->
                                StagedFileWriter.open(
                                        format,
                                        ParquetCompression.ZSTD,
                                        "job",
                                        TableDestination.of("project", "dataset", "table"),
                                        "gs://bucket/object",
                                        SCHEMA,
                                        stream,
                                        16L * 1024 * 1024))
                .isSameAs(writeFailure);
        assertThat(stream.closeCalls).isZero();
    }

    /** Fails every write, as a staging upload that cannot start would. */
    private static final class FailingStream extends OutputStream {
        private final IOException failure;
        private int closeCalls;

        private FailingStream(IOException failure) {
            this.failure = failure;
        }

        @Override
        public void write(int b) throws IOException {
            throw failure;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            throw failure;
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }
}
