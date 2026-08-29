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

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.FileLoadsWriterTest.TestRow;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;

/** Child-process entry point for {@link FileLoadsWriterMemoryBoundaryTest}. */
final class FileLoadsWriterMemoryProbe {

    private static final int DESTINATIONS = 1_000;
    private static final int MODELED_UPLOAD_CHUNK_BYTES = 4 * 1024 * 1024;

    private FileLoadsWriterMemoryProbe() {}

    public static void main(String[] args) throws Exception {
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        FileLoadsWriter<TestRow> writer =
                new FileLoadsWriter<>(
                        FileLoadsWriterTest.config(FailureHandler.failJob()),
                        FileLoadsOptions.builder().stagingPath("gs://bucket/probe").build(),
                        new TouchingStorage(),
                        metrics,
                        "0123456789abcdef0123456789abcdef",
                        0,
                        0,
                        new ManualProcessingTimeService());

        for (int i = 0; i < DESTINATIONS; i++) {
            writer.write(new TestRow("t" + i, "row", (long) i), TestContexts.NO_OP);
        }

        int active = metrics.gaugeValue("openDestinations");
        Collection<FileLoadsCommittable> files = writer.prepareCommit();
        writer.close();
        if (active != FileLoadsOptions.DEFAULT_MAX_OPEN_DESTINATIONS
                || files.size() != DESTINATIONS) {
            throw new AssertionError("active=" + active + ", files=" + files.size());
        }
        System.out.println("PASS active=" + active + " files=" + files.size());
    }

    /**
     * Models the measured GCS cost: one touched 4 MiB chunk per open object, released on finish.
     */
    private static final class TouchingStorage implements StagingStorage {
        private static final long serialVersionUID = 1L;

        @Override
        public OutputStream createObject(String gcsUri) {
            return new OutputStream() {
                private byte[] chunk = touchedChunk();

                @Override
                public void write(int value) throws IOException {
                    chunk[0] = (byte) value;
                }

                @Override
                public void write(byte[] bytes, int offset, int length) throws IOException {
                    if (length > 0) {
                        chunk[0] = bytes[offset];
                    }
                }

                @Override
                public void close() {
                    chunk = null;
                }
            };
        }

        @Override
        public void deleteObjects(List<String> gcsUris) {}

        @Override
        public void close() {}

        private static byte[] touchedChunk() {
            byte[] chunk = new byte[MODELED_UPLOAD_CHUNK_BYTES];
            for (int i = 0; i < chunk.length; i += 4096) {
                chunk[i] = 1;
            }
            return chunk;
        }
    }
}
