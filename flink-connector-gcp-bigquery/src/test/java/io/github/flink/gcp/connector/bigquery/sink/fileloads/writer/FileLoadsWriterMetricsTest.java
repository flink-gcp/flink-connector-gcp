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

import org.apache.flink.api.connector.sink2.SinkWriter;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.FileLoadsWriterTest.TestRow;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the metrics {@link FileLoadsWriter} registers, against the same in-memory staging
 * storage its behavioural tests use.
 *
 * <p>Every assertion goes through the name a metric registered under, so renaming one — or failing
 * to register it — fails here.
 */
class FileLoadsWriterMetricsTest {

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
    private final InMemoryStagingStorage storage = new InMemoryStagingStorage();

    @Test
    void countsEveryRecordStagedAndTheBytesOfEachFinishedFile() throws Exception {
        FileLoadsWriter<TestRow> writer = writer();

        writer.write(new TestRow("t", "alice", 1L), CONTEXT);
        writer.write(new TestRow("t", "bob", 2L), CONTEXT);

        // The staging file is the hand-off, so the records count immediately — but their bytes
        // are unknown until the Avro file is closed.
        assertThat(counter("numRecordsSend")).isEqualTo(2);
        assertThat(counter("numBytesSend")).isZero();
        assertThat(counter("filesStaged")).isZero();
        assertThat(this.<Integer>gauge("pendingFiles")).isEqualTo(1);

        Collection<FileLoadsCommittable> committables = writer.prepareCommit();

        assertThat(committables).hasSize(1);
        assertThat(this.<Integer>gauge("pendingFiles")).isZero();
        assertThat(counter("filesStaged")).isEqualTo(1);
        assertThat(counter("numBytesSend"))
                .isEqualTo(committables.iterator().next().getByteCount())
                .isPositive();
    }

    @Test
    void countsOneStagedFilePerRolledFile() throws Exception {
        // A tiny roll threshold makes every record its own file.
        FileLoadsWriter<TestRow> writer = writer(1);

        writer.write(new TestRow("t", "alice", 1L), CONTEXT);
        writer.write(new TestRow("t", "bob", 2L), CONTEXT);
        Collection<FileLoadsCommittable> committables = writer.prepareCommit();

        assertThat(counter("filesStaged")).isEqualTo(2);
        assertThat(counter("destinationActivations")).isEqualTo(2);
        assertThat(this.<Integer>gauge("openDestinations")).isZero();
        assertThat(this.<Integer>gauge("pendingFiles")).isZero();
        assertThat(counter("numRecordsSend")).isEqualTo(2);
        assertThat(counter("numBytesSend"))
                .isEqualTo(
                        committables.stream().mapToLong(FileLoadsCommittable::getByteCount).sum());
    }

    @Test
    void countsARecordTheSerializerRejectedAsASendError() throws Exception {
        FileLoadsWriter<TestRow> writer = writer();

        writer.write(new TestRow("t", "alice", 1L, true, false), CONTEXT);

        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        assertThat(counter("numRecordsSend")).isZero();
        // No file was opened for a destination whose only record never reached one.
        assertThat(this.<Integer>gauge("openDestinations")).isZero();
    }

    @Test
    void skipsRecordsTheSerializerReturnsNullFor() throws Exception {
        // Per-destination metrics on, or the last assertion below could not fail: with them off
        // every destination lookup is a no-op and registers nothing whatever the writer does.
        FileLoadsWriter<TestRow> writer =
                writer(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .perDestinationMetrics(true)
                                .build());

        writer.write(new TestRow("skipped-table", "skip-me", 1L), CONTEXT);
        writer.write(new TestRow("t", "alice", 1L), CONTEXT);

        // Skipped, not failed: staged nowhere, and no file — nor a per-destination counter, which
        // registers on first use and can never be unregistered — for the table it would have gone
        // to. The staged record is the control that keeps this from passing on a counter that
        // counted every record.
        assertThat(counter("recordsSkipped")).isEqualTo(1);
        assertThat(counter("numRecordsSend")).isEqualTo(1);
        assertThat(counter("numRecordsSendErrors")).isZero();
        assertThat(this.<Integer>gauge("openDestinations")).isEqualTo(1);
        assertThat(metrics.hasMetric("destination", "p.d.skipped-table", "recordsSend")).isFalse();
    }

    @Test
    void countsARowThatDoesNotConformToTheDescriptorAsASendError() throws Exception {
        FileLoadsWriter<TestRow> writer = writer();

        writer.write(new TestRow("t", "alice", 1L, false, true), CONTEXT);

        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        assertThat(counter("numRecordsSend")).isZero();
    }

    @Test
    void countsARowTheAvroConversionRejectedAsASendError() throws Exception {
        // The other half of the routed-failure pair: this row parses and then fails on a value,
        // which is a different catch block from the one above.
        FileLoadsWriter<TestRow> writer = writer();

        writer.write(new TestRow("t", "unconvertible", 1L), CONTEXT);

        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        assertThat(counter("numRecordsSend")).isZero();
    }

    @Test
    void gaugeReportsDestinationsWithOpenStagingFiles() throws Exception {
        FileLoadsWriter<TestRow> writer = writer();

        assertThat(this.<Integer>gauge("openDestinations")).isZero();

        writer.write(new TestRow("t1", "alice", 1L), CONTEXT);
        writer.write(new TestRow("t2", "bob", 2L), CONTEXT);
        writer.write(new TestRow("t1", "carol", 3L), CONTEXT);

        assertThat(this.<Integer>gauge("openDestinations")).isEqualTo(2);
    }

    @Test
    void registersNoPerDestinationCountersByDefault() throws Exception {
        FileLoadsWriter<TestRow> writer = writer();

        writer.write(new TestRow("t", "alice", 1L), CONTEXT);

        assertThat(counter("numRecordsSend")).isEqualTo(1);
        assertThat(metrics.hasMetric("destination", "p.d.t", "recordsSend")).isFalse();
    }

    @Test
    void countsPerTableWhenPerDestinationMetricsAreOn() throws Exception {
        FileLoadsWriter<TestRow> writer =
                writer(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .perDestinationMetrics(true)
                                .build());

        writer.write(new TestRow("t1", "alice", 1L), CONTEXT);
        writer.write(new TestRow("t2", "bob", 2L), CONTEXT);
        writer.write(new TestRow("t1", "carol", 3L, true, false), CONTEXT);

        assertThat(metrics.counterValue("destination", "p.d.t1", "recordsSend")).isEqualTo(1);
        assertThat(metrics.counterValue("destination", "p.d.t1", "sendErrors")).isEqualTo(1);
        assertThat(metrics.counterValue("destination", "p.d.t2", "recordsSend")).isEqualTo(1);
        assertThat(metrics.counterValue("destination", "p.d.t2", "sendErrors")).isZero();
    }

    @Test
    void writerStateGaugesAreClearedWhenTheWriterIsClosed() throws Exception {
        FileLoadsWriter<TestRow> writer = writer();

        writer.write(new TestRow("t1", "alice", 1L), CONTEXT);
        writer.write(new TestRow("t2", "bob", 2L), CONTEXT);
        assertThat(this.<Integer>gauge("openDestinations")).isEqualTo(2);
        assertThat(this.<Integer>gauge("pendingFiles")).isEqualTo(2);

        writer.close();

        assertThat(this.<Integer>gauge("openDestinations")).isZero();
        assertThat(this.<Integer>gauge("pendingFiles")).isZero();
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private long counter(String... identifier) {
        return metrics.counterValue(identifier);
    }

    private <T> T gauge(String name) {
        return metrics.gaugeValue(name);
    }

    private FileLoadsWriter<TestRow> writer() {
        return writer(FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES);
    }

    private FileLoadsWriter<TestRow> writer(long maxStagingFileBytes) {
        return writer(
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxStagingFileBytes(maxStagingFileBytes)
                        .build());
    }

    private FileLoadsWriter<TestRow> writer(FileLoadsOptions options) {
        return new FileLoadsWriter<>(
                FileLoadsWriterTest.config(FailureHandler.logAndDrop()),
                options,
                storage,
                metrics,
                "0123456789abcdef0123456789abcdef",
                3,
                1,
                new ManualProcessingTimeService());
    }
}
