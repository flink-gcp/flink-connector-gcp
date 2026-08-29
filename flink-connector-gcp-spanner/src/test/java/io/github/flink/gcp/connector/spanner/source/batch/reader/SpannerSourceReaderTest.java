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

package io.github.flink.gcp.connector.spanner.source.batch.reader;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;

import com.google.cloud.spanner.TestPartitions;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceBuilder;
import io.github.flink.gcp.connector.spanner.source.TestSources;
import io.github.flink.gcp.connector.spanner.source.batch.BatchReadSplit;
import io.github.flink.gcp.connector.testutils.CollectingReaderOutput;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link SpannerSourceReader}, driven through the source's production {@code
 * createReader}.
 */
class SpannerSourceReaderTest {

    private TestReaderMetrics metrics;

    @AfterEach
    void forgetRecordings() {
        ScriptedStructStreamOpener.reset();
    }

    @Test
    void aReaderWithNoSplitsAsksForOneWhenItStarts() throws Exception {
        FakeSourceReaderContext context = context();
        try (SourceReader<Long, BatchReadSplit> reader = reader(context, "r1")) {
            reader.start();

            assertThat(context.splitRequests()).isEqualTo(1);
        }
    }

    @Test
    void aRestoredReaderWithSplitsDoesNotAskForAnother() throws Exception {
        FakeSourceReaderContext context = context();
        try (SourceReader<Long, BatchReadSplit> reader = reader(context, "r2")) {
            reader.addSplits(Collections.singletonList(split("p0")));
            reader.start();

            // Its splits arrived before it started; asking again would hand it a second one it
            // cannot begin until the first is done.
            assertThat(context.splitRequests()).isZero();
        }
    }

    @Test
    @Timeout(60)
    void aFinishedSplitIsFollowedByARequestForTheNextOne() throws Exception {
        FakeSourceReaderContext context = context();
        CollectingReaderOutput<Long> output = new CollectingReaderOutput<>();
        try (SourceReader<Long, BatchReadSplit> reader = reader(context, "r3")) {
            reader.start();
            reader.addSplits(Collections.singletonList(split("p0")));
            reader.notifyNoMoreSplits();

            InputStatus status = reader.pollNext(output);
            while (status != InputStatus.END_OF_INPUT) {
                status = reader.pollNext(output);
            }

            assertThat(output.records()).containsExactly(1L, 2L);
            // One for the start, one for the finished split: pull assignment is what keeps the
            // enumerator from having to track who holds what.
            assertThat(context.splitRequests()).isEqualTo(2);
        }
    }

    @Test
    void closingTheReaderClosesTheSharedOpener() throws Exception {
        FakeSourceReaderContext context = context();
        ScriptedStructStreamOpener opener = ScriptedStructStreamOpener.single("r4", 1, 2);

        SourceReader<Long, BatchReadSplit> reader =
                TestSources.source(builder -> TestSources.withOpener(builder, opener))
                        .createReader(context);
        reader.close();

        // The opener holds the reader's Spanner client, and the reader is its only owner.
        assertThat(opener.closes()).isEqualTo(1);
    }

    @Test
    @Timeout(30)
    void publicFetchLimitsReachTheRuntimeSplitReader() throws Exception {
        assertFirstBatchReturnsBeforeBlockedSecondRow("r5", builder -> builder.maxRowsPerFetch(1));
        assertFirstBatchReturnsBeforeBlockedSecondRow("r6", builder -> builder.maxBytesPerFetch(8));
    }

    private void assertFirstBatchReturnsBeforeBlockedSecondRow(
            String id, UnaryOperator<SpannerSourceBuilder<Long>> limits) throws Exception {
        FakeSourceReaderContext context = context();
        ScriptedStructStreamOpener opener = ScriptedStructStreamOpener.single(id, 1, 2);
        opener.blockBefore(1);
        CollectingReaderOutput<Long> output = new CollectingReaderOutput<>();

        try (SourceReader<Long, BatchReadSplit> reader =
                TestSources.source(builder -> limits.apply(TestSources.withOpener(builder, opener)))
                        .createReader(context)) {
            reader.start();
            reader.addSplits(Collections.singletonList(split("p0")));

            reader.isAvailable().get(10, TimeUnit.SECONDS);
            reader.pollNext(output);

            assertThat(output.records()).containsExactly(1L);
        }
    }

    private FakeSourceReaderContext context() {
        metrics = new TestReaderMetrics();
        return new FakeSourceReaderContext(metrics.metricGroup());
    }

    private SourceReader<Long, BatchReadSplit> reader(FakeSourceReaderContext context, String id)
            throws Exception {
        ScriptedStructStreamOpener opener = ScriptedStructStreamOpener.single(id, 1, 2);
        return TestSources.source(builder -> TestSources.withOpener(builder, opener))
                .createReader(context);
    }

    private static BatchReadSplit split(String token) {
        return new BatchReadSplit(
                token,
                TestPartitions.batchTransactionId(),
                TestPartitions.queryPartition(token, "SELECT id FROM singers"));
    }
}
