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

package io.github.flink.gcp.connector.base.lineage;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.RowData;

import io.github.flink.gcp.connector.testutils.CollectingReaderOutput;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class LineageTestFixturesTest {
    @Test
    void readerWaitsForCompletionNotificationAndWakesPendingAvailability() throws Exception {
        FakeSourceReaderContext context = readerContext();
        CollectingReaderOutput<RowData> output = new CollectingReaderOutput<>();
        try (SourceReader<RowData, SourceSplit> reader = source().createReader(context)) {
            reader.start();
            CompletableFuture<Void> firstAvailability = reader.isAvailable();
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
            assertThat(firstAvailability).isNotDone();
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
            CompletableFuture<Void> nextAvailability = reader.isAvailable();
            assertThat(nextAvailability).isNotDone();
            assertThat(context.splitRequests()).isZero();

            reader.notifyNoMoreSplits();

            assertThat(firstAvailability).isCompletedWithValue(null);
            assertThat(nextAvailability).isCompletedWithValue(null);
            assertThat(reader.isAvailable()).isCompletedWithValue(null);
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
            assertThat(output.records()).isEmpty();
        }
    }

    @Test
    void notificationBeforePollingAndRepeatedNotificationsKeepReaderFinished() throws Exception {
        CollectingReaderOutput<RowData> output = new CollectingReaderOutput<>();
        try (SourceReader<RowData, SourceSplit> reader = source().createReader(readerContext())) {
            reader.start();
            reader.notifyNoMoreSplits();
            assertThat(reader.isAvailable()).isCompletedWithValue(null);
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);

            reader.notifyNoMoreSplits();
            assertThat(reader.isAvailable()).isCompletedWithValue(null);
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
            assertThat(output.records()).isEmpty();
        }
    }

    @Test
    void completionNotificationOnlyReleasesItsOwnReader() throws Exception {
        LineageTestFixtures.FakeSource source = source();
        CollectingReaderOutput<RowData> output = new CollectingReaderOutput<>();
        try (SourceReader<RowData, SourceSplit> first = source.createReader(readerContext());
                SourceReader<RowData, SourceSplit> second = source.createReader(readerContext())) {
            first.start();
            second.start();
            CompletableFuture<Void> secondAvailability = second.isAvailable();
            first.notifyNoMoreSplits();

            assertThat(first.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
            assertThat(second.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
            assertThat(secondAvailability).isNotDone();

            second.notifyNoMoreSplits();
            assertThat(secondAvailability).isCompletedWithValue(null);
            assertThat(second.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
        }
    }

    @Test
    void enumeratorSignalsCompletionOnRegistrationWithoutRepeatingItForRequests() throws Exception {
        FakeSplitEnumeratorContext<SourceSplit> context = new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<SourceSplit, Void> enumerator = source().createEnumerator(context)) {
            enumerator.start();
            assertThat(context.events()).isEmpty();
            context.registerReader(0);
            enumerator.addReader(0);
            assertThat(context.events()).containsExactly("noMoreSplits:0");

            enumerator.handleSplitRequest(0, "localhost");
            assertThat(context.events()).containsExactly("noMoreSplits:0");

            context.unregisterReader(0);
            context.registerReader(0);
            enumerator.addReader(0);
            assertThat(context.events()).containsExactly("noMoreSplits:0", "noMoreSplits:0");
        }
    }

    private static LineageTestFixtures.FakeSource source() {
        return new LineageTestFixtures.FakeSource(List.of(), Boundedness.BOUNDED, false);
    }

    private static FakeSourceReaderContext readerContext() {
        return new FakeSourceReaderContext(
                UnregisteredMetricsGroup.createSourceReaderMetricGroup());
    }
}
