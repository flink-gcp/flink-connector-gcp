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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.DeserializationFailurePolicy;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the checkpoint and acknowledgement wiring the reader owns.
 *
 * <p>Most tests add no split, so no fetcher thread starts: the reader's own contribution is exactly
 * the two lifecycle hooks exercised here. The split-ownership test is the exception — what it pins
 * only shows on a reader that actually holds a split, so it runs one idle fake split reader.
 */
class PubSubSourceReaderTest {

    private static final String SPLIT_ID = "0";

    private final TestReaderMetrics testMetrics = new TestReaderMetrics();
    private final PubSubAckTracker ackTracker = new PubSubAckTracker(testMetrics.metrics(), null);

    @Test
    void aCompletedCheckpointAcknowledgesTheMessagesItCovers() throws Exception {
        RecordingAckHandle staged = new RecordingAckHandle("staged");
        RecordingAckHandle afterBarrier = new RecordingAckHandle("afterBarrier");
        receiveAndStage("staged", staged);

        try (PubSubSourceReader<String> reader = reader()) {
            reader.snapshotState(1L);
            // Emitted after the barrier, so it belongs to the next checkpoint.
            receiveAndStage("afterBarrier", afterBarrier);

            reader.notifyCheckpointComplete(1L);

            assertThat(staged.isAcked()).isTrue();
            assertThat(afterBarrier.isUnsettled()).isTrue();
            assertThat(testMetrics.counter("messagesAcked")).isEqualTo(1);
        }
    }

    @Test
    void completingALaterCheckpointSweepsTheOnesBeforeIt() throws Exception {
        RecordingAckHandle first = new RecordingAckHandle("first");
        RecordingAckHandle second = new RecordingAckHandle("second");

        try (PubSubSourceReader<String> reader = reader()) {
            receiveAndStage("first", first);
            reader.snapshotState(1L);
            receiveAndStage("second", second);
            reader.snapshotState(2L);

            // Checkpoint 1 was aborted or its notification lost; 2 heals it.
            reader.notifyCheckpointComplete(2L);

            assertThat(first.isAcked()).isTrue();
            assertThat(second.isAcked()).isTrue();
        }
    }

    @Test
    void reportsOutstandingMessagesAndPendingCheckpointsAsGauges() throws Exception {
        try (PubSubSourceReader<String> reader = reader()) {
            testMetrics.metrics().bindAckTracker(ackTracker);
            receiveAndStage("m1", new RecordingAckHandle("m1"));

            assertThat(testMetrics.gauge("pendingAcks")).isEqualTo(1);
            assertThat(testMetrics.gauge("pendingCheckpoints")).isZero();

            reader.snapshotState(1L);

            assertThat(testMetrics.gauge("pendingAcks")).isEqualTo(1);
            assertThat(testMetrics.gauge("pendingCheckpoints")).isEqualTo(1);

            reader.notifyCheckpointComplete(1L);

            assertThat(testMetrics.gauge("pendingAcks")).isZero();
            assertThat(testMetrics.gauge("pendingCheckpoints")).isZero();
        }
    }

    @Test
    void checkpointsNeverCarrySplits() throws Exception {
        // SourceOperator unions the restored splits with the enumerator's recomputed assignment,
        // so reporting splits here would let one subscription land on two subtasks. The reader has
        // to hold a split for this to pin anything: with none assigned, the base class reports
        // none too, and reverting the override would go unnoticed (measured — the assertion on an
        // empty reader survived exactly that mutant).
        try (PubSubSourceReader<String> reader =
                reader(
                        new MissingCheckpointDetector(Duration.ZERO, () -> 0),
                        IdleSplitReader::new)) {
            reader.addSplits(
                    List.of(
                            new SubscriptionSplit(
                                    SubscriptionDestination.of("it-project", "held"), "0")));

            assertThat(reader.snapshotState(1L)).isEmpty();
        }
    }

    @Test
    void takingACheckpointRetiresTheMissingCheckpointDetector() throws Exception {
        MissingCheckpointDetector detector =
                new MissingCheckpointDetector(Duration.ofMinutes(10), () -> 1);
        // The split reader would do this on its first assignment; nothing is armed until it does.
        detector.startBudget();
        try (PubSubSourceReader<String> reader = reader(detector)) {
            assertThat(detector.parkTimeoutMillis()).isPositive();

            reader.snapshotState(1L);

            assertThat(detector.parkTimeoutMillis()).isZero();
        }
    }

    private void receiveAndStage(String messageId, RecordingAckHandle handle) {
        ackTracker.addPendingAck(SPLIT_ID, messageId, handle);
        ackTracker.stagePendingAck(SPLIT_ID, messageId);
    }

    private PubSubSourceReader<String> reader() {
        return reader(new MissingCheckpointDetector(Duration.ZERO, () -> 0));
    }

    private PubSubSourceReader<String> reader(MissingCheckpointDetector detector) {
        return reader(
                detector,
                () -> {
                    throw new AssertionError(
                            "No split is added, so no split reader should be created.");
                });
    }

    private PubSubSourceReader<String> reader(
            MissingCheckpointDetector detector,
            Supplier<SplitReader<PubsubMessage, SubscriptionSplit>> splitReaders) {
        return new PubSubSourceReader<>(
                splitReaders,
                new PubSubRecordEmitter<>(
                        PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()),
                        ackTracker,
                        DeserializationFailurePolicy.FAIL,
                        testMetrics.metrics()),
                new Configuration(),
                new FakeSourceReaderContext(testMetrics.metricGroup()),
                ackTracker,
                detector);
    }

    /** Accepts splits and fetches nothing, so a reader can hold a split without an emulator. */
    private static final class IdleSplitReader
            implements SplitReader<PubsubMessage, SubscriptionSplit> {

        @Override
        public RecordsWithSplitIds<PubsubMessage> fetch() {
            try {
                // Nothing will ever arrive; the pause just keeps the fetcher loop from spinning.
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new RecordsBySplits.Builder<PubsubMessage>().build();
        }

        @Override
        public void handleSplitsChanges(SplitsChange<SubscriptionSplit> splitsChange) {}

        @Override
        public void wakeUp() {}

        @Override
        public void close() {}
    }
}
