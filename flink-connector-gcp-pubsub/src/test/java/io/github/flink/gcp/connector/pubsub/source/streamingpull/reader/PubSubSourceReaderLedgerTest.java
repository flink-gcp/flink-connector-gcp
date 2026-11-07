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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.DeserializationFailurePolicy;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.LedgerChunkEvent;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.LedgerUploadedEvent;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.MessageIdLedgerCodec;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests how {@link PubSubSourceReader} carries the restore-deduplication ledger (#2049). */
class PubSubSourceReaderLedgerTest {

    private static final SubscriptionDestination SUB =
            SubscriptionDestination.of("it-project", "held");
    private static final SubscriptionSplit SPLIT = new SubscriptionSplit(SUB, "0");

    private final TestReaderMetrics testMetrics = new TestReaderMetrics();
    private final PubSubAckTracker ackTracker = new PubSubAckTracker(testMetrics.metrics(), null);
    private final AtomicLong clock = new AtomicLong();
    private final RestoredLedger ledger =
            new RestoredLedger(Duration.ofMinutes(5), clock::get, testMetrics.metrics());
    private final FakeSourceReaderContext context =
            new FakeSourceReaderContext(testMetrics.metricGroup());

    /** Filled on the fetcher thread, so a test waits for the change it expects. */
    private final BlockingQueue<SplitsChange<SubscriptionSplit>> splitChanges =
            new LinkedBlockingQueue<>();

    @Test
    void aCheckpointCarriesTheIdsItCoversAsLedgerChunksAndNoConsumingSplit() throws Exception {
        try (PubSubSourceReader<String> reader = reader(ledger)) {
            reader.addSplits(List.of(SPLIT));
            emit("covered");
            ackTracker.addPendingAck(SPLIT.splitId(), "received-only", new RecordingAckHandle("x"));

            List<SubscriptionSplit> state = reader.snapshotState(9L);

            assertThat(state).hasSize(1);
            SubscriptionSplit chunk = state.get(0);
            assertThat(chunk.isLedgerChunk()).isTrue();
            assertThat(chunk.getSubscription()).isEqualTo(SUB);
            assertThat(MessageIdLedgerCodec.decode(chunk.getLedgerChunk()))
                    .containsExactly("covered");
        }
    }

    @Test
    void eachChunkCarriesTheSubscriptionItsIdsWereReceivedOn() throws Exception {
        SubscriptionSplit other =
                new SubscriptionSplit(SubscriptionDestination.of("it-project", "other"), "1");
        try (PubSubSourceReader<String> reader = reader(ledger)) {
            reader.addSplits(List.of(SPLIT, other));
            emit("on-held");
            ackTracker.addPendingAck(other.splitId(), "on-other", new RecordingAckHandle("o"));
            ackTracker.stagePendingAck(other.splitId(), "on-other");

            List<SubscriptionSplit> state = reader.snapshotState(3L);

            assertThat(state).hasSize(2);
            for (SubscriptionSplit chunk : state) {
                String expected = chunk.getSubscription().equals(SUB) ? "on-held" : "on-other";
                assertThat(MessageIdLedgerCodec.decode(chunk.getLedgerChunk()))
                        .containsExactly(expected);
            }
            assertThat(state)
                    .extracting(SubscriptionSplit::getSubscription)
                    .containsExactlyInAnyOrder(SUB, other.getSubscription());
        }
    }

    @Test
    void chunksAreUniqueAcrossChunksOfOneSubscriptionAndAcrossSubtasks() throws Exception {
        List<String> firstSubtask;
        try (PubSubSourceReader<String> reader = reader(ledger)) {
            reader.setLedgerChunkBytes(16);
            reader.addSplits(List.of(SPLIT));
            for (int i = 0; i < 20; i++) {
                emit("message-" + i);
            }
            List<SubscriptionSplit> state = reader.snapshotState(5L);
            assertThat(state).hasSizeGreaterThan(1);
            firstSubtask = splitIds(state);
            assertThat(firstSubtask).doesNotHaveDuplicates();
        }

        // A second subtask restored from the same checkpoint with chunks of the same subscription;
        // the enumerator keeps the first chunk per split id, so equal ids would drop one.
        context.setIndexOfSubtask(1);
        PubSubAckTracker otherTracker = new PubSubAckTracker(testMetrics.metrics(), null);
        try (PubSubSourceReader<String> reader = reader(ledger, otherTracker)) {
            reader.setLedgerChunkBytes(16);
            reader.addSplits(List.of(SPLIT));
            for (int i = 0; i < 20; i++) {
                otherTracker.addPendingAck(
                        SPLIT.splitId(), "other-" + i, new RecordingAckHandle("other-" + i));
                otherTracker.stagePendingAck(SPLIT.splitId(), "other-" + i);
            }
            List<String> secondSubtask = splitIds(reader.snapshotState(5L));
            assertThat(secondSubtask).hasSizeGreaterThan(1);
            assertThat(secondSubtask).doesNotContainAnyElementsOf(firstSubtask);
        }
    }

    @Test
    void aRestoredLedgerIsNeverWrittenIntoALaterCheckpoint() throws Exception {
        try (PubSubSourceReader<String> reader = reader(ledger)) {
            reader.addSplits(List.of(restoredChunk()));
            reader.start();
            reader.handleSourceEvents(new LedgerChunkEvent(restoredChunk()));
            reader.addSplits(List.of(SPLIT));

            assertThat(reader.snapshotState(6L)).isEmpty();
        }
    }

    @Test
    void aCheckpointDropsAnExpiredLedgerEvenWithoutDeliveries() throws Exception {
        try (PubSubSourceReader<String> reader = reader(ledger)) {
            reader.handleSourceEvents(new LedgerChunkEvent(restoredChunk()));
            reader.addSplits(List.of(SPLIT));
            clock.addAndGet(Duration.ofMinutes(5).toNanos());

            reader.snapshotState(7L);

            assertThat(ledger.size()).isZero();
        }
    }

    @Test
    void theSplitFlinkRetainsNoLongerHoldsThePayloadOnceUploaded() throws Exception {
        // SourceOperator keeps every restored split object for the execution's lifetime.
        SubscriptionSplit retainedByFlink = restoredChunk();
        try (PubSubSourceReader<String> reader = reader(ledger)) {
            reader.addSplits(List.of(retainedByFlink));
            reader.start();

            assertThatThrownBy(retainedByFlink::getLedgerChunk)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("released");
            SubscriptionSplit uploaded = ((LedgerChunkEvent) context.sentEvents().get(0)).chunk();
            assertThat(MessageIdLedgerCodec.decode(uploaded.getLedgerChunk()))
                    .containsExactly("r1", "r2");
        }
    }

    @Test
    void aCheckpointWhoseMessagesAreAcknowledgedCarriesNothing() throws Exception {
        try (PubSubSourceReader<String> reader = reader(ledger)) {
            reader.addSplits(List.of(SPLIT));
            emit("m1");
            reader.snapshotState(1L);
            reader.notifyCheckpointComplete(1L);

            assertThat(reader.snapshotState(2L)).isEmpty();
        }
    }

    @Test
    void restoredChunksAreUploadedAtStartAndNeverReachTheSplitReader() throws Exception {
        try (PubSubSourceReader<String> reader = reader(ledger)) {
            // SourceOperator hands the restored state to addSplits before start().
            reader.addSplits(List.of(restoredChunk()));
            reader.start();

            assertThat(context.sentEvents()).hasSize(2);
            assertThat(((LedgerChunkEvent) context.sentEvents().get(0)).chunk())
                    .isEqualTo(restoredChunk());
            assertThat(context.sentEvents().get(1)).isInstanceOf(LedgerUploadedEvent.class);
            // The split reader sees changes in order, so once the consuming split's change has
            // arrived, a chunk passed on before it would have arrived too.
            reader.addSplits(List.of(SPLIT));
            assertThat(nextSplitChange().splits()).containsExactly(SPLIT);
            assertThat(splitChanges).isEmpty();
        }
    }

    @Test
    void aReaderWithNothingRestoredStillReportsItsUpload() throws Exception {
        try (PubSubSourceReader<String> reader = reader(ledger)) {
            reader.start();

            assertThat(context.sentEvents()).hasSize(1);
            assertThat(context.sentEvents().get(0)).isInstanceOf(LedgerUploadedEvent.class);
        }
    }

    @Test
    void theOutputOpenedForARestoredChunkIsReleasedOnTheFirstPoll() throws Exception {
        // SourceOperator opens a per-split output for every restored split; one that never emits
        // would hold the combined watermark back for good.
        RecordingReaderOutput output = new RecordingReaderOutput();
        try (PubSubSourceReader<String> reader = reader(ledger)) {
            reader.addSplits(List.of(restoredChunk()));
            reader.start();

            reader.pollNext(output);

            assertThat(output.released).containsExactly(restoredChunk().splitId());
        }
    }

    @Test
    void aForwardedChunkFillsTheLedger() throws Exception {
        try (PubSubSourceReader<String> reader = reader(ledger)) {
            reader.handleSourceEvents(new LedgerChunkEvent(restoredChunk()));

            assertThat(ledger.suppresses(SUB, "r1")).isTrue();
            assertThat(ledger.suppresses(SUB, "r2")).isTrue();
            assertThat(ledger.suppresses(SUB, "other")).isFalse();
        }
    }

    @Test
    void anUnreadableChunkIsIgnoredRatherThanFailingTheRestore() throws Exception {
        try (PubSubSourceReader<String> reader = reader(ledger)) {
            reader.handleSourceEvents(
                    new LedgerChunkEvent(
                            SubscriptionSplit.ledgerChunk(SUB, "4-0-1", new byte[] {42})));

            assertThat(ledger.size()).isZero();
        }
    }

    @Test
    void theClockStartsWhenTheFirstConsumingSplitArrives() throws Exception {
        try (PubSubSourceReader<String> reader = reader(ledger)) {
            // Restored chunks arrive through addSplits too, but consume nothing, so they leave
            // the clock stopped while the enumerator collects the ledger.
            reader.addSplits(List.of(restoredChunk()));
            reader.handleSourceEvents(new LedgerChunkEvent(restoredChunk()));
            clock.addAndGet(Duration.ofHours(1).toNanos());
            assertThat(ledger.suppresses(SUB, "r1")).isTrue();

            reader.addSplits(List.of(SPLIT));
            clock.addAndGet(Duration.ofMinutes(5).toNanos());

            assertThat(ledger.suppresses(SUB, "r2")).isFalse();
        }
    }

    @Test
    void withDeduplicationOffNothingIsCheckpointedOrSentAndRestoredChunksAreDropped()
            throws Exception {
        RecordingReaderOutput output = new RecordingReaderOutput();
        try (PubSubSourceReader<String> reader = reader(null)) {
            reader.addSplits(List.of(SPLIT, restoredChunk()));
            emit("m1");
            reader.start();
            reader.pollNext(output);

            assertThat(reader.snapshotState(1L)).isEmpty();
            assertThat(context.sentEvents()).isEmpty();
            assertThat(output.released).containsExactly(restoredChunk().splitId());
            assertThat(nextSplitChange().splits()).containsExactly(SPLIT);
        }
    }

    /**
     * A fresh restored chunk per use: the reader takes the payload out of the split it is handed,
     * as it must for the instance Flink retains, so a shared constant would be emptied.
     */
    private static SubscriptionSplit restoredChunk() {
        return SubscriptionSplit.ledgerChunk(
                SUB, "4-0-0", MessageIdLedgerCodec.encode(List.of("r1", "r2"), 1 << 20).get(0));
    }

    private SplitsChange<SubscriptionSplit> nextSplitChange() throws InterruptedException {
        SplitsChange<SubscriptionSplit> change = splitChanges.poll(30, TimeUnit.SECONDS);
        assertThat(change).as("a split change within 30 s").isNotNull();
        return change;
    }

    private static List<String> splitIds(List<SubscriptionSplit> splits) {
        List<String> ids = new ArrayList<>();
        for (SubscriptionSplit split : splits) {
            ids.add(split.splitId());
        }
        return ids;
    }

    private void emit(String messageId) {
        ackTracker.addPendingAck(SPLIT.splitId(), messageId, new RecordingAckHandle(messageId));
        ackTracker.stagePendingAck(SPLIT.splitId(), messageId);
    }

    private PubSubSourceReader<String> reader(@Nullable RestoredLedger restoredLedger) {
        return reader(restoredLedger, ackTracker);
    }

    private PubSubSourceReader<String> reader(
            @Nullable RestoredLedger restoredLedger, PubSubAckTracker ackTracker) {
        return new PubSubSourceReader<>(
                RecordingSplitReader::new,
                new PubSubRecordEmitter<>(
                        PubSubDeserializationSchema.payload(new SimpleStringSchema()),
                        ackTracker,
                        DeserializationFailurePolicy.FAIL,
                        testMetrics.metrics(),
                        restoredLedger),
                new Configuration(),
                context,
                ackTracker,
                new MissingCheckpointDetector(Duration.ZERO, () -> 0),
                restoredLedger);
    }

    /** Records split changes and fetches nothing. */
    private final class RecordingSplitReader
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
        public void handleSplitsChanges(SplitsChange<SubscriptionSplit> splitsChange) {
            splitChanges.add(splitsChange);
        }

        @Override
        public void wakeUp() {}

        @Override
        public void close() {}
    }

    /** Records which split outputs the reader released. */
    private static final class RecordingReaderOutput implements ReaderOutput<String> {

        private final List<String> released = new ArrayList<>();

        @Override
        public void collect(String record) {}

        @Override
        public void collect(String record, long timestamp) {}

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public SourceOutput<String> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {
            released.add(splitId);
        }
    }
}
