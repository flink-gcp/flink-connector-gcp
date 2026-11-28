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
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.DeserializationFailurePolicy;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import io.github.flink.gcp.connector.testutils.CollectingSourceOutput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds that a restore-deduplication match is a suppression, not necessarily an acknowledgement
 * (#2390), which the {@code redeliveriesSuppressed} row of the DataStream page states.
 *
 * <p>The split reader hands a fetched batch to Flink's element queue, and the task thread emits it
 * later. A paused split can be parked in between, which nacks every delivery the tracker holds for
 * it. When the record emitter then reaches a queued message the restored ledger covers, the match
 * is counted and the record is not emitted, but no delivery is left to acknowledge. The real split
 * reader, subscriber and tracker run here; only Flink's queue is stood in for, by holding the first
 * fetch's batch and emitting from it after the park.
 */
@Timeout(30)
class PubSubParkedSplitLedgerMatchTest {

    private static final SubscriptionSplit PAUSED =
            new SubscriptionSplit(SubscriptionDestination.of("project", "sub-a"), "0");
    private static final SubscriptionSplit ACTIVE =
            new SubscriptionSplit(SubscriptionDestination.of("project", "sub-b"), "1");

    private final TestReaderMetrics readerMetrics = new TestReaderMetrics();
    private final PubSubAckTracker ackTracker = new PubSubAckTracker(readerMetrics.metrics(), null);
    private final Map<String, SubscriberFactory.MessageConsumer> receivers = new HashMap<>();

    @Test
    void aQueuedRecordOfAParkedSplitIsCountedAsSuppressedButNotAcknowledged() throws Exception {
        PubSubSplitReader reader =
                new PubSubSplitReader(
                        (split, signal) ->
                                new StreamingPullSubscriber(
                                        split.splitId(),
                                        split.getSubscription(),
                                        (subscription, receiver) -> {
                                            receivers.put(split.splitId(), receiver);
                                            return new InertClient();
                                        },
                                        ackTracker,
                                        signal,
                                        Duration.ofSeconds(1),
                                        SubscriberBufferBudget.unbounded()),
                        10,
                        new MissingCheckpointDetector(Duration.ZERO, () -> 0),
                        PausedSplitBufferLimits.of(
                                PubSubSubscriberOptions.builder()
                                        .pausedSplitBufferMaxMessages(1)
                                        .pausedSplitBufferMaxBytes(Long.MAX_VALUE)
                                        .build()),
                        readerMetrics.metrics());
        try {
            reader.handleSplitsChanges(new SplitsAddition<>(List.of(PAUSED, ACTIVE)));

            RecordingAckHandle covered = new RecordingAckHandle("covered");
            receivers.get(PAUSED.splitId()).receive(message("covered"), covered);
            // Handed to Flink's element queue; the task thread has not emitted it yet.
            RecordsWithSplitIds<PubsubMessage> queued = reader.fetch();

            reader.pauseOrResumeSplits(List.of(PAUSED), List.of());
            receivers.get(PAUSED.splitId()).receive(message("a"), new RecordingAckHandle("a"));
            receivers.get(PAUSED.splitId()).receive(message("b"), new RecordingAckHandle("b"));
            // A delivery on the active split ends the fetch's wait; a paused split delivers none.
            receivers.get(ACTIVE.splitId()).receive(message("x"), new RecordingAckHandle("x"));
            // Settled by nothing yet: the pause alone must not nack it.
            assertThat(covered.isUnsettled()).as("the queued delivery before the park").isTrue();
            reader.fetch();

            assertThat(readerMetrics.gauge("parkedSplits")).as("parkedSplits").isEqualTo(1);
            assertThat(covered.nackCount()).as("the park nacks the queued delivery").isEqualTo(1);

            // The task thread now emits the queued batch, with a restored ledger covering the
            // message.
            RestoredLedger ledger =
                    new RestoredLedger(Duration.ofMinutes(5), () -> 0L, readerMetrics.metrics());
            readerMetrics.metrics().bindRestoredLedger(ledger);
            ledger.add(PAUSED.getSubscription(), List.of("covered"));
            PubSubRecordEmitter<String> emitter =
                    new PubSubRecordEmitter<>(
                            PubSubDeserializationSchema.payload(new SimpleStringSchema()),
                            ackTracker,
                            DeserializationFailurePolicy.FAIL,
                            readerMetrics.metrics(),
                            ledger);
            CollectingSourceOutput<String> output = new CollectingSourceOutput<>();
            assertThat(queued.nextSplit()).isEqualTo(PAUSED.splitId());
            emitter.emitRecord(queued.nextRecordFromSplit(), output, PAUSED);

            assertThat(output.records()).as("emitted records").isEmpty();
            assertThat(readerMetrics.counter("redeliveriesSuppressed"))
                    .as("redeliveriesSuppressed")
                    .isEqualTo(1);
            assertThat(readerMetrics.counter("messagesAcked")).as("messagesAcked").isZero();
            assertThat(covered.ackCount()).as("acknowledgements of the queued delivery").isZero();
        } finally {
            reader.close();
        }
    }

    private static PubsubMessage message(String id) {
        return PubsubMessage.newBuilder()
                .setMessageId(id)
                .setData(ByteString.copyFrom(id, StandardCharsets.UTF_8))
                .build();
    }

    private static final class InertClient implements SubscriberClient {
        @Override
        public void start(Consumer<Throwable> onPermanentFailure) {}

        @Override
        public void stopAsync() {}

        @Override
        public void awaitTerminated(long timeout, TimeUnit unit) {}

        @Override
        public void close() {}
    }
}
