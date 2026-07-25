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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.AbstractPubSubSourceEmulatorITCase;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests driving {@link PubSubSplitReader} and the production {@link
 * DefaultSubscriberFactory} against the Pub/Sub emulator, covering the acknowledgement lifecycle
 * end to end.
 */
class PubSubSplitReaderITCase extends AbstractPubSubSourceEmulatorITCase {

    /** Long enough that redelivery in these tests can only come from an explicit nack. */
    private static final int ACK_DEADLINE_SECONDS = 60;

    private static final int MAX_RECORDS_PER_FETCH = 100;

    @Test
    void acknowledgedMessagesAreNotRedelivered() throws Exception {
        SubscriptionDestination subscription =
                createTopicAndSubscription("reader-ack", ACK_DEADLINE_SECONDS);
        publish("reader-ack", "m1", "m2", "m3");
        SubscriptionSplit split = new SubscriptionSplit(subscription, "0");
        PubSubAckTracker ackTracker = new PubSubAckTracker();

        try (PubSubSplitReader reader = reader(ackTracker)) {
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
            List<PubsubMessage> received = fetchUntil(reader, 3, Duration.ofSeconds(30));

            assertThat(payloads(received)).containsExactlyInAnyOrder("m1", "m2", "m3");

            // Emit, snapshot and complete the checkpoint, exactly as the reader does.
            received.forEach(
                    message -> ackTracker.stagePendingAck(split.splitId(), message.getMessageId()));
            ackTracker.addCheckpoint(1L);
            ackTracker.notifyCheckpointComplete(1L);
        }

        // A fresh subscriber must see nothing: the messages were acknowledged, and closing the
        // first reader had nothing left to nack.
        assertThat(payloads(receiveWithFreshReader(split, Duration.ofSeconds(5)))).isEmpty();
    }

    @Test
    void closingTheReaderNacksUnacknowledgedMessagesSoTheyComeBackAtOnce() throws Exception {
        SubscriptionDestination subscription =
                createTopicAndSubscription("reader-nack", ACK_DEADLINE_SECONDS);
        publish("reader-nack", "m1", "m2");
        SubscriptionSplit split = new SubscriptionSplit(subscription, "0");
        PubSubAckTracker ackTracker = new PubSubAckTracker();

        try (PubSubSplitReader reader = reader(ackTracker)) {
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
            List<PubsubMessage> received = fetchUntil(reader, 2, Duration.ofSeconds(30));
            assertThat(payloads(received)).containsExactlyInAnyOrder("m1", "m2");
            // Emitted but never covered by a completed checkpoint, so they must not be lost.
            received.forEach(
                    message -> ackTracker.stagePendingAck(split.splitId(), message.getMessageId()));
            ackTracker.addCheckpoint(1L);
        }

        // Without the nack these would only reappear after the 60 s acknowledgement deadline.
        assertThat(payloads(receiveWithFreshReader(split, Duration.ofSeconds(30))))
                .containsExactlyInAnyOrder("m1", "m2");
    }

    @Test
    void oneReaderConsumesSeveralSubscriptionsAtOnce() throws Exception {
        SubscriptionDestination first =
                createTopicAndSubscription("reader-multi-a", ACK_DEADLINE_SECONDS);
        SubscriptionDestination second =
                createTopicAndSubscription("reader-multi-b", ACK_DEADLINE_SECONDS);
        publish("reader-multi-a", "a1", "a2");
        publish("reader-multi-b", "b1");

        try (PubSubSplitReader reader = reader(new PubSubAckTracker())) {
            reader.handleSplitsChanges(
                    new SplitsAddition<>(
                            List.of(
                                    new SubscriptionSplit(first, "0"),
                                    new SubscriptionSplit(second, "1"))));

            assertThat(payloads(fetchUntil(reader, 3, Duration.ofSeconds(30))))
                    .containsExactlyInAnyOrder("a1", "a2", "b1");
        }
    }

    private static PubSubSplitReader reader(PubSubAckTracker ackTracker) {
        return new PubSubSplitReader(
                new DefaultSubscriberFactory(OrderingMode.NONE, emulatorEndpoint()),
                ackTracker,
                MAX_RECORDS_PER_FETCH);
    }

    private static List<PubsubMessage> receiveWithFreshReader(
            SubscriptionSplit split, Duration timeout) throws Exception {
        try (PubSubSplitReader reader = reader(new PubSubAckTracker())) {
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
            return fetchUntil(reader, Integer.MAX_VALUE, timeout);
        }
    }

    /**
     * Fetches until {@code expected} messages have been collected or the timeout elapses. A fetch
     * blocks until data arrives, so a waker nudges the reader once the deadline passes.
     */
    private static List<PubsubMessage> fetchUntil(
            PubSubSplitReader reader, int expected, Duration timeout) throws Exception {
        List<PubsubMessage> received = new ArrayList<>();
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        ScheduledExecutorService waker = Executors.newSingleThreadScheduledExecutor();
        try {
            // Nudge the reader periodically so a fetch that has nothing to return cannot outlive
            // the deadline check.
            waker.scheduleAtFixedRate(reader::wakeUp, 200, 200, TimeUnit.MILLISECONDS);
            while (received.size() < expected && System.nanoTime() < deadlineNanos) {
                drain(reader.fetch(), received);
            }
        } finally {
            waker.shutdownNow();
        }
        return received;
    }

    private static void drain(
            RecordsWithSplitIds<PubsubMessage> records, List<PubsubMessage> into) {
        while (records.nextSplit() != null) {
            PubsubMessage message;
            while ((message = records.nextRecordFromSplit()) != null) {
                into.add(message);
            }
        }
    }

    private static List<String> payloads(List<PubsubMessage> messages) {
        return messages.stream()
                .map(message -> message.getData().toString(StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }
}
