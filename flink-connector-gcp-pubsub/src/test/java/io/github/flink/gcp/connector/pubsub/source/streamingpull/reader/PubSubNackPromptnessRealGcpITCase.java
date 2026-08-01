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
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.AbstractPubSubRealGcpITCase;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The promptness half of the nack-on-close claim, against real Cloud Pub/Sub: after a clean reader
 * close with unacknowledged messages, redelivery arrives well before the acknowledgement deadline.
 *
 * <p>This is the assertion the emulator IT used to make and gave up in issue #118: a nack is a
 * fire-and-forget {@code modifyAckDeadline(0)}, so how fast it takes effect is the service's
 * property, and the emulator does not specify it. Real Pub/Sub documents nack-triggered redelivery
 * without a timing guarantee either, so the bound here is an <em>observed-behaviour</em> bound,
 * chosen generously — half the acknowledgement deadline, against a round trip that completes in
 * about a second when healthy — not a contract. If it ever flakes, the reading is "the service got
 * slower", not "the connector broke": the connector's own half (the nack was issued) is asserted
 * deterministically through the metric first.
 */
@EnabledIfEnvironmentVariable(named = "PUBSUB_IT_PROJECT", matches = ".+")
class PubSubNackPromptnessRealGcpITCase extends AbstractPubSubRealGcpITCase {

    private static final int ACK_DEADLINE_SECONDS = 60;
    private static final Duration PROMPT_REDELIVERY_BOUND = Duration.ofSeconds(30);
    private static final int MAX_RECORDS_PER_FETCH = 100;

    @Test
    void closingTheReaderNacksUnacknowledgedMessagesSoTheyComeBackPromptly() throws Exception {
        TopicDestination topic = createTopic("nack-prompt");
        SubscriptionDestination subscription =
                createSubscription(
                        topic,
                        "nack-prompt",
                        builder -> builder.setAckDeadlineSeconds(ACK_DEADLINE_SECONDS));
        publish(topic, "m1", "m2");
        SubscriptionSplit split = new SubscriptionSplit(subscription, "0");
        TestReaderMetrics readerMetrics = new TestReaderMetrics();
        PubSubAckTracker ackTracker = new PubSubAckTracker(readerMetrics.metrics(), null);

        try (PubSubSplitReader reader = reader(ackTracker)) {
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
            List<PubsubMessage> received = fetchUntil(reader, 2, Duration.ofSeconds(60));
            assertThat(payloads(received)).containsExactlyInAnyOrder("m1", "m2");
            // Emitted but never covered by a completed checkpoint, so close must nack them.
            received.forEach(
                    message -> ackTracker.stagePendingAck(split.splitId(), message.getMessageId()));
            ackTracker.addCheckpoint(1L);
        }

        // The connector's half, deterministic: the close issued the nacks.
        assertThat(readerMetrics.counter("messagesNacked")).isEqualTo(2);

        // The service's half: redelivery lands well inside the acknowledgement deadline, which is
        // what distinguishes an applied nack from the deadline fallback the emulator IT settles
        // for.
        try (PubSubSplitReader reader = reader(newTracker())) {
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
            assertThat(payloads(fetchUntil(reader, 2, PROMPT_REDELIVERY_BOUND)))
                    .containsExactlyInAnyOrder("m1", "m2");
        }
    }

    private static PubSubSplitReader reader(PubSubAckTracker ackTracker) {
        PubSubSubscriberOptions options =
                PubSubSubscriberOptions.builder().maxRecordsPerFetch(MAX_RECORDS_PER_FETCH).build();
        // A null endpoint is the production path: application-default credentials, real service.
        return new PubSubSplitReader(
                new DefaultSubscriberFactory(options, OrderingMode.NONE, null),
                ackTracker,
                options,
                new MissingCheckpointDetector(Duration.ZERO, ackTracker::outstandingAckCount));
    }

    /**
     * Fetches until {@code expected} distinct messages arrive or the timeout elapses — the shape of
     * {@code PubSubSplitReaderITCase.fetchUntil}, duplicated because that helper is private to its
     * class (folding the harnesses together is issue #27).
     */
    private static List<PubsubMessage> fetchUntil(
            PubSubSplitReader reader, int expected, Duration timeout) throws Exception {
        Map<String, PubsubMessage> received = new LinkedHashMap<>();
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        ScheduledExecutorService waker = Executors.newSingleThreadScheduledExecutor();
        try {
            waker.scheduleAtFixedRate(reader::wakeUp, 200, 200, TimeUnit.MILLISECONDS);
            while (received.size() < expected && System.nanoTime() < deadlineNanos) {
                RecordsWithSplitIds<PubsubMessage> records = reader.fetch();
                String splitId;
                while ((splitId = records.nextSplit()) != null) {
                    PubsubMessage message;
                    while ((message = records.nextRecordFromSplit()) != null) {
                        received.putIfAbsent(splitId + "/" + message.getMessageId(), message);
                    }
                }
            }
        } finally {
            waker.shutdownNow();
        }
        return new ArrayList<>(received.values());
    }

    private static List<String> payloads(List<PubsubMessage> messages) {
        return messages.stream()
                .map(message -> message.getData().toString(StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }

    private static PubSubAckTracker newTracker() {
        return new PubSubAckTracker(new TestReaderMetrics().metrics(), null);
    }
}
