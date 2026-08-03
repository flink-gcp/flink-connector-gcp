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

import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.source.AbstractPubSubSourceEmulatorITCase;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static io.github.flink.gcp.connector.testutils.pubsub.PubSubSplitReaders.fetchUntil;
import static io.github.flink.gcp.connector.testutils.pubsub.PubSubSplitReaders.payloads;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests driving {@link PubSubSplitReader} and the production {@link
 * DefaultSubscriberFactory} against the Pub/Sub emulator, covering the acknowledgement lifecycle
 * end to end.
 */
class PubSubSplitReaderITCase extends AbstractPubSubSourceEmulatorITCase {

    /**
     * Long enough that a first delivery in these tests can only come from the initial publish. It
     * is also the fallback bound {@link #REDELIVERY_TIMEOUT} must cover: an unacknowledged message
     * whose nack never took effect is redelivered when this deadline expires.
     */
    private static final int ACK_DEADLINE_SECONDS = 60;

    /**
     * Bound on waiting for redelivery after a close, deliberately longer than {@link
     * #ACK_DEADLINE_SECONDS}. The claim under test is that closing the reader loses nothing, not
     * that the emulator redelivers a nacked message promptly: a nack is a fire-and-forget {@code
     * modifyAckDeadline(0)}, so its timing — even whether it was applied at all — is the service's
     * property, not the connector's, and the emulator does not specify it (issue #118; asserting
     * promptness moved to the real-GCP suite, issue #82). Under this bound a dropped or delayed
     * nack degrades to a pass slow enough to stand out in the test timing, while an actual loss — a
     * close that acknowledged instead of nacking, say — still fails.
     */
    private static final Duration REDELIVERY_TIMEOUT = Duration.ofSeconds(90);

    private static final int MAX_RECORDS_PER_FETCH = 100;

    @Test
    void acknowledgedMessagesAreNotRedelivered() throws Exception {
        SubscriptionDestination subscription =
                createTopicAndSubscription("reader-ack", ACK_DEADLINE_SECONDS);
        publish("reader-ack", "m1", "m2", "m3");
        SubscriptionSplit split = new SubscriptionSplit(subscription, "0");
        PubSubAckTracker ackTracker = newTracker();

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
        List<PubsubMessage> redelivered =
                receiveWithFreshReader(split, Integer.MAX_VALUE, Duration.ofSeconds(5));
        assertThat(payloads(redelivered)).isEmpty();
    }

    @Test
    void closingTheReaderNacksUnacknowledgedMessagesSoTheyAreRedelivered() throws Exception {
        SubscriptionDestination subscription =
                createTopicAndSubscription("reader-nack", ACK_DEADLINE_SECONDS);
        publish("reader-nack", "m1", "m2");
        SubscriptionSplit split = new SubscriptionSplit(subscription, "0");
        TestReaderMetrics readerMetrics = new TestReaderMetrics();
        PubSubAckTracker ackTracker = new PubSubAckTracker(readerMetrics.metrics(), null);

        try (PubSubSplitReader reader = reader(ackTracker)) {
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
            List<PubsubMessage> received = fetchUntil(reader, 2, Duration.ofSeconds(30));
            assertThat(payloads(received)).containsExactlyInAnyOrder("m1", "m2");
            // Emitted but never covered by a completed checkpoint, so they must not be lost.
            received.forEach(
                    message -> ackTracker.stagePendingAck(split.splitId(), message.getMessageId()));
            ackTracker.addCheckpoint(1L);
        }

        // The connector's half of the contract, asserted deterministically: closing issued a nack
        // for both messages. This is what tells a working close from one silently relying on the
        // acknowledgement-deadline fallback the redelivery window below also covers.
        assertThat(readerMetrics.counter("messagesNacked")).isEqualTo(2);

        // The service's half, reduced to what the emulator can honestly promise: the messages come
        // back rather than being lost.
        assertThat(payloads(receiveWithFreshReader(split, 2, REDELIVERY_TIMEOUT)))
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

        try (PubSubSplitReader reader = reader(newTracker())) {
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
        PubSubSubscriberOptions options =
                PubSubSubscriberOptions.builder().maxRecordsPerFetch(MAX_RECORDS_PER_FETCH).build();
        return new PubSubSplitReader(
                new DefaultSubscriberFactory(
                        options, OrderingMode.NONE, EmulatorEndpoint.parse(emulatorEndpoint())),
                ackTracker,
                options,
                new MissingCheckpointDetector(Duration.ZERO, ackTracker::outstandingAckCount));
    }

    /**
     * Receives with a fresh reader until {@code expected} distinct messages have arrived or the
     * timeout elapses, so a wait for messages known to be coming ends when they do, while an
     * emptiness check ({@code expected = Integer.MAX_VALUE}) holds its full window.
     */
    private static List<PubsubMessage> receiveWithFreshReader(
            SubscriptionSplit split, int expected, Duration timeout) throws Exception {
        try (PubSubSplitReader reader = reader(newTracker())) {
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
            return fetchUntil(reader, expected, timeout);
        }
    }

    private static PubSubAckTracker newTracker() {
        return new PubSubAckTracker(new TestReaderMetrics().metrics(), null);
    }
}
