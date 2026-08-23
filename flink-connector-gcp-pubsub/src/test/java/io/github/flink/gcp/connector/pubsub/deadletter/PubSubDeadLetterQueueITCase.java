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

package io.github.flink.gcp.connector.pubsub.deadletter;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.github.flink.gcp.connector.testutils.pubsub.PubSubEmulatorContainers;
import io.github.flink.gcp.connector.testutils.pubsub.PubSubTestClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round trip for {@link PubSubDeadLetterQueue} against the Pub/Sub emulator: what a handler offers
 * comes back out of a subscription with its payload and its five attributes.
 *
 * <p>Emulator evidence is enough for this one, unusually: nothing here depends on a behaviour the
 * emulator is known to fake — it is a plain publish and pull, with no create-option knob, no
 * ordering and no dead-letter policy involved.
 */
@Testcontainers
@Timeout(180)
class PubSubDeadLetterQueueITCase {

    private static final String PROJECT = "it-project";

    private static final Duration PULL_DEADLINE = Duration.ofSeconds(30);

    @Container
    private static final PubSubEmulatorContainer EMULATOR = PubSubEmulatorContainers.newContainer();

    private static PubSubTestClients clients;

    private static final AtomicInteger NAMES = new AtomicInteger();

    @BeforeAll
    static void createClients() throws IOException {
        clients = PubSubTestClients.forEmulator(EMULATOR.getEmulatorEndpoint());
    }

    @AfterAll
    static void closeClients() {
        if (clients != null) {
            clients.close();
        }
    }

    /** A failed element standing in for another connector's — the case this queue exists for. */
    private static final class StubElement implements FailedElement {

        @Nullable private final ByteString payload;

        private StubElement(@Nullable ByteString payload) {
            this.payload = payload;
        }

        @Override
        public String getConnector() {
            return "bigquery";
        }

        @Override
        public String describeDestination() {
            return "it-project.it_dataset.orders";
        }

        @Override
        @Nullable
        public ByteString getPayloadBytes() {
            return payload;
        }

        @Override
        public String getErrorMessage() {
            return "The row was rejected.";
        }

        @Override
        @Nullable
        public Throwable getCause() {
            return new IllegalStateException("not carried in the envelope");
        }
    }

    @Test
    void offersFlushesAndTheMessagesComeBackOut() throws Exception {
        try (Fixture fixture = newFixture()) {
            Instant before = Instant.now();
            fixture.queue.open(DefaultFailureHandlerContext.of(new StubWriterInitContext(3)));
            fixture.queue.offer(new StubElement(ByteString.copyFromUtf8("row one")));
            fixture.queue.offer(new StubElement(ByteString.copyFromUtf8("row two")));
            assertThat(fixture.queue.getInFlightMessages()).isEqualTo(2);
            fixture.queue.flush();
            // Awaited and released: a flush that left them behind would await the same futures
            // again at the next checkpoint, and grow without bound.
            assertThat(fixture.queue.getInFlightMessages()).isZero();

            List<PubsubMessage> pulled =
                    clients.pullMessagesUntil(fixture.subscriptionPath, 2, PULL_DEADLINE);

            assertThat(pulled).hasSize(2);
            assertThat(pulled)
                    .extracting(message -> message.getData().toStringUtf8())
                    .containsExactlyInAnyOrder("row one", "row two");
            PubsubMessage message = pulled.get(0);
            assertThat(message.getAttributesMap())
                    .containsEntry("dlq-connector", "bigquery")
                    .containsEntry("dlq-destination", "it-project.it_dataset.orders")
                    .containsEntry("dlq-error", "The row was rejected.")
                    .containsEntry("dlq-subtask", "3");
            assertThat(
                            Instant.from(
                                    DateTimeFormatter.ISO_INSTANT.parse(
                                            message.getAttributesMap().get("dlq-timestamp"))))
                    .isBetween(before, Instant.now());
        }
    }

    @Test
    void carriesAnElementWhoseSerializationFailedWithEmptyData() throws Exception {
        try (Fixture fixture = newFixture()) {
            fixture.queue.open(DefaultFailureHandlerContext.of(new StubWriterInitContext(0)));
            fixture.queue.offer(new StubElement(null));
            fixture.queue.flush();

            List<PubsubMessage> pulled =
                    clients.pullMessagesUntil(fixture.subscriptionPath, 1, PULL_DEADLINE);

            assertThat(pulled).hasSize(1);
            assertThat(pulled.get(0).getData()).isEqualTo(ByteString.EMPTY);
            assertThat(pulled.get(0).getAttributesMap()).containsEntry("dlq-connector", "bigquery");
        }
    }

    @Test
    void writeThroughPublishesWithoutWaitingForAFlush() throws Exception {
        try (Fixture fixture = newFixture(PubSubDeadLetterQueue.WRITE_THROUGH)) {
            StubWriterInitContext context = new StubWriterInitContext(0);
            fixture.queue.open(DefaultFailureHandlerContext.of(context));
            fixture.queue.offer(new StubElement(ByteString.copyFromUtf8("durable already")));

            // The point of the mode: offer returned only once the publish was acknowledged, so
            // nothing is in flight and the message is already in the topic — and the confirmed
            // count says so without a flush, which is what makes it a confirmation count.
            assertThat(fixture.queue.getInFlightMessages()).isZero();
            assertThat(context.getSinkWriterMetricGroup().counterValue("deadLettersPublished"))
                    .isEqualTo(1);
            assertThat(clients.pullMessagesUntil(fixture.subscriptionPath, 1, PULL_DEADLINE))
                    .hasSize(1);
        }
    }

    /**
     * The metrics against a live topic: the confirmed count moves when the service has answered,
     * which under the default bound is at the flush and not at the offers. The unit tests pin the
     * same rule against fake futures; this one pins it against a real client, where a mutant
     * counting at the hand-off would report two before the flush.
     */
    @Test
    void theConfirmedCountMovesAtTheFlushAndTheGaugeEmpties() throws Exception {
        try (Fixture fixture = newFixture()) {
            StubWriterInitContext context = new StubWriterInitContext(0);
            TestSinkWriterMetricGroup group = context.getSinkWriterMetricGroup();
            fixture.queue.open(DefaultFailureHandlerContext.of(context));

            fixture.queue.offer(new StubElement(ByteString.copyFromUtf8("row one")));
            fixture.queue.offer(new StubElement(ByteString.copyFromUtf8("row two")));

            assertThat(group.<Integer>gaugeValue("inFlightDeadLetters")).isEqualTo(2);
            assertThat(group.counterValue("deadLettersPublished")).isZero();

            fixture.queue.flush();

            assertThat(group.counterValue("deadLettersPublished")).isEqualTo(2);
            assertThat(group.<Integer>gaugeValue("inFlightDeadLetters")).isZero();
            assertThat(clients.pullMessagesUntil(fixture.subscriptionPath, 2, PULL_DEADLINE))
                    .hasSize(2);
        }
    }

    @Test
    void theBoundAwaitsWhileTheDefaultKeepsBuffering() throws Exception {
        try (Fixture bounded = newFixture(2)) {
            bounded.queue.open(DefaultFailureHandlerContext.of(new StubWriterInitContext(0)));
            bounded.queue.offer(new StubElement(ByteString.copyFromUtf8("one")));
            assertThat(bounded.queue.getInFlightMessages()).isEqualTo(1);

            // The second offer reaches the bound and awaits both, so the buffer is empty again.
            bounded.queue.offer(new StubElement(ByteString.copyFromUtf8("two")));
            assertThat(bounded.queue.getInFlightMessages()).isZero();

            bounded.queue.offer(new StubElement(ByteString.copyFromUtf8("three")));
            bounded.queue.flush();
            assertThat(clients.pullMessagesUntil(bounded.subscriptionPath, 3, PULL_DEADLINE))
                    .hasSize(3);
        }
    }

    @Test
    void unboundedBuffersEverythingUntilTheFlush() throws Exception {
        try (Fixture fixture = newFixture(PubSubDeadLetterQueue.UNBOUNDED)) {
            fixture.queue.open(DefaultFailureHandlerContext.of(new StubWriterInitContext(0)));
            for (int i = 0; i < 3; i++) {
                fixture.queue.offer(new StubElement(ByteString.copyFromUtf8("row " + i)));
            }

            // Nothing awaited before the flush, whatever the count: this is the one mode whose
            // memory this queue does not bound.
            assertThat(fixture.queue.getInFlightMessages()).isEqualTo(3);
            fixture.queue.flush();
            assertThat(clients.pullMessagesUntil(fixture.subscriptionPath, 3, PULL_DEADLINE))
                    .hasSize(3);
        }
    }

    @Test
    void servesAHandlerThroughSendToDeadLetterQueue() throws Exception {
        Fixture fixture = newFixture();
        FailureHandler<FailedElement> handler = FailureHandler.sendToDeadLetterQueue(fixture.queue);

        // The lifecycle a sink writer drives, through the built-in handler rather than directly.
        handler.open(DefaultFailureHandlerContext.of(new StubWriterInitContext(1)));
        try {
            handler.handle(new StubElement(ByteString.copyFromUtf8("through the handler")));
            handler.flush();
        } finally {
            handler.close();
        }

        assertThat(clients.pullMessagesUntil(fixture.subscriptionPath, 1, PULL_DEADLINE))
                .singleElement()
                .satisfies(
                        message ->
                                assertThat(message.getData().toStringUtf8())
                                        .isEqualTo("through the handler"));
    }

    private Fixture newFixture() {
        return newFixture(PubSubDeadLetterQueue.DEFAULT_MAX_IN_FLIGHT_MESSAGES);
    }

    /** Creates a topic and a subscription of its own, plus a queue publishing to that topic. */
    private Fixture newFixture(int maxInFlightMessages) {
        String name = "dlq-" + NAMES.incrementAndGet();
        TopicDestination topic = TopicDestination.of(PROJECT, name);
        clients.topicAdmin().createTopic(TopicName.of(PROJECT, name));
        String subscriptionPath = SubscriptionName.of(PROJECT, name).toString();
        clients.subscriptionAdmin()
                .createSubscription(
                        SubscriptionName.of(PROJECT, name),
                        TopicName.of(PROJECT, name),
                        PushConfig.getDefaultInstance(),
                        10);
        return new Fixture(
                PubSubDeadLetterQueue.builder()
                        .topic(topic)
                        .emulatorEndpoint(EMULATOR.getEmulatorEndpoint())
                        .maxInFlightMessages(maxInFlightMessages)
                        .build(),
                subscriptionPath);
    }

    /** A queue and the subscription its topic feeds; closes the queue at the end of a test. */
    private static final class Fixture implements AutoCloseable {
        private final PubSubDeadLetterQueue queue;
        private final String subscriptionPath;

        private Fixture(PubSubDeadLetterQueue queue, String subscriptionPath) {
            this.queue = queue;
            this.subscriptionPath = subscriptionPath;
        }

        @Override
        public void close() throws Exception {
            queue.close();
        }
    }
}
