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

import com.google.api.core.ApiService;
import com.google.pubsub.v1.DeadLetterPolicy;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.AbstractPubSubRealGcpITCase;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.BackpressuredArm.ACK_DEADLINE_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the hard subscriber-buffer response against real Cloud Pub/Sub (#1138).
 *
 * <p>The subscription enables ordering and a five-attempt dead-letter policy. Four messages share
 * one ordering key, while the connector admits one message and rejects the next delivery
 * non-blockingly. The rejection NACKs that delivery and asynchronously stops the subscriber. The
 * test records every callback by message id before the connector handles it, so it can prove no one
 * message loops through the five-attempt dead-letter budget. Teardown NACKs the one retained
 * delivery. A fresh pull must then receive and acknowledge the complete sequence in order, while
 * the dead-letter observer stays empty.
 *
 * <p>That outcome covers the two service behaviors the emulator cannot provide. Rejected and
 * retained deliveries are redelivered, but the response does not loop through the dead-letter
 * attempt budget, and the ordered sequence remains intact across the stop. Message volume and the
 * response window are deliberately small because this is a gated, billable test.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "PUBSUB_IT_PROJECT", matches = ".+")
class PubSubBackpressuredSplitBufferRealGcpITCase extends AbstractPubSubRealGcpITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(PubSubBackpressuredSplitBufferRealGcpITCase.class);

    private static final int MAX_DELIVERY_ATTEMPTS = 5;
    private static final Duration RESPONSE_WINDOW = Duration.ofSeconds(15);
    private static final Duration DEAD_LETTER_OBSERVATION = Duration.ofSeconds(20);
    private static final String ORDERING_KEY = "bounded";
    private static final String[] SEQUENCE = {"0", "1", "2", "3"};

    @Test
    void aLimitStopPreservesOrderingWithoutExhaustingDeadLetterAttempts() throws Exception {
        TopicDestination topic = createTopic("bounded-source");
        TopicDestination deadLetterTopic = createTopic("bounded-dead-letter");
        SubscriptionDestination deadLetterObserver =
                createSubscription(deadLetterTopic, "bounded-dead-letter-observer");
        SubscriptionDestination subscription =
                createSubscription(
                        topic,
                        "bounded-source",
                        builder ->
                                builder.setEnableMessageOrdering(true)
                                        .setAckDeadlineSeconds(ACK_DEADLINE_SECONDS)
                                        .setDeadLetterPolicy(
                                                DeadLetterPolicy.newBuilder()
                                                        .setDeadLetterTopic(
                                                                deadLetterTopic.toTopicPath())
                                                        .setMaxDeliveryAttempts(
                                                                MAX_DELIVERY_ATTEMPTS)
                                                        .build()));
        publishOrdered(topic, ORDERING_KEY, SEQUENCE);

        BackpressuredArm arm =
                new BackpressuredArm(
                        "bounded-real",
                        0,
                        subscription,
                        null,
                        1,
                        Long.MAX_VALUE,
                        OrderingMode.PER_KEY);
        BackpressuredArm.runFor(RESPONSE_WINDOW, List.of(arm));

        LOG.info("Real Pub/Sub hard-limit response (#1138): {}", arm);
        assertThat(arm.limitExceeded()).isNotNull();
        assertThat(arm.limitExceeded().attemptedMessages()).isEqualTo(2);
        assertThat(arm.buffered()).isEqualTo(1);
        assertThat(arm.callbackDeliveries())
                .as("the service delivered beyond the one admitted message")
                .isGreaterThan(1);
        assertThat(arm.maximumCallbackDeliveriesForOneMessage())
                .as("the limit response stopped before one message exhausted its attempts")
                .isLessThan(MAX_DELIVERY_ATTEMPTS);
        assertThat(arm.clientStateBeforeClose())
                .as("the limit response stopped the SDK client before harness teardown")
                .isIn(ApiService.State.STOPPING, ApiService.State.TERMINATED);

        List<PubsubMessage> redelivered =
                pullMessagesUntil(subscription, SEQUENCE.length, COLLECT_TIMEOUT);
        assertThat(
                        redelivered.stream()
                                .map(message -> message.getData().toStringUtf8())
                                .collect(Collectors.toList()))
                .containsExactly(SEQUENCE);
        assertThat(redelivered)
                .allSatisfy(
                        message -> assertThat(message.getOrderingKey()).isEqualTo(ORDERING_KEY));

        assertThat(pullMessagesUntil(deadLetterObserver, 1, DEAD_LETTER_OBSERVATION))
                .as("one limit response did not exhaust the dead-letter attempt budget")
                .isEmpty();
    }
}
