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

import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.AbstractPubSubRealGcpITCase;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import static io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.BackpressuredArm.ACK_DEADLINE_SECONDS;
import static io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.BackpressuredArm.BACKLOG;
import static io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.BackpressuredArm.FLOW_CONTROL_MESSAGES;
import static io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.BackpressuredArm.REFILL_RATE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The one part of the #377 measurement the emulator cannot take: what redelivery does to a
 * backpressured split's buffer, against real Cloud Pub/Sub.
 *
 * <p>{@link PubSubBackpressuredSplitBufferITCase} establishes on the emulator that a buffer grows
 * while its drain is under the refill rate and not while it is over. That reading rests on the
 * flow-control permit being the only thing that gates delivery. One channel sits outside it: when a
 * lease finally lapses the service redelivers the message, {@code
 * PubSubNotifyingPullSubscriber.receiveMessage} appends the new copy <em>beside</em> the one the
 * connector is still holding, and {@code PubSubAckTracker.addPendingAck} nacks the superseded
 * handle — returning the permit the redelivery took. A redelivery therefore adds a buffered message
 * at no permit cost, and nothing in the deque dedupes.
 *
 * <p>The emulator never redelivers a lease-expired message at all (ADR-0066; the emulator arms
 * assert it), so this class is the only place that channel can be observed. It runs the slow arm
 * alone — the one that both drains and loses ground — because that is where a redelivered copy
 * arrives while the connector still holds its predecessor.
 *
 * <p><b>The assertion is the nack, not the duplicate.</b> While an arm runs the supersede is the
 * only thing that nacks — the deserialization policy never reaches an arm, which fetches records
 * without deserializing them, and the park and the teardown both land outside the sampled window —
 * so {@code messagesNacked} observes the channel directly and at the moment it happens. Counting
 * repeats among the drained messages observes the same thing much later and much more weakly: the
 * copies are appended at the back of the deque while the drain takes from the front at one message
 * a second, so whether a repeat is reached inside the window depends on how much the service
 * delivered in the first twenty seconds — a margin that shrinks on a <em>faster</em> connection. It
 * is logged, and asserted only as the weaker corroboration it is.
 *
 * <p><b>Measured 2026-08-09, two runs against real Cloud Pub/Sub and {@code google-cloud-pubsub}
 * 1.152.0.</b> Over the 90 s window the arm was delivered 369 and 462 messages while draining 90
 * and 91, and was left holding 279 and 371. Of those deliveries, <b>215 and 338 were supersedes</b>
 * — a redelivered copy of a message the connector was still holding — so on the real service the
 * majority of what a backpressured split is handed, and the majority of what its buffer then
 * carries, is churn rather than new data. Of the 90 drained, 74 were distinct: 16 records reached
 * the pipeline twice. Three things the emulator cannot say follow. The channel is real; it puts
 * duplicates into a <em>running</em> pipeline rather than only at a restart; and the growth has no
 * two-wave ceiling — the emulator stops there, the service does not.
 *
 * <p>The churn is also why {@code messagesReceived} is not a delivery total to reason about with
 * the permit accounting: a supersede returns the permit it took, so this channel is free of it.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "PUBSUB_IT_PROJECT", matches = ".+")
class PubSubBackpressuredSplitBufferRealGcpITCase extends AbstractPubSubRealGcpITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(PubSubBackpressuredSplitBufferRealGcpITCase.class);

    /** A fifth of the refill rate, the emulator run's slow arm exactly. */
    private static final double SLOW_RATE = REFILL_RATE / 5;

    /**
     * Longer than the emulator's 40 s: a redelivery arrives one acknowledgement deadline after the
     * client stops extending, so the first copy cannot land before a wave and a deadline in, and
     * the service is network-paced besides.
     */
    private static final Duration WINDOW = Duration.ofSeconds(90);

    private static final long MINIMUM_SAMPLES = WINDOW.toMillis() / 250 / 2;

    @Test
    void aBackpressuredSplitBuffersRedeliveredCopiesBesideTheOnesItStillHolds() throws Exception {
        TopicDestination topic = createTopic("bp-redelivery");
        SubscriptionDestination subscription =
                createSubscription(
                        topic,
                        "bp-redelivery",
                        builder -> builder.setAckDeadlineSeconds(ACK_DEADLINE_SECONDS));
        publish(topic, BackpressuredArm.payloads(BACKLOG));

        BackpressuredArm arm = new BackpressuredArm("bp-redelivery", SLOW_RATE, subscription, null);

        BackpressuredArm.runFor(WINDOW, List.of(arm));

        LOG.info("Backpressured split buffer against real Pub/Sub (#377): {}", arm);

        assertThat(arm.samples())
                .as("the series was sampled throughout, %s", arm)
                .isGreaterThanOrEqualTo(MINIMUM_SAMPLES);
        assertThat(arm.drained())
                .as("the arm was demonstrably draining, %s", arm)
                .isGreaterThanOrEqualTo((long) (0.5 * SLOW_RATE * WINDOW.toSeconds()));
        assertThat(arm.buffered())
                .as("and still lost ground against the lapse, %s", arm)
                .isGreaterThan(FLOW_CONTROL_MESSAGES);
        assertThat(arm.received())
                .as("delivery outgrew the flow-control window, %s", arm)
                .isGreaterThan(2 * FLOW_CONTROL_MESSAGES);

        // The channel the emulator cannot show, observed where it happens: a nack before the
        // teardown is a redelivery superseding a message the connector was still holding.
        assertThat(arm.nacked())
                .as("a redelivery superseded a message this split still held, %s", arm)
                .isPositive();
        assertThat(arm.distinct())
                .as("and a superseded copy reached the drain twice, %s", arm)
                .isLessThan(arm.drained());
    }
}
