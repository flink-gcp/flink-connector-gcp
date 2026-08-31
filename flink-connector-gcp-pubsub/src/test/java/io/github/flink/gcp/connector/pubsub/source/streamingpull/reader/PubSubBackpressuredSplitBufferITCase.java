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

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.source.AbstractPubSubSourceEmulatorITCase;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriberBufferLimitExceededEvent;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import static io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.BackpressuredArm.ACK_DEADLINE_SECONDS;
import static io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.BackpressuredArm.BACKLOG;
import static io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.BackpressuredArm.FLOW_CONTROL_MESSAGES;
import static io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.BackpressuredArm.MAX_ACK_EXTENSION_PERIOD;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that the callback-side reader budget stops buffer growth while downstream is completely
 * stalled (#1138).
 *
 * <p>The stalled arm never calls {@code fetch()}. It therefore reproduces the state in which every
 * reader-side guard is blind because Flink's fetcher cannot enqueue another batch. The emulator
 * releases flow-control permits after acknowledgement extension expires, so the arm would continue
 * to grow without the callback-side budget (the earlier #377 measurement observed 50, then about
 * 101, then about 152 messages).
 *
 * <p>The window is twice {@link BackpressuredArm#MAX_ACK_EXTENSION_PERIOD}. The stalled arm's
 * hard-limit event must occur during that window, and samples taken after it must remain at the
 * exact cap. A concurrently drained control keeps its hard limit out of the way, continues beyond
 * two flow-control windows, and ends below one window. These assertions distinguish a working bound
 * from an emulator or subscriber that stopped on its own while leaving ordinary delivery bursts
 * unconstrained.
 *
 * <p>The test is tagged {@code slow} because elapsed time past acknowledgement extension is the
 * instrument.
 */
@Tag("slow")
class PubSubBackpressuredSplitBufferITCase extends AbstractPubSubSourceEmulatorITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(PubSubBackpressuredSplitBufferITCase.class);

    private static final long BUFFER_CAP = 75;
    private static final double FAST_RATE = BackpressuredArm.REFILL_RATE * 3;
    private static final Duration WINDOW = MAX_ACK_EXTENSION_PERIOD.multipliedBy(2);
    private static final long MINIMUM_POST_LIMIT_SAMPLES = 20;

    @Test
    void aFrozenDownstreamPlateausPastTheAckExtensionBudget() throws Exception {
        BackpressuredArm stalled = arm("bounded-stalled", 0, BUFFER_CAP);
        BackpressuredArm drained = arm("drained-control", FAST_RATE, Long.MAX_VALUE);

        BackpressuredArm.runFor(WINDOW, List.of(stalled, drained));

        LOG.info("Bounded stalled Pub/Sub source (#1138): {}", stalled);
        LOG.info("Drained control for Pub/Sub source (#1138): {}", drained);

        SubscriberBufferLimitExceededEvent event = stalled.limitExceeded();
        assertThat(event).as("the stalled arm crossed the callback-side budget").isNotNull();
        assertThat(event.attemptedMessages()).isEqualTo(BUFFER_CAP + 1);
        assertThat(event.maxMessages()).isEqualTo(BUFFER_CAP);
        assertThat(stalled.buffered()).isEqualTo(BUFFER_CAP);
        assertThat(stalled.peakBuffered()).isEqualTo(BUFFER_CAP);
        assertThat(stalled.samplesAfterLimit())
                .as("the buffer stayed capped for samples after the subscriber was stopped")
                .isGreaterThanOrEqualTo(MINIMUM_POST_LIMIT_SAMPLES);
        assertThat(stalled.nacked())
                .as("the crossing delivery was rejected rather than retained")
                .isPositive();

        assertThat(drained.received())
                .as("the control subscriber kept delivering throughout the trial")
                .isGreaterThan(2 * FLOW_CONTROL_MESSAGES);
        assertThat(drained.buffered()).isLessThan(FLOW_CONTROL_MESSAGES);
    }

    private BackpressuredArm arm(String name, double ratePerSecond, long bufferMaxMessages)
            throws Exception {
        SubscriptionDestination subscription =
                createTopicAndSubscription(name, ACK_DEADLINE_SECONDS);
        publish(name, BackpressuredArm.payloads(BACKLOG));
        return new BackpressuredArm(
                name,
                ratePerSecond,
                subscription,
                EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint"),
                bufferMaxMessages,
                Long.MAX_VALUE);
    }
}
