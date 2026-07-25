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

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriberShutdownSettings;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the settings mapping of {@link DefaultSubscriberFactory}. */
class DefaultSubscriberFactoryTest {

    private static final FlowControlSettings SDK_FLOW_CONTROL_DEFAULTS =
            Subscriber.Builder.getDefaultFlowControlSettings();

    private static final SubscriptionDestination SUBSCRIPTION =
            SubscriptionDestination.of("test-project", "test-subscription");

    /**
     * Unreachable on purpose: gRPC connects lazily, so a subscriber builds offline against it. The
     * emulator path also swaps in no-credentials, so nothing looks for application-default
     * credentials either.
     */
    private static final String UNREACHABLE_ENDPOINT = "localhost:1";

    private static final MessageReceiver NO_OP_RECEIVER = (message, consumer) -> {};

    @Test
    void flowControlOverlaysOnlySetLimitsOverSdkDefaults() {
        PubSubSubscriberOptions options =
                PubSubSubscriberOptions.builder().flowControlMaxOutstandingElementCount(50).build();

        FlowControlSettings flowControl = DefaultSubscriberFactory.flowControlSettings(options);

        assertThat(flowControl.getMaxOutstandingElementCount()).isEqualTo(50);
        assertThat(flowControl.getMaxOutstandingRequestBytes())
                .isEqualTo(SDK_FLOW_CONTROL_DEFAULTS.getMaxOutstandingRequestBytes());
        assertThat(flowControl.getLimitExceededBehavior())
                .isEqualTo(SDK_FLOW_CONTROL_DEFAULTS.getLimitExceededBehavior());
    }

    @Test
    void flowControlAppliesTheByteLimit() {
        PubSubSubscriberOptions options =
                PubSubSubscriberOptions.builder()
                        .flowControlMaxOutstandingRequestBytes(4_096)
                        .build();

        FlowControlSettings flowControl = DefaultSubscriberFactory.flowControlSettings(options);

        assertThat(flowControl.getMaxOutstandingRequestBytes()).isEqualTo(4_096);
        assertThat(flowControl.getMaxOutstandingElementCount())
                .isEqualTo(SDK_FLOW_CONTROL_DEFAULTS.getMaxOutstandingElementCount());
    }

    @Test
    void unsetOptionsKeepEverySdkDefault() throws Exception {
        Subscriber subscriber = subscriber(PubSubSubscriberOptions.defaults(), OrderingMode.NONE);

        assertThat(subscriber.getFlowControlSettings()).isEqualTo(SDK_FLOW_CONTROL_DEFAULTS);
        assertThat(field(subscriber, "maxAckExtensionPeriod"))
                .isEqualTo(DefaultSubscriberFactory.DEFAULT_MAX_ACK_EXTENSION_PERIOD);
        assertThat(field(subscriber, "minDurationPerAckExtensionDefaultUsed"))
                .isEqualTo(Boolean.TRUE);
        assertThat(field(subscriber, "maxDurationPerAckExtensionDefaultUsed"))
                .isEqualTo(Boolean.TRUE);
        assertThat(field(subscriber, "numPullers")).isEqualTo(1);
    }

    @Test
    void ackExtensionAndFlowControlKnobsReachTheSubscriber() throws Exception {
        PubSubSubscriberOptions options =
                PubSubSubscriberOptions.builder()
                        .flowControlMaxOutstandingElementCount(50)
                        .flowControlMaxOutstandingRequestBytes(4_096)
                        .maxAckExtensionPeriod(Duration.ofMinutes(20))
                        .minDurationPerAckExtension(Duration.ofSeconds(15))
                        .maxDurationPerAckExtension(Duration.ofSeconds(45))
                        .build();

        Subscriber subscriber = subscriber(options, OrderingMode.NONE);

        assertThat(subscriber.getFlowControlSettings().getMaxOutstandingElementCount())
                .isEqualTo(50);
        assertThat(subscriber.getFlowControlSettings().getMaxOutstandingRequestBytes())
                .isEqualTo(4_096);
        assertThat(field(subscriber, "maxAckExtensionPeriod")).isEqualTo(Duration.ofMinutes(20));
        assertThat(field(subscriber, "minDurationPerAckExtension"))
                .isEqualTo(Duration.ofSeconds(15));
        assertThat(field(subscriber, "maxDurationPerAckExtension"))
                .isEqualTo(Duration.ofSeconds(45));
        assertThat(field(subscriber, "minDurationPerAckExtensionDefaultUsed"))
                .isEqualTo(Boolean.FALSE);
        assertThat(field(subscriber, "maxDurationPerAckExtensionDefaultUsed"))
                .isEqualTo(Boolean.FALSE);
    }

    @Test
    void parallelPullCountIsAppliedWhenOrderingIsDisabled() throws Exception {
        Subscriber subscriber =
                subscriber(
                        PubSubSubscriberOptions.builder().parallelPullCount(4).build(),
                        OrderingMode.NONE);

        assertThat(field(subscriber, "numPullers")).isEqualTo(4);
    }

    @Test
    void orderedModeForcesASingleStreamingPullConnection() throws Exception {
        // The linchpin of the ordering guarantee: per-ordering-key callback serialization is per
        // message dispatcher, and each streaming-pull connection has its own.
        Subscriber subscriber =
                subscriber(PubSubSubscriberOptions.defaults(), OrderingMode.PER_KEY);

        assertThat(field(subscriber, "numPullers")).isEqualTo(1);
    }

    @Test
    void shutdownAlwaysNacksImmediatelyWithTheConfiguredTimeout() throws Exception {
        Subscriber subscriber =
                subscriber(
                        PubSubSubscriberOptions.builder()
                                .shutdownTimeout(Duration.ofSeconds(7))
                                .build(),
                        OrderingMode.NONE);

        SubscriberShutdownSettings shutdown =
                (SubscriberShutdownSettings) field(subscriber, "subscriberShutdownSettings");
        assertThat(shutdown.getMode())
                .isEqualTo(SubscriberShutdownSettings.ShutdownMode.NACK_IMMEDIATELY);
        assertThat(shutdown.getTimeout()).isEqualTo(Duration.ofSeconds(7));
    }

    @Test
    void mirroredMaxAckExtensionPeriodDefaultMatchesTheSdk() throws Exception {
        Field sdkDefault = Subscriber.class.getDeclaredField("DEFAULT_MAX_ACK_EXTENSION_PERIOD");
        sdkDefault.setAccessible(true);

        assertThat(DefaultSubscriberFactory.DEFAULT_MAX_ACK_EXTENSION_PERIOD)
                .isEqualTo(sdkDefault.get(null));
    }

    private static Subscriber subscriber(PubSubSubscriberOptions options, OrderingMode orderingMode)
            throws IOException {
        return new DefaultSubscriberFactory(options, orderingMode, UNREACHABLE_ENDPOINT)
                .create(SUBSCRIPTION, NO_OP_RECEIVER);
    }

    private static Object field(Subscriber subscriber, String name) throws Exception {
        Field field = Subscriber.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(subscriber);
    }
}
