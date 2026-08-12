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
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriberShutdownSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the settings mapping of {@link DefaultSubscriberFactory}.
 *
 * <p>Assertions go through the {@link Subscriber.Builder} the factory configures rather than a
 * built {@link Subscriber}: {@code build()} eagerly allocates a scheduled thread pool that only a
 * started-then-stopped subscriber releases, so building one per assertion would leak daemon threads
 * for the life of the test JVM. One test builds a real subscriber as the end-to-end anchor.
 */
class DefaultSubscriberFactoryTest {

    private static final FlowControlSettings SDK_FLOW_CONTROL_DEFAULTS =
            Subscriber.Builder.getDefaultFlowControlSettings();

    private static final SubscriptionDestination SUBSCRIPTION =
            SubscriptionDestination.of("test-project", "test-subscription");

    private static final SubscriberFactory.MessageConsumer NO_OP_CONSUMER =
            (message, ackHandle) -> {};

    @Test
    void flowControlOverlaysOnlySetLimitsOverSdkDefaults() {
        FlowControlSettings byCount =
                DefaultSubscriberFactory.flowControlSettings(
                        PubSubSubscriberOptions.builder()
                                .flowControlMaxOutstandingElementCount(50)
                                .build());
        FlowControlSettings byBytes =
                DefaultSubscriberFactory.flowControlSettings(
                        PubSubSubscriberOptions.builder()
                                .flowControlMaxOutstandingRequestBytes(4_096)
                                .build());

        assertThat(byCount.getMaxOutstandingElementCount()).isEqualTo(50);
        assertThat(byCount.getMaxOutstandingRequestBytes())
                .isEqualTo(SDK_FLOW_CONTROL_DEFAULTS.getMaxOutstandingRequestBytes());
        assertThat(byBytes.getMaxOutstandingRequestBytes()).isEqualTo(4_096);
        assertThat(byBytes.getMaxOutstandingElementCount())
                .isEqualTo(SDK_FLOW_CONTROL_DEFAULTS.getMaxOutstandingElementCount());
        // The subscriber forces blocking regardless of the settings, so the factory leaves the
        // behavior alone rather than pretending it is a choice.
        assertThat(byCount.getLimitExceededBehavior())
                .isEqualTo(SDK_FLOW_CONTROL_DEFAULTS.getLimitExceededBehavior());
    }

    @Test
    void unsetOptionsKeepEverySdkDefault() throws Exception {
        Subscriber.Builder builder =
                configured(PubSubSubscriberOptions.defaults(), OrderingMode.NONE);

        assertThat(field(builder, "flowControlSettings")).isEqualTo(SDK_FLOW_CONTROL_DEFAULTS);
        assertThat(field(builder, "maxAckExtensionPeriod"))
                .isEqualTo(DefaultSubscriberFactory.DEFAULT_MAX_ACK_EXTENSION_PERIOD);
        assertThat(field(builder, "minDurationPerAckExtensionDefaultUsed")).isEqualTo(Boolean.TRUE);
        assertThat(field(builder, "maxDurationPerAckExtensionDefaultUsed")).isEqualTo(Boolean.TRUE);
        assertThat(field(builder, "parallelPullCount")).isEqualTo(1);
    }

    @Test
    void ackExtensionKnobsReachTheSubscriberBuilder() throws Exception {
        PubSubSubscriberOptions options =
                PubSubSubscriberOptions.builder()
                        .maxAckExtensionPeriod(Duration.ofMinutes(20))
                        .minDurationPerAckExtension(Duration.ofSeconds(15))
                        .maxDurationPerAckExtension(Duration.ofSeconds(45))
                        .build();

        Subscriber.Builder builder = configured(options, OrderingMode.NONE);

        assertThat(field(builder, "maxAckExtensionPeriod")).isEqualTo(Duration.ofMinutes(20));
        assertThat(field(builder, "minDurationPerAckExtension")).isEqualTo(Duration.ofSeconds(15));
        assertThat(field(builder, "maxDurationPerAckExtension")).isEqualTo(Duration.ofSeconds(45));
        assertThat(field(builder, "minDurationPerAckExtensionDefaultUsed"))
                .isEqualTo(Boolean.FALSE);
        assertThat(field(builder, "maxDurationPerAckExtensionDefaultUsed"))
                .isEqualTo(Boolean.FALSE);
    }

    @Test
    void parallelPullCountIsAppliedWhenOrderingIsDisabled() throws Exception {
        Subscriber.Builder builder =
                configured(
                        PubSubSubscriberOptions.builder().parallelPullCount(4).build(),
                        OrderingMode.NONE);

        assertThat(field(builder, "parallelPullCount")).isEqualTo(4);
    }

    @Test
    void orderedModeForcesASingleStreamingPullConnection() throws Exception {
        // The linchpin of the ordering guarantee: per-ordering-key callback serialization is per
        // message dispatcher, and each streaming-pull connection has its own. The source builder
        // rejects an explicit count above one, so this only ever overrides the SDK default —
        // which is what keeps the guarantee independent of that default.
        Subscriber.Builder builder =
                configured(PubSubSubscriberOptions.defaults(), OrderingMode.PER_KEY);

        assertThat(field(builder, "parallelPullCount")).isEqualTo(1);
    }

    @Test
    void shutdownAlwaysNacksImmediatelyWithTheConfiguredTimeout() throws Exception {
        Subscriber.Builder builder =
                configured(
                        PubSubSubscriberOptions.builder()
                                .shutdownTimeout(Duration.ofSeconds(7))
                                .build(),
                        OrderingMode.NONE);

        SubscriberShutdownSettings shutdown =
                (SubscriberShutdownSettings) field(builder, "subscriberShutdownSettings");
        assertThat(shutdown.getMode())
                .isEqualTo(SubscriberShutdownSettings.ShutdownMode.NACK_IMMEDIATELY);
        assertThat(shutdown.getTimeout()).isEqualTo(Duration.ofSeconds(7));
    }

    @Test
    void theFactoryBuildsAConfiguredSubscriberOffline() throws Exception {
        // End-to-end through the production path. The endpoint is unreachable on purpose: gRPC
        // connects lazily, and the emulator path swaps in no-credentials, so nothing here looks
        // for application-default credentials or opens a socket.
        PubSubSubscriberOptions options =
                PubSubSubscriberOptions.builder()
                        .flowControlMaxOutstandingElementCount(50)
                        .parallelPullCount(2)
                        .build();

        Subscriber subscriber =
                new DefaultSubscriberFactory(
                                options, OrderingMode.NONE, EmulatorEndpoint.parse("localhost:1"))
                        .create(SUBSCRIPTION, NO_OP_CONSUMER);

        assertThat(subscriber.getFlowControlSettings().getMaxOutstandingElementCount())
                .isEqualTo(50);
        assertThat(field(subscriber, "numPullers")).isEqualTo(2);
    }

    @Test
    void configuredCredentialsReachTheSubscriberBuilder() throws Exception {
        NoCredentialsProvider credentials = NoCredentialsProvider.create();
        DefaultSubscriberFactory factory =
                new DefaultSubscriberFactory(
                        PubSubSubscriberOptions.defaults(), OrderingMode.NONE, null, credentials);

        Subscriber.Builder builder = factory.newBuilder(SUBSCRIPTION, NO_OP_CONSUMER);

        assertThat(field(builder, "credentialsProvider")).isSameAs(credentials);
    }

    @Test
    void theReceiverFlavorFollowsAwaitAckConfirmation() throws Exception {
        // The two SDK receiver interfaces are chosen when the subscriber is built and cannot be
        // swapped afterwards, so this is what decides whether an acknowledgement can be confirmed.
        Subscriber fireAndForget =
                new DefaultSubscriberFactory(
                                PubSubSubscriberOptions.defaults(),
                                OrderingMode.NONE,
                                EmulatorEndpoint.parse("localhost:1"))
                        .create(SUBSCRIPTION, NO_OP_CONSUMER);
        Subscriber confirming =
                new DefaultSubscriberFactory(
                                PubSubSubscriberOptions.builder()
                                        .awaitAckConfirmation(Duration.ofSeconds(30))
                                        .build(),
                                OrderingMode.NONE,
                                EmulatorEndpoint.parse("localhost:1"))
                        .create(SUBSCRIPTION, NO_OP_CONSUMER);

        assertThat(field(fireAndForget, "receiver")).isNotNull();
        assertThat(field(fireAndForget, "receiverWithAckResponse")).isNull();
        assertThat(field(confirming, "receiver")).isNull();
        assertThat(field(confirming, "receiverWithAckResponse")).isNotNull();
    }

    @Test
    void mirroredMaxAckExtensionPeriodDefaultMatchesTheSdk() throws Exception {
        Field sdkDefault = Subscriber.class.getDeclaredField("DEFAULT_MAX_ACK_EXTENSION_PERIOD");
        sdkDefault.setAccessible(true);

        assertThat(DefaultSubscriberFactory.DEFAULT_MAX_ACK_EXTENSION_PERIOD)
                .isEqualTo(sdkDefault.get(null));
    }

    private static Subscriber.Builder configured(
            PubSubSubscriberOptions options, OrderingMode orderingMode) {
        // Any receiver flavor will do here: configure() only writes settings, and which flavor a
        // real subscriber gets is covered by receiverFlavorFollowsAwaitAckConfirmation.
        Subscriber.Builder builder =
                Subscriber.newBuilder(
                        SUBSCRIPTION.toSubscriptionPath(), (MessageReceiver) (m, reply) -> {});
        DefaultSubscriberFactory.configure(builder, options, orderingMode);
        return builder;
    }

    private static Object field(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
