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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.MessageReceiverWithAckResponse;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriberShutdownSettings;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.grpc.ManagedChannelBuilder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;

/**
 * {@link SubscriberFactory} building {@code google-cloud-pubsub} {@link Subscriber} instances
 * configured from {@link PubSubSubscriberOptions}; every knob left unset keeps the SDK default, so
 * the default configuration differs from the SDK's only in the two settings the source owns.
 *
 * <p>Those two are:
 *
 * <ul>
 *   <li><b>Shutdown mode.</b> The SDK defaults to {@code WAIT_FOR_PROCESSING}, which waits for
 *       every outstanding message to be acknowledged — but this source only acknowledges on
 *       checkpoint completion, so shutdown would stall. {@code NACK_IMMEDIATELY} instead releases
 *       messages the client still holds, including any it buffered without ever handing them to the
 *       receiver, so Pub/Sub redelivers them at once instead of after the acknowledgement deadline.
 *       The mode is fixed for that reason; only its timeout is a knob.
 *   <li><b>Parallel pull count under {@link OrderingMode#PER_KEY}.</b> Ordered subscriptions use
 *       exactly one streaming-pull connection; {@code PubSubSourceBuilder.build()} rejects an
 *       explicit count above one and explains why, so forcing it here only ever overrides the SDK
 *       default — which is what keeps the ordering guarantee independent of that default.
 * </ul>
 *
 * <p>When an emulator endpoint is set, subscribers connect to it over a plaintext channel with no
 * credentials. The transport provider is the instantiating one, so each subscriber creates the
 * channel it uses and releases it when it stops.
 */
@Internal
public final class DefaultSubscriberFactory implements SubscriberFactory {

    /**
     * Mirror of the SDK's default acknowledgement-deadline extension budget ({@code
     * Subscriber.DEFAULT_MAX_ACK_EXTENSION_PERIOD} is package-private): the source compares the
     * checkpoint interval against it when the options leave that knob unset. A drift-guard test
     * pins this mirror to the SDK constant.
     */
    public static final Duration DEFAULT_MAX_ACK_EXTENSION_PERIOD = Duration.ofMinutes(60);

    private final PubSubSubscriberOptions options;
    private final OrderingMode orderingMode;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * Creates the factory.
     *
     * @param options the subscriber tuning options
     * @param orderingMode the ordering mode, which decides the streaming-pull connection count
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Pub/Sub
     */
    public DefaultSubscriberFactory(
            PubSubSubscriberOptions options,
            OrderingMode orderingMode,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.options = options;
        this.orderingMode = orderingMode;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public Subscriber create(SubscriptionDestination subscription, MessageConsumer consumer)
            throws IOException {
        try {
            Subscriber.Builder builder = newBuilder(subscription, consumer);
            configure(builder, options, orderingMode);
            if (emulatorEndpoint != null) {
                builder.setChannelProvider(
                                SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                        .setEndpoint(emulatorEndpoint.getTarget())
                                        .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
                                        .build())
                        .setCredentialsProvider(NoCredentialsProvider.create());
            }
            return builder.build();
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to create the Pub/Sub subscriber for subscription " + subscription, e);
        }
    }

    /**
     * Starts a builder on the receiver flavor the options call for. The two flavors are separate
     * SDK interfaces, selected here and nowhere else — which is why everything above this class
     * settles messages through {@link AckHandle}.
     */
    private Subscriber.Builder newBuilder(
            SubscriptionDestination subscription, MessageConsumer consumer) {
        String path = subscription.toSubscriptionPath();
        if (options.getAwaitAckConfirmation() != null) {
            return Subscriber.newBuilder(
                    path,
                    (MessageReceiverWithAckResponse)
                            (message, reply) -> consumer.receive(message, AckHandle.of(reply)));
        }
        return Subscriber.newBuilder(
                path,
                (MessageReceiver)
                        (message, reply) -> consumer.receive(message, AckHandle.of(reply)));
    }

    /** Applies the options onto the subscriber builder; unset knobs are left at SDK defaults. */
    @VisibleForTesting
    static void configure(
            Subscriber.Builder builder,
            PubSubSubscriberOptions options,
            OrderingMode orderingMode) {
        builder.setSubscriberShutdownSettings(
                SubscriberShutdownSettings.newBuilder()
                        .setMode(SubscriberShutdownSettings.ShutdownMode.NACK_IMMEDIATELY)
                        .setTimeout(options.getShutdownTimeout())
                        .build());
        if (orderingMode == OrderingMode.PER_KEY) {
            builder.setParallelPullCount(1);
        } else if (options.getParallelPullCount() != null) {
            builder.setParallelPullCount(options.getParallelPullCount());
        }
        if (options.getFlowControlMaxOutstandingElementCount() != null
                || options.getFlowControlMaxOutstandingRequestBytes() != null) {
            builder.setFlowControlSettings(flowControlSettings(options));
        }
        if (options.getMaxAckExtensionPeriod() != null) {
            builder.setMaxAckExtensionPeriodDuration(options.getMaxAckExtensionPeriod());
        }
        if (options.getMinDurationPerAckExtension() != null) {
            builder.setMinDurationPerAckExtensionDuration(options.getMinDurationPerAckExtension());
        }
        if (options.getMaxDurationPerAckExtension() != null) {
            builder.setMaxDurationPerAckExtensionDuration(options.getMaxDurationPerAckExtension());
        }
    }

    /**
     * Builds the SDK flow-control settings: the SDK defaults overlaid with the set limits.
     *
     * <p>The limit behavior is deliberately not set. The subscriber's constructor overrides it to
     * blocking whatever the settings say — which for a subscriber is not a blocked thread but a
     * client that stops pulling, so there is nothing to choose.
     */
    @VisibleForTesting
    static FlowControlSettings flowControlSettings(PubSubSubscriberOptions options) {
        FlowControlSettings.Builder flowControl =
                Subscriber.Builder.getDefaultFlowControlSettings().toBuilder();
        if (options.getFlowControlMaxOutstandingElementCount() != null) {
            flowControl.setMaxOutstandingElementCount(
                    options.getFlowControlMaxOutstandingElementCount());
        }
        if (options.getFlowControlMaxOutstandingRequestBytes() != null) {
            flowControl.setMaxOutstandingRequestBytes(
                    options.getFlowControlMaxOutstandingRequestBytes());
        }
        return flowControl.build();
    }
}
