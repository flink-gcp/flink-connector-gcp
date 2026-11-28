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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.core.ApiService;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.MessageReceiverWithAckResponse;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriberShutdownSettings;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.PubSubTransports;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * {@link SubscriberFactory} building {@code google-cloud-pubsub} {@link Subscriber} instances
 * configured from {@link PubSubSubscriberOptions}; every knob left unset keeps the SDK default, so
 * the default configuration differs from the SDK's only in the two settings the source owns.
 *
 * <p>Those two are:
 *
 * <ul>
 *   <li><b>Shutdown mode.</b> The SDK defaults to {@code WAIT_FOR_PROCESSING}, which waits for
 *       every outstanding message to be acknowledged — but this source acknowledges a message only
 *       once a completed checkpoint covers it, so shutdown would stall. {@code NACK_IMMEDIATELY}
 *       instead releases messages the client still holds, including any it buffered without ever
 *       handing them to the receiver, so Pub/Sub redelivers them at once instead of after the
 *       acknowledgement deadline. The mode is fixed for that reason; only its timeout is a knob.
 *   <li><b>Parallel pull count under {@link OrderingMode#PER_KEY}.</b> Ordered subscriptions use
 *       exactly one streaming-pull connection; {@code PubSubSourceBuilder.build()} rejects an
 *       explicit count above one and explains why, so forcing it here only ever overrides the SDK
 *       default — which is what keeps the ordering guarantee independent of that default.
 * </ul>
 *
 * <p>Every subscriber runs on a {@link PubSubTransports} transport, with its channels on the
 * connector's gRPC event-loop group (ADR-0208), which the returned {@link SubscriberClient} holds
 * until its owner closes it. With an emulator endpoint the subscriber connects over a plaintext
 * channel with no credentials.
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

    /**
     * Returns the acknowledgement-deadline extension budget the subscriber runs with: the
     * configured one, or the client library's default when it is unset.
     *
     * @param options the subscriber options
     * @return the effective {@code maxAckExtensionPeriod}
     */
    public static Duration effectiveMaxAckExtensionPeriod(PubSubSubscriberOptions options) {
        Duration configured = options.getMaxAckExtensionPeriod();
        return configured != null ? configured : DEFAULT_MAX_ACK_EXTENSION_PERIOD;
    }

    private final PubSubSubscriberOptions options;
    private final OrderingMode orderingMode;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    @Nullable private final CredentialsProvider credentialsOverride;

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
        this(options, orderingMode, emulatorEndpoint, null);
    }

    /**
     * Creates the factory with an optional production credentials override.
     *
     * @param options the subscriber tuning options
     * @param orderingMode the ordering mode, which decides the streaming-pull connection count
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for production Pub/Sub
     * @param credentialsOverride credentials to use in production, or {@code null} for ADC
     */
    public DefaultSubscriberFactory(
            PubSubSubscriberOptions options,
            OrderingMode orderingMode,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable CredentialsProvider credentialsOverride) {
        this.options = options;
        this.orderingMode = orderingMode;
        this.emulatorEndpoint = emulatorEndpoint;
        this.credentialsOverride = credentialsOverride;
    }

    @Override
    public SubscriberClient create(SubscriptionDestination subscription, MessageConsumer consumer)
            throws IOException {
        PubSubTransports.Transport transport = null;
        try {
            transport = PubSubTransports.openForSubscriber(emulatorEndpoint);
            Subscriber.Builder builder =
                    newBuilder(subscription, consumer)
                            .setChannelProvider(transport.channelProvider());
            configure(builder, options, orderingMode);
            return new SubscriberAdapter(builder.build(), transport);
        } catch (IOException | RuntimeException | Error e) {
            // Owned here until the caller takes it over on success; which failure is reported is
            // closeAllSuppressing's contract.
            Closers.closeAllSuppressing(e, transport);
            if (e instanceof Error) {
                throw (Error) e;
            }
            throw new IOException(
                    "Failed to create the Pub/Sub subscriber for subscription " + subscription, e);
        }
    }

    /**
     * Starts a builder on the receiver flavor the options call for. The two flavors are separate
     * SDK interfaces, selected here and nowhere else — which is why everything above this class
     * settles messages through {@link AckHandle}.
     */
    @VisibleForTesting
    Subscriber.Builder newBuilder(SubscriptionDestination subscription, MessageConsumer consumer) {
        String path = subscription.toSubscriptionPath();
        Subscriber.Builder builder;
        if (options.getAwaitAckConfirmation() != null) {
            builder =
                    Subscriber.newBuilder(
                            path,
                            (MessageReceiverWithAckResponse)
                                    (message, reply) ->
                                            consumer.receive(message, AckHandle.of(reply)));
        } else {
            builder =
                    Subscriber.newBuilder(
                            path,
                            (MessageReceiver)
                                    (message, reply) ->
                                            consumer.receive(message, AckHandle.of(reply)));
        }
        if (emulatorEndpoint != null) {
            builder.setCredentialsProvider(NoCredentialsProvider.create());
        } else if (credentialsOverride != null) {
            builder.setCredentialsProvider(credentialsOverride);
        }
        return builder;
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

    /**
     * Adapts the SDK {@link Subscriber} and its transport to the reader-facing {@link
     * SubscriberClient} interface.
     */
    @VisibleForTesting
    static final class SubscriberAdapter implements SubscriberClient {

        private final Subscriber subscriber;
        private final PubSubTransports.Transport transport;

        SubscriberAdapter(Subscriber subscriber, PubSubTransports.Transport transport) {
            this.subscriber = subscriber;
            this.transport = transport;
        }

        /** The SDK client, for a test that reads what the builder configured. */
        @VisibleForTesting
        Subscriber subscriber() {
            return subscriber;
        }

        /**
         * The production wiring. {@code StreamingPullSubscriberTest} drives its seam constructor,
         * so this is pinned by {@code StreamingPullSubscriberFailedStartTest}, whose failed start
         * shows that the listener is registered <em>before</em> the start (a failure raised in
         * between would be lost, and the split would then stall silently rather than fail) and that
         * its executor is a direct one. That the stop and the wait act on the client this starts is
         * structural: all three use the one {@code subscriber} this adapter holds.
         */
        @Override
        public void start(Consumer<Throwable> onPermanentFailure) {
            subscriber.addListener(
                    new ApiService.Listener() {
                        @Override
                        public void failed(ApiService.State from, Throwable failure) {
                            onPermanentFailure.accept(failure);
                        }
                    },
                    // A direct executor: recording the failure and waking the fetcher are both
                    // cheap.
                    Runnable::run);
            subscriber.startAsync().awaitRunning();
        }

        @Override
        public void stopAsync() {
            subscriber.stopAsync();
        }

        @Override
        public void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {
            subscriber.awaitTerminated(timeout, unit);
        }

        @Override
        public void close() {
            transport.close();
        }
    }
}
