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

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriberShutdownSettings;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.grpc.ManagedChannelBuilder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;

/**
 * {@link SubscriberFactory} building {@code google-cloud-pubsub} {@link Subscriber} instances.
 *
 * <p>Two settings are owned by the source rather than left at their SDK defaults:
 *
 * <ul>
 *   <li><b>Shutdown mode.</b> The SDK defaults to {@code WAIT_FOR_PROCESSING}, which waits for
 *       every outstanding message to be acknowledged — but this source only acknowledges on
 *       checkpoint completion, so shutdown would stall. {@code NACK_IMMEDIATELY} instead releases
 *       messages the client still holds, including any it buffered without ever handing them to the
 *       receiver, so Pub/Sub redelivers them at once instead of after the acknowledgement deadline.
 *   <li><b>Parallel pull count under {@link OrderingMode#PER_KEY}.</b> Each streaming-pull
 *       connection has its own message dispatcher, and per-ordering-key callback serialization is
 *       per dispatcher — so more than one connection would let two messages of the same key be
 *       delivered concurrently. Ordered subscriptions therefore use exactly one connection.
 * </ul>
 *
 * <p>When an emulator endpoint is set, subscribers connect to it over a plaintext channel with no
 * credentials. The transport provider is the instantiating one, so each subscriber creates the
 * channel it uses and releases it when it stops.
 */
@Internal
public final class DefaultSubscriberFactory implements SubscriberFactory {

    /**
     * Bounds how long {@code close()} waits for one client to release outstanding messages. Kept
     * well under Flink's {@code source.reader.close.timeout} (30 s by default) because a reader
     * closes its splits' subscribers one after another: a split whose turn never comes within that
     * budget is a split whose messages are not nacked, so they only return after their
     * acknowledgement deadline instead of at once.
     */
    static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);

    private final OrderingMode orderingMode;
    @Nullable private final String emulatorEndpoint;

    /**
     * Creates the factory.
     *
     * @param orderingMode the ordering mode, which decides the streaming-pull connection count
     * @param emulatorEndpoint the emulator endpoint as {@code host:port} (plaintext, no
     *     credentials), or {@code null} for production Pub/Sub
     */
    public DefaultSubscriberFactory(OrderingMode orderingMode, @Nullable String emulatorEndpoint) {
        this.orderingMode = orderingMode;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public Subscriber create(SubscriptionDestination subscription, MessageReceiver receiver)
            throws IOException {
        try {
            Subscriber.Builder builder =
                    Subscriber.newBuilder(subscription.toSubscriptionPath(), receiver)
                            .setSubscriberShutdownSettings(
                                    SubscriberShutdownSettings.newBuilder()
                                            .setMode(
                                                    SubscriberShutdownSettings.ShutdownMode
                                                            .NACK_IMMEDIATELY)
                                            .setTimeout(SHUTDOWN_TIMEOUT)
                                            .build());
            if (orderingMode == OrderingMode.PER_KEY) {
                builder.setParallelPullCount(1);
            }
            if (emulatorEndpoint != null) {
                builder.setChannelProvider(
                                SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                        .setEndpoint(emulatorEndpoint)
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
}
