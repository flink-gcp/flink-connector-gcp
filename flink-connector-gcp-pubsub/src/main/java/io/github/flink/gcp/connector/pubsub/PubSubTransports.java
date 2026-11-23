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

package io.github.flink.gcp.connector.pubsub;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorChannels;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;

/**
 * The transports this connector's publishers and subscribers run on: their channels on the
 * connector's gRPC event-loop group (ADR-0208), held there until the client is done with them.
 *
 * <p>In production the client builds its own channels, from a provider that mirrors the one its SDK
 * builder would use by default, with the group applied. Both builders keep that default in a
 * private field with no getter, and a provider has to be set whole to carry the group, so the
 * defaults are restated here and everything else about the channels stays the SDK's; a drift-guard
 * test pins each mirror to the SDK's own field. With an emulator endpoint the client is handed one
 * plaintext channel opened here instead, carrying the same channel settings: the SDK's own provider
 * cannot dial a {@code dns:///} target (ADR-0208).
 *
 * <p>Neither {@code Publisher} nor {@code Subscriber} is a gax {@code BackgroundResource}, so the
 * hold cannot travel with the client as a {@link GrpcEventLoops.LeasedClient}; the client's owner
 * closes the {@link Transport} once the client has shut down or been given up on.
 */
@Internal
public final class PubSubTransports {

    private PubSubTransports() {}

    /**
     * Opens the transport for one publisher.
     *
     * @param emulatorEndpoint the emulator, or {@code null} for Pub/Sub itself
     * @return the transport; the caller closes it after the publisher
     * @throws IOException if the emulator channel cannot be opened
     */
    public static Transport openForPublisher(@Nullable EmulatorEndpoint emulatorEndpoint)
            throws IOException {
        return open(publisherDefaults(), emulatorEndpoint);
    }

    /**
     * Opens the transport for one subscriber. In production the subscriber sizes the channel pool
     * from its parallel pull count itself; an emulator's one channel carries every pull stream.
     *
     * @param emulatorEndpoint the emulator, or {@code null} for Pub/Sub itself
     * @return the transport; the caller closes it after the subscriber
     * @throws IOException if the emulator channel cannot be opened
     */
    public static Transport openForSubscriber(@Nullable EmulatorEndpoint emulatorEndpoint)
            throws IOException {
        return open(subscriberDefaults(), emulatorEndpoint);
    }

    private static Transport open(
            InstantiatingGrpcChannelProvider.Builder defaults,
            @Nullable EmulatorEndpoint emulatorEndpoint)
            throws IOException {
        if (emulatorEndpoint == null) {
            GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
            try {
                return new Transport(lease.applyTo(defaults).build(), lease::close);
            } catch (RuntimeException | Error e) {
                lease.close();
                throw e;
            }
        }
        GrpcEventLoops.LeasedClient<GrpcTransportChannel> channel =
                EmulatorChannels.openLeasedPlaintextChannel(defaults, emulatorEndpoint);
        return new Transport(
                EmulatorChannels.fixedProvider(channel),
                () -> {
                    // Whatever the client left in flight is cancelled at once, as the sink always
                    // has; the holder then waits for the channel and releases its lease, even if
                    // the cancellation threw; the first failure is the one reported.
                    try {
                        Closers.closeAll(() -> channel.client().shutdownNow(), channel);
                    } catch (Exception e) {
                        // Neither step declares a checked exception, so this is the unchecked
                        // one closeAll collected; an Error leaves closeAll as itself.
                        ExceptionUtils.rethrow(e);
                    }
                });
    }

    /**
     * Mirror of {@code Publisher.Builder}'s default channel provider: one channel per CPU, on the
     * topic admin API's transport defaults.
     */
    @VisibleForTesting
    static InstantiatingGrpcChannelProvider.Builder publisherDefaults() {
        return TopicAdminSettings.defaultGrpcTransportProviderBuilder().setChannelsPerCpu(1);
    }

    /**
     * Mirror of {@code Subscriber.Builder}'s default channel provider: the subscription admin API's
     * transport defaults with a 20 MiB inbound message limit, a 4 MiB inbound metadata limit and a
     * five-minute keep-alive.
     */
    @VisibleForTesting
    static InstantiatingGrpcChannelProvider.Builder subscriberDefaults() {
        return SubscriptionAdminSettings.defaultGrpcTransportProviderBuilder()
                .setMaxInboundMessageSize(20 * 1024 * 1024)
                .setMaxInboundMetadataSize(4 * 1024 * 1024)
                .setKeepAliveTimeDuration(Duration.ofMinutes(5));
    }

    /**
     * A client's channel provider and its hold on the event-loop group. Closing it releases the
     * hold, and for an emulator also shuts the channel down; close it once the client has shut down
     * or been given up on, since the group stops under any channel still open on it. Closing it
     * repeatedly releases the lease only once.
     */
    public static final class Transport implements AutoCloseable {

        private final TransportChannelProvider channelProvider;
        private final Runnable release;

        private Transport(TransportChannelProvider channelProvider, Runnable release) {
            this.channelProvider = channelProvider;
            this.release = Preconditions.checkNotNull(release);
        }

        /**
         * Returns the provider to hand the client's builder.
         *
         * @return the provider
         */
        public TransportChannelProvider channelProvider() {
            return channelProvider;
        }

        @Override
        public void close() {
            release.run();
        }
    }
}
