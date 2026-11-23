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

package io.github.flink.gcp.connector.base.rpc;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.core.BackgroundResource;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Transport channel providers pointing a gRPC client at an emulator.
 *
 * <p>An emulator here terminates no TLS, so a client reaches one only over a plaintext channel —
 * and the settings' own {@code setEndpoint} does not deliver that: it names the host the client
 * dials, while the channel it builds stays TLS. The plaintext has to come from the transport
 * channel provider, which is what these methods build. The other half of the pair, {@code
 * setCredentialsProvider(NoCredentialsProvider.create())}, stays at each call site: the setter for
 * it lives on {@code ClientSettings.Builder}, {@code Publisher.Builder} and {@code
 * Subscriber.Builder}, three types that share no supertype to write one signature against.
 *
 * <p>Which method a caller wants is decided by who owns the channel, and that is a real difference
 * rather than a spelling. {@link #plaintextProvider(InstantiatingGrpcChannelProvider.Builder,
 * EmulatorEndpoint)} yields an instantiating provider, which the client creates a channel from and
 * closes with itself. {@link #openLeasedPlaintextChannel(InstantiatingGrpcChannelProvider.Builder,
 * EmulatorEndpoint)} plus {@link #fixedProvider(GrpcEventLoops.LeasedClient)} hand the client a
 * channel the caller made, on {@link GrpcEventLoops}' group (ADR-0208), which the client does
 * <em>not</em> close, so a caller taking that route owns the shutdown; {@link
 * #openLeasedClient(InstantiatingGrpcChannelProvider.Builder, EmulatorEndpoint,
 * EmulatorClientFactory)} takes it for a gax client and closes the two in order. The second route
 * is the one to take. The first hands the target to gax, which parses it as {@code host:port}, so a
 * {@code dns:///} target fails, and builds TLS credentials when a client certificate is configured,
 * which the plaintext configurator cannot override (ADR-0208); it stays only for the BigQuery and
 * Datastore emulator arms until they move (#2266, #2267).
 *
 * <p>Nothing here reaches a client library, so one signature covers Pub/Sub, BigQuery, Cloud Tasks
 * and whatever arrives next. Products whose SDK offers its own emulator entry point use that
 * instead and never come here — Bigtable's {@code newBuilderForEmulator(host, port)} and Spanner's
 * {@code setEmulatorHost} both switch the channel and the credentials in one call.
 */
@Internal
public final class EmulatorChannels {

    private EmulatorChannels() {}

    /**
     * Points an API's own transport provider at an emulator, over plaintext.
     *
     * <p>The builder is a parameter rather than something built here because each API seeds its own
     * with defaults a bare {@code InstantiatingGrpcChannelProvider.newBuilder()} does not have:
     * BigQuery Storage raises the maximum inbound message size to {@link Integer#MAX_VALUE} and
     * Pub/Sub's subscriber stub to 20 MiB, so building one from scratch would silently run the
     * emulator path at gRPC's 4 MiB default. Pass {@code
     * <Api>Settings.defaultGrpcTransportProviderBuilder()}.
     *
     * @param providerBuilder the API's default gRPC transport provider builder
     * @param endpoint the emulator to dial
     * @return the provider, built; the client it is given to creates and closes the channel
     */
    public static InstantiatingGrpcChannelProvider plaintextProvider(
            InstantiatingGrpcChannelProvider.Builder providerBuilder, EmulatorEndpoint endpoint) {
        Preconditions.checkNotNull(providerBuilder, "providerBuilder must not be null");
        Preconditions.checkNotNull(endpoint, "endpoint must not be null");
        return providerBuilder
                .setEndpoint(endpoint.getTarget())
                .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
                .build();
    }

    /**
     * Creates a client on a plaintext channel to an emulator, both on the connector's gRPC
     * event-loop group: the channel opened as {@link
     * #openLeasedPlaintextChannel(InstantiatingGrpcChannelProvider.Builder, EmulatorEndpoint)}
     * opens it, handed to the factory as a fixed provider. Closing the returned holder closes the
     * client within {@link GrpcEventLoops.LeasedClient}'s bounds, then the channel; a failure to
     * create the client closes the channel.
     *
     * <p>This is the emulator arm of a gax client that owns no channel of its own; a client that is
     * not a {@code BackgroundResource} opens the channel itself.
     *
     * @param transportDefaults the API's default gRPC transport provider builder
     * @param endpoint the emulator to dial
     * @param factory creates the client on the channel's provider, with no credentials
     * @param <C> the client type
     * @return the client, holding its channel
     * @throws IOException if the factory does
     */
    public static <C extends BackgroundResource> GrpcEventLoops.LeasedClient<C> openLeasedClient(
            InstantiatingGrpcChannelProvider.Builder transportDefaults,
            EmulatorEndpoint endpoint,
            EmulatorClientFactory<C> factory)
            throws IOException {
        Preconditions.checkNotNull(factory, "factory must not be null");
        GrpcEventLoops.LeasedClient<GrpcTransportChannel> channel =
                openLeasedPlaintextChannel(transportDefaults, endpoint);
        try {
            // The channel's own lease covers it; the client's only gives it the bounded close.
            return GrpcEventLoops.open(lease -> factory.create(fixedProvider(channel)), channel);
        } catch (IOException | RuntimeException | Error e) {
            Closers.closeAllSuppressing(e, channel);
            throw e;
        }
    }

    /**
     * Opens a plaintext channel to an emulator on the connector's gRPC event-loop group, for a
     * caller that owns the channel itself, carrying over the API's own channel defaults.
     *
     * <p>The channel takes the maximum inbound message and metadata sizes and the keep-alive
     * settings the API's provider builder carries, for the reason {@link
     * #plaintextProvider(InstantiatingGrpcChannelProvider.Builder, EmulatorEndpoint)} takes the
     * builder: Pub/Sub's subscriber stub raises the inbound limit to 20 MiB and BigQuery Storage to
     * {@link Integer#MAX_VALUE}, and a channel built without them would run the emulator path at
     * gRPC's 4 MiB default. Its endpoint, pool and configurator are not read: the target is the
     * emulator's, the caller owns exactly one channel, and the group is applied here.
     *
     * <p>The returned holder covers the channel with a lease: closing it shuts the channel down,
     * waits for it to terminate and then releases the lease. Close the clients using the channel
     * first, and pass it to them through {@link #fixedProvider(GrpcEventLoops.LeasedClient)}.
     *
     * @param transportDefaults the API's default gRPC transport provider builder, such as {@code
     *     <Api>Settings.defaultGrpcTransportProviderBuilder()}
     * @param endpoint the emulator to dial
     * @return the channel, open, with its lease
     * @throws IOException never in practice; {@link
     *     GrpcEventLoops#open(GrpcEventLoops.ClientFactory)} declares it
     */
    public static GrpcEventLoops.LeasedClient<GrpcTransportChannel> openLeasedPlaintextChannel(
            InstantiatingGrpcChannelProvider.Builder transportDefaults, EmulatorEndpoint endpoint)
            throws IOException {
        Preconditions.checkNotNull(transportDefaults, "transportDefaults must not be null");
        Preconditions.checkNotNull(endpoint, "endpoint must not be null");
        NettyChannelBuilder builder =
                NettyChannelBuilder.forTarget(endpoint.getTarget()).usePlaintext();
        applyTransportDefaults(transportDefaults, builder);
        return GrpcEventLoops.open(
                lease -> GrpcTransportChannel.create(lease.applyTo(builder).build()));
    }

    /** Copies the settings gax would put on a channel it built from the provider. */
    @VisibleForTesting
    static void applyTransportDefaults(
            InstantiatingGrpcChannelProvider.Builder transportDefaults,
            NettyChannelBuilder builder) {
        Integer maxInboundMessageSize = transportDefaults.getMaxInboundMessageSize();
        if (maxInboundMessageSize != null) {
            builder.maxInboundMessageSize(maxInboundMessageSize);
        }
        Integer maxInboundMetadataSize = transportDefaults.getMaxInboundMetadataSize();
        if (maxInboundMetadataSize != null) {
            builder.maxInboundMetadataSize(maxInboundMetadataSize);
        }
        Duration keepAliveTime = transportDefaults.getKeepAliveTimeDuration();
        if (keepAliveTime != null) {
            builder.keepAliveTime(keepAliveTime.toMillis(), TimeUnit.MILLISECONDS);
        }
        Duration keepAliveTimeout = transportDefaults.getKeepAliveTimeoutDuration();
        if (keepAliveTimeout != null) {
            builder.keepAliveTimeout(keepAliveTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        Boolean keepAliveWithoutCalls = transportDefaults.getKeepAliveWithoutCalls();
        if (keepAliveWithoutCalls != null) {
            builder.keepAliveWithoutCalls(keepAliveWithoutCalls);
        }
    }

    /**
     * Wraps a leased caller-owned channel as a fixed transport channel provider, which the client
     * it is given to does not close; the holder does.
     *
     * @param channel the channel from {@link
     *     #openLeasedPlaintextChannel(InstantiatingGrpcChannelProvider.Builder, EmulatorEndpoint)}
     * @return the provider
     */
    public static FixedTransportChannelProvider fixedProvider(
            GrpcEventLoops.LeasedClient<GrpcTransportChannel> channel) {
        Preconditions.checkNotNull(channel, "channel must not be null");
        return FixedTransportChannelProvider.create(channel.client());
    }

    /** Creates a client on an emulator channel's provider. */
    @FunctionalInterface
    public interface EmulatorClientFactory<C extends BackgroundResource> {

        /**
         * Creates the client.
         *
         * @param channel the fixed provider around the emulator channel
         * @return the client
         * @throws IOException if the client cannot be created
         */
        C create(TransportChannelProvider channel) throws IOException;
    }
}
