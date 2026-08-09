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

package io.github.flink.gcp.connector.base.rpc;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

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
 * rather than a spelling. {@link #plaintextProvider} yields an instantiating provider, which the
 * client creates a channel from and closes with itself; {@link #openPlaintextChannel} plus {@link
 * #fixedProvider} hand the client a channel the caller made, which the client does <em>not</em>
 * close — so a caller taking that route owns the shutdown, and shares one channel across several
 * clients if it wants to.
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
     * Opens a plaintext channel to an emulator, for a caller that owns the channel itself.
     *
     * <p>The caller shuts it down: pair it with {@link #fixedProvider}, whose provider the client
     * will not close.
     *
     * @param endpoint the emulator to dial
     * @return the channel, open
     */
    public static ManagedChannel openPlaintextChannel(EmulatorEndpoint endpoint) {
        Preconditions.checkNotNull(endpoint, "endpoint must not be null");
        return ManagedChannelBuilder.forTarget(endpoint.getTarget()).usePlaintext().build();
    }

    /**
     * Wraps a caller-owned channel as a fixed transport channel provider.
     *
     * <p>A fixed provider is <em>not</em> auto-closed by the client it is given to, which is the
     * property a caller wants when it shuts the channel down on its own schedule or shares one
     * channel across clients — and the trap when it does not, since nothing then closes it.
     *
     * @param channel the channel the caller owns
     * @return the provider
     */
    public static FixedTransportChannelProvider fixedProvider(ManagedChannel channel) {
        Preconditions.checkNotNull(channel, "channel must not be null");
        return FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    }
}
