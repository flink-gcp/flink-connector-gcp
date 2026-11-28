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

import com.google.api.gax.core.BackgroundResource;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import io.github.flink.gcp.connector.testutils.EventLoopObserver;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link EmulatorChannels}. */
class EmulatorChannelsTest {

    private static final EmulatorEndpoint ENDPOINT =
            EmulatorEndpoint.parse("localhost:8086", "emulatorEndpoint");

    @AfterEach
    void noLeaseIsLeftOpen() {
        assertThat(GrpcEventLoops.isRunning()).as("an event-loop lease was left open").isFalse();
    }

    @ParameterizedTest(name = "target scheme \"{0}\"")
    @ValueSource(strings = {"", "dns:///"})
    void aLeasedChannelDialsTheEndpointOnTheGroupAndReleasesItOnClose(String scheme)
            throws Exception {
        EventLoopObserver.LoopStarts starts =
                EventLoopObserver.loopStartsFromNow(
                        GrpcEventLoops::isRunning, GrpcEventLoops::loopsStartedWhileRunning);
        try (ServerSocket server = new ServerSocket(0)) {
            String hostPort = "127.0.0.1:" + server.getLocalPort();
            // The dns:/// form is the one gax's own provider cannot dial (it splits the target at
            // its last colon), and the reason a connector opens its emulator channel here.
            GrpcEventLoops.LeasedClient<GrpcTransportChannel> leased =
                    EmulatorChannels.openLeasedPlaintextChannel(
                            anApiDefaultBuilder(),
                            EmulatorEndpoint.parse(scheme + hostPort, "emulatorEndpoint"));
            try {
                FixedTransportChannelProvider provider = EmulatorChannels.fixedProvider(leased);
                // The client the provider is given to keeps its hands off the channel: the holder
                // closes it, after the clients using it.
                assertThat(provider.shouldAutoClose()).isFalse();
                assertThat(provider.getTransportChannel()).isSameAs(leased.client());

                // Connecting registers the channel on an event loop, which starts its thread.
                ManagedChannel channel = (ManagedChannel) leased.client().getChannel();
                assertThat(channel.authority()).isEqualTo(hostPort);
                channel.getState(true);
                assertThat(starts.awaitAnyStarted())
                        .as("a group event loop started once the channel connects")
                        .isTrue();
            } finally {
                leased.close();
            }
            assertThat(leased.client().isTerminated()).isTrue();
            assertThat(GrpcEventLoops.isRunning()).isFalse();
        }
    }

    @Test
    void aLeasedClientClosesItsClientBeforeItsChannel() throws Exception {
        List<String> closed = new ArrayList<>();
        AtomicReference<GrpcTransportChannel> channelSeen = new AtomicReference<>();
        GrpcEventLoops.LeasedClient<RecordingClient> leased =
                EmulatorChannels.openLeasedClient(
                        anApiDefaultBuilder(),
                        ENDPOINT,
                        provider -> {
                            assertThat(provider.shouldAutoClose())
                                    .as("the client does not close the channel; the holder does")
                                    .isFalse();
                            GrpcTransportChannel channel =
                                    (GrpcTransportChannel) provider.getTransportChannel();
                            channelSeen.set(channel);
                            return new RecordingClient(
                                    () ->
                                            closed.add(
                                                    channel.isShutdown()
                                                            ? "client after channel"
                                                            : "client"));
                        });
        assertThat(GrpcEventLoops.isRunning()).isTrue();

        leased.close();

        assertThat(closed).containsExactly("client");
        assertThat(channelSeen.get().isTerminated()).as("the channel closes after it").isTrue();
        assertThat(GrpcEventLoops.isRunning()).as("both leases released").isFalse();
    }

    @Test
    void aClientWhoseShutdownFailsStillHasItsChannelClosed() throws Exception {
        AtomicReference<GrpcTransportChannel> channelSeen = new AtomicReference<>();
        NoClassDefFoundError broken = new NoClassDefFoundError("the client could not shut down");
        GrpcEventLoops.LeasedClient<RecordingClient> leased =
                EmulatorChannels.openLeasedClient(
                        anApiDefaultBuilder(),
                        ENDPOINT,
                        provider -> {
                            channelSeen.set((GrpcTransportChannel) provider.getTransportChannel());
                            return new RecordingClient(
                                    () -> {
                                        throw broken;
                                    });
                        });

        // The client's failure is the one reported, and the channel and both leases go anyway.
        assertThatThrownBy(leased::close).isSameAs(broken);
        assertThat(channelSeen.get().isTerminated()).isTrue();
        assertThat(GrpcEventLoops.isRunning()).as("both leases released").isFalse();
    }

    @Test
    void aClientThatCannotBeCreatedClosesItsChannel() {
        IOException refused = new IOException("no client");

        assertThatThrownBy(
                        () ->
                                EmulatorChannels.openLeasedClient(
                                        anApiDefaultBuilder(),
                                        ENDPOINT,
                                        provider -> {
                                            throw refused;
                                        }))
                .isSameAs(refused);
        assertThat(GrpcEventLoops.isRunning()).as("the channel's lease released").isFalse();
    }

    @Test
    void aLeasedChannelCarriesTheApisTransportDefaults() throws Exception {
        NettyChannelBuilder builder = NettyChannelBuilder.forTarget("localhost:1");
        EmulatorChannels.applyTransportDefaults(
                anApiDefaultBuilder()
                        .setMaxInboundMetadataSize(4 << 20)
                        .setKeepAliveTimeDuration(Duration.ofMinutes(5))
                        .setKeepAliveTimeoutDuration(Duration.ofSeconds(30))
                        .setKeepAliveWithoutCalls(true),
                builder);

        // Read back through the builder's fields: it has no getters, and these are the settings
        // gax would have put on a channel it built from the same provider.
        assertThat(field(builder, "maxInboundMessageSize")).isEqualTo(20_971_520);
        assertThat(field(builder, "maxHeaderListSize")).isEqualTo(4 << 20);
        assertThat(field(builder, "keepAliveTimeNanos")).isEqualTo(Duration.ofMinutes(5).toNanos());
        assertThat(field(builder, "keepAliveTimeoutNanos"))
                .isEqualTo(Duration.ofSeconds(30).toNanos());
        assertThat(field(builder, "keepAliveWithoutCalls")).isEqualTo(true);
    }

    @Test
    void aLeasedChannelKeepsGrpcsDefaultsWhereTheApiSetsNone() throws Exception {
        NettyChannelBuilder untouched = NettyChannelBuilder.forTarget("localhost:1");
        NettyChannelBuilder builder = NettyChannelBuilder.forTarget("localhost:1");
        EmulatorChannels.applyTransportDefaults(
                InstantiatingGrpcChannelProvider.newBuilder(), builder);

        for (String name :
                new String[] {
                    "maxInboundMessageSize",
                    "maxHeaderListSize",
                    "keepAliveTimeNanos",
                    "keepAliveTimeoutNanos",
                    "keepAliveWithoutCalls"
                }) {
            assertThat(field(builder, name)).as(name).isEqualTo(field(untouched, name));
        }
    }

    @Test
    void rejectsNull() {
        assertThatThrownBy(
                        () ->
                                EmulatorChannels.openLeasedClient(
                                        anApiDefaultBuilder(), ENDPOINT, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("factory must not be null");
        assertThatThrownBy(
                        () ->
                                EmulatorChannels.openLeasedPlaintextChannel(
                                        anApiDefaultBuilder(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("endpoint must not be null");
        assertThatThrownBy(() -> EmulatorChannels.openLeasedPlaintextChannel(null, ENDPOINT))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("transportDefaults must not be null");
        assertThatThrownBy(() -> EmulatorChannels.fixedProvider(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("channel must not be null");
    }

    private static Object field(NettyChannelBuilder builder, String name) throws Exception {
        Field field = NettyChannelBuilder.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(builder);
    }

    /**
     * Stands in for an {@code <Api>Settings.defaultGrpcTransportProviderBuilder()}: base depends on
     * no client library, so the property under test — that whatever the caller seeded survives — is
     * pinned with a seeded builder rather than with one API's.
     */
    private static InstantiatingGrpcChannelProvider.Builder anApiDefaultBuilder() {
        return InstantiatingGrpcChannelProvider.newBuilder().setMaxInboundMessageSize(20_971_520);
    }

    /** A client with no channel of its own, recording when it is shut down. */
    private static final class RecordingClient implements BackgroundResource {

        private final Runnable onShutdown;
        private boolean shutdown;

        RecordingClient(Runnable onShutdown) {
            this.onShutdown = onShutdown;
        }

        @Override
        public void shutdown() {
            if (!shutdown) {
                shutdown = true;
                onShutdown.run();
            }
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public void shutdownNow() {
            shutdown();
        }

        @Override
        public boolean awaitTermination(long duration, TimeUnit unit) {
            return shutdown;
        }

        @Override
        public void close() {
            shutdown();
        }
    }
}
