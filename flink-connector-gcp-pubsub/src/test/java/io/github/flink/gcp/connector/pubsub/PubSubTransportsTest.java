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

import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.testutils.EventLoopObserver;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PubSubTransports}. */
class PubSubTransportsTest {

    /**
     * A group left running by an earlier class in this reused fork would make the thread checks
     * below pass or fail for the wrong reason, so it is ruled out first.
     */
    @BeforeEach
    void noGroupIsRunning() throws InterruptedException {
        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread before the test", GrpcEventLoopThreads.PREFIX)
                .isTrue();
    }

    @AfterEach
    void noLeaseIsLeftOpen() {
        assertThat(GrpcEventLoops.isRunning()).as("an event-loop lease was left open").isFalse();
    }

    /**
     * Pins the mirrors to the providers the SDK's builders hold by default (private fields), so an
     * SDK upgrade that changes one fails loudly here instead of the connector's channels silently
     * diverging from the SDK's.
     */
    @Test
    void theMirrorsMatchTheSdksDefaultProviders() throws Exception {
        assertSameChannels(
                PubSubTransports.publisherDefaults().build(),
                defaultProvider(Publisher.newBuilder("projects/p/topics/t")));
        assertSameChannels(
                PubSubTransports.subscriberDefaults().build(),
                defaultProvider(
                        Subscriber.newBuilder(
                                "projects/p/subscriptions/s",
                                (MessageReceiver) (message, reply) -> reply.nack())));
    }

    @Test
    void aProductionTransportBuildsTheSdksChannelsOnTheGroup() throws Exception {
        // The channels connect lazily, so nothing here talks to the service.
        EventLoopObserver.LoopStarts starts = EventLoopChannelChecks.loopStartsFromNow();
        try (PubSubTransports.Transport transport = PubSubTransports.openForPublisher(null)) {
            assertThat(GrpcEventLoops.isRunning()).as("the transport holds a lease").isTrue();
            InstantiatingGrpcChannelProvider provider = instantiating(transport);
            assertThat(provider.getChannelPoolSettings())
                    .as("the lease costs the SDK's pool nothing")
                    .isEqualTo(
                            PubSubTransports.publisherDefaults().build().getChannelPoolSettings());
            EventLoopChannelChecks.assertBuildsChannelsOnTheGroup(provider, starts);
        }
        assertThat(GrpcEventLoops.isRunning()).as("closing released the lease").isFalse();

        starts = EventLoopChannelChecks.loopStartsFromNow();
        try (PubSubTransports.Transport transport = PubSubTransports.openForSubscriber(null)) {
            InstantiatingGrpcChannelProvider provider = instantiating(transport);
            assertThat(provider.toBuilder().getMaxInboundMessageSize()).isEqualTo(20 << 20);
            // The subscriber resizes the pool to its parallel pull count; the copy keeps the lease.
            EventLoopChannelChecks.assertBuildsChannelsOnTheGroup(provider.withPoolSize(4), starts);
        }
    }

    @ParameterizedTest(name = "target scheme \"{0}\"")
    @ValueSource(strings = {"", "dns:///"})
    void anEmulatorTransportHandsTheClientAPlaintextChannelOnTheGroup(String scheme)
            throws Exception {
        // Both target forms EmulatorEndpoint accepts; gax's own provider cannot dial the second.
        try (ServerSocket server = new ServerSocket(0)) {
            server.setSoTimeout(10_000);
            String target = scheme + "127.0.0.1:" + server.getLocalPort();
            GrpcTransportChannel transportChannel;
            EventLoopObserver.LoopStarts starts = EventLoopChannelChecks.loopStartsFromNow();
            try (PubSubTransports.Transport transport =
                    PubSubTransports.openForSubscriber(
                            EmulatorEndpoint.parse(target, "emulatorEndpoint"))) {
                TransportChannelProvider provider = transport.channelProvider();
                assertThat(provider.shouldAutoClose())
                        .as("the client does not close the channel; the transport does")
                        .isFalse();
                transportChannel = (GrpcTransportChannel) provider.getTransportChannel();

                ManagedChannel channel = (ManagedChannel) transportChannel.getChannel();
                channel.getState(true);
                // The connection reaching this socket, opening with HTTP/2's cleartext preface
                // rather than a TLS handshake, shows the target resolved to it and the channel is
                // plaintext; the group's count shows the channel runs on it.
                try (Socket accepted = server.accept()) {
                    accepted.setSoTimeout(10_000);
                    byte[] preface = new byte[3];
                    accepted.getInputStream().readNBytes(preface, 0, 3);
                    assertThat(new String(preface, StandardCharsets.US_ASCII))
                            .as("the first bytes %s sends", target)
                            .isEqualTo("PRI");
                }
                assertThat(starts.awaitAnyStarted())
                        .as("a group event loop started for %s", target)
                        .isTrue();
            }
            assertThat(transportChannel.isTerminated()).isTrue();
            assertThat(GrpcEventLoops.isRunning()).as("closing released the lease").isFalse();
        }
    }

    @Test
    void closingATransportTwiceDoesNothingTheSecondTime() throws Exception {
        PubSubTransports.Transport production = PubSubTransports.openForPublisher(null);
        production.close();
        production.close();
        PubSubTransports.Transport emulator =
                PubSubTransports.openForPublisher(
                        EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"));
        emulator.close();
        emulator.close();
    }

    private static InstantiatingGrpcChannelProvider instantiating(
            PubSubTransports.Transport transport) {
        assertThat(transport.channelProvider())
                .isInstanceOf(InstantiatingGrpcChannelProvider.class);
        return (InstantiatingGrpcChannelProvider) transport.channelProvider();
    }

    private static InstantiatingGrpcChannelProvider defaultProvider(Object builder)
            throws Exception {
        Field field = builder.getClass().getDeclaredField("channelProvider");
        field.setAccessible(true);
        return (InstantiatingGrpcChannelProvider) field.get(builder);
    }

    /**
     * Compares every field of the two providers, so a setting the SDK adds to its default (a
     * DirectPath flag, an interceptor) fails here too: value types by equality, the rest, which gax
     * creates per builder, by class.
     */
    private static void assertSameChannels(
            InstantiatingGrpcChannelProvider mirror, InstantiatingGrpcChannelProvider sdk)
            throws Exception {
        for (Field field : InstantiatingGrpcChannelProvider.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            field.setAccessible(true);
            Object ours = field.get(mirror);
            Object theirs = field.get(sdk);
            if (ours == null || theirs == null || isValue(field.getType())) {
                assertThat(ours).as(field.getName()).isEqualTo(theirs);
            } else {
                assertThat(ours.getClass()).as(field.getName()).isEqualTo(theirs.getClass());
            }
        }
    }

    private static boolean isValue(Class<?> type) {
        return type.isPrimitive()
                || type == String.class
                || Number.class.isAssignableFrom(type)
                || type == Boolean.class
                || type == java.time.Duration.class
                || type == ChannelPoolSettings.class
                || java.util.Map.class.isAssignableFrom(type)
                || java.util.List.class.isAssignableFrom(type);
    }
}
