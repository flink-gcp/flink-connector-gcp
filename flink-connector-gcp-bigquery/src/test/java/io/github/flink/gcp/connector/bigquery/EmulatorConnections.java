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

package io.github.flink.gcp.connector.bigquery;

import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.TransportChannelProvider;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.grpc.ManagedChannel;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks what an emulator client's channel dials, for the Storage Read and Write clients. */
public final class EmulatorConnections {

    private EmulatorConnections() {}

    /**
     * Asserts that a client's transport is a channel the client does not close, that connecting it
     * reaches {@code server} over plaintext, and that a thread of the connector's event-loop group
     * serves it.
     *
     * @param provider the client's transport channel provider
     * @param server the socket the emulator target points at
     * @param target the emulator target, for the failure message
     */
    public static void assertPlaintextOnTheGroup(
            TransportChannelProvider provider, ServerSocket server, String target)
            throws IOException, InterruptedException {
        assertThat(provider.shouldAutoClose())
                .as("the client does not close the caller-owned channel")
                .isFalse();
        ManagedChannel channel =
                (ManagedChannel)
                        ((GrpcTransportChannel) provider.getTransportChannel()).getChannel();
        channel.getState(true);
        // The connection reaching this socket, opening with HTTP/2's cleartext preface rather than
        // a TLS handshake, shows the target resolved to it and the channel is plaintext; the
        // thread name shows which group serves it.
        try (Socket accepted = server.accept()) {
            accepted.setSoTimeout(10_000);
            byte[] preface = new byte[3];
            accepted.getInputStream().readNBytes(preface, 0, 3);
            assertThat(new String(preface, StandardCharsets.US_ASCII))
                    .as("the first bytes %s sends", target)
                    .isEqualTo("PRI");
        }
        assertThat(GrpcEventLoopThreads.awaitAlive())
                .as("a %s* thread serves %s", GrpcEventLoopThreads.PREFIX, target)
                .isTrue();
    }
}
