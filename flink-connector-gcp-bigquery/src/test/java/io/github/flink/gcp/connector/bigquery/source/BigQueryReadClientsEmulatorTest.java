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

package io.github.flink.gcp.connector.bigquery.source;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadGrpc;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.bigquery.EmulatorConnections;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link BigQueryReadClients}' emulator clients, apart from {@link
 * BigQueryReadClientsTest} because each starts with the event-loop group stopped, which that
 * class's per-test lease would keep running.
 */
class BigQueryReadClientsEmulatorTest {

    private static final Duration RPC_DEADLINE = Duration.ofSeconds(30);

    // Both target forms EmulatorEndpoint accepts: host:port, and a scheme-prefixed gRPC target,
    // which gax's own provider cannot dial (#2267).

    @ParameterizedTest(name = "target scheme \"{0}\"")
    @ValueSource(strings = {"", "dns:///"})
    void aSessionClientRunsItsOwnPlaintextChannelOnTheEventLoopGroup(String scheme)
            throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            server.setSoTimeout(10_000);
            String target = scheme + "127.0.0.1:" + server.getLocalPort();
            assertThat(GrpcEventLoopThreads.awaitNone())
                    .as("no group thread before the client opens")
                    .isTrue();
            assertThat(GrpcEventLoops.isRunning())
                    .as("no lease open before the client opens")
                    .isFalse();
            try (GrpcEventLoops.LeasedClient<BigQueryReadClient> client =
                    BigQueryReadClients.createForSessions(
                            null, EmulatorEndpoint.parse(target, "emulatorEndpoint"))) {
                BigQueryReadSettings settings = client.client().getSettings();
                assertThat(settings.getCredentialsProvider())
                        .isInstanceOf(NoCredentialsProvider.class);
                EmulatorConnections.assertPlaintextOnTheGroup(
                        settings.getTransportChannelProvider(), server, target);
            }
            assertThat(GrpcEventLoops.isRunning())
                    .as("closing the client released both leases")
                    .isFalse();
        }
    }

    @ParameterizedTest(name = "target scheme \"{0}\"")
    @ValueSource(strings = {"", "dns:///"})
    void aReadingClientRunsItsOwnPlaintextChannelOnTheEventLoopGroupWithItsRetryBound(String scheme)
            throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            server.setSoTimeout(10_000);
            String target = scheme + "127.0.0.1:" + server.getLocalPort();
            assertThat(GrpcEventLoopThreads.awaitNone())
                    .as("no group thread before the client opens")
                    .isTrue();
            assertThat(GrpcEventLoops.isRunning())
                    .as("no lease open before the client opens")
                    .isFalse();
            try (GrpcEventLoops.LeasedClient<BigQueryReadClient> client =
                    BigQueryReadClients.createForReads(
                            null,
                            EmulatorEndpoint.parse(target, "emulatorEndpoint"),
                            7,
                            () -> {})) {
                BigQueryReadSettings settings = client.client().getSettings();
                assertThat(settings.getCredentialsProvider())
                        .isInstanceOf(NoCredentialsProvider.class);
                // The emulator arm bounds the client's own ReadRows retry and wires the listener
                // the same way the production arm does.
                assertThat(settings.readRowsSettings().getRetrySettings().getMaxAttempts())
                        .isEqualTo(7);
                assertThat(settings.getReadRowsRetryAttemptListener()).isNotNull();
                EmulatorConnections.assertPlaintextOnTheGroup(
                        settings.getTransportChannelProvider(), server, target);
            }
            assertThat(GrpcEventLoops.isRunning())
                    .as("closing the client released both leases")
                    .isFalse();
        }
    }

    @Test
    @Timeout(60)
    void bothEmulatorClientsReceiveAResponseAboveGrpcsDefaultInboundLimit() throws Exception {
        // Storage Read responses carry whole row batches, and the API's own transport builder
        // raises the limit to Integer.MAX_VALUE for that reason. A channel built from a bare
        // InstantiatingGrpcChannelProvider.newBuilder() keeps gRPC's 4 MiB default, which fails
        // only on the emulator and only once a response grows past it.
        String large = "x".repeat(5 << 20);
        Server server =
                NettyServerBuilder.forAddress(
                                new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0))
                        .addService(
                                new BigQueryReadGrpc.BigQueryReadImplBase() {
                                    @Override
                                    public void createReadSession(
                                            CreateReadSessionRequest request,
                                            StreamObserver<ReadSession> response) {
                                        response.onNext(
                                                ReadSession.newBuilder().setTable(large).build());
                                        response.onCompleted();
                                    }
                                })
                        .build()
                        .start();
        try {
            EmulatorEndpoint endpoint =
                    EmulatorEndpoint.parse("127.0.0.1:" + server.getPort(), "emulatorEndpoint");
            try (GrpcEventLoops.LeasedClient<BigQueryReadClient> client =
                    BigQueryReadClients.createForSessions(null, endpoint)) {
                assertThat(sessionTableLength(client.client()))
                        .as("the session client's response")
                        .isEqualTo(large.length());
            }
            try (GrpcEventLoops.LeasedClient<BigQueryReadClient> client =
                    BigQueryReadClients.createForReads(null, endpoint, 7, null)) {
                assertThat(sessionTableLength(client.client()))
                        .as("the reading client's response")
                        .isEqualTo(large.length());
            }
        } finally {
            server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static int sessionTableLength(BigQueryReadClient client) {
        // One attempt under a 30-second deadline: gax waits for a unary call uninterruptibly, so
        // the test's own timeout cannot end the API's ten-minute retry budget for it.
        return client.createReadSessionCallable()
                .call(
                        CreateReadSessionRequest.getDefaultInstance(),
                        GrpcCallContext.createDefault()
                                .withRetrySettings(
                                        client
                                                .getSettings()
                                                .createReadSessionSettings()
                                                .getRetrySettings()
                                                .toBuilder()
                                                .setMaxAttempts(1)
                                                .setInitialRpcTimeoutDuration(RPC_DEADLINE)
                                                .setMaxRpcTimeoutDuration(RPC_DEADLINE)
                                                .setTotalTimeoutDuration(RPC_DEADLINE)
                                                .build()))
                .getTable()
                .length();
    }
}
