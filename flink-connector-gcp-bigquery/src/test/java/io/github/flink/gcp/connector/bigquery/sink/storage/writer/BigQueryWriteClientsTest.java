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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteGrpc;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.GetWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.bigquery.EmulatorConnections;
import io.github.flink.gcp.connector.testutils.EventLoopObserver;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BigQueryWriteClients}. */
class BigQueryWriteClientsTest {

    @TempDir Path tempDir;

    @Test
    void productionSettingsCarryTheConfiguredCredentials() throws Exception {
        BigQueryWriteSettings settings;
        try (GrpcEventLoops.Lease lease = GrpcEventLoops.acquire()) {
            settings =
                    BigQueryWriteClients.productionSettings(
                            ServiceAccountKeyFiles.create(tempDir).toString(), lease);
        }

        assertThat(settings.getCredentialsProvider().getCredentials())
                .isInstanceOf(ServiceAccountCredentials.class)
                .extracting(
                        credentials -> ((ServiceAccountCredentials) credentials).getClientEmail())
                .isEqualTo(ServiceAccountKeyFiles.CLIENT_EMAIL);
    }

    @Test
    @Timeout(60)
    void anEmulatorClientReceivesAResponseAboveGrpcsDefaultInboundLimit() throws Exception {
        // The API's own transport builder raises the limit to Integer.MAX_VALUE; a channel built
        // from a bare InstantiatingGrpcChannelProvider.newBuilder() keeps gRPC's 4 MiB default,
        // which fails only on the emulator and only once a response grows past it.
        String large = "x".repeat(5 << 20);
        Server server =
                NettyServerBuilder.forAddress(
                                new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0))
                        .addService(
                                new BigQueryWriteGrpc.BigQueryWriteImplBase() {
                                    @Override
                                    public void getWriteStream(
                                            GetWriteStreamRequest request,
                                            StreamObserver<WriteStream> response) {
                                        response.onNext(
                                                WriteStream.newBuilder()
                                                        .setLocation(large)
                                                        .build());
                                        response.onCompleted();
                                    }
                                })
                        .build()
                        .start();
        try (GrpcEventLoops.LeasedClient<BigQueryWriteClient> client =
                BigQueryWriteClients.forEmulator(
                        EmulatorEndpoint.parse(
                                "127.0.0.1:" + server.getPort(), "emulatorEndpoint"))) {
            // One attempt under a 30-second deadline: the API's own policy retries
            // RESOURCE_EXHAUSTED for ten minutes, and gax waits for a unary call uninterruptibly,
            // so the test's own timeout could not end it.
            BigQueryWriteClient writeClient = client.client();
            WriteStream stream =
                    writeClient
                            .getWriteStreamCallable()
                            .call(
                                    GetWriteStreamRequest.getDefaultInstance(),
                                    GrpcCallContext.createDefault()
                                            .withRetrySettings(
                                                    writeClient
                                                            .getSettings()
                                                            .getWriteStreamSettings()
                                                            .getRetrySettings()
                                                            .toBuilder()
                                                            .setMaxAttempts(1)
                                                            .setInitialRpcTimeoutDuration(
                                                                    Duration.ofSeconds(30))
                                                            .setMaxRpcTimeoutDuration(
                                                                    Duration.ofSeconds(30))
                                                            .setTotalTimeoutDuration(
                                                                    Duration.ofSeconds(30))
                                                            .build()));
            assertThat(stream.getLocation().length()).isEqualTo(large.length());
        } finally {
            server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest(name = "target scheme \"{0}\"")
    @ValueSource(strings = {"", "dns:///"})
    void anEmulatorClientRunsItsOwnPlaintextChannelOnTheEventLoopGroup(String scheme)
            throws Exception {
        // Both target forms EmulatorEndpoint accepts: host:port, and a scheme-prefixed gRPC target,
        // which gax's own provider cannot dial (#2267).
        try (ServerSocket server = new ServerSocket(0)) {
            server.setSoTimeout(10_000);
            String target = scheme + "127.0.0.1:" + server.getLocalPort();
            assertThat(GrpcEventLoopThreads.awaitNone())
                    .as("no group thread before the client opens")
                    .isTrue();
            assertThat(GrpcEventLoops.isRunning())
                    .as("no lease open before the client opens")
                    .isFalse();
            EventLoopObserver.LoopStarts starts = EmulatorConnections.loopStartsFromNow();
            try (GrpcEventLoops.LeasedClient<BigQueryWriteClient> client =
                    BigQueryWriteClients.forEmulator(
                            EmulatorEndpoint.parse(target, "emulatorEndpoint"))) {
                BigQueryWriteSettings settings = client.client().getSettings();
                assertThat(settings.getCredentialsProvider())
                        .isInstanceOf(NoCredentialsProvider.class);
                EmulatorConnections.assertPlaintextOnTheGroup(
                        settings.getTransportChannelProvider(), server, target, starts);
            }
            assertThat(GrpcEventLoops.isRunning())
                    .as("closing the client released both leases")
                    .isFalse();
        }
    }

    /**
     * The production client's channels run on the connector's event-loop group (#2237). The SDK's
     * default provider carries no configurator, so one here is the group's.
     */
    @Test
    void theProductionSettingsRunOnTheEventLoopGroup() throws IOException {
        BigQueryWriteSettings settings;
        try (GrpcEventLoops.Lease lease = GrpcEventLoops.acquire()) {
            settings = BigQueryWriteClients.productionSettings(null, lease);
        }

        assertThat(provider(settings).toBuilder().getChannelConfigurator()).isNotNull();
    }

    private static InstantiatingGrpcChannelProvider provider(BigQueryWriteSettings settings) {
        // The cast is safe by construction: the production settings keep the SDK's default
        // provider type.
        return (InstantiatingGrpcChannelProvider) settings.getTransportChannelProvider();
    }
}
