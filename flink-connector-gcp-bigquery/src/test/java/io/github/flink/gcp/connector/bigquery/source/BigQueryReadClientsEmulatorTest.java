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
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.rpc.RetryInfo;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.bigquery.EmulatorConnections;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                // The emulator arm bounds the client's own ReadRows retry the same way the
                // production arm does; the scripted reads below hold its retry count.
                assertThat(settings.readRowsSettings().getRetrySettings().getMaxAttempts())
                        .isEqualTo(7);
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

    // #2301: the client re-issues a broken ReadRows for statuses it classifies two ways — the
    // configured retryable code (UNAVAILABLE), and its own list (the INTERNAL transport faults, a
    // RESOURCE_EXHAUSTED whose RetryInfo carries a delay). The SDK's retry listener saw only the
    // second.

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"UNAVAILABLE", "INTERNAL_RST_STREAM", "RESOURCE_EXHAUSTED_RETRY_INFO"})
    @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aRetriedReadAttemptIsCountedWhateverStatusEndedTheOneBefore(String failure)
            throws Exception {
        // Three rows, then the failure; the resumed call delivers the remaining two.
        ScriptedReadRows script = new ScriptedReadRows(failure, 1);
        AtomicInteger retries = new AtomicInteger();

        long rows = script.read(retries);

        assertThat(script.offsets).as("resumed where the break left").containsExactly(0L, 3L);
        assertThat(rows).as("every row, read once").isEqualTo(ScriptedReadRows.STREAM_ROWS);
        assertThat(retries).as("one re-issued attempt").hasValue(1);
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"UNAVAILABLE", "INTERNAL_RST_STREAM"})
    @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void theAttemptTheRetryBoundRefusesIsNotCounted(String failure) throws Exception {
        // Every call fails; the first delivers three rows and each resumed one none, so the
        // retryMaxAttempts bound of 3 ends the read after two re-issued attempts. The SDK listener
        // counted a third for the INTERNAL family, because it runs before the bound is consulted.
        ScriptedReadRows script = new ScriptedReadRows(failure, Integer.MAX_VALUE);
        AtomicInteger retries = new AtomicInteger();

        assertThatThrownBy(() -> script.read(retries))
                .rootCause()
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        e ->
                                assertThat(e.getStatus().getCode().name())
                                        .as("the status the bound gave up on")
                                        .isEqualTo(failure.replace("_RST_STREAM", "")));

        assertThat(script.offsets).containsExactly(0L, 3L, 3L);
        assertThat(retries).as("two re-issued attempts").hasValue(2);
    }

    /**
     * A {@code ReadRows} server over a five-row stream, read through a production reading client
     * bounded at three attempts. Each call serves the stream from its requested offset: the first
     * three rows and then the named failure on the first call, nothing and then the failure on each
     * further call up to {@code failures}, and the rest of the stream after that, so a resume at
     * the wrong offset changes the row count as well as the recorded offsets.
     */
    private static final class ScriptedReadRows {

        static final int STREAM_ROWS = 5;

        private final String failure;
        private final int failures;
        private final List<Long> offsets = new CopyOnWriteArrayList<>();

        ScriptedReadRows(String failure, int failures) {
            this.failure = failure;
            this.failures = failures;
        }

        long read(AtomicInteger retries) throws Exception {
            Server server =
                    NettyServerBuilder.forAddress(
                                    new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0))
                            .addService(
                                    new BigQueryReadGrpc.BigQueryReadImplBase() {
                                        @Override
                                        public void readRows(
                                                ReadRowsRequest request,
                                                StreamObserver<ReadRowsResponse> response) {
                                            serve(request, response);
                                        }
                                    })
                            .build()
                            .start();
            try (GrpcEventLoops.LeasedClient<BigQueryReadClient> client =
                    BigQueryReadClients.createForReads(
                            null,
                            EmulatorEndpoint.parse(
                                    "127.0.0.1:" + server.getPort(), "emulatorEndpoint"),
                            3,
                            retries::incrementAndGet)) {
                long rows = 0;
                // The deadline bounds each attempt; the test's timeout bounds the whole read.
                for (ReadRowsResponse response :
                        client.client()
                                .readRowsCallable()
                                .call(
                                        ReadRowsRequest.newBuilder().setReadStream("s").build(),
                                        GrpcCallContext.createDefault()
                                                .withTimeoutDuration(RPC_DEADLINE))) {
                    rows += response.getRowCount();
                }
                return rows;
            } finally {
                server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            }
        }

        private void serve(ReadRowsRequest request, StreamObserver<ReadRowsResponse> response) {
            offsets.add(request.getOffset());
            int call = offsets.size();
            long rows =
                    call == 1
                            ? 3
                            : call <= failures ? 0 : Math.max(0, STREAM_ROWS - request.getOffset());
            for (long i = 0; i < rows; i++) {
                response.onNext(ReadRowsResponse.newBuilder().setRowCount(1).build());
            }
            if (call <= failures) {
                response.onError(failure());
            } else {
                response.onCompleted();
            }
        }

        private StatusRuntimeException failure() {
            switch (failure) {
                case "UNAVAILABLE":
                    return Status.UNAVAILABLE
                            .withDescription("connection reset")
                            .asRuntimeException();
                case "INTERNAL_RST_STREAM":
                    // One of the INTERNAL descriptions the client classifies as a transport fault.
                    return Status.INTERNAL
                            .withDescription("HTTP/2 error code: RST_STREAM")
                            .asRuntimeException();
                case "RESOURCE_EXHAUSTED_RETRY_INFO":
                    Metadata trailers = new Metadata();
                    trailers.put(
                            ProtoUtils.keyForProto(RetryInfo.getDefaultInstance()),
                            RetryInfo.newBuilder()
                                    .setRetryDelay(
                                            com.google.protobuf.Duration.newBuilder()
                                                    .setNanos(10_000_000))
                                    .build());
                    return Status.RESOURCE_EXHAUSTED.asRuntimeException(trailers);
                default:
                    throw new IllegalArgumentException(failure);
            }
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
