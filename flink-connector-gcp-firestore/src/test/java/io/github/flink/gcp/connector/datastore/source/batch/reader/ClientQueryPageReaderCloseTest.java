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

package io.github.flink.gcp.connector.datastore.source.batch.reader;

import org.apache.flink.util.InstantiationUtil;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The page reader's close ends a page still in flight: a fetcher can outlive the reader's close
 * timeout, and the client's bounded close ends with {@code shutdownNow()}, which drops a retry gax
 * has scheduled without completing the call's future, so the close cancels the future first.
 */
@Timeout(60)
class ClientQueryPageReaderCloseTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p");

    @BeforeEach
    void noLeaseIsOpenBeforehand() {
        assertThat(GrpcEventLoops.isRunning())
                .as("a lease is open before the test: an earlier class in this fork leaked one")
                .isFalse();
    }

    @AfterEach
    void noLeaseIsLeftOpen() {
        assertThat(GrpcEventLoops.isRunning()).as("a lease was left open").isFalse();
    }

    /**
     * Against an endpoint that accepts the connection and never answers, a page waits for as long
     * as the library's retry settings allow, tens of seconds; closing the reader cancels it at once
     * and releases the client's lease.
     */
    @Test
    void closeEndsAPageInFlight() throws Exception {
        try (ServerSocket silent = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            CountDownLatch connected = new CountDownLatch(1);
            List<Socket> accepted = new ArrayList<>();
            Thread acceptor =
                    new Thread(
                            () -> {
                                try {
                                    while (true) {
                                        Socket socket = silent.accept();
                                        synchronized (accepted) {
                                            accepted.add(socket);
                                        }
                                        connected.countDown();
                                    }
                                } catch (IOException e) {
                                    // The server socket closed: the test is over.
                                }
                            },
                            "silent-endpoint");
            acceptor.setDaemon(true);
            acceptor.start();
            String target =
                    InetAddress.getLoopbackAddress().getHostAddress() + ":" + silent.getLocalPort();
            ClientQueryPageReader reader =
                    new ClientQueryPageReader(EmulatorEndpoint.parse(target, "emulatorEndpoint"));
            CompletableFuture<Throwable> page =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    reader.read(DATABASE, request());
                                    return null;
                                } catch (Throwable t) {
                                    return t;
                                }
                            });
            try {
                assertThat(connected.await(20, TimeUnit.SECONDS))
                        .as("the page's call connected")
                        .isTrue();

                reader.close();

                // The connection opens as the call starts, and the call is recorded under the
                // reader's monitor before close() can take it, so the close always finds it.
                assertThat(page.get(10, TimeUnit.SECONDS))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("closed while a page was being read");
                assertThat(GrpcEventLoops.isRunning())
                        .as("the reader's lease was released")
                        .isFalse();
                assertThatThrownBy(() -> reader.read(DATABASE, request()))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("closed before use");
            } finally {
                reader.close();
                synchronized (accepted) {
                    for (Socket socket : accepted) {
                        socket.close();
                    }
                }
            }
        }
    }

    /**
     * gax can miss a cancellation that lands between two attempts of a call, and then schedules the
     * next one, which the client's {@code shutdownNow()} drops without completing the call's future
     * ({@code CallbackChainRetryingFuture}, gax 2.91.0). The page's own wait ends on close anyway,
     * and the call is still asked to cancel.
     */
    @Test
    void closeEndsAPageWhoseCallMissesTheCancellation() throws Exception {
        DeafFuture call = new DeafFuture();
        CountDownLatch started = new CountDownLatch(1);
        ClientQueryPageReader reader =
                new ClientQueryPageReader(
                        (database, page) -> {
                            started.countDown();
                            return call;
                        });
        CompletableFuture<Throwable> page =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                reader.read(DATABASE, request());
                                return null;
                            } catch (Throwable t) {
                                return t;
                            }
                        });
        assertThat(started.await(10, TimeUnit.SECONDS)).as("the page's call started").isTrue();

        reader.close();

        assertThat(page.get(10, TimeUnit.SECONDS))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed while a page was being read");
        assertThat(call.cancelRequested).as("the call was asked to cancel").isTrue();
        assertThat(call.isDone()).as("the call ended").isFalse();
    }

    @Test
    void aCallsFailureFailsThePageWithItsCause() throws Exception {
        IllegalStateException failure = new IllegalStateException("refused");
        try (ClientQueryPageReader reader =
                new ClientQueryPageReader(
                        (database, page) -> ApiFutures.immediateFailedFuture(failure))) {
            assertThatThrownBy(() -> reader.read(DATABASE, request()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Failed to read a page")
                    .hasCause(failure);
        }
    }

    @Test
    void aCallsAnswerIsThePagesBatch() throws Exception {
        QueryResultBatch batch = QueryResultBatch.newBuilder().setSkippedResults(3).build();
        try (ClientQueryPageReader reader =
                new ClientQueryPageReader(
                        (database, page) ->
                                ApiFutures.immediateFuture(
                                        RunQueryResponse.newBuilder().setBatch(batch).build()))) {
            assertThat(reader.read(DATABASE, request())).isEqualTo(batch);
        }
    }

    /**
     * The reader travels in the job graph, so what it holds is a named class rather than a lambda
     * the connector minted (ADR-0125), and a restored copy still works: it refuses a page once it
     * is closed, before starting a call.
     */
    @Test
    void theReaderCrossesTheJobGraphWithoutALambda() throws Exception {
        ClientQueryPageReader reader =
                new ClientQueryPageReader(
                        EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"));

        byte[] bytes = InstantiationUtil.serializeObject(reader);

        assertThat(new String(bytes, StandardCharsets.ISO_8859_1))
                .doesNotContain("SerializedLambda");
        ClientQueryPageReader restored =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        restored.close();
        assertThatThrownBy(() -> restored.read(DATABASE, request()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed before use");
    }

    /**
     * A call that never completes and ignores cancellation, as one between two gax attempts can,
     * recording that it was asked to cancel.
     */
    private static final class DeafFuture implements ApiFuture<RunQueryResponse> {

        private volatile boolean cancelRequested;

        @Override
        public void addListener(Runnable listener, Executor executor) {}

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            cancelRequested = true;
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public RunQueryResponse get() {
            throw new AssertionError("nothing waits on the call itself");
        }

        @Override
        public RunQueryResponse get(long timeout, TimeUnit unit) {
            throw new AssertionError("nothing waits on the call itself");
        }
    }

    private static RunQueryRequest request() {
        return RunQueryRequest.newBuilder()
                .setProjectId(DATABASE.getProject())
                .setDatabaseId(DATABASE.getDatabaseId())
                .setPartitionId(
                        PartitionId.newBuilder()
                                .setProjectId(DATABASE.getProject())
                                .setDatabaseId(DATABASE.getDatabaseId()))
                .setQuery(SplittableQueries.ofKind("K"))
                .build();
    }
}
