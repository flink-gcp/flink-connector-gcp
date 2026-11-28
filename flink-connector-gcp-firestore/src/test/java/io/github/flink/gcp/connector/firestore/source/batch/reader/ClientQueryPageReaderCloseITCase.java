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

package io.github.flink.gcp.connector.firestore.source.batch.reader;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Query;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.StructuredQuery;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.firestore.source.TestSources;
import io.github.flink.gcp.connector.testutils.EventLoopObserver;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Closing the page reader while a page waits on a query stream the server never answers ends the
 * page and the client at once, and stops the event-loop group (docs/adr/0208, #2330).
 *
 * <p>The server accepts {@code RunQuery} and never answers, which the emulator cannot be made to
 * do; the read goes through the production path, a read-only transaction at the read time.
 */
@Timeout(120)
class ClientQueryPageReaderCloseITCase {

    private static final MethodDescriptor<RunQueryRequest, RunQueryResponse> RUN_QUERY =
            MethodDescriptor.<RunQueryRequest, RunQueryResponse>newBuilder()
                    .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                    .setFullMethodName("google.firestore.v1.Firestore/RunQuery")
                    .setRequestMarshaller(
                            ProtoUtils.marshaller(RunQueryRequest.getDefaultInstance()))
                    .setResponseMarshaller(
                            ProtoUtils.marshaller(RunQueryResponse.getDefaultInstance()))
                    .build();

    private final CountDownLatch queried = new CountDownLatch(1);
    private Server server;

    /**
     * A lease left open by an earlier class in this reused fork would make the checks below pass or
     * fail for the wrong reason, so it is ruled out first.
     */
    @BeforeEach
    void startSilentServer() throws IOException, InterruptedException {
        assertThat(
                        EventLoopObserver.awaitNoGroup(
                                GrpcEventLoops::isRunning, Duration.ofSeconds(60)))
                .as("no lease and no %s* thread before the test", GrpcEventLoopThreads.PREFIX)
                .isTrue();
        assertThat(GrpcEventLoops.isRunning()).as("a lease open before the test").isFalse();
        server =
                // Bound and dialled on 127.0.0.1, not localhost, which a concurrent Docker publish
                // can take (ADR-0132).
                NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                        .addService(
                                ServerServiceDefinition.builder("google.firestore.v1.Firestore")
                                        .addMethod(
                                                RUN_QUERY,
                                                ServerCalls.asyncServerStreamingCall(
                                                        (request, observer) -> queried.countDown()))
                                        .build())
                        .build()
                        .start();
    }

    @AfterEach
    void stopServer() {
        server.shutdownNow();
    }

    @Test
    void closingTheReaderMidPageEndsThePageAndStopsTheGroup() throws Exception {
        String target = "127.0.0.1:" + server.getPort();
        EventLoopObserver.LoopStarts starts =
                EventLoopObserver.loopStartsFromNow(
                        GrpcEventLoops::isRunning, GrpcEventLoops::loopsStartedWhileRunning);
        ClientQueryPageReader reader =
                new ClientQueryPageReader(EmulatorEndpoint.parse(target, "emulatorEndpoint"));
        try {
            Query page = reader.query(TestSources.DATABASE, request());
            Timestamp readTime = Timestamp.ofTimeSecondsAndNanos(1_700_000_000, 0);
            CompletableFuture<Throwable> read =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    reader.read(page, readTime);
                                    return null;
                                } catch (Throwable t) {
                                    return t;
                                }
                            });
            assertThat(queried.await(30, TimeUnit.SECONDS))
                    .as("the page's query reached the server")
                    .isTrue();
            assertThat(starts.anyStarted())
                    .as("a group event loop started for the page's call")
                    .isTrue();
            assertThat(GrpcEventLoops.isRunning())
                    .as("the reader's client holds its lease while the call waits")
                    .isTrue();

            CompletableFuture<Void> closing =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    reader.close();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            // Fails with a TimeoutException if the close takes 10 s or more.
            closing.get(10, TimeUnit.SECONDS);
            assertThat(read.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("closed while a page was being read");
            assertThat(GrpcEventLoops.isRunning()).as("the reader's lease").isFalse();
            assertThat(GrpcEventLoopThreads.awaitNone())
                    .as("no %s* thread once the reader closed", GrpcEventLoopThreads.PREFIX)
                    .isTrue();
            assertThatThrownBy(() -> reader.read(page, readTime))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("closed before use");
        } finally {
            // Idempotent; ends the page's call if a check above failed before the close.
            reader.close();
        }
    }

    private static RunQueryRequest request() {
        return RunQueryRequest.newBuilder()
                .setParent(
                        "projects/"
                                + TestSources.DATABASE.getProject()
                                + "/databases/"
                                + TestSources.DATABASE.getDatabaseId()
                                + "/documents")
                .setStructuredQuery(
                        StructuredQuery.newBuilder()
                                .addFrom(
                                        StructuredQuery.CollectionSelector.newBuilder()
                                                .setCollectionId("c"))
                                .setLimit(com.google.protobuf.Int32Value.of(10)))
                .build();
    }
}
