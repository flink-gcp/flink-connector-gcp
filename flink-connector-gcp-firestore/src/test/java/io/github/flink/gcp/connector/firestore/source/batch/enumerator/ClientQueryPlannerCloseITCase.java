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

package io.github.flink.gcp.connector.firestore.source.batch.enumerator;

import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceConfig;
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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Closing the planner while a plan waits on a query stream the server never answers ends the plan
 * and the client at once, and stops the event-loop group (docs/adr/0208, #2330).
 *
 * <p>Cancelling the future of a Firestore query ends the caller's wait but not the stream, and the
 * client's own {@code close()} then waits for the stream until its deadline: measured at 300 s
 * before {@code FirestoreClients.close} shut the client down first. The server here accepts {@code
 * RunQuery} and never answers, which the emulator cannot be made to do.
 */
@Timeout(120)
class ClientQueryPlannerCloseITCase {

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

    @BeforeEach
    void startSilentServer() throws IOException {
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

    /**
     * A lease left open by an earlier class in this reused fork would make the checks below pass or
     * fail for the wrong reason, so it is ruled out first.
     */
    @BeforeEach
    void noGroupIsRunning() throws InterruptedException {
        assertThat(
                        EventLoopObserver.awaitNoGroup(
                                GrpcEventLoops::isRunning, Duration.ofSeconds(60)))
                .as("no lease and no %s* thread before the test", GrpcEventLoopThreads.PREFIX)
                .isTrue();
        assertThat(GrpcEventLoops.isRunning()).as("a lease open before the test").isFalse();
    }

    /**
     * Without a configured read time the plan waits in its first query; with one, in the read at
     * that time, which the client runs as a read-only transaction.
     */
    @ParameterizedTest(name = "configured read time: {0}")
    @ValueSource(booleans = {false, true})
    void closingThePlannerMidQueryEndsThePlanAndStopsTheGroup(boolean configuredReadTime)
            throws Exception {
        String target = "127.0.0.1:" + server.getPort();
        FirestoreSourceConfig<String> config =
                TestSources.scanConfig(
                        builder -> {
                            builder.partitionCount(1).emulatorEndpoint(target);
                            return configuredReadTime
                                    ? builder.readTime(Instant.now().minusSeconds(60))
                                    : builder;
                        });
        EventLoopObserver.LoopStarts starts =
                EventLoopObserver.loopStartsFromNow(
                        GrpcEventLoops::isRunning, GrpcEventLoops::loopsStartedWhileRunning);
        ClientQueryPlanner planner = new ClientQueryPlanner(EmulatorEndpoint.parse(target, "e"));
        try {
            CompletableFuture<QueryPlan> plan =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return planner.plan(config, 1);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            assertThat(queried.await(30, TimeUnit.SECONDS))
                    .as("the plan's query reached the server")
                    .isTrue();
            assertThat(starts.anyStarted())
                    .as("a group event loop started for the plan's call")
                    .isTrue();
            assertThat(GrpcEventLoops.isRunning())
                    .as("the planner's client holds its lease while the call waits")
                    .isTrue();

            CompletableFuture<Void> closing =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    planner.close();
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            });
            // Fails with a TimeoutException if the close takes 4 s or more: below the 5 s a
            // channel's bounded close waits for its calls before it cuts them, which an emulator
            // client's close must not wait out (#2343). Both cases took under 1 s once the
            // channel was cut first (measured 2026-11-26).
            closing.get(4, TimeUnit.SECONDS);
            assertThatThrownBy(() -> plan.get(10, TimeUnit.SECONDS))
                    .hasStackTraceContaining("closed while a call was in flight");
            assertThat(GrpcEventLoopThreads.awaitNone())
                    .as("no %s* thread once the planner closed", GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        } finally {
            // Idempotent; ends the plan's call if a check above failed before the close.
            planner.close();
        }
    }
}
