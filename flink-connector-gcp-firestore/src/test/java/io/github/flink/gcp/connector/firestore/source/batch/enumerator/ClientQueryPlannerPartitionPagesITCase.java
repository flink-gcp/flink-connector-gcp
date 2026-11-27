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

import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.PartitionQueryResponse;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.Value;
import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceConfig;
import io.github.flink.gcp.connector.firestore.source.TestSources;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.grpc.Context;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The planner pages {@code PartitionQuery} itself, one call of its session per page, so a close
 * cancels a later page in flight, and the cursors of every page become partitions in document-name
 * order (#2346, docs/adr/0208).
 *
 * <p>The client library's {@code CollectionGroup.getPartitions(long)} fetched a later page inside
 * the first page's callback, synchronously, where a close could not reach it (docs/adr/0208). The
 * emulator does not implement {@code PartitionQuery}, so a server here answers the plan's probes
 * with a read time and scripts the pages.
 */
@Timeout(120)
class ClientQueryPlannerPartitionPagesITCase {

    private static final String ROOT = "projects/p/databases/(default)/documents";

    private static final MethodDescriptor<RunQueryRequest, RunQueryResponse> RUN_QUERY =
            MethodDescriptor.<RunQueryRequest, RunQueryResponse>newBuilder()
                    .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                    .setFullMethodName("google.firestore.v1.Firestore/RunQuery")
                    .setRequestMarshaller(
                            ProtoUtils.marshaller(RunQueryRequest.getDefaultInstance()))
                    .setResponseMarshaller(
                            ProtoUtils.marshaller(RunQueryResponse.getDefaultInstance()))
                    .build();

    private static final MethodDescriptor<PartitionQueryRequest, PartitionQueryResponse>
            PARTITION_QUERY =
                    MethodDescriptor.<PartitionQueryRequest, PartitionQueryResponse>newBuilder()
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName("google.firestore.v1.Firestore/PartitionQuery")
                            .setRequestMarshaller(
                                    ProtoUtils.marshaller(
                                            PartitionQueryRequest.getDefaultInstance()))
                            .setResponseMarshaller(
                                    ProtoUtils.marshaller(
                                            PartitionQueryResponse.getDefaultInstance()))
                            .build();

    private final List<PartitionQueryRequest> pages = new CopyOnWriteArrayList<>();
    private Server server;

    /** Answers each {@code PartitionQuery} page; set by each test before it plans. */
    private volatile BiConsumer<PartitionQueryRequest, StreamObserver<PartitionQueryResponse>>
            partitionQuery;

    @BeforeEach
    void startServer() throws IOException {
        server =
                // Bound and dialled on 127.0.0.1, not localhost, which a concurrent Docker publish
                // can take (ADR-0132).
                NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                        .addService(
                                ServerServiceDefinition.builder("google.firestore.v1.Firestore")
                                        .addMethod(
                                                RUN_QUERY,
                                                ServerCalls.asyncServerStreamingCall(
                                                        ClientQueryPlannerPartitionPagesITCase
                                                                ::answerWithAReadTime))
                                        .addMethod(
                                                PARTITION_QUERY,
                                                ServerCalls.asyncUnaryCall(
                                                        (request, observer) -> {
                                                            pages.add(request);
                                                            partitionQuery.accept(
                                                                    request, observer);
                                                        }))
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
        // With the variable set, the plan's client would be built off the connector's group.
        assertThat(System.getenv("FIRESTORE_EMULATOR_HOST"))
                .as("FIRESTORE_EMULATOR_HOST is set; unset it to run this test")
                .isNull();
        assertThat(GrpcEventLoopThreads.awaitNone(Duration.ofSeconds(60)))
                .as("no %s* thread before the test", GrpcEventLoopThreads.PREFIX)
                .isTrue();
        assertThat(GrpcEventLoops.isRunning()).as("a lease open before the test").isFalse();
    }

    @Test
    void thePagesCursorsBecomePartitionsInDocumentNameOrder() throws Exception {
        partitionQuery =
                (request, observer) -> {
                    // The service orders cursors within a page but not across pages.
                    if (request.getPageToken().isEmpty()) {
                        observer.onNext(page("second", "orders/b", "orders/d"));
                    } else {
                        observer.onNext(page("", "orders/a", "orders/c"));
                    }
                    observer.onCompleted();
                };

        QueryPlan plan;
        try (ClientQueryPlanner planner = planner()) {
            plan = planner.plan(config(5), 1);
        }

        assertThat(pages)
                .extracting(PartitionQueryRequest::getPageToken)
                .containsExactly("", "second");
        PartitionQueryRequest first = pages.get(0);
        assertThat(first.getParent()).isEqualTo(ROOT);
        assertThat(first.getPartitionCount()).isEqualTo(4);
        assertThat(first.getPageSize()).as("the page size is left to the service").isZero();
        assertThat(first.getStructuredQuery().getFrom(0).getAllDescendants()).isTrue();
        assertThat(first.getStructuredQuery().getOrderByList())
                .extracting(order -> order.getField().getFieldPath())
                .containsExactly("__name__");
        assertThat(pages.get(1).toBuilder().clearPageToken().build())
                .as("the later page asks for what the first did")
                .isEqualTo(first);

        assertThat(plan.getQueries()).hasSize(5);
        assertThat(boundaries(plan))
                .containsExactly(
                        Arrays.asList(null, "orders/a"),
                        Arrays.asList("orders/a", "orders/b"),
                        Arrays.asList("orders/b", "orders/c"),
                        Arrays.asList("orders/c", "orders/d"),
                        Arrays.asList("orders/d", null));
    }

    @Test
    void aPageSizeTheSeamSetsIsSentWithEveryPage() throws Exception {
        partitionQuery =
                (request, observer) -> {
                    observer.onNext(
                            request.getPageToken().isEmpty()
                                    ? page("second", "orders/a")
                                    : page("", "orders/b"));
                    observer.onCompleted();
                };

        try (ClientQueryPlanner planner =
                new ClientQueryPlanner(EmulatorEndpoint.parse(target(), "e")) {
                    @Override
                    protected int partitionPageSize() {
                        return 2;
                    }
                }) {
            planner.plan(config(3), 1);
        }

        assertThat(pages).extracting(PartitionQueryRequest::getPageSize).containsExactly(2, 2);
    }

    @Test
    void closingThePlannerDuringALaterPageCancelsItAndStopsTheGroup() throws Exception {
        CountDownLatch secondPage = new CountDownLatch(1);
        CountDownLatch secondPageCancelled = new CountDownLatch(1);
        partitionQuery =
                (request, observer) -> {
                    if (request.getPageToken().isEmpty()) {
                        observer.onNext(page("second", "orders/b"));
                        observer.onCompleted();
                    } else {
                        Context.current()
                                .addListener(
                                        context -> secondPageCancelled.countDown(), Runnable::run);
                        secondPage.countDown();
                        // Never answered.
                    }
                };
        ClientQueryPlanner planner = planner();
        try {
            FirestoreSourceConfig<String> config = config(4);
            CompletableFuture<QueryPlan> plan =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return planner.plan(config, 1);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            });
            assertThat(secondPage.await(30, TimeUnit.SECONDS))
                    .as("the plan asked for the second page")
                    .isTrue();
            assertThat(threadsIn("com.google.cloud.firestore.CollectionGroup"))
                    .as(
                            "no thread waits on a page inside the client library, where a close"
                                    + " cannot reach it")
                    .isEmpty();

            CompletableFuture<Void> closing =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    planner.close();
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            });
            // Fails with a TimeoutException if the close takes 10 s or more.
            closing.get(10, TimeUnit.SECONDS);
            assertThatThrownBy(() -> plan.get(10, TimeUnit.SECONDS))
                    .hasStackTraceContaining("closed while a call was in flight");
            assertThat(secondPageCancelled.await(10, TimeUnit.SECONDS))
                    .as("the server saw the second page's call end")
                    .isTrue();
            assertThat(GrpcEventLoopThreads.awaitNone())
                    .as("no %s* thread once the planner closed", GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        } finally {
            // The server first, so that a close after a failed check above does not wait out a
            // page the server holds; then the planner, idempotently.
            server.shutdownNow();
            planner.close();
        }
    }

    private ClientQueryPlanner planner() {
        return new ClientQueryPlanner(EmulatorEndpoint.parse(target(), "e"));
    }

    private FirestoreSourceConfig<String> config(int partitionCount) {
        return TestSources.scanConfig(
                builder -> builder.partitionCount(partitionCount).emulatorEndpoint(target()));
    }

    private String target() {
        return "127.0.0.1:" + server.getPort();
    }

    /** Answers a query with no document, at a read time, which is all the plan's probes need. */
    private static void answerWithAReadTime(
            RunQueryRequest request, StreamObserver<RunQueryResponse> observer) {
        long now = System.currentTimeMillis() / 1000;
        observer.onNext(
                RunQueryResponse.newBuilder()
                        .setReadTime(Timestamp.newBuilder().setSeconds(now))
                        .build());
        observer.onCompleted();
    }

    private static PartitionQueryResponse page(String nextPageToken, String... documents) {
        PartitionQueryResponse.Builder page =
                PartitionQueryResponse.newBuilder().setNextPageToken(nextPageToken);
        for (String document : documents) {
            page.addPartitions(
                    Cursor.newBuilder()
                            .addValues(
                                    Value.newBuilder().setReferenceValue(ROOT + "/" + document)));
        }
        return page.build();
    }

    /** Returns each partition's start and end document, relative to the root, or null for none. */
    private static List<List<String>> boundaries(QueryPlan plan) {
        List<List<String>> boundaries = new ArrayList<>();
        for (RunQueryRequest request : plan.getQueries()) {
            StructuredQuery query = request.getStructuredQuery();
            boundaries.add(
                    Arrays.asList(
                            query.hasStartAt() ? document(query.getStartAt()) : null,
                            query.hasEndAt() ? document(query.getEndAt()) : null));
        }
        return boundaries;
    }

    private static String document(Cursor cursor) {
        return cursor.getValues(0).getReferenceValue().substring(ROOT.length() + 1);
    }

    /**
     * Returns the names of the live threads with a frame of the given class, or of a class nested
     * in it, on their stack.
     */
    private static List<String> threadsIn(String className) {
        return Thread.getAllStackTraces().entrySet().stream()
                .filter(
                        entry ->
                                Arrays.stream(entry.getValue())
                                        .anyMatch(
                                                frame ->
                                                        frame.getClassName().startsWith(className)))
                .map(Map.Entry::getKey)
                .map(Thread::getName)
                .collect(Collectors.toList());
    }
}
