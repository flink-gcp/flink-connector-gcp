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

package io.github.flink.gcp.connector.firestore.table.source;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchGetDocumentsResponse;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.firestore.source.TestSources;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Closing a lookup while a read waits on a server that never answers ends the read and the client
 * at once, and stops the event-loop group (docs/adr/0208, #2330): the client closes with {@code
 * shutdownNow()}, which drops a retry gax has scheduled without completing the read's future, so
 * the close cancels the read's future first.
 */
@Timeout(120)
class FirestoreCollectionDocumentLookupCloseITCase {

    private static final MethodDescriptor<BatchGetDocumentsRequest, BatchGetDocumentsResponse>
            BATCH_GET =
                    MethodDescriptor
                            .<BatchGetDocumentsRequest, BatchGetDocumentsResponse>newBuilder()
                            .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                            .setFullMethodName("google.firestore.v1.Firestore/BatchGetDocuments")
                            .setRequestMarshaller(
                                    ProtoUtils.marshaller(
                                            BatchGetDocumentsRequest.getDefaultInstance()))
                            .setResponseMarshaller(
                                    ProtoUtils.marshaller(
                                            BatchGetDocumentsResponse.getDefaultInstance()))
                            .build();

    private final CountDownLatch requested = new CountDownLatch(1);
    private Server server;

    /**
     * A lease left open by an earlier class in this reused fork would make the checks below pass or
     * fail for the wrong reason, so it is ruled out first.
     */
    @BeforeEach
    void startSilentServer() throws IOException, InterruptedException {
        assertThat(GrpcEventLoopThreads.awaitNone(Duration.ofSeconds(60)))
                .as("no %s* thread before the test", GrpcEventLoopThreads.PREFIX)
                .isTrue();
        assertThat(GrpcEventLoops.isRunning()).as("a lease open before the test").isFalse();
        server =
                // Bound and dialled on 127.0.0.1, not localhost, which a concurrent Docker publish
                // can take (ADR-0132).
                NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                        .addService(
                                ServerServiceDefinition.builder("google.firestore.v1.Firestore")
                                        .addMethod(
                                                BATCH_GET,
                                                ServerCalls.asyncServerStreamingCall(
                                                        (request, observer) ->
                                                                requested.countDown()))
                                        .build())
                        .build()
                        .start();
    }

    @AfterEach
    void stopServer() {
        server.shutdownNow();
    }

    @Test
    void closingTheLookupMidReadEndsTheReadAndStopsTheGroup() throws Exception {
        FirestoreCollectionDocumentLookup lookup =
                new FirestoreCollectionDocumentLookup(
                        TestSources.DATABASE,
                        "c",
                        new String[0],
                        "127.0.0.1:" + server.getPort(),
                        null);
        lookup.open();
        try {
            ApiFuture<DocumentSnapshot> read = lookup.readAsync("a");
            assertThat(requested.await(30, TimeUnit.SECONDS))
                    .as("the read reached the server")
                    .isTrue();
            assertThat(GrpcEventLoopThreads.alive())
                    .as("a %s* thread carries the read", GrpcEventLoopThreads.PREFIX)
                    .isTrue();

            CompletableFuture<Void> closing =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    lookup.close();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
            // Fails with a TimeoutException if the close takes 10 s or more.
            closing.get(10, TimeUnit.SECONDS);
            assertThat(read.isDone()).as("the read ended with the close").isTrue();
            assertThatThrownBy(read::get).isInstanceOf(CancellationException.class);
            assertThat(GrpcEventLoops.isRunning()).as("the lookup's lease").isFalse();
            assertThat(GrpcEventLoopThreads.awaitNone())
                    .as("no %s* thread once the lookup closed", GrpcEventLoopThreads.PREFIX)
                    .isTrue();
            // Refused by the lookup itself, before the closed client is reached.
            assertThatThrownBy(() -> lookup.readAsync("b"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Firestore document lookup");
        } finally {
            lookup.close();
        }
    }
}
