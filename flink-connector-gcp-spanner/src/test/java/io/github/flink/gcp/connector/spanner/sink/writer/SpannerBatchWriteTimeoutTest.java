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

package io.github.flink.gcp.connector.spanner.sink.writer;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.spanner.v1.stub.SpannerStub;
import com.google.cloud.spanner.v1.stub.SpannerStubSettings;
import com.google.rpc.Status;
import com.google.spanner.v1.BatchWriteRequest;
import com.google.spanner.v1.BatchWriteResponse;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.SpannerGrpc;
import com.google.spanner.v1.Transaction;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.api.gax.rpc.StatusCode.Code.DEADLINE_EXCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Proves the configured BatchWrite total timeout ends a server stream that never completes.
 *
 * <p>The {@code @Timeout} on the test is a bail-out ceiling, not an assertion: nothing here
 * measures elapsed time, so it is the only bound on <em>lateness</em>. It is deliberately close
 * rather than generous. The regression it has to catch is the configured deadline not reaching
 * {@code batchWriteSettings}, which gax would leave at its {@code no_retry_0_params} default of an
 * hour; a ceiling far above the deadline would also accept any wrong-but-finite value under it. Ten
 * seconds keeps a 20x margin over the 500 ms deadline and stays clear of the warm-up's own 5 s
 * bound, while a wrong 30 s deadline still fails here rather than passing green.
 */
class SpannerBatchWriteTimeoutTest {

    private static final DatabaseDestination DATABASE =
            DatabaseDestination.of("my-project", "my-instance", "my-db");

    /**
     * The deadline under test. Large enough that the warm-up's residual (below) cannot consume a
     * meaningful fraction of it, small enough that two parameters cost a second. It is a configured
     * knob rather than a measured property of anything, so it may move; what may not move is the
     * ratio between it and the cost of reaching the server, which is what #1198 was.
     */
    private static final Duration BATCH_WRITE_TIMEOUT = Duration.ofMillis(500);

    /**
     * Bounds the warm-up so a broken fixture reports itself rather than racing {@code @Timeout}.
     */
    private static final Duration WARM_UP_TIMEOUT = Duration.ofSeconds(5);

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(10)
    void aStalledBatchWriteReturnsAtTheConfiguredDeadline(boolean reportOneGroup) throws Exception {
        String serverName = InProcessServerBuilder.generateName() + UUID.randomUUID();
        AtomicInteger calls = new AtomicInteger();
        Server server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(stallingService(reportOneGroup, calls))
                        .build()
                        .start();
        ManagedChannel channel =
                InProcessChannelBuilder.forName(serverName).directExecutor().build();
        SpannerStub stub = null;
        try {
            SpannerStubSettings.Builder settings =
                    new DefaultSpannerDatabaseAccessFactory(
                                    DATABASE,
                                    SpannerWriterOptions.builder()
                                            .batchWriteTimeout(BATCH_WRITE_TIMEOUT)
                                            .build(),
                                    null)
                            .settings().getSpannerStubSettings().toBuilder();
            settings.setTransportChannelProvider(
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)));
            settings.setCredentialsProvider(NoCredentialsProvider.create());
            // The warm-up's own budget. Left at the default it would be beginTransaction's
            // retry_policy_3 totalTimeout of 30 s, three times the @Timeout ceiling below, so a
            // warm-up that failed would be aborted by JUnit while gax was still retrying and
            // would report `execution timed out` instead of the exception that says the fixture
            // is broken. Bounded under both, so the diagnostic survives.
            settings.beginTransactionSettings().setSimpleTimeoutNoRetriesDuration(WARM_UP_TIMEOUT);
            stub = settings.build().createStub();

            // Warm the channel before the deadline starts running. The measured failure (#1198)
            // was `calls` still at zero: the deadline expired before the RPC reached the handler,
            // so the two DEADLINE_EXCEEDED assertions below passed against a call the server
            // never saw. What caught it was the call count, and — when a group is expected — the
            // response count that precedes it.
            //
            // What sits inside the armed deadline is channel-level, not gax-level: createStub()
            // above already built every callable into final fields, but the first call finds the
            // channel IDLE and pays exitIdleMode, the NameResolver and LoadBalancer ServiceLoader
            // scans, and transport creation, parked in DelayedClientTransport meanwhile. One
            // completed round trip pays that once, for the channel rather than for the callable.
            // Measured 2026-09-03 on this fixture: call-to-handler 36 ms cold, 2.9 ms warmed.
            //
            // The residual is real and deliberately accepted: this warms a UnaryCallable while the
            // timed call is a ServerStreamingCallable, so the streaming listener adapters and the
            // BatchWrite marshallers still load inside the window. That is the small remainder of
            // 2.9 ms against a 500 ms deadline, where the flake was 36 ms against 100 ms. Warming
            // batchWriteCallable itself would close the gap and costs a second stub, because a
            // warm-up on this one would run under the very deadline being tested.
            //
            // It also asserts the channel works, so a later DEADLINE_EXCEEDED cannot be a broken
            // fixture reported as a deadline.
            stub.beginTransactionCallable().call(BeginTransactionRequest.getDefaultInstance());

            List<BatchWriteResponse> responses = new ArrayList<>();
            SpannerStub activeStub = stub;
            Throwable failure =
                    catchThrowable(
                            () -> {
                                ServerStream<BatchWriteResponse> stream =
                                        activeStub
                                                .batchWriteCallable()
                                                .call(BatchWriteRequest.getDefaultInstance());
                                stream.forEach(responses::add);
                            });

            assertThat(failure).isInstanceOf(ApiException.class);
            assertThat(((ApiException) failure).getStatusCode().getCode())
                    .isEqualTo(DEADLINE_EXCEEDED);
            assertThat(responses).hasSize(reportOneGroup ? 1 : 0);
            // Not a "did not retry" assertion, although it reads like one: gax cannot retry here,
            // because setSimpleTimeoutNoRetriesDuration pins maxAttempts to 1 with no retryable
            // codes — DefaultSpannerDatabaseAccessFactoryTest is what holds that contract. What
            // this guards is that the RPC reached the handler at all, which is exactly what #1198
            // was: the deadline expired first, so DEADLINE_EXCEEDED arrived on a call the server
            // never saw and both status assertions above passed vacuously. Under
            // reportOneGroup=true the response count above catches it first; this is the only
            // assertion that catches it under false.
            assertThat(calls).hasValue(1);
        } finally {
            if (stub != null) {
                stub.close();
            }
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    private static SpannerGrpc.SpannerImplBase stallingService(
            boolean reportOneGroup, AtomicInteger calls) {
        return new SpannerGrpc.SpannerImplBase() {
            /** Completes immediately; the warm-up, not the stall under test. */
            @Override
            public void beginTransaction(
                    BeginTransactionRequest request, StreamObserver<Transaction> responseObserver) {
                responseObserver.onNext(Transaction.getDefaultInstance());
                responseObserver.onCompleted();
            }

            @Override
            public void batchWrite(
                    BatchWriteRequest request,
                    StreamObserver<BatchWriteResponse> responseObserver) {
                calls.incrementAndGet();
                if (reportOneGroup) {
                    responseObserver.onNext(
                            BatchWriteResponse.newBuilder()
                                    .addIndexes(0)
                                    .setStatus(Status.newBuilder().setCode(0).build())
                                    .build());
                }
                // Deliberately leave the stream open. The client deadline must regain control.
            }
        };
    }
}
