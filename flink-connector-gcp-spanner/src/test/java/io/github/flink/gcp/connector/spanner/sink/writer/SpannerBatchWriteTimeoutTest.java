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
import com.google.spanner.v1.SpannerGrpc;
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

/** Proves the configured BatchWrite total timeout ends a server stream that never completes. */
class SpannerBatchWriteTimeoutTest {

    private static final DatabaseDestination DATABASE =
            DatabaseDestination.of("my-project", "my-instance", "my-db");

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(5)
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
                                            .batchWriteTimeout(Duration.ofMillis(100))
                                            .build(),
                                    null)
                            .settings().getSpannerStubSettings().toBuilder();
            settings.setTransportChannelProvider(
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)));
            settings.setCredentialsProvider(NoCredentialsProvider.create());
            stub = settings.build().createStub();

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
