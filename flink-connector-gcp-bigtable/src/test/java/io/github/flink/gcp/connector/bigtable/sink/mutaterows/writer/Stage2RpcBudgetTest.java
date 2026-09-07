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

package io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer;

import com.google.bigtable.v2.MutateRowsRequest;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2RpcBudgetTest {
    @Test
    void factoryBudgetSurvivesSdkClientConstructionAndCountsActualWire() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger received = new AtomicInteger();
        io.grpc.Server server =
                io.grpc.ServerBuilder.forPort(0)
                        .addService(
                                new com.google.bigtable.v2.BigtableGrpc.BigtableImplBase() {
                                    @Override
                                    public void mutateRows(
                                            MutateRowsRequest request,
                                            io.grpc.stub.StreamObserver<
                                                            com.google.bigtable.v2
                                                                    .MutateRowsResponse>
                                                    response) {
                                        received.addAndGet(request.getEntriesCount());
                                        var result =
                                                com.google.bigtable.v2.MutateRowsResponse
                                                        .newBuilder();
                                        for (int index = 0;
                                                index < request.getEntriesCount();
                                                index++) {
                                            result.addEntries(
                                                    com.google.bigtable.v2.MutateRowsResponse.Entry
                                                            .newBuilder()
                                                            .setIndex(index)
                                                            .setStatus(
                                                                    com.google.rpc.Status
                                                                            .getDefaultInstance()));
                                        }
                                        response.onNext(result.build());
                                        response.onCompleted();
                                    }
                                })
                        .build()
                        .start();
        var factory =
                new Stage2BudgetedBatcherFactory(
                        "single-cluster",
                        4,
                        request -> attempts.addAndGet(request.getEntriesCount()));
        try {
            var settings =
                    factory
                            .settings(
                                    io.github.flink.gcp.connector.bigtable.TableDestination.of(
                                            "local-project", "local-instance", "local-table"))
                            .toBuilder();
            String endpoint = "127.0.0.1:" + server.getPort();
            settings.setMetricsProvider(
                            com.google.cloud.bigtable.data.v2.stub.metrics.NoopMetricsProvider
                                    .INSTANCE)
                    .disableInternalMetrics();
            var provider =
                    (com.google.api.gax.grpc.InstantiatingGrpcChannelProvider)
                            settings.stubSettings().getTransportChannelProvider();
            settings.stubSettings()
                    .setEndpoint(endpoint)
                    .setCredentialsProvider(com.google.api.gax.core.NoCredentialsProvider.create())
                    .setTransportChannelProvider(
                            provider.toBuilder()
                                    .setEndpoint(endpoint)
                                    .setChannelConfigurator(builder -> builder.usePlaintext())
                                    .build());
            try (var client =
                    com.google.cloud.bigtable.data.v2.BigtableDataClient.create(settings.build())) {
                var mutation =
                        com.google.cloud.bigtable.data.v2.models.Mutation.create()
                                .setCell("cf", "q", "value");
                var bulk =
                        com.google.cloud.bigtable.data.v2.models.BulkMutation.create("local-table")
                                .add("row-1", mutation)
                                .add("row-2", mutation);
                client.bulkMutateRowsAsync(bulk).get(10, java.util.concurrent.TimeUnit.SECONDS);
            }
            assertThat(received.get()).isEqualTo(2);
            assertThat(attempts.get()).isEqualTo(received.get());
        } finally {
            factory.close();
            server.shutdownNow();
            assertThat(server.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void retryIsCountedAgainAndRejectedBeforeForwarding() {
        AtomicInteger attempts = new AtomicInteger();
        AtomicLong bytes = new AtomicLong();
        AtomicInteger forwarded = new AtomicInteger();
        var interceptor =
                Stage2BudgetedBatcherFactory.interceptor(
                        request -> {
                            bytes.addAndGet(request.getSerializedSize());
                            if (attempts.addAndGet(request.getEntriesCount()) > 3) {
                                throw new IllegalStateException("wire budget exhausted");
                            }
                        });
        Channel channel =
                new Channel() {
                    @Override
                    public String authority() {
                        return "local";
                    }

                    @Override
                    public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                            MethodDescriptor<ReqT, RespT> method, CallOptions options) {
                        return new ClientCall<>() {
                            @Override
                            public void start(Listener<RespT> listener, Metadata headers) {}

                            @Override
                            public void request(int count) {}

                            @Override
                            public void cancel(String message, Throwable cause) {}

                            @Override
                            public void halfClose() {}

                            @Override
                            public void sendMessage(ReqT message) {
                                forwarded.incrementAndGet();
                            }
                        };
                    }
                };
        MutateRowsRequest request =
                MutateRowsRequest.newBuilder()
                        .addEntries(MutateRowsRequest.Entry.getDefaultInstance())
                        .addEntries(MutateRowsRequest.Entry.getDefaultInstance())
                        .build();
        ClientCall<MutateRowsRequest, Object> first =
                interceptor.interceptCall(null, CallOptions.DEFAULT, channel);
        first.sendMessage(request);
        ClientCall<MutateRowsRequest, Object> retry =
                interceptor.interceptCall(null, CallOptions.DEFAULT, channel);
        assertThatThrownBy(() -> retry.sendMessage(request)).hasMessageContaining("wire budget");
        assertThat(attempts.get()).isEqualTo(4);
        assertThat(bytes.get()).isEqualTo(2L * request.getSerializedSize());
        assertThat(forwarded.get()).isEqualTo(1);
    }
}
