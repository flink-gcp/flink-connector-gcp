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

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.MethodDescriptor;

import java.util.List;
import java.util.function.Consumer;

/** Runtime-only test factory counting wire submissions, including SDK retries. */
public final class Stage2BudgetedBatcherFactory extends DefaultMutationBatcherFactory {
    private static final long serialVersionUID = 1L;
    private final transient Consumer<MutateRowsRequest> budget;

    public Stage2BudgetedBatcherFactory(
            String profile, int inFlight, Consumer<MutateRowsRequest> budget) {
        super(
                profile,
                BigtableWriterOptions.builder().maxInFlightEntries(inFlight).build(),
                null,
                null);
        this.budget = budget;
    }

    @Override
    BigtableDataSettings settings(TableDestination destination) {
        BigtableDataSettings.Builder settings = super.settings(destination).toBuilder();
        InstantiatingGrpcChannelProvider provider =
                (InstantiatingGrpcChannelProvider)
                        settings.stubSettings().getTransportChannelProvider();
        settings.stubSettings()
                .setTransportChannelProvider(
                        provider.toBuilder()
                                .setInterceptorProvider(() -> List.of(interceptor(budget)))
                                .build());
        return settings.build();
    }

    static ClientInterceptor interceptor(Consumer<MutateRowsRequest> budget) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method, CallOptions options, Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(
                        next.newCall(method, options)) {
                    @Override
                    public void sendMessage(ReqT message) {
                        if (message instanceof MutateRowsRequest) {
                            budget.accept((MutateRowsRequest) message);
                        }
                        super.sendMessage(message);
                    }
                };
            }
        };
    }
}
