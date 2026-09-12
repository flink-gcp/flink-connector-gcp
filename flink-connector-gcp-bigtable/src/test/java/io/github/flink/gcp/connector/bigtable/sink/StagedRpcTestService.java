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

package io.github.flink.gcp.connector.bigtable.sink;

import com.google.bigtable.admin.v2.AppProfile;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.GetAppProfileRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** Local service boundary for production-path recovery tests; no cloud credentials or resources. */
final class StagedRpcTestService implements AutoCloseable {
    final StagedMutationTestSink.Probe probe = new StagedMutationTestSink.Probe();
    final Server server;
    volatile java.util.Map<String, ColumnFamily> families =
            java.util.Map.of("flink_commit", ColumnFamily.getDefaultInstance());
    final java.util.List<String> metadata = new java.util.concurrent.CopyOnWriteArrayList<>();
    volatile boolean invalidProfile;

    StagedRpcTestService() throws Exception {
        server =
                ServerBuilder.forPort(0)
                        .addService(
                                new BigtableGrpc.BigtableImplBase() {
                                    @Override
                                    public void checkAndMutateRow(
                                            CheckAndMutateRowRequest request,
                                            StreamObserver<CheckAndMutateRowResponse> response) {
                                        try {
                                            response.onNext(
                                                    CheckAndMutateRowResponse.newBuilder()
                                                            .setPredicateMatched(
                                                                    probe.apply(request))
                                                            .build());
                                            response.onCompleted();
                                        } catch (IOException failure) {
                                            response.onError(
                                                    Status.UNAVAILABLE
                                                            .withDescription(failure.getMessage())
                                                            .asRuntimeException());
                                        }
                                    }
                                })
                        .addService(
                                adminService(
                                        "BigtableInstanceAdmin",
                                        "GetAppProfile",
                                        GetAppProfileRequest.getDefaultInstance(),
                                        AppProfile.getDefaultInstance(),
                                        (request, response) -> {
                                            metadata.add(request.getName());
                                            response.onNext(
                                                    AppProfile.newBuilder()
                                                            .setName(request.getName())
                                                            .setSingleClusterRouting(
                                                                    AppProfile.SingleClusterRouting
                                                                            .newBuilder()
                                                                            .setClusterId("cluster")
                                                                            .setAllowTransactionalWrites(
                                                                                    !invalidProfile))
                                                            .build());
                                            response.onCompleted();
                                        }))
                        .addService(
                                adminService(
                                        "BigtableTableAdmin",
                                        "GetTable",
                                        GetTableRequest.getDefaultInstance(),
                                        Table.getDefaultInstance(),
                                        (request, response) -> {
                                            metadata.add(request.getName());
                                            response.onNext(
                                                    Table.newBuilder()
                                                            .setName(request.getName())
                                                            .putAllColumnFamilies(families)
                                                            .build());
                                            response.onCompleted();
                                        }))
                        .build()
                        .start();
    }

    static <Q extends com.google.protobuf.Message, R extends com.google.protobuf.Message>
            io.grpc.ServerServiceDefinition adminService(
                    String service,
                    String method,
                    Q request,
                    R response,
                    io.grpc.stub.ServerCalls.UnaryMethod<Q, R> implementation) {
        String name = "google.bigtable.admin.v2." + service;
        return io.grpc.ServerServiceDefinition.builder(name)
                .addMethod(
                        io.grpc.MethodDescriptor.<Q, R>newBuilder()
                                .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                .setFullMethodName(name + "/" + method)
                                .setRequestMarshaller(
                                        io.grpc.protobuf.ProtoUtils.marshaller(request))
                                .setResponseMarshaller(
                                        io.grpc.protobuf.ProtoUtils.marshaller(response))
                                .build(),
                        io.grpc.stub.ServerCalls.asyncUnaryCall(implementation))
                .build();
    }

    @Override
    public void close() throws Exception {
        server.shutdownNow();
        if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Local Bigtable server did not terminate");
        }
    }
}
