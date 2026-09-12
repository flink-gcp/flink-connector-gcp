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
import com.google.bigtable.admin.v2.GetAppProfileRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Bounded loopback fault boundary for correctness only; it is not a performance instrument. */
final class ProductionRecoveryProxy implements AutoCloseable {
    interface Backend {
        boolean mutate(CheckAndMutateRowRequest request) throws Exception;

        AppProfile profile(GetAppProfileRequest request) throws Exception;

        Table table(GetTableRequest request) throws Exception;
    }

    final Server server;
    final Map<ByteString, CheckAndMutateRowRequest> envelopes = new HashMap<>();
    final java.util.Set<ByteString> acknowledged = new java.util.HashSet<>();
    int responses;
    int duplicates;
    int discarded;
    int attempts;
    Throwable fatal;
    private final Backend backend;
    private final String table;
    private final String profile;

    ProductionRecoveryProxy(Backend backend, String table, String profile) throws IOException {
        this.backend = backend;
        this.table = table;
        this.profile = profile;
        server =
                io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder.forAddress(
                                new InetSocketAddress("127.0.0.1", 0))
                        .addService(
                                new BigtableGrpc.BigtableImplBase() {
                                    @Override
                                    public void checkAndMutateRow(
                                            CheckAndMutateRowRequest request,
                                            StreamObserver<CheckAndMutateRowResponse> response) {
                                        mutate(request, response);
                                    }
                                })
                        .addService(
                                StagedRpcTestService.adminService(
                                        "BigtableInstanceAdmin",
                                        "GetAppProfile",
                                        GetAppProfileRequest.getDefaultInstance(),
                                        AppProfile.getDefaultInstance(),
                                        (request, response) ->
                                                reply(
                                                        response,
                                                        () -> {
                                                            if (!request.getName()
                                                                    .equals(
                                                                            table.substring(
                                                                                            0,
                                                                                            table
                                                                                                    .indexOf(
                                                                                                            "/tables/"))
                                                                                    + "/appProfiles/"
                                                                                    + profile)) {
                                                                throw new IOException(
                                                                        "Unexpected proxy profile target");
                                                            }
                                                            return backend.profile(request);
                                                        })))
                        .addService(
                                StagedRpcTestService.adminService(
                                        "BigtableTableAdmin",
                                        "GetTable",
                                        GetTableRequest.getDefaultInstance(),
                                        Table.getDefaultInstance(),
                                        (request, response) ->
                                                reply(
                                                        response,
                                                        () -> {
                                                            if (!request.getName().equals(table)
                                                                    || request.getView()
                                                                            != Table.View
                                                                                    .SCHEMA_VIEW) {
                                                                throw new IOException(
                                                                        "Unexpected proxy table target or view");
                                                            }
                                                            return backend.table(request);
                                                        })))
                        .build()
                        .start();
    }

    String endpoint() {
        return "127.0.0.1:" + server.getPort();
    }

    private synchronized void mutate(
            CheckAndMutateRowRequest request, StreamObserver<CheckAndMutateRowResponse> response) {
        try {
            requireHealthy();
            if (!request.getTableName().equals(table)
                    || !request.getAppProfileId().equals(profile)
                    || request.getSerializedSize() > 2048
                    || ++attempts > 2048
                    || request.getFalseMutationsCount() != 2
                    || request.getTrueMutationsCount() != 0) {
                throw new IOException(
                        "Production recovery request exceeds its fixed target or reservation");
            }
            var delta = request.getFalseMutations(0).getAddToCell();
            if (!delta.getFamilyName().equals("agg")
                    || !delta.getColumnQualifier()
                            .getRawValue()
                            .equals(ByteString.copyFromUtf8("count"))
                    || delta.getInput().getIntValue() != 1
                    || delta.getTimestamp().getRawTimestampMicros() != 1000) {
                throw new IOException("Recovery SUM mutation differs from the fixed workload");
            }
            var marker = request.getFalseMutations(1).getSetCell();
            if (!marker.getFamilyName().equals("flink_commit")
                    || marker.getColumnQualifier().size() != 32
                    || marker.getTimestampMicros() != 0
                    || !marker.getValue().equals(ByteString.copyFromUtf8("1"))) {
                throw new IOException(
                        "Production marker differs from the frozen recovery contract");
            }
            CheckAndMutateRowRequest prior = envelopes.get(marker.getColumnQualifier());
            if (prior != null && !prior.equals(request)) {
                throw new IOException("Persisted envelope changed across replay");
            }
            if (prior == null && envelopes.size() >= 128) {
                throw new IOException("Recovery created more identities than the input inventory");
            }
            if (prior == null) {
                System.out.println(
                        "PRODUCTION_ENVELOPE "
                                + java.util.Base64.getEncoder()
                                        .encodeToString(request.toByteArray()));
            }
            envelopes.putIfAbsent(marker.getColumnQualifier(), request);
            boolean matched = backend.mutate(request);
            if (matched) {
                duplicates++;
            }
            responses++;
            System.out.println(
                    "PRODUCTION_RESPONSE attempt="
                            + attempts
                            + " matched="
                            + matched
                            + " discarded="
                            + (responses == 8));
            // Lose a successful service answer during a partial checkpoint collection.
            if (responses == 8) {
                discarded++;
                response.onError(
                        Status.UNAVAILABLE
                                .withDescription("Injected successful response loss")
                                .asRuntimeException());
                return;
            }
            acknowledged.add(marker.getColumnQualifier());
            response.onNext(
                    CheckAndMutateRowResponse.newBuilder().setPredicateMatched(matched).build());
            response.onCompleted();
        } catch (Exception failure) {
            fail(failure);
            response.onError(
                    Status.ABORTED
                            .withDescription("Recovery proxy failed; stop the lease")
                            .withCause(failure)
                            .asRuntimeException());
        }
    }

    synchronized void requireHealthy() throws IOException {
        if (fatal != null) {
            throw new IOException("Production recovery proxy failed", fatal);
        }
    }

    private synchronized void fail(Throwable failure) {
        if (fatal == null) {
            fatal = failure;
            System.err.println("PRODUCTION_BACKEND_FAILURE " + failure);
        }
    }

    private <T> void reply(StreamObserver<T> response, Checked<T> call) {
        try {
            requireHealthy();
            T value = call.get();
            response.onNext(value);
            response.onCompleted();
        } catch (Exception failure) {
            fail(failure);
            response.onError(
                    Status.ABORTED
                            .withDescription("Recovery metadata forwarding failed")
                            .withCause(failure)
                            .asRuntimeException());
        }
    }

    private interface Checked<T> {
        T get() throws Exception;
    }

    @Override
    public void close() throws Exception {
        server.shutdownNow();
        if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
            throw new IOException("Recovery proxy did not terminate");
        }
    }
}
