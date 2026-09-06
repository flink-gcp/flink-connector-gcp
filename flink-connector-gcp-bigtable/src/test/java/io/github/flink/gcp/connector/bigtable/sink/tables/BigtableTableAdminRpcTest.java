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

package io.github.flink.gcp.connector.bigtable.sink.tables;

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.admin.v2.Type;
import com.google.protobuf.Message;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.ColumnFamilyType;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(30)
class BigtableTableAdminRpcTest {
    private static final String SERVICE = "google.bigtable.admin.v2.BigtableTableAdmin";

    @Test
    void addingAFamilyDoesNotParseAnUnrelatedTypeInTheModificationResponse() throws Exception {
        String name = "projects/p/instances/i/tables/t";
        Table original =
                Table.newBuilder()
                        .setName(name)
                        .putColumnFamilies(
                                "unrelated",
                                ColumnFamily.newBuilder()
                                        .setValueType(
                                                Type.newBuilder()
                                                        .setStringType(
                                                                Type.String.getDefaultInstance()))
                                        .build())
                        .build();
        AtomicInteger creates = new AtomicInteger();
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger modifications = new AtomicInteger();
        AtomicReference<Table> current = new AtomicReference<>(original);
        AtomicReference<ModifyColumnFamiliesRequest> modified = new AtomicReference<>();
        Server server =
                ServerBuilder.forPort(0)
                        .addService(
                                ServerServiceDefinition.builder(SERVICE)
                                        .addMethod(
                                                method(
                                                        "CreateTable",
                                                        CreateTableRequest.getDefaultInstance(),
                                                        (request, observer) -> {
                                                            creates.incrementAndGet();
                                                            observer.onError(
                                                                    Status.ALREADY_EXISTS
                                                                            .asRuntimeException());
                                                        }))
                                        .addMethod(
                                                method(
                                                        "GetTable",
                                                        GetTableRequest.getDefaultInstance(),
                                                        (request, observer) -> {
                                                            reads.incrementAndGet();
                                                            observer.onNext(current.get());
                                                            observer.onCompleted();
                                                        }))
                                        .addMethod(
                                                method(
                                                        "ModifyColumnFamilies",
                                                        ModifyColumnFamiliesRequest
                                                                .getDefaultInstance(),
                                                        (request, observer) -> {
                                                            modified.set(request);
                                                            modifications.incrementAndGet();
                                                            Table.Builder result =
                                                                    current.get().toBuilder();
                                                            request.getModificationsList()
                                                                    .forEach(
                                                                            modification ->
                                                                                    result
                                                                                            .putColumnFamilies(
                                                                                                    modification
                                                                                                            .getId(),
                                                                                                    modification
                                                                                                            .getCreate()));
                                                            current.set(result.build());
                                                            observer.onNext(current.get());
                                                            observer.onCompleted();
                                                        }))
                                        .build())
                        .build()
                        .start();
        try (BigtableTableAdmin admin =
                new BigtableTableAdmin(
                        EmulatorEndpoint.parse(
                                "localhost:" + server.getPort(), "emulator-endpoint"))) {
            TableAdmin.EnsureResult result =
                    admin.ensureTable(
                            TableDestination.of("p", "i", "t"),
                            TableCreateOptions.builder()
                                    .columnFamily("totals", ColumnFamilyType.INT64_SUM, null)
                                    .build());
            assertThat(result.tableCreated()).isFalse();
            assertThat(result.columnFamiliesAdded()).isEqualTo(1);
            assertThat(result.existingColumnFamilies())
                    .containsExactlyInAnyOrder("unrelated", "totals");
            assertThat(creates.get()).isEqualTo(1);
            assertThat(reads.get()).isEqualTo(1);
            assertThat(modified.get().getName()).isEqualTo(name);
            assertThat(modified.get().getModificationsList())
                    .singleElement()
                    .satisfies(
                            modification -> {
                                assertThat(modification.getId()).isEqualTo("totals");
                                assertThat(modification.hasCreate()).isTrue();
                                assertThat(
                                                modification
                                                        .getCreate()
                                                        .getValueType()
                                                        .getAggregateType()
                                                        .hasSum())
                                        .isTrue();
                            });
            assertThatThrownBy(
                            () ->
                                    admin.ensureTable(
                                            TableDestination.of("p", "i", "t"),
                                            TableCreateOptions.builder()
                                                    .columnFamily(
                                                            "totals",
                                                            ColumnFamilyType.INT64_MIN,
                                                            null)
                                                    .build()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "column family 'totals' has value type int64-sum; expected int64-min.");
            assertThat(creates.get()).isEqualTo(2);
            assertThat(reads.get()).isEqualTo(2);
            assertThat(modifications.get()).isEqualTo(1);
        } finally {
            server.shutdownNow();
            assertThat(server.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static <T extends Message> ServerMethodDefinition<T, Table> method(
            String name, T defaultRequest, ServerCalls.UnaryMethod<T, Table> implementation) {
        MethodDescriptor<T, Table> descriptor =
                MethodDescriptor.<T, Table>newBuilder()
                        .setType(MethodDescriptor.MethodType.UNARY)
                        .setFullMethodName(SERVICE + "/" + name)
                        .setRequestMarshaller(ProtoUtils.marshaller(defaultRequest))
                        .setResponseMarshaller(ProtoUtils.marshaller(Table.getDefaultInstance()))
                        .build();
        return ServerMethodDefinition.create(
                descriptor, ServerCalls.asyncUnaryCall(implementation));
    }
}
