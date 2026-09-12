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
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProductionRecoveryProxyTest {
    @TempDir Path directory;

    @Test
    void productionLeaseRefusesExperimentalWorkersBeforeAnyCredentialsOrClaim() throws Exception {
        var lease = Stage2Lease.plan(directory.resolve("lease.properties"), true);
        assertThat(lease.tables).hasSize(7).contains("recovery-table-flink1");
        assertThatThrownBy(
                        () ->
                                BigtableStage2Probe.main(
                                        new String[] {
                                            "service",
                                            lease.manifest.toString(),
                                            "recovery-table-flink1"
                                        }))
                .hasMessageContaining("Experimental workers");
        assertThatThrownBy(
                        () ->
                                BigtableProductionRecoveryProbe.main(
                                        new String[] {
                                            "service",
                                            lease.manifest.toString(),
                                            "recovery-datastream-unsupported"
                                        }))
                .hasMessageContaining("supported Flink line");
        lease.update(properties -> properties.setProperty("tables", "bulk-r1"));
        assertThatThrownBy(() -> new Stage2Lease(lease.manifest))
                .hasMessageContaining("fixed profile");
    }

    @Test
    void unexpectedDestinationPermanentlyStopsProxyBeforeBackendWrites() throws Exception {
        var backend = new FakeBackend();
        try (var proxy =
                new ProductionRecoveryProxy(
                        backend, "projects/p/instances/i/tables/t", "profile")) {
            var channel = ManagedChannelBuilder.forTarget(proxy.endpoint()).usePlaintext().build();
            try {
                var stub = BigtableGrpc.newBlockingStub(channel);
                assertThatThrownBy(
                                () ->
                                        stub.checkAndMutateRow(
                                                CheckAndMutateRowRequest.newBuilder()
                                                        .setTableName(
                                                                "projects/p/instances/i/tables/other")
                                                        .build()))
                        .hasMessageContaining("ABORTED");
                assertThatThrownBy(proxy::requireHealthy).hasMessageContaining("proxy failed");
                assertThat(backend.probe.sent).isEmpty();
            } finally {
                channel.shutdownNow();
                assertThat(channel.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS))
                        .isTrue();
            }
        }
    }

    @Test
    void failedBackendIsNotRetriedThroughTheProxy() throws Exception {
        var backend = new FakeBackend();
        backend.disappeared = true;
        try (var run = new LocalStagedHarness(32, true, true, 1);
                var proxy =
                        new ProductionRecoveryProxy(
                                backend,
                                LocalStagedHarness.tableName(run.table),
                                LocalStagedHarness.PROFILE)) {
            var input = run.input(0);
            var request =
                    io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableCommittable.stage(
                                    run.table,
                                    LocalStagedHarness.PROFILE,
                                    "flink_commit",
                                    com.google.bigtable.v2.MutateRowsRequest.Entry.newBuilder()
                                            .setRowKey(input.row())
                                            .addMutations(input.mutations().get(1))
                                            .build(),
                                    new java.security.SecureRandom())
                            .getRequest();
            var channel = ManagedChannelBuilder.forTarget(proxy.endpoint()).usePlaintext().build();
            try {
                var stub = BigtableGrpc.newBlockingStub(channel);
                assertThatThrownBy(() -> stub.checkAndMutateRow(request))
                        .hasMessageContaining("ABORTED");
                assertThatThrownBy(() -> stub.checkAndMutateRow(request))
                        .hasMessageContaining("ABORTED");
                assertThat(backend.calls).isEqualTo(1);
                assertThat(proxy.discarded).isZero();
                assertThat(backend.probe.sent).isEmpty();
            } finally {
                channel.shutdownNow();
                assertThat(channel.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS))
                        .isTrue();
            }
        }
    }

    @Test
    void fixedWorkerOrderAndReservationSurviveReplacement() throws Exception {
        var lease = Stage2Lease.plan(directory.resolve("lease.properties"), true);
        lease.update(
                properties -> {
                    properties.setProperty("phase", "ACTIVE");
                    properties.setProperty("startedAt", Long.toString(System.currentTimeMillis()));
                });
        assertThatThrownBy(() -> lease.claim("recovery-table-flink1"))
                .hasMessageContaining("Previous production");
        lease.claim("recovery-datastream-flink1");
        assertThatThrownBy(() -> lease.claim("recovery-datastream-flink1"))
                .hasMessageContaining("already claimed");
        lease.reserve(8192, 16L << 20);
        var replacement = new Stage2Lease(lease.manifest);
        assertThatThrownBy(() -> replacement.reserve(1, 1))
                .hasMessageContaining("budget exhausted");
        assertThatThrownBy(() -> replacement.claim("marker-gc"))
                .hasMessageContaining("four recovery cells");
        lease.update(
                properties -> properties.setProperty("run.recovery-datastream-flink1", "PASS"));
        assertThatThrownBy(() -> replacement.claim("recovery-table-flink1"))
                .hasMessageContaining("still running");
    }

    static final class FakeBackend implements ProductionRecoveryProxy.Backend {
        boolean disappeared;
        int calls;
        final StagedMutationTestSink.Probe probe = new StagedMutationTestSink.Probe();

        @Override
        public boolean mutate(CheckAndMutateRowRequest request) throws Exception {
            calls++;
            if (disappeared) {
                throw io.grpc.Status.NOT_FOUND.asRuntimeException();
            }
            var delta = request.getFalseMutations(0).getAddToCell();
            if (!delta.getFamilyName().equals("agg")
                    || delta.getInput().getIntValue() != 1
                    || delta.getTimestamp().getRawTimestampMicros() != 1000) {
                throw new IOException(
                        "SUM input or fixed bucket differs from the generated workload");
            }
            return probe.apply(request);
        }

        @Override
        public AppProfile profile(GetAppProfileRequest request) {
            return AppProfile.newBuilder()
                    .setName(request.getName())
                    .setSingleClusterRouting(
                            AppProfile.SingleClusterRouting.newBuilder()
                                    .setClusterId("cluster")
                                    .setAllowTransactionalWrites(true))
                    .build();
        }

        @Override
        public Table table(GetTableRequest request) {
            return Table.newBuilder()
                    .setName(request.getName())
                    .putAllColumnFamilies(
                            Map.of(
                                    "flink_commit",
                                    ColumnFamily.getDefaultInstance(),
                                    "agg",
                                    BigtableStagedTableRuntimeTest.sumFamily()))
                    .build();
        }

        List<Row> rows() {
            synchronized (probe) {
                List<Row> rows = new ArrayList<>();
                probe.cells.forEach(
                        (name, stored) -> {
                            List<RowCell> cells = new ArrayList<>();
                            stored.forEach(
                                    (column, value) -> {
                                        String[] parts = column.split(":", 2);
                                        cells.add(
                                                RowCell.create(
                                                        parts[0],
                                                        ByteString.copyFromUtf8(parts[1]),
                                                        parts[0].equals("agg") ? 1000 : 0,
                                                        List.of(),
                                                        parts[0].equals("agg")
                                                                ? ByteString.copyFrom(
                                                                        ByteBuffer.allocate(8)
                                                                                .putLong(
                                                                                        Long
                                                                                                .parseLong(
                                                                                                        value
                                                                                                                .toStringUtf8()))
                                                                                .array())
                                                                : value));
                                    });
                            rows.add(
                                    Row.create(
                                            ByteString.copyFromUtf8(
                                                    name.substring(name.lastIndexOf('/') + 1)),
                                            cells));
                        });
                return rows;
            }
        }
    }
}
