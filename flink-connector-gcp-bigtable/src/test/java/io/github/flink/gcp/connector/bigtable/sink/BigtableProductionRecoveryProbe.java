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
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.tables.BigtableStagedTableValidator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Standalone production correctness acceptance; requires a separately reviewed execution freeze.
 */
public final class BigtableProductionRecoveryProbe {
    private BigtableProductionRecoveryProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("plan")) {
            Stage2Lease lease = Stage2Lease.plan(Path.of(args[1]), true);
            System.out.println("PRODUCTION_RECOVERY_PLAN " + lease.instance + " " + lease.tables);
            return;
        }
        if (args.length < 2 || args.length > 3) {
            throw new IllegalArgumentException(
                    "Commands: plan|create|preflight manifest; service manifest recovery-table");
        }
        Stage2Lease lease = new Stage2Lease(Path.of(args[1]));
        if (!lease.productionRecovery) {
            throw new IllegalArgumentException("A fresh production-recovery lease is required");
        }
        try {
            if (args.length == 2 && args[0].equals("create")) {
                lease.create();
            } else if (args.length == 2 && args[0].equals("preflight")) {
                preflight(lease);
            } else if (args.length == 3 && args[0].equals("service")) {
                String table = args[2];
                String line =
                        org.apache.flink.runtime.util.EnvironmentInformation.getVersion()
                                        .equals("1.20.4")
                                ? "flink1"
                                : org.apache.flink.runtime.util.EnvironmentInformation.getVersion()
                                                .equals("2.2.1")
                                        ? "flink2"
                                        : "unsupported";
                if (!table.matches("recovery-(datastream|table)-" + line)) {
                    throw new IllegalArgumentException(
                            "Worker table must match the fixed API and supported Flink line");
                }
                lease.claim(table);
                service(lease, table);
            } else {
                throw new IllegalArgumentException("Unknown production recovery command");
            }
        } catch (Exception failure) {
            try {
                Files.writeString(
                        lease.manifest.resolveSibling("stop"),
                        "Production recovery failed; stop exact owned lease");
            } catch (IOException signalFailure) {
                failure.addSuppressed(signalFailure);
            }
            throw failure;
        }
    }

    private static void preflight(Stage2Lease lease) throws Exception {
        lease.requireLive();
        lease.update(
                properties -> {
                    if (properties.containsKey("productionPreflight")) {
                        throw new IllegalStateException(
                                "Production preflight was already attempted");
                    }
                    properties.setProperty("productionPreflight", "STARTED");
                });
        lease.reserveRead(1L << 20);
        var validator = new BigtableStagedTableValidator(null, null);
        var valid = lease.table("recovery-datastream-flink2");
        validator.validate(
                valid,
                LocalStagedHarness.PROFILE,
                "flink_commit",
                Map.of("agg", ColumnFamilyType.INT64_SUM));
        for (String profile : new String[] {"no-tx", "multi-cluster"}) {
            reject(validator, valid, profile, "single-cluster");
        }
        for (String table : new String[] {"marker-missing", "marker-gc", "marker-typed"}) {
            reject(validator, lease.table(table), LocalStagedHarness.PROFILE, "marker");
        }
        lease.update(properties -> properties.setProperty("productionPreflight", "PASS"));
        System.out.println("PRODUCTION_PREFLIGHT PASS routing-and-marker-metadata");
    }

    private static void reject(
            BigtableStagedTableValidator validator,
            TableDestination table,
            String profile,
            String expected)
            throws Exception {
        try {
            validator.validate(table, profile, "flink_commit", Map.of());
        } catch (IOException failure) {
            if (!failure.getMessage().contains(expected)) {
                throw failure;
            }
            return;
        }
        throw new IOException(
                "Production preflight accepted invalid metadata: " + table + " " + profile);
    }

    private static void service(Stage2Lease lease, String table) throws Exception {
        lease.update(
                properties -> {
                    if (!properties.getProperty("productionPreflight", "").equals("PASS")) {
                        throw new IllegalStateException(
                                "Production metadata preflight must pass before worker admission");
                    }
                });
        lease.reserve(2048, 2048L * 2048);
        lease.reserveRead(8L << 20);
        var destination = lease.table(table);
        var settings =
                BigtableDataSettings.newBuilder()
                        .setProjectId(destination.getProject())
                        .setInstanceId(destination.getInstance())
                        .setAppProfileId(LocalStagedHarness.PROFILE);
        settings.stubSettings()
                .checkAndMutateRowSettings()
                .setSimpleTimeoutNoRetriesDuration(Duration.ofSeconds(20));
        settings.stubSettings()
                .readRowsSettings()
                .setRetryableCodes(java.util.Collections.emptySet());
        Path directory = lease.work.resolve(table);
        Files.createDirectories(lease.work);
        Files.createDirectory(directory);
        try (var instances = BigtableInstanceAdminClient.create(destination.getProject());
                var tables =
                        BigtableTableAdminClient.create(
                                destination.getProject(), destination.getInstance());
                var data = BigtableDataClient.create(settings.build());
                var run = ProductionRecoveryJob.newRun(destination)) {
            var backend =
                    new ProductionRecoveryProxy.Backend() {
                        long metadataBytes;
                        int metadataCalls;

                        private void checkMetadata(int bytes) throws IOException {
                            metadataBytes += bytes;
                            if (++metadataCalls > 256 || metadataBytes > (1L << 20)) {
                                throw new IOException("Recovery metadata reservation exhausted");
                            }
                        }

                        @Override
                        public boolean mutate(CheckAndMutateRowRequest request) throws Exception {
                            lease.requireTarget(destination);
                            if (Files.exists(lease.manifest.resolveSibling("stop"))) {
                                throw new IOException("Lease stopped");
                            }
                            return data.checkAndMutateRow(
                                    ConditionalRowMutation.fromProto(request));
                        }

                        @Override
                        public AppProfile profile(GetAppProfileRequest request) throws Exception {
                            lease.requireLive();
                            var response = instances.getBaseClient().getAppProfile(request);
                            checkMetadata(response.getSerializedSize());
                            return response;
                        }

                        @Override
                        public Table table(GetTableRequest request) throws Exception {
                            lease.requireLive();
                            var response = tables.getBaseClient().getTable(request);
                            checkMetadata(response.getSerializedSize());
                            return response;
                        }
                    };
            try (var proxy =
                    new ProductionRecoveryProxy(
                            backend,
                            LocalStagedHarness.tableName(destination),
                            LocalStagedHarness.PROFILE)) {
                ProductionRecoveryJob.run(
                        run,
                        proxy,
                        directory,
                        table.startsWith("recovery-table"),
                        () -> {
                            lease.requireLive();
                            instances.getInstance(lease.instance);
                            verifyReadback(
                                    run, proxy, data.readRows(Query.create(table).limit(129)));
                            if (BigtableStage2Probe.directorySize(lease.work) > (2L << 30)) {
                                throw new IOException("Recovery local storage cap exhausted");
                            }
                        });
            }
        }
        lease.update(properties -> properties.setProperty("run." + table, "PASS"));
    }

    static void verifyReadback(
            LocalStagedHarness run, ProductionRecoveryProxy proxy, Iterable<Row> rows)
            throws IOException {
        Map<ByteString, CheckAndMutateRowRequest> envelopes;
        synchronized (proxy) {
            proxy.requireHealthy();
            envelopes = Map.copyOf(proxy.envelopes);
        }
        Map<ByteString, Long> expected = new HashMap<>();
        for (long number = 0; number < 128; number++) {
            expected.merge(run.input(number).row(), 1L, Long::sum);
        }
        Set<ByteString> observedRows = new HashSet<>();
        Set<ByteString> markers = new HashSet<>();
        long bytes = 0;
        for (Row row : rows) {
            if (!observedRows.add(row.getKey()) || !expected.containsKey(row.getKey())) {
                throw new IOException("Recovery returned an unexpected or duplicate row");
            }
            bytes += row.getKey().size();
            int sums = 0;
            for (var cell : row.getCells()) {
                bytes +=
                        cell.getValue().size()
                                + cell.getQualifier().size()
                                + cell.getFamily().length()
                                + 8;
                if (bytes > (1L << 20)) {
                    throw new IOException("Recovery readback reservation exhausted");
                }
                if (cell.getFamily().equals("agg")) {
                    if (++sums != 1
                            || !cell.getQualifier().equals(ByteString.copyFromUtf8("count"))
                            || cell.getTimestamp() != 1000
                            || cell.getValue().size() != 8
                            || ByteBuffer.wrap(cell.getValue().toByteArray()).getLong()
                                    != expected.get(row.getKey())) {
                        throw new IOException(
                                "Recovery SUM differs from distinct input contributions");
                    }
                } else if (cell.getFamily().equals("flink_commit")) {
                    CheckAndMutateRowRequest envelope = envelopes.get(cell.getQualifier());
                    if (envelope == null
                            || !envelope.getRowKey().equals(row.getKey())
                            || !markers.add(cell.getQualifier())
                            || cell.getTimestamp() != 0
                            || !cell.getValue().equals(ByteString.copyFromUtf8("1"))) {
                        throw new IOException(
                                "Recovery marker differs from its persisted envelope");
                    }
                } else {
                    throw new IOException("Recovery returned an unexpected application family");
                }
            }
            if (sums != 1) {
                throw new IOException("Recovery SUM cell is missing");
            }
        }
        if (!observedRows.equals(expected.keySet())
                || markers.size() != 128
                || envelopes.size() != 128) {
            throw new IOException("Recovery readback is empty or incomplete");
        }
        System.out.println(
                "PRODUCTION_READBACK PASS inputs=128 markers=128 rows=" + observedRows.size());
    }
}
