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
import com.google.bigtable.admin.v2.GcRule;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2HarnessTest {
    @TempDir Path directory;

    @Test
    void unavailableAllocationRemainsUnavailableAfterLaterSuccess() {
        var total = new java.util.concurrent.atomic.AtomicLong();
        Stage2Harness.recordAllocation(total, 10, 30);
        assertThat(total.get()).isEqualTo(20);
        Stage2Harness.recordAllocation(total, -1, -1);
        Stage2Harness.recordAllocation(total, 30, 90);
        assertThat(total.get()).isEqualTo(-1);
    }

    @Test
    void serviceReadbackWithoutLeaseFailsBeforeCredentialDiscovery() throws Exception {
        try (Stage2Harness run =
                new Stage2Harness(
                        TableDestination.of("local-project", "local-instance", "local-table"),
                        "127.0.0.1:1",
                        directory.resolve("inventory"),
                        8,
                        1024,
                        false,
                        false,
                        1,
                        false,
                        null)) {
            assertThatThrownBy(() -> run.readback(true, false))
                    .hasMessageContaining("requires an owned Stage 2 lease");
        }
    }

    @Test
    void monitoringCollisionDoesNotConsumeAnotherReadReservation() throws Exception {
        Stage2Lease lease = Stage2Lease.plan(directory.resolve("lease.properties"));
        assertThatThrownBy(lease::startedAt).hasMessageContaining("never started creation");
        Path output = directory.resolve("monitoring.jsonl");
        try (var writer = Stage2Monitoring.openCapture(lease, output, 1)) {
            writer.write("first capture");
        }
        assertThatThrownBy(() -> Stage2Monitoring.openCapture(lease, output, 1))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        lease.reserveRead((1L << 30) - 1);
        assertThat(java.nio.file.Files.readString(output)).isEqualTo("first capture");
    }

    @Test
    void workerCleanupPreservesReusedPidAndEscalatesOnlyTheOriginalWorker() throws Exception {
        FakeWorker other = new FakeWorker("replacement", false);
        Stage2Lease.stopWorker("original", other);
        assertThat(other.actions).isEmpty();
        FakeWorker graceful = new FakeWorker("original", false);
        Stage2Lease.stopWorker("original", graceful);
        assertThat(graceful.actions).containsExactly("destroy", "await:10");
        FakeWorker stuck = new FakeWorker("original", true);
        Stage2Lease.stopWorker("original", stuck);
        assertThat(stuck.actions).containsExactly("destroy", "await:10", "force", "await:20");
    }

    private static final class FakeWorker implements Stage2Lease.Worker {
        private final String started;
        private final boolean stuck;
        final java.util.List<String> actions = new java.util.ArrayList<>();

        FakeWorker(String started, boolean stuck) {
            this.started = started;
            this.stuck = stuck;
        }

        @Override
        public String started() {
            return started;
        }

        @Override
        public void destroy(boolean forcibly) {
            actions.add(forcibly ? "force" : "destroy");
        }

        @Override
        public void awaitExit(long seconds) throws Exception {
            actions.add("await:" + seconds);
            if (stuck && seconds == 10) {
                throw new java.util.concurrent.TimeoutException("original worker still alive");
            }
        }
    }

    @Test
    void failedInventorySizingClosesItsFileDescriptor() throws Exception {
        java.io.RandomAccessFile file =
                new java.io.RandomAccessFile(directory.resolve("sizing").toFile(), "rw");
        java.io.FileDescriptor descriptor = file.getFD();
        assertThatThrownBy(() -> Stage2Ledger.sizeInventory(file, -1))
                .isInstanceOf(IOException.class);
        assertThat(descriptor.valid()).isFalse();
    }

    @Test
    void readyOwnedInstanceCanBeDeletedWhenCreationResponseWasLost() throws Exception {
        FakeInstance remote = new FakeInstance("ours");
        remote.finished = false;
        remote.ready = true;
        Stage2Lease.removeOwned("ours", remote);
        assertThat(remote.deletes).isEqualTo(1);
        FakeInstance pending = new FakeInstance("ours");
        pending.finished = false;
        assertThatThrownBy(() -> Stage2Lease.removeOwned("ours", pending))
                .hasMessageContaining("unresolved");
        assertThat(pending.deletes).isZero();
    }

    @Test
    void missingCreationJournalReconcilesOnlyTheExactOwnedOperation() throws Exception {
        var request =
                com.google.bigtable.admin.v2.CreateInstanceRequest.newBuilder()
                        .setParent("projects/flink-gcp")
                        .setInstanceId("ours")
                        .setInstance(
                                com.google.bigtable.admin.v2.Instance.newBuilder()
                                        .putLabels("stage2-owner", "token"));
        var operation =
                com.google.longrunning.Operation.newBuilder()
                        .setName("operation-ours")
                        .setMetadata(
                                com.google.protobuf.Any.pack(
                                        com.google.bigtable.admin.v2.CreateInstanceMetadata
                                                .newBuilder()
                                                .setOriginalRequest(request)
                                                .build()))
                        .setDone(true)
                        .build();
        assertThat(Stage2Lease.ownedCreation("ours", "token", List.of(operation)))
                .isEqualTo(operation);
        assertThat(Stage2Lease.ownedCreation("other", "token", List.of(operation))).isNull();
        assertThat(Stage2Lease.ownedCreation("ours", "different", List.of(operation))).isNull();
        var pending = operation.toBuilder().setName("pending").setDone(false).build();
        assertThat(Stage2Lease.ownedCreation("ours", "token", List.of(operation, pending)))
                .isEqualTo(pending);
    }

    @Test
    void sumInventoryOracleDetectsMissingAggregateEvenWhenEnvelopesApplied() throws Exception {
        try (Stage2Harness run = local(false)) {
            run.ledger.window(100, 200);
            run.ledger.admit(0, 110);
            StagedMutationTestSink.Writer writer = new StagedMutationTestSink.Writer(1, 100_000);
            writer.write(run.input(0), null);
            var wire = writer.prepareCommit().iterator().next();
            var withoutAggregate = wire.toBuilder().clearFalseMutations();
            for (var mutation : wire.getFalseMutationsList()) {
                if (!mutation.hasAddToCell()) {
                    withoutAggregate.addFalseMutations(mutation);
                }
            }
            run.store.apply(withoutAggregate.build());
            run.ledger.acknowledge(0, 150);
            assertThat(run.store.applied).isEqualTo(1);
            assertThatThrownBy(run::verifyFakeSums).hasMessageContaining("SUM readback");
        }
    }

    @Test
    void measuredInventoryIncludesDrainButExcludesWarmupTailAndDuplicateAcks() throws Exception {
        try (Stage2Ledger ledger = new Stage2Ledger(directory.resolve("ledger"), 4, 256)) {
            ledger.window(100, 200);
            ledger.admit(0, 90);
            ledger.admit(1, 110);
            ledger.admit(2, 210);
            ledger.acknowledge(0, 250);
            ledger.acknowledge(1, 310);
            assertThat(ledger.acknowledge(1, 700)).isFalse();
            ledger.acknowledge(2, 900);
            Stage2Ledger.Summary summary = ledger.summary();
            assertThat(summary.count).isEqualTo(1);
            assertThat(ledger.measuredCount()).isEqualTo(1);
            assertThat(summary.throughput).isEqualTo(1_000_000_000.0 / 210);
            assertThat(summary.p95).isEqualTo(200);
            assertThat(summary.drainNanos).isEqualTo(110);
            assertThat(ledger.entry(0).phase).isEqualTo(Stage2Ledger.WARMUP);
            assertThat(ledger.entry(2).phase).isEqualTo(Stage2Ledger.TAIL);
            assertThat(ledger.acknowledgedCount()).isEqualTo(3);
        }
    }

    @Test
    void rejectsUndrainedUnknownAndOutOfOrderAcknowledgements() throws Exception {
        try (Stage2Ledger ledger = new Stage2Ledger(directory.resolve("ledger"), 2, 128)) {
            ledger.window(100, 200);
            ledger.admit(0, 110);
            assertThatThrownBy(ledger::summary).hasMessageContaining("Undrained");
            assertThatThrownBy(() -> ledger.acknowledge(1, 150))
                    .hasMessageContaining("preceding admission");
            assertThatThrownBy(() -> ledger.acknowledge(0, 100))
                    .hasMessageContaining("preceding admission");
            assertThatThrownBy(() -> ledger.admit(2, 150))
                    .hasMessageContaining("capacity exhausted");
        }
    }

    @Test
    void inventoryStorageIsFixedAndAnExistingInventoryIsNotOverwritten() throws Exception {
        Path file = directory.resolve("ledger");
        try (Stage2Ledger ledger = new Stage2Ledger(file, 16, 1024)) {
            ledger.window(100, 200);
            for (int i = 0; i < 16; i++) {
                ledger.admit(i, 110);
                ledger.acknowledge(i, 150);
                ledger.acknowledge(i, 200);
            }
            assertThat(Files.size(file)).isEqualTo(1024);
            assertThatThrownBy(() -> new Stage2Ledger(file, 16, 1024))
                    .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        }
        assertThatThrownBy(() -> new Stage2Ledger(directory.resolve("too-large"), 17, 1024))
                .hasMessageContaining("storage budget");
        assertThat(directory.resolve("too-large")).doesNotExist();
    }

    @Test
    void persistedIdentityCannotBeSilentlyRegenerated() throws Exception {
        try (Stage2Ledger ledger = new Stage2Ledger(directory.resolve("ledger"), 1, 64)) {
            ledger.window(100, 200);
            ledger.admit(0, 110);
            ledger.marker(0, "0123456789abcdef0123456789abcdef");
            ledger.marker(0, "0123456789abcdef0123456789abcdef");
            assertThatThrownBy(() -> ledger.marker(0, "1123456789abcdef0123456789abcdef"))
                    .hasMessageContaining("different envelope identity");
            assertThatThrownBy(() -> ledger.marker(0, "wrong"))
                    .hasMessageContaining("Invalid persisted marker");
        }
    }

    @Test
    void metadataRejectsRoutingGcTypedAndMissingMarkerBeforeAnySend() throws Exception {
        AppProfile valid =
                AppProfile.newBuilder()
                        .setSingleClusterRouting(
                                AppProfile.SingleClusterRouting.newBuilder()
                                        .setClusterId("c")
                                        .setAllowTransactionalWrites(true))
                        .build();
        Table table =
                Table.newBuilder()
                        .putColumnFamilies(
                                StagedMutationTestSink.MARKER_FAMILY,
                                ColumnFamily.getDefaultInstance())
                        .build();
        Stage2Preflight.validate(valid, table);
        Stage2Preflight.validate(
                valid,
                table.toBuilder()
                        .putColumnFamilies(
                                StagedMutationTestSink.MARKER_FAMILY,
                                ColumnFamily.newBuilder()
                                        .setGcRule(GcRule.getDefaultInstance())
                                        .setValueType(
                                                com.google.bigtable.admin.v2.Type
                                                        .getDefaultInstance())
                                        .build())
                        .build());
        Table bytesTyped =
                table.toBuilder()
                        .putColumnFamilies(
                                StagedMutationTestSink.MARKER_FAMILY,
                                ColumnFamily.newBuilder()
                                        .setValueType(
                                                com.google.bigtable.admin.v2.Type.newBuilder()
                                                        .setBytesType(
                                                                com.google.bigtable.admin.v2.Type
                                                                        .Bytes
                                                                        .getDefaultInstance()))
                                        .build())
                        .build();
        assertThatThrownBy(() -> Stage2Preflight.validate(valid, bytesTyped))
                .hasMessageContaining("raw marker family");
        assertThatThrownBy(() -> Stage2Preflight.validate(AppProfile.getDefaultInstance(), table))
                .hasMessageContaining("single-cluster");
        assertThatThrownBy(
                        () ->
                                Stage2Preflight.validate(
                                        valid.toBuilder()
                                                .setSingleClusterRouting(
                                                        valid.getSingleClusterRouting().toBuilder()
                                                                .setAllowTransactionalWrites(false))
                                                .build(),
                                        table))
                .hasMessageContaining("transactional");
        assertThatThrownBy(() -> Stage2Preflight.validate(valid, Table.getDefaultInstance()))
                .hasMessageContaining("raw marker family");
        Table gc =
                table.toBuilder()
                        .putColumnFamilies(
                                StagedMutationTestSink.MARKER_FAMILY,
                                ColumnFamily.newBuilder()
                                        .setGcRule(GcRule.newBuilder().setMaxNumVersions(1))
                                        .build())
                        .build();
        assertThatThrownBy(() -> Stage2Preflight.validate(valid, gc))
                .hasMessageContaining("GC rule");
        Table typed =
                table.toBuilder()
                        .putColumnFamilies(
                                StagedMutationTestSink.MARKER_FAMILY,
                                ColumnFamily.newBuilder()
                                        .setValueType(
                                                com.google.bigtable.admin.v2.Type.newBuilder()
                                                        .setAggregateType(
                                                                com.google.bigtable.admin.v2.Type
                                                                        .Aggregate
                                                                        .getDefaultInstance()))
                                        .build())
                        .build();
        assertThatThrownBy(() -> Stage2Preflight.validate(valid, typed))
                .hasMessageContaining("raw marker family");
    }

    @Test
    void restoredEnvelopeValidationRejectsBrokenProtectionBeforeTransport() throws Exception {
        try (Stage2Harness run = local(false)) {
            run.ledger.window(100, 200);
            run.ledger.admit(0, 110);
            StagedMutationTestSink.Writer writer = new StagedMutationTestSink.Writer(10, 100_000);
            writer.write(run.input(0), null);
            CheckAndMutateRowRequest wire = writer.prepareCommit().iterator().next();
            run.prepared(List.of(wire));
            assertThat(run.serializer().deserialize(1, wire.toByteArray())).isEqualTo(wire);
            assertThatThrownBy(() -> run.serializer().deserialize(2, wire.toByteArray()))
                    .hasMessageContaining("Unsupported probe version");
            assertThatThrownBy(
                            () ->
                                    run.serializer()
                                            .deserialize(
                                                    1,
                                                    wire.toBuilder()
                                                            .clearPredicateFilter()
                                                            .build()
                                                            .toByteArray()))
                    .hasMessageContaining("predicate");
            assertThatThrownBy(
                            () ->
                                    Stage2Preflight.envelope(
                                            wire.toBuilder()
                                                    .addTrueMutations(wire.getFalseMutations(0))
                                                    .build()))
                    .hasMessageContaining("structure");
            assertThat(run.attemptsCount.get()).isZero();
        }
    }

    @Test
    void sustainedReceiverAndMeasurementsDoNotRetainCompletedRequests() throws Exception {
        try (Stage2Harness run = local(true)) {
            run.startWindow(System.nanoTime(), 0, 1_000_000_000);
            for (int i = 0; i < 100; i++) {
                run.admitted(i);
                StagedMutationTestSink.Writer writer = new StagedMutationTestSink.Writer(1, 10_000);
                writer.write(run.input(i), null);
                CheckAndMutateRowRequest wire = writer.prepareCommit().iterator().next();
                run.prepared(List.of(wire));
                var future = run.fake(wire);
                run.trace(wire, future);
                future.get();
                run.clientCompleted(i, 100, System.nanoTime());
                run.acknowledged(i, System.nanoTime());
            }
            assertThat(run.attempts).isEmpty();
            assertThat(run.originalFutures).isEmpty();
            assertThat(run.clientCompletionNanos).isEmpty();
            assertThat(run.admissions).isEmpty();
            assertThat(run.acknowledgements).isEmpty();
            assertThat(run.store.sent).isEmpty();
            assertThat(run.store.cells).isEmpty();
            assertThat(run.acknowledgedCount()).isEqualTo(100);
            assertThat(run.clientQuantileUpperBound(.95)).isBetween(100L, 102L);
        }
    }

    @Test
    void frozenWindowAndDistributionHaveDeterministicBoundaries() throws Exception {
        Stage2Harness.checkDistribution();
        try (Stage2Harness run = local(true)) {
            run.startWindow(100, 10, 20);
            assertThat(run.windowEnded(129)).isFalse();
            assertThat(run.windowEnded(130)).isTrue();
        }
    }

    @Test
    void sumNegativeControlDetectsAnUnprotectedReplay() throws Exception {
        try (Stage2Harness run = local(false)) {
            run.ledger.window(100, 200);
            run.ledger.admit(0, 110);
            StagedMutationTestSink.Writer writer = new StagedMutationTestSink.Writer(2, 10_000);
            writer.write(run.input(0), null);
            CheckAndMutateRowRequest request = writer.prepareCommit().iterator().next();
            assertThat(run.store.apply(request)).isFalse();
            assertThat(run.store.apply(request)).isTrue();
            assertThat(run.store.sum(request.getTableName(), request.getRowKey().toStringUtf8()))
                    .isEqualTo(1);
            var broken =
                    request.toBuilder()
                            .setPredicateFilter(
                                    request.getPredicateFilter().toBuilder()
                                            .setChain(
                                                    request
                                                            .getPredicateFilter()
                                                            .getChain()
                                                            .toBuilder()
                                                            .setFilters(
                                                                    1,
                                                                    com.google.bigtable.v2.RowFilter
                                                                            .newBuilder()
                                                                            .setColumnQualifierRegexFilter(
                                                                                    ByteString
                                                                                            .copyFromUtf8(
                                                                                                    "absent")))))
                            .build();
            assertThat(run.store.apply(broken)).isFalse();
            assertThat(run.store.sum(request.getTableName(), request.getRowKey().toStringUtf8()))
                    .isEqualTo(2);
        }
    }

    @Test
    void exactOwnerCleanupTreatsDisappearanceAsAbsentAndRefusesOtherOwners() throws Exception {
        FakeInstance remote = new FakeInstance("ours");
        Stage2Lease.removeOwned("ours", remote);
        assertThat(remote.deletes).isEqualTo(1);
        Stage2Lease.removeOwned("ours", remote);
        assertThat(remote.deletes).isEqualTo(1);
        FakeInstance other = new FakeInstance("other");
        assertThatThrownBy(() -> Stage2Lease.removeOwned("ours", other))
                .hasMessageContaining("Ownership mismatch");
        assertThat(other.deletes).isZero();
    }

    @Test
    void cleanupDoesNotMistakePendingCreationOrPermissionDenialForAbsence() {
        FakeInstance pending = new FakeInstance(null);
        pending.finished = false;
        assertThatThrownBy(() -> Stage2Lease.removeOwned("ours", pending))
                .hasMessageContaining("unresolved");
        FakeInstance denied = new FakeInstance("ours");
        denied.denied = true;
        assertThatThrownBy(() -> Stage2Lease.removeOwned("ours", denied))
                .hasMessageContaining("PERMISSION_DENIED");
        assertThat(denied.deletes).isZero();
        FakeInstance leftover = new FakeInstance("ours");
        leftover.deleteWorks = false;
        assertThatThrownBy(() -> Stage2Lease.removeOwned("ours", leftover))
                .hasMessageContaining("remains");
    }

    @Test
    void manifestBudgetsAndAdmissionDeadlineSurviveReopening() throws Exception {
        Stage2Lease lease = Stage2Lease.plan(directory.resolve("lease.properties"));
        assertThatThrownBy(() -> lease.reserve(1, 1)).hasMessageContaining("not active");
        lease.update(
                properties -> {
                    properties.setProperty("phase", "ACTIVE");
                    properties.setProperty("startedAt", Long.toString(System.currentTimeMillis()));
                });
        lease.reserve(249_999, 1024);
        Stage2Lease reopened = new Stage2Lease(lease.manifest);
        assertThat(reopened.reserve(1, 1024)).isEqualTo(1);
        assertThatThrownBy(() -> reopened.reserve(1, 1)).hasMessageContaining("budget exhausted");
        assertThatThrownBy(
                        () ->
                                lease.requireTarget(
                                        TableDestination.of("flink-gcp", "another", "hot")))
                .hasMessageContaining("not owned");
        lease.claim("hot");
        assertThatThrownBy(() -> lease.claim("hot")).hasMessageContaining("already claimed");
        lease.update(
                properties ->
                        properties.setProperty(
                                "startedAt",
                                Long.toString(System.currentTimeMillis() - 46 * 60_000L)));
        assertThatThrownBy(() -> lease.claim("bulk-r1")).hasMessageContaining("admission deadline");
        lease.update(properties -> properties.setProperty("startedAt", "1"));
        assertThatThrownBy(lease::requireLive).hasMessageContaining("deadline expired");
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
    void metadataPermissionFailureStopsTheRestoredSendBeforeSdkCreation(boolean bulk)
            throws Exception {
        Stage2Lease lease = Stage2Lease.plan(directory.resolve("lease.properties"));
        lease.update(
                properties -> {
                    properties.setProperty("phase", "ACTIVE");
                    properties.setProperty("startedAt", Long.toString(System.currentTimeMillis()));
                });
        try (Stage2Harness run =
                new Stage2Harness(
                        lease.table("staged-r1"),
                        "127.0.0.1:1",
                        directory.resolve("inventory"),
                        8,
                        1024,
                        false,
                        false,
                        1,
                        false,
                        lease)) {
            run.metadataValidator = (destination, profile) -> {};
            if (bulk) {
                run.batcherFactory();
            } else {
                run.singleRowFactory();
            }
            run.metadataValidator =
                    (destination, profile) -> {
                        throw new IOException("PERMISSION_DENIED metadata");
                    };
            run.ledger.window(100, 200);
            run.ledger.admit(0, 110);
            StagedMutationTestSink.Writer writer = new StagedMutationTestSink.Writer(1, 10_000);
            writer.write(run.input(0), null);
            CheckAndMutateRowRequest wire = writer.prepareCommit().iterator().next();
            CheckAndMutateRowRequest restored = run.serializer().deserialize(1, wire.toByteArray());
            if (bulk) {
                assertThatThrownBy(run::batcherFactory).hasMessageContaining("PERMISSION_DENIED");
            } else {
                assertThatThrownBy(run::singleRowFactory).hasMessageContaining("PERMISSION_DENIED");
            }
            assertThatThrownBy(() -> run.beforeSend(restored))
                    .hasMessageContaining("PERMISSION_DENIED");
            assertThat(run.attemptsCount.get()).isZero();
        }
    }

    @Test
    void clientLatencyMetricsExcludeWarmupAndKeepMissingMetricsMissing() throws Exception {
        try (Stage2Harness run = local(true)) {
            run.ledger.window(100, 200);
            run.ledger.admit(0, 90);
            run.ledger.admit(1, 110);
            run.clientCompleted(0, 10_000, 10_090);
            run.clientCompleted(1, 100, 210);
            assertThat(run.clientCompletions.get()).isEqualTo(1);
            assertThat(run.clientQuantileUpperBound(.95)).isBetween(100L, 102L);
        }
        var mapper =
                new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper();
        assertThat(Stage2Sampler.metricNames(mapper.readTree("[]"))).isEmpty();
        assertThat(
                        Stage2Sampler.metricNames(
                                mapper.readTree(
                                        "[{\"id\":\"Sink: Committer.pendingCommittables\"},{\"id\":\"unrelated\"}]")))
                .containsExactly("Sink: Committer.pendingCommittables");
    }

    @Test
    void constructorFailureDoesNotRegisterAnUnusableRun() throws Exception {
        int before = LocalStagedHarness.RUNS.size();
        Files.createFile(directory.resolve("inventory"));
        assertThatThrownBy(() -> local(true))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        assertThat(LocalStagedHarness.RUNS).hasSize(before);
    }

    @Test
    void directoryBudgetCountsCurrentFilesAndRejectsAMissingRoot() throws Exception {
        Path root = Files.createDirectory(directory.resolve("checkpoints"));
        Files.write(root.resolve("old"), new byte[1024]);
        Files.write(root.resolve("new"), new byte[2048]);
        assertThat(BigtableStage2Probe.directorySize(root)).isEqualTo(3072);
        Files.delete(root.resolve("old"));
        assertThat(BigtableStage2Probe.directorySize(root)).isEqualTo(2048);
        assertThatThrownBy(() -> BigtableStage2Probe.directorySize(directory.resolve("missing")))
                .hasMessageContaining("disappeared");
    }

    private Stage2Harness local(boolean timed) throws IOException {
        return new Stage2Harness(
                TableDestination.of("local-project", "local-instance", "local-table"),
                "127.0.0.1:1",
                directory.resolve("inventory"),
                128,
                1024,
                true,
                true,
                4,
                timed,
                null);
    }

    private static final class FakeInstance implements Stage2Lease.OwnedInstance {
        String owner;
        int deletes;
        boolean finished = true;
        boolean denied;
        boolean ready;
        boolean deleteWorks = true;

        FakeInstance(String owner) {
            this.owner = owner;
        }

        @Override
        public String owner() throws IOException {
            if (denied) {
                throw new IOException("PERMISSION_DENIED");
            }
            return owner;
        }

        @Override
        public boolean creationFinished() {
            return finished;
        }

        @Override
        public boolean ready() {
            return ready;
        }

        @Override
        public void delete() {
            deletes++;
            if (deleteWorks) {
                owner = null;
            }
        }

        @Override
        public boolean listed() {
            return owner != null;
        }
    }
}
