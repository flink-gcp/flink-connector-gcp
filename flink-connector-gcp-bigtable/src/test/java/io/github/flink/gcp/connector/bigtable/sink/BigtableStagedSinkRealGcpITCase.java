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

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.admin.v2.models.Type;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.DefaultSingleRowClientFactory;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.RequestFailures;
import io.github.flink.gcp.connector.bigtable.sink.tables.BigtableStagedTableAdmin;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Native-transport acceptance of the checkpoint-owned {@code EXACTLY_ONCE} sink against real
 * Bigtable, through both production API entry points.
 *
 * <p>The production recovery lease reaches the service through a loopback proxy on {@code
 * emulatorEndpoint(...)}, which is what lets it discard one successful response deterministically;
 * the connector's own TLS and application-default-credentials branch never runs there. This class
 * is the other half: no emulator endpoint, no key file, no forwarding client — the sink built by
 * {@link BigtableSink#builder()} and by the Table factory dials {@code bigtable.googleapis.com} and
 * both admin endpoints itself. It cannot inject an ambiguous response, so response-loss recovery
 * stays with the lease; what it establishes is that the same envelopes, markers and metadata policy
 * hold on the native path, and which direct remote error paths that path classifies.
 *
 * <p>The credential path has no runtime observable of its own: the assertion is that the sink's
 * configuration carries neither an emulator endpoint nor a key file and that the run succeeded
 * against the service. Exercised paths, and the ones this project deliberately cannot reach without
 * an IAM change, are listed in {@code docs/adr/evidence/}.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
@Timeout(value = 1_200, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class BigtableStagedSinkRealGcpITCase extends AbstractBigtableRealGcpITCase {
    private static final Logger LOG =
            LoggerFactory.getLogger(BigtableStagedSinkRealGcpITCase.class);

    /** Distinct inputs each scenario stages; a hot-row distribution puts 21 of them on one row. */
    private static final int INPUTS = 24;

    /** An interval no held job reaches, so only an explicit savepoint can complete a checkpoint. */
    private static final long HELD_INTERVAL_MILLIS = 3_600_000;

    /**
     * Bound on each checkpoint or savepoint control future. On 2026-09-14 the initial
     * stop-with-savepoint of both entry points exceeded the recovery lease's 90 seconds with no
     * failure recorded (attempt 2), after taking about 62 seconds on the DataStream entry and 14
     * seconds on the Table entry in attempt 1; 180 seconds lets a slow but correct commit stage be
     * measured rather than cancelled, and the phase log records where the time went. A scenario
     * waits on up to seven such futures, so the class raises the inherited 600-second method
     * timeout to 1,200 seconds; the fork ceiling stays far above that.
     */
    private static final long CONTROL_TIMEOUT_MILLIS = 180_000;

    private static final String MARKER = StagedMutationTestSink.MARKER_FAMILY;
    private static final String NO_TX = "no-tx";
    private static final String MULTI_CLUSTER = "multi-cluster";

    private static TableDestination datastreamTable;
    private static TableDestination tableApiTable;
    private static TableDestination noTxTable;
    private static TableDestination markerMissing;
    private static TableDestination markerGc;
    private static TableDestination markerTyped;

    @TempDir Path directory;

    @BeforeAll
    static void createProfilesAndTables() {
        createSingleClusterAppProfile(LocalStagedHarness.PROFILE, true);
        createSingleClusterAppProfile(NO_TX, false);
        createMultiClusterAppProfile(MULTI_CLUSTER);
        datastreamTable = createStagedTable("staged-datastream");
        tableApiTable = createStagedTable("staged-table");
        noTxTable = createStagedTable("staged-no-tx");
        markerMissing = createTable("marker-missing", request -> request.addFamily(FAMILY));
        markerGc =
                createTable(
                        "marker-gc",
                        request ->
                                request.addFamily(FAMILY)
                                        .addFamily(MARKER, GCRules.GCRULES.maxVersions(1)));
        markerTyped =
                createTable(
                        "marker-typed",
                        request -> request.addFamily(FAMILY).addFamily(MARKER, Type.int64Sum()));
    }

    /** The shape the staged mode documents: data family, INT64 SUM family, raw no-GC markers. */
    private static TableDestination createStagedTable(String tableId) {
        return createTable(
                tableId,
                request ->
                        request.addFamily(FAMILY)
                                .addFamily("agg", Type.int64Sum())
                                .addFamily(MARKER));
    }

    @ParameterizedTest(name = "tableApi={0}")
    @ValueSource(booleans = {false, true})
    void nativeTransportCommitsAtStopAndReplaysRestoredEnvelopesWithoutReserializing(
            boolean tableApi) throws Exception {
        TableDestination table = tableApi ? tableApiTable : datastreamTable;
        try (var run = ProductionRecoveryJob.newRun(table, CONTROL_TIMEOUT_MILLIS)) {
            var sink = ProductionRecoveryJob.sink(run, null, tableApi, LocalStagedHarness.PROFILE);
            assertThat(sink.config().getEmulatorEndpoint())
                    .as("the native branch is selected by the absence of an emulator endpoint")
                    .isNull();
            assertThat(sink.config().getServiceAccountKeyFile())
                    .as("application-default credentials are selected by the absence of a key file")
                    .isNull();

            // The held source and an hour-long interval with an equal minimum pause leave the
            // stop-with-savepoint as the only checkpoint that can complete: Flink draws the first
            // periodic trigger between the pause and the interval, so an equal pause pins it.
            String stop;
            run.minPauseMillis = HELD_INTERVAL_MILLIS;
            try (var job = job(run, "initial", sink, 2, HELD_INTERVAL_MILLIS, null)) {
                phase(tableApi, "initial job submitted");
                job.awaitAdmissions(INPUTS);
                phase(tableApi, "initial admissions complete");
                assertThat(readRows(table))
                        .as("nothing reaches the table before its owning checkpoint completes")
                        .isEmpty();
                phase(tableApi, "initial readback empty; requesting stop-with-savepoint");
                stop = stopWithSavepoint(job, run, directory.resolve("stop-initial"), 0);
                LOG.info("Native tableApi={} stop-savepoint {} on {}", tableApi, stop, table);
            }
            int serialized = run.staged.get();
            assertThat(serialized).isEqualTo(INPUTS);
            var inventory =
                    ProductionRecoveryJob.requireCommitted(run.productionCommits, 0, INPUTS);
            assertReadback(run, table, inventory);

            // Both restores read the initial stop-savepoint, whose committer state still holds all
            // 24 envelopes: the snapshot precedes the commits it authorized. A restored committer
            // re-commits them while initializing, before its vertex is RUNNING, so waiting for
            // RUNNING also waits for that replay. The delegating committer records each replayed
            // request and the deduplication the service answered with; the readback then shows
            // the effect was absorbed.
            run.minPauseMillis = 0;
            int mark = run.productionCommits.size();
            try (var job = job(run, "rescale-1", sink, 1, 1_000, stop)) {
                phase(tableApi, "rescale-1 restored job submitted");
                ProductionRecoveryJob.awaitRunning(job);
                phase(tableApi, "rescale-1 running; requesting stop-with-savepoint");
                stopWithSavepoint(job, run, directory.resolve("stop-rescale-1"), mark);
            }
            assertThat(run.staged.get())
                    .as("restoring the stop-savepoint at parallelism 1 re-stages nothing")
                    .isEqualTo(serialized);
            ProductionRecoveryJob.requireReplayed(run.productionCommits, mark, inventory);
            assertReadback(run, table, inventory);

            mark = run.productionCommits.size();
            try (var job = job(run, "rescale-3", sink, 3, 1_000, stop)) {
                phase(tableApi, "rescale-3 restored job submitted");
                ProductionRecoveryJob.awaitRunning(job);
                phase(tableApi, "rescale-3 running; finishing");
                job.finish();
                phase(tableApi, "rescale-3 finished");
            }
            assertThat(run.staged.get())
                    .as("restoring the stop-savepoint at parallelism 3 re-stages nothing")
                    .isEqualTo(serialized);
            ProductionRecoveryJob.requireReplayed(run.productionCommits, mark, inventory);
            assertReadback(run, table, inventory);
            LOG.info(
                    "Native tableApi={} acceptance PASS inputs={} table={}",
                    tableApi,
                    INPUTS,
                    table);
        }
    }

    @Test
    void metadataValidationRejectsIncompatibleProfilesAndMarkerFamiliesBeforeAnyWrite()
            throws Exception {
        var admin = new BigtableStagedTableAdmin(null, null);
        Map<String, ColumnFamilyType> aggregate = Map.of("agg", ColumnFamilyType.INT64_SUM);
        // The accepted shape, so a later rejection is the profile's or the family's, not the
        // transport's.
        admin.validate(datastreamTable, LocalStagedHarness.PROFILE, MARKER, aggregate);

        String routing =
                "Bigtable EXACTLY_ONCE requires single-cluster routing with transactional writes";
        for (String profile : List.of(NO_TX, MULTI_CLUSTER)) {
            assertThatThrownBy(() -> admin.validate(datastreamTable, profile, MARKER, aggregate))
                    .as("profile %s", profile)
                    .isInstanceOf(IOException.class)
                    .hasMessage(routing);
        }
        String family =
                "Bigtable EXACTLY_ONCE requires an existing raw marker family without a GC rule: "
                        + MARKER;
        for (TableDestination fixture : List.of(markerMissing, markerGc, markerTyped)) {
            assertThatThrownBy(
                            () ->
                                    admin.validate(
                                            fixture, LocalStagedHarness.PROFILE, MARKER, Map.of()))
                    .as("fixture %s", fixture.getTable())
                    .isInstanceOf(IOException.class)
                    .hasMessage(family);
        }

        TableDestination absent = tableDestination("absent");
        assertThatThrownBy(
                        () -> admin.validate(absent, LocalStagedHarness.PROFILE, MARKER, Map.of()))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "Bigtable staged metadata validation failed before target writes: "
                                + absent)
                .hasCauseInstanceOf(NotFoundException.class);
        assertThatThrownBy(
                        () -> admin.validate(datastreamTable, "absent-profile", MARKER, Map.of()))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "Bigtable staged metadata validation failed before target writes: "
                                + datastreamTable)
                .hasCauseInstanceOf(NotFoundException.class);
    }

    @Test
    void dataStreamCommitterRejectsANonTransactionalProfileBeforeItsFirstTargetWrite()
            throws Exception {
        try (var run = ProductionRecoveryJob.newRun(noTxTable)) {
            var sink = ProductionRecoveryJob.sink(run, null, false, NO_TX);
            String failures;
            run.minPauseMillis = HELD_INTERVAL_MILLIS;
            try (var job = job(run, "no-tx", sink, 2, HELD_INTERVAL_MILLIS, null)) {
                job.awaitAdmissions(INPUTS);
                Throwable direct =
                        catchThrowable(() -> job.savepoint(directory.resolve("stop-no-tx"), true));
                failures = ProductionRecoveryJob.failureText(job, direct);
            }
            assertThat(failures)
                    .as("the committer reads the profile before its first CheckAndMutateRow")
                    .contains("single-cluster routing with transactional writes");
            assertThat(readRows(noTxTable)).as("no target write preceded the rejection").isEmpty();
        }
    }

    @Test
    void tableWriterRejectsAGcManagedMarkerFamilyBeforeStagingAnything() throws Exception {
        try (var run = ProductionRecoveryJob.newRun(markerGc)) {
            var sink = ProductionRecoveryJob.sink(run, null, true, LocalStagedHarness.PROFILE);
            String failures;
            run.minPauseMillis = HELD_INTERVAL_MILLIS;
            try (var job = job(run, "marker-gc", sink, 1, HELD_INTERVAL_MILLIS, null)) {
                failures = ProductionRecoveryJob.failureText(job, null);
            }
            assertThat(failures)
                    .as("the Table writer validates the declared schema when it opens")
                    .contains("raw marker family without a GC rule: " + MARKER);
            assertThat(run.staged.get()).as("nothing was staged").isZero();
            assertThat(readRows(markerGc)).as("no target write preceded the rejection").isEmpty();
        }
    }

    @Test
    void nativeDataClientReportsAMissingTableAsAFatalRequestFailure() throws Exception {
        TableDestination absent = tableDestination("absent");
        try (var factory =
                new DefaultSingleRowClientFactory(
                        LocalStagedHarness.PROFILE,
                        BigtableRequestOptions.builder().build(),
                        null,
                        null)) {
            LOG.info("Native data client at {}: creating", java.time.Instant.now());
            var client = factory.create(absent);
            LOG.info("Native data client at {}: created; sending", java.time.Instant.now());
            try {
                var mutation =
                        ConditionalRowMutation.create(TableId.of(absent.getTable()), "row")
                                .condition(Filters.FILTERS.family().exactMatch(MARKER))
                                .otherwise(Mutation.create().setCell(FAMILY, "q", "v"));
                Throwable failure =
                        catchThrowable(
                                () -> client.checkAndMutateRow(mutation).get(60, TimeUnit.SECONDS));
                assertThat(failure).isInstanceOf(ExecutionException.class);
                Throwable cause = failure.getCause();
                assertThat(cause).isInstanceOf(NotFoundException.class);
                assertThat(RequestFailures.classify(cause)).isEqualTo(RequestFailures.Kind.FATAL);
                assertThat(
                                RequestFailures.jobFailure(
                                                RequestFailures.Kind.FATAL,
                                                RowOperation.CHECK_AND_MUTATE_ROW,
                                                absent,
                                                cause)
                                        .getMessage())
                        .contains("because the table or one of its column families does not exist");
            } finally {
                LOG.info("Native data client at {}: answered; releasing", java.time.Instant.now());
                factory.release(absent);
            }
        }
        LOG.info("Native data client at {}: closed", java.time.Instant.now());
    }

    private static void phase(boolean tableApi, String what) {
        LOG.info("Native tableApi={} at {}: {}", tableApi, java.time.Instant.now(), what);
    }

    /**
     * Requests the stop and, if the control future does not complete in time, logs what the
     * committer had received and which state each vertex was in before rethrowing. One snapshot can
     * help localize the delay; it does not by itself prove progress inside an invocation, because
     * an observation becomes {@code APPLIED} only when the whole production invocation returns, and
     * the observations are read before the bounded RPCs that fetch the states.
     */
    private static String stopWithSavepoint(
            LocalStagedJob job, LocalStagedHarness run, Path target, int mark) throws Exception {
        try {
            return job.savepoint(target, true);
        } catch (Exception failure) {
            // The observations need no RPC; the job status and vertex states do, and a stalled
            // JobManager could refuse them, so they are bounded and can only add to the failure.
            String commits = ProductionRecoveryJob.summarizeCommits(run, mark);
            String status = "unavailable";
            StringBuilder states = new StringBuilder();
            try {
                status = String.valueOf(job.client.getJobStatus().get(10, TimeUnit.SECONDS));
                for (var vertex :
                        job.cluster
                                .getExecutionGraph(job.client.getJobID())
                                .get(10, TimeUnit.SECONDS)
                                .getAllExecutionVertices()) {
                    states.append(' ')
                            .append(vertex.getTaskNameWithSubtaskIndex())
                            .append('=')
                            .append(vertex.getExecutionState());
                }
            } catch (Exception diagnostic) {
                failure.addSuppressed(diagnostic);
            }
            LOG.error(
                    "Stop-with-savepoint did not complete: status={} {} vertices:{} admissions={}",
                    status,
                    commits,
                    states,
                    run.admissions.size());
            throw failure;
        }
    }

    private LocalStagedJob job(
            LocalStagedHarness run,
            String phase,
            ProductionRecoveryJob.MappedSink<?> sink,
            int parallelism,
            long intervalMillis,
            String restore)
            throws Exception {
        // No phase restarts. Before any completed checkpoint a restart would re-stage the held
        // inputs under fresh identities and fail the serialization count on a run the connector
        // handled correctly; after a restore it could replay the same envelopes twice and fail the
        // one-attempt replay oracle the same way. A transient service failure fails the run
        // instead, and the authorization allows four class runs in total.
        return new LocalStagedJob(
                run,
                directory.resolve(phase),
                true,
                false,
                parallelism,
                INPUTS,
                intervalMillis,
                true,
                restore,
                false,
                sink);
    }

    /**
     * Compares the table with the generated inputs and the committed inventory: one INT64 SUM cell
     * per row equal to that row's distinct contributions, exactly the inventory's markers on the
     * rows the inventory recorded, and no other family. Repeated commits of the same envelopes must
     * leave this unchanged.
     */
    private static void assertReadback(
            LocalStagedHarness run, TableDestination table, Map<ByteString, ByteString> inventory) {
        Map<ByteString, Long> expected = ProductionRecoveryJob.expectedContributions(run, INPUTS);
        Set<ByteString> markers = new HashSet<>();
        Map<ByteString, Long> observed = new HashMap<>();
        for (Row row : readRows(table)) {
            assertThat(expected)
                    .as("row %s", row.getKey().toStringUtf8())
                    .containsKey(row.getKey());
            long sum = -1;
            long rowMarkers = 0;
            for (RowCell cell : row.getCells()) {
                if (cell.getFamily().equals("agg")) {
                    assertThat(sum).as("one SUM cell per row").isEqualTo(-1);
                    assertThat(cell.getQualifier()).isEqualTo(ByteString.copyFromUtf8("count"));
                    assertThat(cell.getTimestamp()).isEqualTo(1000);
                    assertThat(cell.getValue().size()).isEqualTo(Long.BYTES);
                    sum = ByteBuffer.wrap(cell.getValue().toByteArray()).getLong();
                } else if (cell.getFamily().equals(MARKER)) {
                    assertThat(cell.getQualifier().size()).isEqualTo(32);
                    assertThat(inventory.get(cell.getQualifier()))
                            .as("marker %s belongs to this row", cell.getQualifier().toStringUtf8())
                            .isEqualTo(row.getKey());
                    assertThat(markers.add(cell.getQualifier())).as("distinct identity").isTrue();
                    assertThat(cell.getTimestamp()).isZero();
                    assertThat(cell.getValue()).isEqualTo(ByteString.copyFromUtf8("1"));
                    rowMarkers++;
                } else {
                    throw new AssertionError("Unexpected family " + cell.getFamily());
                }
            }
            assertThat(sum)
                    .as("SUM of %s", row.getKey().toStringUtf8())
                    .isEqualTo(expected.get(row.getKey()));
            assertThat(rowMarkers)
                    .as("markers on %s", row.getKey().toStringUtf8())
                    .isEqualTo(expected.get(row.getKey()));
            observed.put(row.getKey(), sum);
        }
        assertThat(observed).isEqualTo(expected);
        assertThat(markers).isEqualTo(inventory.keySet());
    }
}
