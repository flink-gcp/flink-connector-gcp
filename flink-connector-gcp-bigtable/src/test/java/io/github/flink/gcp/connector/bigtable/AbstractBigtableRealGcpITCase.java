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

package io.github.flink.gcp.connector.bigtable;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ApiExceptions;
import com.google.api.gax.rpc.NotFoundException;
import com.google.bigtable.admin.v2.AppProfile;
import com.google.bigtable.admin.v2.ChangeStreamConfig;
import com.google.bigtable.admin.v2.Cluster;
import com.google.bigtable.admin.v2.ClusterName;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.CreateAppProfileRequest;
import com.google.bigtable.admin.v2.CreateInstanceRequest;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.Instance;
import com.google.bigtable.admin.v2.InstanceName;
import com.google.bigtable.admin.v2.ListClustersResponse;
import com.google.bigtable.admin.v2.ListInstancesResponse;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.LocationName;
import com.google.bigtable.admin.v2.ProjectName;
import com.google.bigtable.admin.v2.StorageType;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.admin.v2.TableName;
import com.google.bigtable.admin.v2.UpdateTableRequest;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.ReadChangeStreamQuery;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import com.google.protobuf.FieldMask;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Shared harness for the gated integration tests that run against real Cloud Bigtable — what the
 * emulator cannot show: the statuses the service actually rejects a mutation with, and the
 * production client-construction path itself, since every emulator test goes through {@code
 * emulatorEndpoint(...)} and so never builds a client over application-default credentials.
 *
 * <p>Clients authenticate with application-default credentials; the project comes from {@code
 * BIGTABLE_IT_PROJECT}. Instances are the one thing this suite cannot share with anything: nothing
 * persistent is provisioned for it, because a one-node instance is a standing cost of roughly $470
 * a month, so each class creates an instance of its own and deletes it in {@link AfterAll}.
 *
 * <p><b>Per class, not per run</b>, which is the one deviation from the design settled on #218. The
 * {@code integration-tests} surefire execution that {@code just e2e} invokes runs with {@code
 * forkCount=2} and {@code reuseForks=true} (the #243 root-pom override on the parent's config) —
 * classes run sequentially inside two long-lived JVMs, two at once across forks. A shared holder
 * would be raced by those forks; a per-fork holder became possible with the fork reuse and was
 * declined, because a single class must stay runnable by hand and the per-class deletion below
 * tracks per class. The cost of the granularity is one instance per class for the length of that
 * class; the benefit is that the forks provision in parallel and every class cleans up after
 * itself.
 *
 * <p>Teardown reports deletion failures. Crashes can still bypass it, so instance names carry their
 * creation time and {@link #sweepStaleInstances} deletes anything older than {@link #STALE_AFTER}
 * before creating this class's own. The integration-test fork's default ceiling is below that
 * threshold, so the age-gated sweep cannot reach an instance its owning fork is still using. The
 * E2E workflow has a separate whole-job ceiling. A class selected through surefire's {@code
 * default-test} execution after clearing the gated exclusion, or run from an IDE, does not inherit
 * the integration-test fork ceiling and can cross the age gate. A post-E2E or explicitly requested
 * manual {@code --all} sweep deliberately ignores age and can still collide with a concurrent local
 * run; {@code docs/adr/0119} records both residuals.
 *
 * <p>The {@code @EnabledIfEnvironmentVariable} gate lives on every concrete class, never here:
 * {@code scripts/e2e-gated-its.sh} discovers the suite by parsing the annotation on each file and
 * then expects a surefire report per matching file, which an abstract class never produces. The
 * {@code gated} tag beside it (issue #245) has to stay on the concrete classes for the same reason,
 * even though JUnit would inherit it from here: {@code --check-tags} checks both annotations per
 * file, so hoisting one leaves the other unpaired.
 *
 * <p>The timeout runs in a separate thread because the default mode cannot end a wait that ignores
 * interruption, which is how #951 outlived its own deadline. ADR-0119 records the measurement and
 * the fork-level ceiling that covers what abandoning a thread cannot.
 */
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
public abstract class AbstractBigtableRealGcpITCase {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBigtableRealGcpITCase.class);

    /** The project the suite runs against; null when the gate is off (the tests then skip). */
    protected static final String PROJECT = System.getenv("BIGTABLE_IT_PROJECT");

    /** The column family every table in this suite is created with. */
    protected static final String FAMILY = "cf";

    /**
     * Identifies an instance as this suite's, for the sweep. Instance ids are 6–33 characters of
     * lowercase letters, digits and hyphens starting with a letter, which {@code flink-it-} plus
     * ten digits of epoch seconds plus an eight-character run id fits with room to spare.
     */
    private static final String INSTANCE_PREFIX = "flink-it-";

    /** Where the ephemeral cluster goes — in the region the other IT resources already use. */
    private static final String ZONE = "us-central1-b";

    /** An instance older than this belongs to a run that crashed; see the class javadoc. */
    private static final Duration STALE_AFTER = Duration.ofHours(2);

    /** How long a new change stream may refuse a fresh start; well inside the class timeout. */
    private static final Duration CHANGE_STREAM_START_TIMEOUT = Duration.ofMinutes(2);

    /** The pause between change-stream start probes. */
    private static final Duration CHANGE_STREAM_START_RETRY = Duration.ofSeconds(1);

    /**
     * Bounds each probe RPC, retries included, by the probe's own timeout. The client's default
     * ReadChangeStream settings allow five-minute attempts within twelve hours, longer than the
     * class timeout, so without this a probe that never closes would end as a generic timeout
     * rather than as the RPC's own status. A retry the server directs through {@code RetryInfo} is
     * not clipped to what is left of the total and can get a full attempt timeout, so one RPC can
     * run for two budgets. The bound is per RPC, not for the whole probe.
     */
    private static final RetrySettings PROBE_RPC_RETRY =
            RetrySettings.newBuilder()
                    .setInitialRetryDelayDuration(Duration.ofMillis(10))
                    .setRetryDelayMultiplier(2.0)
                    .setMaxRetryDelayDuration(CHANGE_STREAM_START_RETRY)
                    .setInitialRpcTimeoutDuration(CHANGE_STREAM_START_TIMEOUT)
                    .setRpcTimeoutMultiplier(1.0)
                    .setMaxRpcTimeoutDuration(CHANGE_STREAM_START_TIMEOUT)
                    .setTotalTimeoutDuration(CHANGE_STREAM_START_TIMEOUT)
                    .build();

    private static String instanceId;
    private static BigtableInstanceAdminClient instanceAdmin;
    private static BigtableTableAdminClient tableAdmin;
    private static BigtableDataClient dataClient;

    @BeforeAll
    protected static void createInstanceAndClients() throws IOException {
        instanceAdmin = BigtableInstanceAdminClient.create(PROJECT);
        sweepStaleInstances();

        String runId = TestNames.runId();
        instanceId = INSTANCE_PREFIX + Instant.now().getEpochSecond() + "-" + runId;
        LOG.info("Creating ephemeral Bigtable instance {} in {}", instanceId, ZONE);
        ApiExceptions.callAndTranslateApiException(
                instanceAdmin
                        .getBaseClient()
                        .createInstanceAsync(
                                CreateInstanceRequest.newBuilder()
                                        .setParent(ProjectName.of(PROJECT).toString())
                                        .setInstanceId(instanceId)
                                        .setInstance(
                                                Instance.newBuilder()
                                                        .setDisplayName("flink-connector-gcp E2E")
                                                        .setType(Instance.Type.PRODUCTION))
                                        // One node is the minimum a production instance takes,
                                        // and this suite writes tens of rows. The cluster id is
                                        // built from the run id rather than from the instance id,
                                        // which at 28 characters leaves no room under a cluster
                                        // id's own 30-character limit.
                                        .putClusters(
                                                "c-" + runId,
                                                Cluster.newBuilder()
                                                        .setLocation(
                                                                LocationName.of(PROJECT, ZONE)
                                                                        .toString())
                                                        .setServeNodes(1)
                                                        .setDefaultStorageType(StorageType.SSD)
                                                        .build())
                                        .build()));

        tableAdmin = BigtableTableAdminClient.create(PROJECT, instanceId);
        dataClient = BigtableDataClient.create(PROJECT, instanceId);
    }

    @AfterAll
    protected static void deleteInstanceAndCloseClients() throws Exception {
        try (AutoCloseable closeClients =
                () -> Closers.closeAll(dataClient, tableAdmin, instanceAdmin)) {
            if (instanceAdmin != null && instanceId != null) {
                // Disable retention before deleting the billed instance, while clients are open.
                if (tableAdmin != null) {
                    disableChangeStreams(tableAdmin, instanceId);
                }
                try {
                    instanceAdmin
                            .getBaseClient()
                            .deleteInstance(InstanceName.of(PROJECT, instanceId));
                } catch (NotFoundException absent) {
                    // A failed creation may never have installed the registered instance.
                }
            }
        } finally {
            dataClient = null;
            tableAdmin = null;
            instanceAdmin = null;
            instanceId = null;
        }
    }

    /**
     * Deletes instances this suite created that are older than {@link #STALE_AFTER}, so a run
     * killed before its {@link AfterAll} leaves a cost that stops at the next run rather than
     * standing indefinitely.
     *
     * <p>Two forks sweeping at once can both pick the same instance; the loser sees the delete fail
     * and logs it. An id that carries no parsable timestamp is left alone: this deletes instances,
     * and a name it cannot date is a name it does not understand.
     */
    private static void sweepStaleInstances() {
        Instant cutoff = Instant.now().minus(STALE_AFTER);
        ListInstancesResponse instances =
                instanceAdmin.getBaseClient().listInstances(ProjectName.of(PROJECT));
        BigtableAdminProtos.checkListedEverywhere(instances.getFailedLocationsList(), "instances");
        for (Instance instance : instances.getInstancesList()) {
            String id = InstanceName.parse(instance.getName()).getInstance();
            Instant created = createdAt(id);
            if (created == null || !created.isBefore(cutoff)) {
                continue;
            }
            LOG.warn("Sweeping stale instance {}, created {}", id, created);
            try {
                disableChangeStreams(id);
                instanceAdmin.getBaseClient().deleteInstance(InstanceName.of(PROJECT, id));
            } catch (Exception e) {
                LOG.warn("Failed to sweep {}", id, e);
            }
        }
    }

    private static void disableChangeStreams(String staleInstanceId) throws Exception {
        try (BigtableTableAdminClient staleTableAdmin =
                BigtableTableAdminClient.create(PROJECT, staleInstanceId)) {
            disableChangeStreams(staleTableAdmin, staleInstanceId);
        }
    }

    private static void disableChangeStreams(BigtableTableAdminClient admin, String instance)
            throws Exception {
        List<String> tableNames = new ArrayList<>();
        admin.getBaseClient()
                .listTables(
                        ListTablesRequest.newBuilder()
                                .setParent(InstanceName.of(PROJECT, instance).toString())
                                .setView(Table.View.NAME_ONLY)
                                .build())
                .iterateAll()
                .forEach(table -> tableNames.add(table.getName()));
        disableChangeStreams(
                tableNames,
                name ->
                        admin.getBaseClient()
                                .getTable(
                                        GetTableRequest.newBuilder()
                                                .setName(name)
                                                .setView(Table.View.SCHEMA_VIEW)
                                                .build()),
                request ->
                        ApiExceptions.callAndTranslateApiException(
                                admin.getBaseClient().updateTableAsync(request)));
    }

    /**
     * Disables the change stream of every listed table that has one, attempting each table even
     * when an earlier one fails.
     *
     * @param tableNames the tables, by full resource name
     * @param getTable reads a table's schema view by name
     * @param updateTable applies a table update
     */
    static void disableChangeStreams(
            List<String> tableNames,
            Function<String, Table> getTable,
            Consumer<UpdateTableRequest> updateTable)
            throws Exception {
        List<AutoCloseable> updates = new ArrayList<>();
        for (String tableName : tableNames) {
            updates.add(
                    () -> {
                        if (getTable.apply(tableName).hasChangeStreamConfig()) {
                            updateTable.accept(
                                    UpdateTableRequest.newBuilder()
                                            .setTable(Table.newBuilder().setName(tableName))
                                            .setUpdateMask(
                                                    FieldMask.newBuilder()
                                                            .addPaths("change_stream_config"))
                                            .build());
                        }
                    });
        }
        Closers.closeAll(updates);
    }

    /** The creation time encoded in an instance id, or null if this suite did not create it. */
    private static Instant createdAt(String id) {
        if (!id.startsWith(INSTANCE_PREFIX)) {
            return null;
        }
        String remainder = id.substring(INSTANCE_PREFIX.length());
        int end = remainder.indexOf('-');
        try {
            return Instant.ofEpochSecond(
                    Long.parseLong(end < 0 ? remainder : remainder.substring(0, end)));
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** Creates an Int64 Sum fixture directly through the SDK. */
    protected static TableDestination createAggregateTable(String tableId) {
        return createTable(
                tableId,
                Map.of(FAMILY, BigtableAdminProtos.typedFamily(BigtableAdminProtos.int64Sum())));
    }

    /** Creates a table with the shared column family and returns its destination. */
    protected static TableDestination createTable(String tableId) {
        return createTable(tableId, Map.of(FAMILY, BigtableAdminProtos.rawFamily()));
    }

    /** Creates a table whose Change Streams history is retained for one day. */
    protected static TableDestination createChangeStreamTable(String tableId) {
        return createChangeStreamTableWithSplits(tableId);
    }

    /** Creates a Change Streams table already split at the given row keys. */
    protected static TableDestination createChangeStreamTableWithSplits(
            String tableId, String... splitKeys) {
        return create(
                tableId,
                Table.newBuilder()
                        .putColumnFamilies(FAMILY, BigtableAdminProtos.rawFamily())
                        .setChangeStreamConfig(
                                ChangeStreamConfig.newBuilder()
                                        .setRetentionPeriod(
                                                com.google.protobuf.Duration.newBuilder()
                                                        .setSeconds(
                                                                Duration.ofHours(24)
                                                                        .getSeconds()))),
                splitKeys);
    }

    /**
     * Writes a server-stamped marker cell and returns its timestamp.
     *
     * <p>The timestamp-free SDK overload stamps client time. Explicit {@code -1} asks Bigtable for
     * server time; repeated marker versions are harmless on this unrelated cell.
     */
    protected static Instant writeServerTimeMarker(TableDestination table, String rowKey) {
        dataClient.mutateRow(
                RowMutation.create(
                        TableId.of(table.getTable()),
                        rowKey,
                        Mutation.createUnsafe().setCell(FAMILY, "marker", -1L, "time")));
        Row marker = dataClient.readRow(TableId.of(table.getTable()), rowKey);
        if (marker == null) {
            throw new IllegalStateException("Marker row " + rowKey + " was not read back");
        }
        long micros = marker.getCells(FAMILY, "marker").get(0).getTimestamp();
        return Instant.ofEpochSecond(
                Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
    }

    /**
     * Returns a server-time start position from which the table's change stream is readable.
     *
     * <p>A start must be after the change stream's creation. A read from a marker written right
     * after {@code createTable} returned was refused with {@code NOT_FOUND}, in one test about 30
     * seconds after the table was created (#1613), so this does not assume the start is readable;
     * it proves it. Each attempt writes a fresh marker, truncates its timestamp to the millisecond
     * a {@code scan.startup.timestamp-millis} option carries, and reads every initial partition
     * from it through the given application profile. A {@code NOT_FOUND} retries with a later
     * marker, since a fixed start that precedes the change stream never becomes readable.
     */
    protected static Instant awaitReadableChangeStreamStart(
            TableDestination table, String appProfileId, String markerRowKey) throws Exception {
        long startNanos = System.nanoTime();
        long deadline = startNanos + CHANGE_STREAM_START_TIMEOUT.toNanos();
        BigtableDataSettings.Builder settings =
                BigtableDataSettings.newBuilder()
                        .setProjectId(PROJECT)
                        .setInstanceId(table.getInstance())
                        .setAppProfileId(appProfileId);
        settings.stubSettings()
                .generateInitialChangeStreamPartitionsSettings()
                .setRetrySettings(PROBE_RPC_RETRY);
        settings.stubSettings().readChangeStreamSettings().setRetrySettings(PROBE_RPC_RETRY);
        try (BigtableDataClient profileClient = BigtableDataClient.create(settings.build())) {
            for (int attempt = 1; ; attempt++) {
                Instant start =
                        Instant.ofEpochMilli(
                                writeServerTimeMarker(table, markerRowKey).toEpochMilli());
                try {
                    // The partition stream is drained before any read, so a NOT_FOUND below
                    // cannot abandon it half consumed.
                    List<ByteStringRange> partitions = new ArrayList<>();
                    profileClient
                            .generateInitialChangeStreamPartitions(table.getTable())
                            .forEach(partitions::add);
                    for (ByteStringRange partition : partitions) {
                        profileClient
                                .readChangeStream(
                                        ReadChangeStreamQuery.create(table.getTable())
                                                .streamPartition(partition)
                                                .startTime(start)
                                                .endTime(start.plusMillis(1)))
                                .forEach(record -> {});
                    }
                    LOG.info(
                            "Change stream of {} readable from {} after {} attempt(s) in {} ms",
                            table.getTable(),
                            start,
                            attempt,
                            Duration.ofNanos(System.nanoTime() - startNanos).toMillis());
                    return start;
                } catch (NotFoundException notReadable) {
                    if (System.nanoTime() - deadline >= 0) {
                        throw new IllegalStateException(
                                "Change stream of "
                                        + table.getTable()
                                        + " still unreadable after "
                                        + attempt
                                        + " attempt(s)",
                                notReadable);
                    }
                    LOG.info(
                            "Change stream of {} not readable from {} (attempt {}): {}",
                            table.getTable(),
                            start,
                            attempt,
                            notReadable.getMessage());
                    Thread.sleep(CHANGE_STREAM_START_RETRY.toMillis());
                }
            }
        }
    }

    /** Returns a destination in the ephemeral instance without creating the table. */
    protected static TableDestination tableDestination(String tableId) {
        return TableDestination.of(PROJECT, instanceId, tableId);
    }

    /** Returns the live table description, for asserting what auto-creation actually made. */
    protected static Table describeTable(String tableId) {
        return tableAdmin
                .getBaseClient()
                .getTable(
                        GetTableRequest.newBuilder()
                                .setName(TableName.of(PROJECT, instanceId, tableId).toString())
                                .setView(Table.View.SCHEMA_VIEW)
                                .build());
    }

    /** Reads every row of the table, in row-key order. */
    protected static List<Row> readRows(TableDestination destination) {
        List<Row> rows = new ArrayList<>();
        dataClient.readRows(Query.create(TableId.of(destination.getTable()))).forEach(rows::add);
        return rows;
    }

    /**
     * Creates a table already split at the given row keys.
     *
     * <p>The single most valuable thing this suite can do that nothing else can: a pre-split table
     * has real tablets, so {@code SampleRowKeys} answers with one boundary per split point and the
     * scan source's split planning is exercised against the service. The emulator models no tablets
     * at all.
     *
     * @param tableId the table to create
     * @param splitKeys the row keys to split the table at
     * @return the table's destination
     */
    protected static TableDestination createTableWithSplits(String tableId, String... splitKeys) {
        return create(
                tableId,
                Table.newBuilder().putColumnFamilies(FAMILY, BigtableAdminProtos.rawFamily()),
                splitKeys);
    }

    /** Writes one cell per given row key, so a read test has something to find. */
    protected static void seedRows(TableDestination destination, String... rowKeys) {
        for (String rowKey : rowKeys) {
            dataClient.mutateRow(
                    RowMutation.create(TableId.of(destination.getTable()), rowKey)
                            .setCell(FAMILY, "q", rowKey));
        }
    }

    /** Applies one caller-defined mutation to a binary row key in the ephemeral instance. */
    protected static void mutateRow(
            TableDestination destination,
            ByteString rowKey,
            Consumer<RowMutation> mutationBuilder) {
        RowMutation mutation = RowMutation.create(TableId.of(destination.getTable()), rowKey);
        mutationBuilder.accept(mutation);
        dataClient.mutateRow(mutation);
    }

    /** Returns what the service answers {@code SampleRowKeys} with. */
    protected static List<KeyOffset> sampleRowKeys(TableDestination destination) {
        return dataClient.sampleRowKeys(TableId.of(destination.getTable()));
    }

    /** Reads one range directly, for measuring what the service does with an unusual one. */
    protected static List<Row> readRange(TableDestination destination, ByteStringRange range) {
        List<Row> rows = new ArrayList<>();
        dataClient
                .readRows(Query.create(TableId.of(destination.getTable())).range(range))
                .forEach(rows::add);
        return rows;
    }

    /** Creates an application profile routing to and returns the instance's only cluster. */
    protected static String createSingleClusterAppProfile(String appProfileId) {
        return createSingleClusterAppProfile(appProfileId, false);
    }

    /**
     * Creates a single-cluster application profile and returns the instance's only cluster.
     *
     * <p>The single-argument overload creates the routing-only shape the source tests need. A
     * conditional or staged write additionally needs {@code allowTransactionalWrites}, which the
     * service refuses to enable on a multi-cluster profile and which the staged sink's metadata
     * validation requires before its first target write.
     */
    protected static String createSingleClusterAppProfile(
            String appProfileId, boolean allowTransactionalWrites) {
        ListClustersResponse clusters =
                instanceAdmin.getBaseClient().listClusters(InstanceName.of(PROJECT, instanceId));
        BigtableAdminProtos.checkListedEverywhere(clusters.getFailedLocationsList(), "clusters");
        String clusterId = ClusterName.parse(clusters.getClusters(0).getName()).getCluster();
        createAppProfile(
                appProfileId,
                AppProfile.newBuilder()
                        .setSingleClusterRouting(
                                AppProfile.SingleClusterRouting.newBuilder()
                                        .setClusterId(clusterId)
                                        .setAllowTransactionalWrites(allowTransactionalWrites)));
        return clusterId;
    }

    /**
     * Creates a multi-cluster application profile, the routing a single-row transaction rejects.
     */
    protected static void createMultiClusterAppProfile(String appProfileId) {
        createAppProfile(
                appProfileId,
                AppProfile.newBuilder()
                        .setMultiClusterRoutingUseAny(
                                AppProfile.MultiClusterRoutingUseAny.getDefaultInstance()));
    }

    private static void createAppProfile(String appProfileId, AppProfile.Builder profile) {
        instanceAdmin
                .getBaseClient()
                .createAppProfile(
                        CreateAppProfileRequest.newBuilder()
                                .setParent(InstanceName.of(PROJECT, instanceId).toString())
                                .setAppProfileId(appProfileId)
                                .setAppProfile(
                                        profile.setDescription(
                                                "flink-connector-gcp integration test"))
                                .build());
    }

    /**
     * Creates a table with the given column families, and returns its destination.
     *
     * @param tableId the table to create
     * @param families the families by name; {@link BigtableAdminProtos} builds them
     * @return the table's destination
     */
    protected static TableDestination createTable(
            String tableId, Map<String, ColumnFamily> families) {
        return create(tableId, Table.newBuilder().putAllColumnFamilies(families));
    }

    private static TableDestination create(
            String tableId, Table.Builder table, String... splitKeys) {
        CreateTableRequest.Builder request =
                CreateTableRequest.newBuilder()
                        .setParent(InstanceName.of(PROJECT, instanceId).toString())
                        .setTableId(tableId)
                        .setTable(table);
        for (String splitKey : splitKeys) {
            request.addInitialSplitsBuilder().setKey(ByteString.copyFromUtf8(splitKey));
        }
        tableAdmin.getBaseClient().createTable(request.build());
        return tableDestination(tableId);
    }
}
