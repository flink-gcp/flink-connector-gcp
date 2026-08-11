/*
 * Copyright 2026 laughingman7743
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

import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.AppProfile;
import com.google.cloud.bigtable.admin.v2.models.CreateAppProfileRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateInstanceRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Instance;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

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
 * declined, because a single class must stay runnable by hand and the best-effort deletion below
 * tracks per class. The cost of the granularity is one instance per class for the length of that
 * class; the benefit is that the forks provision in parallel and every class cleans up after
 * itself.
 *
 * <p>Deletion is best-effort, so instance names carry their creation time and {@link
 * #sweepStaleInstances} deletes anything older than {@link #STALE_AFTER} before creating this
 * class's own. That threshold is far above the E2E workflow's timeout, so the sweep cannot reach a
 * live run's instance; a local run left running for longer than it is the one case where it could,
 * and reclaiming that instance is the desired outcome anyway.
 *
 * <p>The {@code @EnabledIfEnvironmentVariable} gate lives on every concrete class, never here:
 * {@code scripts/e2e-gated-its.sh} discovers the suite by grepping for the annotation literal and
 * then expects a surefire report per matching file, which an abstract class never produces. The
 * {@code gated} tag beside it (issue #245) has to stay on the concrete classes for the same reason,
 * even though JUnit would inherit it from here: {@code --check-tags} greps both literals per file,
 * so hoisting one leaves the other unpaired.
 */
@Timeout(600)
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
        instanceAdmin.createInstance(
                CreateInstanceRequest.of(instanceId)
                        .setDisplayName("flink-connector-gcp E2E")
                        .setType(Instance.Type.PRODUCTION)
                        // One node is the minimum a production instance takes, and this suite
                        // writes tens of rows. The cluster id is built from the run id rather
                        // than from the instance id, which at 28 characters leaves no room
                        // under a cluster id's own 30-character limit.
                        .addCluster("c-" + runId, ZONE, 1, StorageType.SSD));

        tableAdmin = BigtableTableAdminClient.create(PROJECT, instanceId);
        dataClient = BigtableDataClient.create(PROJECT, instanceId);
    }

    @AfterAll
    protected static void deleteInstanceAndCloseClients() throws Exception {
        try {
            // The instance goes first, before any client is closed: it is the only part of this
            // fixture that costs money, and a client throwing on close must not be able to skip
            // its deletion. Deleting it takes its clusters and tables with it, so there is nothing
            // else to clean up.
            if (instanceAdmin != null && instanceId != null) {
                instanceAdmin.deleteInstance(instanceId);
            }
        } catch (RuntimeException e) {
            LOG.warn(
                    "Failed to delete instance {}; a later run's sweep reclaims it", instanceId, e);
        } finally {
            // Closers.closeAll rather than a sequence, for the same reason: one client failing to
            // close would otherwise leave the others open.
            Closers.closeAll(dataClient, tableAdmin, instanceAdmin);
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
        for (Instance instance : instanceAdmin.listInstances()) {
            Instant created = createdAt(instance.getId());
            if (created == null || !created.isBefore(cutoff)) {
                continue;
            }
            LOG.warn("Sweeping stale instance {}, created {}", instance.getId(), created);
            try {
                instanceAdmin.deleteInstance(instance.getId());
            } catch (RuntimeException e) {
                LOG.warn("Failed to sweep {}", instance.getId(), e);
            }
        }
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

    /** Creates a table with the shared column family and returns its destination. */
    protected static TableDestination createTable(String tableId) {
        tableAdmin.createTable(CreateTableRequest.of(tableId).addFamily(FAMILY));
        return TableDestination.of(PROJECT, instanceId, tableId);
    }

    /** Creates a table whose Change Streams history is retained for one day. */
    protected static TableDestination createChangeStreamTable(String tableId) {
        tableAdmin.createTable(
                CreateTableRequest.of(tableId)
                        .addFamily(FAMILY)
                        .addChangeStreamRetention(org.threeten.bp.Duration.ofHours(24)));
        return TableDestination.of(PROJECT, instanceId, tableId);
    }

    /** Returns a destination in the ephemeral instance without creating the table. */
    protected static TableDestination tableDestination(String tableId) {
        return TableDestination.of(PROJECT, instanceId, tableId);
    }

    /** Returns the live table description, for asserting what auto-creation actually made. */
    protected static com.google.cloud.bigtable.admin.v2.models.Table describeTable(String tableId) {
        return tableAdmin.getTable(tableId);
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
        CreateTableRequest request = CreateTableRequest.of(tableId).addFamily(FAMILY);
        for (String splitKey : splitKeys) {
            request.addSplit(ByteString.copyFromUtf8(splitKey));
        }
        tableAdmin.createTable(request);
        return TableDestination.of(PROJECT, instanceId, tableId);
    }

    /** Writes one cell per given row key, so a read test has something to find. */
    protected static void seedRows(TableDestination destination, String... rowKeys) {
        for (String rowKey : rowKeys) {
            dataClient.mutateRow(
                    RowMutation.create(TableId.of(destination.getTable()), rowKey)
                            .setCell(FAMILY, "q", rowKey));
        }
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

    /** Creates an application profile routing to the instance's only cluster. */
    protected static void createSingleClusterAppProfile(String appProfileId) {
        String clusterId = instanceAdmin.listClusters(instanceId).get(0).getId();
        instanceAdmin.createAppProfile(
                CreateAppProfileRequest.of(instanceId, appProfileId)
                        .setRoutingPolicy(AppProfile.SingleClusterRoutingPolicy.of(clusterId))
                        .setDescription("flink-connector-gcp source integration test"));
    }
}
