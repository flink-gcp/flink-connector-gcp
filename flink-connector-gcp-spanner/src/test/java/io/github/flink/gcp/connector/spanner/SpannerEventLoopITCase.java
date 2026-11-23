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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;
import io.github.flink.gcp.connector.spanner.sink.writer.DefaultSpannerDatabaseAccessFactory;
import io.github.flink.gcp.connector.spanner.sink.writer.SpannerDatabaseAccess;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.BatchClientPartitionPlanner;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.PartitionPlan;
import io.github.flink.gcp.connector.spanner.source.batch.reader.BatchClientStructStreamOpener;
import io.github.flink.gcp.connector.spanner.source.batch.reader.StructStream;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.DefaultSpannerChangeStreamCoordinatorClientFactory;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.DefaultSpannerChangeStreamQueryClientFactory;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.SpannerChangeStreamQueryClient;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The connector's Spanner clients run on the connector's own event-loop group and stop it when they
 * close (docs/adr/0208, #2265).
 *
 * <p>gRPC's shared group is destroyed a second after its last channel closes, which for a job-jar
 * deployment can be after Flink has closed the job's class loader; under {@code
 * cluster.uncaught-exception-handling: FAIL} that exits the TaskManager or JobManager. The emulator
 * arm is the one a test can reach, and the one {@code SpannerOptions.Builder.build()} replaces the
 * channel configurator on; the production arm sets the same configurator without that replacement,
 * which {@code SpannerClientsTest} covers. The harness's own client is built without {@link
 * SpannerClients} and runs on gRPC's group, so it never shows up here.
 */
class SpannerEventLoopITCase extends AbstractSpannerEmulatorITCase {

    /**
     * How long a check before a test waits for a group an earlier class left running: a MiniCluster
     * job that class cancelled closes its clients after it has returned, each within its own
     * shutdown budget.
     */
    private static final Duration LEFTOVER_TEARDOWN = Duration.ofSeconds(60);

    private static DatabaseDestination database;
    private static EmulatorEndpoint endpoint;

    @BeforeAll
    static void createSchema() throws Exception {
        database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE singers (id INT64 NOT NULL, name STRING(MAX))"
                                + " PRIMARY KEY (id)",
                        "CREATE CHANGE STREAM changes FOR singers");
        client(database)
                .write(
                        Collections.singletonList(
                                Mutation.newInsertBuilder("singers")
                                        .set("id")
                                        .to(1)
                                        .set("name")
                                        .to("Ada")
                                        .build()));
        endpoint = EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint");
    }

    /**
     * A group left running by an earlier class in this reused fork would make the checks below pass
     * or fail for the wrong reason, so it is ruled out first.
     */
    @BeforeEach
    void noGroupIsRunning() throws InterruptedException {
        assertThat(GrpcEventLoopThreads.awaitNone(LEFTOVER_TEARDOWN))
                .as(
                        "no %s* thread before the test: an earlier class in this fork left the"
                                + " group running",
                        GrpcEventLoopThreads.PREFIX)
                .isTrue();
    }

    @Test
    void theWritersClientRunsOnTheGroupAndStopsItOnClose() throws Exception {
        SpannerDatabaseAccess access =
                new DefaultSpannerDatabaseAccessFactory(
                                database, SpannerWriterOptions.defaults(), endpoint)
                        .create();
        try {
            access.readPrimaryKeys("singers");

            assertThat(GrpcEventLoopThreads.alive())
                    .as(
                            "a %s* thread while the writer's client is open",
                            GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        } finally {
            access.close();
        }

        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once the writer's client closed", GrpcEventLoopThreads.PREFIX)
                .isTrue();
    }

    @Test
    void thePlannerClosesItsClientWhenThePlanReturnsAndTheReaderOnClose() throws Exception {
        PartitionPlan plan;
        try (BatchClientPartitionPlanner planner =
                new BatchClientPartitionPlanner(database, endpoint)) {
            plan =
                    planner.plan(
                            SpannerReadOperation.query(
                                    Statement.of("SELECT id, name FROM singers")),
                            TimestampBound.strong(),
                            PartitionOptions.getDefaultInstance(),
                            false,
                            null);

            // Nothing else holds a lease, so the planner's own close of its client stopped the
            // group, before the enumerator that owns the planner closes it.
            assertThat(GrpcEventLoopThreads.awaitNone())
                    .as("no %s* thread once the plan returned", GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        }
        assertThat(plan.getPartitions()).isNotEmpty();

        BatchClientStructStreamOpener opener =
                new BatchClientStructStreamOpener(database, endpoint);
        int rows = 0;
        try {
            // The readers rejoin the snapshot by its id, after the planner's client is gone.
            for (Partition partition : plan.getPartitions()) {
                try (StructStream stream = opener.open(plan.getBatchTransactionId(), partition)) {
                    while (stream.next() != null) {
                        rows++;
                    }
                }
            }
            assertThat(GrpcEventLoopThreads.alive())
                    .as(
                            "a %s* thread while the reader's client is open",
                            GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        } finally {
            opener.close();
        }
        assertThat(rows).as("rows read through the plan").isOne();

        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once the reader's client closed", GrpcEventLoopThreads.PREFIX)
                .isTrue();
    }

    @Test
    void theChangeStreamClientsRunOnTheGroupAndStopItOnClose() throws Exception {
        try (SpannerChangeStreamCoordinatorClient coordinator =
                new DefaultSpannerChangeStreamCoordinatorClientFactory(
                                database, "changes", endpoint)
                        .create((io.github.flink.gcp.connector.base.metrics.ClientMetrics) null)) {
            assertThat(coordinator.initialize()).isPositive();
            assertThat(GrpcEventLoopThreads.alive())
                    .as(
                            "a %s* thread while the coordinator's client is open",
                            GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        }
        assertThat(GrpcEventLoopThreads.awaitNone())
                .as(
                        "no %s* thread once the coordinator's client closed",
                        GrpcEventLoopThreads.PREFIX)
                .isTrue();

        try (SpannerChangeStreamQueryClient reader =
                new DefaultSpannerChangeStreamQueryClientFactory(
                                database, "changes", SpannerRpcPriority.HIGH, 1, endpoint)
                        .create(null)) {
            assertThat(GrpcEventLoopThreads.alive())
                    .as(
                            "a %s* thread while the reader's client is open",
                            GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        }
        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once the reader's client closed", GrpcEventLoopThreads.PREFIX)
                .isTrue();
    }

    @Test
    void theCatalogsClientRunsOnTheGroupUntilTheCatalogCloses() throws Exception {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(
                "CREATE CATALOG sp WITH ("
                        + "'type' = 'spanner', "
                        + "'project' = '"
                        + database.getProject()
                        + "', 'instance' = '"
                        + database.getInstance()
                        + "', 'default-database' = '"
                        + database.getDatabase()
                        + "', 'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "')");
        Catalog catalog = table.getCatalog("sp").orElseThrow(IllegalStateException::new);
        try {
            assertThat(catalog.listTables(database.getDatabase())).contains("singers");
            assertThat(GrpcEventLoopThreads.alive())
                    .as("a %s* thread while the catalog is open", GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        } finally {
            catalog.close();
        }
        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once the catalog closed", GrpcEventLoopThreads.PREFIX)
                .isTrue();
    }
}
