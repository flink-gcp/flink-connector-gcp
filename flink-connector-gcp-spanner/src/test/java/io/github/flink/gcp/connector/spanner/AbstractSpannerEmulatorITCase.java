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

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.spanner.SpannerEmulatorContainers;
import io.github.flink.gcp.connector.testutils.spanner.SpannerTestClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Base for the Spanner emulator integration tests, in both directions: one emulator container and
 * one instance per test class, with a fresh database per test.
 *
 * <p>The image is <b>not</b> the {@code google-cloud-cli} bundle the Bigtable and Pub/Sub tests
 * use. Its Spanner emulator predates the {@code BatchWrite} RPC, which the emulator only implements
 * from v1.5.31 — the sink's entire write path would answer {@code UNIMPLEMENTED} against it.
 *
 * <p>An emulator is a convenience, never evidence about the service: where the two disagree, the
 * real service decides. The deviations these tests found are recorded on the connector's docs page.
 */
@Testcontainers
@Timeout(300)
public abstract class AbstractSpannerEmulatorITCase {

    protected static final String PROJECT = "it-project";
    protected static final String INSTANCE = "it-instance";

    @Container
    static final SpannerEmulatorContainer EMULATOR = SpannerEmulatorContainers.newContainer();

    private static Spanner spanner;

    @BeforeAll
    static void startHarness() throws Exception {
        spanner = SpannerTestClients.forEmulator(emulatorEndpoint(), PROJECT);
        spanner.getInstanceAdminClient()
                .createInstance(
                        InstanceInfo.newBuilder(InstanceId.of(PROJECT, INSTANCE))
                                .setInstanceConfigId(
                                        InstanceConfigId.of(PROJECT, "emulator-config"))
                                .setNodeCount(1)
                                .setDisplayName("integration tests")
                                .build())
                .get();
    }

    @AfterAll
    static void stopHarness() {
        if (spanner != null) {
            spanner.close();
        }
    }

    protected static String emulatorEndpoint() {
        return EMULATOR.getEmulatorGrpcEndpoint();
    }

    /**
     * Creates a database of the given dialect with the given DDL and a name of its own, so tests
     * within a class never see each other's schema.
     */
    protected static DatabaseDestination createDatabase(Dialect dialect, String... ddl)
            throws Exception {
        // Lower case, and underscores rather than the hyphens TestNames.unique would give: a
        // database id must match [a-z][a-z0-9_-]{1,29}.
        String databaseId =
                "db_"
                        + TestNames.unique("")
                                .replace("-", "")
                                .toLowerCase(Locale.ROOT)
                                .substring(0, 20);
        DatabaseAdminClient admin = spanner.getDatabaseAdminClient();
        // The second argument is the CREATE DATABASE *statement*, not the id — and the two
        // dialects spell and quote it differently, which the client library's own Dialect knows.
        admin.createDatabase(
                        INSTANCE,
                        dialect.createDatabaseStatementFor(databaseId),
                        dialect,
                        Arrays.asList(ddl))
                .get();
        return DatabaseDestination.of(PROJECT, INSTANCE, databaseId);
    }

    /** Runs a query against the database and materializes its rows. */
    protected static List<Struct> query(DatabaseDestination database, String sql) {
        DatabaseClient client = client(database);
        List<Struct> rows = new ArrayList<>();
        try (ResultSet resultSet = client.singleUse().executeQuery(Statement.of(sql))) {
            while (resultSet.next()) {
                rows.add(resultSet.getCurrentRowAsStruct());
            }
        }
        return rows;
    }

    /**
     * Applies mutations through the harness's own client, for arranging a test's starting state.
     */
    protected static DatabaseClient client(DatabaseDestination database) {
        return spanner.getDatabaseClient(
                DatabaseId.of(
                        database.getProject(), database.getInstance(), database.getDatabase()));
    }

    /** Applies schema changes to an existing emulator database. */
    protected static void updateDdl(DatabaseDestination database, String... ddl) throws Exception {
        spanner.getDatabaseAdminClient()
                .updateDatabaseDdl(
                        database.getInstance(), database.getDatabase(), Arrays.asList(ddl), null)
                .get();
    }
}
