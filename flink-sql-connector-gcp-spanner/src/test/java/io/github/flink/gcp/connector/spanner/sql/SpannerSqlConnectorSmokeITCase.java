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

package io.github.flink.gcp.connector.spanner.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import io.github.flink.gcp.connector.testutils.spanner.SpannerEmulatorContainers;
import io.github.flink.gcp.connector.testutils.spanner.SpannerTestClients;
import io.github.flink.gcp.connector.testutils.sql.AbstractSqlConnectorSmokeITCase;
import io.github.flink.gcp.connector.testutils.sql.ShadedJar;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs SQL through the shaded connector while a stock Spanner client verifies the result. */
@Testcontainers
@Timeout(180)
class SpannerSqlConnectorSmokeITCase extends AbstractSqlConnectorSmokeITCase {

    private static final String PROJECT = "it-project";
    private static final String INSTANCE = "it-instance";
    private static final String DATABASE = "sql_smoke";
    private static final Duration JOB_TIMEOUT = Duration.ofSeconds(60);

    @Container
    private static final SpannerEmulatorContainer EMULATOR =
            SpannerEmulatorContainers.newContainer();

    private static Spanner spanner;
    private static DatabaseClient databaseClient;

    @BeforeAll
    static void createDatabase() throws Exception {
        spanner = SpannerTestClients.forEmulator(EMULATOR.getEmulatorGrpcEndpoint(), PROJECT);
        spanner.getInstanceAdminClient()
                .createInstance(
                        InstanceInfo.newBuilder(InstanceId.of(PROJECT, INSTANCE))
                                .setInstanceConfigId(
                                        InstanceConfigId.of(PROJECT, "emulator-config"))
                                .setNodeCount(1)
                                .setDisplayName("SQL smoke test")
                                .build())
                .get();
        spanner.getDatabaseAdminClient()
                .createDatabase(
                        INSTANCE,
                        DATABASE,
                        List.of(
                                "CREATE TABLE records (id INT64 NOT NULL, name STRING(64))"
                                        + " PRIMARY KEY (id)"))
                .get();
        databaseClient = spanner.getDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, DATABASE));
    }

    @AfterAll
    static void closeClient() {
        if (spanner != null) {
            spanner.close();
        }
    }

    @Override
    protected ShadedJar shadedJar() {
        return UberJar.SHADED;
    }

    @Override
    protected String factoryClass() {
        return UberJar.FACTORY_CLASS;
    }

    @Test
    void writesAndReadsThroughTheShadedFactory() throws Exception {
        TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        table.getConfig().set("parallelism.default", "1");
        table.executeSql(tableDdl());

        table.executeSql("INSERT INTO target VALUES (1, 'alice'), (2, 'bob')")
                .await(JOB_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        try (ResultSet rows =
                databaseClient
                        .singleUse()
                        .executeQuery(Statement.of("SELECT id, name FROM records ORDER BY id"))) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getLong("id")).isEqualTo(1L);
            assertThat(rows.getString("name")).isEqualTo("alice");
            assertThat(rows.next()).isTrue();
            assertThat(rows.getLong("id")).isEqualTo(2L);
            assertThat(rows.getString("name")).isEqualTo("bob");
            assertThat(rows.next()).isFalse();
        }
    }

    /**
     * The catalog's read-only skeleton lives in the base module, which this jar relocates, so a
     * catalog the planner opens through the shaded factory proves the relocated skeleton is still a
     * Flink {@code Catalog}, and its listing proves the relocated client reaches the emulator.
     */
    @Test
    void aCatalogListsAndResolvesTablesThroughTheShadedClasses() throws Exception {
        TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        table.executeSql(
                "CREATE CATALOG sp WITH ('type' = 'spanner', 'project' = '"
                        + PROJECT
                        + "', 'instance' = '"
                        + INSTANCE
                        + "', 'default-database' = '"
                        + DATABASE
                        + "', 'emulator-endpoint' = '"
                        + EMULATOR.getEmulatorGrpcEndpoint()
                        + "')");
        table.executeSql("USE CATALOG sp");

        assertThat(table.getCatalog("sp").orElseThrow().getClass().getSuperclass().getName())
                .as("the skeleton the planner's catalog extends")
                .isEqualTo(
                        "io.github.flink.gcp.connector.spanner.shaded."
                                + "io.github.flink.gcp.connector.base.catalog.AbstractReadOnlyCatalog");
        List<String> tables = new ArrayList<>();
        try (CloseableIterator<Row> rows = table.executeSql("SHOW TABLES").collect()) {
            rows.forEachRemaining(row -> tables.add(row.getFieldAs(0).toString()));
        }
        assertThat(tables).contains("records");
        assertThat(table.from("records").getResolvedSchema().getColumnNames())
                .containsExactly("id", "name");
    }

    private static String tableDdl() {
        return "CREATE TABLE target (\n"
                + "  id BIGINT,\n"
                + "  name STRING,\n"
                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'spanner',\n"
                + "  'project' = '"
                + PROJECT
                + "',\n"
                + "  'instance' = '"
                + INSTANCE
                + "',\n"
                + "  'database' = '"
                + DATABASE
                + "',\n"
                + "  'table' = 'records',\n"
                + "  'emulator-endpoint' = '"
                + EMULATOR.getEmulatorGrpcEndpoint()
                + "'\n"
                + ")";
    }
}
