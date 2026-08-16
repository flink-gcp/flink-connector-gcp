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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import io.github.flink.gcp.connector.testutils.sql.AbstractSqlConnectorSmokeITCase;
import io.github.flink.gcp.connector.testutils.sql.ShadedJar;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
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
            new SpannerEmulatorContainer(
                    DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator:1.5.56"));

    private static Spanner spanner;
    private static DatabaseClient databaseClient;

    @BeforeAll
    static void createDatabase() throws Exception {
        spanner =
                SpannerOptions.newBuilder()
                        .setProjectId(PROJECT)
                        .setEmulatorHost(EMULATOR.getEmulatorGrpcEndpoint())
                        .build()
                        .getService();
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
