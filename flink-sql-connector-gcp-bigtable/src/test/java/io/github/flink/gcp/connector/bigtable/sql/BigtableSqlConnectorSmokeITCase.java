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

package io.github.flink.gcp.connector.bigtable.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import io.github.flink.gcp.connector.testutils.bigtable.BigtableEmulatorContainers;
import io.github.flink.gcp.connector.testutils.bigtable.BigtableTestClients;
import io.github.flink.gcp.connector.testutils.sql.AbstractSqlConnectorSmokeITCase;
import io.github.flink.gcp.connector.testutils.sql.ShadedJar;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.BigtableEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs a SQL job through the shaded classes, against the Bigtable emulator.
 *
 * <p>The only test here that exercises relocation at <em>runtime</em>. {@link
 * BigtableSqlConnectorPackagingITCase} proves the jar has the right shape; this proves the shape
 * works — creating a Bigtable channel is what puts relocated gRPC and the deliberately unrelocated
 * {@code grpc-netty-shaded} transport together, and a mistake there is invisible to any jar-content
 * assertion.
 *
 * <p>The connector under test comes from the uber-jar, not from the reactor's classes: the module's
 * surefire configuration drops {@code flink-connector-gcp-bigtable} from the test classpath and
 * adds the shaded jar. {@link #theConnectorUnderTestComesFromTheShadedJar()} asserts that rather
 * than trusting it, because a regression there would leave this whole class passing against
 * unshaded code and proving nothing.
 *
 * <p>The harness drives the emulator with the <em>stock</em> Bigtable clients while the connector
 * uses its relocated copies. That the two coexist on one classpath is not incidental — it is the
 * property an uber-jar exists to provide. The stock data client is also how the written rows come
 * back at all: the {@code bigtable} factory is sink-only, so there is no second SQL table to read
 * through.
 *
 * <p>The container image and the stock clients come from the shared test-utils module ({@link
 * BigtableEmulatorContainers}, {@link BigtableTestClients}), which deals only in stock {@code
 * com.google.*} types — the connector module's harnesses cannot be reused here, because their
 * helpers touch production classes whose relocated and unrelocated forms would not type-check
 * against each other across this boundary (issue #27).
 */
@Testcontainers
@Timeout(180)
class BigtableSqlConnectorSmokeITCase extends AbstractSqlConnectorSmokeITCase {

    private static final String PROJECT = "it-project";

    /** Emulator instances are opaque path segments; no instance has to exist. */
    private static final String INSTANCE = "it-instance";

    /** Comfortably inside the class timeout, so a shortfall fails the assertion instead. */
    private static final Duration JOB_TIMEOUT = Duration.ofSeconds(60);

    @Container
    private static final BigtableEmulatorContainer EMULATOR =
            BigtableEmulatorContainers.newContainer();

    private static BigtableTableAdminClient adminClient;
    private static BigtableDataClient dataClient;

    @BeforeAll
    static void createClients() throws IOException {
        adminClient = BigtableTestClients.adminClient(EMULATOR, PROJECT, INSTANCE);
        dataClient = BigtableTestClients.dataClient(EMULATOR, PROJECT, INSTANCE);
    }

    @AfterAll
    static void closeClients() {
        if (dataClient != null) {
            dataClient.close();
        }
        if (adminClient != null) {
            adminClient.close();
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
    void whatSqlWritesThroughTheShadedClassesIsWhatTheStockClientReadsBack() throws Exception {
        String table = "sql-smoke";
        adminClient.createTable(CreateTableRequest.of(table).addFamily("cf1"));

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<name STRING, amount BIGINT>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'bigtable',\n"
                        + ("  'project' = '" + PROJECT + "',\n")
                        + ("  'instance' = '" + INSTANCE + "',\n")
                        + ("  'table' = '" + table + "',\n")
                        + ("  'emulator-endpoint' = '"
                                + EMULATOR.getHost()
                                + ":"
                                + EMULATOR.getEmulatorPort()
                                + "'\n")
                        + ")");

        // Bounded on purpose: the no-argument await() has no timeout, so an INSERT job that never
        // finishes would hang to the class timeout and report an interrupt instead of a shortfall.
        tEnv.executeSql(
                        "INSERT INTO bt VALUES"
                                + " ('r1', ROW('alice', CAST(7 AS BIGINT))),"
                                + " ('r2', ROW('bob', CAST(9 AS BIGINT)))")
                .await(JOB_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        List<Row> rows = new ArrayList<>();
        dataClient.readRows(Query.create(TableId.of(table))).forEach(rows::add);
        assertThat(rows).extracting(row -> row.getKey().toStringUtf8()).containsExactly("r1", "r2");
        assertThat(cellValue(rows.get(0), "name"))
                .isEqualTo("alice".getBytes(StandardCharsets.UTF_8));
        // The connector's own big-endian encoding, decoded by nothing under test: the bytes the
        // stock client hands back are the bytes the relocated cell codec wrote.
        assertThat(cellValue(rows.get(0), "amount")).isEqualTo(new byte[] {0, 0, 0, 0, 0, 0, 0, 7});
        assertThat(cellValue(rows.get(1), "name"))
                .isEqualTo("bob".getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] cellValue(Row row, String qualifier) {
        assertThat(row.getCells("cf1", qualifier))
                .as("cell cf1:%s of row %s", qualifier, row.getKey().toStringUtf8())
                .hasSize(1);
        return row.getCells("cf1", qualifier).get(0).getValue().toByteArray();
    }
}
