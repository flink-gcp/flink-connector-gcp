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

package io.github.flink.gcp.connector.bigquery.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import io.github.flink.gcp.connector.testutils.bigquery.BigQueryEmulatorContainers;
import io.github.flink.gcp.connector.testutils.sql.AbstractSqlConnectorSmokeITCase;
import io.github.flink.gcp.connector.testutils.sql.ShadedJar;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs a SQL job through the shaded classes, against the BigQuery emulator.
 *
 * <p>The only test here that exercises relocation at <em>runtime</em>. {@link
 * BigQuerySqlConnectorPackagingITCase} proves the jar has the right shape; this proves the shape
 * works — opening Storage Read and Write API streams is what puts relocated gRPC, relocated Avro,
 * and relocated netty together, and a mistake there is invisible to any jar-content assertion.
 *
 * <p>Both halves of the sink are exercised on purpose. The table is auto-created, so the run goes
 * through the relocated REST client as well as the relocated gRPC one — and those are two
 * transports on two ports, which is why the DDL below carries two emulator options rather than the
 * one every sibling connector needs.
 *
 * <p>The connector under test comes from the uber-jar, not from the reactor's classes: the module's
 * surefire configuration drops {@code flink-connector-gcp-bigquery} from the test classpath and
 * adds the shaded jar. {@link #theConnectorUnderTestComesFromTheShadedJar()} asserts that rather
 * than trusting it, because a regression there would leave this whole class passing against
 * unshaded code and proving nothing.
 *
 * <p>The harness drives the emulator with the <em>stock</em> BigQuery REST client while the
 * connector uses its relocated copy. That the two coexist on one classpath is not incidental — it
 * is the property an uber-jar exists to provide.
 */
@Testcontainers
@Timeout(180)
class BigQuerySqlConnectorSmokeITCase extends AbstractSqlConnectorSmokeITCase {

    // The emulator uses the project as an Avro namespace on reads; a hyphen is not legal there.
    private static final String PROJECT = "itproject";

    private static final String DATASET = "it_dataset";

    @Container
    private static final GenericContainer<?> EMULATOR =
            BigQueryEmulatorContainers.newContainer(PROJECT, DATASET);

    private static BigQuery restClient;

    @BeforeAll
    static void createRestClient() {
        restClient = BigQueryEmulatorContainers.restClient(EMULATOR, PROJECT);
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
    void whatSqlWritesThroughTheShadedClassesLandsInATableItCreated() throws Exception {
        String table = "sql_smoke";
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // Nothing creates it but the sink, which is what makes the REST half of this run real
        // rather than incidental: without it, a regressed auto-create path would still pass here
        // if the emulator happened to materialise the table on first append.
        assertThat(restClient.getTable(TableId.of(DATASET, table)))
                .as("the table this test claims the sink creates")
                .isNull();

        tEnv.executeSql(
                "CREATE TABLE events (\n"
                        + "  name STRING,\n"
                        + "  amount BIGINT\n"
                        + ") "
                        + withOptions(table));

        // Bounded: the no-argument await() has no timeout, so a job that never finishes would hang
        // to the class timeout and report an interrupt instead of a diagnosable assertion.
        tEnv.executeSql("INSERT INTO events VALUES ('alice', 1), ('bob', 2)")
                .await(60, TimeUnit.SECONDS);

        assertThat(query("SELECT name FROM `" + qualified(table) + "` ORDER BY name"))
                .containsExactly("alice", "bob");
        assertThat(query("SELECT amount FROM `" + qualified(table) + "` ORDER BY amount"))
                .containsExactly("1", "2");

        List<String> sourceRows = new ArrayList<>();
        try (CloseableIterator<Row> rows = tEnv.executeSql("SELECT name FROM events").collect()) {
            rows.forEachRemaining(row -> sourceRows.add(row.getFieldAs(0).toString()));
        }
        assertThat(sourceRows).containsExactlyInAnyOrder("alice", "bob");
    }

    private static String qualified(String table) {
        return PROJECT + "." + DATASET + "." + table;
    }

    /** The first column of every row the query returns, as a string. */
    private static List<String> query(String sql) throws InterruptedException {
        List<String> values = new ArrayList<>();
        restClient
                .query(QueryJobConfiguration.newBuilder(sql).build())
                .iterateAll()
                .forEach((FieldValueList row) -> values.add(row.get(0).getStringValue()));
        return values;
    }

    /**
     * Renders a {@code WITH} clause carrying the connector, the destination and both emulator
     * endpoints.
     */
    private static String withOptions(String table) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", "bigquery");
        options.put("project", PROJECT);
        options.put("dataset", DATASET);
        options.put("table", table);
        options.put("emulator-endpoint", BigQueryEmulatorContainers.grpcEndpoint(EMULATOR));
        options.put("emulator-rest-endpoint", BigQueryEmulatorContainers.restEndpoint(EMULATOR));
        return options.entrySet().stream()
                .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                .collect(Collectors.joining(",\n  ", "WITH (\n  ", "\n)"));
    }
}
