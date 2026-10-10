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

package io.github.flink.gcp.connector.firestore.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import com.google.cloud.NoCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.testutils.firestore.FirestoreEmulatorContainers;
import io.github.flink.gcp.connector.testutils.sql.AbstractSqlConnectorSmokeITCase;
import io.github.flink.gcp.connector.testutils.sql.ShadedJar;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.FirestoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs a SQL insert through the shaded Datastore-mode classes against the Firestore emulator in
 * Datastore mode.
 *
 * <p>The connector writes through its relocated Datastore client while this harness reads the
 * entity back with the stock client that arrives transitively from the connector. The row carries
 * the values the connector builds from relocated client-library classes: a blob, a timestamp, an
 * embedded entity, and a value excluded from indexes.
 */
@Testcontainers
@Timeout(180)
class DatastoreSqlConnectorSmokeITCase extends AbstractSqlConnectorSmokeITCase {

    private static final String PROJECT = "it-project";

    @Container
    private static final FirestoreEmulatorContainer EMULATOR =
            FirestoreEmulatorContainers.newContainer().withFlags("--database-mode=datastore-mode");

    private static Datastore client;

    @BeforeAll
    static void openClient() {
        client =
                DatastoreOptions.newBuilder()
                        .setProjectId(PROJECT)
                        .setHost(EMULATOR.getEmulatorEndpoint())
                        .setCredentials(NoCredentials.getInstance())
                        .build()
                        .getService();
    }

    @AfterAll
    static void closeClient() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Override
    protected ShadedJar shadedJar() {
        return UberJar.SHADED;
    }

    @Override
    protected String factoryClass() {
        return UberJar.DATASTORE_FACTORY_CLASS;
    }

    @Test
    void sqlWritesTheEntityThroughTheShadedFactory() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.executeSql(
                "CREATE TABLE stores (\n"
                        + "  id STRING NOT NULL,\n"
                        + "  name STRING,\n"
                        + "  address ROW<city STRING>,\n"
                        + "  logo BYTES,\n"
                        + "  opened TIMESTAMP_LTZ(3),\n"
                        + "  PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datastore',\n"
                        + "  'project' = '"
                        + PROJECT
                        + "',\n"
                        + "  'kind' = 'Store',\n"
                        + "  'sink.unindexed-columns' = 'logo',\n"
                        + "  'emulator-endpoint' = '"
                        + EMULATOR.getEmulatorEndpoint()
                        + "'\n"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO stores VALUES ('tokyo', '東京', ROW('Chiyoda'), X'CAFE',"
                                + " TO_TIMESTAMP_LTZ(1700000000123, 3))")
                .await(60, TimeUnit.SECONDS);

        Entity stored = client.get(Key.newBuilder(PROJECT, "Store", "tokyo").build());
        assertThat(stored).isNotNull();
        assertThat(stored.getString("name")).isEqualTo("東京");
        assertThat(stored.getEntity("address").getString("city")).isEqualTo("Chiyoda");
        assertThat(stored.getBlob("logo").toByteArray()).containsExactly(0xCA, 0xFE);
        assertThat(stored.getValue("logo").excludeFromIndexes()).isTrue();
        assertThat(stored.getTimestamp("opened"))
                .isEqualTo(Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 123_000_000));
    }
}
