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

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.GeoPoint;
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
 * Runs a SQL insert through the shaded classes against the Firestore emulator.
 *
 * <p>The module's integration-test classpath excludes the plain connector and adds the uber-jar.
 * {@link #theConnectorUnderTestComesFromTheShadedJar()} proves that boundary. The connector writes
 * through its relocated Firestore client while this harness reads the document back with the stock
 * client that arrives transitively from the connector, so the two coexist on one classpath. The row
 * carries every value the connector builds from relocated client-library classes rather than
 * passing through: a geographical point, a reference, bytes and a timestamp.
 */
@Testcontainers
@Timeout(180)
class FirestoreSqlConnectorSmokeITCase extends AbstractSqlConnectorSmokeITCase {

    private static final String PROJECT = "it-project";

    @Container
    private static final FirestoreEmulatorContainer EMULATOR =
            FirestoreEmulatorContainers.newContainer();

    private static Firestore client;

    @BeforeAll
    static void openClient() {
        client =
                FirestoreOptions.newBuilder()
                        .setProjectId(PROJECT)
                        .setEmulatorHost(EMULATOR.getEmulatorEndpoint())
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
        return UberJar.FACTORY_CLASS;
    }

    @Test
    void sqlWritesTheDocumentThroughTheShadedFactory() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.executeSql(
                "CREATE TABLE stores (\n"
                        + "  id STRING NOT NULL,\n"
                        + "  name STRING,\n"
                        + "  location ROW<latitude DOUBLE, longitude DOUBLE>,\n"
                        + "  owner STRING,\n"
                        + "  logo BYTES,\n"
                        + "  opened TIMESTAMP_LTZ(3),\n"
                        + "  PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'firestore',\n"
                        + "  'project' = '"
                        + PROJECT
                        + "',\n"
                        + "  'collection' = 'stores',\n"
                        + "  'geo-point-field-paths' = 'location',\n"
                        + "  'reference-field-paths' = 'owner',\n"
                        + "  'emulator-endpoint' = '"
                        + EMULATOR.getEmulatorEndpoint()
                        + "'\n"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO stores VALUES ('tokyo', '東京', ROW(35.68, 139.76),"
                                + " 'owners/alice', X'CAFE', TO_TIMESTAMP_LTZ(1700000000123, 3))")
                .await(60, TimeUnit.SECONDS);

        DocumentSnapshot stored = client.document("stores/tokyo").get().get(30, TimeUnit.SECONDS);
        assertThat(stored.getString("name")).isEqualTo("東京");
        assertThat(stored.getGeoPoint("location")).isEqualTo(new GeoPoint(35.68, 139.76));
        assertThat(stored.get("owner")).isInstanceOf(DocumentReference.class);
        assertThat(((DocumentReference) stored.get("owner")).getPath()).isEqualTo("owners/alice");
        assertThat(stored.getBlob("logo").toBytes()).containsExactly(0xCA, 0xFE);
        assertThat(stored.getTimestamp("opened"))
                .isEqualTo(Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 123_000_000));
    }
}
