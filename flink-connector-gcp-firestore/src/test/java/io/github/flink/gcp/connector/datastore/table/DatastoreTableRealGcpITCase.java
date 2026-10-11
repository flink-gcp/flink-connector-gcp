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

package io.github.flink.gcp.connector.datastore.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.LatLng;
import com.google.cloud.datastore.PathElement;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreRealGcpITCase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs the {@code datastore} table's geographical points and key values against the real service in
 * Datastore mode: a SQL write, a scan and a lookup of both, and the service's answer to a key value
 * outside the entity's partition, which the docs and ADR-0184 rest on.
 */
@EnabledIfEnvironmentVariable(named = "FIRESTORE_IT_PROJECT", matches = ".+")
@Tag("gated")
class DatastoreTableRealGcpITCase extends AbstractDatastoreRealGcpITCase {

    private static final String NAMESPACE = "tenant-a";

    /**
     * Closes a key path element literal: every element of an ARRAY literal is cast to the column's
     * element type, because Flink 1.20's planner infers NOT NULL fields for an ARRAY of ROW
     * literals whose elements hold NULL in different fields, and fails the conversion.
     */
    private static final String ELEMENT = " AS ROW<kind STRING, name STRING, id BIGINT>)";

    private static String with(String kind, String... extra) {
        StringBuilder options =
                new StringBuilder(
                        "'connector' = 'datastore', 'project' = '"
                                + PROJECT
                                + "', 'database' = '"
                                + database().getDatabaseId()
                                + "', 'kind' = '"
                                + kind
                                + "', 'namespace' = '"
                                + NAMESPACE
                                + "'");
        for (String option : extra) {
            options.append(", ").append(option);
        }
        return "WITH (" + options + ")";
    }

    private static List<Row> collect(TableEnvironment table, String query) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(query).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static Key.Builder keyIn(String kind, String name) {
        return Key.newBuilder(PROJECT, kind, name, database().getDatabaseId())
                .setNamespace(NAMESPACE);
    }

    /**
     * The service stores a key value whose namespace differs from its entity's, and refuses one in
     * another database and one naming a project that does not exist; the emulator stores all three.
     * Another existing project is not measured: the suite owns no second project.
     */
    @Test
    void theServiceStoresAKeyValueOfAnotherNamespaceButNotOfAnotherDatabaseOrAMissingProject() {
        String kind = uniqueKind();
        Key otherNamespace =
                Key.newBuilder(PROJECT, "User", "u", database().getDatabaseId()).build();
        client().put(
                        Entity.newBuilder(keyIn(kind, "ns").build())
                                .set("ref", otherNamespace)
                                .build());
        assertThat(read(keyIn(kind, "ns").build()).getKey("ref")).isEqualTo(otherNamespace);

        Key otherDatabase = Key.newBuilder(PROJECT, "User", "u").setNamespace(NAMESPACE).build();
        assertThatThrownBy(
                        () ->
                                client().put(
                                                Entity.newBuilder(keyIn(kind, "db").build())
                                                        .set("ref", otherDatabase)
                                                        .build()))
                .isInstanceOf(DatastoreException.class)
                .hasMessageContaining("INVALID_ARGUMENT")
                .hasMessageContaining("does not match containing entity's");
        Key otherProject =
                Key.newBuilder("flink-gcp-no-such-project", "User", "u", database().getDatabaseId())
                        .setNamespace(NAMESPACE)
                        .build();
        assertThatThrownBy(
                        () ->
                                client().put(
                                                Entity.newBuilder(keyIn(kind, "project").build())
                                                        .set("ref", otherProject)
                                                        .build()))
                .isInstanceOf(DatastoreException.class)
                .hasMessageContaining("INVALID_ARGUMENT")
                .hasMessageContaining("The project flink-gcp-no-such-project does not exist");
        assertThat(read(keyIn(kind, "db").build())).isNull();
        assertThat(read(keyIn(kind, "project").build())).isNull();
    }

    @Test
    void pointsAndKeyValuesRoundTripThroughTheSinkTheScanAndTheLookup() throws Exception {
        String trips = uniqueKind();
        String users = uniqueKind();
        client().put(Entity.newBuilder(keyIn(users, "u1").build()).set("label", "Ada").build());
        TableEnvironment batch =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        String columns =
                "(id STRING NOT NULL,"
                        + " place ROW<latitude DOUBLE, longitude DOUBLE>,"
                        + " owner ROW<kind STRING, name STRING, id BIGINT>,"
                        + " line ROW<kind STRING, name STRING, id BIGINT,"
                        + " ancestors ARRAY<ROW<kind STRING, name STRING, id BIGINT>>>,"
                        + " refs ARRAY<ROW<kind STRING, name STRING, id BIGINT>>,"
                        + " PRIMARY KEY (id) NOT ENFORCED) ";
        String markers =
                "'geo-point-field-paths' = 'place', 'reference-field-paths' = 'owner;line;refs'";
        batch.executeSql("CREATE TABLE trips " + columns + with(trips, markers));

        batch.executeSql(
                        "INSERT INTO trips VALUES ('t1',"
                                + " ROW(35.5, -139.25),"
                                + " ROW('"
                                + users
                                + "', 'u1', CAST(NULL AS BIGINT)),"
                                + " ROW('Line', CAST(NULL AS STRING), 3,"
                                + " ARRAY[CAST(ROW('Org', 'o', CAST(NULL AS BIGINT))"
                                + ELEMENT
                                + ", CAST(ROW('Route', CAST(NULL AS STRING), -7)"
                                + ELEMENT
                                + "]),"
                                + " ARRAY[CAST(ROW('User', CAST(NULL AS STRING), 9)"
                                + ELEMENT
                                + "])")
                .await(120, TimeUnit.SECONDS);

        Entity stored = read(keyIn(trips, "t1").build());
        assertThat(stored.getLatLng("place")).isEqualTo(LatLng.of(35.5, -139.25));
        assertThat(stored.getKey("owner")).isEqualTo(keyIn(users, "u1").build());
        assertThat(stored.getKey("line"))
                .isEqualTo(
                        Key.newBuilder(PROJECT, "Line", 3L, database().getDatabaseId())
                                .setNamespace(NAMESPACE)
                                .addAncestors(
                                        PathElement.of("Org", "o"), PathElement.of("Route", -7L))
                                .build());

        List<Row> scanned = collect(batch, "SELECT * FROM trips");
        assertThat(scanned).hasSize(1);
        Row row = scanned.get(0);
        assertThat(row.getField("place")).isEqualTo(Row.of(35.5, -139.25));
        assertThat(row.getField("owner")).isEqualTo(Row.of(users, "u1", null));
        Row line = (Row) row.getField("line");
        assertThat((Row[]) line.getField(3))
                .containsExactly(Row.of("Org", "o", null), Row.of("Route", null, -7L));
        assertThat((Row[]) row.getField("refs")).containsExactly(Row.of("User", null, 9L));

        TableEnvironment streaming =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        streaming.executeSql("CREATE TABLE trips " + columns + with(trips, markers));
        streaming.executeSql(
                "CREATE TABLE users (id STRING NOT NULL, label STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(users));
        streaming.executeSql(
                "CREATE TEMPORARY VIEW probe AS SELECT k, PROCTIME() AS t"
                        + " FROM (VALUES ('t1')) AS v(k)");
        streaming.executeSql(
                "CREATE TEMPORARY VIEW joined AS SELECT p.k, d.place, d.owner, PROCTIME() AS t"
                        + " FROM probe AS p JOIN trips FOR SYSTEM_TIME AS OF p.t AS d"
                        + " ON p.k = d.id");

        List<Row> joined =
                collect(
                        streaming,
                        "SELECT j.k, j.place, u.label FROM joined AS j"
                                + " JOIN users FOR SYSTEM_TIME AS OF j.t AS u"
                                + " ON j.owner.name = u.id");

        assertThat(joined).containsExactly(Row.of("t1", Row.of(35.5, -139.25), "Ada"));
    }
}
