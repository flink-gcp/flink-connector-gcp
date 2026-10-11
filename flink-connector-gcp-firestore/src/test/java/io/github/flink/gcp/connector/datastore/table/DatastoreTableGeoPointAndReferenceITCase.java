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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityValue;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyValue;
import com.google.cloud.datastore.LatLng;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Value;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Writes geographical points and key values through the SQL sink, and reads them back through the
 * scan and the lookup, against the emulator.
 */
@Testcontainers
class DatastoreTableGeoPointAndReferenceITCase extends AbstractDatastoreEmulatorITCase {

    private static final String NAMESPACE = "tenant-a";

    /**
     * Closes a key path element literal: every element of an ARRAY literal is cast to the column's
     * element type, because Flink 1.20's planner infers NOT NULL fields for an ARRAY of ROW
     * literals whose elements hold NULL in different fields, and fails the conversion.
     */
    private static final String ELEMENT = " AS ROW<kind STRING, name STRING, id BIGINT>)";

    private static final String COLUMNS =
            "(id STRING NOT NULL,"
                    + " place ROW<latitude DOUBLE, longitude DOUBLE>,"
                    + " owner ROW<kind STRING, name STRING, id BIGINT>,"
                    + " line ROW<kind STRING, name STRING, id BIGINT,"
                    + " ancestors ARRAY<ROW<kind STRING, name STRING, id BIGINT>>>,"
                    + " stops ARRAY<ROW<label STRING, spot ROW<latitude DOUBLE,"
                    + " longitude DOUBLE>>>,"
                    + " refs ARRAY<ROW<kind STRING, name STRING, id BIGINT>>,"
                    + " PRIMARY KEY (id) NOT ENFORCED) ";

    private static final String MARKERS =
            "'geo-point-field-paths' = 'place;stops.spot',"
                    + " 'reference-field-paths' = 'owner;line;refs'";

    private static TableEnvironment batch() {
        return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    }

    private static TableEnvironment streaming() {
        return TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
    }

    private static String with(String kind, String... extra) {
        StringBuilder options =
                new StringBuilder(
                        "'connector' = 'datastore', 'project' = '"
                                + PROJECT
                                + "', 'kind' = '"
                                + kind
                                + "', 'namespace' = '"
                                + NAMESPACE
                                + "', 'emulator-endpoint' = '"
                                + emulatorEndpoint()
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
        return Key.newBuilder(PROJECT, kind, name).setNamespace(NAMESPACE);
    }

    @Test
    void pointsAndKeyValuesRoundTripThroughTheSinkAndTheScan() throws Exception {
        String kind = uniqueKind();
        TableEnvironment table = batch();
        table.executeSql("CREATE TABLE trips " + COLUMNS + with(kind, MARKERS));

        table.executeSql(
                        "INSERT INTO trips VALUES ('t1',"
                                + " ROW(35.5, -139.25),"
                                + " ROW('User', 'u1', CAST(NULL AS BIGINT)),"
                                + " ROW('Line', CAST(NULL AS STRING), 3,"
                                + " ARRAY[CAST(ROW('Org', 'o', CAST(NULL AS BIGINT))"
                                + ELEMENT
                                + ", CAST(ROW('Route', CAST(NULL AS STRING), -7)"
                                + ELEMENT
                                + "]),"
                                + " ARRAY[ROW('a', ROW(-90.0, 180.0))],"
                                + " ARRAY[CAST(ROW('User', CAST(NULL AS STRING), 9)"
                                + ELEMENT
                                + ", CAST(ROW('User', 'u2', CAST(NULL AS BIGINT))"
                                + ELEMENT
                                + "])")
                .await(60, TimeUnit.SECONDS);

        Entity stored =
                client().get(Key.newBuilder(PROJECT, kind, "t1").setNamespace(NAMESPACE).build());
        assertThat(stored.getLatLng("place")).isEqualTo(LatLng.of(35.5, -139.25));
        assertThat(stored.getKey("owner")).isEqualTo(keyIn("User", "u1").build());
        assertThat(stored.getKey("line"))
                .isEqualTo(
                        Key.newBuilder(PROJECT, "Line", 3L)
                                .setNamespace(NAMESPACE)
                                .addAncestors(
                                        PathElement.of("Org", "o"), PathElement.of("Route", -7L))
                                .build());
        FullEntity<?> stop = ((EntityValue) stored.getList("stops").get(0)).get();
        assertThat(stop.getLatLng("spot")).isEqualTo(LatLng.of(-90.0, 180.0));
        List<Value<?>> refs = stored.getList("refs");
        assertThat(refs)
                .extracting(value -> ((KeyValue) value).get())
                .containsExactly(
                        Key.newBuilder(PROJECT, "User", 9L).setNamespace(NAMESPACE).build(),
                        keyIn("User", "u2").build());

        List<Row> rows = collect(table, "SELECT * FROM trips");

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        assertThat(row.getField("place")).isEqualTo(Row.of(35.5, -139.25));
        assertThat(row.getField("owner")).isEqualTo(Row.of("User", "u1", null));
        Row line = (Row) row.getField("line");
        assertThat(line.getField(0)).isEqualTo("Line");
        assertThat(line.getField(1)).isNull();
        assertThat(line.getField(2)).isEqualTo(3L);
        assertThat((Row[]) line.getField(3))
                .containsExactly(Row.of("Org", "o", null), Row.of("Route", null, -7L));
        Row[] stops = (Row[]) row.getField("stops");
        assertThat(stops).containsExactly(Row.of("a", Row.of(-90.0, 180.0)));
        assertThat((Row[]) row.getField("refs"))
                .containsExactly(Row.of("User", null, 9L), Row.of("User", "u2", null));
    }

    @Test
    void aLookupReadsPointsAndKeyValuesAndJoinsOnAKeyValuesName() throws Exception {
        String users = uniqueKind();
        String trips = uniqueKind();
        client().put(
                        Entity.newBuilder(keyIn(trips, "t1").build())
                                .set("place", LatLng.of(1.5, 2.5))
                                .set("owner", keyIn(users, "u1").build())
                                .build(),
                        Entity.newBuilder(keyIn(users, "u1").build()).set("label", "Ada").build());
        TableEnvironment table = streaming();
        table.executeSql(
                "CREATE TABLE trips (id STRING NOT NULL,"
                        + " place ROW<latitude DOUBLE, longitude DOUBLE>,"
                        + " owner ROW<kind STRING, name STRING, id BIGINT>,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(
                                trips,
                                "'geo-point-field-paths' = 'place'",
                                "'reference-field-paths' = 'owner'"));
        table.executeSql(
                "CREATE TABLE users (id STRING NOT NULL, label STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(users));
        table.executeSql(
                "CREATE TEMPORARY VIEW probe AS SELECT k, PROCTIME() AS t"
                        + " FROM (VALUES ('t1')) AS v(k)");
        table.executeSql(
                "CREATE TEMPORARY VIEW joined AS SELECT p.k, d.place, d.owner, PROCTIME() AS t"
                        + " FROM probe AS p JOIN trips FOR SYSTEM_TIME AS OF p.t AS d"
                        + " ON p.k = d.id");

        List<Row> rows =
                collect(
                        table,
                        "SELECT j.k, j.place, j.owner.kind, u.label FROM joined AS j"
                                + " JOIN users FOR SYSTEM_TIME AS OF j.t AS u"
                                + " ON j.owner.name = u.id");

        assertThat(rows).containsExactly(Row.of("t1", Row.of(1.5, 2.5), users, "Ada"));
    }

    @Test
    void aKeyValueInAnotherNamespaceFailsTheReadOrReadsAsNull() throws Exception {
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(keyIn(kind, "t1").build())
                                .set("owner", Key.newBuilder(PROJECT, "User", "u1").build())
                                .build());
        TableEnvironment table = batch();
        String columns =
                "(id STRING NOT NULL, owner ROW<kind STRING, name STRING, id BIGINT>,"
                        + " PRIMARY KEY (id) NOT ENFORCED) ";
        table.executeSql(
                "CREATE TABLE strict " + columns + with(kind, "'reference-field-paths' = 'owner'"));
        table.executeSql(
                "CREATE TABLE lenient "
                        + columns
                        + with(
                                kind,
                                "'reference-field-paths' = 'owner'",
                                "'type-mismatch-policy' = 'null'"));

        assertThatThrownBy(() -> collect(table, "SELECT * FROM strict"))
                .hasStackTraceContaining(
                        "which is not in the table's project, database and namespace");
        assertThat(collect(table, "SELECT * FROM lenient")).containsExactly(Row.of("t1", null));
    }

    @Test
    void aKeyValueThatIsNotACompleteKeyFailsTheJob() {
        String kind = uniqueKind();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE trips (id STRING NOT NULL,"
                        + " owner ROW<kind STRING, name STRING, id BIGINT>,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, "'reference-field-paths' = 'owner'"));

        assertThatThrownBy(
                        () ->
                                table.executeSql(
                                                "INSERT INTO trips VALUES ('t1', ROW('User',"
                                                        + " CAST(NULL AS STRING),"
                                                        + " CAST(NULL AS BIGINT)))")
                                        .await(60, TimeUnit.SECONDS))
                .hasStackTraceContaining(
                        "Field 'owner' is a key value, but its last path element has neither a"
                                + " name nor an id");
        assertThat(client().get(keyIn(kind, "t1").build())).isNull();
    }
}
