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

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityValue;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.LatLng;
import com.google.cloud.datastore.ListValue;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.StringValue;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Reads entities written by the client library through a SQL scan of the emulator. */
@Testcontainers
class DatastoreTableSourceITCase extends AbstractDatastoreEmulatorITCase {

    private static TableEnvironment batch() {
        return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    }

    private static String with(String kind, String... extra) {
        StringBuilder options =
                new StringBuilder(
                        "'connector' = 'datastore', 'project' = '"
                                + PROJECT
                                + "', 'kind' = '"
                                + kind
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

    @Test
    void everyTypeIsReadBackWithTheMetadata() throws Exception {
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(key(kind, "o1"))
                                .set("s", "text")
                                .set("b", true)
                                .set("n", 42L)
                                .set("d", 1.5d)
                                .set("payload", Blob.copyFrom(new byte[] {1, 2}))
                                .set(
                                        "ts",
                                        Timestamp.ofTimeSecondsAndNanos(
                                                1_700_000_000L, 123_456_000))
                                .set("tags", ListValue.of(StringValue.of("a"), NullValue.of()))
                                .set(
                                        "nested",
                                        EntityValue.of(
                                                FullEntity.newBuilder()
                                                        .set("x", 1L)
                                                        .set(
                                                                "child",
                                                                FullEntity.newBuilder()
                                                                        .set("y", "deep")
                                                                        .build())
                                                        .build()))
                                .setNull("missing")
                                .build());
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE source ("
                        + " id STRING NOT NULL,"
                        + " s STRING, b BOOLEAN, n BIGINT, d DOUBLE, payload BYTES,"
                        + " ts TIMESTAMP_LTZ(6), tags ARRAY<STRING>,"
                        + " nested ROW<x BIGINT, child ROW<y STRING>>, missing STRING, absent STRING,"
                        + " keyName STRING METADATA FROM 'key-name' VIRTUAL,"
                        + " keyId BIGINT METADATA FROM 'key-id' VIRTUAL,"
                        + " version BIGINT NOT NULL METADATA VIRTUAL,"
                        + " created TIMESTAMP_LTZ(6) NOT NULL METADATA FROM 'create-time' VIRTUAL,"
                        + " updated TIMESTAMP_LTZ(6) NOT NULL METADATA FROM 'update-time' VIRTUAL,"
                        + " readAt TIMESTAMP_LTZ(6) NOT NULL METADATA FROM 'read-time' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));

        List<Row> rows = collect(table, "SELECT * FROM source");

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        assertThat(row.getField("id")).isEqualTo("o1");
        assertThat(row.getField("s")).isEqualTo("text");
        assertThat(row.getField("b")).isEqualTo(true);
        assertThat(row.getField("n")).isEqualTo(42L);
        assertThat(row.getField("d")).isEqualTo(1.5d);
        assertThat((byte[]) row.getField("payload")).containsExactly(1, 2);
        assertThat(row.getField("ts"))
                .isEqualTo(Instant.ofEpochSecond(1_700_000_000L, 123_456_000));
        assertThat((String[]) row.getField("tags")).containsExactly("a", null);
        Row nested = (Row) row.getField("nested");
        assertThat(nested.getField(0)).isEqualTo(1L);
        assertThat(((Row) nested.getField(1)).getField(0)).isEqualTo("deep");
        assertThat(row.getField("missing")).isNull();
        assertThat(row.getField("absent")).isNull();
        assertThat(row.getField("keyName")).isEqualTo("o1");
        assertThat(row.getField("keyId")).isNull();
        assertThat((Long) row.getField("version")).isPositive();
        Instant created = (Instant) row.getField("created");
        Instant updated = (Instant) row.getField("updated");
        Instant readAt = (Instant) row.getField("readAt");
        assertThat(created).isBeforeOrEqualTo(updated);
        assertThat(updated).isBeforeOrEqualTo(readAt);
    }

    @Test
    void aTableWithoutAKeyReadsTheAllocatedIdsItWroteThroughTheMetadata() throws Exception {
        String kind = uniqueKind();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE events (label STRING,"
                        + " id BIGINT METADATA FROM 'key-id' VIRTUAL) "
                        + with(kind, "'namespace' = 'tenant-a'"));
        table.executeSql("INSERT INTO events (label) VALUES ('a'), ('b')")
                .await(60, TimeUnit.SECONDS);

        List<Row> rows = collect(table, "SELECT label, id FROM events");

        assertThat(rows).extracting(row -> row.getField(0)).containsExactlyInAnyOrder("a", "b");
        assertThat(rows).extracting(row -> row.getField(1)).doesNotContainNull();
    }

    @Test
    void aBigintKeyReadsTheIdAndAKeyOfTheOtherFormFailsTheRead() throws Exception {
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(Key.newBuilder(PROJECT, kind, 7L).build())
                                .set("v", 1L)
                                .build());
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE byId (id BIGINT NOT NULL, v BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));
        table.executeSql(
                "CREATE TABLE byName (id STRING NOT NULL, v BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));

        assertThat(collect(table, "SELECT id, v FROM byId")).containsExactly(Row.of(7L, 1L));
        assertThatThrownBy(() -> collect(table, "SELECT id, v FROM byName"))
                .hasStackTraceContaining("its key has the numeric id 7");
    }

    @Test
    void aMismatchedValueFailsOrReadsAsNullByThePolicy() throws Exception {
        String kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "a")).set("v", "not a number").build());
        client().put(
                        Entity.newBuilder(key(kind, "b"))
                                .set("v", 2L)
                                .set("where", LatLng.of(1.0, 2.0))
                                .build());
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE failing (id STRING NOT NULL, v BIGINT,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));
        table.executeSql(
                "CREATE TABLE lenient (id STRING NOT NULL, v BIGINT, `where` STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, "'type-mismatch-policy' = 'null'"));

        assertThatThrownBy(() -> collect(table, "SELECT * FROM failing"))
                .hasStackTraceContaining("Property 'v' holds a value of type STRING")
                .hasStackTraceContaining("'type-mismatch-policy' = 'null'");
        assertThat(collect(table, "SELECT id, v, `where` FROM lenient"))
                .containsExactlyInAnyOrder(Row.of("a", null, null), Row.of("b", 2L, null));
    }

    @Test
    void aProjectedScanReadsOnlyItsColumnsAndKeepsEveryEntity() throws Exception {
        // Neither entity holds an indexed 'body', and 'a' holds two 'tags': a Datastore
        // projection query would return nothing for 'body' and 'a' once per tag. The scan reads
        // whole entities instead, and converts only the columns it produces: 'n' does not hold
        // an integer, which fails only a query that reads it.
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(key(kind, "a"))
                                .set(
                                        "body",
                                        StringValue.newBuilder("x")
                                                .setExcludeFromIndexes(true)
                                                .build())
                                .set("tags", "t1", "t2")
                                .set("n", "not a number")
                                .build());
        client().put(
                        Entity.newBuilder(key(kind, "b"))
                                .set("tags", ListValue.of(StringValue.of("t3")))
                                .build());
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE source (id STRING NOT NULL, body STRING, tags ARRAY<STRING>,"
                        + " n BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));

        assertThat(collect(table, "SELECT id, body FROM source"))
                .containsExactlyInAnyOrder(Row.of("a", "x"), Row.of("b", null));
        assertThat(collect(table, "SELECT tags, id FROM source"))
                .containsExactlyInAnyOrder(
                        Row.of(new String[] {"t1", "t2"}, "a"), Row.of(new String[] {"t3"}, "b"));
        assertThatThrownBy(() -> collect(table, "SELECT id, n FROM source"))
                .hasStackTraceContaining("Property 'n' holds a value of type STRING");
    }

    @Test
    void aKindQueryReturnsChildEntitiesWhichOnlyATableWithoutAKeyReads() throws Exception {
        String kind = uniqueKind();
        Key parent = Key.newBuilder(PROJECT, "Parent", 1L).build();
        client().put(
                        Entity.newBuilder(Key.newBuilder(parent, kind, "a").build())
                                .set("v", 1L)
                                .build(),
                        Entity.newBuilder(key(kind, "b")).set("v", 2L).build());
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE keyed (id STRING NOT NULL, v BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));
        table.executeSql(
                "CREATE TABLE keyless (v BIGINT,"
                        + " name STRING METADATA FROM 'key-name' VIRTUAL) "
                        + with(kind));

        assertThatThrownBy(() -> collect(table, "SELECT id, v FROM keyed"))
                .hasStackTraceContaining("its key has the parent");
        // The planner drops this aggregate on the key's uniqueness and the key column with it,
        // planning a scan of 'v' alone; the check still runs.
        assertThatThrownBy(() -> collect(table, "SELECT SUM(v) FROM keyed GROUP BY id"))
                .hasStackTraceContaining("its key has the parent");
        assertThat(collect(table, "SELECT v, name FROM keyless"))
                .containsExactlyInAnyOrder(Row.of(1L, "a"), Row.of(2L, "b"));
    }

    @Test
    void theScanReadsAtTheConfiguredReadTime() throws Exception {
        String kind = uniqueKind();
        String columns =
                "(id STRING NOT NULL, v BIGINT,"
                        + " readAt TIMESTAMP_LTZ(6) NOT NULL METADATA FROM 'read-time' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED) ";
        client().put(Entity.newBuilder(key(kind, "early")).set("v", 1L).build());
        TableEnvironment table = batch();
        table.executeSql("CREATE TABLE firstRead " + columns + with(kind));
        // A time the emulator issued, between the two writes: neither clock nor sleep decides it.
        Instant between =
                (Instant) collect(table, "SELECT readAt FROM firstRead").get(0).getField(0);
        client().put(Entity.newBuilder(key(kind, "late")).set("v", 2L).build());
        table.executeSql(
                "CREATE TABLE source "
                        + columns
                        + with(kind, "'scan.read-time' = '" + between + "'"));

        List<Row> rows = collect(table, "SELECT id, readAt FROM source");

        assertThat(rows).extracting(row -> row.getField(0)).containsExactly("early");
        assertThat(rows.get(0).getField(1)).isEqualTo(between);
    }
}
