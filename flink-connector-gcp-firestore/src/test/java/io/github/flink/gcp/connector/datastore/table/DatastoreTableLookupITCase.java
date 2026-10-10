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
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Joins against entities the stock client wrote, through SQL lookups against the emulator. */
@Testcontainers
class DatastoreTableLookupITCase extends AbstractDatastoreEmulatorITCase {

    private static TableEnvironment batch() {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        return table;
    }

    private static String with(String kind, boolean async, String... extra) {
        StringBuilder options =
                new StringBuilder(
                        "'connector' = 'datastore', 'project' = '"
                                + PROJECT
                                + "', 'kind' = '"
                                + kind
                                + "', 'emulator-endpoint' = '"
                                + emulatorEndpoint()
                                + "', 'lookup.async' = '"
                                + async
                                + "'");
        for (String option : extra) {
            options.append(", ").append(option);
        }
        return "WITH (" + options + ")";
    }

    /** A probe of the given SQL key literals, one row each, with a processing-time attribute. */
    private static void probe(TableEnvironment table, String... keys) {
        table.executeSql(
                "CREATE TEMPORARY VIEW probe AS SELECT k, PROCTIME() AS t FROM (VALUES ("
                        + String.join("), (", keys)
                        + ")) AS v(k)");
    }

    private static String quoted(String key) {
        return "'" + key + "'";
    }

    private static List<Row> collect(TableEnvironment table, String query) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(query).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    @ParameterizedTest(name = "async={0}")
    @ValueSource(booleans = {false, true})
    void aLookupReadsTheEntityWithTheNameAndNoRowForAnyOther(boolean async) throws Exception {
        String kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "a")).set("name", "Ada").set("n", 1L).build());
        // A child named like a root key the probe asks for: the lookup reads root keys only.
        client().put(
                        Entity.newBuilder(Key.newBuilder(key(kind, "a"), kind, "x").build())
                                .set("name", "child")
                                .build());
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, name STRING, n BIGINT,"
                        + " keyName STRING METADATA FROM 'key-name' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, async));
        String tooLong = "x".repeat(1501);
        probe(
                table,
                quoted("a"),
                quoted("missing"),
                quoted("x"),
                quoted(""),
                quoted("__x__"),
                quoted(tooLong),
                "CAST(NULL AS STRING)");

        List<Row> rows =
                collect(
                        table,
                        "SELECT p.k, d.name, d.n, d.keyName FROM probe AS p LEFT JOIN dim"
                                + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id");

        // The service refuses a lookup of the empty name or one over 1,500 bytes; neither is
        // sent, so neither fails the join.
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("a", "Ada", 1L, "a"),
                        Row.of("missing", null, null, null),
                        Row.of("x", null, null, null),
                        Row.of("", null, null, null),
                        Row.of("__x__", null, null, null),
                        Row.of(tooLong, null, null, null),
                        Row.of(null, null, null, null));
    }

    @ParameterizedTest(name = "async={0}")
    @ValueSource(booleans = {false, true})
    void aLookupReadsANumericIdInTheTablesNamespace(boolean async) throws Exception {
        String kind = uniqueKind();
        for (long id : new long[] {-5L, 7L}) {
            client().put(
                            Entity.newBuilder(
                                            Key.newBuilder(PROJECT, kind, id)
                                                    .setNamespace("tenant")
                                                    .build())
                                    .set("v", id * 10)
                                    .build());
        }
        client().put(
                        Entity.newBuilder(Key.newBuilder(PROJECT, kind, 8L).build())
                                .set("v", 80L)
                                .build());
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id BIGINT NOT NULL, v BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, async, "'namespace' = 'tenant'"));
        probe(table, "CAST(7 AS BIGINT)", "-5", "0", "8", "CAST(NULL AS BIGINT)");

        List<Row> rows =
                collect(
                        table,
                        "SELECT p.k, d.v FROM probe AS p LEFT JOIN dim"
                                + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id");

        // 8 lives in the default namespace; the id 0 addresses no entity and is not sent.
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(7L, 70L),
                        Row.of(-5L, -50L),
                        Row.of(0L, null),
                        Row.of(8L, null),
                        Row.of(null, null));
    }

    @ParameterizedTest(name = "async={0}")
    @ValueSource(booleans = {false, true})
    void aLookupOfOnlyTheKeyAndMetadataReadsTheMetadata(boolean async) throws Exception {
        String kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "a")).set("name", "Ada").build());
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL,"
                        + " version BIGINT NOT NULL METADATA VIRTUAL,"
                        + " updated TIMESTAMP_LTZ(6) NOT NULL METADATA FROM 'update-time' VIRTUAL,"
                        + " readAt TIMESTAMP_LTZ(6) NOT NULL METADATA FROM 'read-time' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, async));
        probe(table, quoted("a"), quoted("missing"));

        List<Row> rows =
                collect(
                        table,
                        "SELECT p.k, d.id, d.version > 0, d.readAt >= d.updated FROM probe AS p"
                                + " LEFT JOIN dim FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id");

        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("a", "a", true, true), Row.of("missing", null, null, null));
    }

    @ParameterizedTest(name = "async={0}")
    @ValueSource(booleans = {false, true})
    void aMismatchFailsTheLookupOrReadsAsNullUnderThePolicy(boolean async) throws Exception {
        String kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "bad")).set("n", "not a number").build());
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE strict (id STRING NOT NULL, n BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, async));
        table.executeSql(
                "CREATE TABLE lenient (id STRING NOT NULL, n BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, async, "'type-mismatch-policy' = 'null'"));
        probe(table, quoted("bad"));

        assertThatThrownBy(
                        () ->
                                collect(
                                        table,
                                        "SELECT p.k, d.n FROM probe AS p JOIN strict"
                                                + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id"))
                .hasStackTraceContaining("cannot be read into the table");
        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.n FROM probe AS p JOIN lenient"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id"))
                .containsExactly(Row.of("bad", null));
    }

    @ParameterizedTest(name = "async={0}")
    @ValueSource(booleans = {false, true})
    void aLookupReadsTheCurrentEntityWhateverScanReadTimeSays(boolean async) throws Exception {
        String kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "early")).set("name", "Eve").build());
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE firstRead (id STRING NOT NULL,"
                        + " readAt TIMESTAMP_LTZ(6) NOT NULL METADATA FROM 'read-time' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, async));
        // A time the emulator issued, before the entity the lookup reads was written.
        Instant before =
                (Instant) collect(table, "SELECT readAt FROM firstRead").get(0).getField(0);
        client().put(Entity.newBuilder(key(kind, "a")).set("name", "Ada").build());
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, name STRING, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, async, "'scan.read-time' = '" + before + "'"));
        probe(table, quoted("a"));

        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.name FROM probe AS p LEFT JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id"))
                .containsExactly(Row.of("a", "Ada"));
    }

    @ParameterizedTest(name = "async={0}")
    @ValueSource(booleans = {false, true})
    void additionalEqualityKeysFilterUnderBothCaches(boolean async) throws Exception {
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(key(kind, "a"))
                                .set("name", "Ada")
                                .set("tier", "gold")
                                .build());
        client().put(
                        Entity.newBuilder(key(kind, "b"))
                                .set("name", "Bob")
                                .set("tier", "silver")
                                .build());
        for (boolean partial : new boolean[] {false, true}) {
            TableEnvironment table = batch();
            String[] cache =
                    partial
                            ? new String[] {
                                "'lookup.cache' = 'PARTIAL'",
                                "'lookup.partial-cache.max-rows' = '10'"
                            }
                            : new String[0];
            // The key follows the attribute so a constant key precedes the addressing key.
            table.executeSql(
                    "CREATE TABLE dim (tier STRING, name STRING, id STRING NOT NULL,"
                            + " PRIMARY KEY (id) NOT ENFORCED) "
                            + with(kind, async, cache));
            probe(table, quoted("a"), quoted("b"), quoted("missing"), quoted("a"));

            assertThat(
                            collect(
                                    table,
                                    "SELECT p.k, d.name FROM probe AS p LEFT JOIN dim"
                                            + " FOR SYSTEM_TIME AS OF p.t AS d"
                                            + " ON p.k = d.id AND d.tier = 'gold'"))
                    .as("partial=%s", partial)
                    .containsExactlyInAnyOrder(
                            Row.of("a", "Ada"),
                            Row.of("a", "Ada"),
                            Row.of("b", null),
                            Row.of("missing", null));
        }
    }
}
