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

package io.github.flink.gcp.connector.firestore.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Joins against documents the stock client wrote, through SQL lookups against the emulator. */
@Testcontainers
class FirestoreTableLookupITCase extends AbstractFirestoreEmulatorITCase {

    private static TableEnvironment batch() {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        return table;
    }

    private static String with(String collection, boolean async, String... extra) {
        StringBuilder options =
                new StringBuilder(
                        "'connector' = 'firestore', 'project' = '"
                                + PROJECT
                                + "', 'collection' = '"
                                + collection
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

    /** A probe of the given keys, one row each, with a processing-time attribute. */
    private static void probe(TableEnvironment table, String... keys) {
        StringBuilder values = new StringBuilder();
        for (String key : keys) {
            values.append(values.length() == 0 ? "" : ", ")
                    .append(key == null ? "(CAST(NULL AS STRING))" : "('" + key + "')");
        }
        table.executeSql(
                "CREATE TEMPORARY VIEW probe AS SELECT k, PROCTIME() AS t FROM (VALUES "
                        + values
                        + ") AS v(k)");
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
    void aLookupReadsTheDocumentWithTheKeyAndNoRowForAnyOther(boolean async) throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/a").set(Map.of("name", "Ada", "n", 1L)).get();
        client().document(collection + "/a/sub/x").set(Map.of("name", "nested")).get();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, name STRING, n BIGINT,"
                        + " path STRING METADATA FROM 'document-path' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection, async));
        probe(table, "a", "missing", "a/sub/x", "a/", "", "..", "__x__", null);

        List<Row> rows =
                collect(
                        table,
                        "SELECT p.k, d.name, d.n, d.path FROM probe AS p LEFT JOIN dim"
                                + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id");

        // A key holding '/' would address another document ("a/" reads "a", "a/sub/x" a
        // subcollection's); a key the service never stores as an id is not sent to it.
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("a", "Ada", 1L, collection + "/a"),
                        Row.of("missing", null, null, null),
                        Row.of("a/sub/x", null, null, null),
                        Row.of("a/", null, null, null),
                        Row.of("", null, null, null),
                        Row.of("..", null, null, null),
                        Row.of("__x__", null, null, null),
                        Row.of(null, null, null, null));
    }

    @ParameterizedTest(name = "async={0}")
    @ValueSource(booleans = {false, true})
    void aLookupOfOnlyTheKeyAndMetadataReadsNoField(boolean async) throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/a").set(Map.of("name", "Ada")).get();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL,"
                        + " updated TIMESTAMP_LTZ(6) METADATA FROM 'update-time' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection, async));
        probe(table, "a", "missing");

        List<Row> rows =
                collect(
                        table,
                        "SELECT p.k, d.id, d.updated IS NOT NULL FROM probe AS p LEFT JOIN dim"
                                + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id");

        assertThat(rows)
                .containsExactlyInAnyOrder(Row.of("a", "a", true), Row.of("missing", null, false));
    }

    @ParameterizedTest(name = "async={0}")
    @ValueSource(booleans = {false, true})
    void aMismatchFailsTheLookupOrReadsAsNullUnderThePolicy(boolean async) throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/bad").set(Map.of("n", "not a number")).get();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE strict (id STRING NOT NULL, n BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection, async));
        table.executeSql(
                "CREATE TABLE lenient (id STRING NOT NULL, n BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection, async, "'type-mismatch-policy' = 'null'"));
        probe(table, "bad");

        assertThatThrownBy(
                        () ->
                                collect(
                                        table,
                                        "SELECT p.k, d.n FROM probe AS p JOIN strict"
                                                + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id"))
                .hasStackTraceContaining("Field 'n' holds a value of type java.lang.String");
        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.n FROM probe AS p JOIN lenient"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id"))
                .containsExactly(Row.of("bad", null));
    }

    @ParameterizedTest(name = "async={0}")
    @ValueSource(booleans = {false, true})
    void aLookupReadsTheCurrentDocumentWhateverScanReadTimeSays(boolean async) throws Exception {
        String collection = uniqueCollection();
        Instant before = Instant.now().minusSeconds(5);
        client().document(collection + "/a").set(Map.of("name", "Ada")).get();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, name STRING, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection, async, "'scan.read-time' = '" + before + "'"));
        probe(table, "a");

        // At the scan's read time the document did not exist yet.
        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.name FROM probe AS p LEFT JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id"))
                .containsExactly(Row.of("a", "Ada"));
    }

    @ParameterizedTest(name = "async={0}")
    @ValueSource(booleans = {false, true})
    void aJoinUnderAPartialCacheReturnsTheSameRows(boolean async) throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/a").set(Map.of("name", "Ada")).get();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, name STRING, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(
                                collection,
                                async,
                                "'lookup.cache' = 'PARTIAL'",
                                "'lookup.partial-cache.max-rows' = '10'"));
        probe(table, "a", "a", "missing");

        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.name FROM probe AS p LEFT JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id"))
                .containsExactlyInAnyOrder(
                        Row.of("a", "Ada"), Row.of("a", "Ada"), Row.of("missing", null));
    }
}
