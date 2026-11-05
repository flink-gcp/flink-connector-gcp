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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.firestore.GeoPoint;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reads nested fields through the {@code firestore} scan's nested projection against the emulator
 * and compares them with the same fields read from whole columns ({@code docs/adr/0179}, the
 * revision for #1839).
 *
 * <p>The oracle is a table of the same collection that also declares {@code guard ROW<v BIGINT NOT
 * NULL>}, a field no document holds: a {@code NOT NULL} field inside a nullable {@code ROW} turns
 * nested projection off for the whole table, so the oracle reads every column whole. Its plan is
 * asserted, so an oracle that projects paths fails rather than compares the projection with itself.
 */
@Testcontainers
class FirestoreTableNestedProjectionITCase extends AbstractFirestoreEmulatorITCase {

    private static final String COLUMNS =
            "id STRING, r ROW<a BIGINT, b STRING, c ROW<x BIGINT>>, `p.q` ROW<`u.v` STRING>,"
                    + " g ROW<latitude DOUBLE, longitude DOUBLE>";

    private static final String QUERY =
            "SELECT id, r.a, r.b, r.c.x, `p.q`.`u.v`, g.latitude FROM %s";

    private static Map<String, Object> map(Object... entries) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            map.put((String) entries[i], entries[i + 1]);
        }
        return map;
    }

    /** Creates the projected table {@code t} and the oracle {@code o} over one collection. */
    private static TableEnvironment tables(String collection, String policy) {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        String with =
                " PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'firestore', 'project' = '"
                        + PROJECT
                        + "', 'collection' = '"
                        + collection
                        + "', 'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "', 'geo-point-field-paths' = 'g', 'type-mismatch-policy' = '"
                        + policy
                        + "')";
        table.executeSql("CREATE TABLE t (" + COLUMNS + "," + with);
        // The oracle through the Table API: Flink 1.20 drops a NOT NULL that DDL declares on a
        // ROW's field, which would leave nested projection on.
        TableDescriptor.Builder oracle =
                TableDescriptor.forConnector("firestore")
                        .schema(
                                Schema.newBuilder()
                                        .fromResolvedSchema(table.from("t").getResolvedSchema())
                                        .column(
                                                "guard",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "v", DataTypes.BIGINT().notNull())))
                                        .build());
        oracle.option("project", PROJECT)
                .option("collection", collection)
                .option("emulator-endpoint", emulatorEndpoint())
                .option("geo-point-field-paths", "g")
                .option("type-mismatch-policy", policy);
        table.createTemporaryTable("o", oracle.build());
        return table;
    }

    private static List<Row> collect(TableEnvironment table, String query) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(query).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static String write(Object... idsAndDocuments) throws Exception {
        String collection = uniqueCollection();
        for (int i = 0; i < idsAndDocuments.length; i += 2) {
            @SuppressWarnings("unchecked")
            Map<String, Object> document = (Map<String, Object>) idsAndDocuments[i + 1];
            client().document(collection + "/" + idsAndDocuments[i]).set(document).get();
        }
        return collection;
    }

    @Test
    void theOracleReadsWholeColumnsAndTheTableReadsPaths() {
        TableEnvironment table = tables("c", "fail");

        assertThat(table.explainSql(String.format(QUERY, "t")))
                .contains("project=[id, r_a, r_b, r_c_x, p.q_u.v, g_latitude]");
        assertThat(table.explainSql(String.format(QUERY, "o"))).contains("project=[id, r, p.q, g]");
    }

    @Test
    void underTheNullPolicyEveryProjectedFieldEqualsTheWholeColumnsField() throws Exception {
        String collection =
                write(
                        "full",
                        map(
                                "r",
                                map("a", 1L, "b", "x", "c", map("x", 2L), "wide", map("k", 1L)),
                                "p.q",
                                map("u.v", "dotted", "u", map("v", "nested")),
                                "g",
                                new GeoPoint(35.6, 139.7)),
                        "missing",
                        map("other", 1L),
                        "nullRow",
                        map("r", null),
                        "string",
                        map("r", "hello", "g", "not a point"),
                        "array",
                        map("r", List.of(map("a", 1L))),
                        "empty",
                        map("r", map()),
                        "noA",
                        map("r", map("b", "x")),
                        "aNull",
                        map("r", map("a", null)),
                        "aMismatch",
                        map("r", map("a", "x", "b", "y")),
                        "bMismatch",
                        map("r", map("a", 3L, "b", 7L)),
                        "cNotAMap",
                        map("r", map("a", 4L, "c", "x")),
                        "xMismatch",
                        map("r", map("c", map("x", true))));
        TableEnvironment table = tables(collection, "null");

        List<Row> projected = collect(table, String.format(QUERY, "t"));

        assertThat(projected)
                .hasSize(12)
                .containsExactlyInAnyOrderElementsOf(collect(table, String.format(QUERY, "o")))
                .contains(
                        Row.of("full", 1L, "x", 2L, "dotted", 35.6),
                        Row.of("string", null, null, null, null, null),
                        Row.of("bMismatch", 3L, null, null, null, null),
                        Row.of("cNotAMap", 4L, null, null, null, null));
    }

    @Test
    void underTheFailPolicyAProjectionReadsWhatTheWholeColumnReadsWhereNeitherMismatches()
            throws Exception {
        String collection =
                write(
                        "full",
                        map("r", map("a", 1L, "b", "x", "c", map("x", 2L))),
                        "missing",
                        map("other", 1L),
                        "noA",
                        map("r", map("b", "x")),
                        "point",
                        map("g", new GeoPoint(1.0, 2.0)));
        TableEnvironment table = tables(collection, "fail");

        assertThat(collect(table, String.format(QUERY, "t")))
                .hasSize(4)
                .containsExactlyInAnyOrderElementsOf(collect(table, String.format(QUERY, "o")))
                .contains(
                        Row.of("full", 1L, "x", 2L, null, null),
                        Row.of("noA", null, "x", null, null, null),
                        Row.of("point", null, null, null, null, 1.0));
    }

    @Test
    void underTheFailPolicyAMismatchAtTheNamedFieldStillFails() throws Exception {
        String collection = write("aMismatch", map("r", map("a", "x")));
        TableEnvironment table = tables(collection, "fail");

        assertThatThrownBy(() -> collect(table, "SELECT r.a FROM t"))
                .hasStackTraceContaining("Field 'r.a' holds a value of type");
        assertThatThrownBy(() -> collect(table, "SELECT r.a FROM o"))
                .hasStackTraceContaining("Field 'r.a' holds a value of type");
    }

    @Test
    void underTheFailPolicyWhatTheProjectionDoesNotReadIsNotChecked() throws Exception {
        String sibling = write("bMismatch", map("r", map("a", 3L, "b", 7L)));
        String notAMap = write("string", map("r", "hello"));
        TableEnvironment siblings = tables(sibling, "fail");
        TableEnvironment notMaps = tables(notAMap, "fail");

        // A sibling the query does not name, and a value that is no map on the selected path,
        // which the service does not return: the whole column fails on each document.
        assertThat(collect(siblings, "SELECT id, r.a FROM t"))
                .containsExactly(Row.of("bMismatch", 3L));
        assertThatThrownBy(() -> collect(siblings, "SELECT id, r.a FROM o"))
                .hasStackTraceContaining("Field 'r.b' holds a value of type");
        assertThat(collect(notMaps, "SELECT id, r.a FROM t"))
                .containsExactly(Row.of("string", null));
        assertThatThrownBy(() -> collect(notMaps, "SELECT id, r.a FROM o"))
                .hasStackTraceContaining("Field 'r' holds a value of type");
    }

    @Test
    void aCollectionGroupScanReadsTheSamePaths() throws Exception {
        String collection =
                write(
                        "full",
                        map("r", map("a", 1L, "b", "x", "c", map("x", 2L))),
                        "string",
                        map("r", "hello"));
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(
                "CREATE TABLE grp (r ROW<a BIGINT, b STRING, c ROW<x BIGINT>>,"
                        + " path STRING METADATA FROM 'document-path' VIRTUAL) WITH ("
                        + "'connector' = 'firestore', 'project' = '"
                        + PROJECT
                        + "', 'collection' = '"
                        + collection
                        + "', 'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "', 'scan.collection-group' = 'true',"
                        + " 'scan.partition.max-partitions' = '1')");

        assertThat(collect(table, "SELECT path, r.c.x FROM grp"))
                .containsExactlyInAnyOrder(
                        Row.of(collection + "/full", 2L), Row.of(collection + "/string", null));
    }
}
