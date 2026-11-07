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

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A filtered scan returns the rows of the unfiltered scan followed by Flink's filter ({@code
 * docs/adr/0197}), against the emulator.
 *
 * <p>The oracle is the same collection read as a collection group, which pushes nothing: its id is
 * unique to the class, so the group holds exactly the collection's documents. The documents cover
 * what a pushed equality must not drop and what it may admit for Flink to remove: a missing field,
 * a stored null, a value of another type, an integer under {@code DOUBLE}, a double under {@code
 * BIGINT}, {@code -0.0}, a fractional double, and a near miss of a string and of a timestamp; a
 * {@code NaN} sits beside them as a value no numeric equality may admit into Flink's result. A
 * fixed-length column holds a shorter, a space-padded and a longer value, and a {@code ROW} column
 * a map with a double, an array, a non-map value, a list of maps and a top-level field whose name
 * is the path. The document id has no oracle column, so its equalities are compared with a filter
 * on the document path, beside documents whose ids are near misses of it.
 */
@Testcontainers
class FirestoreTableFilterPushDownITCase extends AbstractFirestoreEmulatorITCase {

    private static final String COLUMNS =
            "s STRING, n BIGINT, d DOUBLE, b BOOLEAN, y BYTES, ts TIMESTAMP_LTZ(6), c CHAR(3),"
                    + " bin BINARY(2), e ROW<x BIGINT, flag BOOLEAN>,"
                    + " path STRING METADATA FROM 'document-path' VIRTUAL";

    private static final Timestamp TIME = Timestamp.ofTimeSecondsAndNanos(1_000, 123_456_000);

    private static String collection;

    @BeforeAll
    static void writeDocuments() throws Exception {
        collection = uniqueCollection();
        Map<String, Object> nulls = new HashMap<>();
        for (String field : List.of("s", "n", "d", "b", "y", "ts", "c", "bin", "e")) {
            nulls.put(field, null);
        }
        write("missing", Map.of());
        write("nulls", nulls);
        write(
                "match",
                Map.of(
                        "s",
                        "x",
                        "n",
                        5L,
                        "d",
                        5.0d,
                        "b",
                        true,
                        "y",
                        Blob.fromBytes(new byte[] {1, 2}),
                        "ts",
                        TIME,
                        "c",
                        "abc",
                        "bin",
                        Blob.fromBytes(new byte[] {1, 2}),
                        "e",
                        Map.of("x", 1L, "flag", true)));
        // A double under BIGINT is a mismatch; an exact integer under DOUBLE widens.
        write("numbers", Map.of("n", 5.0d, "d", 5L, "b", false));
        // A value of another type under every column.
        write(
                "others",
                Map.of(
                        "s", 5L,
                        "n", "5",
                        "d", "5",
                        "b", "true",
                        "y", "\u0001\u0002",
                        "ts", 1_000L));
        write("zero", Map.of("d", -0.0d, "n", 0L));
        write("nan", Map.of("d", Double.NaN));
        write("fraction", Map.of("d", 5.5d));
        write("near", Map.of("s", "x ", "ts", Timestamp.ofTimeSecondsAndNanos(1_000, 123_457_000)));
        // A fixed-length column reads a stored value unpadded and untruncated; a double under
        // the nested BIGINT is a mismatch the service matches to 1.
        write(
                "short",
                Map.of(
                        "c",
                        "ab",
                        "bin",
                        Blob.fromBytes(new byte[] {1}),
                        "e",
                        Map.of("x", 1.0d, "flag", false)));
        write("padded", Map.of("c", "ab ", "e", Map.of("x", List.of(1L))));
        write("long", Map.of("c", "abcd", "bin", Blob.fromBytes(new byte[] {1, 2, 3})));
        write("notmap", Map.of("e", "x"));
        write("maps", Map.of("e", List.of(Map.of("x", 1L, "flag", true))));
        write("dotted", Map.of("e.x", 1L, "e.flag", true));
        // Near misses of the id the id tests filter on.
        for (String id : List.of("matc", "match ", "MATCH")) {
            write(id, Map.of("s", "y"));
        }
    }

    private static void write(String id, Map<String, Object> fields) throws Exception {
        client().document(collection + "/" + id).set(fields).get();
    }

    private static TableEnvironment tables(String policy) {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        // The timestamp literals below are UTC wall-clock times.
        table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
        String common =
                "'connector' = 'firestore', 'project' = '"
                        + PROJECT
                        + "', 'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "', 'collection' = '"
                        + collection
                        + "', 'type-mismatch-policy' = '"
                        + policy
                        + "'";
        table.executeSql("CREATE TABLE pushed (" + COLUMNS + ") WITH (" + common + ")");
        table.executeSql(
                "CREATE TABLE keyed (id STRING, "
                        + COLUMNS
                        + ", PRIMARY KEY (id) NOT ENFORCED) WITH ("
                        + common
                        + ")");
        table.executeSql(
                "CREATE TABLE unpushed ("
                        + COLUMNS
                        + ") WITH ("
                        + common
                        + ", 'scan.collection-group' = 'true',"
                        // The emulator implements no PartitionQuery; one partition needs none.
                        + " 'scan.partition.max-partitions' = '1')");
        return table;
    }

    private static List<Row> collect(TableEnvironment table, String query) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(query).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static List<Row> ids(TableEnvironment table, String from, String predicate)
            throws Exception {
        return collect(table, "SELECT path FROM " + from + " WHERE " + predicate);
    }

    private static Row id(String id) {
        return Row.of(collection + "/" + id);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "s = 'x'",
                "n = 5",
                "n = 0",
                "d = 5",
                "d = 0",
                "d = 5.5",
                "b",
                "NOT b",
                "y = x'0102'",
                "ts = CAST(TIMESTAMP '1970-01-01 00:16:40.123456' AS TIMESTAMP_LTZ(6))",
                "s = 'x' AND n = 5 AND b",
                "s = 'x' AND n > 4",
                "c = 'abc'",
                "c = 'ab '",
                "bin = x'0102'",
                "e.x = 1",
                "e.flag",
                "NOT e.flag",
                "e.x = 1 AND e.flag"
            })
    void aPushedEqualityReturnsTheRowsFlinksFilterKeeps(String predicate) throws Exception {
        TableEnvironment table = tables("null");

        assertThat(table.explainSql("SELECT path FROM pushed WHERE " + predicate))
                .containsPattern("filter=\\[[^\\]]");
        assertThat(ids(table, "pushed", predicate))
                .containsExactlyInAnyOrderElementsOf(ids(table, "unpushed", predicate));
    }

    @Test
    void theOracleSeesWhatTheEqualitiesShouldAndShouldNotMatch() throws Exception {
        // Firing control for the comparison above: these are the rows the oracle keeps, so an
        // equality the service matched more narrowly than Flink would show as a missing id.
        TableEnvironment table = tables("null");

        assertThat(ids(table, "unpushed", "n = 5")).containsExactly(id("match"));
        assertThat(ids(table, "unpushed", "d = 5"))
                .containsExactlyInAnyOrder(id("match"), id("numbers"));
        assertThat(ids(table, "unpushed", "d = 0")).containsExactly(id("zero"));
        assertThat(ids(table, "unpushed", "d = 5.5")).containsExactly(id("fraction"));
        assertThat(ids(table, "unpushed", "n = 0")).containsExactly(id("zero"));
        assertThat(ids(table, "unpushed", "b")).containsExactly(id("match"));
        assertThat(ids(table, "unpushed", "y = x'0102'")).containsExactly(id("match"));
        assertThat(ids(table, "unpushed", "s = 'x' AND n = 5 AND b")).containsExactly(id("match"));
        assertThat(ids(table, "unpushed", "s = 'x' AND n > 4")).containsExactly(id("match"));
        assertThat(ids(table, "unpushed", "NOT b")).containsExactly(id("numbers"));
        assertThat(ids(table, "unpushed", "s = 'x'")).containsExactly(id("match"));
        assertThat(
                        ids(
                                table,
                                "unpushed",
                                "ts = CAST(TIMESTAMP '1970-01-01 00:16:40.123456' AS TIMESTAMP_LTZ(6))"))
                .containsExactly(id("match"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"match", "matc", "match ", "MATCH", "absent"})
    void aPushedDocumentIdEqualityReturnsThatDocumentOnly(String id) throws Exception {
        TableEnvironment table = tables("null");
        String predicate = "id = '" + id + "'";

        assertThat(table.explainSql("SELECT path FROM keyed WHERE " + predicate))
                .containsPattern("filter=\\[[^\\]]");
        assertThat(ids(table, "keyed", predicate + " AND s = 'x'"))
                .containsExactlyInAnyOrderElementsOf(
                        ids(
                                table,
                                "unpushed",
                                "path = '" + collection + "/" + id + "' AND s = 'x'"));
        assertThat(ids(table, "keyed", predicate))
                .containsExactlyInAnyOrderElementsOf(
                        ids(table, "unpushed", "path = '" + collection + "/" + id + "'"));
    }

    @Test
    void theOracleSeesWhatTheNewShapesShouldAndShouldNotMatch() throws Exception {
        // Firing control for the fixed-length, nested and id comparisons above.
        TableEnvironment table = tables("null");

        assertThat(ids(table, "unpushed", "c = 'abc'")).containsExactly(id("match"));
        assertThat(ids(table, "unpushed", "c = 'ab '")).containsExactly(id("padded"));
        assertThat(ids(table, "unpushed", "bin = x'0102'")).containsExactly(id("match"));
        assertThat(ids(table, "unpushed", "e.x = 1")).containsExactly(id("match"));
        assertThat(ids(table, "unpushed", "e.flag")).containsExactly(id("match"));
        assertThat(ids(table, "unpushed", "NOT e.flag")).containsExactly(id("short"));
        assertThat(ids(table, "unpushed", "e.x = 1 AND e.flag")).containsExactly(id("match"));
        assertThat(ids(table, "unpushed", "path = '" + collection + "/match '"))
                .containsExactly(id("match "));
        assertThat(ids(table, "unpushed", "path = '" + collection + "/match'"))
                .containsExactly(id("match"));
    }

    @Test
    void aDocumentTheFilterExcludesIsNotReadSoItsMismatchCannotFailTheScan() throws Exception {
        // The documented consequence of pushing under 'fail': type checking covers the documents
        // read. Unfiltered, the string under BIGINT fails the scan; filtered, it is never read.
        TableEnvironment table = tables("fail");

        assertThat(ids(table, "pushed", "s = 'x'")).containsExactly(id("match"));
        assertThatThrownBy(() -> ids(table, "unpushed", "s = 'x'"))
                .hasStackTraceContaining("holds a value of type");
    }
}
