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

import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Int32Value;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.firestore.admin.v1.Database;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreRealGcpITCase;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code firestore} scan's pushed equalities against the service ({@code docs/adr/0197}): what
 * the emulator cannot show. A Standard-edition database answers two equalities on one collection
 * without a composite index, and so is an equality on the document id or on a map's field beside
 * one, which matches the map's field and not a top-level field named by the whole path; it would
 * refuse the same filter on a collection group, which is why that scan pushes none. An
 * Enterprise-edition database matches its 32-bit integers by the equality a {@code BIGINT} column
 * pushes, and refuses to store a BSON binary of subtype 0, so every value a {@code BYTES} column
 * reads is native bytes, which its pushed equality matches: no row the converter reads is filtered
 * out by the service.
 */
@EnabledIfEnvironmentVariable(named = "FIRESTORE_IT_PROJECT", matches = ".+")
@Tag("gated")
class FirestoreTableFilterPushDownRealGcpITCase extends AbstractFirestoreRealGcpITCase {

    private static TableEnvironment batch() {
        return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    }

    private static String with(DatabaseDestination database, String collection, String extra) {
        return "WITH ('connector' = 'firestore', 'project' = '"
                + PROJECT
                + "', 'database' = '"
                + database.getDatabaseId()
                + "', 'collection' = '"
                + collection
                + "', 'type-mismatch-policy' = 'null'"
                + extra
                + ")";
    }

    private static List<Row> collect(TableEnvironment table, String query) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(query).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    @Test
    void aStandardEditionCollectionAnswersTwoEqualitiesAndAGroupScanIsLeftUnfiltered()
            throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/a")
                .set(Map.of("s", "x", "n", 5L))
                .get(30, TimeUnit.SECONDS);
        // The service matches 5.0 to 5; the converter reads a double under BIGINT as NULL, and
        // Flink's residual removes the row.
        client().document(collection + "/b")
                .set(Map.of("s", "x", "n", 5.0d))
                .get(30, TimeUnit.SECONDS);
        client().document(collection + "/c")
                .set(Map.of("s", "y", "n", 5L))
                .get(30, TimeUnit.SECONDS);
        client().document(collection + "/d").set(Map.of("s", "x")).get(30, TimeUnit.SECONDS);
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE keyed (id STRING NOT NULL, s STRING, n BIGINT,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(database(), collection, ""));
        table.executeSql(
                "CREATE TABLE grouped (s STRING, n BIGINT,"
                        + " path STRING METADATA FROM 'document-path' VIRTUAL) "
                        + with(database(), collection, ", 'scan.collection-group' = 'true'"));
        String predicate = " WHERE s = 'x' AND n = 5";

        assertThat(table.explainSql("SELECT id FROM keyed" + predicate))
                .containsPattern("filter=\\[[^\\]]");
        assertThat(collect(table, "SELECT id FROM keyed" + predicate)).containsExactly(Row.of("a"));
        // Pushed, this filter would need a collection-group index the service does not create,
        // and the read would fail with FAILED_PRECONDITION.
        assertThat(collect(table, "SELECT path FROM grouped" + predicate))
                .containsExactly(Row.of(collection + "/a"));
    }

    @Test
    void aStandardEditionCollectionAnswersAnIdAndAMapFieldEquality() throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/a")
                .set(Map.of("s", "x", "e", Map.of("x", 1L)))
                .get(30, TimeUnit.SECONDS);
        client().document(collection + "/b")
                .set(Map.of("s", "x", "e", Map.of("x", 1L)))
                .get(30, TimeUnit.SECONDS);
        // A field named by the whole path is not the map's field.
        client().document(collection + "/c")
                .set(Map.of("s", "x", "e.x", 1L))
                .get(30, TimeUnit.SECONDS);
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE keyed (id STRING NOT NULL, s STRING, e ROW<x BIGINT>,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(database(), collection, ""));

        for (String predicate : new String[] {"id = 'b'", "e.x = 1 AND s = 'x'"}) {
            assertThat(table.explainSql("SELECT id FROM keyed WHERE " + predicate))
                    .containsPattern("filter=\\[[^\\]]");
        }
        assertThat(collect(table, "SELECT id FROM keyed WHERE id = 'b' AND s = 'x'"))
                .containsExactly(Row.of("b"));
        assertThat(collect(table, "SELECT id FROM keyed WHERE e.x = 1 AND s = 'x'"))
                .containsExactlyInAnyOrder(Row.of("a"), Row.of("b"));
        // Flink's residual would remove c either way, so the path's matching is pinned on the
        // query the scan sends.
        List<String> ids = new ArrayList<>();
        for (QueryDocumentSnapshot document :
                client().collection(collection)
                        .whereEqualTo(FieldPath.of("e", "x"), 1L)
                        .get()
                        .get(30, TimeUnit.SECONDS)
                        .getDocuments()) {
            ids.add(document.getId());
        }
        assertThat(ids).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void anEnterpriseEditionDatabaseMatchesItsBsonIntegersAndStoresNoSubtypeZeroBinary()
            throws Exception {
        DatabaseDestination enterprise =
                createDatabase(
                        Database.DatabaseType.FIRESTORE_NATIVE,
                        Database.DatabaseEdition.ENTERPRISE);
        String collection = uniqueCollection();
        try (Firestore client = open(enterprise)) {
            client.document(collection + "/int32")
                    .set(Map.of("n", new Int32Value(5)))
                    .get(30, TimeUnit.SECONDS);
            client.document(collection + "/bytes")
                    .set(Map.of("y", Blob.fromBytes(new byte[] {1, 2})))
                    .get(30, TimeUnit.SECONDS);
            // The library sends a subtype-0 Blob as native bytes, so the reserved form is built by
            // hand. Were it stored, the converter would read it as BYTES and the pushed equality
            // on native bytes would have to be measured against it.
            Value subtypeZero =
                    Value.newBuilder()
                            .setMapValue(
                                    MapValue.newBuilder()
                                            .putFields(
                                                    "__binary__",
                                                    Value.newBuilder()
                                                            .setBytesValue(
                                                                    ByteString.copyFrom(
                                                                            new byte[] {0, 1, 2}))
                                                            .build()))
                            .build();
            assertThatThrownBy(
                            () ->
                                    client.document(collection + "/bson")
                                            .set(Map.of("y", subtypeZero))
                                            .get(30, TimeUnit.SECONDS))
                    .hasStackTraceContaining("subtype 0 is not permitted");
        }
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE t (id STRING NOT NULL, n BIGINT, y BYTES,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(enterprise, collection, ""));

        for (String predicate : new String[] {"n = 5", "y = x'0102'"}) {
            // Pushed, or the residual alone would produce the rows and the match go unguarded.
            assertThat(table.explainSql("SELECT id FROM t WHERE " + predicate))
                    .containsPattern("filter=\\[[^\\]]");
        }
        assertThat(collect(table, "SELECT id FROM t WHERE n = 5")).containsExactly(Row.of("int32"));
        assertThat(collect(table, "SELECT id FROM t WHERE y = x'0102'"))
                .containsExactly(Row.of("bytes"));
    }

    /** The predicates both editions must answer as the unpushed scan of the same collection. */
    private static final String[] COMPARISONS = {
        "n > 4",
        "n <= 5",
        "n <> 5",
        "n IN (5, 7)",
        "n IS NOT NULL",
        "d > 5",
        "d < 5.5",
        "d <> 5",
        "d IS NOT NULL",
        "s > 'z'",
        "s < 'x '",
        "s IS NOT NULL",
        "y > x'7f'",
        "ts3 = CAST(TIMESTAMP '1970-01-01 00:16:40.123' AS TIMESTAMP_LTZ(3))",
        "ts3 <= CAST(TIMESTAMP '1970-01-01 00:16:40.123' AS TIMESTAMP_LTZ(3))",
        "id > 'm'"
    };

    /**
     * On both editions a pushed range, {@code <>}, {@code IN} and {@code IS NOT NULL} return the
     * rows the same scan returns with {@code scan.filter-push-down.enabled} off, on Standard read
     * in pages of two so that every query resumes at a cursor (an Enterprise read of several pages
     * skips documents, #2088): what the emulator answered of integers and doubles comparing as
     * numbers, UTF-8 string and unsigned byte order, a range matching no value of another type, and
     * the explicit ordering a {@code != null} filter needs. On Enterprise a 32-bit BSON integer
     * passes the ranges a {@code BIGINT} column pushes, and {@code !=} also returns a missing
     * field, which the residual removes. On Standard, a range beside an equality on another field
     * needs a composite index, and the job fails when it starts.
     */
    @Test
    void pushedComparisonsReturnTheUnpushedRowsOnBothEditionsAndAnIndexIsRefusedLoudly()
            throws Exception {
        DatabaseDestination enterprise =
                createDatabase(
                        Database.DatabaseType.FIRESTORE_NATIVE,
                        Database.DatabaseEdition.ENTERPRISE);
        String collection = uniqueCollection();
        try (Firestore client = open(enterprise)) {
            write(client(), collection, false);
            write(client, collection, true);
        }
        TableEnvironment table = batch();
        String columns =
                "(id STRING NOT NULL, n BIGINT, d DOUBLE, s STRING, y BYTES, ts3 TIMESTAMP_LTZ(3),"
                        + " PRIMARY KEY (id) NOT ENFORCED) ";
        for (DatabaseDestination database : List.of(database(), enterprise)) {
            String edition = database == enterprise ? "ent" : "std";
            // An Enterprise-edition database returns a query without orderBy in no name order,
            // so a read of several pages skips documents there (#2088); one page reads them all.
            String paged = database == enterprise ? "" : ", 'scan.max-rows-per-fetch' = '2'";
            table.executeSql(
                    "CREATE TABLE pushed_" + edition + columns + with(database, collection, paged));
            table.executeSql(
                    "CREATE TABLE unpushed_"
                            + edition
                            + columns
                            + with(
                                    database,
                                    collection,
                                    paged + ", 'scan.filter-push-down.enabled' = 'false'"));
            for (String predicate : COMPARISONS) {
                assertThat(
                                table.explainSql(
                                        "SELECT id FROM pushed_" + edition + " WHERE " + predicate))
                        .containsPattern("filter=\\[[^\\]]");
                assertThat(
                                collect(
                                        table,
                                        "SELECT id FROM pushed_" + edition + " WHERE " + predicate))
                        .as("%s on %s", predicate, edition)
                        .containsExactlyInAnyOrderElementsOf(
                                collect(
                                        table,
                                        "SELECT id FROM unpushed_"
                                                + edition
                                                + " WHERE "
                                                + predicate));
            }
        }
        // Firing control: the rows a narrower service match would miss.
        assertThat(collect(table, "SELECT id FROM unpushed_ent WHERE n > 4"))
                .containsExactlyInAnyOrder(Row.of("int32"), Row.of("high"), Row.of("match"));
        assertThat(collect(table, "SELECT id FROM unpushed_std WHERE d > 5"))
                .containsExactlyInAnyOrder(Row.of("low"), Row.of("fraction"));
        assertThat(collect(table, "SELECT id FROM unpushed_std WHERE d <> 5"))
                .contains(Row.of("notANumber"));
        assertThat(collect(table, "SELECT id FROM unpushed_std WHERE s > 'z'"))
                .containsExactlyInAnyOrder(Row.of("low"), Row.of("high"));

        assertThatThrownBy(() -> collect(table, "SELECT id FROM pushed_std WHERE n = 5 AND d > 1"))
                .hasStackTraceContaining("FAILED_PRECONDITION")
                .hasStackTraceContaining("requires an index");
    }

    private static void write(Firestore client, String collection, boolean enterprise)
            throws Exception {
        Map<String, Map<String, Object>> documents = new java.util.LinkedHashMap<>();
        documents.put("missing", Map.of());
        documents.put(
                "low",
                Map.of(
                        "n",
                        3L,
                        "d",
                        7L,
                        "s",
                        "\uFFFD",
                        "y",
                        Blob.fromBytes(new byte[] {0x7f}),
                        "ts3",
                        com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1_000, 123_456_000)));
        documents.put(
                "high",
                Map.of(
                        "n",
                        7L,
                        "d",
                        -1.5d,
                        "s",
                        "\uD83D\uDE00",
                        "y",
                        Blob.fromBytes(new byte[] {(byte) 0x80}),
                        "ts3",
                        com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1_000, 123_999_000)));
        documents.put(
                "match",
                Map.of(
                        "n",
                        5L,
                        "d",
                        5.0d,
                        "s",
                        "x",
                        "ts3",
                        com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1_000, 124_000_000)));
        documents.put("fraction", Map.of("d", 5.5d, "n", 5.5d, "s", 5L));
        // Flink keeps NaN under d <> 5.
        documents.put("notANumber", Map.of("d", Double.NaN));
        documents.put("text", Map.of("n", "7", "d", "7", "y", "\u0080"));
        for (Map.Entry<String, Map<String, Object>> document : documents.entrySet()) {
            client.document(collection + "/" + document.getKey())
                    .set(document.getValue())
                    .get(30, TimeUnit.SECONDS);
        }
        if (enterprise) {
            client.document(collection + "/int32")
                    .set(Map.of("n", new Int32Value(6)))
                    .get(30, TimeUnit.SECONDS);
        }
    }
}
