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
import com.google.cloud.firestore.GeoPoint;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Scans documents the stock client wrote, through SQL against the emulator. */
@Testcontainers
class FirestoreTableSourceITCase extends AbstractFirestoreEmulatorITCase {

    private static TableEnvironment batch() {
        return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    }

    private static String with(String collection, String... extra) {
        StringBuilder options =
                new StringBuilder(
                        "'connector' = 'firestore', 'project' = '"
                                + PROJECT
                                + "', 'collection' = '"
                                + collection
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
    void everyTypeIsReadBackAsItsColumn() throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/o1")
                .set(
                        Map.ofEntries(
                                Map.entry("s", "text"),
                                Map.entry("n", 42L),
                                Map.entry("d", 7L),
                                Map.entry("f", 1.5d),
                                Map.entry("b", true),
                                Map.entry("attrs", Map.of("k", 3L)),
                                Map.entry("payload", Blob.fromBytes(new byte[] {1, 2})),
                                Map.entry(
                                        "ts",
                                        Timestamp.ofTimeSecondsAndNanos(
                                                1_700_000_000L, 123_456_000)),
                                Map.entry("tags", List.of("a", "b")),
                                Map.entry(
                                        "nested",
                                        Map.of("x", 1L, "where", new GeoPoint(35.6, 139.7))),
                                Map.entry("author", client().document("users/bob"))))
                .get();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE source (id STRING NOT NULL, s STRING, n BIGINT, d DOUBLE, f DOUBLE,"
                        + " b BOOLEAN, attrs MAP<STRING, BIGINT>,"
                        + " payload BYTES, ts TIMESTAMP_LTZ(6), tags ARRAY<STRING>,"
                        + " nested ROW<x BIGINT, `where` ROW<latitude DOUBLE, longitude DOUBLE>>,"
                        + " author STRING, missing STRING,"
                        + " path STRING METADATA FROM 'document-path' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(
                                collection,
                                "'geo-point-field-paths' = 'nested.where'",
                                "'reference-field-paths' = 'author'"));

        List<Row> rows = collect(table, "SELECT * FROM source");

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        assertThat(row.getField("id")).isEqualTo("o1");
        assertThat(row.getField("s")).isEqualTo("text");
        assertThat(row.getField("n")).isEqualTo(42L);
        assertThat(row.getField("d")).as("an integer a DOUBLE holds exactly").isEqualTo(7.0d);
        assertThat(row.getField("f")).isEqualTo(1.5d);
        assertThat(row.getField("b")).isEqualTo(true);
        assertThat(row.getField("attrs")).isEqualTo(Map.of("k", 3L));
        assertThat((byte[]) row.getField("payload")).containsExactly(1, 2);
        assertThat(row.getField("ts"))
                .isEqualTo(Instant.ofEpochSecond(1_700_000_000L, 123_456_000));
        assertThat((String[]) row.getField("tags")).containsExactly("a", "b");
        assertThat(row.getField("nested")).isEqualTo(Row.of(1L, Row.of(35.6, 139.7)));
        assertThat(row.getField("author")).isEqualTo("users/bob");
        assertThat(row.getField("missing")).isNull();
        assertThat(row.getField("path")).isEqualTo(collection + "/o1");
    }

    @Test
    void aProjectedScanWithMetadataReadsItsColumnsAndTheDocumentsTimes() throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/a").set(Map.of("v", 1L, "other", "x")).get();
        client().document(collection + "/b").set(Map.of("v", 2L, "other", "y")).get();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE source (id STRING NOT NULL, v BIGINT, other STRING,"
                        + " created TIMESTAMP_LTZ(6) METADATA FROM 'create-time' VIRTUAL,"
                        + " readAt TIMESTAMP_LTZ(6) METADATA FROM 'read-time' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection));

        List<Row> rows = collect(table, "SELECT id, v, created <= readAt FROM source ORDER BY id");

        assertThat(rows).containsExactly(Row.of("a", 1L, true), Row.of("b", 2L, true));
    }

    @Test
    void aCollectionGroupScanReadsEveryCollectionWithTheId() throws Exception {
        String group = uniqueCollection();
        client().document(group + "/top").set(Map.of("v", 1L)).get();
        client().document("parents/p1/" + group + "/child").set(Map.of("v", 2L)).get();
        TableEnvironment table = batch();
        table.executeSql(
                // No PRIMARY KEY: ids repeat across the group's collections.
                "CREATE TABLE source (v BIGINT,"
                        + " path STRING METADATA FROM 'document-path' VIRTUAL) "
                        + with(
                                group,
                                "'scan.collection-group' = 'true'",
                                // The emulator implements no PartitionQuery; one partition
                                // needs none.
                                "'scan.partition.max-partitions' = '1'"));

        assertThat(collect(table, "SELECT path, v FROM source"))
                .containsExactlyInAnyOrder(
                        Row.of(group + "/top", 1L), Row.of("parents/p1/" + group + "/child", 2L));
    }

    @Test
    void aCollectionGroupScanReadsAFieldWhoseNameContainsADot() throws Exception {
        // The field mask travels in the library's encoded form; read back as a dot-separated
        // path, `a.b` would select field b inside map a and read nothing.
        String group = uniqueCollection();
        client().document(group + "/d")
                .set(Map.of("a.b", "literal", "a", Map.of("b", "nested")))
                .get();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE source (`a.b` STRING) "
                        + with(
                                group,
                                "'scan.collection-group' = 'true'",
                                "'scan.partition.max-partitions' = '1'"));

        assertThat(collect(table, "SELECT `a.b` FROM source")).containsExactly(Row.of("literal"));
    }

    @Test
    void aMismatchedValueFailsTheReadOrReadsAsNullUnderThePolicy() throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/bad").set(Map.of("n", "not a number")).get();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE strict (id STRING NOT NULL, n BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection));
        table.executeSql(
                "CREATE TABLE lenient (id STRING NOT NULL, n BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection, "'type-mismatch-policy' = 'null'"));

        assertThatThrownBy(() -> collect(table, "SELECT * FROM strict"))
                .hasStackTraceContaining("Field 'n' holds a value of type java.lang.String");
        assertThat(collect(table, "SELECT * FROM lenient")).containsExactly(Row.of("bad", null));
    }

    @Test
    void whatTheSinkWritesTheScanReadsBack() throws Exception {
        String collection = uniqueCollection();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE t (id STRING NOT NULL, location ROW<latitude DOUBLE, longitude"
                        + " DOUBLE>, owner STRING, tags ARRAY<STRING>, PRIMARY KEY (id) NOT"
                        + " ENFORCED) "
                        + with(
                                collection,
                                "'geo-point-field-paths' = 'location'",
                                "'reference-field-paths' = 'owner'"));

        table.executeSql("INSERT INTO t VALUES ('k', ROW(1.5, 2.5), 'users/a', ARRAY['x'])")
                .await();

        List<Row> rows = collect(table, "SELECT * FROM t");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField("location")).isEqualTo(Row.of(1.5, 2.5));
        assertThat(rows.get(0).getField("owner")).isEqualTo("users/a");
        assertThat((String[]) rows.get(0).getField("tags")).containsExactly("x");
    }
}
