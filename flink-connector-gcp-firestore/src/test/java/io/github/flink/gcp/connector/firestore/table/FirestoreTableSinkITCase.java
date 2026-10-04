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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.GeoPoint;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Writes through SQL into the emulator and reads the documents back with the stock client. */
@Testcontainers
class FirestoreTableSinkITCase extends AbstractFirestoreEmulatorITCase {

    private static TableEnvironment streaming() {
        return TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
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

    @Test
    void everyTypeIsStoredAsItsFirestoreValue() throws Exception {
        String collection = uniqueCollection();
        TableEnvironment table = streaming();
        table.executeSql(
                "CREATE TABLE target ("
                        + " id STRING NOT NULL,"
                        + " s STRING, b BOOLEAN, n BIGINT, d DOUBLE, payload BYTES,"
                        + " ts TIMESTAMP_LTZ(6),"
                        + " tags ARRAY<STRING>,"
                        + " attrs MAP<STRING, BIGINT>,"
                        + " nested ROW<x BIGINT, `where` ROW<latitude DOUBLE, longitude DOUBLE>>,"
                        + " location ROW<latitude DOUBLE, longitude DOUBLE>,"
                        + " author STRING,"
                        + " items ARRAY<ROW<product STRING>>,"
                        + " missing STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(
                                collection,
                                "'geo-point-field-paths' = 'location;nested.where'",
                                "'reference-field-paths' = 'author;items.product'"));

        table.executeSql(
                        "INSERT INTO target SELECT 'o1', 'text', TRUE, 42, 1.5, X'0102',"
                                + " TO_TIMESTAMP_LTZ(1700000000123, 3),"
                                + " ARRAY['a', 'b'],"
                                + " MAP['k', CAST(7 AS BIGINT)],"
                                + " ROW(CAST(1 AS BIGINT), ROW(35.6, 139.7)),"
                                + " ROW(-33.9, 151.2),"
                                + " 'users/bob',"
                                + " ARRAY[ROW('products/p1')],"
                                + " CAST(NULL AS STRING)")
                .await(60, TimeUnit.SECONDS);

        DocumentSnapshot stored = read(collection + "/o1");
        assertThat(stored.getString("s")).isEqualTo("text");
        assertThat(stored.getBoolean("b")).isTrue();
        assertThat(stored.getLong("n")).isEqualTo(42L);
        assertThat(stored.getDouble("d")).isEqualTo(1.5d);
        assertThat(stored.getBlob("payload")).isEqualTo(Blob.fromBytes(new byte[] {1, 2}));
        assertThat(stored.getTimestamp("ts"))
                .isEqualTo(Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 123_000_000));
        assertThat(stored.get("tags")).isEqualTo(List.of("a", "b"));
        assertThat(stored.get("attrs")).isEqualTo(Map.of("k", 7L));
        assertThat(stored.get("nested.x")).isEqualTo(1L);
        assertThat(stored.getGeoPoint("nested.where")).isEqualTo(new GeoPoint(35.6, 139.7));
        assertThat(stored.getGeoPoint("location")).isEqualTo(new GeoPoint(-33.9, 151.2));
        assertThat(stored.get("author")).isInstanceOf(DocumentReference.class);
        assertThat(((DocumentReference) stored.get("author")).getPath()).isEqualTo("users/bob");
        Object product = ((Map<?, ?>) ((List<?>) stored.get("items")).get(0)).get("product");
        assertThat(product).isInstanceOf(DocumentReference.class);
        assertThat(((DocumentReference) product).getPath()).isEqualTo("products/p1");
        assertThat(stored.contains("missing")).isTrue();
        assertThat(stored.get("missing")).isNull();
        assertThat(stored.contains("id")).as("the key is the document id, not a field").isFalse();
    }

    @Test
    void anUpsertChangelogSetsAndDeletesDocumentsByKey() throws Exception {
        // One change per key: the sink does not keep the order of two writes to one document
        // (ADR-0171, #1556), so a test that relied on it would pass or fail by timing.
        String collection = uniqueCollection();
        client().document(collection + "/updated").set(Map.of("name", "old")).get();
        client().document(collection + "/gone").set(Map.of("name", "old")).get();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        Table changes =
                table.fromChangelogStream(
                        env.fromData(
                                Row.ofKind(RowKind.INSERT, "inserted", "first"),
                                Row.ofKind(RowKind.UPDATE_AFTER, "updated", "second"),
                                Row.ofKind(RowKind.DELETE, "gone", "old")),
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING().notNull())
                                .column("f1", DataTypes.STRING())
                                .primaryKey("f0")
                                .build());
        table.executeSql(
                "CREATE TABLE target (id STRING NOT NULL, name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection));

        changes.executeInsert("target").await(60, TimeUnit.SECONDS);

        assertThat(read(collection + "/inserted").getData()).isEqualTo(Map.of("name", "first"));
        assertThat(read(collection + "/updated").getData()).isEqualTo(Map.of("name", "second"));
        assertThat(read(collection + "/gone").exists()).isFalse();
    }

    @Test
    void anUpdateTableRefusesAChangelogWithDeletesWhenPlanned() {
        String collection = uniqueCollection();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        Table changes =
                table.fromChangelogStream(
                        env.fromData(Row.ofKind(RowKind.DELETE, "gone", "old")),
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING().notNull())
                                .column("f1", DataTypes.STRING())
                                .primaryKey("f0")
                                .build());
        table.executeSql(
                "CREATE TABLE target (id STRING NOT NULL, name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection, "'sink.write-mode' = 'update'"));

        assertThatThrownBy(() -> changes.executeInsert("target"))
                .hasMessageContaining("doesn't support consuming")
                .hasMessageContaining("delete");
    }

    @Test
    void anUpdateTableFailsOnTheDeleteAnUpsertMaterializationProducesAndDeletesNothing()
            throws Exception {
        // The query's key (f0) differs from the table's, so the planner materializes the upsert
        // and turns the old count's retraction into a DELETE, although the sink declares none.
        String collection = uniqueCollection();
        client().document(collection + "/1").set(Map.of("f0", "x")).get();
        client().document(collection + "/2").set(Map.of("f0", "x")).get();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        table.createTemporaryView(
                "events",
                table.fromDataStream(
                        env.fromData("x", "x", "x").map(v -> v).returns(String.class)));
        table.executeSql(
                "CREATE TABLE target (id STRING NOT NULL, category STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection, "'sink.write-mode' = 'update'"));

        assertThatThrownBy(
                        () ->
                                table.executeSql(
                                                "INSERT INTO target SELECT CAST(COUNT(*) AS"
                                                        + " STRING), f0 FROM events GROUP BY f0")
                                        .await(60, TimeUnit.SECONDS))
                .hasStackTraceContaining(
                        "whose sink.write-mode is 'update', which takes no deletes");
        assertThat(read(collection + "/1").exists()).isTrue();
        assertThat(read(collection + "/2").exists()).isTrue();
    }

    @Test
    void mergeKeepsUndeclaredFieldsAndSetReplacesThem() throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/m")
                .set(Map.of("other", 1L, "attrs", Map.of("a", 1L)))
                .get();
        client().document(collection + "/s").set(Map.of("other", 1L)).get();
        TableEnvironment table = streaming();
        table.executeSql(
                "CREATE TABLE merged (id STRING NOT NULL, attrs MAP<STRING, BIGINT>,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection, "'sink.write-mode' = 'merge'"));
        table.executeSql(
                "CREATE TABLE replaced (id STRING NOT NULL, attrs MAP<STRING, BIGINT>,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection));

        table.executeSql("INSERT INTO merged SELECT 'm', MAP['b', CAST(2 AS BIGINT)]")
                .await(60, TimeUnit.SECONDS);
        table.executeSql("INSERT INTO replaced SELECT 's', MAP['b', CAST(2 AS BIGINT)]")
                .await(60, TimeUnit.SECONDS);

        assertThat(read(collection + "/m").getData())
                .isEqualTo(Map.of("other", 1L, "attrs", Map.of("a", 1L, "b", 2L)));
        assertThat(read(collection + "/s").getData()).isEqualTo(Map.of("attrs", Map.of("b", 2L)));
    }

    @Test
    void aTableWithoutAKeyCreatesADocumentPerRow() throws Exception {
        String collection = uniqueCollection();
        TableEnvironment table = streaming();
        table.executeSql("CREATE TABLE events (kind STRING, n BIGINT) " + with(collection));

        table.executeSql("INSERT INTO events VALUES ('a', 1), ('a', 1), ('b', 2)")
                .await(60, TimeUnit.SECONDS);

        List<QueryDocumentSnapshot> documents =
                client().collection(collection).get().get(30, TimeUnit.SECONDS).getDocuments();
        assertThat(documents).hasSize(3);
        assertThat(documents)
                .extracting(DocumentSnapshot::getId)
                .allSatisfy(id -> assertThat(id).matches("[A-Za-z0-9]{20}"));
        assertThat(documents)
                .extracting(DocumentSnapshot::getData)
                .containsExactlyInAnyOrder(
                        Map.of("kind", "a", "n", 1L),
                        Map.of("kind", "a", "n", 1L),
                        Map.of("kind", "b", "n", 2L));
    }
}
