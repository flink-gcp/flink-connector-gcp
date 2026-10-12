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
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityValue;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.ListValue;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.Value;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Writes through SQL into the emulator in Datastore mode and reads the entities back. */
@Testcontainers
class DatastoreTableSinkITCase extends AbstractDatastoreEmulatorITCase {

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
                                + "', 'emulator-endpoint' = '"
                                + emulatorEndpoint()
                                + "'");
        for (String option : extra) {
            options.append(", ").append(option);
        }
        return "WITH (" + options + ")";
    }

    @Test
    void everyTypeIsStoredAsItsDatastoreValue() throws Exception {
        String kind = uniqueKind();
        TableEnvironment table = streaming();
        table.executeSql(
                "CREATE TABLE target ("
                        + " id STRING NOT NULL,"
                        + " s STRING, b BOOLEAN, n BIGINT, d DOUBLE, payload BYTES,"
                        + " ts TIMESTAMP_LTZ(6),"
                        + " tags ARRAY<STRING>,"
                        + " nested ROW<x BIGINT, child ROW<y STRING>>,"
                        + " items ARRAY<ROW<sku STRING>>,"
                        + " missing STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));

        table.executeSql(
                        "INSERT INTO target SELECT 'o1', 'text', TRUE, 42, 1.5, X'0102',"
                                + " TO_TIMESTAMP_LTZ(1700000000123, 3),"
                                + " ARRAY['a', 'b'],"
                                + " ROW(CAST(1 AS BIGINT), ROW('deep')),"
                                + " ARRAY[ROW('p1'), ROW('p2')],"
                                + " CAST(NULL AS STRING)")
                .await(60, TimeUnit.SECONDS);

        Entity stored = read(key(kind, "o1"));
        assertThat(stored).isNotNull();
        assertThat(stored.getString("s")).isEqualTo("text");
        assertThat(stored.getBoolean("b")).isTrue();
        assertThat(stored.getLong("n")).isEqualTo(42L);
        assertThat(stored.getDouble("d")).isEqualTo(1.5d);
        assertThat(stored.getBlob("payload")).isEqualTo(Blob.copyFrom(new byte[] {1, 2}));
        assertThat(stored.getTimestamp("ts"))
                .isEqualTo(Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 123_000_000));
        List<Value<?>> tags = stored.getList("tags");
        assertThat(tags).extracting(value -> (Object) value.get()).containsExactly("a", "b");
        FullEntity<?> nested = stored.getEntity("nested");
        assertThat(nested.getLong("x")).isEqualTo(1L);
        assertThat(nested.getEntity("child").getString("y")).isEqualTo("deep");
        assertThat(stored.<EntityValue>getList("items"))
                .extracting(item -> item.get().getString("sku"))
                .containsExactly("p1", "p2");
        assertThat(stored.contains("missing")).isTrue();
        assertThat(stored.isNull("missing")).isTrue();
        assertThat(stored.contains("id"))
                .as("the key is the entity's name, not a property")
                .isFalse();
        assertThat(stored.getProperties().values())
                .allSatisfy(value -> assertThat(value.excludeFromIndexes()).isFalse());
    }

    @Test
    void anUpsertChangelogUpsertsAndDeletesEntitiesByKeyInOrder() throws Exception {
        // Within one subtask the sink applies one key's writes in order, so two changes of one
        // key leave the later one. The default merge keeps a property the table does not
        // declare, and deleting a missing key succeeds.
        String kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "gone")).set("name", "old").build());
        client().put(
                        Entity.newBuilder(key(kind, "kept"))
                                .set("name", "old")
                                .set("undeclared", 1L)
                                .build());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        Table changes =
                table.fromChangelogStream(
                        env.fromData(
                                Row.ofKind(RowKind.INSERT, "kept", "first"),
                                Row.ofKind(RowKind.UPDATE_AFTER, "kept", "second"),
                                Row.ofKind(RowKind.INSERT, "removed", "first"),
                                Row.ofKind(RowKind.DELETE, "removed", "first"),
                                Row.ofKind(RowKind.DELETE, "gone", "old"),
                                Row.ofKind(RowKind.DELETE, "never", "none")),
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING().notNull())
                                .column("f1", DataTypes.STRING())
                                .primaryKey("f0")
                                .build());
        table.executeSql(
                "CREATE TABLE target (id STRING NOT NULL, name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));

        changes.executeInsert("target").await(60, TimeUnit.SECONDS);

        Entity kept = read(key(kind, "kept"));
        assertThat(kept.getString("name")).isEqualTo("second");
        assertThat(kept.getNames()).containsExactlyInAnyOrder("name", "undeclared");
        assertThat(read(key(kind, "removed"))).isNull();
        assertThat(read(key(kind, "gone"))).isNull();
        assertThat(read(key(kind, "never"))).isNull();
    }

    @Test
    void theDefaultMergeKeepsWhatTheTableDoesNotDeclareAtAnyDepth() throws Exception {
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(key(kind, "o1"))
                                .set("other", 1L)
                                .set(
                                        "nested",
                                        FullEntity.newBuilder()
                                                .set("x", 0L)
                                                .set("y", "kept")
                                                .build())
                                .set("tags", "a", "b", "c")
                                .set("cleared", "old")
                                .build());
        client().put(Entity.newBuilder(key(kind, "k1")).set("other", 1L).build());
        TableEnvironment table = streaming();
        table.executeSql(
                "CREATE TABLE target (id STRING NOT NULL, nested ROW<x BIGINT>,"
                        + " tags ARRAY<STRING>, cleared STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));
        table.executeSql(
                "CREATE TABLE keys (id STRING NOT NULL, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));

        table.executeSql(
                        "INSERT INTO target VALUES ('o1', ROW(CAST(1 AS BIGINT)), ARRAY['d'],"
                                + " CAST(NULL AS STRING))")
                .await(60, TimeUnit.SECONDS);
        table.executeSql("INSERT INTO keys VALUES ('k1'), ('k2')").await(60, TimeUnit.SECONDS);

        Entity merged = read(key(kind, "o1"));
        assertThat(merged.getLong("other")).isEqualTo(1L);
        assertThat(merged.getEntity("nested").getLong("x")).isEqualTo(1L);
        assertThat(merged.getEntity("nested").getString("y")).isEqualTo("kept");
        List<Value<?>> tags = merged.getList("tags");
        assertThat(tags).extracting(value -> (Object) value.get()).containsExactly("d");
        assertThat(merged.isNull("cleared")).as("a NULL column is written as a null").isTrue();
        // A merge of no property leaves a stored entity as it was and creates a missing one.
        assertThat(read(key(kind, "k1")).getLong("other")).isEqualTo(1L);
        assertThat(read(key(kind, "k2"))).isNotNull();
        assertThat(read(key(kind, "k2")).getNames()).isEmpty();
    }

    @Test
    void replaceRemovesWhatTheTableDoesNotDeclare() throws Exception {
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(key(kind, "o1"))
                                .set("other", 1L)
                                .set("nested", FullEntity.newBuilder().set("y", "gone").build())
                                .build());
        TableEnvironment table = streaming();
        table.executeSql(
                "CREATE TABLE target (id STRING NOT NULL, nested ROW<x BIGINT>,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, "'sink.write-mode' = 'replace'"));

        table.executeSql("INSERT INTO target VALUES ('o1', ROW(CAST(1 AS BIGINT)))")
                .await(60, TimeUnit.SECONDS);

        Entity replaced = read(key(kind, "o1"));
        assertThat(replaced.getNames()).containsExactly("nested");
        assertThat(replaced.getEntity("nested").getNames()).containsExactly("x");
    }

    @Test
    void anUpdateReplacesTheDeclaredPropertiesWholeAndKeepsTheRest() throws Exception {
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(key(kind, "o1"))
                                .set("other", 1L)
                                .set(
                                        "nested",
                                        FullEntity.newBuilder()
                                                .set("x", 0L)
                                                .set("y", "dropped")
                                                .build())
                                .set("cleared", "old")
                                .build());
        TableEnvironment table = streaming();
        table.executeSql(
                "CREATE TABLE target (id STRING NOT NULL, nested ROW<x BIGINT>, cleared STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, "'sink.write-mode' = 'update'"));

        table.executeSql(
                        "INSERT INTO target VALUES ('o1', ROW(CAST(1 AS BIGINT)),"
                                + " CAST(NULL AS STRING))")
                .await(60, TimeUnit.SECONDS);

        Entity updated = read(key(kind, "o1"));
        assertThat(updated.getNames()).containsExactlyInAnyOrder("other", "nested", "cleared");
        assertThat(updated.getLong("other")).isEqualTo(1L);
        // A ROW replaces the stored embedded entity whole, unlike a merge.
        assertThat(updated.getEntity("nested").getNames()).containsExactly("x");
        assertThat(updated.getEntity("nested").getLong("x")).isEqualTo(1L);
        assertThat(updated.isNull("cleared")).as("a NULL column is written as a null").isTrue();
    }

    @Test
    void anUpdateOfAMissingEntityFailsTheJobAndCreatesNothing() {
        String kind = uniqueKind();
        TableEnvironment table = streaming();
        table.executeSql(
                "CREATE TABLE target (id STRING NOT NULL, name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, "'sink.write-mode' = 'update'"));

        assertThatThrownBy(
                        () ->
                                table.executeSql("INSERT INTO target VALUES ('missing', 'x')")
                                        .await(60, TimeUnit.SECONDS))
                // Routed as the update's refusal, after the lookup found the database; a status the
                // operation could not earn would read "refused the UPDATE_PROPERTIES of".
                .hasStackTraceContaining("Datastore refused the UPDATE_PROPERTIES with NOT_FOUND");
        assertThat(read(key(kind, "missing"))).isNull();
    }

    @Test
    void anUpdateTableTakesAChangingQueryKeyedByItsPrimaryKey() throws Exception {
        // The query's key is the table's, so the planner sends inserts and updates-after and no
        // delete; the last count of each key is what stays.
        String kind = uniqueKind();
        for (String id : new String[] {"a", "b"}) {
            client().put(
                            Entity.newBuilder(key(kind, id))
                                    .set("hits", "0")
                                    .set("other", 1L)
                                    .build());
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        table.createTemporaryView(
                "events",
                table.fromDataStream(
                        env.fromData("a", "b", "a").map(v -> v).returns(String.class)));
        table.executeSql(
                "CREATE TABLE target (id STRING NOT NULL, hits STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, "'sink.write-mode' = 'update'"));

        table.executeSql(
                        "INSERT INTO target SELECT f0, CAST(COUNT(*) AS STRING) FROM events"
                                + " GROUP BY f0")
                .await(60, TimeUnit.SECONDS);

        assertThat(read(key(kind, "a")).getString("hits")).isEqualTo("2");
        assertThat(read(key(kind, "b")).getString("hits")).isEqualTo("1");
        assertThat(read(key(kind, "a")).getLong("other")).isEqualTo(1L);
    }

    @Test
    void anUpdateTableRefusesAChangelogWithDeletesWhenPlanned() {
        String kind = uniqueKind();
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
                        + with(kind, "'sink.write-mode' = 'update'"));

        assertThatThrownBy(() -> changes.executeInsert("target"))
                .hasMessageContaining("doesn't support consuming")
                .hasMessageContaining("delete");
    }

    @Test
    void anUpdateTableFailsOnTheDeleteAnUpsertMaterializationProducesAndDeletesNothing() {
        // The query's key (f0) differs from the table's, so the planner materializes the upsert
        // and turns the old count's retraction into a DELETE, although the sink declares none.
        String kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "1")).set("category", "x").build());
        client().put(Entity.newBuilder(key(kind, "2")).set("category", "x").build());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        // Flink 2.3 refuses this mismatched key while planning unless the statement resolves the
        // conflict; relaxing that keeps 2.2's materialization, so the delete reaches the sink.
        // Flink 1.20 and 2.2 do not define the setting and ignore it.
        table.getConfig().set("table.exec.sink.require-on-conflict", "false");
        table.createTemporaryView(
                "events",
                table.fromDataStream(
                        env.fromData("x", "x", "x").map(v -> v).returns(String.class)));
        table.executeSql(
                "CREATE TABLE target (id STRING NOT NULL, category STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, "'sink.write-mode' = 'update'"));

        assertThatThrownBy(
                        () ->
                                table.executeSql(
                                                "INSERT INTO target SELECT CAST(COUNT(*) AS"
                                                        + " STRING), f0 FROM events GROUP BY f0")
                                        .await(60, TimeUnit.SECONDS))
                .hasStackTraceContaining(
                        "whose sink.write-mode is 'update', which takes no deletes");
        assertThat(read(key(kind, "1"))).isNotNull();
        assertThat(read(key(kind, "2"))).isNotNull();
    }

    @Test
    void aMergeMeetsAStoredValueOfAnotherShape() throws Exception {
        // A NULL ROW replaces the stored entity with a null; a ROW written where the entity
        // holds a string or holds a null replaces it with an embedded entity.
        String kind = uniqueKind();
        for (String id : new String[] {"nulled", "string", "null"}) {
            Entity.Builder stored = Entity.newBuilder(key(kind, id));
            switch (id) {
                case "nulled":
                    stored.set("nested", FullEntity.newBuilder().set("y", "gone").build());
                    break;
                case "string":
                    stored.set("nested", "text");
                    break;
                default:
                    stored.setNull("nested");
            }
            client().put(stored.build());
        }
        TableEnvironment table = streaming();
        table.executeSql(
                "CREATE TABLE target (id STRING NOT NULL, nested ROW<x BIGINT>,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));

        table.executeSql(
                        "INSERT INTO target VALUES ('nulled', CAST(NULL AS ROW<x BIGINT>)),"
                                + " ('string', ROW(CAST(1 AS BIGINT))),"
                                + " ('null', ROW(CAST(2 AS BIGINT)))")
                .await(60, TimeUnit.SECONDS);

        assertThat(read(key(kind, "nulled")).isNull("nested")).isTrue();
        assertThat(read(key(kind, "string")).getEntity("nested").getNames()).containsExactly("x");
        assertThat(read(key(kind, "null")).getEntity("nested").getLong("x")).isEqualTo(2L);
    }

    @Test
    void aMergeIntoAnIndexedEntityMarksOnlyTheValuesItWrites() throws Exception {
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(key(kind, "o1"))
                                .set(
                                        "meta",
                                        FullEntity.newBuilder()
                                                .set("note", "old")
                                                .set("kept", "indexed")
                                                .build())
                                .build());
        TableEnvironment table = streaming();
        table.executeSql(
                "CREATE TABLE target (id STRING NOT NULL, meta ROW<note STRING>,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, "'sink.unindexed-columns' = 'meta'"));

        table.executeSql("INSERT INTO target VALUES ('o1', ROW(REPEAT('z', 2000)))")
                .await(60, TimeUnit.SECONDS);

        EntityValue meta = read(key(kind, "o1")).getValue("meta");
        assertThat(meta.get().getString("note")).hasSize(2000);
        assertThat(meta.get().getValue("note").excludeFromIndexes()).isTrue();
        assertThat(meta.get().getValue("kept").excludeFromIndexes()).isFalse();
        assertThat(meta.excludeFromIndexes()).as("the stored entity's own mark").isFalse();
    }

    @Test
    void aTimestampIsStoredToTheMicrosecond() throws Exception {
        // entity.proto: precise only to microseconds, finer precision rounded down. The client
        // sends all nine digits; the stored value has six.
        String kind = uniqueKind();
        TableEnvironment table = streaming();
        table.executeSql(
                "CREATE TABLE target (id STRING NOT NULL, ts TIMESTAMP_LTZ(9),"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));

        table.executeSql(
                        "INSERT INTO target SELECT 'o1', CAST(CAST('2026-01-01 00:00:00.123456789'"
                                + " AS TIMESTAMP(9)) AS TIMESTAMP_LTZ(9))")
                .await(60, TimeUnit.SECONDS);

        assertThat(read(key(kind, "o1")).getTimestamp("ts").getNanos()).isEqualTo(123_456_000);
    }

    @Test
    void aBigintKeyIsTheIdOfAnEntityInTheNamespace() throws Exception {
        String kind = uniqueKind();
        TableEnvironment table = streaming();
        table.executeSql(
                "CREATE TABLE target (id BIGINT NOT NULL, name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, "'namespace' = 'tenant-a'"));

        table.executeSql("INSERT INTO target VALUES (CAST(7 AS BIGINT), 'seven')")
                .await(60, TimeUnit.SECONDS);

        Entity stored = read(Key.newBuilder(PROJECT, kind, 7L).setNamespace("tenant-a").build());
        assertThat(stored).isNotNull();
        assertThat(stored.getString("name")).isEqualTo("seven");
        assertThat(readAll(kind)).as("nothing in the default namespace").isEmpty();
    }

    @Test
    void anUnindexedColumnIsStoredExcludedFromIndexes() throws Exception {
        String kind = uniqueKind();
        TableEnvironment table = streaming();
        table.executeSql(
                "CREATE TABLE target (id STRING NOT NULL, body STRING, tags ARRAY<STRING>,"
                        + " meta ROW<note STRING>, title STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, "'sink.unindexed-columns' = 'body;tags;meta'"));

        table.executeSql(
                        "INSERT INTO target VALUES ('o1', 'long text', ARRAY['a'], ROW('n'),"
                                + " 'short')")
                .await(60, TimeUnit.SECONDS);

        Entity stored = read(key(kind, "o1"));
        assertThat(stored.getValue("body").excludeFromIndexes()).isTrue();
        ListValue tags = stored.getValue("tags");
        assertThat(tags.get()).allSatisfy(v -> assertThat(v.excludeFromIndexes()).isTrue());
        EntityValue meta = stored.getValue("meta");
        assertThat(meta.excludeFromIndexes()).isTrue();
        assertThat(stored.getValue("title").excludeFromIndexes()).isFalse();
    }

    @Test
    void anUnindexedColumnHoldsWhatAnIndexedOneCannot() throws Exception {
        // A string over 1,500 bytes is refused in an indexed property, the emulator enforcing the
        // limit as the service documents it, at the top level, as an array element and inside an
        // embedded entity alike. Unindexed, each is stored.
        String kind = uniqueKind();
        TableEnvironment table = streaming();
        String columns =
                " (id STRING NOT NULL, body STRING, tags ARRAY<STRING>, meta ROW<note STRING>,"
                        + " PRIMARY KEY (id) NOT ENFORCED) ";
        table.executeSql(
                "CREATE TABLE unindexed"
                        + columns
                        + with(kind, "'sink.unindexed-columns' = 'body;tags;meta'"));
        table.executeSql("CREATE TABLE indexed" + columns + with(kind));
        String longBody = "('%s', REPEAT('x', 2000), ARRAY['y'], ROW('z'))";
        String longTag = "('%s', 'x', ARRAY[REPEAT('y', 2000)], ROW('z'))";
        String longNote = "('%s', 'x', ARRAY['y'], ROW(REPEAT('z', 2000)))";

        table.executeSql(
                        "INSERT INTO unindexed VALUES ('o1', REPEAT('x', 2000),"
                                + " ARRAY[REPEAT('y', 2000)], ROW(REPEAT('z', 2000)))")
                .await(60, TimeUnit.SECONDS);

        Entity stored = read(key(kind, "o1"));
        assertThat(stored.getString("body")).hasSize(2000);
        List<Value<?>> tags = stored.getList("tags");
        assertThat(tags)
                .singleElement()
                .satisfies(tag -> assertThat((String) tag.get()).hasSize(2000));
        assertThat(stored.getEntity("meta").getString("note")).hasSize(2000);
        for (String[] row : new String[][] {{"o2", longBody}, {"o3", longTag}, {"o4", longNote}}) {
            assertThatThrownBy(
                            () ->
                                    table.executeSql(
                                                    "INSERT INTO indexed VALUES "
                                                            + String.format(row[1], row[0]))
                                            .await(60, TimeUnit.SECONDS))
                    .as(row[1])
                    .hasStackTraceContaining("longer than 1500 bytes");
            assertThat(read(key(kind, row[0]))).isNull();
        }
    }

    @Test
    void aTableWithoutAKeyUpsertsAnEntityPerRowUnderAnAllocatedId() throws Exception {
        String kind = uniqueKind();
        TableEnvironment table = streaming();
        table.executeSql(
                "CREATE TABLE events (label STRING, n BIGINT) "
                        + with(kind, "'namespace' = 'tenant-a'"));

        table.executeSql("INSERT INTO events VALUES ('a', 1), ('a', 1), ('b', 2)")
                .await(60, TimeUnit.SECONDS);

        // Three rows, three entities: two identical rows got two ids, both written in the table's
        // namespace under numeric ids the service allocated.
        List<Entity> entities = new ArrayList<>();
        client().run(Query.newEntityQueryBuilder().setNamespace("tenant-a").setKind(kind).build())
                .forEachRemaining(entities::add);
        assertThat(entities).hasSize(3);
        assertThat(entities)
                .allSatisfy(
                        entity -> {
                            assertThat(entity.getKey().getNamespace()).isEqualTo("tenant-a");
                            assertThat(entity.getKey().hasId()).isTrue();
                        });
        assertThat(entities)
                .extracting(entity -> entity.getString("label") + entity.getLong("n"))
                .containsExactlyInAnyOrder("a1", "a1", "b2");
        assertThat(readAll(kind)).as("nothing in the default namespace").isEmpty();
    }
}
