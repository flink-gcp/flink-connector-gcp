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
import com.google.cloud.datastore.DoubleValue;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityValue;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.ListValue;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * With {@code scan.filter-push-down.enabled}, a filtered scan of indexed data returns the rows of
 * the unfiltered scan followed by Flink's filter, and drops an entity whose filtered value is
 * excluded from indexes, the loss the option's assertion accepts ({@code docs/adr/0184}); against
 * the emulator.
 *
 * <p>The oracle is a table of the same kind without the option, which pushes no equality on a
 * property. The indexed entities cover what a pushed equality must not drop and what it may admit
 * for Flink to remove: a missing property, a stored null, a value of another type, a double under
 * {@code BIGINT}, a string where a blob is filtered, an array holding the filtered value, and a
 * near miss of a string and of a timestamp. A {@code DOUBLE} column holds an integer and a fraction
 * beside the matching one, a fixed-length column a shorter, a space-padded and a longer value, an
 * embedded entity column a double, an array, a non-entity value and a list of entities, and the
 * dotted names both the property of that name and an embedded entity on the path.
 *
 * <p>A key equality is pushed with the option or without it, so the table without the option is no
 * oracle for one: the key tests read kinds of their own and name the rows they expect, near-miss
 * names, a child entity and entities of another namespace included.
 */
@Testcontainers
class DatastoreTableFilterPushDownITCase extends AbstractDatastoreEmulatorITCase {

    private static final String COLUMNS =
            "id STRING NOT NULL, s STRING, n BIGINT, b BOOLEAN, y BYTES, ts TIMESTAMP_LTZ(6),"
                    + " ts9 TIMESTAMP_LTZ(9), d DOUBLE, c CHAR(3), bin BINARY(2),"
                    + " e ROW<x BIGINT, flag BOOLEAN>, `a.b` STRING, `a``b` STRING, `a\\b` STRING,"
                    + " PRIMARY KEY (id) NOT ENFORCED";

    private static final Timestamp TIME = Timestamp.ofTimeSecondsAndNanos(1_000, 123_456_000);

    private static String kind;

    private static String unindexedKind;

    private static String keyKind;

    private static String idKind;

    private static final String NAMESPACE = "tenant";

    @BeforeAll
    static void writeEntities() {
        kind = uniqueKind();
        put(Entity.newBuilder(key(kind, "missing")).build());
        put(
                Entity.newBuilder(key(kind, "nulls"))
                        .setNull("s")
                        .setNull("n")
                        .setNull("b")
                        .setNull("y")
                        .setNull("ts")
                        .build());
        put(
                Entity.newBuilder(key(kind, "match"))
                        .set("s", "x")
                        .set("n", 5L)
                        .set("b", true)
                        .set("y", Blob.copyFrom(new byte[] {1, 2}))
                        .set("ts", TIME)
                        .set("ts9", TIME)
                        .set("d", 5.5d)
                        .set("c", "abc")
                        .set("bin", Blob.copyFrom(new byte[] {1, 2}))
                        .set("e", FullEntity.newBuilder().set("x", 1L).set("flag", true).build())
                        .set("a.b", "q")
                        .set("a`b", "q")
                        .set("a\\b", "q")
                        .build());
        // A double under BIGINT is a mismatch; the service does not match it to an integer.
        put(
                Entity.newBuilder(key(kind, "numbers"))
                        .set("n", 5.0d)
                        .set("b", false)
                        .set("d", 5L)
                        .build());
        // A value of another type under every column; the service matches the string to the
        // blob filter, and Flink reads it as a mismatch.
        put(
                Entity.newBuilder(key(kind, "others"))
                        .set("s", 5L)
                        .set("n", "5")
                        .set("b", "true")
                        .set("y", "\u0001\u0002")
                        .set("ts", 1_000L)
                        .build());
        // An array matches an equality on any element; Flink reads it as a mismatch.
        put(
                Entity.newBuilder(key(kind, "arrays"))
                        .set("s", ListValue.of(StringValue.of("x"), StringValue.of("z")))
                        .set("n", ListValue.of(LongValue.of(5), LongValue.of(7)))
                        .set("d", ListValue.of(DoubleValue.of(5.5), DoubleValue.of(1.0)))
                        .set(
                                "e",
                                ListValue.of(
                                        EntityValue.of(
                                                FullEntity.newBuilder()
                                                        .set("x", 1L)
                                                        .set("flag", true)
                                                        .build())))
                        .build());
        put(
                Entity.newBuilder(key(kind, "near"))
                        .set("s", "x ")
                        .set("n", 6L)
                        .set("ts", Timestamp.ofTimeSecondsAndNanos(1_000, 123_457_000))
                        .set("ts9", Timestamp.ofTimeSecondsAndNanos(1_000, 123_457_000))
                        .set("d", 5.25d)
                        .build());
        // A fixed-length column reads a stored value unpadded and untruncated; a double under
        // the nested BIGINT is a mismatch.
        put(
                Entity.newBuilder(key(kind, "short"))
                        .set("c", "ab")
                        .set("bin", Blob.copyFrom(new byte[] {1}))
                        .set("e", FullEntity.newBuilder().set("x", 1.0d).set("flag", false).build())
                        .build());
        put(
                Entity.newBuilder(key(kind, "padded"))
                        .set("c", "ab ")
                        .set(
                                "e",
                                FullEntity.newBuilder()
                                        .set("x", ListValue.of(LongValue.of(1)))
                                        .build())
                        .build());
        put(
                Entity.newBuilder(key(kind, "long"))
                        .set("c", "abcd")
                        .set("bin", Blob.copyFrom(new byte[] {1, 2, 3}))
                        .set("e", "x")
                        .build());
        // A path's property names match both the embedded entity and a property named by the
        // whole path; Flink reads only the property its column names.
        put(
                Entity.newBuilder(key(kind, "paths"))
                        .set("e.x", 1L)
                        .set("e.flag", true)
                        .set("a", FullEntity.newBuilder().set("b", "q").build())
                        .build());

        keyKind = uniqueKind();
        put(Entity.newBuilder(key(keyKind, "k")).set("s", "x").build());
        put(Entity.newBuilder(key(keyKind, "k ")).set("s", "x").build());
        put(Entity.newBuilder(key(keyKind, "K")).set("s", "x").build());
        put(
                Entity.newBuilder(Key.newBuilder(key("Parent", "p"), keyKind, "k").build())
                        .set("s", "x")
                        .build());
        // The namespace holds its own near misses, so a key built in another namespace, or one
        // admitting another name, shows in the namespaced test.
        for (String name : new String[] {"k", "K", "k "}) {
            put(
                    Entity.newBuilder(
                                    Key.newBuilder(PROJECT, keyKind, name)
                                            .setNamespace(NAMESPACE)
                                            .build())
                            .set("s", "x")
                            .build());
        }

        idKind = uniqueKind();
        for (long id : new long[] {5, -5, 6}) {
            put(
                    Entity.newBuilder(Key.newBuilder(PROJECT, idKind, id).build())
                            .set("s", "x")
                            .build());
        }
        // A name the table cannot read under a BIGINT key: a filter on the id 5 must not admit
        // it, or the scan fails.
        put(Entity.newBuilder(key(idKind, "5")).set("s", "x").build());

        unindexedKind = uniqueKind();
        put(Entity.newBuilder(key(unindexedKind, "indexed")).set("s", "x").build());
        put(
                Entity.newBuilder(key(unindexedKind, "unindexed"))
                        .set("s", StringValue.newBuilder("x").setExcludeFromIndexes(true).build())
                        .build());
    }

    private static void put(Entity entity) {
        client().put(entity);
    }

    private static TableEnvironment tables(String kind, String policy) {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        // The timestamp literals below are UTC wall-clock times.
        table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
        String common =
                "'connector' = 'datastore', 'project' = '"
                        + PROJECT
                        + "', 'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "', 'kind' = '"
                        + kind
                        + "', 'type-mismatch-policy' = '"
                        + policy
                        + "'";
        table.executeSql(
                "CREATE TABLE pushed ("
                        + COLUMNS
                        + ") WITH ("
                        + common
                        + ", 'scan.filter-push-down.enabled' = 'true',"
                        // The builder refuses a split count for a query it cannot split, so this
                        // also holds that a pushed query stays splittable.
                        + " 'scan.partition.max-partitions' = '4')");
        table.executeSql("CREATE TABLE unpushed (" + COLUMNS + ") WITH (" + common + ")");
        return table;
    }

    private static List<Row> ids(TableEnvironment table, String from, String predicate)
            throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator =
                table.executeSql("SELECT id FROM " + from + " WHERE " + predicate).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "s = 'x'",
                "'x' = s",
                "n = 5",
                "5 = n",
                "b",
                "NOT b",
                "y = x'0102'",
                "ts = CAST(TIMESTAMP '1970-01-01 00:16:40.123456' AS TIMESTAMP_LTZ(6))",
                "ts9 = CAST(TIMESTAMP '1970-01-01 00:16:40.123456' AS TIMESTAMP_LTZ(9))",
                "s = 'x' AND n = 5 AND b",
                "s = 'x' AND n > 4",
                "d = 5.5",
                "c = 'abc'",
                "c = 'ab '",
                "bin = x'0102'",
                "e.x = 1",
                "e.flag",
                "NOT e.flag",
                "e.x = 1 AND e.flag",
                "`a.b` = 'q'",
                "`a``b` = 'q'",
                "`a\\b` = 'q'"
            })
    void aPushedEqualityReturnsTheRowsFlinksFilterKeeps(String predicate) throws Exception {
        TableEnvironment table = tables(kind, "null");

        assertThat(table.explainSql("SELECT id FROM pushed WHERE " + predicate))
                .containsPattern("filter=\\[[^\\]]");
        assertThat(ids(table, "pushed", predicate))
                .containsExactlyInAnyOrderElementsOf(ids(table, "unpushed", predicate));
    }

    @Test
    void theOracleSeesWhatTheEqualitiesShouldAndShouldNotMatch() throws Exception {
        // Firing control for the comparison above: these are the rows the oracle keeps, so an
        // equality the service matched more narrowly than Flink would show as a missing id.
        TableEnvironment table = tables(kind, "null");

        assertThat(ids(table, "unpushed", "s = 'x'")).containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "n = 5")).containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "b")).containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "NOT b")).containsExactly(Row.of("numbers"));
        assertThat(ids(table, "unpushed", "y = x'0102'")).containsExactly(Row.of("match"));
        assertThat(
                        ids(
                                table,
                                "unpushed",
                                "ts = CAST(TIMESTAMP '1970-01-01 00:16:40.123456' AS TIMESTAMP_LTZ(6))"))
                .containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "'x' = s")).containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "5 = n")).containsExactly(Row.of("match"));
        assertThat(
                        ids(
                                table,
                                "unpushed",
                                "ts9 = CAST(TIMESTAMP '1970-01-01 00:16:40.123456' AS TIMESTAMP_LTZ(9))"))
                .containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "s = 'x' AND n = 5 AND b"))
                .containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "s = 'x' AND n > 4")).containsExactly(Row.of("match"));
    }

    @Test
    void theOracleSeesWhatTheNewShapesShouldAndShouldNotMatch() throws Exception {
        // Firing control for the DOUBLE, fixed-length, nested and dotted-name comparisons above.
        TableEnvironment table = tables(kind, "null");

        assertThat(ids(table, "unpushed", "d = 5.5")).containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "c = 'abc'")).containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "c = 'ab '")).containsExactly(Row.of("padded"));
        assertThat(ids(table, "unpushed", "bin = x'0102'")).containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "e.x = 1")).containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "e.flag")).containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "NOT e.flag")).containsExactly(Row.of("short"));
        assertThat(ids(table, "unpushed", "e.x = 1 AND e.flag")).containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "`a.b` = 'q'")).containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "`a``b` = 'q'")).containsExactly(Row.of("match"));
        assertThat(ids(table, "unpushed", "`a\\b` = 'q'")).containsExactly(Row.of("match"));
    }

    private static TableEnvironment keyTables() {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        String common =
                "'connector' = 'datastore', 'project' = '"
                        + PROJECT
                        + "', 'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "', 'kind' = '"
                        + keyKind
                        + "', 'scan.partition.max-partitions' = '4'";
        String columns = "id STRING NOT NULL, s STRING, PRIMARY KEY (id) NOT ENFORCED";
        table.executeSql("CREATE TABLE keyed (" + columns + ") WITH (" + common + ")");
        table.executeSql(
                "CREATE TABLE tenant ("
                        + columns
                        + ") WITH ("
                        + common
                        + ", 'namespace' = '"
                        + NAMESPACE
                        + "')");
        return table;
    }

    @ParameterizedTest
    @ValueSource(strings = {"k", "k ", "K", "absent"})
    void aKeyEqualityIsPushedWithoutTheOptionAndReadsThatRootEntityOnly(String name)
            throws Exception {
        TableEnvironment table = keyTables();
        String predicate = "id = '" + name + "'";

        assertThat(table.explainSql("SELECT id FROM keyed WHERE " + predicate))
                .containsPattern("filter=\\[[^\\]]");
        // The kind holds a child entity named k, whose key the table cannot read: the filter
        // admits only the root entity.
        assertThat(ids(table, "keyed", predicate))
                .containsExactlyElementsOf(
                        name.equals("absent") ? List.of() : List.of(Row.of(name)));
    }

    @Test
    void aKeyEqualityCarriesTheTablesNamespace() throws Exception {
        TableEnvironment table = keyTables();

        assertThat(table.explainSql("SELECT id FROM tenant WHERE id = 'k'"))
                .containsPattern("filter=\\[[^\\]]");
        assertThat(ids(table, "tenant", "id = 'k'")).containsExactly(Row.of("k"));
        assertThat(ids(table, "tenant", "id = 'K'")).containsExactly(Row.of("K"));
        assertThat(ids(table, "tenant", "id = 'absent'")).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(longs = {5, -5, 7})
    void anIdKeyEqualityReadsThatNumberedEntityOnly(long id) throws Exception {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(
                "CREATE TABLE numbered (id BIGINT NOT NULL, s STRING, PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('connector' = 'datastore', 'project' = '"
                        + PROJECT
                        + "', 'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "', 'kind' = '"
                        + idKind
                        + "', 'scan.partition.max-partitions' = '4')");
        String query = "SELECT id FROM numbered WHERE id = " + id;

        assertThat(table.explainSql(query)).containsPattern("filter=\\[[^\\]]");
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(query).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        // The kind also holds the name "5", which fails an unfiltered read of this table.
        assertThat(rows).containsExactlyElementsOf(id == 7 ? List.of() : List.of(Row.of(id)));
        assertThatThrownBy(
                        () -> {
                            try (CloseableIterator<Row> iterator =
                                    table.executeSql("SELECT id FROM numbered WHERE s = 'x'")
                                            .collect()) {
                                iterator.forEachRemaining(row -> {});
                            }
                        })
                .hasStackTraceContaining("its key has the name '5'");
    }

    @Test
    void theKeyOracleFailsOnTheChildEntityTheFilterExcludes() {
        // Firing control: without a key equality the scan reads the child entity, whose key the
        // table cannot read, so a pushed key equality also spares the scan that failure.
        TableEnvironment table = keyTables();

        assertThatThrownBy(() -> ids(table, "keyed", "s = 'x'"))
                .hasStackTraceContaining("its key has the parent");
    }

    @Test
    void anEntityWhoseFilteredValueIsUnindexedIsDropped() throws Exception {
        // The loss the option's assertion accepts: the oracle keeps both entities, the pushed
        // filter cannot see the unindexed value.
        TableEnvironment table = tables(unindexedKind, "null");

        assertThat(ids(table, "unpushed", "s = 'x'"))
                .containsExactlyInAnyOrder(Row.of("indexed"), Row.of("unindexed"));
        assertThat(ids(table, "pushed", "s = 'x'")).containsExactly(Row.of("indexed"));
    }

    @Test
    void aFloatingPointEqualityMissesAStoredIntegerSoAnIntegralDoubleIsNotPushed() {
        // The premise behind declining an integral DOUBLE literal: the converter reads the integer
        // 5 into a DOUBLE column as 5.0, which the service's equality on 5.0 does not match.
        String numbers = uniqueKind();
        put(Entity.newBuilder(key(numbers, "integer")).set("d", 5L).build());
        put(Entity.newBuilder(key(numbers, "double")).set("d", 5.0d).build());

        List<String> names = new ArrayList<>();
        client().run(
                        Query.newEntityQueryBuilder()
                                .setKind(numbers)
                                .setFilter(PropertyFilter.eq("d", 5.0d))
                                .build())
                .forEachRemaining(entity -> names.add(entity.getKey().getName()));

        assertThat(names).containsExactly("double");
    }

    @Test
    void anEntityTheFilterExcludesIsNotReadSoItsMismatchCannotFailTheScan() throws Exception {
        // Type checking covers the entities read: unfiltered, the string under BIGINT fails the
        // scan; filtered, it is never read. The arrays entity is excluded by b, which it lacks:
        // an entity the filter admits is read and checked as before.
        TableEnvironment table = tables(kind, "fail");

        assertThat(ids(table, "pushed", "s = 'x' AND n = 5 AND b"))
                .containsExactly(Row.of("match"));
        assertThatThrownBy(() -> ids(table, "unpushed", "s = 'x' AND n = 5 AND b"))
                .hasStackTraceContaining("holds a value of type");
    }
}
