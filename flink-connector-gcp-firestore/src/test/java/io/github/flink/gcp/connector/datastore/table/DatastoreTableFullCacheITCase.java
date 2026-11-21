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
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.StringValue;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Joins served by Flink's FULL lookup cache, loaded from entities in the emulator. */
@Testcontainers
class DatastoreTableFullCacheITCase extends AbstractDatastoreEmulatorITCase {

    private static final Duration ROW_DEADLINE = Duration.ofSeconds(90);

    private static TableEnvironment batch() {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        return table;
    }

    private static TableEnvironment streaming() {
        TableEnvironment table =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        table.getConfig().set("parallelism.default", "1");
        return table;
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
                                + "', 'lookup.cache' = 'FULL',"
                                + " 'lookup.full-cache.periodic-reload.interval' = '1 h'");
        for (String option : extra) {
            options.append(", ").append(option);
        }
        return "WITH (" + options + ")";
    }

    /** A probe of the given string values, one row each, with a processing-time attribute. */
    private static void probe(TableEnvironment table, String... values) {
        StringBuilder rows = new StringBuilder();
        for (String value : values) {
            rows.append(rows.length() == 0 ? "" : ", ").append("('").append(value).append("')");
        }
        table.executeSql(
                "CREATE TEMPORARY VIEW probe AS SELECT k, PROCTIME() AS t FROM (VALUES "
                        + rows
                        + ") AS v(k)");
    }

    private static List<Row> collect(TableEnvironment table, String query) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(query).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static StringValue unindexed(String value) {
        return StringValue.newBuilder(value).setExcludeFromIndexes(true).build();
    }

    @Test
    void aFullCacheJoinsOnTheKeyAndOnAnUnindexedProperty() throws Exception {
        String kind = uniqueKind();
        // A Datastore filter never matches an unindexed value, and a point lookup addresses the
        // key alone, so only a load of whole entities can join on 'tier'.
        client().put(
                        Entity.newBuilder(key(kind, "a"))
                                .set("name", "Ada")
                                .set("tier", unindexed("gold"))
                                .build(),
                        Entity.newBuilder(key(kind, "b"))
                                .set("name", "Bob")
                                .set("tier", unindexed("gold"))
                                .build(),
                        Entity.newBuilder(key(kind, "c"))
                                .set("name", "Cy")
                                .set("tier", unindexed("silver"))
                                .build());
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, name STRING, tier STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));
        probe(table, "a", "gold", "missing");

        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.name FROM probe AS p LEFT JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id"))
                .containsExactlyInAnyOrder(
                        Row.of("a", "Ada"), Row.of("gold", null), Row.of("missing", null));
        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.name FROM probe AS p LEFT JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.tier"))
                .containsExactlyInAnyOrder(
                        Row.of("a", null),
                        Row.of("gold", "Ada"),
                        Row.of("gold", "Bob"),
                        Row.of("missing", null));
    }

    @Test
    void aConditionOnTheDimensionTableNarrowsAStreamingLoadButNotABatchLoad() throws Exception {
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(key(kind, "a")).set("name", "Ada").set("n", 1L).build(),
                        Entity.newBuilder(key(kind, "b")).set("name", "Ada").set("n", 2L).build(),
                        // A filter never matches an unindexed value, so a load whose query
                        // carries the comparison leaves this entity out; a load that reads it
                        // fails under 'fail'. Flink's LookupJoin prints only the table's name, so
                        // the load's outcome is the evidence.
                        Entity.newBuilder(key(kind, "c"))
                                .set("name", "Ada")
                                .set("n", unindexed("x"))
                                .build());
        String ddl =
                "CREATE TABLE dim (id STRING NOT NULL, name STRING, n BIGINT,"
                        + " PRIMARY KEY (id) NOT ENFORCED) ";
        List<String> joins =
                List.of(
                        "SELECT p.k, d.id FROM probe AS p JOIN dim FOR SYSTEM_TIME AS OF p.t AS d"
                                + " ON p.k = d.name WHERE d.n > 1",
                        "SELECT p.k, d.id FROM probe AS p JOIN dim FOR SYSTEM_TIME AS OF p.t AS d"
                                + " ON p.k = d.name AND d.n > 1");

        // Streaming mode offers the condition to the lookup source, so under the option the
        // load's query carries it, and Flink still applies it to the cached rows.
        TableEnvironment pushed = streaming();
        pushed.executeSql(ddl + with(kind, "'scan.filter-push-down.enabled' = 'true'"));
        probe(pushed, "Ada");
        for (String join : joins) {
            assertThat(collect(pushed, join)).as(join).containsExactly(Row.of("Ada", "b"));
        }

        // Without the option nothing but a key equality is pushed, so the streaming load reads
        // the whole kind: the firing control of the streaming half.
        TableEnvironment unpushed = streaming();
        unpushed.executeSql(ddl + with(kind));
        probe(unpushed, "Ada");
        for (String join : joins) {
            assertLoadFails(unpushed, join);
        }

        // Batch mode keeps the condition on the join, so every load reads the whole kind even
        // under the option; the docs' cost statement rests on both halves.
        TableEnvironment batch = batch();
        batch.executeSql(ddl + with(kind, "'scan.filter-push-down.enabled' = 'true'"));
        batch.executeSql(
                ddl.replace("TABLE dim", "TABLE lenient")
                        + with(
                                kind,
                                "'scan.filter-push-down.enabled' = 'true'",
                                "'type-mismatch-policy' = 'null'"));
        probe(batch, "Ada");
        for (String join : joins) {
            assertLoadFails(batch, join);
            assertThat(collect(batch, join.replace("JOIN dim", "JOIN lenient")))
                    .as(join)
                    .containsExactly(Row.of("Ada", "b"));
        }
    }

    /**
     * Asserts that the query's FULL load failed on entity 'c'. Flink releases the first-load latch
     * before it publishes the load's failure (Flink 2.2.1 and 1.20.5, {@code CacheLoader#reload}
     * and {@code ReloadTriggerContext}), so a probe can see the empty cache instead of the
     * exception; a load that succeeded would have joined entity 'b', so either outcome shows the
     * load read 'c'.
     */
    private static void assertLoadFails(TableEnvironment table, String query) {
        List<Row> rows;
        try {
            rows = collect(table, query);
        } catch (Exception e) {
            assertThat(e).as(query).hasStackTraceContaining("cannot be read into the table");
            return;
        }
        assertThat(rows).as("rows of a failed load: %s", query).isEmpty();
    }

    @Test
    void aTableWithoutAPrimaryKeyJoinsOnTheKeysNumericId() throws Exception {
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(Key.newBuilder(PROJECT, kind, 5L).build())
                                .set("name", "Ada")
                                .build(),
                        Entity.newBuilder(Key.newBuilder(PROJECT, kind, 7L).build())
                                .set("name", "Bob")
                                .build());
        TableEnvironment table = batch();
        // A point lookup refuses a table without a PRIMARY KEY; the full cache indexes the
        // metadata column like any other.
        table.executeSql(
                "CREATE TABLE dim (name STRING, id BIGINT METADATA FROM 'key-id' VIRTUAL) "
                        + with(kind));
        table.executeSql(
                "CREATE TEMPORARY VIEW ids AS SELECT k, PROCTIME() AS t FROM (VALUES"
                        + " (CAST(7 AS BIGINT)), (CAST(9 AS BIGINT))) AS v(k)");

        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.name FROM ids AS p LEFT JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id"))
                .containsExactlyInAnyOrder(Row.of(7L, "Bob"), Row.of(9L, null));
    }

    @Test
    void bytesAndCoarseTimestampKeysMatchByValueAndANullKeyMatchesNothing() throws Exception {
        String kind = uniqueKind();
        // Rounding would read .123789 as .124, so only truncation joins the .123 probe below.
        Timestamp stored = Timestamp.ofTimeSecondsAndNanos(1_786_000_000L, 123_789_000);
        client().put(
                        Entity.newBuilder(key(kind, "a"))
                                .set("code", Blob.copyFrom(new byte[] {1, 2}))
                                .set("seen", stored)
                                .build(),
                        Entity.newBuilder(key(kind, "b")).setNull("code").setNull("seen").build());
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, code BYTES, seen TIMESTAMP_LTZ(3),"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));
        table.executeSql(
                "CREATE TEMPORARY VIEW probes AS SELECT code, TO_TIMESTAMP_LTZ(millis, 3) AS seen,"
                        + " PROCTIME() AS t FROM (VALUES (X'0102', CAST(1786000000123 AS BIGINT)),"
                        + " (X'0103', CAST(1786000000124 AS BIGINT)),"
                        + " (CAST(NULL AS BYTES), CAST(NULL AS BIGINT))) AS v(code, millis)");

        // A byte array matches by content, and the stored microseconds read truncated to the
        // declared milliseconds, so the millisecond probe finds them.
        assertThat(
                        collect(
                                table,
                                "SELECT p.code, d.id FROM probes AS p LEFT JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.code = d.code"))
                .containsExactlyInAnyOrder(
                        Row.of(new byte[] {1, 2}, "a"),
                        Row.of(new byte[] {1, 3}, null),
                        Row.of(null, null));
        assertThat(
                        collect(
                                table,
                                "SELECT p.seen IS NULL, d.id FROM probes AS p LEFT JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.seen = d.seen"))
                .containsExactlyInAnyOrder(
                        Row.of(false, "a"), Row.of(false, null), Row.of(true, null));
    }

    @Test
    void fixedLengthKeysMatchAsFlinksOwnEqualityDoes() throws Exception {
        String kind = uniqueKind();
        // A fixed-length column reads a stored value unpadded, so a shorter one is not equal to a
        // padded probe; the full cache's index agrees with a regular join over the scan's rows.
        client().put(
                        Entity.newBuilder(key(kind, "full"))
                                .set("code", "abc")
                                .set("bin", Blob.copyFrom(new byte[] {1, 2}))
                                .build(),
                        Entity.newBuilder(key(kind, "short"))
                                .set("code", "ab")
                                .set("bin", Blob.copyFrom(new byte[] {1}))
                                .build());
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, code CHAR(3), bin BINARY(2),"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));
        table.executeSql(
                "CREATE TEMPORARY VIEW probes AS SELECT CAST(c AS CHAR(3)) AS code,"
                        + " CAST(b AS BINARY(2)) AS bin, PROCTIME() AS t FROM (VALUES"
                        + " ('abc', X'0102'), ('ab', X'01')) AS v(c, b)");

        for (String column : List.of("code", "bin")) {
            List<Row> cached =
                    collect(
                            table,
                            "SELECT p."
                                    + column
                                    + ", d.id FROM probes AS p LEFT JOIN dim"
                                    + " FOR SYSTEM_TIME AS OF p.t AS d ON p."
                                    + column
                                    + " = d."
                                    + column);
            List<Row> regular =
                    collect(
                            table,
                            "SELECT p."
                                    + column
                                    + ", d.id FROM (SELECT code, bin FROM probes) AS p"
                                    + " LEFT JOIN dim AS d ON p."
                                    + column
                                    + " = d."
                                    + column);
            assertThat(cached).as(column).containsExactlyInAnyOrderElementsOf(regular);
            assertThat(cached)
                    .as(column)
                    .extracting(row -> row.getField(1))
                    .containsExactlyInAnyOrder("full", null);
        }
    }

    @Test
    void aMetadataKeyOfItsAdvertisedTypeJoinsAndACastOneIsNoKey() throws Exception {
        String kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "a")).set("name", "Ada").build());
        TableEnvironment table = batch();
        table.getConfig().set("table.local-time-zone", "UTC");
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, name STRING,"
                        + " updated TIMESTAMP_LTZ(6) NOT NULL METADATA FROM 'update-time' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));
        table.executeSql(
                "CREATE TABLE coarse (id STRING NOT NULL, name STRING,"
                        + " updated TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'update-time' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));
        // The scan reads the stored update time, whole microseconds, so the literal is exact.
        Instant updated = (Instant) collect(table, "SELECT updated FROM dim").get(0).getField(0);
        String literal =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                        .withZone(ZoneOffset.UTC)
                        .format(updated);
        table.executeSql(
                "CREATE TEMPORARY VIEW stamps AS SELECT CAST(ts AS TIMESTAMP_LTZ(6)) AS seen,"
                        + " PROCTIME() AS t FROM (VALUES (TIMESTAMP '"
                        + literal
                        + "')) AS v(ts)");

        assertThat(
                        collect(
                                table,
                                "SELECT d.name FROM stamps AS s JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF s.t AS d ON s.seen = d.updated"))
                .containsExactly(Row.of("Ada"));
        assertThatThrownBy(
                        () ->
                                table.explainSql(
                                        "SELECT d.name FROM stamps AS s JOIN coarse"
                                                + " FOR SYSTEM_TIME AS OF s.t AS d"
                                                + " ON CAST(s.seen AS TIMESTAMP_LTZ(3)) = d.updated"))
                .hasStackTraceContaining("a cast column is not a key");
    }

    @Test
    void aLoadReadsTheCurrentEntitiesWhateverScanReadTimeSays() throws Exception {
        String kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "early")).set("name", "Eve").build());
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE firstRead (id STRING NOT NULL,"
                        + " readAt TIMESTAMP_LTZ(6) NOT NULL METADATA FROM 'read-time' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind));
        // A time the emulator issued, before the entity the load must read was written.
        Instant before =
                (Instant) collect(table, "SELECT readAt FROM firstRead").get(0).getField(0);
        client().put(Entity.newBuilder(key(kind, "a")).set("name", "Ada").build());
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, name STRING, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(kind, "'scan.read-time' = '" + before + "'"));
        probe(table, "a");

        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.name FROM probe AS p LEFT JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id"))
                .containsExactly(Row.of("a", "Ada"));
        // The firing control: the scan of the same table reads at the configured time.
        assertThat(collect(table, "SELECT id FROM dim")).containsExactly(Row.of("early"));
    }

    @Test
    void aFullCacheRefusesAsynchronousLookupsWhileTheJoinIsPlanned() {
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, name STRING, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(uniqueKind(), "'lookup.async' = 'true'"));
        probe(table, "a");

        assertThatThrownBy(
                        () ->
                                table.explainSql(
                                        "SELECT p.k, d.name FROM probe AS p JOIN dim"
                                                + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id"))
                .hasStackTraceContaining(
                        "'lookup.async' cannot be true when 'lookup.cache' is FULL");
    }

    @Test
    void aPeriodicReloadObservesAChangedEntity() throws Exception {
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(Key.newBuilder(PROJECT, kind, 1L).build())
                                .set("name", "Ada")
                                .build());
        TableEnvironment table = streaming();
        String options =
                with(kind)
                        .replace(
                                "'lookup.full-cache.periodic-reload.interval' = '1 h'",
                                "'lookup.full-cache.periodic-reload.interval' = '1 s'");
        assertThat(options).as("the one-second reload reached the DDL").contains("'1 s'");
        table.executeSql(
                "CREATE TABLE dim (id BIGINT NOT NULL, name STRING, PRIMARY KEY (id) NOT ENFORCED) "
                        + options);
        // datagen refuses min == max, and a constant key would fold away; ids 1 and 2 are random.
        table.executeSql(
                "CREATE TABLE ticks (k BIGINT, t AS PROCTIME())"
                        + " WITH ('connector' = 'datagen', 'rows-per-second' = '20',"
                        + " 'fields.k.min' = '1', 'fields.k.max' = '2')");
        List<Row> seen = Collections.synchronizedList(new ArrayList<>());

        try (CloseableIterator<Row> rows =
                table.executeSql(
                                "SELECT k.k, d.name FROM ticks AS k JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF k.t AS d ON k.k = d.id")
                        .collect()) {
            awaitRow(rows, row -> "Ada".equals(row.getField(1)), seen);
            client().put(
                            Entity.newBuilder(Key.newBuilder(PROJECT, kind, 1L).build())
                                    .set("name", "Grace")
                                    .build());
            awaitRow(rows, row -> "Grace".equals(row.getField(1)), seen);
        }
    }

    /**
     * Reads an unbounded query's rows until one matches, within {@link #ROW_DEADLINE}. The iterator
     * stays open for the caller's next wait; the caller closes it, which cancels the job and ends a
     * read still blocked past the deadline.
     */
    private static void awaitRow(CloseableIterator<Row> rows, Predicate<Row> match, List<Row> seen)
            throws Exception {
        ExecutorService reader =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "it-await-row");
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            Future<?> reading =
                    reader.submit(
                            () -> {
                                while (rows.hasNext()) {
                                    Row row = rows.next();
                                    seen.add(row);
                                    if (match.test(row)) {
                                        return;
                                    }
                                }
                                throw new AssertionError(
                                        "The query ended after "
                                                + seen.size()
                                                + " rows without a matching row");
                            });
            try {
                reading.get(ROW_DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new AssertionError(
                        "No matching row within "
                                + ROW_DEADLINE
                                + " after "
                                + seen.size()
                                + " rows; the last was "
                                + (seen.isEmpty() ? "none" : seen.get(seen.size() - 1)),
                        e);
            }
        } finally {
            reader.shutdownNow();
        }
    }
}
