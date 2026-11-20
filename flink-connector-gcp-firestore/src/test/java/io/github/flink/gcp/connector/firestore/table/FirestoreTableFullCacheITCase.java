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
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Joins served by Flink's FULL lookup cache, loaded from documents in the emulator. */
@Testcontainers
class FirestoreTableFullCacheITCase extends AbstractFirestoreEmulatorITCase {

    private static final Duration ROW_DEADLINE = Duration.ofSeconds(90);

    private static TableEnvironment batch() {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        return table;
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
                                + "', 'lookup.cache' = 'FULL',"
                                + " 'lookup.full-cache.periodic-reload.interval' = '1 h'");
        for (String option : extra) {
            options.append(", ").append(option);
        }
        return "WITH (" + options + ")";
    }

    /** A probe of the given values, one row each, with a processing-time attribute. */
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

    @Test
    void aFullCacheJoinsOnTheIdAndOnAnotherField() throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/a").set(Map.of("name", "Ada", "tier", "gold")).get();
        client().document(collection + "/b").set(Map.of("name", "Bob", "tier", "gold")).get();
        client().document(collection + "/c").set(Map.of("name", "Cy", "tier", "silver")).get();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, name STRING, tier STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection));
        probe(table, "a", "gold", "missing");

        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.name FROM probe AS p LEFT JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id"))
                .containsExactlyInAnyOrder(
                        Row.of("a", "Ada"), Row.of("gold", null), Row.of("missing", null));
        // A point lookup can address the id alone; the full cache serves any field.
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
        String collection = uniqueCollection();
        client().document(collection + "/a").set(Map.of("name", "Ada", "n", 1L)).get();
        client().document(collection + "/b").set(Map.of("name", "Ada", "n", 2L)).get();
        // A range matches only values of its own type, so a load whose query carries the
        // comparison leaves this document out; a load that reads it fails under 'fail'. Flink's
        // LookupJoin prints only the table's name, so the load's outcome is the evidence.
        client().document(collection + "/c").set(Map.of("name", "Ada", "n", "x")).get();
        String table =
                "CREATE TABLE dim (id STRING NOT NULL, name STRING, n BIGINT,"
                        + " PRIMARY KEY (id) NOT ENFORCED) ";
        List<String> joins =
                List.of(
                        "SELECT p.k, d.id FROM probe AS p JOIN dim FOR SYSTEM_TIME AS OF p.t AS d"
                                + " ON p.k = d.name WHERE d.n > 1",
                        "SELECT p.k, d.id FROM probe AS p JOIN dim FOR SYSTEM_TIME AS OF p.t AS d"
                                + " ON p.k = d.name AND d.n > 1");

        // Streaming mode offers the condition to the lookup source, so the load's query carries
        // it, and Flink still applies it to the cached rows.
        TableEnvironment streaming =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        streaming.getConfig().set("parallelism.default", "1");
        streaming.executeSql(table + with(collection));
        probe(streaming, "Ada");
        for (String join : joins) {
            assertThat(collect(streaming, join)).containsExactly(Row.of("Ada", "b"));
        }

        // Batch mode keeps the condition on the join, so every load reads the whole collection;
        // the docs' cost statement rests on both halves.
        TableEnvironment batch = batch();
        batch.executeSql(table + with(collection));
        batch.executeSql(
                table.replace("TABLE dim", "TABLE lenient")
                        + with(collection, "'type-mismatch-policy' = 'null'"));
        probe(batch, "Ada");
        for (String join : joins) {
            assertLoadFails(batch, join);
            assertThat(collect(batch, join.replace("JOIN dim", "JOIN lenient")))
                    .containsExactly(Row.of("Ada", "b"));
        }
    }

    /**
     * Asserts that the query's FULL load failed on document 'c'. Flink releases the first-load
     * latch before it publishes the load's failure (Flink 2.2.1 and 1.20.5, {@code
     * CacheLoader#reload} and {@code ReloadTriggerContext}), so a probe can see the empty cache
     * instead of the exception; a load that succeeded would have joined document 'b', so either
     * outcome shows the load read 'c'.
     */
    private static void assertLoadFails(TableEnvironment table, String query) {
        List<Row> rows;
        try {
            rows = collect(table, query);
        } catch (Exception e) {
            assertThat(e)
                    .as(query)
                    .hasStackTraceContaining("Field 'n' holds a value of type java.lang.String");
            return;
        }
        assertThat(rows).as("rows of a failed load: %s", query).isEmpty();
    }

    @Test
    void aCollectionGroupTableJoinsOnItsDocumentPath() throws Exception {
        String group = uniqueCollection();
        client().document("a/1/" + group + "/x").set(Map.of("name", "Ada")).get();
        client().document("b/1/" + group + "/x").set(Map.of("name", "Bob")).get();
        TableEnvironment table = batch();
        // A collection-group table has no PRIMARY KEY, so only the full cache can look it up.
        table.executeSql(
                "CREATE TABLE dim (name STRING,"
                        + " path STRING METADATA FROM 'document-path' VIRTUAL) "
                        + with(group, "'scan.collection-group' = 'true'"));
        probe(table, "b/1/" + group + "/x", "c/1/" + group + "/x");

        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.name FROM probe AS p LEFT JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.path"))
                .containsExactlyInAnyOrder(
                        Row.of("b/1/" + group + "/x", "Bob"), Row.of("c/1/" + group + "/x", null));
    }

    @Test
    void bytesAndCoarseTimestampKeysMatchByValueAndANullKeyMatchesNothing() throws Exception {
        String collection = uniqueCollection();
        Timestamp stored = Timestamp.ofTimeSecondsAndNanos(1_786_000_000L, 123_456_000);
        client().document(collection + "/a")
                .set(Map.of("code", Blob.fromBytes(new byte[] {1, 2}), "seen", stored))
                .get();
        Map<String, Object> nulls = new HashMap<>();
        nulls.put("code", null);
        nulls.put("seen", null);
        client().document(collection + "/b").set(nulls).get();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, code BYTES, seen TIMESTAMP_LTZ(3),"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection));
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
    void aMetadataKeyOfItsAdvertisedTypeJoinsAndACastOneIsNoKey() throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/a").set(Map.of("name", "Ada")).get();
        Timestamp updated = read(collection + "/a").getUpdateTime();
        // The service's times are whole microseconds, so the literal is the stored value.
        String literal =
                LocalDateTime.ofEpochSecond(
                                updated.getSeconds(), updated.getNanos(), ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));
        TableEnvironment table = batch();
        table.getConfig().set("table.local-time-zone", "UTC");
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, name STRING,"
                        + " updated TIMESTAMP_LTZ(6) METADATA FROM 'update-time' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection));
        table.executeSql(
                "CREATE TABLE coarse (id STRING NOT NULL, name STRING,"
                        + " updated TIMESTAMP_LTZ(3) METADATA FROM 'update-time' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection));
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
    void aLoadReadsTheCurrentDocumentsWhateverScanReadTimeSays() throws Exception {
        String collection = uniqueCollection();
        // The service's clock, not this JVM's, decides what a read time sees.
        Timestamp written =
                client().document(collection + "/before")
                        .set(Map.of("name", "Before"))
                        .get()
                        .getUpdateTime();
        Instant before = Instant.ofEpochSecond(written.getSeconds(), written.getNanos());
        client().document(collection + "/a").set(Map.of("name", "Ada")).get();
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, name STRING, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(collection, "'scan.read-time' = '" + before + "'"));
        probe(table, "a");

        // At the scan's read time the document did not exist yet.
        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.name FROM probe AS p LEFT JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id"))
                .containsExactly(Row.of("a", "Ada"));
        // The firing control: the scan of the same table reads at the configured time.
        assertThat(collect(table, "SELECT id FROM dim")).containsExactly(Row.of("before"));
    }

    @Test
    void aFullCacheRefusesAsynchronousLookupsWhileTheJoinIsPlanned() {
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, name STRING, PRIMARY KEY (id) NOT ENFORCED) "
                        + with(uniqueCollection(), "'lookup.async' = 'true'"));
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
    void aPeriodicReloadObservesAChangedDocument() throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/1").set(Map.of("name", "Ada")).get();
        TableEnvironment table =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        table.getConfig().set("parallelism.default", "1");
        String options =
                with(collection)
                        .replace(
                                "'lookup.full-cache.periodic-reload.interval' = '1 h'",
                                "'lookup.full-cache.periodic-reload.interval' = '1 s'");
        assertThat(options).as("the one-second reload reached the DDL").contains("'1 s'");
        table.executeSql(
                "CREATE TABLE dim (id STRING NOT NULL, name STRING, PRIMARY KEY (id) NOT ENFORCED) "
                        + options);
        // datagen refuses min == max, and a constant key would fold away; ids 1 and 2 are random.
        table.executeSql(
                "CREATE TABLE ticks (n BIGINT, k AS CAST(n AS STRING), t AS PROCTIME())"
                        + " WITH ('connector' = 'datagen', 'rows-per-second' = '20',"
                        + " 'fields.n.min' = '1', 'fields.n.max' = '2')");
        List<Row> seen = Collections.synchronizedList(new ArrayList<>());

        try (CloseableIterator<Row> rows =
                table.executeSql(
                                "SELECT k.k, d.name FROM ticks AS k JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF k.t AS d ON k.k = d.id")
                        .collect()) {
            awaitRow(rows, row -> "Ada".equals(row.getField(1)), seen);
            client().document(collection + "/1").set(Map.of("name", "Grace")).get();
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
