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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.sink.SpannerSink;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSource;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The client-library metrics of {@code exportClientMetricsToFlink(true)}, observed the way a user
 * observes them: through a configured Flink metric reporter, while a job reads or writes the
 * emulator, for every component that builds a client.
 *
 * <p>The real client records them, but only outside its emulator mode, in which it records nothing
 * into a metrics provider. {@link SpannerClients#bypassEmulatorModeForTesting(boolean)} reaches the
 * same emulator over the same plaintext channel without that mode, for the components the jobs
 * build in this JVM. The client also leaves that mode only while {@code SPANNER_EMULATOR_HOST} does
 * not name the emulator, so the environment must not set it.
 *
 * <p>Each job asserts the methods its components register, that each method the component is
 * documented to call was recorded, and that the client recorded no method the component did not
 * register: {@link ClientMetrics} logs each such method once.
 */
class SpannerClientMetricsITCase extends AbstractSpannerEmulatorITCase {

    private static final String[] LEAVES = {
        "operations",
        "operationTimeMillis",
        "attempts",
        "attemptTimeMillis",
        "attemptsWithGfeTiming",
        "gfeTimeMillis",
        "gfeConnectivityErrors",
        "attemptsWithAfeTiming",
        "afeTimeMillis",
        "afeConnectivityErrors"
    };

    private static final String SINGERS_DDL =
            "CREATE TABLE singers (id INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id)";

    @BeforeAll
    static void requireNoEmulatorEnvironment() {
        assertThat(System.getenv("SPANNER_EMULATOR_HOST"))
                .as(
                        "SPANNER_EMULATOR_HOST puts the client back in its emulator mode, where it"
                                + " records no client metrics; unset it to run this test")
                .isNull();
    }

    @BeforeEach
    void leaveTheEmulatorMode() {
        RpcMethodSeriesReporter.reset();
        SpannerClients.bypassEmulatorModeForTesting(true);
    }

    @AfterEach
    void restoreTheEmulatorMode() {
        // After the job: its MiniCluster has shut down by the time execute() returns.
        SpannerClients.bypassEmulatorModeForTesting(false);
        RpcMethodSeriesReporter.reset();
    }

    @Test
    void theWriterReportsItsSessionSchemaReadsAndBatchWrites() throws Exception {
        DatabaseDestination database = createDatabase(Dialect.GOOGLE_STANDARD_SQL, SINGERS_DDL);

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            StreamExecutionEnvironment env = environment(1);
            env.enableCheckpointing(500);
            env.fromSource(
                            new DataGeneratorSource<>(
                                    (GeneratorFunction<Long, Long>) index -> index,
                                    40,
                                    RateLimiterStrategy.perSecond(10),
                                    Types.LONG),
                            WatermarkStrategy.noWatermarks(),
                            "records")
                    .sinkTo(
                            SpannerSink.<Long>builder()
                                    .database(database)
                                    .serializer(
                                            (id, context) ->
                                                    Mutation.newInsertOrUpdateBuilder("singers")
                                                            .set("id")
                                                            .to(id)
                                                            .set("name")
                                                            .to("singer-" + id)
                                                            .build())
                                    .emulatorEndpoint(emulatorEndpoint())
                                    .exportClientMetricsToFlink(true)
                                    .build());
            env.execute("spanner-client-metrics-sink");

            assertRegistered("task", "CreateSession", "ExecuteStreamingSql", "BatchWrite");
            assertRecorded("task", "CreateSession", "ExecuteStreamingSql", "BatchWrite");
            // A report read the series while the job ran, which is what a reporter ever sees.
            assertThat(RpcMethodSeriesReporter.reported().get("task/BatchWrite.operations"))
                    .isPositive();
            assertEmulatorDeviation("task", "BatchWrite");
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void theEnumeratorAndTheReadersReportAQueryRead() throws Exception {
        DatabaseDestination database = singersWithRows();

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            List<Long> ids =
                    readIds(
                            database,
                            SpannerReadOperation.query(Statement.of("SELECT id FROM singers")));

            assertThat(ids).containsExactlyInAnyOrder(1L, 2L, 3L);
            assertRegistered(
                    "coordinator",
                    "CreateSession",
                    "BeginTransaction",
                    "ExecuteStreamingSql",
                    "PartitionQuery",
                    "PartitionRead");
            assertRecorded("coordinator", "CreateSession", "BeginTransaction", "PartitionQuery");
            assertRegistered("task", "ExecuteStreamingSql", "StreamingRead");
            assertRecorded("task", "ExecuteStreamingSql");
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void theEnumeratorAndTheReadersReportATableRead() throws Exception {
        DatabaseDestination database = singersWithRows();

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            List<Long> ids =
                    readIds(
                            database,
                            SpannerReadOperation.read(
                                    "singers", KeySet.all(), Collections.singletonList("id")));

            assertThat(ids).containsExactlyInAnyOrder(1L, 2L, 3L);
            assertRecorded("coordinator", "CreateSession", "BeginTransaction", "PartitionRead");
            assertRecorded("task", "StreamingRead");
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void theCoordinatorAndTheReadersReportAChangeStreamRead() throws Exception {
        DatabaseDestination database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        SINGERS_DDL,
                        "CREATE CHANGE STREAM changes FOR singers");
        Timestamp first = insert(database, 1L);
        Timestamp last = insert(database, 2L);

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            StreamExecutionEnvironment env = environment(2);
            SpannerChangeStreamSource<DataChangeRecord> source =
                    SpannerChangeStreamSource.<DataChangeRecord>builder()
                            .database(database)
                            .changeStreamName("changes")
                            .deserializer(new Records())
                            .startPosition(StartPosition.at(instant(first)))
                            .boundedTimestamp(instant(last))
                            .heartbeatInterval(Duration.ofSeconds(1))
                            .emulatorEndpoint(emulatorEndpoint())
                            .exportClientMetricsToFlink(true)
                            .build();
            List<DataChangeRecord> records = new ArrayList<>();
            try (CloseableIterator<DataChangeRecord> collected =
                    env.fromSource(source, WatermarkStrategy.noWatermarks(), "changes")
                            .executeAndCollect()) {
                collected.forEachRemaining(records::add);
            }

            assertThat(records).hasSize(2);
            assertRegistered("coordinator", "CreateSession", "ExecuteStreamingSql");
            assertRecorded("coordinator", "CreateSession", "ExecuteStreamingSql");
            assertRegistered("task", "CreateSession", "ExecuteStreamingSql");
            assertRecorded("task", "CreateSession", "ExecuteStreamingSql");
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void theTableScanReportsThroughItsOption() throws Exception {
        DatabaseDestination database = singersWithRows();

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            StreamTableEnvironment table = tableEnvironment(RuntimeExecutionMode.BATCH);
            table.executeSql(singersTableDdl(database, "singers_scan", ""));
            List<Row> rows = collect(table, "SELECT id FROM singers_scan");

            assertThat(rows).hasSize(3);
            assertRecorded("coordinator", "CreateSession", "BeginTransaction", "PartitionRead");
            assertRecorded("task", "StreamingRead");
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void theEnumeratorReportsTheMetadataQueryOfAnIndexScan() throws Exception {
        DatabaseDestination database = singersWithRows();
        updateDdl(database, "CREATE INDEX singers_by_name ON singers (name)");

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            StreamTableEnvironment table = tableEnvironment(RuntimeExecutionMode.BATCH);
            table.executeSql(
                    singersTableDdl(
                            database, "singers_by_name", ",\n  'scan.index' = 'singers_by_name'"));
            List<Row> rows = collect(table, "SELECT id FROM singers_by_name");

            assertThat(rows).hasSize(3);
            // The index scan resolves its index and key columns with a query before it
            // partitions the read.
            assertRecorded("coordinator", "ExecuteStreamingSql");
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void theLookupReportsItsPointReads(boolean async) throws Exception {
        DatabaseDestination database = singersWithRows();

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            StreamTableEnvironment table = tableEnvironment(RuntimeExecutionMode.BATCH);
            table.executeSql(
                    singersTableDdl(
                            database, "singers_lookup", ",\n  'lookup.async' = '" + async + "'"));
            table.executeSql(
                    "CREATE TABLE facts (id BIGINT, event_time AS PROCTIME()) WITH ("
                            + "'connector'='datagen', 'number-of-rows'='4', "
                            + "'fields.id.kind'='sequence', 'fields.id.start'='1', "
                            + "'fields.id.end'='4')");
            List<Row> rows =
                    collect(
                            table,
                            "SELECT f.id, s.name FROM facts AS f LEFT JOIN singers_lookup "
                                    + "FOR SYSTEM_TIME AS OF f.event_time AS s ON f.id = s.id");

            assertThat(rows).hasSize(4);
            assertRegistered("task", "CreateSession", "StreamingRead");
            assertRecorded("task", "CreateSession", "StreamingRead");
            assertThat(RpcMethodSeriesReporter.current().get("task/StreamingRead.operations"))
                    .isEqualTo(4);
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void nothingIsRegisteredWhenTheExportIsOff() throws Exception {
        DatabaseDestination database = createDatabase(Dialect.GOOGLE_STANDARD_SQL, SINGERS_DDL);

        StreamExecutionEnvironment env = environment(1);
        env.fromSequence(0, 4)
                .sinkTo(
                        SpannerSink.<Long>builder()
                                .database(database)
                                .serializer(
                                        (id, context) ->
                                                Mutation.newInsertBuilder("singers")
                                                        .set("id")
                                                        .to(id)
                                                        .build())
                                .emulatorEndpoint(emulatorEndpoint())
                                .build());
        env.execute("spanner-client-metrics-off");

        assertThat(RpcMethodSeriesReporter.current()).isEmpty();
    }

    // ---------------------------------------------------------------- helpers

    /** Every leaf of every method is registered for the component, and nothing else is. */
    private static void assertRegistered(String component, String... methods) {
        List<String> expected = new ArrayList<>();
        for (String method : methods) {
            for (String leaf : LEAVES) {
                expected.add(component + "/" + method + "." + leaf);
            }
        }
        assertThat(
                        RpcMethodSeriesReporter.current().keySet().stream()
                                .filter(key -> key.startsWith(component + "/"))
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    /** Each method completed at least one operation of at least one attempt. */
    private static void assertRecorded(String component, String... methods) {
        Map<String, Long> current = RpcMethodSeriesReporter.current();
        for (String method : methods) {
            String prefix = component + "/" + method + ".";
            assertThat(current.get(prefix + "operations")).as(prefix + "operations").isPositive();
            assertThat(current.get(prefix + "attempts"))
                    .as(prefix + "attempts")
                    .isGreaterThanOrEqualTo(current.get(prefix + "operations"));
        }
    }

    /**
     * The emulator sends neither front end's timing header, so the client counts every attempt
     * under both connectivity-error leaves and records no front-end time. The documentation tells
     * an emulator user to expect exactly this.
     */
    private static void assertEmulatorDeviation(String component, String method) {
        Map<String, Long> current = RpcMethodSeriesReporter.current();
        String prefix = component + "/" + method + ".";
        long attempts = current.get(prefix + "attempts");
        assertThat(current.get(prefix + "gfeConnectivityErrors")).isEqualTo(attempts);
        assertThat(current.get(prefix + "attemptsWithGfeTiming")).isZero();
        assertThat(current.get(prefix + "attemptsWithAfeTiming")).isZero();
        // The client records no AFE series at all when this variable turns them off.
        if (!Boolean.parseBoolean(System.getenv("SPANNER_DISABLE_AFE_SERVER_TIMING"))) {
            assertThat(current.get(prefix + "afeConnectivityErrors")).isEqualTo(attempts);
        }
    }

    private static StreamExecutionEnvironment environment(int parallelism) {
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        configuration.setString(
                "metrics.reporter.rpc.factory.class", RpcMethodSeriesReporter.class.getName());
        configuration.setString("metrics.reporter.rpc.interval", "100 ms");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);
        return env;
    }

    private static StreamTableEnvironment tableEnvironment(RuntimeExecutionMode mode) {
        StreamExecutionEnvironment env = environment(1);
        env.setRuntimeMode(mode);
        return StreamTableEnvironment.create(env);
    }

    private static List<Row> collect(StreamTableEnvironment table, String query) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> collected = table.executeSql(query).collect()) {
            collected.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static List<Long> readIds(DatabaseDestination database, SpannerReadOperation operation)
            throws Exception {
        StreamExecutionEnvironment env = environment(2);
        List<Long> ids = new ArrayList<>();
        try (CloseableIterator<Long> collected =
                env.fromSource(
                                SpannerSource.<Long>builder()
                                        .database(database)
                                        .readOperation(operation)
                                        .deserializer(new Ids())
                                        .emulatorEndpoint(emulatorEndpoint())
                                        .exportClientMetricsToFlink(true)
                                        .build(),
                                WatermarkStrategy.noWatermarks(),
                                "singers")
                        .executeAndCollect()) {
            collected.forEachRemaining(ids::add);
        }
        return ids;
    }

    private static DatabaseDestination singersWithRows() throws Exception {
        DatabaseDestination database = createDatabase(Dialect.GOOGLE_STANDARD_SQL, SINGERS_DDL);
        insert(database, 1L);
        insert(database, 2L);
        insert(database, 3L);
        return database;
    }

    private static Timestamp insert(DatabaseDestination database, long id) {
        return client(database)
                .write(
                        Collections.singletonList(
                                Mutation.newInsertBuilder("singers")
                                        .set("id")
                                        .to(id)
                                        .set("name")
                                        .to("singer-" + id)
                                        .build()));
    }

    private static Instant instant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    private static String singersTableDdl(
            DatabaseDestination database, String name, String extraOptions) {
        return "CREATE TABLE "
                + name
                + " (\n"
                + "  id BIGINT,\n"
                + "  name STRING,\n"
                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'spanner',\n"
                + "  'project' = '"
                + database.getProject()
                + "',\n"
                + "  'instance' = '"
                + database.getInstance()
                + "',\n"
                + "  'database' = '"
                + database.getDatabase()
                + "',\n"
                + "  'table' = 'singers',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "',\n"
                + "  'client-metrics.export-to-flink' = 'true'"
                + extraOptions
                + "\n)";
    }

    /** Reads the id column of each row. */
    private static final class Ids implements SpannerStructDeserializationSchema<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Struct row, Collector<Long> out) {
            out.collect(row.getLong("id"));
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            return Types.LONG;
        }
    }

    /** Passes each change record through. */
    private static final class Records
            implements SpannerChangeStreamDeserializationSchema<DataChangeRecord> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DataChangeRecord record, Collector<DataChangeRecord> out) {
            out.collect(record);
        }

        @Override
        public TypeInformation<DataChangeRecord> getProducedType() {
            return TypeInformation.of(DataChangeRecord.class);
        }
    }
}
