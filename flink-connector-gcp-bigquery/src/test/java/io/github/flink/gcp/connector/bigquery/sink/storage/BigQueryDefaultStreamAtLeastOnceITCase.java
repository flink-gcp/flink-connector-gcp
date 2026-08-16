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

package io.github.flink.gcp.connector.bigquery.sink.storage;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigquery.FieldValueList;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests of the default write method ({@link
 * WriteMethod#STORAGE_API_AT_LEAST_ONCE}) against <b>real</b> BigQuery: MiniCluster streaming jobs
 * running the production {@code StreamWriterRowAppenderFactory} — SDK connection-pool multiplexing,
 * scale-up and in-stream retries under the service's real flow control and ack latencies, which the
 * emulator's plaintext single-connection appender cannot exercise.
 *
 * <p>Two scenarios from issue #16: multiplexed fan-out to many tables through one connection pool,
 * and an induced mid-run restart with dynamic destinations proving the at-least-once contract —
 * <b>no gaps</b> (every generated index lands), duplicates permitted (rows appended after the last
 * completed checkpoint are legitimately replayed).
 *
 * <p>Quota and retry behavior is covered <em>implicitly</em> by running the production factory
 * against the live service. {@code RESOURCE_EXHAUSTED} is deliberately not synthesized: reliably
 * tripping a quota means sustained abusive load against the shared free-tier project and the
 * dataset every other gated ITCase writes into — flaky by construction. Error classification for
 * quota responses stays pinned by unit tests against fakes.
 *
 * <p>Destination tables are created up front (and deleted afterwards) so the tests exercise the
 * write path, not table-metadata propagation; auto-creation is covered by unit and emulator tests.
 *
 * <p>Skipped unless {@code BQ_IT_PROJECT} and {@code BQ_IT_DATASET} are set (no bucket needed —
 * nothing is staged).
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(600)
class BigQueryDefaultStreamAtLeastOnceITCase {

    private static final String RUN_ID = TestNames.runId();

    private static final int FAN_OUT_TABLE_COUNT = 8;
    private static final long FAN_OUT_RECORD_COUNT = 120;
    private static final double FAN_OUT_RECORDS_PER_SECOND = 12;

    private static final int RESTART_TABLE_COUNT = 4;
    private static final long RESTART_RECORD_COUNT = 40;
    private static final double RESTART_RECORDS_PER_SECOND = 4;
    private static final long FAIL_AT_INDEX = 15;

    /** Trips once per JVM: the induced failure fires on the first pass only. */
    private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

    @AfterAll
    static void cleanUp() {
        RealBigQuery.deleteTables(tables("fan", FAN_OUT_TABLE_COUNT));
        RealBigQuery.deleteTables(tables("restart", RESTART_TABLE_COUNT));
    }

    /**
     * Fan-out across eight tables through one JVM-static connection pool: enough destinations that
     * the pool actually multiplexes several streams per connection (the pool starts at two
     * connections), on a clean streaming run spanning several checkpoints.
     */
    @Test
    void fanOutAcrossEightTablesThroughOneConnectionPool() throws Exception {
        createTables("fan", FAN_OUT_TABLE_COUNT);
        // Fail fast instead of looping on a permanent failure.
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(2_000);
        env.setParallelism(2);

        env.fromSource(
                        source(FAN_OUT_RECORD_COUNT, FAN_OUT_RECORDS_PER_SECOND),
                        WatermarkStrategy.noWatermarks(),
                        "rows")
                .sinkTo(sink("fan", FAN_OUT_TABLE_COUNT));

        env.execute("default-stream-fan-out-it");

        assertTablesComplete("fan", FAN_OUT_TABLE_COUNT, FAN_OUT_RECORD_COUNT);
    }

    /**
     * The at-least-once inversion of the exactly-once restart test: the map function throws once
     * mid-run — after at least one checkpoint completed and while appends are in flight — and the
     * job restores with dynamic destinations, rebuilding per-destination writer state. Afterwards
     * every generated index must be present in its table (no gaps); rows re-appended since the last
     * checkpoint may appear twice (duplicates permitted).
     */
    @Test
    void atLeastOnceAcrossAnInducedRestartWithFanOut() throws Exception {
        createTables("restart", RESTART_TABLE_COUNT);
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(2_000);
        env.setParallelism(2);

        env.fromSource(
                        source(RESTART_RECORD_COUNT, RESTART_RECORDS_PER_SECOND),
                        WatermarkStrategy.noWatermarks(),
                        "rows")
                // Throws once, roughly eight seconds in (perSecond() divides the rate among the
                // two subtasks, and index 15 is the sixteenth record of subtask 0's range) —
                // after several checkpoints completed and while further appends are in flight —
                // so the restore path actually runs.
                .map(
                        element -> {
                            long value = Long.parseLong(element.split("\\|", -1)[1]);
                            if (value == FAIL_AT_INDEX && FAILED_ONCE.compareAndSet(false, true)) {
                                throw new IllegalStateException(
                                        "induced failure at index " + value);
                            }
                            return element;
                        })
                .sinkTo(sink("restart", RESTART_TABLE_COUNT));

        env.execute("default-stream-restart-it");

        assertThat(FAILED_ONCE).isTrue();
        assertTablesComplete("restart", RESTART_TABLE_COUNT, RESTART_RECORD_COUNT);
    }

    private static DataGeneratorSource<String> source(long recordCount, double recordsPerSecond) {
        return new DataGeneratorSource<>(
                (GeneratorFunction<Long, String>) index -> "row" + index + "|" + index,
                recordCount,
                RateLimiterStrategy.perSecond(recordsPerSecond),
                Types.STRING);
    }

    /** A sink routing each record to {@link #table}{@code (prefix, value % tableCount)}. */
    private static org.apache.flink.api.connector.sink2.Sink<String> sink(
            String prefix, int tableCount) {
        String project = RealBigQuery.project();
        String dataset = RealBigQuery.dataset();
        return BigQuerySink.<String>builder()
                .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                .destinationResolver(
                        (element, context) -> {
                            long value = Long.parseLong(element.split("\\|", -1)[1]);
                            return TableDestination.of(
                                    project, dataset, table(prefix, (int) (value % tableCount)));
                        })
                .serializer(new NameValueRowSerializer())
                .build();
    }

    /**
     * One query job over all the tables instead of a query job per assertion per table: per table,
     * the distinct count (a shortfall is a gap; duplicates are permitted by at-least-once and
     * deliberately unasserted — {@code COUNT(*) >= COUNT(DISTINCT)} holds trivially) and a
     * misrouting counter.
     */
    private static void assertTablesComplete(String prefix, int tableCount, long recordCount)
            throws Exception {
        // Exact only while recordCount is a multiple of tableCount (both record-count constants
        // are): each residue of value % tableCount then appears exactly this many times.
        long expectedDistinct = recordCount / tableCount;
        String sql =
                IntStream.range(0, tableCount)
                                .mapToObj(
                                        i ->
                                                String.format(
                                                        "SELECT %1$d AS t,"
                                                                + " COUNT(DISTINCT value) AS dst,"
                                                                + " COUNTIF(MOD(value, %2$d) != %1$d)"
                                                                + " AS misrouted FROM %3$s",
                                                        i,
                                                        tableCount,
                                                        RealBigQuery.tablePath(table(prefix, i))))
                                .collect(Collectors.joining(" UNION ALL "))
                        + " ORDER BY t";
        List<FieldValueList> rows = RealBigQuery.queryRows(sql);
        assertThat(rows).hasSize(tableCount);
        for (FieldValueList row : rows) {
            long tableIndex = row.get("t").getLongValue();
            // No gaps: every index routed to this table is present at least once.
            assertThat(row.get("dst").getLongValue())
                    .as("distinct values in table %d", tableIndex)
                    .isEqualTo(expectedDistinct);
            assertThat(row.get("misrouted").getLongValue())
                    .as("misrouted rows in table %d", tableIndex)
                    .isZero();
        }
    }

    private static void createTables(String prefix, int count) {
        for (int i = 0; i < count; i++) {
            RealBigQuery.createTable(table(prefix, i), NameValueRowSerializer.SCHEMA);
        }
    }

    private static String[] tables(String prefix, int count) {
        return IntStream.range(0, count).mapToObj(i -> table(prefix, i)).toArray(String[]::new);
    }

    private static String table(String prefix, int index) {
        return "default_stream_it_" + prefix + index + "_" + RUN_ID;
    }
}
