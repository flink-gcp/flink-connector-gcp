/*
 * Copyright 2026 laughingman7743
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

import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests of {@link WriteMethod#STORAGE_API_EXACTLY_ONCE} against <b>real</b>
 * BigQuery — the goccy emulator keeps no flush cursor (every {@code FlushRows} re-inserts all rows
 * up to the offset), so idempotent re-flush and the restore probe can only be verified here.
 *
 * <p>The acceptance test of issue #30 is {@link #exactlyOnceAcrossAnInducedRestart()}: a
 * MiniCluster streaming job whose map function throws once mid-run, restarting and restoring the
 * job while appends and pending commits are in flight; the destination table must hold every
 * generated row exactly once afterwards.
 *
 * <p>Destination tables are created up front (and deleted afterwards) so the tests exercise the
 * write path, not table-metadata propagation. Auto-creation on this write path — and what the
 * service answers for a table that is not there — is {@code
 * BigQueryBufferedStreamMissingTableITCase}, which exists because this pre-creation left the
 * question unasked.
 *
 * <p>Skipped unless {@code BQ_IT_PROJECT} and {@code BQ_IT_DATASET} are set (no bucket needed —
 * nothing is staged).
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(600)
class BigQueryBufferedStreamExactlyOnceITCase {

    private static final String RUN_ID = TestNames.runId();
    private static final String TABLE_RESTART = "buffered_stream_it_restart_" + RUN_ID;
    private static final String TABLE_CLEAN = "buffered_stream_it_clean_" + RUN_ID;
    private static final String TABLE_BATCH = "buffered_stream_it_batch_" + RUN_ID;

    private static final long RECORD_COUNT = 40;
    private static final double RECORDS_PER_SECOND = 4;
    private static final long FAIL_AT_INDEX = 15;

    /** Trips once per JVM: the induced failure fires on the first pass only. */
    private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

    @AfterAll
    static void cleanUp() {
        RealBigQuery.deleteTables(TABLE_RESTART, TABLE_CLEAN, TABLE_BATCH);
    }

    @Test
    void exactlyOnceAcrossAnInducedRestart() throws Exception {
        createTable(TABLE_RESTART);
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(2_000);
        env.setParallelism(2);

        env.fromSource(source(), WatermarkStrategy.noWatermarks(), "rows")
                // Throws once, roughly four seconds in — after at least one checkpoint completed
                // (committed rows exist) and while further appends are in flight — so the restore
                // path (writer-state probe, re-commit of pending flushes) actually runs.
                .map(
                        element -> {
                            long value = Long.parseLong(element.split("\\|", -1)[1]);
                            if (value == FAIL_AT_INDEX && FAILED_ONCE.compareAndSet(false, true)) {
                                throw new IllegalStateException(
                                        "induced failure at index " + value);
                            }
                            return element;
                        })
                .sinkTo(sink(TABLE_RESTART));

        env.execute("buffered-stream-exactly-once-restart-it");

        assertThat(FAILED_ONCE).isTrue();
        String restartPath = RealBigQuery.tablePath(TABLE_RESTART);
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(*) FROM " + restartPath))
                .containsExactly(RECORD_COUNT);
        // No duplicates and no gaps: every generated index landed exactly once.
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(DISTINCT value) FROM " + restartPath))
                .containsExactly(RECORD_COUNT);
        assertThat(RealBigQuery.queryLongs("SELECT SUM(value) FROM " + restartPath))
                .containsExactly(RECORD_COUNT * (RECORD_COUNT - 1) / 2);
    }

    @Test
    void cleanStreamingRunCommitsEveryCheckpoint() throws Exception {
        createTable(TABLE_CLEAN);
        // Fail fast instead of looping on a permanent failure.
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(2_000);
        env.setParallelism(2);

        env.fromSource(source(), WatermarkStrategy.noWatermarks(), "rows")
                .sinkTo(sink(TABLE_CLEAN));

        env.execute("buffered-stream-clean-streaming-it");

        String cleanPath = RealBigQuery.tablePath(TABLE_CLEAN);
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(*) FROM " + cleanPath))
                .containsExactly(RECORD_COUNT);
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(DISTINCT value) FROM " + cleanPath))
                .containsExactly(RECORD_COUNT);
    }

    @Test
    void batchExecutionCommitsAtEndOfInput() throws Exception {
        createTable(TABLE_BATCH);
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);

        DataGeneratorSource<String> source =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, String>) index -> "row" + index + "|" + index,
                        RECORD_COUNT,
                        Types.STRING);
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "rows").sinkTo(sink(TABLE_BATCH));

        env.execute("buffered-stream-batch-it");

        String batchPath = RealBigQuery.tablePath(TABLE_BATCH);
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(*) FROM " + batchPath))
                .containsExactly(RECORD_COUNT);
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(DISTINCT value) FROM " + batchPath))
                .containsExactly(RECORD_COUNT);
    }

    private static DataGeneratorSource<String> source() {
        return new DataGeneratorSource<>(
                (GeneratorFunction<Long, String>) index -> "row" + index + "|" + index,
                RECORD_COUNT,
                RateLimiterStrategy.perSecond(RECORDS_PER_SECOND),
                Types.STRING);
    }

    private static org.apache.flink.api.connector.sink2.Sink<String> sink(String table) {
        return BigQuerySink.<String>builder()
                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                .destination(RealBigQuery.destination(table))
                .serializer(new NameValueRowSerializer())
                .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                .build();
    }

    private static void createTable(String table) {
        RealBigQuery.createTable(table, NameValueRowSerializer.SCHEMA);
    }
}
