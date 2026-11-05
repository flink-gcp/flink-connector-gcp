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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.RealTables;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.bigquery.RealGcs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end streaming integration test against <b>real</b> BigQuery and Cloud Storage (see {@link
 * BigQueryFileLoadsITCase} for why there is no emulator).
 *
 * <p>Runs a MiniCluster DataStream job in streaming mode with a rate-limited source that spans
 * several 5-second checkpoints, so multiple checkpoint-triggered load jobs fire against two dynamic
 * destinations; the guard-interval override ({@code minCheckpointInterval}) makes the fast
 * checkpoints acceptable for this short-lived job.
 *
 * <p>Same environment variables and skip behavior as {@link BigQueryFileLoadsITCase}.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
class BigQueryFileLoadsStreamingITCase {

    private static final String RUN_ID = TestNames.runId();
    private static final String TABLE_A = "file_loads_stream_it_a_" + RUN_ID;
    private static final String TABLE_B = "file_loads_stream_it_b_" + RUN_ID;
    private static final String UPLOADER_TABLE_A = "file_loads_stream_it_ua_" + RUN_ID;
    private static final String UPLOADER_TABLE_B = "file_loads_stream_it_ub_" + RUN_ID;
    private static final String STAGING_PREFIX = "flink-file-loads-stream-it/" + RUN_ID;

    private static final long RECORD_COUNT = 40;
    private static final double RECORDS_PER_SECOND = 4;

    private static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("name")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.REQUIRED))
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("value")
                                    .setType(TableFieldSchema.Type.INT64)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();

    /** Rows travel as {@code "table|name|value"} strings. */
    private static final class RowSerializer extends FixedSchemaProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return SCHEMA;
        }

        @Override
        public ByteString serialize(String element) {
            String[] parts = element.split("\\|", -1);
            DynamicMessage.Builder row = DynamicMessage.newBuilder(descriptor());
            row.setField(field("name"), parts[1]);
            row.setField(field("value"), Long.parseLong(parts[2]));
            return row.build().toByteString();
        }
    }

    @AfterAll
    static void cleanUp() throws Exception {
        Closers.closeAll(
                () ->
                        RealBigQuery.deleteTables(
                                TABLE_A, TABLE_B, UPLOADER_TABLE_A, UPLOADER_TABLE_B),
                () -> RealGcs.deletePrefix(STAGING_PREFIX));
    }

    @Test
    void checkpointTriggeredLoadsAcrossTwoTables() throws Exception {
        String prefix = STAGING_PREFIX + "/default";
        loadAcrossTwoTables(
                TABLE_A, TABLE_B, "file-loads-streaming-it", prefix, 0, options(prefix));
    }

    /**
     * The same job with the background staging uploader on, a 256 KiB upload chunk, rows of about
     * 600 KiB of random text and a one-byte roll threshold. Every record therefore rolls its file
     * after sending at least one full chunk, so the real client takes full chunks and closes the
     * way the uploader hands them over, and real checkpoints drain the uploader before their
     * committables leave the writer. The loads must still carry each record exactly once.
     *
     * <p>Which thread finalized a file is not observable from here; the writer's unit tests pin
     * that. This case holds what they cannot: the real client and real load jobs behind it.
     */
    @Test
    void uploaderFinalizedFilesLoadExactlyOnceAcrossCheckpoints() throws Exception {
        String prefix = STAGING_PREFIX + "/uploader";
        loadAcrossTwoTables(
                UPLOADER_TABLE_A,
                UPLOADER_TABLE_B,
                "file-loads-streaming-uploader-it",
                prefix,
                600 * 1024,
                options(prefix)
                        .backgroundStagingUploadEnabled(true)
                        .maxInFlightUploadOperations(2)
                        .stagingUploadChunkBytes(256L * 1024)
                        .maxStagingFileBytes(1));
    }

    private static FileLoadsOptions.Builder options(String prefix) {
        return FileLoadsOptions.builder()
                .stagingPath(RealGcs.uri(prefix))
                // Explicit opt-in to fast checkpoints for this short-lived test job.
                .minCheckpointInterval(Duration.ofSeconds(1));
    }

    private static void loadAcrossTwoTables(
            String tableA,
            String tableB,
            String jobName,
            String prefix,
            int paddingChars,
            FileLoadsOptions.Builder options)
            throws Exception {
        // With checkpointing enabled Flink defaults to endless fixed-delay restarts; a
        // permanently failing load would loop until the test times out instead of failing fast.
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(5_000);
        env.setParallelism(2);

        DataGeneratorSource<String> source =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, String>)
                                index ->
                                        (index % 2 == 0 ? tableA : tableB)
                                                + "|row"
                                                + index
                                                + padding(index, paddingChars)
                                                + "|"
                                                + index,
                        RECORD_COUNT,
                        RateLimiterStrategy.perSecond(RECORDS_PER_SECOND),
                        Types.STRING);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "rows")
                .sinkTo(
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                .destinationResolver(
                                        (element, context) ->
                                                RealTables.destination(
                                                        element.substring(0, element.indexOf('|'))))
                                .serializer(new RowSerializer())
                                .fileLoadsOptions(options.build())
                                .build());

        env.execute(jobName);

        String tableAPath = RealBigQuery.tablePath(tableA);
        String tableBPath = RealBigQuery.tablePath(tableB);
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(*) FROM " + tableAPath))
                .containsExactly(RECORD_COUNT / 2);
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(*) FROM " + tableBPath))
                .containsExactly(RECORD_COUNT / 2);
        // Every record landed exactly once: as many distinct values as rows, all from the
        // generator's indexes (their sums match), so none is missing and none repeated.
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(DISTINCT value) FROM " + tableAPath))
                .containsExactly(RECORD_COUNT / 2);
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(DISTINCT value) FROM " + tableBPath))
                .containsExactly(RECORD_COUNT / 2);
        assertThat(RealBigQuery.queryLongs("SELECT SUM(value) FROM " + tableAPath))
                .containsExactly(sumOfIndexes(0));
        assertThat(RealBigQuery.queryLongs("SELECT SUM(value) FROM " + tableBPath))
                .containsExactly(sumOfIndexes(1));

        // Staged objects of every checkpoint were deleted after their loads succeeded.
        assertThat(RealGcs.list(prefix)).isEmpty();
    }

    /** Random printable text, seeded by the index, that Zstandard shrinks only a little. */
    private static String padding(long index, int chars) {
        SplittableRandom random = new SplittableRandom(index);
        StringBuilder text = new StringBuilder(chars);
        for (int i = 0; i < chars; i++) {
            text.append((char) ('!' + random.nextInt(90)));
        }
        return text.toString();
    }

    private static long sumOfIndexes(int parity) {
        long sum = 0;
        for (long i = parity; i < RECORD_COUNT; i += 2) {
            sum += i;
        }
        return sum;
    }
}
