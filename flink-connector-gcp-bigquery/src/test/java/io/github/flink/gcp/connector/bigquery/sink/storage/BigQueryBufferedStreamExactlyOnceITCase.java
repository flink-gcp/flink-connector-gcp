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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.tables.StorageSchemaConverter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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
 * write path, not table-metadata propagation; auto-creation is covered by unit and emulator tests.
 *
 * <p>Skipped unless {@code BQ_IT_PROJECT} and {@code BQ_IT_DATASET} are set (no bucket needed —
 * nothing is staged).
 */
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(600)
class BigQueryBufferedStreamExactlyOnceITCase {

    private static final String PROJECT = System.getenv("BQ_IT_PROJECT");
    private static final String DATASET = System.getenv("BQ_IT_DATASET");

    private static final String RUN_ID = UUID.randomUUID().toString().substring(0, 8);
    private static final String TABLE_RESTART = "buffered_stream_it_restart_" + RUN_ID;
    private static final String TABLE_CLEAN = "buffered_stream_it_clean_" + RUN_ID;
    private static final String TABLE_BATCH = "buffered_stream_it_batch_" + RUN_ID;

    private static final long RECORD_COUNT = 40;
    private static final double RECORDS_PER_SECOND = 4;
    private static final long FAIL_AT_INDEX = 15;

    /** Trips once per JVM: the induced failure fires on the first pass only. */
    private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

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

    /** Rows travel as {@code "name|value"} strings. */
    private static final class RowSerializer extends BigQueryProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        private transient Descriptors.Descriptor descriptor;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return SCHEMA;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return descriptor();
        }

        private Descriptors.Descriptor descriptor() {
            if (descriptor == null) {
                try {
                    descriptor =
                            BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                                    SCHEMA);
                } catch (Descriptors.DescriptorValidationException e) {
                    throw new IllegalStateException(e);
                }
            }
            return descriptor;
        }

        @Override
        public ByteString serialize(String element) {
            String[] parts = element.split("\\|", -1);
            DynamicMessage.Builder row = DynamicMessage.newBuilder(descriptor());
            row.setField(descriptor().findFieldByName("name"), parts[0]);
            row.setField(descriptor().findFieldByName("value"), Long.parseLong(parts[1]));
            return row.build().toByteString();
        }
    }

    @AfterAll
    static void cleanUp() {
        BigQuery bigQuery = bigQuery();
        bigQuery.delete(TableId.of(PROJECT, DATASET, TABLE_RESTART));
        bigQuery.delete(TableId.of(PROJECT, DATASET, TABLE_CLEAN));
        bigQuery.delete(TableId.of(PROJECT, DATASET, TABLE_BATCH));
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
        assertThat(queryLongs("SELECT COUNT(*) FROM `%s`", TABLE_RESTART))
                .containsExactly(RECORD_COUNT);
        // No duplicates and no gaps: every generated index landed exactly once.
        assertThat(queryLongs("SELECT COUNT(DISTINCT value) FROM `%s`", TABLE_RESTART))
                .containsExactly(RECORD_COUNT);
        assertThat(queryLongs("SELECT SUM(value) FROM `%s`", TABLE_RESTART))
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

        assertThat(queryLongs("SELECT COUNT(*) FROM `%s`", TABLE_CLEAN))
                .containsExactly(RECORD_COUNT);
        assertThat(queryLongs("SELECT COUNT(DISTINCT value) FROM `%s`", TABLE_CLEAN))
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

        assertThat(queryLongs("SELECT COUNT(*) FROM `%s`", TABLE_BATCH))
                .containsExactly(RECORD_COUNT);
        assertThat(queryLongs("SELECT COUNT(DISTINCT value) FROM `%s`", TABLE_BATCH))
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
                .destination(TableDestination.of(PROJECT, DATASET, table))
                .serializer(new RowSerializer())
                .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                .build();
    }

    private static void createTable(String table) {
        bigQuery()
                .create(
                        TableInfo.newBuilder(
                                        TableId.of(PROJECT, DATASET, table),
                                        StandardTableDefinition.newBuilder()
                                                .setSchema(
                                                        StorageSchemaConverter.toBigQuerySchema(
                                                                SCHEMA))
                                                .build())
                                .build());
    }

    private static List<Long> queryLongs(String queryTemplate, String table) throws Exception {
        String query = String.format(queryTemplate, PROJECT + "." + DATASET + "." + table);
        TableResult result = bigQuery().query(QueryJobConfiguration.newBuilder(query).build());
        List<Long> values = new ArrayList<>();
        result.iterateAll()
                .forEach(row -> values.add(row.get(0).isNull() ? null : row.get(0).getLongValue()));
        return values;
    }

    private static BigQuery bigQuery() {
        return BigQueryOptions.newBuilder().setProjectId(PROJECT).build().getService();
    }
}
