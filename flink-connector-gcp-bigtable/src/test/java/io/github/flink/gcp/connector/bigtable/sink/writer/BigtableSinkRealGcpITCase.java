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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The sink against real Cloud Bigtable, driven through the public builder with <b>no</b> {@code
 * emulatorEndpoint(...)}. That absence is the whole point: every other integration test in this
 * module passes an emulator endpoint, so the branch of {@code DefaultMutationBatcherFactory} that
 * builds a client over application-default credentials against the production endpoint — the one
 * every real job takes — runs nowhere else.
 *
 * <p>A MiniCluster streaming job with a rate-limited source, so checkpoints happen while records
 * are still arriving and a lost flush shows up as a missing row. Batch execution is not repeated
 * here: what distinguishes it is the end-of-input flush rather than anything about the service, and
 * {@code BigtableSinkJobITCase} covers it against the emulator.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableSinkRealGcpITCase extends AbstractBigtableRealGcpITCase {

    private static final long RECORD_COUNT = 40;
    private static final double RECORDS_PER_SECOND = 10;

    /** Microseconds, and a multiple of 1000 as a table's millisecond granularity requires. */
    private static final long CELL_TIMESTAMP = 1_000L;

    @Test
    void streamingJobWritesEveryRecord() throws Exception {
        TableDestination table = createTable("job-streaming");

        // With checkpointing enabled Flink defaults to endless fixed-delay restarts; a permanently
        // failing mutation would loop until the test times out instead of failing fast.
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(2);
        // 40 records at 10/s outlive several of these, so the checkpoint flush runs while the
        // source is still producing rather than only at the end.
        env.enableCheckpointing(1_000);

        DataGeneratorSource<String> source =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, String>) index -> "record-" + index,
                        RECORD_COUNT,
                        RateLimiterStrategy.perSecond(RECORDS_PER_SECOND),
                        Types.STRING);

        BigtableSerializationSchema<String> serializer =
                (element, context) ->
                        RowMutationEntry.create(element)
                                // An explicit timestamp, so a replayed record overwrites its cell
                                // instead of adding a version — the idempotency the sink documents.
                                .setCell(FAMILY, "payload", CELL_TIMESTAMP, element);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "records")
                .sinkTo(BigtableSink.<String>builder().table(table).serializer(serializer).build());

        env.execute("bigtable-sink-real-gcp-it");

        Set<String> expected = new LinkedHashSet<>();
        for (long index = 0; index < RECORD_COUNT; index++) {
            expected.add("record-" + index);
        }
        List<Row> rows = readRows(table);
        // Row keys are unique per record, so a duplicate write of one is invisible here — which is
        // the point: at-least-once with an explicit cell timestamp is idempotent.
        assertThat(
                        rows.stream()
                                .map(row -> row.getKey().toStringUtf8())
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(expected);

        RowCell cell = rows.get(0).getCells(FAMILY, "payload").get(0);
        assertThat(cell.getValue().toStringUtf8()).isEqualTo(rows.get(0).getKey().toStringUtf8());
        assertThat(cell.getTimestamp()).isEqualTo(CELL_TIMESTAMP);
    }

    @Test
    void streamingJobRoutesEachRecordToTheTableItsResolverNames() throws Exception {
        // Per-record destinations against the real service (#232). Two tables of one instance, so
        // the writer holds a batcher each over a client they share — the arrangement the emulator
        // cannot falsify, since what is at stake is whether a real BigtableDataClient hands out a
        // working batcher per table and survives both of their teardowns.
        TableDestination even = createTable("job-even");
        TableDestination odd = createTable("job-odd");

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(2);
        env.enableCheckpointing(1_000);

        DataGeneratorSource<String> source =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, String>) index -> "record-" + index,
                        RECORD_COUNT,
                        RateLimiterStrategy.perSecond(RECORDS_PER_SECOND),
                        Types.STRING);

        DestinationResolver<String> resolver =
                (element, context) -> index(element) % 2 == 0 ? even : odd;
        BigtableSerializationSchema<String> serializer =
                (element, context) ->
                        RowMutationEntry.create(element)
                                .setCell(FAMILY, "payload", CELL_TIMESTAMP, element);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "records")
                .sinkTo(
                        BigtableSink.<String>builder()
                                .destinationResolver(resolver)
                                .serializer(serializer)
                                .build());

        env.execute("bigtable-sink-real-gcp-dynamic-destinations-it");

        Set<String> expectedEven = new LinkedHashSet<>();
        Set<String> expectedOdd = new LinkedHashSet<>();
        for (long index = 0; index < RECORD_COUNT; index++) {
            (index % 2 == 0 ? expectedEven : expectedOdd).add("record-" + index);
        }
        assertThat(rowKeys(even)).containsExactlyInAnyOrderElementsOf(expectedEven);
        assertThat(rowKeys(odd)).containsExactlyInAnyOrderElementsOf(expectedOdd);
    }

    private static long index(String record) {
        return Long.parseLong(record.substring("record-".length()));
    }

    private static List<String> rowKeys(TableDestination table) {
        return readRows(table).stream()
                .map(row -> row.getKey().toStringUtf8())
                .collect(Collectors.toList());
    }
}
