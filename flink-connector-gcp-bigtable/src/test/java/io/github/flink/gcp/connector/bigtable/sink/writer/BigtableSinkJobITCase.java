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

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests against the Bigtable emulator, driving the sink exclusively through
 * the public {@code BigtableSink.builder()...emulatorEndpoint(...)} path — no test seams. These are
 * the only tests that build the writer through {@code BigtableMutateRowsSink.createWriter(
 * WriterInitContext)}, so they are what covers the serializer's {@code open(...)}, the failure
 * handler's {@code open(...)} and the runtime's own metric group.
 *
 * <p>Both jobs run on the MiniCluster with a rate-limited source, so the streaming one checkpoints
 * several times while records are still arriving and the batch one has nothing but the end-of-input
 * flush. A lost flush shows up as a missing row.
 */
class BigtableSinkJobITCase extends AbstractBigtableEmulatorITCase {

    private static final long RECORD_COUNT = 40;
    private static final double RECORDS_PER_SECOND = 10;

    @Test
    void streamingJobWritesEveryRecord() throws Exception {
        runJob(RuntimeExecutionMode.STREAMING, "job-streaming");
    }

    @Test
    void batchJobWritesEveryRecord() throws Exception {
        // Batch has no checkpoints, so everything rides the end-of-input flush.
        runJob(RuntimeExecutionMode.BATCH, "job-batch");
    }

    @Test
    void streamingJobRoutesEachRecordToTheTableItsResolverNames() throws Exception {
        // The production path with per-record destinations (#232): the writer builds a batcher per
        // table on the task thread, over one client shared by both, and both are torn down by the
        // job's own close. A unit test drives the pool against fakes; this is what says the real
        // client hands out a batcher per table and that neither teardown takes the other down.
        TableDestination even = createTable("job-even");
        TableDestination odd = createTable("job-odd");
        String endpoint = emulatorEndpoint();

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

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "records")
                .sinkTo(
                        BigtableSink.<String>builder()
                                .destinationResolver(resolver)
                                .serializer(
                                        (element, context) ->
                                                RowMutationEntry.create(element)
                                                        .setCell(
                                                                FAMILY, "payload", 1_000L, element))
                                .emulatorEndpoint(endpoint)
                                .build());

        env.execute("bigtable-sink-dynamic-destinations-it");

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

    private static void runJob(RuntimeExecutionMode mode, String tableId) throws Exception {
        TableDestination table = createTable(tableId);

        // Travels into the job graph as a plain value (the container handle is not serializable).
        String endpoint = emulatorEndpoint();

        // With checkpointing enabled Flink defaults to endless fixed-delay restarts; a permanently
        // failing mutation would loop until the test times out instead of failing fast.
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(mode);
        env.setParallelism(2);
        if (mode == RuntimeExecutionMode.STREAMING) {
            // 40 records at 10/s outlive several of these, so the checkpoint flush runs while the
            // source is still producing rather than only at the end.
            env.enableCheckpointing(1_000);
        }

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
                                .setCell(FAMILY, "payload", 1_000L, element);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "records")
                .sinkTo(
                        BigtableSink.<String>builder()
                                .table(table)
                                .serializer(serializer)
                                .emulatorEndpoint(endpoint)
                                .build());

        env.execute("bigtable-sink-" + tableId + "-it");

        Set<String> expected = new LinkedHashSet<>();
        for (long index = 0; index < RECORD_COUNT; index++) {
            expected.add("record-" + index);
        }
        // Row keys are unique per record, so a duplicate write of one is invisible here — which is
        // the point: at-least-once with an explicit cell timestamp is idempotent.
        assertThat(rowKeys(table)).containsExactlyInAnyOrderElementsOf(expected);
    }
}
