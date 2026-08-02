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
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
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
        assertThat(
                        readRows(table).stream()
                                .map(row -> row.getKey().toStringUtf8())
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(expected);
    }
}
