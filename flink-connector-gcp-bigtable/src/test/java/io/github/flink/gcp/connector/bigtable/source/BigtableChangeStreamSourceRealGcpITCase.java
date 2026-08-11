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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.serializer.ChangeStreamMutationDeserializationSchema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Gated production-service coverage for the API the emulator cannot implement. */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableChangeStreamSourceRealGcpITCase extends AbstractBigtableRealGcpITCase {

    private static final String APP_PROFILE = "flink-it-change-stream";
    private static final int ROWS = 100;
    private static final AtomicBoolean CHECKPOINTED_AFTER_RECORDS = new AtomicBoolean();
    private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();
    private static final AtomicInteger SEEN = new AtomicInteger();

    @Test
    void readsMutationsAndCompletesAtEndTime() throws Exception {
        TableDestination table = createChangeStreamTable("change-stream-source");
        createSingleClusterAppProfile(APP_PROFILE);
        Instant start = Instant.now();
        String[] expected =
                IntStream.range(0, ROWS)
                        .mapToObj(index -> String.format("row-%04d", index))
                        .toArray(String[]::new);
        seedRows(table, expected);
        Instant end = Instant.now().plusSeconds(30);
        BigtableChangeStreamSource<ChangeStreamMutation> source =
                BigtableChangeStreamSource.<ChangeStreamMutation>builder()
                        .table(table)
                        .appProfileId(APP_PROFILE)
                        .deserializer(new ChangeStreamMutationDeserializationSchema())
                        .startPosition(StartPosition.at(start))
                        .endTime(end)
                        .build();
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, java.time.Duration.ZERO);
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        environment.setParallelism(2);
        environment.enableCheckpointing(500L);
        List<String> keys = new ArrayList<>();

        try (CloseableIterator<ChangeStreamMutation> mutations =
                environment
                        .fromSource(source, WatermarkStrategy.noWatermarks(), "change-stream")
                        .map(new FailAfterACompletedCheckpoint())
                        .executeAndCollect()) {
            mutations.forEachRemaining(mutation -> keys.add(mutation.getRowKey().toStringUtf8()));
        }

        assertThat(FAILED_ONCE).as("the gated job restarted from a completed checkpoint").isTrue();
        assertThat(keys).contains(expected);
    }

    /**
     * Slows the live feed enough for a barrier, then fails once after that checkpoint completes.
     */
    private static final class FailAfterACompletedCheckpoint
            extends RichMapFunction<ChangeStreamMutation, ChangeStreamMutation>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;
        private static final Map<Long, Integer> SEEN_AT_BARRIER = new ConcurrentHashMap<>();

        @Override
        public ChangeStreamMutation map(ChangeStreamMutation mutation) throws Exception {
            Thread.sleep(20L);
            SEEN.incrementAndGet();
            if (CHECKPOINTED_AFTER_RECORDS.get() && FAILED_ONCE.compareAndSet(false, true)) {
                throw new IllegalStateException("Failing the gated Change Streams job once.");
            }
            return mutation;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            SEEN_AT_BARRIER.merge(context.getCheckpointId(), SEEN.get(), Math::max);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (SEEN_AT_BARRIER.getOrDefault(checkpointId, 0) > 0) {
                CHECKPOINTED_AFTER_RECORDS.set(true);
            }
        }
    }
}
