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
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.DefaultChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.bigtable.source.serializer.ChangeStreamMutationDeserializationSchema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
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
    private static final Map<Integer, Set<Integer>> TABLET_RANGES_BY_SUBTASK =
            new ConcurrentHashMap<>();

    @Test
    void readsMutationsAndCompletesAtEndTime() throws Exception {
        CHECKPOINTED_AFTER_RECORDS.set(false);
        FAILED_ONCE.set(false);
        SEEN.set(0);
        TABLET_RANGES_BY_SUBTASK.clear();
        ObserveConcurrencyAndFailAfterCheckpoint.SEEN_AT_BARRIER.clear();

        TableDestination table =
                createChangeStreamTableWithSplits(
                        "change-stream-source", "row-0025", "row-0050", "row-0075");
        createSingleClusterAppProfile(APP_PROFILE);
        try (DefaultChangeStreamCoordinatorClient coordinator =
                new DefaultChangeStreamCoordinatorClient(table, APP_PROFILE)) {
            coordinator.loadCredentials();
            assertThat(coordinator.generateInitialPartitions()).hasSizeGreaterThanOrEqualTo(4);
        }
        Instant start = Instant.now();
        String[] expected =
                IntStream.range(0, ROWS)
                        .mapToObj(index -> String.format("row-%04d", index))
                        .toArray(String[]::new);
        seedRows(table, expected);
        Instant end = Instant.now().plusSeconds(120);
        ChangeStreamMutationDeserializationSchema deserializer =
                new ChangeStreamMutationDeserializationSchema();
        BigtableChangeStreamSource<ChangeStreamMutation> source =
                BigtableChangeStreamSource.<ChangeStreamMutation>builder()
                        .table(table)
                        .appProfileId(APP_PROFILE)
                        .deserializer(deserializer)
                        .startPosition(StartPosition.at(start))
                        .endTime(end)
                        .build();
        Configuration configuration = new Configuration();
        ActiveChangeStreamReadsReporter.reset();
        configuration.set(MetricOptions.REPORTERS_LIST, "active-reads");
        Configuration reporter = MetricOptions.forReporter(configuration, "active-reads");
        reporter.set(
                MetricOptions.REPORTER_FACTORY_CLASS,
                ActiveChangeStreamReadsReporter.class.getName());
        reporter.set(MetricOptions.REPORTER_INTERVAL, java.time.Duration.ofMillis(10));
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, java.time.Duration.ZERO);
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        environment.setParallelism(2);
        environment.enableCheckpointing(500L);
        List<String> keys = new ArrayList<>();
        ExecutorService triggerExecutor = Executors.newSingleThreadExecutor();
        Future<?> restartTrigger =
                triggerExecutor.submit(
                        () -> {
                            await(
                                    "a completed checkpoint after Change Streams records",
                                    java.time.Duration.ofMinutes(3),
                                    CHECKPOINTED_AFTER_RECORDS::get);
                            seedRows(table, "row-0100");
                            return null;
                        });

        try {
            try (CloseableIterator<ChangeStreamMutation> mutations =
                    environment
                            .fromSource(source, WatermarkStrategy.noWatermarks(), "change-stream")
                            .map(new ObserveConcurrencyAndFailAfterCheckpoint())
                            .returns(deserializer.getProducedType())
                            .executeAndCollect()) {
                mutations.forEachRemaining(
                        mutation -> keys.add(mutation.getRowKey().toStringUtf8()));
            }
            restartTrigger.get();
        } finally {
            triggerExecutor.shutdownNow();
        }

        assertThat(FAILED_ONCE).as("the gated job restarted from a completed checkpoint").isTrue();
        assertThat(keys).contains(expected);
        assertThat(TABLET_RANGES_BY_SUBTASK)
                .as("each source subtask processed mutations from at least two tablet ranges")
                .hasSize(2)
                .allSatisfy((subtask, ranges) -> assertThat(ranges).hasSizeGreaterThanOrEqualTo(2));
        assertThat(ActiveChangeStreamReadsReporter.peaks())
                .as("the default opened at least two concurrent partition reads in each subtask")
                .containsEntry(0, 2)
                .containsEntry(1, 2);
    }

    /** Fails on the first mutation written after a checkpoint that included earlier mutations. */
    private static final class ObserveConcurrencyAndFailAfterCheckpoint
            extends RichMapFunction<ChangeStreamMutation, ChangeStreamMutation>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;
        private static final Map<Long, Integer> SEEN_AT_BARRIER = new ConcurrentHashMap<>();

        @Override
        public ChangeStreamMutation map(ChangeStreamMutation mutation) throws Exception {
            int row = Integer.parseInt(mutation.getRowKey().toStringUtf8().substring(4));
            int tabletRange = Math.min(row / 25, 3);
            TABLET_RANGES_BY_SUBTASK
                    .computeIfAbsent(
                            getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                            ignored -> ConcurrentHashMap.newKeySet())
                    .add(tabletRange);
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
