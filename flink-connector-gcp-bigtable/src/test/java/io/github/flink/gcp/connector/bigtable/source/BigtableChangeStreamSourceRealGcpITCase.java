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

import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.DefaultChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamMutationDeserializationSchema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
    private static final String ROTATION_APP_PROFILE = "flink-it-change-stream-rotation";
    private static final int ROWS = 100;
    private static final AtomicBoolean CHECKPOINTED_AFTER_RECORDS = new AtomicBoolean();
    private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();
    private static final AtomicInteger SEEN = new AtomicInteger();
    private static final Map<Integer, Set<Integer>> TABLET_RANGES_BY_SUBTASK =
            new ConcurrentHashMap<>();
    private static final Map<Integer, Instant> FIRST_ARRIVAL_BY_TABLET_RANGE =
            new ConcurrentHashMap<>();

    @Test
    void readsMutationsAndCompletesAtBoundedTimestamp() throws Exception {
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
            List<ByteStringRange> partitions = coordinator.generateInitialPartitions();
            assertThat(partitions).hasSizeGreaterThanOrEqualTo(4);

            // The production service's own spelling, which no emulator or fake reproduces: the
            // client builds each partition with ByteStringRange.create(start_key_closed,
            // end_key_open), so the keyspace ends arrive as bounded bounds at the empty key rather
            // than as UNBOUNDED (#943). Everything downstream reads bound types, so this is the
            // fold that has to have happened by here. Asserting the fold and the reason for it: no
            // returned range may carry an empty key on a bounded side, and the two keyspace ends
            // must be unbounded.
            assertThat(partitions)
                    .allSatisfy(
                            partition -> {
                                if (partition.getStartBound() != BoundType.UNBOUNDED) {
                                    assertThat(partition.getStart()).isNotEqualTo(ByteString.EMPTY);
                                }
                                if (partition.getEndBound() != BoundType.UNBOUNDED) {
                                    assertThat(partition.getEnd()).isNotEqualTo(ByteString.EMPTY);
                                }
                            });
            assertThat(partitions)
                    .filteredOn(partition -> partition.getStartBound() == BoundType.UNBOUNDED)
                    .hasSize(1);
            assertThat(partitions)
                    .filteredOn(partition -> partition.getEndBound() == BoundType.UNBOUNDED)
                    .hasSize(1);
        }
        Instant start = Instant.now();
        String[] expected =
                IntStream.range(0, ROWS)
                        .mapToObj(index -> String.format("row-%04d", index))
                        .toArray(String[]::new);
        seedRows(table, expected);
        Instant end = Instant.now().plusSeconds(120);
        BigtableChangeStreamMutationDeserializationSchema deserializer =
                new BigtableChangeStreamMutationDeserializationSchema();
        BigtableChangeStreamSource<BigtableChangeStreamMutation> source =
                BigtableChangeStreamSource.<BigtableChangeStreamMutation>builder()
                        .table(table)
                        .appProfileId(APP_PROFILE)
                        .deserializer(deserializer)
                        .startPosition(StartPosition.at(start))
                        .boundedTimestamp(end)
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
        ExecutorService triggerExecutor = Executors.newFixedThreadPool(2);
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
        // A bounded run that fails to notice it is over blocks here forever: hasNext() waits on the
        // job, and JUnit's interrupt does not break that wait, so before #959 the fork outlived
        // its own deadline and kept a paid instance alive (#951 ran for over 40 minutes this
        // way). Two other ceilings now cover that too — the class @Timeout runs in a separate
        // thread, and surefire kills the fork at it.fork.timeout.seconds — and this one still
        // earns its place: it fires in five minutes rather than ten or ninety, and it says
        // which invariant broke instead of reporting a generic timeout.
        // Cancelling the job is what ends the wait — CollectResultFetcher.close() cancels through
        // the job client, and the fetch loop leaves by its terminated-job branch. That branch can
        // also surface as an IOException instead of an orderly end, which is why the catch below
        // reports the deadline rather than letting a cancellation stack stand as the diagnosis.
        // Five minutes is well over twice this case's duration when the run completes.
        CountDownLatch collectionFinished = new CountDownLatch(1);
        AtomicBoolean cancelledByDeadline = new AtomicBoolean();

        try {
            try (CloseableIterator<BigtableChangeStreamMutation> mutations =
                    environment
                            .fromSource(source, WatermarkStrategy.noWatermarks(), "change-stream")
                            .map(new ObserveConcurrencyAndFailAfterCheckpoint())
                            .returns(deserializer.getProducedType())
                            .executeAndCollect()) {
                Future<?> collectDeadline =
                        triggerExecutor.submit(
                                () -> {
                                    if (!collectionFinished.await(5, TimeUnit.MINUTES)) {
                                        cancelledByDeadline.set(true);
                                        mutations.close();
                                    }
                                    return null;
                                });
                try {
                    mutations.forEachRemaining(
                            mutation -> keys.add(mutation.getRowKey().toStringUtf8()));
                } catch (Exception e) {
                    if (!cancelledByDeadline.get()) {
                        throw e;
                    }
                    throw new AssertionError(deadlineDiagnosis(), e);
                } finally {
                    collectionFinished.countDown();
                }
                collectDeadline.get();
            }
            restartTrigger.get();
        } finally {
            triggerExecutor.shutdownNow();
        }

        assertThat(cancelledByDeadline).as(deadlineDiagnosis()).isFalse();
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

    @Test
    void readsEveryPartitionBeyondTheReadSlotsLongBeforeTheEnd() throws Exception {
        FIRST_ARRIVAL_BY_TABLET_RANGE.clear();
        TableDestination table =
                createChangeStreamTableWithSplits(
                        "change-stream-rotation", "row-0025", "row-0050", "row-0075");
        createSingleClusterAppProfile(ROTATION_APP_PROFILE);
        try (DefaultChangeStreamCoordinatorClient coordinator =
                new DefaultChangeStreamCoordinatorClient(table, ROTATION_APP_PROFILE)) {
            assertThat(coordinator.generateInitialPartitions())
                    .as("more partitions than the subtask's two read slots")
                    .hasSizeGreaterThanOrEqualTo(4);
        }
        Instant start = awaitReadableChangeStreamStart(table, ROTATION_APP_PROFILE, "marker");
        String[] expected =
                IntStream.range(0, ROWS)
                        .mapToObj(index -> String.format("row-%04d", index))
                        .toArray(String[]::new);
        seedRows(table, expected);
        Instant end = Instant.now().plusSeconds(180);
        BigtableChangeStreamMutationDeserializationSchema deserializer =
                new BigtableChangeStreamMutationDeserializationSchema();
        // One subtask with two read slots and four partitions: two partitions are more than it
        // can open at once.
        BigtableChangeStreamSource<BigtableChangeStreamMutation> source =
                BigtableChangeStreamSource.<BigtableChangeStreamMutation>builder()
                        .table(table)
                        .appProfileId(ROTATION_APP_PROFILE)
                        .maxConcurrentStreamsPerSubtask(2)
                        .deserializer(deserializer)
                        .startPosition(StartPosition.at(start))
                        .boundedTimestamp(end)
                        .build();
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.createLocalEnvironment(new Configuration());
        environment.setParallelism(1);
        List<String> keys = new ArrayList<>();
        // The same five-minute collect deadline as the first case, for the same reason: a bounded
        // run that never completes blocks hasNext() past JUnit's interrupt, and only closing the
        // iterator cancels the job.
        ExecutorService deadlineExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch collectionFinished = new CountDownLatch(1);
        AtomicBoolean cancelledByDeadline = new AtomicBoolean();
        try (CloseableIterator<BigtableChangeStreamMutation> mutations =
                environment
                        .fromSource(source, WatermarkStrategy.noWatermarks(), "change-stream")
                        .map(new RecordFirstArrival())
                        .returns(deserializer.getProducedType())
                        .executeAndCollect()) {
            Future<?> collectDeadline =
                    deadlineExecutor.submit(
                            () -> {
                                if (!collectionFinished.await(5, TimeUnit.MINUTES)) {
                                    cancelledByDeadline.set(true);
                                    mutations.close();
                                }
                                return null;
                            });
            try {
                mutations.forEachRemaining(
                        mutation -> keys.add(mutation.getRowKey().toStringUtf8()));
            } catch (Exception e) {
                if (!cancelledByDeadline.get()) {
                    throw e;
                }
                throw new AssertionError(deadlineDiagnosis(), e);
            } finally {
                collectionFinished.countDown();
            }
            collectDeadline.get();
        } finally {
            deadlineExecutor.shutdownNow();
        }

        assertThat(cancelledByDeadline).as(deadlineDiagnosis()).isFalse();
        assertThat(keys).contains(expected);
        // Before #1893 the enumerator held the two partitions the subtask had no slot for until
        // the two it read ended at the bounded timestamp, so their first rows arrived about two
        // minutes after the others, past the end; now the subtask queues them and rotates them in
        // at five-second service heartbeats. Measured from the earliest arrival, so a job that
        // starts late does not fail it.
        Instant earliest = Collections.min(FIRST_ARRIVAL_BY_TABLET_RANGE.values());
        // A job that started so late that the end came within a heartbeat or two of its first row
        // would let the held partitions arrive inside the minute below without any rotation.
        assertThat(earliest)
                .as("the job's first row arrived early enough to judge, before %s", end)
                .isBefore(end.minusSeconds(90));
        Map<Integer, Long> secondsAfterEarliest = new TreeMap<>();
        FIRST_ARRIVAL_BY_TABLET_RANGE.forEach(
                (range, arrival) ->
                        secondsAfterEarliest.put(
                                range, java.time.Duration.between(earliest, arrival).getSeconds()));
        assertThat(secondsAfterEarliest)
                .as("seconds from the earliest tablet range's first row to each range's")
                .containsOnlyKeys(0, 1, 2, 3)
                .allSatisfy((range, seconds) -> assertThat(seconds).isLessThan(60));
    }

    private static String deadlineDiagnosis() {
        return "the bounded Change Streams job did not complete at its end time and was cancelled"
                + " by the five-minute collect deadline; the enumerator never signalled"
                + " completion, so look for repeated tokenless partition restarts in the"
                + " JobManager log (#951)";
    }

    /** Records when the first row of each tablet range reaches the job. */
    private static final class RecordFirstArrival
            extends RichMapFunction<BigtableChangeStreamMutation, BigtableChangeStreamMutation> {

        private static final long serialVersionUID = 1L;

        @Override
        public BigtableChangeStreamMutation map(BigtableChangeStreamMutation mutation) {
            String key = mutation.getRowKey().toStringUtf8();
            if (key.startsWith("row-")) {
                int tabletRange = Math.min(Integer.parseInt(key.substring(4)) / 25, 3);
                FIRST_ARRIVAL_BY_TABLET_RANGE.putIfAbsent(tabletRange, Instant.now());
            }
            return mutation;
        }
    }

    /** Fails on the first mutation written after a checkpoint that included earlier mutations. */
    private static final class ObserveConcurrencyAndFailAfterCheckpoint
            extends RichMapFunction<BigtableChangeStreamMutation, BigtableChangeStreamMutation>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;
        private static final Map<Long, Integer> SEEN_AT_BARRIER = new ConcurrentHashMap<>();

        @Override
        public BigtableChangeStreamMutation map(BigtableChangeStreamMutation mutation)
                throws Exception {
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
