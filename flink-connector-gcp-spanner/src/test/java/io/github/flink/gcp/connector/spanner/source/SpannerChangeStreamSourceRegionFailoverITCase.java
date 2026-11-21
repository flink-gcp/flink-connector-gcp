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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamSplitEnumerator;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.RestoreScriptedQueryClientFactory;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.RestoreScriptedQueryClientFactory.Run;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.RestoreScriptedQueryClientFactory.Step;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;
import io.github.flink.gcp.connector.testutils.Awaits;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static io.github.flink.gcp.connector.spanner.source.changestream.reader.RestoreScriptedQueryClientFactory.INITIAL;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Restarts a job by region failover after a checkpoint (#2204).
 *
 * <p>A {@code keyBy} behind the source makes one pipelined region, so a task failure restarts every
 * task while Flink keeps the source coordinator and returns the splits assigned since the restored
 * checkpoint through {@code addSplitsBack}. A downstream operator counts a record only once a
 * checkpoint covering it completes, so a record the failover rolled back is not counted.
 */
@Timeout(300)
class SpannerChangeStreamSourceRegionFailoverITCase {

    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private static final Map<String, Set<String>> COMMITTED = new ConcurrentHashMap<>();
    private static final Map<String, AtomicBoolean> FAILED = new ConcurrentHashMap<>();

    /**
     * After checkpoint 1, child C1 runs to its finish and C2 emits ten records; the failure comes
     * before another checkpoint covers either, so both must be read again. The control takes a
     * second checkpoint after C1 finished, which covers C1's finish and C2's first records.
     */
    @ParameterizedTest(name = "checkpointAfterChildrenStart={0}")
    @ValueSource(booleans = {false, true})
    void noRecordOfAPartitionAssignedAfterTheRestoredCheckpointIsLost(
            boolean checkpointAfterChildrenStart, @InjectMiniCluster MiniCluster cluster)
            throws Exception {
        String run = UUID.randomUUID().toString();
        Run script =
                RestoreScriptedQueryClientFactory.register(run)
                        .partition(
                                INITIAL,
                                10,
                                Step.await("release"),
                                Step.children(0, "C1", "C2"),
                                Step.finish())
                        .partition("C1", 10, records(5, -1))
                        .partition("C2", 30, records(30, 6));
        COMMITTED.put(run, ConcurrentHashMap.newKeySet());
        FAILED.put(run, new AtomicBoolean());
        Supplier<String> diagnosis =
                () -> "log=" + script.log + " committed=" + new TreeSet<>(COMMITTED.get(run));

        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(100));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        // Only the checkpoints the test triggers.
        env.enableCheckpointing(Duration.ofHours(1).toMillis());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Duration.ofHours(1).toMillis());
        env.setParallelism(2);
        env.fromSource(
                        SpannerChangeStreamSource.<String>builder()
                                .database(DatabaseDestination.of("project", "instance", "database"))
                                .changeStreamName("changes")
                                .deserializer(new Sequences())
                                .startPosition(StartPosition.latest())
                                .boundedTimestamp(Instant.now().plus(Duration.ofHours(1)))
                                .coordinatorClientFactory(
                                        clientMetrics -> new NoOpCoordinatorClient())
                                .queryClientFactory(new RestoreScriptedQueryClientFactory(run))
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "spanner")
                .uid("spanner")
                .keyBy(value -> value, Types.STRING)
                .map(new FailOnce(run, "C2#10"))
                .uid("fail")
                .map(new Committing(run))
                .uid("commit")
                .sinkTo(new DiscardingSink<>());
        try (LogCapture coordinators =
                LogCapture.of(SpannerChangeStreamSplitEnumerator.class, LogCapture.Level.INFO)) {
            JobClient job = env.executeAsync("region-failover");
            awaitRunning(cluster, job);
            Awaits.await(
                    "the initial query to wait",
                    Duration.ofSeconds(30),
                    () -> script.reached.contains(INITIAL + ":release"),
                    diagnosis);
            cluster.triggerCheckpoint(job.getJobID()).get(30, TimeUnit.SECONDS);
            script.open("release");
            // C1 finishes before the failure in both variants, so the failover returns a finished
            // split; only the control covers that finish with a checkpoint.
            Awaits.await(
                    "C1 to finish",
                    Duration.ofSeconds(30),
                    () -> script.closes("C1") == 1,
                    diagnosis);
            if (checkpointAfterChildrenStart) {
                Awaits.await(
                        "C1 to finish and C2 to pause",
                        Duration.ofSeconds(30),
                        () -> script.closes("C1") == 1 && script.reached.contains("C2:cp2"),
                        diagnosis);
                cluster.triggerCheckpoint(job.getJobID()).get(30, TimeUnit.SECONDS);
            }
            script.open("cp2");
            Awaits.await(
                    "the injected failure",
                    Duration.ofSeconds(30),
                    () -> FAILED.get(run).get(),
                    diagnosis);

            // Checkpoints complete what downstream counts; the bounded job then finishes.
            Awaits.await(
                    "the job to finish",
                    Duration.ofSeconds(90),
                    () -> {
                        try {
                            cluster.triggerCheckpoint(job.getJobID()).get(10, TimeUnit.SECONDS);
                        } catch (Exception ignored) {
                            // Restarting or finishing.
                        }
                        return status(job) == JobStatus.FINISHED;
                    },
                    diagnosis);

            Set<String> expected = new TreeSet<>();
            for (int i = 1; i <= 5; i++) {
                expected.add("C1#" + i);
            }
            for (int i = 1; i <= 30; i++) {
                expected.add("C2#" + i);
            }
            assertThat(COMMITTED.get(run)).as(diagnosis.get()).containsAll(expected);
            // One coordinator initialized: the failure was a region failover, which keeps it, not a
            // global one, which would have restored it from the checkpoint.
            assertThat(coordinators.getMessages())
                    .filteredOn(m -> m.startsWith("Initialized Spanner Change Streams"))
                    .hasSize(1);
        }
        // Checkpoint 1 restored the initial query into its reader, so the coordinator took its
        // finish back and the query ran again; checkpoint 2 covered that finish.
        assertThat(script.opens(INITIAL))
                .as(diagnosis.get())
                .isEqualTo(checkpointAfterChildrenStart ? 1 : 2);
    }

    /** Data records 1..count, 1 ms apart in commit time, with a latch before {@code pauseAt}. */
    private static Step[] records(int count, int pauseAt) {
        List<Step> steps = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            if (i == pauseAt) {
                steps.add(Step.await("cp2"));
            }
            steps.add(Step.data(i));
        }
        steps.add(Step.finish());
        return steps.toArray(new Step[0]);
    }

    private static JobStatus status(JobClient job) {
        try {
            return job.getJobStatus().get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            return JobStatus.FAILED;
        }
    }

    private static void awaitRunning(MiniCluster cluster, JobClient job)
            throws InterruptedException {
        Awaits.await(
                "every task to run",
                Duration.ofSeconds(30),
                () -> {
                    if (status(job) != JobStatus.RUNNING) {
                        return false;
                    }
                    try {
                        for (AccessExecutionVertex vertex :
                                cluster.getExecutionGraph(job.getJobID())
                                        .get(5, TimeUnit.SECONDS)
                                        .getAllExecutionVertices()) {
                            ExecutionState state = vertex.getExecutionState();
                            if (state != ExecutionState.RUNNING
                                    && state != ExecutionState.FINISHED) {
                                return false;
                            }
                        }
                        return true;
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                });
    }

    private static final class NoOpCoordinatorClient
            implements SpannerChangeStreamCoordinatorClient {

        @Override
        public Duration initialize() {
            return Duration.ofDays(7);
        }

        @Override
        public void close() {}
    }

    private static final class Sequences
            implements SpannerChangeStreamDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DataChangeRecord record, Collector<String> out) {
            out.collect(record.getRecordSequence());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    private static final class FailOnce extends RichMapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        private final String run;
        private final String trigger;

        FailOnce(String run, String trigger) {
            this.run = run;
            this.trigger = trigger;
        }

        @Override
        public String map(String value) {
            if (value.equals(trigger) && FAILED.get(run).compareAndSet(false, true)) {
                throw new IllegalStateException("Injected failure at " + value);
            }
            return value;
        }
    }

    /** Counts what a completed checkpoint covers, so a rolled-back record is never counted. */
    private static final class Committing extends RichMapFunction<String, String>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;

        private final String run;
        private transient List<String> pending;
        private transient TreeMap<Long, List<String>> snapshotted;
        private transient ListState<String> state;

        Committing(String run) {
            this.run = run;
        }

        @Override
        public String map(String value) {
            pending.add(value);
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            snapshotted.put(context.getCheckpointId(), new ArrayList<>(pending));
            pending.clear();
            List<String> all = new ArrayList<>();
            snapshotted.values().forEach(all::addAll);
            state.update(all);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("uncommitted", Types.STRING));
            pending = new ArrayList<>();
            snapshotted = new TreeMap<>();
            if (context.isRestored()) {
                for (String value : state.get()) {
                    COMMITTED.get(run).add(value);
                }
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            Map<Long, List<String>> covered = snapshotted.headMap(checkpointId, true);
            covered.values().forEach(COMMITTED.get(run)::addAll);
            covered.clear();
        }
    }
}
