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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.RestoreScriptedQueryClientFactory;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.RestoreScriptedQueryClientFactory.Run;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.RestoreScriptedQueryClientFactory.Step;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.SpannerChangeStreamReader;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;
import io.github.flink.gcp.connector.testutils.Awaits;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static io.github.flink.gcp.connector.spanner.source.changestream.reader.RestoreScriptedQueryClientFactory.INITIAL;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Restores a job from a checkpoint whose coordinator snapshot missed a partition's finish (#2205).
 *
 * <p>Flink snapshots the source coordinator before it injects the checkpoint's barriers, and a
 * reader's event sent between the two reaches neither snapshot. A second source whose coordinator
 * blocks in its own snapshot holds the barrier back, which widens that window deterministically
 * enough to finish partition {@code S} inside it. The restored job must still read {@code S}'s
 * child {@code C}.
 */
@Timeout(300)
class SpannerChangeStreamSourceRestoreWindowITCase {

    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private static final String RE_REPORT = "Reporting the finish of 1 restored";

    private static final Map<String, Set<String>> SEEN = new ConcurrentHashMap<>();
    private static final Map<String, Gate> GATES = new ConcurrentHashMap<>();

    @TempDir Path temporary;

    /** What happens between the coordinator's snapshot and the reader's barrier. */
    enum Window {
        /** Nothing: {@code S} finishes and {@code C} starts before the checkpoint. */
        NOTHING,
        /** {@code S}'s finish; the coordinator received its child report before. */
        FINISH,
        /** {@code S}'s child report and its finish. */
        CHILDREN_AND_FINISH
    }

    @ParameterizedTest
    @EnumSource(Window.class)
    void aRestoreReadsTheChildOfAPartitionThatFinishedWhileTheCheckpointWasTaken(
            Window window, @InjectMiniCluster MiniCluster cluster) throws Exception {
        String run = UUID.randomUUID().toString();
        Instant bound = Instant.now().plus(Duration.ofHours(1));
        Run script =
                RestoreScriptedQueryClientFactory.register(run)
                        .partition(INITIAL, 10, Step.children(0, "S"), Step.finish())
                        .partition(
                                "S",
                                20,
                                Step.data(1),
                                Step.data(2),
                                Step.data(3),
                                Step.await("report-children"),
                                Step.children(4, "C"),
                                Step.await("finish-S"),
                                Step.finish())
                        .partition(
                                "C",
                                20,
                                Step.data(1),
                                Step.data(2),
                                Step.await("hold-C"),
                                Step.data(3),
                                Step.data(4),
                                Step.data(5),
                                Step.finish());
        SEEN.put(run, ConcurrentHashMap.newKeySet());
        Gate gate = new Gate();
        GATES.put(run, gate);

        Supplier<String> diagnosis = () -> "log=" + script.log + " seen=" + SEEN.get(run);
        JobClient first = start(run, bound, null);
        awaitRunning(cluster, first);
        if (window != Window.CHILDREN_AND_FINISH) {
            script.open("report-children");
        }
        Awaits.await(
                "S to wait",
                Duration.ofSeconds(30),
                () ->
                        script.reached.contains(
                                window == Window.CHILDREN_AND_FINISH
                                        ? "S:report-children"
                                        : "S:finish-S"),
                diagnosis);

        String checkpoint;
        if (window == Window.NOTHING) {
            script.open("finish-S");
            Awaits.await(
                    "S to finish and C to wait",
                    Duration.ofSeconds(30),
                    () -> script.closes("S") == 1 && script.reached.contains("C:hold-C"),
                    diagnosis);
            checkpoint = cluster.triggerCheckpoint(first.getJobID()).get(60, TimeUnit.SECONDS);
        } else {
            gate.armed.set(true);
            CompletableFuture<String> pending = cluster.triggerCheckpoint(first.getJobID());
            assertThat(gate.entered.await(30, TimeUnit.SECONDS)).isTrue();
            // Flink snapshots the coordinators concurrently; give the Spanner one, which does
            // not block, time to finish before S reports anything. The restored reader's log
            // line below fails the case if that snapshot came late and closed the window.
            Thread.sleep(500);
            script.open("report-children");
            script.open("finish-S");
            Awaits.await(
                    "S to finish",
                    Duration.ofSeconds(30),
                    () -> script.closes("S") == 1,
                    diagnosis);
            gate.release.countDown();
            checkpoint = pending.get(60, TimeUnit.SECONDS);
        }
        first.cancel().get(30, TimeUnit.SECONDS);
        Awaits.await(
                "the first job to stop",
                Duration.ofSeconds(30),
                () -> status(first).isGloballyTerminalState());

        SEEN.get(run).clear();
        script.open("hold-C");
        int opensOfS = script.opens("S");
        try (LogCapture capture =
                LogCapture.of(SpannerChangeStreamReader.class, LogCapture.Level.INFO)) {
            JobClient second = start(run, bound, checkpoint);
            try {
                awaitRunning(cluster, second);
                Awaits.await(
                        "the restored job to read C after its hold",
                        Duration.ofSeconds(60),
                        () -> SEEN.get(run).containsAll(List.of("C#3", "C#4", "C#5")),
                        diagnosis);
            } finally {
                second.cancel().get(30, TimeUnit.SECONDS);
            }
            // The checkpoint kept S in the reader's state only if the coordinator applied its
            // finish after its own snapshot, so the line proves the window was exercised.
            if (window == Window.NOTHING) {
                assertThat(capture.getMessages()).noneMatch(m -> m.contains(RE_REPORT));
            } else {
                assertThat(capture.getMessages()).anyMatch(m -> m.contains(RE_REPORT));
            }
        }
        // S is never read again: the restored reader reports its finish rather than reopening it.
        assertThat(script.opens("S")).as(diagnosis.get()).isEqualTo(opensOfS);
        assertThat(SEEN.get(run)).noneMatch(record -> record.startsWith("S#"));
    }

    private JobClient start(String run, Instant bound, String restorePath) throws Exception {
        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        config.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                temporary.resolve("checkpoints").toUri().toString());
        if (restorePath != null) {
            config.set(StateRecoveryOptions.SAVEPOINT_PATH, restorePath);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        // Only the checkpoints the test triggers.
        env.enableCheckpointing(Duration.ofHours(1).toMillis());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Duration.ofHours(1).toMillis());
        env.setParallelism(1);
        env.fromSource(
                        SpannerChangeStreamSource.<String>builder()
                                .database(DatabaseDestination.of("project", "instance", "database"))
                                .changeStreamName("changes")
                                .deserializer(new Sequences())
                                .startPosition(StartPosition.latest())
                                .boundedTimestamp(bound)
                                .coordinatorClientFactory(
                                        clientMetrics -> new NoOpCoordinatorClient())
                                .queryClientFactory(new RestoreScriptedQueryClientFactory(run))
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "spanner")
                .uid("spanner")
                .map(new Observe(run))
                .uid("observe")
                .sinkTo(new DiscardingSink<>())
                .uid("sink");
        env.fromSource(new GateSource(run), WatermarkStrategy.noWatermarks(), "gate", Types.STRING)
                .uid("gate")
                .sinkTo(new DiscardingSink<>())
                .uid("gate-sink");
        return env.executeAsync("restore-window");
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

    private static final class Observe extends RichMapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        private final String run;

        Observe(String run) {
            this.run = run;
        }

        @Override
        public String map(String value) {
            SEEN.get(run).add(value);
            return value;
        }
    }

    private static final class Gate {
        private final AtomicBoolean armed = new AtomicBoolean();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
    }

    /** A source whose coordinator can hold one checkpoint's barriers back. */
    private static final class GateSource implements Source<String, GateSplit, Integer> {

        private static final long serialVersionUID = 1L;

        private final String run;

        GateSource(String run) {
            this.run = run;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SplitEnumerator<GateSplit, Integer> createEnumerator(
                SplitEnumeratorContext<GateSplit> context) {
            return new GateEnumerator(run);
        }

        @Override
        public SplitEnumerator<GateSplit, Integer> restoreEnumerator(
                SplitEnumeratorContext<GateSplit> context, Integer checkpoint) {
            return new GateEnumerator(run);
        }

        @Override
        public SimpleVersionedSerializer<GateSplit> getSplitSerializer() {
            return new GateSplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<Integer> getEnumeratorCheckpointSerializer() {
            return new IntSerializer();
        }

        @Override
        public SourceReader<String, GateSplit> createReader(SourceReaderContext context) {
            return new GateReader();
        }
    }

    private static final class GateSplit implements SourceSplit {

        @Override
        public String splitId() {
            return "gate";
        }
    }

    private static final class GateSplitSerializer implements SimpleVersionedSerializer<GateSplit> {

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(GateSplit split) {
            return new byte[0];
        }

        @Override
        public GateSplit deserialize(int version, byte[] serialized) {
            return new GateSplit();
        }
    }

    private static final class IntSerializer implements SimpleVersionedSerializer<Integer> {

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Integer value) {
            return ByteBuffer.allocate(4).putInt(value).array();
        }

        @Override
        public Integer deserialize(int version, byte[] serialized) {
            return ByteBuffer.wrap(serialized).getInt();
        }
    }

    private static final class GateEnumerator implements SplitEnumerator<GateSplit, Integer> {

        private final String run;

        GateEnumerator(String run) {
            this.run = run;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {}

        @Override
        public void addSplitsBack(List<GateSplit> splits, int subtaskId) {}

        @Override
        public void addReader(int subtaskId) {}

        @Override
        public Integer snapshotState(long checkpointId) throws Exception {
            Gate gate = GATES.get(run);
            if (gate != null && gate.armed.compareAndSet(true, false)) {
                gate.entered.countDown();
                assertThat(gate.release.await(60, TimeUnit.SECONDS)).isTrue();
            }
            return 0;
        }

        @Override
        public void close() {}
    }

    private static final class GateReader implements SourceReader<String, GateSplit> {

        private final CompletableFuture<Void> never = new CompletableFuture<>();

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<String> output) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        @Override
        public List<GateSplit> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return never;
        }

        @Override
        public void addSplits(List<GateSplit> splits) {}

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() {}
    }
}
