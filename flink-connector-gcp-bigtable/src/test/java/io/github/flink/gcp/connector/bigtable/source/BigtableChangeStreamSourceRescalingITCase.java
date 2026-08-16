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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.ChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ScriptedChangeStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static org.assertj.core.api.Assertions.assertThat;

/** Savepoint rescaling coverage for bounded asynchronous Change Streams partition reads. */
@Timeout(180)
class BigtableChangeStreamSourceRescalingITCase {

    private static final int PARTITIONS = 6;
    private static final Duration WAIT = Duration.ofSeconds(30);

    @TempDir private static Path savepointDirectory;

    @Test
    void rescalingRedistributesReadsAndRotatesEveryConstrainedPartition() throws Exception {
        String initialRun = UUID.randomUUID().toString();
        String scaleOutRun = UUID.randomUUID().toString();
        String scaleInRun = UUID.randomUUID().toString();
        JobClient scaleInJob = null;
        try {
            JobClient initialJob = run(initialRun, 1, 6, null);
            String initialSavepoint = savepointAfter(initialJob, initialRun);
            Map<String, Long> initialPositions = RecordingMap.positions(initialRun);
            assertThat(ScriptedChangeStreamOpener.peaks(initialRun)).containsExactly(6);

            JobClient scaleOutJob = run(scaleOutRun, 3, 2, initialSavepoint);
            String scaleOutSavepoint = savepointAfter(scaleOutJob, scaleOutRun);
            Map<String, Long> scaleOutPositions = RecordingMap.positions(scaleOutRun);
            assertAdvancedByOne(initialPositions, scaleOutPositions);
            assertThat(RecordingMap.subtaskCounts(scaleOutRun))
                    .containsExactlyInAnyOrderEntriesOf(Map.of(0, 2L, 1, 2L, 2, 2L));
            assertThat(ScriptedChangeStreamOpener.peaks(scaleOutRun))
                    .containsExactlyInAnyOrder(2, 2, 2);

            scaleInJob = run(scaleInRun, 1, 2, scaleOutSavepoint);
            awaitRecords(scaleInJob, scaleInRun);
            Map<String, Long> scaleInPositions = RecordingMap.positions(scaleInRun);
            assertAdvancedByOne(scaleOutPositions, scaleInPositions);
            assertThat(RecordingMap.subtaskCounts(scaleInRun)).containsOnlyKeys(0);
            assertThat(ScriptedChangeStreamOpener.peaks(scaleInRun)).containsExactly(2);
        } finally {
            if (scaleInJob != null) {
                cancelQuietly(scaleInJob);
            }
            RecordingMap.forget(initialRun);
            RecordingMap.forget(scaleOutRun);
            RecordingMap.forget(scaleInRun);
            ScriptedChangeStreamOpener.forget(initialRun);
            ScriptedChangeStreamOpener.forget(scaleOutRun);
            ScriptedChangeStreamOpener.forget(scaleInRun);
        }
    }

    private static JobClient run(
            String runId, int parallelism, int maximumStreams, String savepointPath)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        if (savepointPath != null) {
            configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
        }
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        environment.setParallelism(parallelism);
        environment.enableCheckpointing(100);
        environment
                .fromSource(
                        source(runId, maximumStreams),
                        WatermarkStrategy.noWatermarks(),
                        "bigtable-change-stream")
                .uid("bigtable-change-stream")
                .map(new RecordingMap(runId))
                .uid("recording-map")
                .sinkTo(new DiscardingSink<>())
                .uid("discarding-sink");
        return environment.executeAsync();
    }

    private static BigtableChangeStreamSource<String> source(String runId, int maximumStreams) {
        return BigtableChangeStreamSource.<String>builder()
                .table(TableDestination.of("project", "instance", "table"))
                .appProfileId("single-cluster")
                .deserializer(new SequenceDeserializer())
                .startPosition(StartPosition.latest())
                .maxConcurrentStreamsPerSubtask(maximumStreams)
                .opener(new ScriptedChangeStreamOpener(runId))
                .restoreResolver((split, ignored) -> split)
                .coordinatorClient(new ScriptedCoordinatorClient())
                .build();
    }

    private static String savepointAfter(JobClient job, String runId) throws Exception {
        boolean saved = false;
        try {
            awaitRecords(job, runId);
            String path =
                    job.stopWithSavepoint(
                                    false,
                                    savepointDirectory.toUri().toString(),
                                    SavepointFormatType.CANONICAL)
                            .get(WAIT.toSeconds(), TimeUnit.SECONDS);
            saved = true;
            return path;
        } finally {
            if (!saved) {
                cancelQuietly(job);
            }
        }
    }

    private static void awaitRecords(JobClient job, String runId) throws InterruptedException {
        await(
                PARTITIONS + " scripted partitions in run " + runId,
                WAIT,
                () -> recordsArrivedOrJobHealthy(job, runId),
                () ->
                        "job status: "
                                + jobStatus(job)
                                + "; observations: "
                                + RecordingMap.observations(runId)
                                + "; RPC events: "
                                + ScriptedChangeStreamOpener.events(runId));
    }

    private static boolean recordsArrivedOrJobHealthy(JobClient job, String runId) {
        if (RecordingMap.positions(runId).size() == PARTITIONS) {
            return true;
        }
        JobStatus status;
        try {
            status = job.getJobStatus().join();
        } catch (CompletionException e) {
            throw new AssertionError(
                    "Could not read job status while awaiting records.", e.getCause());
        }
        if (!status.isGloballyTerminalState()) {
            return false;
        }
        try {
            job.getJobExecutionResult().join();
        } catch (CompletionException e) {
            throw new AssertionError(
                    "Job terminated with " + status + " while awaiting records.", e.getCause());
        }
        throw new AssertionError("Job terminated with " + status + " before all records arrived.");
    }

    private static void assertAdvancedByOne(Map<String, Long> earlier, Map<String, Long> later) {
        assertThat(later).hasSameSizeAs(earlier);
        for (Map.Entry<String, Long> entry : later.entrySet()) {
            assertThat(entry.getValue())
                    .as("restored position for %s", entry.getKey())
                    .isEqualTo(earlier.get(entry.getKey()) + 1);
        }
    }

    private static String jobStatus(JobClient job) {
        try {
            return job.getJobStatus().get(10, TimeUnit.SECONDS).toString();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "unreadable (interrupted)";
        } catch (Exception e) {
            return "unreadable (" + e + ")";
        }
    }

    private static void cancelQuietly(JobClient job) {
        try {
            job.cancel().get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
            // Best effort only.
        }
    }

    private static final class ScriptedCoordinatorClient implements ChangeStreamCoordinatorClient {

        @Override
        public void validateSingleClusterAppProfile() {}

        @Override
        public Duration retention() {
            return Duration.ofDays(7);
        }

        @Override
        public List<ByteStringRange> generateInitialPartitions() {
            List<ByteStringRange> partitions = new ArrayList<>();
            for (int index = 0; index < PARTITIONS; index++) {
                String start = index == 0 ? "" : Integer.toString(index);
                String end = index == PARTITIONS - 1 ? "" : Integer.toString(index + 1);
                partitions.add(ByteStringRange.create(start, end));
            }
            return partitions;
        }

        @Override
        public void close() {}
    }

    private static final class SequenceDeserializer
            implements BigtableChangeStreamDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(ChangeStreamMutation mutation, Collector<String> out) {
            out.collect(mutation.getToken());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    private static final class RecordingMap extends RichMapFunction<String, String> {

        private static final long serialVersionUID = 1L;
        private static final Map<String, List<Observation>> OBSERVATIONS =
                new ConcurrentHashMap<>();

        private final String runId;
        private transient int subtask;

        private RecordingMap(String runId) {
            this.runId = runId;
        }

        @Override
        public void open(OpenContext openContext) {
            subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        }

        @Override
        public String map(String value) {
            List<Observation> current = observations(runId);
            String partition = value.split("\\|", 2)[0];
            synchronized (current) {
                boolean alreadyObserved =
                        current.stream()
                                .map(observation -> observation.value.split("\\|", 2)[0])
                                .anyMatch(partition::equals);
                if (!alreadyObserved) {
                    current.add(new Observation(value, subtask));
                }
            }
            return value;
        }

        private static List<Observation> observations(String runId) {
            return OBSERVATIONS.computeIfAbsent(
                    runId, unused -> Collections.synchronizedList(new ArrayList<>()));
        }

        private static Map<String, Long> positions(String runId) {
            Map<String, Long> positions = new HashMap<>();
            synchronized (observations(runId)) {
                for (Observation observation : observations(runId)) {
                    String[] fields = observation.value.split("\\|", 2);
                    positions.put(fields[0], Long.parseLong(fields[1]));
                }
            }
            return positions;
        }

        private static Map<Integer, Long> subtaskCounts(String runId) {
            synchronized (observations(runId)) {
                return observations(runId).stream()
                        .collect(
                                Collectors.groupingBy(
                                        observation -> observation.subtask, Collectors.counting()));
            }
        }

        private static void forget(String runId) {
            OBSERVATIONS.remove(runId);
        }
    }

    private static final class Observation {

        private final String value;
        private final int subtask;

        private Observation(String value, int subtask) {
            this.value = value;
            this.subtask = subtask;
        }

        @Override
        public String toString() {
            return value + "@" + subtask;
        }
    }
}
