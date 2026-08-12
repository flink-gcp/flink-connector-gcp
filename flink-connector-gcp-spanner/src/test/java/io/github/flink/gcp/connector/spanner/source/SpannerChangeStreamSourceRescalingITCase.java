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

package io.github.flink.gcp.connector.spanner.source;

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

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.ScriptedSpannerChangeStreamQueryClientFactory;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;
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

/** Savepoint rescaling coverage for the Change Streams partition-query topology. */
@Timeout(180)
class SpannerChangeStreamSourceRescalingITCase {

    private static final int PARTITIONS = 6;
    private static final Duration WAIT = Duration.ofSeconds(30);

    @TempDir private static Path savepointDirectory;

    @Test
    void rescalingRedistributesQueriesAndPreservesQueuedPartitionProgress() throws Exception {
        String firstRun = UUID.randomUUID().toString();
        JobClient firstJob = run(firstRun, 1, 6, null);
        String firstSavepoint = savepointAfter(firstJob, firstRun, PARTITIONS);
        Map<String, Long> firstPositions = RecordingMap.positions(firstRun);

        String scaleOutRun = UUID.randomUUID().toString();
        JobClient scaleOutJob = run(scaleOutRun, 3, 2, firstSavepoint);
        String scaleOutSavepoint = savepointAfter(scaleOutJob, scaleOutRun, PARTITIONS);
        Map<String, Long> scaleOutPositions = RecordingMap.positions(scaleOutRun);
        assertAdvancedByOne(firstPositions, scaleOutPositions);
        assertThat(RecordingMap.subtaskCounts(scaleOutRun))
                .containsExactlyInAnyOrderEntriesOf(Map.of(0, 2L, 1, 2L, 2, 2L));

        String constrainedRun = UUID.randomUUID().toString();
        JobClient constrainedJob = run(constrainedRun, 1, 2, scaleOutSavepoint);
        String constrainedSavepoint = savepointAfter(constrainedJob, constrainedRun, 2);
        Map<String, Long> constrainedPositions = RecordingMap.positions(constrainedRun);
        assertThat(constrainedPositions).hasSize(2);
        for (Map.Entry<String, Long> entry : constrainedPositions.entrySet()) {
            assertThat(entry.getValue()).isEqualTo(scaleOutPositions.get(entry.getKey()) + 1);
        }

        String expandedRun = UUID.randomUUID().toString();
        JobClient expandedJob = run(expandedRun, 1, 6, constrainedSavepoint);
        try {
            awaitRecords(expandedJob, expandedRun, PARTITIONS);
            Map<String, Long> expandedPositions = RecordingMap.positions(expandedRun);
            assertThat(expandedPositions).hasSize(PARTITIONS);
            for (Map.Entry<String, Long> entry : expandedPositions.entrySet()) {
                long expectedAdvance = constrainedPositions.containsKey(entry.getKey()) ? 2 : 1;
                assertThat(entry.getValue())
                        .as("restored position for %s", entry.getKey())
                        .isEqualTo(scaleOutPositions.get(entry.getKey()) + expectedAdvance);
            }
        } finally {
            cancelQuietly(expandedJob);
            RecordingMap.forget(firstRun);
            RecordingMap.forget(scaleOutRun);
            RecordingMap.forget(constrainedRun);
            RecordingMap.forget(expandedRun);
        }
    }

    private static JobClient run(
            String runId, int parallelism, int maximumQueries, String savepointPath)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        if (savepointPath != null) {
            configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(parallelism);
        env.enableCheckpointing(100);
        env.fromSource(
                        source(maximumQueries),
                        WatermarkStrategy.noWatermarks(),
                        "spanner-change-stream")
                .uid("spanner-change-stream")
                .map(new RecordingMap(runId))
                .uid("recording-map")
                .sinkTo(new DiscardingSink<>())
                .uid("discarding-sink");
        return env.executeAsync();
    }

    private static SpannerChangeStreamSource<String> source(int maximumQueries) {
        return SpannerChangeStreamSource.<String>builder()
                .database(SpannerDatabase.of("project", "instance", "database"))
                .changeStreamName("changes")
                .deserializer(new SequenceDeserializer())
                .startPosition(StartPosition.latest())
                .maxConcurrentQueriesPerSubtask(maximumQueries)
                .coordinatorClientFactory(NoOpCoordinatorClient::new)
                .queryClientFactory(new ScriptedSpannerChangeStreamQueryClientFactory(PARTITIONS))
                .build();
    }

    private static String savepointAfter(JobClient job, String runId, int expected)
            throws Exception {
        boolean saved = false;
        try {
            awaitRecords(job, runId, expected);
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

    private static void awaitRecords(JobClient job, String runId, int expected)
            throws InterruptedException {
        await(
                expected + " scripted partitions in run " + runId,
                WAIT,
                () -> recordsArrivedOrJobHealthy(job, runId, expected),
                () ->
                        "job status: "
                                + jobStatus(job)
                                + "; observations: "
                                + RecordingMap.observations(runId));
    }

    private static boolean recordsArrivedOrJobHealthy(JobClient job, String runId, int expected) {
        if (RecordingMap.positions(runId).size() == expected) {
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

    private static final class NoOpCoordinatorClient
            implements SpannerChangeStreamCoordinatorClient {

        @Override
        public void validatePartitionMode() {}

        @Override
        public Duration retention() {
            return Duration.ofDays(7);
        }

        @Override
        public void close() {}
    }

    private static final class SequenceDeserializer
            implements SpannerChangeStreamDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String deserialize(DataChangeRecord record) {
            return record.getRecordSequence();
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
            observations(runId).add(new Observation(value, subtask));
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
