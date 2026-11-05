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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A source sized by the documented rule drains a backlog larger than its flow-control window behind
 * a slow, checkpoint-stalling downstream, and keeps checkpointing (#2020).
 *
 * <p>The downstream is shaped after a {@code FILE_LOADS} BigQuery sink, where the observation came
 * from: it is slower than Pub/Sub delivers at all times, and it holds its task thread for a while
 * at every checkpoint, as {@code prepareCommit()} does while it finishes staged files. Both make
 * the subscriber buffer fill until the client's flow control stops it, which before #2020 crossed
 * the fixed 10000-message budget whenever the flow-control limit was above it, failed the job, and
 * failed it again on every restart, since nothing had been acknowledged.
 *
 * <p>The numbers make that crossing reachable here with a wide margin. {@code maxRecordsPerFetch}
 * is small so Flink's fetcher queues, which sit outside the budget, hold a few hundred records
 * rather than thousands, and the map consumes about 2500 records a second, so a 1.5-second
 * checkpoint cycle leaves a few thousand emitted records unacknowledged. The subscriber buffer
 * therefore approaches {@link #FLOW_CONTROL_MESSAGES} minus those, more than twice the former
 * 10000-message cap. The test asserts liveness, not the buffer's peak, so that margin is what lets
 * it fail on a regression to a fixed cap of that size. The backlog is larger than the window, so
 * consuming all of it needs acknowledgements from completed checkpoints to return flow-control
 * permits: the run proves the intake resumes rather than only that it starts.
 *
 * <p>With the budget derived from flow control (twice the window for the one subscriber), the same
 * run completes. Verified to fail before the fix by building the reader's budget with the former
 * fixed defaults, which failed the job through the coordinator with the #2020 message.
 */
class PubSubSourceBacklogDrainITCase extends AbstractPubSubSourceEmulatorITCase {

    private static final int ACK_DEADLINE_SECONDS = 60;
    private static final long FLOW_CONTROL_MESSAGES = 30_000;
    private static final int BACKLOG = 40_000;
    private static final int MAX_RECORDS_PER_FETCH = 100;
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofSeconds(1);
    private static final Duration CHECKPOINT_STALL = Duration.ofMillis(500);
    private static final long NANOS_PER_RECORD = TimeUnit.MICROSECONDS.toNanos(400);
    private static final int MINIMUM_CHECKPOINTS = 3;
    private static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(150);

    @Test
    void aBacklogLargerThanTheFlowControlWindowDrainsBehindASlowDownstream() throws Exception {
        SubscriptionDestination subscription =
                createTopicAndSubscription("backlog-drain", ACK_DEADLINE_SECONDS);
        publish(
                "backlog-drain",
                IntStream.range(0, BACKLOG).mapToObj(i -> "m-" + i).toArray(String[]::new));

        String runId = UUID.randomUUID().toString();
        Configuration configuration = new Configuration();
        // A coordinator failure must fail the test rather than restart into the same backlog.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(CHECKPOINT_INTERVAL.toMillis());
        env.fromSource(
                        PubSubSource.<String>builder()
                                .subscription(subscription)
                                .deserializer(
                                        PubSubDeserializationSchema.payload(
                                                new SimpleStringSchema()))
                                .subscriberOptions(
                                        PubSubSubscriberOptions.builder()
                                                .flowControlMaxOutstandingElementCount(
                                                        FLOW_CONTROL_MESSAGES)
                                                .maxRecordsPerFetch(MAX_RECORDS_PER_FETCH)
                                                .build())
                                .emulatorEndpoint(emulatorEndpoint())
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "pubsub")
                .map(new SlowStallingMap(runId))
                .sinkTo(new DiscardingSink<>());

        JobClient job = env.executeAsync();
        try {
            // Ends early when the job does: a coordinator failure is the outcome under test, and
            // waiting out the timeout after it would only hide the message.
            await(
                    "the whole backlog to drain with checkpoints completing",
                    DRAIN_TIMEOUT,
                    () -> drained(runId) || job.getJobExecutionResult().isDone(),
                    () -> diagnose(job, runId));
            assertThat(job.getJobExecutionResult().isDone()).as(diagnose(job, runId)).isFalse();
            assertThat(drained(runId)).as(diagnose(job, runId)).isTrue();
            assertThat(job.getJobStatus().get(10, TimeUnit.SECONDS)).isEqualTo(JobStatus.RUNNING);
        } finally {
            try {
                job.cancel().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                // Best effort: a job that already failed has nothing to cancel.
            }
            SlowStallingMap.forget(runId);
        }
    }

    private static boolean drained(String runId) {
        return SlowStallingMap.records(runId).size() >= BACKLOG
                && SlowStallingMap.checkpoints(runId).get() >= MINIMUM_CHECKPOINTS;
    }

    private static String diagnose(JobClient job, String runId) {
        String status;
        try {
            status = job.getJobStatus().get(10, TimeUnit.SECONDS).toString();
        } catch (Exception e) {
            status = "unreadable (" + e + ")";
        }
        String failure;
        try {
            failure = String.valueOf(job.getJobExecutionResult().getNow(null));
        } catch (Exception e) {
            // The coordinator's message is the cause chain's innermost link.
            Throwable cause = e;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            failure = String.valueOf(cause);
        }
        return "job status: "
                + status
                + "; "
                + SlowStallingMap.records(runId).size()
                + "/"
                + BACKLOG
                + " distinct records, "
                + SlowStallingMap.checkpoints(runId).get()
                + " completed checkpoints; result: "
                + failure;
    }

    /**
     * Slower than delivery at all times, and holding the task thread for {@link #CHECKPOINT_STALL}
     * at every snapshot. Its registries are static and keyed by a per-test run id, because the
     * MiniCluster shares the JVM with the test.
     */
    private static class SlowStallingMap
            implements MapFunction<String, String>, CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;

        private static final Map<String, Set<String>> RECORDS = new ConcurrentHashMap<>();
        private static final Map<String, AtomicInteger> CHECKPOINTS = new ConcurrentHashMap<>();

        private final String runId;

        SlowStallingMap(String runId) {
            this.runId = runId;
        }

        @Override
        public String map(String value) {
            LockSupport.parkNanos(NANOS_PER_RECORD);
            records(runId).add(value);
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            Thread.sleep(CHECKPOINT_STALL.toMillis());
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            checkpoints(runId).incrementAndGet();
        }

        static Set<String> records(String runId) {
            return RECORDS.computeIfAbsent(runId, unused -> ConcurrentHashMap.newKeySet());
        }

        static AtomicInteger checkpoints(String runId) {
            return CHECKPOINTS.computeIfAbsent(runId, unused -> new AtomicInteger());
        }

        static void forget(String runId) {
            RECORDS.remove(runId);
            CHECKPOINTS.remove(runId);
        }
    }
}
