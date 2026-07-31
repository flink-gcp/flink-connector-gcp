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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MiniCluster tests of the source's recovery paths, driven through the public builder against the
 * emulator: a mid-job failure restoring from a checkpoint, and a savepoint restored at a different
 * parallelism.
 *
 * <p>Both tests assert completeness (at-least-once: a redelivery is allowed, a loss is not), never
 * the absence of duplicates and never ordering — the emulator cannot drive ordered dispatch, and
 * the harm a double-assigned subscription would actually do (two subtasks sharing one
 * subscription's streaming pull) is invisible to a completeness check, because Pub/Sub
 * load-balances a shared subscription rather than duplicating it. The deterministic pin for that
 * failure mode is {@code PubSubSourceReaderTest.checkpointsNeverCarrySplits}: the reader
 * checkpoints no splits, so a restore leaves the enumerator's recomputed plan as the only source of
 * split ownership. What these tests add is the end-to-end half — the checkpoint and savepoint state
 * round-trips through a real restore and the job keeps consuming.
 */
class PubSubSourceRecoveryITCase extends AbstractPubSubSourceEmulatorITCase {

    /**
     * Short enough that even a lost nack (redelivery then waits out the ack deadline) stays well
     * inside {@link #COLLECT_TIMEOUT}.
     */
    private static final int ACK_DEADLINE_SECONDS = 10;

    @TempDir private static Path savepointDirectory;

    /**
     * A failure injected after the first completed checkpoint restores from that checkpoint and
     * consumes the rest. Messages unacknowledged at the failure must be redelivered and nothing may
     * be lost — a #31 acceptance criterion.
     *
     * <p>Observed through {@link RecordingMap} rather than a collect iterator, for two reasons: the
     * test needs to observe progress mid-run and again after the restart, and {@code drainDistinct}
     * owns its iterator for the life of the job (its drain thread keeps consuming after the call
     * returns, so a second call on the same iterator races the first — the shape this test's first
     * version had, and the race lost it the second batch); and the map runs on the task side, so
     * the assertion covers what the restored job processed without also depending on how Flink's
     * collect client rides out a failover.
     */
    @Test
    void aFailureAfterACompletedCheckpointRestartsTheJobWithoutLosingMessages() throws Exception {
        SubscriptionDestination first =
                createTopicAndSubscription("recovery-a", ACK_DEADLINE_SECONDS);
        SubscriptionDestination second =
                createTopicAndSubscription("recovery-b", ACK_DEADLINE_SECONDS);
        List<String> beforeFromFirst = payloads("before-a", 10);
        List<String> beforeFromSecond = payloads("before-b", 10);
        publish("recovery-a", beforeFromFirst.toArray(new String[0]));
        publish("recovery-b", beforeFromSecond.toArray(new String[0]));

        String runId = UUID.randomUUID().toString();
        Configuration configuration = new Configuration();
        // One restart: the injected failure recovers, anything further fails the test.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(2);
        // Checkpointing is what acknowledges messages; the interval is short so the run is quick.
        env.enableCheckpointing(500);
        env.fromSource(dataOnlySource(first, second), WatermarkStrategy.noWatermarks(), "pubsub")
                .map(new RecordingMap(runId))
                .map(new ThrowOnceAfterCompletedCheckpoint(runId))
                .sinkTo(new DiscardingSink<>());

        JobClient job = env.executeAsync();
        try {
            // The failure fires on the first record processed after a completed checkpoint —
            // possibly mid-first-batch, which is fine. Waiting for both the batch and a checkpoint
            // before publishing the second batch guarantees the second batch cannot arrive early
            // enough to leave the failure unexercised.
            await(
                    "the job to consume everything published before the failure",
                    COLLECT_TIMEOUT,
                    () ->
                            RecordingMap.records(runId).containsAll(beforeFromFirst)
                                    && RecordingMap.records(runId).containsAll(beforeFromSecond));
            await(
                    "a checkpoint to complete",
                    COLLECT_TIMEOUT,
                    () -> ThrowOnceAfterCompletedCheckpoint.checkpointCompleted(runId));
            List<String> afterFromFirst = payloads("after-a", 5);
            List<String> afterFromSecond = payloads("after-b", 5);
            publish("recovery-a", afterFromFirst.toArray(new String[0]));
            publish("recovery-b", afterFromSecond.toArray(new String[0]));

            await(
                    "the restarted job to consume everything published around the failure",
                    COLLECT_TIMEOUT,
                    () ->
                            RecordingMap.records(runId).containsAll(afterFromFirst)
                                    && RecordingMap.records(runId).containsAll(afterFromSecond));
            assertThat(ThrowOnceAfterCompletedCheckpoint.fired(runId))
                    .as("the injected failure fired, so a restart actually happened")
                    .isTrue();
        } finally {
            cancelQuietly(job);
            ThrowOnceAfterCompletedCheckpoint.forget(runId);
            RecordingMap.forget(runId);
        }
    }

    @Test
    void aSavepointTakenAtParallelismOneRestoresAtParallelismTwo() throws Exception {
        savepointRestoreAtChangedParallelism("rescale-up", 1, 2);
    }

    /**
     * The direction where reader-checkpointed splits would have collided with the recomputed plan:
     * subtask 0 exists in both runs, so splits it snapshotted at parallelism 2 would have been
     * restored beside the enumerator's fresh parallelism-1 assignment of the same subscriptions.
     */
    @Test
    void aSavepointTakenAtParallelismTwoRestoresAtParallelismOne() throws Exception {
        savepointRestoreAtChangedParallelism("rescale-down", 2, 1);
    }

    /**
     * Consumes at one parallelism, stops with a savepoint, and restores at another: the restore
     * must succeed (state compatibility across the rescale) and everything published after the
     * savepoint must be consumed from every subscription (the recomputed split plan covers them
     * all).
     */
    private static void savepointRestoreAtChangedParallelism(
            String namePrefix, int fromParallelism, int toParallelism) throws Exception {
        SubscriptionDestination first =
                createTopicAndSubscription(namePrefix + "-a", ACK_DEADLINE_SECONDS);
        SubscriptionDestination second =
                createTopicAndSubscription(namePrefix + "-b", ACK_DEADLINE_SECONDS);
        // The same payloads go to both topics, and the schema tags each record with the
        // subscription that delivered it — so the assertion below can require every payload from
        // *each* subscription, which is what proves the recomputed split plan left none of them
        // unassigned after the rescale.
        List<String> beforeSavepoint = payloads(namePrefix + "-before", 10);
        publish(namePrefix + "-a", beforeSavepoint.toArray(new String[0]));
        publish(namePrefix + "-b", beforeSavepoint.toArray(new String[0]));

        String firstRunId = UUID.randomUUID().toString();
        String savepointPath;
        JobClient firstJob =
                runRecording(firstRunId, fromParallelism, null, taggingSource(first, second));
        try {
            await(
                    "the first run to consume everything published before the savepoint",
                    COLLECT_TIMEOUT,
                    () ->
                            RecordingMap.records(firstRunId)
                                            .containsAll(tagged(beforeSavepoint, first))
                                    && RecordingMap.records(firstRunId)
                                            .containsAll(tagged(beforeSavepoint, second)));
            savepointPath =
                    firstJob.stopWithSavepoint(
                                    false,
                                    savepointDirectory.toUri().toString(),
                                    SavepointFormatType.CANONICAL)
                            .get(COLLECT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            cancelQuietly(firstJob);
            throw e;
        } finally {
            RecordingMap.forget(firstRunId);
        }

        List<String> afterSavepoint = payloads(namePrefix + "-after", 10);
        publish(namePrefix + "-a", afterSavepoint.toArray(new String[0]));
        publish(namePrefix + "-b", afterSavepoint.toArray(new String[0]));

        String secondRunId = UUID.randomUUID().toString();
        JobClient secondJob =
                runRecording(
                        secondRunId, toParallelism, savepointPath, taggingSource(first, second));
        try {
            await(
                    "the restored run to consume everything published after the savepoint, from"
                            + " both subscriptions",
                    COLLECT_TIMEOUT,
                    () ->
                            RecordingMap.records(secondRunId)
                                            .containsAll(tagged(afterSavepoint, first))
                                    && RecordingMap.records(secondRunId)
                                            .containsAll(tagged(afterSavepoint, second)));
        } finally {
            cancelQuietly(secondJob);
            RecordingMap.forget(secondRunId);
        }
    }

    private static Source<String, SubscriptionSplit, PubSubEnumeratorState> dataOnlySource(
            SubscriptionDestination... subscriptions) {
        return PubSubSource.<String>builder()
                .subscriptions(subscriptions)
                .deserializationSchema(
                        PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                .emulatorEndpoint(emulatorEndpoint())
                .build();
    }

    private static Source<String, SubscriptionSplit, PubSubEnumeratorState> taggingSource(
            SubscriptionDestination... subscriptions) {
        return PubSubSource.<String>builder()
                .subscriptions(subscriptions)
                .deserializationSchema(new SubscriptionTaggingSchema())
                .emulatorEndpoint(emulatorEndpoint())
                .build();
    }

    /** Submits the source into a recording pipeline and returns the job for later control. */
    private static JobClient runRecording(
            String runId,
            int parallelism,
            String savepointPath,
            Source<String, SubscriptionSplit, PubSubEnumeratorState> source)
            throws Exception {
        Configuration configuration = new Configuration();
        // Without this a permanent failure would be retried forever instead of failing the test.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        if (savepointPath != null) {
            configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(parallelism);
        // Checkpointing is what acknowledges messages; the interval is short so the run is quick.
        env.enableCheckpointing(500);
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "pubsub")
                .map(new RecordingMap(runId))
                .sinkTo(new DiscardingSink<>());
        return env.executeAsync();
    }

    /**
     * Cancels without letting the cancellation fail the test: in a finally block a job that already
     * ended would otherwise throw here and replace the assertion error actually being reported, and
     * skip the static-registry cleanup after it.
     */
    private static void cancelQuietly(JobClient job) {
        try {
            job.cancel().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Best effort only.
        }
    }

    private static List<String> payloads(String prefix, int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> prefix + "-" + i)
                .collect(Collectors.toList());
    }

    /** The records {@link SubscriptionTaggingSchema} emits for these payloads. */
    private static List<String> tagged(List<String> payloads, SubscriptionDestination from) {
        List<String> tagged = new ArrayList<>(payloads.size());
        for (String payload : payloads) {
            tagged.add(from.getSubscription() + "|" + payload);
        }
        return tagged;
    }

    /**
     * Emits {@code subscriptionId|payload} so an assertion can tell which subscription delivered a
     * record — the rescale tests publish the same payloads to both on purpose.
     */
    private static class SubscriptionTaggingSchema implements PubSubDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        private final SimpleStringSchema payloads = new SimpleStringSchema();

        @Override
        public void deserialize(
                PubsubMessage message, SubscriptionDestination subscription, Collector<String> out)
                throws IOException {
            out.collect(
                    subscription.getSubscription()
                            + "|"
                            + payloads.deserialize(message.getData().toByteArray()));
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return payloads.getProducedType();
        }
    }

    /**
     * Throws once, on the first record processed after the first completed checkpoint. Both the
     * "fired" flag and the "checkpoint completed" flag are static and keyed by a per-test run id:
     * the function is re-deserialized on restart, so instance state would forget having thrown and
     * fail the job again, while the MiniCluster shares the JVM, so statics reach across attempts
     * and into the test.
     */
    private static class ThrowOnceAfterCompletedCheckpoint extends RichMapFunction<String, String>
            implements CheckpointListener {

        private static final long serialVersionUID = 1L;

        private static final Set<String> COMPLETED = ConcurrentHashMap.newKeySet();
        private static final Set<String> FIRED = ConcurrentHashMap.newKeySet();

        private final String runId;

        ThrowOnceAfterCompletedCheckpoint(String runId) {
            this.runId = runId;
        }

        @Override
        public String map(String value) {
            if (COMPLETED.contains(runId) && FIRED.add(runId)) {
                throw new IllegalStateException(
                        "Injected failure after the first completed checkpoint (run "
                                + runId
                                + ").");
            }
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            COMPLETED.add(runId);
        }

        static boolean checkpointCompleted(String runId) {
            return COMPLETED.contains(runId);
        }

        static boolean fired(String runId) {
            return FIRED.contains(runId);
        }

        static void forget(String runId) {
            COMPLETED.remove(runId);
            FIRED.remove(runId);
        }
    }

    /**
     * Records every payload it sees into a static registry keyed by a per-test run id, for jobs
     * driven by {@code executeAsync} whose output has no collect iterator to drain. Static for the
     * same reason as {@link ThrowOnceAfterCompletedCheckpoint}'s flags: the MiniCluster shares the
     * JVM with the test.
     */
    private static class RecordingMap extends RichMapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        private static final Map<String, Set<String>> RECORDS = new ConcurrentHashMap<>();

        private final String runId;

        RecordingMap(String runId) {
            this.runId = runId;
        }

        @Override
        public String map(String value) {
            records(runId).add(value);
            return value;
        }

        static Set<String> records(String runId) {
            return RECORDS.computeIfAbsent(runId, unused -> ConcurrentHashMap.newKeySet());
        }

        static void forget(String runId) {
            RECORDS.remove(runId);
        }
    }
}
