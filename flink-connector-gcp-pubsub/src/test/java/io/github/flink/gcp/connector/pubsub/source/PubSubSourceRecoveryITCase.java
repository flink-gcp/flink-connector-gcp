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
import org.apache.flink.api.common.functions.OpenContext;
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
import org.junit.jupiter.api.Timeout;
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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
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
 *
 * <p>Every wait here carries a {@linkplain #diagnose diagnosis} and runs on this class's own {@link
 * #RECOVERY_TIMEOUT} rather than the harness-wide {@code COLLECT_TIMEOUT}; both are consequences of
 * issue #244 and the reason for the raise is on that member.
 *
 * <p>The class timeout overrides the harness's 180 s, which applies per test method. One slow wait
 * still fits inside that (measured: a method whose first wait ran the full 120 s took 138 s end to
 * end), but each method here has three sequential bounded operations, so a run that {@link
 * #RECOVERY_TIMEOUT} rescues after a long first wait would then be killed by the class timeout
 * instead, and the raise would buy nothing. Two slow waits fit in 300 s; a third means the run is
 * failing whatever the budget. The pair (120 s wait, 300 s class) is the one {@code
 * AbstractPubSubRealGcpITCase} already uses.
 */
@Timeout(300)
class PubSubSourceRecoveryITCase extends AbstractPubSubSourceEmulatorITCase {

    /**
     * Bound on every wait in this class, in place of the harness-wide {@code COLLECT_TIMEOUT}.
     *
     * <p>Raised from that 60 s for issue #244, where the first wait of {@link
     * #aFailureAfterACompletedCheckpointRestartsTheJobWithoutLosingMessages} timed out in CI while
     * the streaming pull sat open receiving nothing. It is a hedge and not a fix: the observed
     * silence spanned two subscriber-side stream resets, and whether a third would have recovered
     * it was never measured, so this only buys the run more chances to recover — if the stall is
     * permanent it makes the failure slower rather than rarer, which is why the diagnosis is the
     * load-bearing half of that change and why the shared constant was left alone.
     */
    private static final Duration RECOVERY_TIMEOUT = Duration.ofSeconds(120);

    /**
     * Short enough that even a lost nack (redelivery then waits out the ack deadline) stays well
     * inside {@link #RECOVERY_TIMEOUT}.
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
        env.fromSource(payloadSource(first, second), WatermarkStrategy.noWatermarks(), "pubsub")
                .map(new RecordingMap(runId))
                .map(new ThrowOnceAfterCompletedCheckpoint(runId))
                .sinkTo(new DiscardingSink<>());

        JobClient job = env.executeAsync();
        Supplier<String> beforeTheFailure =
                () ->
                        diagnose(
                                job,
                                runId,
                                "before-a",
                                beforeFromFirst,
                                "before-b",
                                beforeFromSecond);
        try {
            // The failure fires on the first record processed after a completed checkpoint —
            // possibly mid-first-batch, which is fine. Waiting for both the batch and a checkpoint
            // before publishing the second batch guarantees the second batch cannot arrive early
            // enough to leave the failure unexercised.
            await(
                    "the job to consume everything published before the failure",
                    RECOVERY_TIMEOUT,
                    () ->
                            RecordingMap.records(runId).containsAll(beforeFromFirst)
                                    && RecordingMap.records(runId).containsAll(beforeFromSecond),
                    beforeTheFailure);
            await(
                    "a checkpoint to complete",
                    RECOVERY_TIMEOUT,
                    () -> ThrowOnceAfterCompletedCheckpoint.checkpointCompleted(runId),
                    beforeTheFailure);
            List<String> afterFromFirst = payloads("after-a", 5);
            List<String> afterFromSecond = payloads("after-b", 5);
            publish("recovery-a", afterFromFirst.toArray(new String[0]));
            publish("recovery-b", afterFromSecond.toArray(new String[0]));

            await(
                    "the restarted job to consume everything published around the failure",
                    RECOVERY_TIMEOUT,
                    () ->
                            RecordingMap.records(runId).containsAll(afterFromFirst)
                                    && RecordingMap.records(runId).containsAll(afterFromSecond),
                    () ->
                            diagnose(
                                    job,
                                    runId,
                                    "after-a",
                                    afterFromFirst,
                                    "after-b",
                                    afterFromSecond));
            assertThat(ThrowOnceAfterCompletedCheckpoint.fired(runId))
                    .as("the injected failure fired, so a restart actually happened")
                    .isTrue();
            // The map records a payload before the failure point, and only one subtask throws —
            // so in principle every awaited record can be recorded before the restore. This is
            // what pins the restored job as alive rather than terminally failed behind a
            // satisfied await.
            assertThat(job.getJobStatus().get(30, TimeUnit.SECONDS))
                    .as("the restored job is still running")
                    .isEqualTo(JobStatus.RUNNING);
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
        // The savepoint stops the job, so the cancel is the failure path only - but it has to be a
        // finally, not a catch: a timed-out await throws AssertionError, which `catch (Exception)`
        // does not see, and this job is unbounded with no restart strategy, so skipping the cancel
        // leaks it and its MiniCluster into every later class sharing the fork (reuseForks=true).
        boolean savepointTaken = false;
        try {
            await(
                    "the first run to consume everything published before the savepoint",
                    RECOVERY_TIMEOUT,
                    () ->
                            RecordingMap.records(firstRunId)
                                            .containsAll(tagged(beforeSavepoint, first))
                                    && RecordingMap.records(firstRunId)
                                            .containsAll(tagged(beforeSavepoint, second)),
                    () ->
                            diagnose(
                                    firstJob,
                                    firstRunId,
                                    first.getSubscription(),
                                    tagged(beforeSavepoint, first),
                                    second.getSubscription(),
                                    tagged(beforeSavepoint, second)));
            savepointPath =
                    firstJob.stopWithSavepoint(
                                    false,
                                    savepointDirectory.toUri().toString(),
                                    SavepointFormatType.CANONICAL)
                            .get(RECOVERY_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            savepointTaken = true;
        } finally {
            if (!savepointTaken) {
                cancelQuietly(firstJob);
            }
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
                    RECOVERY_TIMEOUT,
                    () ->
                            RecordingMap.records(secondRunId)
                                            .containsAll(tagged(afterSavepoint, first))
                                    && RecordingMap.records(secondRunId)
                                            .containsAll(tagged(afterSavepoint, second)),
                    () ->
                            diagnose(
                                    secondJob,
                                    secondRunId,
                                    first.getSubscription(),
                                    tagged(afterSavepoint, first),
                                    second.getSubscription(),
                                    tagged(afterSavepoint, second)));
        } finally {
            cancelQuietly(secondJob);
            RecordingMap.forget(secondRunId);
        }
    }

    private static Source<String, SubscriptionSplit, PubSubEnumeratorState> payloadSource(
            SubscriptionDestination... subscriptions) {
        return PubSubSource.<String>builder()
                .subscriptions(subscriptions)
                .deserializer(PubSubDeserializationSchema.payload(new SimpleStringSchema()))
                .emulatorEndpoint(emulatorEndpoint())
                .build();
    }

    private static Source<String, SubscriptionSplit, PubSubEnumeratorState> taggingSource(
            SubscriptionDestination... subscriptions) {
        return PubSubSource.<String>builder()
                .subscriptions(subscriptions)
                .deserializer(new SubscriptionTaggingSchema())
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
        } catch (InterruptedException e) {
            // Keep a test-timeout interruption visible to whatever runs after this cleanup.
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // Best effort only.
        }
    }

    /**
     * Reports what a timed-out wait in this class was looking at, as one greppable line.
     *
     * <p>Written for issue #244, where a CI timeout left nothing to read: the module logs nothing
     * below ERROR, and the surviving evidence — the arithmetic that put the whole minute after job
     * submission, and the client library's own keepalive tear-down lines — could say that a
     * streaming pull sat open receiving nothing, but not which subscription's, nor whether both
     * readers had started at all. Three states have to be told apart — the pipeline never started,
     * it started and nothing was delivered, one subscription fell short — and two facts separate
     * them:
     *
     * <ul>
     *   <li><b>Which subtasks opened.</b> {@link JobStatus#RUNNING} does not answer this — a job
     *       reports RUNNING once scheduling starts, while its tasks may still be deploying — so the
     *       map's own {@code open()} is what separates "the pipeline never started" from "it
     *       started and nothing was delivered". The status is reported beside it because the two
     *       disagreeing (a finished or failed job behind a still-waiting assertion) is its own
     *       answer.
     *   <li><b>Which subscription fell short.</b> The two counts are what tell one stalled
     *       streaming pull from a source that delivered nothing anywhere, which is the split issue
     *       #244 could not make.
     * </ul>
     */
    private static String diagnose(
            JobClient job,
            String runId,
            String firstLabel,
            List<String> firstExpected,
            String secondLabel,
            List<String> secondExpected) {
        Set<String> arrived = RecordingMap.records(runId);
        return "job status: "
                + jobStatus(job)
                + "; map opened on subtask(s) "
                + RecordingMap.openedSubtasks(runId)
                + "; "
                + firstLabel
                + " "
                + count(arrived, firstExpected)
                + ", "
                + secondLabel
                + " "
                + count(arrived, secondExpected)
                + " arrived.";
    }

    /** The job's status, or why it could not be read — a diagnosis may not throw. */
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

    private static String count(Set<String> arrived, List<String> expected) {
        long present = expected.stream().filter(arrived::contains).count();
        return present + "/" + expected.size();
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
     *
     * <p>It also records which subtasks reached {@code open()}, which is the only evidence {@link
     * #diagnose} has that the pipeline started at all — the map runs immediately downstream of the
     * source, so its open is a task actually deployed and running rather than a job merely
     * scheduled.
     */
    private static class RecordingMap extends RichMapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        private static final Map<String, Set<String>> RECORDS = new ConcurrentHashMap<>();
        private static final Map<String, Set<Integer>> OPENED = new ConcurrentHashMap<>();

        private final String runId;

        RecordingMap(String runId) {
            this.runId = runId;
        }

        @Override
        public void open(OpenContext openContext) {
            openedSubtasks(runId).add(getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        }

        @Override
        public String map(String value) {
            records(runId).add(value);
            return value;
        }

        static Set<String> records(String runId) {
            return RECORDS.computeIfAbsent(runId, unused -> ConcurrentHashMap.newKeySet());
        }

        /** The subtask indexes whose {@code open()} ran, in a set ordered for the message. */
        static Set<Integer> openedSubtasks(String runId) {
            return OPENED.computeIfAbsent(runId, unused -> new ConcurrentSkipListSet<>());
        }

        static void forget(String runId) {
            RECORDS.remove(runId);
            OPENED.remove(runId);
        }
    }
}
