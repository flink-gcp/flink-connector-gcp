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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * MiniCluster tests of restore deduplication (#2049) against the emulator: a restore from a
 * checkpoint whose completion never reached the readers, at a changed parallelism.
 *
 * <p>The lost notification is reproduced deterministically with an ordinary savepoint. Flink sends
 * no completion notification to tasks for a savepoint that does not stop the job ({@code
 * CheckpointCoordinator} notifies only checkpoints and synchronous savepoints), so the messages a
 * savepoint binds stay unacknowledged; cancelling the job then nacks them, exactly as a
 * cancellation racing a checkpoint's notification does. Restoring the savepoint is the restore the
 * AdaptiveScheduler performs after that race.
 *
 * <p>The control run, with deduplication off, shows that the harness does redeliver those messages
 * into the restored job — without it, the deduplicated runs' "emitted once" could pass on an
 * emulator that simply never redelivered. Each deduplicated run ends by pulling the subscription
 * after the job is cancelled, to catch a covered message that was neither suppressed nor emitted.
 * That check assumes the emulator returns the first run's nacked messages before the fresh ones
 * nacked later, since it pulls only as many messages as the restored run left behind; the emitter's
 * unit test is what pins that a suppressed message is acknowledged.
 */
@Timeout(300)
class PubSubSourceRestoreDeduplicationITCase extends AbstractPubSubSourceEmulatorITCase {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final int ACK_DEADLINE_SECONDS = 10;

    /** Long enough that no periodic checkpoint acknowledges anything during a test. */
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofMinutes(30);

    @TempDir private static Path savepointDirectory;

    @Test
    void withoutDeduplicationTheRestoredJobEmitsTheCoveredMessagesAgain() throws Exception {
        Run run = restoreAfterLostNotification("dedup-off", false, 1, 1, 1);
        try {
            await(
                    "every message the savepoint covered to be emitted a second time",
                    TIMEOUT,
                    () -> run.covered.stream().allMatch(payload -> run.count(payload) == 2),
                    run::describe);
        } finally {
            run.finish();
        }
    }

    /**
     * One subscription read by two subtasks after the restore. Pub/Sub may redeliver to either, so
     * both are sent the whole ledger, but this test does not pin that: measured, it still passes
     * when the enumerator forwards the ledger to subtask 0 alone, because the emulator delivered
     * every redelivery to one stream. {@code PubSubSplitEnumeratorLedgerTest} pins the forwarding
     * to every reader of a subscription.
     */
    @Test
    void aScaleOutRestoreSuppressesEveryCoveredRedelivery() throws Exception {
        Run run = restoreAfterLostNotification("dedup-out", true, 1, 2, 1);

        run.assertEachCoveredMessageEmittedOnceAndAcknowledged();
    }

    @Test
    void aScaleInRestoreSuppressesEveryCoveredRedelivery() throws Exception {
        // Two subtasks' ledgers land on the single restored subtask.
        Run run = restoreAfterLostNotification("dedup-in", true, 2, 1, 2);

        run.assertEachCoveredMessageEmittedOnceAndAcknowledged();
    }

    /**
     * Consumes a batch at one parallelism, takes a savepoint (which binds the batch but is never
     * notified), cancels, publishes a second batch, and restores the savepoint at another
     * parallelism until the second batch has been consumed.
     */
    private static Run restoreAfterLostNotification(
            String prefix,
            boolean deduplicate,
            int fromParallelism,
            int toParallelism,
            int subscriptionCount)
            throws Exception {
        List<SubscriptionDestination> subscriptions = new ArrayList<>();
        for (int i = 0; i < subscriptionCount; i++) {
            subscriptions.add(createTopicAndSubscription(prefix + "-" + i, ACK_DEADLINE_SECONDS));
        }
        List<String> covered = new ArrayList<>();
        for (int i = 0; i < subscriptionCount; i++) {
            List<String> batch = payloads(prefix + "-" + i + "-covered", 20);
            publish(prefix + "-" + i, batch.toArray(new String[0]));
            covered.addAll(batch);
        }

        String firstRun = UUID.randomUUID().toString();
        JobClient first = submit(firstRun, fromParallelism, null, deduplicate, subscriptions);
        String savepoint;
        try {
            await(
                    "the first run to emit every message",
                    TIMEOUT,
                    () -> CountingMap.counts(firstRun).keySet().containsAll(covered),
                    () -> "emitted " + CountingMap.counts(firstRun).size() + "/" + covered.size());
            savepoint =
                    first.triggerSavepoint(
                                    savepointDirectory.toUri().toString(),
                                    SavepointFormatType.CANONICAL)
                            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        } finally {
            // Cancelling closes the readers, which nack every unacknowledged message, including
            // the ones the never-notified savepoint covers.
            cancelQuietly(first);
        }

        String secondRun = UUID.randomUUID().toString();
        // Carry the first run's counts over, so one map counts every emission across both runs.
        CountingMap.counts(secondRun).putAll(CountingMap.counts(firstRun));
        CountingMap.forget(firstRun);
        JobClient second = submit(secondRun, toParallelism, savepoint, deduplicate, subscriptions);
        List<String> fresh = new ArrayList<>();
        Run run = new Run(secondRun, second, subscriptions, covered, fresh);
        try {
            for (int i = 0; i < subscriptionCount; i++) {
                List<String> batch = payloads(prefix + "-" + i + "-fresh", 10);
                publish(prefix + "-" + i, batch.toArray(new String[0]));
                fresh.addAll(batch);
            }
            await(
                    "the restored run to emit every message published after the savepoint",
                    TIMEOUT,
                    () -> CountingMap.counts(secondRun).keySet().containsAll(fresh),
                    run::describe);
        } catch (Throwable t) {
            run.finish();
            throw t;
        }
        return run;
    }

    private static JobClient submit(
            String runId,
            int parallelism,
            @Nullable String savepoint,
            boolean deduplicate,
            List<SubscriptionDestination> subscriptions)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        if (savepoint != null) {
            configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(parallelism);
        env.enableCheckpointing(CHECKPOINT_INTERVAL.toMillis());
        PubSubSourceBuilder<String> source =
                PubSubSource.<String>builder()
                        .subscriptions(subscriptions)
                        .deserializer(PubSubDeserializationSchema.payload(new SimpleStringSchema()))
                        .emulatorEndpoint(emulatorEndpoint())
                        .restoreDeduplication(deduplicate);
        env.fromSource(source.build(), WatermarkStrategy.noWatermarks(), "pubsub")
                // The source's uid is what lets the savepoint's reader state find it again.
                .uid("pubsub-source")
                .map(new CountingMap(runId))
                .sinkTo(new DiscardingSink<>());
        return env.executeAsync();
    }

    private static void cancelQuietly(JobClient job) {
        try {
            job.cancel().get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // Best effort only.
        }
    }

    private static List<String> payloads(String prefix, int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> prefix + "-" + i)
                .collect(Collectors.toList());
    }

    /** The restored job and what it is expected to have emitted. */
    private static final class Run {

        private final String runId;
        private final JobClient job;
        private final List<SubscriptionDestination> subscriptions;
        private final List<String> covered;
        private final List<String> fresh;

        private Run(
                String runId,
                JobClient job,
                List<SubscriptionDestination> subscriptions,
                List<String> covered,
                List<String> fresh) {
            this.runId = runId;
            this.job = job;
            this.subscriptions = subscriptions;
            this.covered = covered;
            this.fresh = fresh;
        }

        int count(String payload) {
            AtomicInteger count = CountingMap.counts(runId).get(payload);
            return count == null ? 0 : count.get();
        }

        void assertEachCoveredMessageEmittedOnceAndAcknowledged() throws Exception {
            try {
                assertThat(covered)
                        .as("messages the savepoint covered, emitted exactly once across both runs")
                        .allSatisfy(payload -> assertThat(count(payload)).isEqualTo(1));
                assertThat(fresh)
                        .as("messages published after the savepoint")
                        .allSatisfy(payload -> assertThat(count(payload)).isPositive());
            } finally {
                cancelQuietly(job);
            }
            // Cancelling nacked whatever the restored job still held, including every message
            // published after the savepoint, since the restored run takes no checkpoint. Pulling
            // those back proves the subscription answered. A covered message that was neither
            // suppressed nor emitted would be among them, and ahead of them: the first run's
            // cancellation nacked it before the fresh messages were published.
            Set<String> remaining = ConcurrentHashMap.newKeySet();
            for (SubscriptionDestination subscription : subscriptions) {
                remaining.addAll(
                        pullAndAckUntil(
                                subscription, fresh.size() / subscriptions.size(), TIMEOUT));
            }
            assertThat(remaining)
                    .as("the fresh messages the restored run left unacknowledged")
                    .containsAll(fresh);
            assertThat(remaining)
                    .as("covered messages left unacknowledged on the subscriptions")
                    .doesNotContainAnyElementsOf(covered);
            CountingMap.forget(runId);
        }

        void finish() {
            cancelQuietly(job);
            CountingMap.forget(runId);
        }

        String describe() {
            long coveredOnce = covered.stream().filter(payload -> count(payload) == 1).count();
            long coveredTwice = covered.stream().filter(payload -> count(payload) >= 2).count();
            long freshSeen = fresh.stream().filter(payload -> count(payload) > 0).count();
            return "covered emitted once "
                    + coveredOnce
                    + ", twice or more "
                    + coveredTwice
                    + " of "
                    + covered.size()
                    + "; fresh "
                    + freshSeen
                    + "/"
                    + fresh.size();
        }
    }

    /**
     * Counts every emission per payload in a static registry keyed by a run id; static because the
     * MiniCluster shares the JVM with the test and the function is re-created on restore.
     */
    private static final class CountingMap extends RichMapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        private static final Map<String, Map<String, AtomicInteger>> COUNTS =
                new ConcurrentHashMap<>();

        private final String runId;

        CountingMap(String runId) {
            this.runId = runId;
        }

        @Override
        public String map(String value) {
            counts(runId).computeIfAbsent(value, key -> new AtomicInteger()).incrementAndGet();
            return value;
        }

        static Map<String, AtomicInteger> counts(String runId) {
            return COUNTS.computeIfAbsent(runId, unused -> new ConcurrentHashMap<>());
        }

        static void forget(String runId) {
            COUNTS.remove(runId);
        }
    }
}
