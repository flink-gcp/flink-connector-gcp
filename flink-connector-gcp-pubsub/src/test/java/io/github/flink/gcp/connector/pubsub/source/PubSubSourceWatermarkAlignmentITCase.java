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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.ExceptionUtils;

import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Watermark alignment against the emulator, and what a subscriber that dies while its split is
 * paused does to the job (#348).
 *
 * <p><b>This class exists to measure a reachability claim rather than to cover a feature.</b> The
 * reader's {@code pauseOrResumeSplits} is bookkeeping only — a paused split is simply not drained —
 * and nothing else in this repository configures alignment, so before #348 the whole premise
 * ("watermark alignment pauses splits routinely") rested on reading Flink's {@code SourceOperator}.
 * The first test measures it end to end; the second is what #348 is actually about, and it needs
 * the first to have established that the state it puts the job in is real.
 *
 * <p>The throttle in {@link ThrottledRecordingMap} is what makes the first test decisive rather
 * than a race. Alignment needs one watermark interval (200 ms by default) plus one alignment update
 * interval before it can pause anything, and a MiniCluster drains a few hundred buffered messages
 * in less time than that — so without a throttle the ahead split finishes before the machinery
 * under test has run once, and the test passes or fails on scheduling noise. With it, consuming one
 * split's batch takes seconds, and the assertion is about a count that alignment has had many
 * intervals to hold down.
 */
class PubSubSourceWatermarkAlignmentITCase extends AbstractPubSubSourceEmulatorITCase {

    private static final int ACK_DEADLINE_SECONDS = 60;

    /** Records per subscription. Large enough that a paused split is unmistakably short. */
    private static final int BATCH = 200;

    /**
     * Per-record delay in the map. {@code BATCH} × 2 subscriptions × this is the run's floor, which
     * has to comfortably exceed the alignment latency below.
     */
    private static final Duration THROTTLE = Duration.ofMillis(5);

    /** How far the ahead split's timestamps sit beyond the behind split's. */
    private static final Duration LEAD = Duration.ofHours(1);

    /** Well under {@link #LEAD}, so the ahead split is unambiguously over the limit. */
    private static final Duration MAX_DRIFT = Duration.ofSeconds(5);

    private static final Duration ALIGNMENT_INTERVAL = Duration.ofMillis(100);

    private static final long BASE_TIMESTAMP = 1_700_000_000_000L;

    /**
     * Bound on each wait. Two of them run in the slowest test, and the class timeout {@link
     * AbstractPubSubSourceEmulatorITCase} sets is 180 s <em>per method</em> — so this has to leave
     * both inside it, or a genuine failure is reported as a method timeout with no diagnosis
     * instead of as the await that actually gave up. A green run spends about 10 s here.
     */
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(60);

    /** How long one stability sample waits. Several times both intervals the pause depends on. */
    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(1);

    /** Consecutive unchanged samples that count as stopped. */
    private static final int STABLE_SAMPLES = 3;

    @Test
    void alignmentPausesTheSplitWhoseWatermarkRunsAhead() throws Exception {
        SubscriptionDestination ahead = publishAhead("align-ahead");
        SubscriptionDestination behind = publishBehind("align-behind");

        String runId = UUID.randomUUID().toString();
        JobClient job = runAlignedJob(runId, ahead, behind);
        try {
            await(
                    "the behind subscription to be consumed whole",
                    AWAIT_TIMEOUT,
                    () -> countWithPrefix(runId, "behind-") == BATCH,
                    () -> progress(runId));

            // The measurement, and it has to be a *stability* one. `ahead < BATCH` alone would be
            // satisfied by a split that is merely lagging: at maxRecordsPerFetch(1) the two splits
            // advance in lockstep, so with alignment doing nothing the ahead count at this instant
            // is 199 or 200 — and 199 passes. Sampling twice, several alignment intervals apart,
            // is what distinguishes "held" from "behind", and it is the claim the docs make.
            long firstSample = awaitStableAheadCount(runId);

            assertThat(countWithPrefix(runId, "ahead-"))
                    .as("the ahead split stopped being drained: %s", progress(runId))
                    .isEqualTo(firstSample)
                    .isLessThan(BATCH);
        } finally {
            cancelQuietly(job);
            ThrottledRecordingMap.forget(runId);
        }
    }

    @Test
    void aSubscriberThatDiesWhileItsSplitIsPausedFailsTheJob() throws Exception {
        SubscriptionDestination ahead = publishAhead("dead-ahead");
        SubscriptionDestination behind = publishBehind("dead-behind");

        String runId = UUID.randomUUID().toString();
        JobClient job = runAlignedJob(runId, ahead, behind);
        try {
            await(
                    "the behind subscription to be consumed whole, which leaves the ahead split"
                            + " paused",
                    AWAIT_TIMEOUT,
                    () -> countWithPrefix(runId, "behind-") == BATCH,
                    () -> progress(runId));
            // Paused, not merely behind — the same stability check as the first test, and here it
            // is load-bearing rather than thorough: if this split were still being drained, the
            // failure below would surface through pullMessages on the ordinary path and the test
            // would pass while measuring nothing about #348.
            long stable = awaitStableAheadCount(runId);
            assertThat(countWithPrefix(runId, "ahead-"))
                    .as(
                            "the ahead split is paused before its subscriber is killed: %s",
                            progress(runId))
                    .isEqualTo(stable)
                    .isLessThan(BATCH);

            // Kill the paused split's subscriber. Nothing drains that split, so the only thing
            // that can report its failure is the check #348 added; before it, the streaming pull
            // died and the job carried on green with this subscription dead.
            deleteSubscription(ahead);

            // Read through the execution result rather than by polling getJobStatus: the
            // per-job MiniCluster shuts down the moment the job reaches a terminal state, and a
            // status poll after that throws instead of answering FAILED. The result future is
            // already completed by then, so it survives the shutdown — and it carries the cause,
            // which is the half worth asserting.
            String failure =
                    ExceptionUtils.stringifyException(
                            catchThrowable(
                                    () ->
                                            job.getJobExecutionResult()
                                                    .get(
                                                            AWAIT_TIMEOUT.toSeconds(),
                                                            TimeUnit.SECONDS)));
            assertThat(failure)
                    .as("the job failed on the paused split's dead subscriber: %s", progress(runId))
                    .contains("The Pub/Sub subscriber for subscription")
                    .contains(ahead.toString());
        } finally {
            cancelQuietly(job);
            ThrottledRecordingMap.forget(runId);
        }
    }

    private static SubscriptionDestination publishAhead(String name)
            throws java.io.IOException, InterruptedException, ExecutionException {
        SubscriptionDestination subscription =
                createTopicAndSubscription(name, ACK_DEADLINE_SECONDS);
        publish(name, payloads("ahead-").toArray(new String[0]));
        return subscription;
    }

    private static SubscriptionDestination publishBehind(String name)
            throws java.io.IOException, InterruptedException, ExecutionException {
        SubscriptionDestination subscription =
                createTopicAndSubscription(name, ACK_DEADLINE_SECONDS);
        publish(name, payloads("behind-").toArray(new String[0]));
        return subscription;
    }

    private static List<String> payloads(String prefix) {
        return IntStream.range(0, BATCH).mapToObj(i -> prefix + i).collect(Collectors.toList());
    }

    private static JobClient runAlignedJob(
            String runId, SubscriptionDestination ahead, SubscriptionDestination behind)
            throws Exception {
        Configuration configuration = new Configuration();
        // No restart: this test reads the job's terminal status, and a retry would hide it.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // One subtask, so one reader owns both splits — which is the configuration #348 is about:
        // the reader drains one split and skips the other.
        env.setParallelism(1);
        env.enableCheckpointing(500);

        env.fromSource(source(ahead, behind), alignedWatermarks(), "pubsub")
                .map(new ThrottledRecordingMap(runId))
                .sinkTo(new DiscardingSink<>());
        return env.executeAsync();
    }

    /**
     * Timestamps derived from the payload rather than from the Pub/Sub publish time, which is what
     * lets the two splits' watermarks be made to diverge on demand — publish times cannot.
     */
    private static WatermarkStrategy<String> alignedWatermarks() {
        return WatermarkStrategy.<String>forMonotonousTimestamps()
                .withTimestampAssigner((value, recordTimestamp) -> eventTime(value))
                .withWatermarkAlignment("pubsub", MAX_DRIFT, ALIGNMENT_INTERVAL);
    }

    private static long eventTime(String payload) {
        return payload.startsWith("ahead-")
                ? BASE_TIMESTAMP + LEAD.toMillis()
                : BASE_TIMESTAMP + Long.parseLong(payload.substring("behind-".length()));
    }

    private static Source<String, SubscriptionSplit, PubSubEnumeratorState> source(
            SubscriptionDestination... subscriptions) {
        return PubSubSource.<String>builder()
                .subscriptions(subscriptions)
                .deserializationSchema(
                        PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                // One record per split per fetch, so the ahead split cannot outrun the alignment
                // machinery in a single drain of its whole buffer.
                .subscriberOptions(PubSubSubscriberOptions.builder().maxRecordsPerFetch(1).build())
                .emulatorEndpoint(emulatorEndpoint())
                .build();
    }

    /**
     * Waits until the ahead split's consumed count stops moving, and returns it.
     *
     * <p>A count is not evidence of a pause; a count that does not change over several alignment
     * intervals is. The settle window is generous against {@link #ALIGNMENT_INTERVAL} and {@code
     * pipeline.auto-watermark-interval} (200 ms by default) because the split has to have been
     * paused for a while, not just this instant.
     */
    private static long awaitStableAheadCount(String runId) throws InterruptedException {
        long previous = -1;
        for (int settled = 0; settled < STABLE_SAMPLES; settled++) {
            Thread.sleep(SAMPLE_INTERVAL.toMillis());
            long current = countWithPrefix(runId, "ahead-");
            if (current != previous) {
                previous = current;
                settled = -1;
            }
        }
        return previous;
    }

    private static long countWithPrefix(String runId, String prefix) {
        return ThrottledRecordingMap.records(runId).stream()
                .filter(r -> r.startsWith(prefix))
                .count();
    }

    private static String progress(String runId) {
        return "consumed "
                + countWithPrefix(runId, "ahead-")
                + " ahead and "
                + countWithPrefix(runId, "behind-")
                + " behind of "
                + BATCH
                + " each";
    }

    private static void cancelQuietly(JobClient job) {
        try {
            job.cancel().get(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // Already terminal, which is the expected end of the second test.
        }
    }

    /**
     * Records what the job consumed and slows it to a pace the alignment machinery can act within;
     * see the class javadoc for why the throttle is load-bearing rather than incidental. Static
     * registry because the MiniCluster shares this JVM, as {@code PubSubSourceRecoveryITCase}'s own
     * recorder does.
     */
    private static final class ThrottledRecordingMap extends RichMapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        private static final Map<String, Set<String>> RECORDS = new ConcurrentHashMap<>();

        private final String runId;

        ThrottledRecordingMap(String runId) {
            this.runId = runId;
        }

        @Override
        public void open(OpenContext openContext) {
            records(runId);
        }

        @Override
        public String map(String value) throws InterruptedException {
            Thread.sleep(THROTTLE.toMillis());
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
