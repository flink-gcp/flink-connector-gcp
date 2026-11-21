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

package io.github.flink.gcp.connector.pubsub.table;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Watermark alignment of a {@code pubsub} SQL table, which only {@code
 * scan.watermark-push-down.enabled} makes possible (#1837).
 *
 * <p>The shape is {@code PubSubSourceWatermarkAlignmentITCase}'s, moved into SQL: one subtask reads
 * two subscriptions, one of which publishes records an hour ahead of the other's, and a throttled
 * function after the scan slows consumption enough for the alignment machinery to act. Its class
 * comment explains why the throttle and the stability sampling are both load-bearing. The second
 * test is the control: the same table without the option consumes the ahead subscription whole,
 * because the {@code scan.watermark.alignment.*} options do nothing for a source that does not
 * generate the watermark itself.
 *
 * <p>The third test measures the other thing a source-generated watermark changes, with no
 * alignment configured: whether rows from a subscription that lags behind another one read by the
 * same subtask arrive behind the watermark, which is what an event-time operator drops as late.
 */
class PubSubTableWatermarkAlignmentITCase extends PubSubTableTestBase {

    private static final int ACK_DEADLINE_SECONDS = 60;

    /** Records per subscription. Large enough that a paused split is unmistakably short. */
    private static final int BATCH = 200;

    /** Per-record delay in the throttled function; see the class comment. */
    private static final long THROTTLE_MILLIS = 5;

    /** How far the ahead subscription's event times sit beyond the behind one's. */
    private static final long LEAD_MILLIS = Duration.ofHours(1).toMillis();

    private static final long BASE_TIMESTAMP = 1_700_000_000_000L;

    /** Bound on each wait; two of them fit inside the class's per-method timeout. */
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(60);

    /** How long one stability sample waits. Several times both alignment intervals. */
    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(1);

    /** Consecutive unchanged samples that count as stopped. */
    private static final int STABLE_SAMPLES = 3;

    @Test
    void alignmentPausesTheSubscriptionWhoseWatermarkRunsAhead() throws Exception {
        String runId = UUID.randomUUID().toString();
        JobClient job = runAlignedQuery(runId, "sql-align-ahead", "sql-align-behind", true);
        try {
            await(
                    "the behind subscription to be consumed whole",
                    AWAIT_TIMEOUT,
                    () -> count(runId, "behind-") == BATCH,
                    () -> progress(runId));

            long stable = awaitStableAheadCount(runId);

            // A failed job also stops consuming, and with restarts off it would freeze the count
            // just as a pause does; only a running job makes the stable count a pause.
            assertThat(job.getJobStatus().get(60, TimeUnit.SECONDS))
                    .as("the job is still running: %s", progress(runId))
                    .isEqualTo(JobStatus.RUNNING);
            assertThat(count(runId, "ahead-"))
                    .as("the ahead subscription stopped being consumed: %s", progress(runId))
                    .isEqualTo(stable)
                    .isLessThan(BATCH);
        } finally {
            cancelQuietly(job);
            ThrottledRecord.forget(runId);
        }
    }

    @Test
    void withoutPushDownTheAlignmentOptionsDoNothing() throws Exception {
        String runId = UUID.randomUUID().toString();
        JobClient job = runAlignedQuery(runId, "sql-noalign-ahead", "sql-noalign-behind", false);
        try {
            await(
                    "both subscriptions to be consumed whole",
                    AWAIT_TIMEOUT,
                    () -> count(runId, "ahead-") == BATCH && count(runId, "behind-") == BATCH,
                    () -> progress(runId));
        } finally {
            cancelQuietly(job);
            ThrottledRecord.forget(runId);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aLaggingSubscriptionIsBehindTheWatermarkOnlyWithoutPushDown(boolean pushDown)
            throws Exception {
        String ahead = "sql-late-ahead-" + pushDown;
        String behind = "sql-late-behind-" + pushDown;
        createTopicAndSubscription(ahead, ACK_DEADLINE_SECONDS);
        createTopicAndSubscription(behind, ACK_DEADLINE_SECONDS);
        // Only the ahead subscription has rows when the job starts; the lagging one is published
        // after some of them were read, so the order the two arrive in is fixed rather than left
        // to the subscribers.
        publish(ahead, payloads("ahead-", BASE_TIMESTAMP + LEAD_MILLIS));

        String runId = UUID.randomUUID().toString();
        TableEnvironment tEnv = checkpointingTableEnvironment();
        // One subtask reads both subscriptions, so one watermark covers both of them.
        tEnv.getConfig().set("parallelism.default", "1");
        tEnv.createTemporarySystemFunction("RECORD_LATENESS", RecordedLateness.class);
        tEnv.executeSql(
                "CREATE TABLE events (\n"
                        + "  tag STRING,\n"
                        + "  millis BIGINT,\n"
                        + "  ts AS TO_TIMESTAMP_LTZ(millis, 3),\n"
                        + "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n"
                        + ") "
                        + withOptions(
                                "subscription",
                                ahead + ";" + behind,
                                "format",
                                "json",
                                // A pushed watermark is emitted with every row rather than on a
                                // timer, so the lagging rows read after the first ones observe one;
                                // the planner's operator ignores the option.
                                "scan.watermark.emit.strategy",
                                "on-event",
                                "scan.watermark-push-down.enabled",
                                Boolean.toString(pushDown)));
        tEnv.executeSql("CREATE TABLE lateness (tag STRING) WITH ('connector' = 'blackhole')");
        JobClient job =
                tEnv.executeSql(
                                "INSERT INTO lateness SELECT RECORD_LATENESS('"
                                        + runId
                                        + "', tag, CURRENT_WATERMARK(ts) IS NOT NULL,"
                                        + " ts < CURRENT_WATERMARK(ts)) FROM events")
                        .getJobClient()
                        .orElseThrow(() -> new AssertionError("the insert started no job"));
        try {
            await(
                    "the ahead subscription to be read",
                    AWAIT_TIMEOUT,
                    () -> RecordedLateness.count(runId, "ahead-", r -> true) > 0,
                    () -> RecordedLateness.describe(runId));
            publish(behind, payloads("behind-", BASE_TIMESTAMP));
            await(
                    "the lagging subscription to be read whole",
                    AWAIT_TIMEOUT,
                    () -> RecordedLateness.count(runId, "behind-", r -> true) == BATCH,
                    () -> RecordedLateness.describe(runId));

            long lateBehind = RecordedLateness.count(runId, "behind-", r -> r.late);
            if (pushDown) {
                // A row read before any watermark compares as unknown, not late, so zero late
                // rows proves nothing unless lagging rows did observe a watermark.
                assertThat(RecordedLateness.count(runId, "behind-", r -> r.observed))
                        .as(
                                "lagging rows that observed a watermark: %s",
                                RecordedLateness.describe(runId))
                        .isPositive();
                // Per-split watermarks: the subtask's watermark is the lagging split's, so none of
                // its rows is behind it.
                assertThat(lateBehind).as(RecordedLateness.describe(runId)).isZero();
            } else {
                // One watermark over the subtask's rows already follows the ahead subscription,
                // an hour past every row of the lagging one.
                assertThat(lateBehind).as(RecordedLateness.describe(runId)).isEqualTo(BATCH);
            }
        } finally {
            cancelQuietly(job);
            RecordedLateness.forget(runId);
        }
    }

    private static JobClient runAlignedQuery(
            String runId, String ahead, String behind, boolean pushDown) throws Exception {
        createTopicAndSubscription(ahead, ACK_DEADLINE_SECONDS);
        createTopicAndSubscription(behind, ACK_DEADLINE_SECONDS);
        publish(ahead, payloads("ahead-", BASE_TIMESTAMP + LEAD_MILLIS));
        publish(behind, payloads("behind-", BASE_TIMESTAMP));

        TableEnvironment tEnv = checkpointingTableEnvironment();
        // One subtask, so one reader owns both splits and alignment has a split to pause.
        tEnv.getConfig().set("parallelism.default", "1");
        tEnv.createTemporarySystemFunction("THROTTLED_RECORD", ThrottledRecord.class);
        tEnv.executeSql(
                "CREATE TABLE events (\n"
                        + "  tag STRING,\n"
                        + "  millis BIGINT,\n"
                        + "  ts AS TO_TIMESTAMP_LTZ(millis, 3),\n"
                        + "  WATERMARK FOR ts AS ts\n"
                        + ") "
                        + withOptions(
                                "subscription",
                                ahead + ";" + behind,
                                "format",
                                "json",
                                // One record per split per fetch, so the ahead split cannot outrun
                                // the alignment machinery in a single drain of its buffer.
                                "scan.max-records-per-fetch",
                                "1",
                                "scan.watermark-push-down.enabled",
                                Boolean.toString(pushDown),
                                "scan.watermark.alignment.group",
                                "pubsub",
                                "scan.watermark.alignment.max-drift",
                                "5 s",
                                "scan.watermark.alignment.update-interval",
                                "100 ms"));
        tEnv.executeSql("CREATE TABLE discarded (tag STRING) WITH ('connector' = 'blackhole')");
        return tEnv.executeSql(
                        "INSERT INTO discarded SELECT THROTTLED_RECORD('"
                                + runId
                                + "', tag) FROM events")
                .getJobClient()
                .orElseThrow(() -> new AssertionError("the insert started no job"));
    }

    /**
     * Cancels without masking the test's own failure: a job that already failed has taken its
     * per-job MiniCluster down with it, and cancelling it then throws.
     */
    private static void cancelQuietly(JobClient job) {
        try {
            job.cancel().get(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // Already terminal; the assertion that ran before this is the report.
        }
    }

    private static String[] payloads(String prefix, long firstMillis) {
        return IntStream.range(0, BATCH)
                .mapToObj(
                        i ->
                                String.format(
                                        "{\"tag\":\"%s%d\",\"millis\":%d}",
                                        prefix, i, firstMillis + i))
                .toArray(String[]::new);
    }

    /** Waits until the ahead count stops moving over several samples, and returns it. */
    private static long awaitStableAheadCount(String runId) throws InterruptedException {
        long previous = -1;
        for (int settled = 0; settled < STABLE_SAMPLES; settled++) {
            Thread.sleep(SAMPLE_INTERVAL.toMillis());
            long current = count(runId, "ahead-");
            if (current != previous) {
                previous = current;
                settled = -1;
            }
        }
        return previous;
    }

    private static long count(String runId, String prefix) {
        return ThrottledRecord.records(runId).stream().filter(r -> r.startsWith(prefix)).count();
    }

    private static String progress(String runId) {
        return "consumed "
                + count(runId, "ahead-")
                + " ahead and "
                + count(runId, "behind-")
                + " behind of "
                + BATCH
                + " each";
    }

    /**
     * Records, per row, whether it observed a watermark and whether it was behind it. Static
     * registry because the MiniCluster shares this JVM; each run keeps the last report per tag,
     * since a redelivery is the same row.
     */
    public static final class RecordedLateness extends ScalarFunction {

        private static final long serialVersionUID = 1L;

        private static final Map<String, Map<String, Report>> REPORTS = new ConcurrentHashMap<>();

        /** What one row saw. */
        static final class Report {
            final boolean observed;
            final boolean late;

            Report(boolean observed, boolean late) {
                this.observed = observed;
                this.late = late;
            }
        }

        public String eval(String runId, String tag, Boolean observed, Boolean late) {
            reports(runId)
                    .put(tag, new Report(Boolean.TRUE.equals(observed), Boolean.TRUE.equals(late)));
            return tag;
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }

        static long count(String runId, String prefix, Predicate<Report> filter) {
            return reports(runId).entrySet().stream()
                    .filter(e -> e.getKey().startsWith(prefix))
                    .filter(e -> filter.test(e.getValue()))
                    .count();
        }

        static String describe(String runId) {
            return String.format(
                    "ahead read %d; lagging read %d, observed a watermark %d, late %d",
                    count(runId, "ahead-", r -> true),
                    count(runId, "behind-", r -> true),
                    count(runId, "behind-", r -> r.observed),
                    count(runId, "behind-", r -> r.late));
        }

        static void forget(String runId) {
            REPORTS.remove(runId);
        }

        private static Map<String, Report> reports(String runId) {
            return REPORTS.computeIfAbsent(runId, unused -> new ConcurrentHashMap<>());
        }
    }

    /**
     * Records what the query consumed and slows it to a pace the alignment machinery can act
     * within. Static registry because the MiniCluster shares this JVM.
     */
    public static final class ThrottledRecord extends ScalarFunction {

        private static final long serialVersionUID = 1L;

        private static final Map<String, Set<String>> RECORDS = new ConcurrentHashMap<>();

        public String eval(String runId, String tag) throws InterruptedException {
            Thread.sleep(THROTTLE_MILLIS);
            records(runId).add(tag);
            return tag;
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }

        static Set<String> records(String runId) {
            return RECORDS.computeIfAbsent(runId, unused -> ConcurrentHashMap.newKeySet());
        }

        static void forget(String runId) {
            RECORDS.remove(runId);
        }
    }
}
