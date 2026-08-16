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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.core.SettableApiFuture;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the no-progress bound {@code publishProgressTimeout} puts on the two waits {@link
 * PubSubWriter} makes on the task thread (#333).
 *
 * <p>What the bound is, and what every test here turns on: it is a budget on the time since a
 * publish last <em>completed</em>, not on the time spent in the call. So a publisher that keeps
 * answering never spends it however long the wait lasts in total, and one that has stopped
 * answering fails the job once. Measured on 2026-08-07: with {@code enableMessageOrdering} the SDK
 * retries a publish without limit, so an ordered sink was still waiting at 700 s where the
 * unordered one gives up at ~591 s — and nothing else ever ends it.
 *
 * <p>Timed out as a class above the largest budget any test here configures. What that buys is a
 * report, not a rescue: JUnit's {@code @Timeout} is same-thread by default and cannot preempt, so a
 * bound that stopped bounding would leave the two tests with no upper-bound assertion blocked in
 * {@code FakeMailboxExecutor.yield()} for good. Each test's own budget is what keeps a regression
 * fast; the sibling writer test classes carry {@code @Timeout(30)} on the same terms.
 */
@Timeout(60)
class PubSubWriterProgressTimeoutTest {

    private static final String PROJECT = "test-project";
    private static final TopicDestination TOPIC = TopicDestination.of(PROJECT, "progress-topic");
    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;
    private static final RetrySchedule UNUSED_RECOVERY = new RetrySchedule(1, 1, 1, 0);

    private final FakePublisherFactory factory = new FakePublisherFactory();
    private final FakeTopicAdmin admin = new FakeTopicAdmin();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
    private final ScheduledExecutorService clock = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void stopTheClock() {
        clock.shutdownNow();
        // The interrupt test schedules an interrupt at this thread; if its assertions fail early
        // the flag can still be pending, and JUnit reuses the thread for the next test.
        Thread.interrupted();
    }

    private PubSubWriter<String> newWriter(PubSubPublisherOptions options) {
        return new PubSubWriter<>(
                TestSinkConfigs.forTopic(
                        TOPIC,
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                        CreateDisposition.CREATE_NEVER,
                        options),
                factory,
                admin,
                mailbox,
                metrics,
                UNUSED_RECOVERY);
    }

    /** A publish that never completes, which is what an outage looks like to the writer. */
    private SettableApiFuture<String> neverCompletes() {
        SettableApiFuture<String> future = SettableApiFuture.create();
        factory.enqueueFuture(future);
        return future;
    }

    @Test
    void aFlushGivesUpWhenNoPublishCompletesWithinTheBudget() throws Exception {
        neverCompletes();
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .publishProgressTimeout(Duration.ofMillis(200))
                                .build());
        writer.write("payload", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("No publish to Pub/Sub completed")
                .hasMessageContaining("draining the in-flight publishes")
                .hasMessageContaining("1 publish(es) still in flight")
                // The configured budget, not a hardcoded one: this is what says the knob reached
                // the wait rather than merely being readable off the options object.
                .hasMessageContaining("publishProgressTimeout of PT0.2S")
                .hasMessageContaining("Nothing is dropped");
    }

    @Test
    void aWriteAtTheInFlightCapGivesUpOnTheSameBudget() throws Exception {
        // The leg a busy ordered sink actually parks in: measured 2026-08-07, at the shipped cap of
        // 1000 and 5000 records/s the task thread was in awaitCapacity, not in the checkpoint
        // drain. A bound on the flush alone would never have fired for it.
        neverCompletes();
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .maxInFlightMessages(1)
                                .publishProgressTimeout(Duration.ofMillis(200))
                                .build());
        writer.write("first", CONTEXT);

        assertThatThrownBy(() -> writer.write("second", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("No publish to Pub/Sub completed")
                .hasMessageContaining("admitting a record")
                .hasMessageContaining("publishProgressTimeout of PT0.2S");

        // This wait made hundreds of passes before expiring, and asked the publisher to send what
        // it was batching on exactly one of them. Pinned here rather than beside the flush itself,
        // where the wait ends on its first pass and "once" and "every pass" are the same number.
        assertThat(factory.publishers.get(TOPIC).flushCalls).isEqualTo(1);
    }

    @Test
    void aPublisherThatKeepsAnsweringNeverSpendsTheBudgetHoweverLongTheWaitLasts()
            throws Exception {
        // The discrimination the whole design rests on. Ten publishes completing 300 ms apart make
        // a flush that lasts ~3 s — well past the 2 s budget — and none of it counts, because every
        // completion restarts the clock. The discrimination needs only budget < total duration, so
        // the budget is 2 s rather than the 500 ms a first draft used: that left 200 ms of slack on
        // each of ten gaps, and one stalled gap anywhere would have failed it.
        List<SettableApiFuture<String>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(neverCompletes());
        }
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .publishProgressTimeout(Duration.ofSeconds(2))
                                .build());
        for (int i = 0; i < 10; i++) {
            writer.write("payload-" + i, CONTEXT);
        }
        for (int i = 0; i < 10; i++) {
            SettableApiFuture<String> future = futures.get(i);
            clock.schedule(() -> future.set("message"), 300L * (i + 1), TimeUnit.MILLISECONDS);
        }

        long start = System.nanoTime();
        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs)
                .as("the flush really did outlast the budget without spending it")
                .isGreaterThan(1_000);
    }

    @Test
    void blockingAtTheCapSendsWhatTheSdkIsStillBatching() throws Exception {
        // A message counts against the caps as soon as the publisher accepts it, which is before
        // it goes anywhere. At the cap, then, every in-flight message can still be inside a batch
        // waiting for batchDelayThreshold — and the writer, holding the task thread, cannot add the
        // message that would trip the size threshold instead. Without the flush the batcher's delay
        // sits inside publishProgressTimeout, so a delay longer than the budget expires the wait on
        // a reachable topic. This publisher models exactly that: it resolves nothing until asked.
        BatchingPublisherFactory batching = new BatchingPublisherFactory();
        PubSubWriter<String> writer =
                new PubSubWriter<>(
                        TestSinkConfigs.forTopic(
                                TOPIC,
                                PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                                CreateDisposition.CREATE_NEVER,
                                PubSubPublisherOptions.builder()
                                        .maxInFlightMessages(1)
                                        // Short, so an unflushed batcher fails this in under a
                                        // second rather than parking the build.
                                        .publishProgressTimeout(Duration.ofMillis(500))
                                        .build()),
                        batching,
                        admin,
                        mailbox,
                        metrics,
                        UNUSED_RECOVERY);

        writer.write("first", CONTEXT);
        // The cap is full and nothing has been sent, so this can only proceed if the wait flushes.
        assertThatCode(() -> writer.write("second", CONTEXT)).doesNotThrowAnyException();

        assertThat(batching.flushes).isEqualTo(1);
    }

    @Test
    void theRepairsOwnDrainAlsoSendsWhatIsStillBatched() throws Exception {
        // The third wait, and the one with no flush in front of it: flush() and
        // TopicRepairer.repair
        // both flush immediately before draining, repairPendingTopics does not. Reaching it needs
        // the parking to happen *during* a capacity wait, because write() tests repairNeeded before
        // that wait and so publishes its own record before the repair it now owes ever runs.
        BatchingPublisherFactory batching = new BatchingPublisherFactory();
        PubSubWriter<String> writer =
                new PubSubWriter<>(
                        TestSinkConfigs.forTopic(
                                TOPIC,
                                PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                                CreateDisposition.CREATE_IF_NEEDED,
                                PubSubPublisherOptions.builder()
                                        .maxInFlightMessages(1)
                                        .publishProgressTimeout(Duration.ofMillis(500))
                                        .build()),
                        batching,
                        admin,
                        mailbox,
                        metrics,
                        UNUSED_RECOVERY);

        // "a" fills the cap and will answer NOT_FOUND when the capacity wait flushes it.
        batching.failNext(new StatusRuntimeException(Status.NOT_FOUND));
        writer.write("a", CONTEXT);
        // "b" waits for capacity; the flush that wait makes parks "a" and sets repairNeeded, and
        // then "b" is published — into the batcher, unsent.
        writer.write("b", CONTEXT);
        assertThat(writer.getInFlightMessages()).isEqualTo(1);

        // "c" opens the repair "a" owes, whose leading drain is waiting for "b".
        assertThatCode(() -> writer.write("c", CONTEXT)).doesNotThrowAnyException();
        assertThat(admin.created).containsExactly(TOPIC);
    }

    /** A publisher that holds every publish until {@code flushOutstanding()} asks for it. */
    private static final class BatchingPublisherFactory implements PublisherFactory {

        private static final long serialVersionUID = 1L;

        private final transient List<SettableApiFuture<String>> batched = new ArrayList<>();
        private int flushes;
        private transient Throwable failNext;

        /** The next publish answers this once it is sent, rather than succeeding. */
        private void failNext(Throwable failure) {
            this.failNext = failure;
        }

        /** Sends the batch from the test thread, as the SDK's delay threshold eventually would. */
        private void flushAll() {
            List<SettableApiFuture<String>> sending = new ArrayList<>(batched);
            batched.clear();
            Throwable failure = failNext;
            failNext = null;
            sending.forEach(
                    future -> {
                        if (failure != null) {
                            future.setException(failure);
                        } else {
                            future.set("message");
                        }
                    });
        }

        @Override
        public TopicPublisher create(TopicDestination destination) {
            return new TopicPublisher() {
                @Override
                public com.google.api.core.ApiFuture<String> publish(
                        com.google.pubsub.v1.PubsubMessage message) {
                    SettableApiFuture<String> future = SettableApiFuture.create();
                    batched.add(future);
                    return future;
                }

                @Override
                public void resumePublish(String orderingKey) {}

                @Override
                public void flushOutstanding() {
                    flushes++;
                    flushAll();
                }

                @Override
                public void shutdown() {}

                @Override
                public void close() {}
            };
        }
    }

    @Test
    void aStalledWaitSaysSoLongBeforeTheBudgetEndsIt() throws Exception {
        // The budget is spent in silence otherwise, and the counters an operator watches cannot
        // report this state: no publish is resolving, which is what the state is. The line has to
        // come out well before the failure, or the first thing anyone sees is the job dying — and
        // at the shipped default that is Flink's checkpoint timeout's moment too.
        neverCompletes();
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .publishProgressTimeout(Duration.ofSeconds(2))
                                .build());
        writer.write("payload", CONTEXT);

        try (LogCapture capture = LogCapture.of(PubSubWriter.class)) {
            long start = System.nanoTime();
            assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            List<String> stallWarnings =
                    capture.getMessages().stream()
                            .filter(line -> line.contains("has completed for"))
                            .collect(Collectors.toList());
            // Rate-limited to one per threshold, not one per pass: the wait polls at 1 ms, so two
            // seconds of it is ~2000 passes against a 200 ms threshold's ~10 opportunities. The
            // range is what discriminates; the exact count depends on scheduling.
            assertThat(stallWarnings).hasSizeBetween(1, 20);
            assertThat(stallWarnings.get(0))
                    .contains("draining the in-flight publishes")
                    .contains("1 publish(es) in flight")
                    .contains("publishProgressTimeout of PT2S")
                    .contains("numRecordsSend")
                    // The rendered idle time is what pins the fraction: a tenth of a 2 s budget.
                    // Without it, changing PROGRESS_WARN_FRACTION to 2 passes every other
                    // assertion here.
                    .contains("completed for PT0.2");
            // And the line beat the failure by an order of magnitude, which is its whole point.
            assertThat(elapsedMs).isGreaterThan(1_500);
        }
    }

    @Test
    void aWaitThatKeepsMakingProgressSaysNothing() throws Exception {
        // The complement, and what stops the line being noise: ordinary backpressure never reaches
        // the warning, because every completion restarts the clock it is measured against.
        List<SettableApiFuture<String>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(neverCompletes());
        }
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                // 10 s, so the 1 s warn threshold is ten times the gaps below and
                                // a stalled runner cannot turn one of them into a warning.
                                .publishProgressTimeout(Duration.ofSeconds(10))
                                .build());
        for (int i = 0; i < 10; i++) {
            writer.write("payload-" + i, CONTEXT);
        }
        for (int i = 0; i < 10; i++) {
            SettableApiFuture<String> future = futures.get(i);
            clock.schedule(() -> future.set("message"), 100L * (i + 1), TimeUnit.MILLISECONDS);
        }

        try (LogCapture capture = LogCapture.of(PubSubWriter.class)) {
            writer.flush(false);

            assertThat(capture.getMessages())
                    .as("a flush spanning a second of 100 ms gaps against a 1 s warn threshold")
                    .noneMatch(line -> line.contains("has completed for"));
        }
    }

    @Test
    void aPublishThatFailsIsStillThePublisherAnswering() throws Exception {
        // Progress is "the publisher responded", not "the publisher succeeded". A topic answering
        // NOT_FOUND is reachable and is being talked to, so a run of failures spread over longer
        // than the budget must not be read as a stall — the repair they provoke is the writer
        // working, not waiting. Left unpinned, a mutant that stamped only successes survived the
        // whole class.
        List<SettableApiFuture<String>> futures = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            futures.add(neverCompletes());
        }
        PubSubWriter<String> writer =
                new PubSubWriter<>(
                        TestSinkConfigs.forTopic(
                                TOPIC,
                                PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                                // CREATE_IF_NEEDED, so a NOT_FOUND parks for a repair instead of
                                // failing the writer — the drain has to keep waiting to be a test
                                // of waiting at all.
                                CreateDisposition.CREATE_IF_NEEDED,
                                PubSubPublisherOptions.builder()
                                        .publishProgressTimeout(Duration.ofMillis(500))
                                        .build()),
                        factory,
                        admin,
                        mailbox,
                        metrics,
                        UNUSED_RECOVERY);
        for (int i = 0; i < 3; i++) {
            writer.write("payload-" + i, CONTEXT);
        }
        for (int i = 0; i < 3; i++) {
            SettableApiFuture<String> future = futures.get(i);
            clock.schedule(
                    () -> future.setException(new StatusRuntimeException(Status.NOT_FOUND)),
                    300L * (i + 1),
                    TimeUnit.MILLISECONDS);
        }

        long start = System.nanoTime();
        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();

        assertThat((System.nanoTime() - start) / 1_000_000)
                .as("the failures really did span more than the budget")
                .isGreaterThan(700);
        assertThat(admin.created).containsExactly(TOPIC);
    }

    @Test
    void unrelatedMailTrafficDelaysTheBudgetButDoesNotDefeatIt() throws Exception {
        // The mailbox belongs to the task, not to this writer, so mails that are not publish
        // completions run through it. The budget is read only once tryYield comes back empty, so
        // such traffic defers the reading — but any gap in it is a reading, and traffic with gaps
        // is what a real task produces. A mail every 20 ms against a 300 ms budget still fails.
        neverCompletes();
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .publishProgressTimeout(Duration.ofMillis(300))
                                .build());
        writer.write("payload", CONTEXT);
        clock.scheduleAtFixedRate(
                () -> mailbox.execute(() -> {}, "unrelated"), 0, 20, TimeUnit.MILLISECONDS);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("No publish to Pub/Sub completed");
    }

    @Test
    void workTheMailboxStillHasToDoIsNotCountedAgainstTheBudget() throws Exception {
        // The other side of the trade, and the reason the budget is read only after tryYield comes
        // back empty. The publisher answers at 100 ms, but its completion mail is queued behind
        // 400 mails of unrelated work, so it does not run for ~2 s. Reading the budget before the
        // yield would expire this wait at 300 ms — failing a job whose only publish had already
        // succeeded, and blaming an unreachable topic for a busy task thread.
        SettableApiFuture<String> future = neverCompletes();
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .publishProgressTimeout(Duration.ofMillis(300))
                                .build());
        writer.write("payload", CONTEXT);
        for (int i = 0; i < 400; i++) {
            mailbox.execute(() -> Thread.sleep(5), "unrelated");
        }
        clock.schedule(() -> future.set("message"), 100, TimeUnit.MILLISECONDS);

        long start = System.nanoTime();
        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs)
                .as("the flush really did outlast the budget working through the backlog")
                .isGreaterThan(1_000);
    }

    @Test
    void anIdleWriterDoesNotStartItsNextWaitWithASpentBudget() throws Exception {
        // The budget runs from the later of "this wait began" and "a publish last completed". Were
        // it only the latter, a sink that published nothing for longer than the budget — an idle
        // stream between checkpoints — would fail on its very next flush.
        SettableApiFuture<String> first = neverCompletes();
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .publishProgressTimeout(Duration.ofSeconds(1))
                                .build());
        writer.write("early", CONTEXT);
        first.set("message");
        writer.flush(false);

        // Idle for well over the budget, with nothing in flight to restamp the clock.
        Thread.sleep(2_000);

        SettableApiFuture<String> later = neverCompletes();
        writer.write("late", CONTEXT);
        clock.schedule(() -> later.set("message"), 50, TimeUnit.MILLISECONDS);
        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
    }

    @Test
    void anInterruptEndsTheWaitRatherThanBeingSpunOnUntilTheBudgetExpires() throws Exception {
        // LockSupport.parkNanos returns on interrupt without throwing and without clearing the
        // flag, so the loop has to read it. Without that read a cancelling task would keep spinning
        // here until the budget ran out — up to ten minutes at the default. The budget here is 5 s
        // rather than that default so a regression fails in five seconds instead of ten minutes:
        // JUnit's @Timeout does not preempt, it reports afterwards.
        neverCompletes();
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .publishProgressTimeout(Duration.ofSeconds(5))
                                .build());
        writer.write("payload", CONTEXT);

        Thread waiting = Thread.currentThread();
        clock.schedule(waiting::interrupt, 200, TimeUnit.MILLISECONDS);
        long start = System.nanoTime();
        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(InterruptedException.class);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs)
                .as("the interrupt ended it, not the five-second budget")
                .isLessThan(2_000);
        assertThat(Thread.interrupted()).as("the flag was consumed by the throw").isFalse();
    }

    @Test
    void aNonPositiveBudgetIsRejectedWhereItIsReliedOn() throws Exception {
        // Deserialization does not run the builder, so the writer re-checks what it relies on — the
        // same guard the two in-flight caps carry. Forged on a builder-built instance and never on
        // defaults(), which hands out a JVM-wide singleton a reflective write would poison for the
        // rest of the fork (#316).
        PubSubPublisherOptions options = PubSubPublisherOptions.builder().build();
        Field field = PubSubPublisherOptions.class.getDeclaredField("publishProgressTimeout");
        field.setAccessible(true);
        // null is the value that matters: the field was added under an unchanged
        // serialVersionUID, so an older stream carries no value for it and the builder never ran.
        for (Duration forged : new Duration[] {Duration.ZERO, Duration.ofSeconds(-1), null}) {
            field.set(options, forged);
            assertThatThrownBy(() -> newWriter(options))
                    .as("forged %s", forged)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("publishProgressTimeout");
        }

        assertThat(PubSubPublisherOptions.defaults())
                .as("the shared defaults() singleton was not the thing forged")
                .isEqualTo(PubSubPublisherOptions.builder().build());
    }

    /**
     * The ceiling half of that guard, which had no test: the constructor's {@code toNanos()} is
     * what it protects, so a forged budget past it would throw {@code ArithmeticException} here
     * rather than the argument failure the floor case gets (#334; ADR-0068). Same forge, same
     * singleton assertion, and it pins the year count the message carries — {@code
     * Duration.toString()} alone renders the ceiling as an hour count nobody reads.
     */
    @Test
    void aBudgetTooLargeForNanosecondsIsRejectedWhereItIsReliedOn() throws Exception {
        PubSubPublisherOptions options = PubSubPublisherOptions.builder().build();
        Field field = PubSubPublisherOptions.class.getDeclaredField("publishProgressTimeout");
        field.setAccessible(true);
        field.set(options, Duration.ofNanos(Long.MAX_VALUE).plusNanos(1));

        assertThatThrownBy(() -> newWriter(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("publishProgressTimeout must be at most")
                .hasMessageContaining("292 years");

        assertThat(PubSubPublisherOptions.defaults())
                .as("the shared defaults() singleton was not the thing forged")
                .isEqualTo(PubSubPublisherOptions.builder().build());
    }
}
