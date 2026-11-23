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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.PubSubShutdownResidue;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriberBufferLimits;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriberBufferLimitExceededEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the client lifecycle {@link StreamingPullSubscriber} drives — the paths a working
 * client never takes, and which nothing reached before #325 made the three operations injectable:
 * the client that fails to start, and the shutdown ordering around a nack that throws. What the
 * teardown itself logs and counts on those paths lives in {@link SubscriberTeardownTest}, the
 * teardown having been extracted to {@link SubscriberTeardown}.
 *
 * <p>The message path is covered by the emulator ITCases through the production factory; what is
 * here is what only a misbehaving client produces.
 */
@Timeout(30)
class StreamingPullSubscriberTest {

    private static final SubscriptionDestination SUBSCRIPTION =
            SubscriptionDestination.of("project", "orders");
    private static final String SPLIT_ID = "0";
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(7);

    private final List<String> calls = new ArrayList<>();
    private final RecordingAckTracker ackTracker = new RecordingAckTracker(calls);

    /**
     * A stuck client's expired wait counts into a process-wide residue, which two tests here assert
     * absolutely. Before and after, so the fork is also left as it was found for the other classes
     * that assert on those counts.
     */
    @BeforeEach
    void clearTheResidues() {
        PubSubShutdownResidue.resetForTests();
    }

    @AfterEach
    void clearTheResiduesAgain() {
        PubSubShutdownResidue.resetForTests();
    }

    @Test
    void aPermanentFailureReachesTheReaderThroughPullMessages() throws Exception {
        ScriptedClient client = new ScriptedClient(calls);
        StreamingPullSubscriber subscriber = subscriberOf(client);
        IllegalStateException boom = new IllegalStateException("the streaming pull gave up");

        client.fail(boom);

        // The wake-up is load-bearing and easy to lose: the fetcher parks on this signal with no
        // timeout, so without it a permanently failed subscriber never reports at all — the thread
        // simply stops asking.
        assertThat(calls).containsExactly("dataAvailable");

        // This is the report the reader consumes and fails the job on. It is what makes the one
        // SubscriberTeardownTest absorbs a *re*-report rather than a first one.
        assertThatThrownBy(() -> subscriber.pullMessages(10))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(SUBSCRIPTION.toString())
                .hasCause(boom);
    }

    @Test
    void theFirstFailureIsTheOneReported() throws Exception {
        ScriptedClient client = new ScriptedClient(calls);
        StreamingPullSubscriber subscriber = subscriberOf(client);
        IllegalStateException first = new IllegalStateException("the streaming pull gave up");

        client.fail(first);
        client.fail(new IllegalStateException("and the shutdown that followed also failed"));

        // First wins: a later failure is usually a consequence of the first, and the first is the
        // one that explains the job's death.
        assertThatThrownBy(() -> subscriber.pullMessages(10)).hasCause(first);
    }

    @Test
    void aThrowingNackStillLeavesTheClientAskedToStopAndWaitedOut() throws Exception {
        // #350: closed is set before the nack, so a nack that threw used to leave shutdown()
        // claiming the client had been asked to stop. close() then returned at the idempotence
        // guard, awaitTerminated spent the whole budget on a client nobody had told to stop, and a
        // WARN about an unclean shutdown was its only trace. Measured on google-cloud-pubsub
        // 1.152.0, the production AckHandle cannot in fact throw — nack() ends in
        // SettableApiFuture.set, which returns a boolean — so this is robustness over an SPI whose
        // implementations need not all be ours, argued as that rather than as a bug closed.
        ScriptedClient client = new ScriptedClient(calls);
        StreamingPullSubscriber subscriber = subscriberOf(client);
        RuntimeException refused = new RuntimeException("the consumer was already settled");
        ackTracker.failNackWith(refused);

        assertThatThrownBy(subscriber::close).isSameAs(refused);

        // Both halves, and neither held before: the stop ran although the nack failed, and the
        // wait ran although the shutdown failed.
        assertThat(calls)
                .containsExactly("nackSplit", "stopAsync", "awaitTerminated", "releaseTransport");
    }

    @Test
    void waitsOutTheShutdownForTheConfiguredTimeout() throws Exception {
        ScriptedClient client = new ScriptedClient(calls);
        StreamingPullSubscriber subscriber = subscriberOf(client);

        long before = System.nanoTime();
        subscriber.close();
        long elapsed = System.nanoTime() - before;

        // The budget is a knob (PubSubSubscriberOptions.shutdownTimeout), so a hand-off that
        // dropped it would leave the client's own default in force and look identical. A range
        // rather than the exact value, because the clock starts in shutdown(), which close() runs
        // before the wait (#2302): the wait gets what is left.
        assertThat(client.awaitedNanos)
                .singleElement()
                .satisfies(
                        nanos ->
                                assertThat(nanos)
                                        .isBetween(
                                                SHUTDOWN_TIMEOUT.toNanos() - elapsed,
                                                SHUTDOWN_TIMEOUT.toNanos()));
        assertThat(client.awaitedUnits).containsExactly(TimeUnit.NANOSECONDS);
    }

    @Test
    void stuckSubscribersShutDownTogetherShareOneBudget() throws Exception {
        // #2302: the reader shuts every subscriber down before it closes any, so that their waits
        // overlap. They overlapped only for clients that did stop: each close() used to wait a
        // full budget from the moment its turn came, so N stuck clients cost N budgets, and seven
        // at the 5 s default outlast source.reader.close.timeout.
        Duration budget = Duration.ofMillis(300);
        ScriptedClient first = new ScriptedClient(calls);
        ScriptedClient second = new ScriptedClient(calls);
        first.neverTerminate();
        second.neverTerminate();
        StreamingPullSubscriber firstSubscriber = subscriberOf(first, budget);
        StreamingPullSubscriber secondSubscriber = subscriberOf(second, budget);

        long shutdownsBegan = System.nanoTime();
        firstSubscriber.shutdown();
        secondSubscriber.shutdown();
        long shutdownsEnded = System.nanoTime();
        firstSubscriber.close();
        secondSubscriber.close();

        // The first wait sleeps until its own deadline, which is at least one budget after
        // shutdownsBegan. The second deadline is at most one budget after shutdownsEnded, so all
        // the second wait can have left is the time the two shutdowns took. A full budget there —
        // a clock started by the wait, or restarted by it — fails.
        assertThat(second.awaitedNanos)
                .singleElement()
                .satisfies(n -> assertThat(n).isLessThanOrEqualTo(shutdownsEnded - shutdownsBegan));
        // Both are still expired waits, each counted: the second was asked, with nothing left,
        // rather than skipped.
        assertThat(PubSubShutdownResidue.SUBSCRIBER_SHUTDOWNS_ABANDONED.sum()).isEqualTo(2);
    }

    @Test
    void aClientThatStoppedWhileAnotherWaitExpiredIsNotCalledAbandoned() throws Exception {
        // With the budget shared, a wait that follows an expired one has nothing left. It must
        // still ask the client, which returns at once if it has terminated, rather than count
        // the subscriber as abandoned without asking.
        Duration budget = Duration.ofMillis(300);
        ScriptedClient stuck = new ScriptedClient(calls);
        ScriptedClient stopped = new ScriptedClient(calls);
        stuck.neverTerminate();
        StreamingPullSubscriber stuckSubscriber = subscriberOf(stuck, budget);
        StreamingPullSubscriber stoppedSubscriber = subscriberOf(stopped, budget);

        // The stopped client's clock starts first, so its deadline is passed by the time the
        // stuck client's wait has slept to its own: the wait it then gets is exactly zero.
        stoppedSubscriber.shutdown();
        stuckSubscriber.shutdown();
        stuckSubscriber.close();
        stoppedSubscriber.close();

        assertThat(stopped.awaitedNanos).containsExactly(0L);
        assertThat(PubSubShutdownResidue.SUBSCRIBER_SHUTDOWNS_ABANDONED.sum()).isEqualTo(1);
    }

    @ParameterizedTest
    @MethodSource("budgetsOutsideTheOptionsRange")
    void aBudgetOutsideTheOptionsRangeIsRefusedBeforeTheClientOpens(Duration outOfRange) {
        // The setter refuses both, so only an options object that never ran the setter carries one.
        // The teardown converts the budget to nanoseconds, and a conversion that threw after the
        // client opened, past either end of the range, would leave its transport with nothing to
        // close it.
        List<String> opened = new ArrayList<>();
        SubscriberFactory factory =
                (subscription, consumer) -> {
                    opened.add("create");
                    return new ScriptedClient(calls);
                };

        assertThatThrownBy(
                        () ->
                                new StreamingPullSubscriber(
                                        SPLIT_ID,
                                        SUBSCRIPTION,
                                        factory,
                                        ackTracker,
                                        () -> {},
                                        outOfRange,
                                        SubscriberBufferBudget.unbounded()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownTimeout");
        assertThat(opened).isEmpty();
    }

    static Stream<Duration> budgetsOutsideTheOptionsRange() {
        return Stream.of(
                Duration.ofNanos(Long.MAX_VALUE).plusNanos(1),
                Duration.ofNanos(Long.MIN_VALUE).minusNanos(1),
                // Expressible, but under the floor, which the failed-start release relies on: it
                // spends the budget as await(toMillis()), and this one would wait for nothing.
                Duration.ofNanos(500_000));
    }

    @Test
    void theLargestBudgetIsNotSpentTheInstantItStarts() throws Exception {
        // The deadline is nanoTime() plus the budget, which overflows at the largest budget the
        // option accepts; the subtraction that reads it wraps back. Clamping the remainder instead
        // hands the client a wait of zero, and saturating the deadline cuts the budget by the
        // clock's origin, which on Linux is the uptime.
        Duration largest = Duration.ofNanos(Long.MAX_VALUE);
        ScriptedClient client = new ScriptedClient(calls);
        StreamingPullSubscriber subscriber = subscriberOf(client, largest);

        long before = System.nanoTime();
        subscriber.close();
        long elapsed = System.nanoTime() - before;

        assertThat(client.awaitedNanos)
                .singleElement()
                .satisfies(n -> assertThat(n).isGreaterThanOrEqualTo(Long.MAX_VALUE - elapsed));
    }

    @Test
    void closeNacksAndStopsBeforeItWaits() throws Exception {
        ScriptedClient client = new ScriptedClient(calls);

        subscriberOf(client).close();

        // Order, not just occurrence: waiting before asking the client to stop would spend the
        // whole budget on a client that had not been told to shut down, and nacking after the wait
        // would hold the split's messages for its duration. The transport goes last: releasing it
        // before the wait would stop the event-loop group under channels the client still uses
        // (ADR-0208).
        assertThat(calls)
                .containsExactly("nackSplit", "stopAsync", "awaitTerminated", "releaseTransport");
    }

    @Test
    void aSecondShutdownNeitherNacksNorStopsAgain() throws Exception {
        ScriptedClient client = new ScriptedClient(calls);
        StreamingPullSubscriber subscriber = subscriberOf(client);

        subscriber.shutdown();
        subscriber.shutdown();
        // The reader shuts every split down and then closes every one, so close() meets an
        // already-shut-down subscriber on the ordinary path and must only wait.
        subscriber.close();

        assertThat(calls)
                .containsExactly("nackSplit", "stopAsync", "awaitTerminated", "releaseTransport");
    }

    @Test
    void stopsTheClientWhenItFailsToStart() {
        ScriptedClient client = new ScriptedClient(calls);
        IllegalStateException refused =
                new IllegalStateException("could not reach the subscription");
        client.failStartWith(refused);

        assertThatThrownBy(() -> subscriberOf(client))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(SUBSCRIPTION.toString())
                .hasCause(refused);

        // The client is asked to stop before the failure is reported. Read this as pinning the
        // call, not a leak-free restart: against the real SDK a failed start leaves the service
        // FAILED, where Guava's stopAsync() is a no-op — startOrRelease's javadoc has the
        // measurement. The split is nacked first, in shutdown()'s order, because a client that
        // failed to start may already have delivered (#2307). The transport is released too,
        // since no caller is left holding this subscriber to release it.
        assertThat(calls)
                .containsExactly("nackSplit", "stopAsync", "awaitTerminated", "releaseTransport");
    }

    @Test
    void stopsTheClientWhenItsFirstClassloadFails() {
        ScriptedClient client = new ScriptedClient(calls);
        NoClassDefFoundError missing = new NoClassDefFoundError("io/grpc/netty/shaded/Absent");
        client.failStartWith(missing);

        // Rethrown unchanged, not wrapped: an Error is not an IOException, and it is the type
        // Flink's escalation reads.
        assertThatThrownBy(() -> subscriberOf(client)).isSameAs(missing);

        // The Error takes the same release as the exception above — the same rule as the sibling
        // guard in DefaultMutationBatcherFactory.create (#324). Same caveat as that test's.
        assertThat(calls)
                .containsExactly("nackSplit", "stopAsync", "awaitTerminated", "releaseTransport");
    }

    @Test
    void aFailedStartReturnsWhatItHadAlreadyAdmitted() {
        // #2307: a connection is RUNNING once it has started its stream, so it can deliver while
        // the
        // subscriber is still STARTING, and a later connection that fails then fails the start
        // with those deliveries already admitted. Measured against the real SDK through a fake
        // Subscriber service; scripted here, through the production constructor, since only it
        // hands the client the receiver. A real tracker, because the recording one cannot show
        // that a handle was settled or that the split's entries are gone. A derived budget with
        // a registered peer, because only the peer's cap shows that the failed split also left
        // the starting holders: usage reads zero whether it did or not.
        PubSubAckTracker tracker = new PubSubAckTracker(new TestReaderMetrics().metrics(), null);
        SubscriberBufferBudget budget =
                new SubscriberBufferBudget(
                        SubscriberBufferLimits.of(
                                PubSubSubscriberOptions.builder()
                                        .flowControlMaxOutstandingElementCount(2)
                                        .flowControlMaxOutstandingRequestBytes(1_000)
                                        .build()),
                        event -> {});
        budget.register("peer", () -> {});
        RecordingAckHandle first = new RecordingAckHandle("1");
        RecordingAckHandle second = new RecordingAckHandle("2");
        IllegalStateException refused = new IllegalStateException("a later stream was refused");
        DeliveringClient client = new DeliveringClient(calls);
        client.onStart =
                () -> {
                    client.deliver(message("1", "x"), first);
                    client.deliver(message("2", "xxxx"), second);
                    throw refused;
                };

        assertThatThrownBy(() -> subscriberOf(client, tracker, budget))
                .isInstanceOf(IOException.class)
                .hasCause(refused);

        // Before #2307 the budget kept both reservations, the tracker both handles, and neither
        // handle was settled by the connector.
        assertThat(budget.usage().messages()).isZero();
        assertThat(budget.usage().bytes()).isZero();
        assertThat(tracker.outstandingAckCount()).isZero();
        assertThat(first.nackCount()).isEqualTo(1);
        assertThat(second.nackCount()).isEqualTo(1);
        // The peer is alone again, so its own derived share of 4 is the cap, not two shares' 8.
        for (int i = 0; i < 4; i++) {
            assertThat(budget.tryReserve("peer", 1).isAdmitted()).isTrue();
        }
        assertThat(budget.tryReserve("peer", 1).isAdmitted()).isFalse();
    }

    @Test
    void aDeliveryAfterAFailedStartIsNackedWithoutReserving() {
        // The failed start's release latches closed, so a delivery that still arrives — from a
        // connection the SDK has not stopped yet — is nacked instead of admitted to a subscriber
        // nothing will drain. Two of them against a one-message budget, so that admitting them
        // would also cross the limit and report an overflow alongside the start failure. One
        // arrives while the client is being stopped, one after the constructor has thrown.
        List<SubscriberBufferLimitExceededEvent> events = new ArrayList<>();
        PubSubAckTracker tracker = new PubSubAckTracker(new TestReaderMetrics().metrics(), null);
        SubscriberBufferBudget budget = new SubscriberBufferBudget(1, 1 << 20, events::add);
        RecordingAckHandle duringStop = new RecordingAckHandle("1");
        RecordingAckHandle afterwards = new RecordingAckHandle("2");
        DeliveringClient client = new DeliveringClient(calls);
        client.onStart =
                () -> {
                    throw new IllegalStateException("the subscription is gone");
                };
        client.onStop = () -> client.deliver(message("1", "x"), duringStop);

        assertThatThrownBy(() -> subscriberOf(client, tracker, budget))
                .isInstanceOf(IOException.class);
        client.deliver(message("2", "x"), afterwards);

        assertThat(duringStop.nackCount()).isEqualTo(1);
        assertThat(afterwards.nackCount()).isEqualTo(1);
        assertThat(budget.usage().messages()).isZero();
        assertThat(tracker.outstandingAckCount()).isZero();
        assertThat(events).isEmpty();
    }

    @Test
    void aThrowingNackOnAFailedStartStillStopsAndReleasesTheClient() {
        // The same list rule as shutdown() (#350), here under closeAllSuppressing: the nack's
        // failure rides on the start failure rather than replacing it, and the stop, the wait and
        // the transport release still run.
        ScriptedClient client = new ScriptedClient(calls);
        IllegalStateException refused =
                new IllegalStateException("could not reach the subscription");
        client.failStartWith(refused);
        RuntimeException nackRefused = new RuntimeException("the consumer was already settled");
        ackTracker.failNackWith(nackRefused);

        assertThatThrownBy(() -> subscriberOf(client))
                .isInstanceOf(IOException.class)
                .hasCause(refused);

        assertThat(refused.getSuppressed()).containsExactly(nackRefused);
        assertThat(calls)
                .containsExactly("nackSplit", "stopAsync", "awaitTerminated", "releaseTransport");
    }

    @Test
    void reportsWhatIsBufferedInBothDimensions() throws Exception {
        // Messages of different sizes, deliberately: an accounting that counted messages where it
        // means bytes passes every same-size test there is.
        StreamingPullSubscriber subscriber = subscriberOf(new ScriptedClient(calls));
        PubsubMessage small = message("s", "x");
        PubsubMessage large = message("l", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");

        subscriber.receiveMessage(small, new RecordingAckHandle("s"));
        subscriber.receiveMessage(large, new RecordingAckHandle("l"));

        assertThat(subscriber.bufferUsage().messages()).isEqualTo(2);
        assertThat(subscriber.bufferUsage().bytes())
                .isEqualTo(small.getSerializedSize() + large.getSerializedSize());
    }

    @Test
    void aDrainReleasesExactlyWhatItRemoved() throws Exception {
        // The half that decides whether the reader's bound is a bound at all: without the drain's
        // subtraction the load only ever grows, so a busy split would eventually be parked the
        // moment it was paused.
        SubscriberBufferBudget budget = new SubscriberBufferBudget(10, Long.MAX_VALUE, event -> {});
        StreamingPullSubscriber subscriber = subscriberOf(new ScriptedClient(calls), budget);
        PubsubMessage first = message("1", "x");
        PubsubMessage second = message("2", "xxxxxxxxxx");
        subscriber.receiveMessage(first, new RecordingAckHandle("1"));
        subscriber.receiveMessage(second, new RecordingAckHandle("2"));

        assertThat(subscriber.pullMessages(1)).containsExactly(first);

        assertThat(subscriber.bufferUsage().messages()).isEqualTo(1);
        assertThat(subscriber.bufferUsage().bytes()).isEqualTo(second.getSerializedSize());
        assertThat(budget.usage().messages()).isEqualTo(1);
        assertThat(budget.usage().bytes()).isEqualTo(second.getSerializedSize());
    }

    @Test
    void drainingPublishesDequeAndBudgetSpaceAtomicallyToCallbacks() throws Exception {
        List<SubscriberBufferLimitExceededEvent> events = new ArrayList<>();
        SubscriberBufferBudget budget = new SubscriberBufferBudget(1, Long.MAX_VALUE, events::add);
        StreamingPullSubscriber subscriber = subscriberOf(new ScriptedClient(calls), budget);
        budget.register(SPLIT_ID, subscriber::requestStop);
        PubsubMessage first = message("1", "x");
        PubsubMessage second = message("2", "y");
        subscriber.receiveMessage(first, new RecordingAckHandle("1"));
        AtomicReference<List<PubsubMessage>> drained = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread puller =
                new Thread(
                        () -> {
                            try {
                                drained.set(subscriber.pullMessages(1));
                            } catch (Throwable thrown) {
                                failure.compareAndSet(null, thrown);
                            }
                        },
                        "subscriber-budget-puller");
        Thread receiver =
                new Thread(
                        () -> {
                            try {
                                subscriber.receiveMessage(second, new RecordingAckHandle("2"));
                            } catch (Throwable thrown) {
                                failure.compareAndSet(null, thrown);
                            }
                        },
                        "subscriber-budget-receiver");

        boolean callbackWaitedForPuller;
        synchronized (budget) {
            puller.start();
            assertThat(waitUntilBlocked(puller, Thread.currentThread().getId())).isTrue();
            receiver.start();
            callbackWaitedForPuller = waitUntilBlocked(receiver, puller.getId());
        }
        puller.join(TimeUnit.SECONDS.toMillis(5));
        receiver.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(puller.isAlive()).isFalse();
        assertThat(receiver.isAlive()).isFalse();
        assertThat(failure.get()).isNull();
        assertThat(callbackWaitedForPuller)
                .as("the pull retains the subscriber monitor until its budget release")
                .isTrue();
        assertThat(drained.get()).containsExactly(first);
        assertThat(subscriber.pullMessages(1)).containsExactly(second);
        assertThat(events).isEmpty();
    }

    @Test
    void rejectsAndStopsBeforeRetainingPastTheReaderBudget() throws Exception {
        List<SubscriberBufferLimitExceededEvent> events = new ArrayList<>();
        ScriptedClient client = new ScriptedClient(calls);
        SubscriberBufferBudget budget = new SubscriberBufferBudget(1, Long.MAX_VALUE, events::add);
        StreamingPullSubscriber subscriber = subscriberOf(client, budget);
        budget.register(SPLIT_ID, client::stopAsync);
        PubsubMessage admitted = message("1", "x");
        RecordingAckHandle rejected = new RecordingAckHandle("2");

        subscriber.receiveMessage(admitted, new RecordingAckHandle("1"));
        subscriber.receiveMessage(message("2", "y"), rejected);

        assertThat(subscriber.bufferUsage().messages()).isEqualTo(1);
        assertThat(subscriber.pullMessages(10)).containsExactly(admitted);
        assertThat(ackTracker.pendingAckCount())
                .as("the rejected delivery never entered the checkpoint acknowledgement state")
                .isEqualTo(1);
        assertThat(rejected.isNacked()).isTrue();
        assertThat(events).hasSize(1);
        assertThat(calls)
                .containsExactly("dataAvailable", "stopAsync", "nackReceived", "dataAvailable");
    }

    @Test
    void shutdownEmptiesTheUsageItReports() throws Exception {
        SubscriberBufferBudget budget =
                new SubscriberBufferBudget(Long.MAX_VALUE, Long.MAX_VALUE, event -> {});
        StreamingPullSubscriber subscriber = subscriberOf(new ScriptedClient(calls), budget);
        PubsubMessage buffered = message("1", "xxxxx");
        subscriber.receiveMessage(buffered, new RecordingAckHandle("1"));

        subscriber.shutdown();

        // The buffer is discarded and nacked by the shutdown, so what a parked subscriber reports
        // holding is nothing.
        assertThat(subscriber.bufferUsage().messages()).isZero();
        assertThat(subscriber.bufferUsage().bytes()).isZero();
        assertThat(budget.usage().messages()).isZero();
        assertThat(budget.usage().bytes()).isZero();
    }

    @Test
    void anOversizedMessageArrivingAfterShutdownIsNackedWithoutAFalseLimitResponse()
            throws Exception {
        List<SubscriberBufferLimitExceededEvent> events = new ArrayList<>();
        SubscriberBufferBudget budget = new SubscriberBufferBudget(1, 1, events::add);
        StreamingPullSubscriber subscriber = subscriberOf(new ScriptedClient(calls), budget);
        subscriber.shutdown();
        RecordingAckHandle late = new RecordingAckHandle("late");

        subscriber.receiveMessage(message("1", "xxxxx"), late);

        assertThat(subscriber.bufferUsage().messages()).isZero();
        assertThat(subscriber.bufferUsage().bytes()).isZero();
        assertThat(budget.usage().messages()).isZero();
        assertThat(budget.usage().bytes()).isZero();
        assertThat(late.isNacked()).isTrue();
        assertThat(events).isEmpty();
    }

    private static PubsubMessage message(String messageId, String payload) {
        return PubsubMessage.newBuilder()
                .setMessageId(messageId)
                .setData(ByteString.copyFromUtf8(payload))
                .build();
    }

    private static boolean waitUntilBlocked(Thread thread, long expectedOwnerId)
            throws InterruptedException {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            ThreadInfo info = threads.getThreadInfo(thread.getId());
            if (info != null
                    && info.getThreadState() == Thread.State.BLOCKED
                    && info.getLockOwnerId() == expectedOwnerId) {
                return true;
            }
            Thread.sleep(1);
        }
        return false;
    }

    private StreamingPullSubscriber subscriberOf(ScriptedClient client) throws IOException {
        return subscriberOf(client, SubscriberBufferBudget.unbounded());
    }

    private StreamingPullSubscriber subscriberOf(ScriptedClient client, Duration shutdownTimeout)
            throws IOException {
        return subscriberOf(client, SubscriberBufferBudget.unbounded(), shutdownTimeout);
    }

    private StreamingPullSubscriber subscriberOf(
            ScriptedClient client, SubscriberBufferBudget bufferBudget) throws IOException {
        return subscriberOf(client, bufferBudget, SHUTDOWN_TIMEOUT);
    }

    private StreamingPullSubscriber subscriberOf(
            ScriptedClient client, SubscriberBufferBudget bufferBudget, Duration shutdownTimeout)
            throws IOException {
        return new StreamingPullSubscriber(
                SPLIT_ID,
                SUBSCRIPTION,
                ackTracker,
                () -> calls.add("dataAvailable"),
                shutdownTimeout,
                bufferBudget,
                client);
    }

    /** Through the production constructor, the only one that hands the client the receiver. */
    private StreamingPullSubscriber subscriberOf(
            DeliveringClient client, AckTracker tracker, SubscriberBufferBudget bufferBudget)
            throws IOException {
        return new StreamingPullSubscriber(
                SPLIT_ID,
                SUBSCRIPTION,
                (subscription, receiver) -> client.attach(receiver),
                tracker,
                () -> calls.add("dataAvailable"),
                SHUTDOWN_TIMEOUT,
                bufferBudget);
    }

    /**
     * A client that delivers through the receiver it was created with, as the SDK's callback
     * threads do: from inside its start, from inside its stop, or afterwards. {@link
     * ScriptedClient} never sees a receiver, since the seam constructor takes a client already
     * built.
     */
    private static final class DeliveringClient implements SubscriberClient {

        private final List<String> calls;
        @Nullable private SubscriberFactory.MessageConsumer receiver;

        /** Runs inside {@link #start}; a failure it throws is the start's. */
        Runnable onStart = () -> {};

        /** Runs inside the first {@link #stopAsync} only, so a stop it causes cannot recurse. */
        @Nullable Runnable onStop;

        DeliveringClient(List<String> calls) {
            this.calls = calls;
        }

        DeliveringClient attach(SubscriberFactory.MessageConsumer receiver) {
            this.receiver = receiver;
            return this;
        }

        void deliver(PubsubMessage message, AckHandle handle) {
            if (receiver == null) {
                throw new IllegalStateException("the client was never created");
            }
            receiver.receive(message, handle);
        }

        @Override
        public void start(Consumer<Throwable> onPermanentFailure) {
            onStart.run();
        }

        @Override
        public void stopAsync() {
            calls.add("stopAsync");
            Runnable hook = onStop;
            onStop = null;
            if (hook != null) {
                hook.run();
            }
        }

        @Override
        public void awaitTerminated(long timeout, TimeUnit unit) {
            calls.add("awaitTerminated");
        }

        @Override
        public void close() {
            calls.add("releaseTransport");
        }
    }
}
