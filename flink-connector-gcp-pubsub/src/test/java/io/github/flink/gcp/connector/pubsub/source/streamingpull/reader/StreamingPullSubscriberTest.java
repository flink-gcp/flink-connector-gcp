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
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriberBufferLimitExceededEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
        assertThat(calls).containsExactly("nackSplit", "stopAsync", "awaitTerminated");
    }

    @Test
    void waitsOutTheShutdownForTheConfiguredTimeout() throws Exception {
        ScriptedClient client = new ScriptedClient(calls);

        subscriberOf(client).close();

        // The budget is a knob (PubSubSubscriberOptions.shutdownTimeout), so a hand-off that
        // dropped it would leave the client's own default in force and look identical.
        assertThat(client.awaitedMillis).containsExactly(SHUTDOWN_TIMEOUT.toMillis());
        assertThat(client.awaitedUnits).containsExactly(TimeUnit.MILLISECONDS);
    }

    @Test
    void closeNacksAndStopsBeforeItWaits() throws Exception {
        ScriptedClient client = new ScriptedClient(calls);

        subscriberOf(client).close();

        // Order, not just occurrence: waiting before asking the client to stop would spend the
        // whole budget on a client that had not been told to shut down, and nacking after the wait
        // would hold the split's messages for its duration.
        assertThat(calls).containsExactly("nackSplit", "stopAsync", "awaitTerminated");
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

        assertThat(calls).containsExactly("nackSplit", "stopAsync", "awaitTerminated");
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
        // measurement. Nothing was nacked: nothing had been received.
        assertThat(calls).containsExactly("stopAsync", "awaitTerminated");
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
        assertThat(calls).containsExactly("stopAsync", "awaitTerminated");
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

    private StreamingPullSubscriber subscriberOf(
            ScriptedClient client, SubscriberBufferBudget bufferBudget) throws IOException {
        return new StreamingPullSubscriber(
                SPLIT_ID,
                SUBSCRIPTION,
                ackTracker,
                () -> calls.add("dataAvailable"),
                SHUTDOWN_TIMEOUT,
                bufferBudget,
                client::start,
                client::stopAsync,
                client::awaitTerminated);
    }
}
