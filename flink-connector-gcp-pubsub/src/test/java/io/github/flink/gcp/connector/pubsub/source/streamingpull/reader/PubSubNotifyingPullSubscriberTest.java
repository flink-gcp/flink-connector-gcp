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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.util.ExceptionUtils;

import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the client lifecycle {@link PubSubNotifyingPullSubscriber} drives — the paths a working
 * client never takes, and which nothing reached before #325 made the three operations injectable:
 * the client that fails to start, the one that never terminates, and the one that reports at
 * teardown a failure this subscriber has already delivered.
 *
 * <p>The message path is covered by the emulator ITCases through the production factory; what is
 * here is what only a misbehaving client produces.
 */
@Timeout(30)
class PubSubNotifyingPullSubscriberTest {

    private static final SubscriptionDestination SUBSCRIPTION =
            SubscriptionDestination.of("project", "orders");
    private static final String SPLIT_ID = "0";
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(7);

    private final RecordingAckTracker ackTracker = new RecordingAckTracker();
    private final List<String> calls = new ArrayList<>();

    @Test
    void aPermanentFailureReachesTheReaderThroughPullMessages() throws Exception {
        ScriptedClient client = new ScriptedClient();
        PubSubNotifyingPullSubscriber subscriber = subscriberOf(client);
        IllegalStateException boom = new IllegalStateException("the streaming pull gave up");

        client.fail(boom);

        // The wake-up is load-bearing and easy to lose: the fetcher parks on this signal with no
        // timeout, so without it a permanently failed subscriber never reports at all — the thread
        // simply stops asking.
        assertThat(calls).containsExactly("dataAvailable");

        // This is the report the reader consumes and fails the job on. It is what makes the one
        // absorbed below a *re*-report rather than a first one.
        assertThatThrownBy(() -> subscriber.pullMessages(10))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(SUBSCRIPTION.toString())
                .hasCause(boom);
    }

    @Test
    void theFirstFailureIsTheOneReported() throws Exception {
        ScriptedClient client = new ScriptedClient();
        PubSubNotifyingPullSubscriber subscriber = subscriberOf(client);
        IllegalStateException first = new IllegalStateException("the streaming pull gave up");

        client.fail(first);
        client.fail(new IllegalStateException("and the shutdown that followed also failed"));

        // First wins: a later failure is usually a consequence of the first, and the first is the
        // one that explains the job's death.
        assertThatThrownBy(() -> subscriber.pullMessages(10)).hasCause(first);
    }

    @Test
    void absorbsTheClientsReportOfTheFailureItAlreadyDelivered() throws Exception {
        ScriptedClient client = new ScriptedClient();
        PubSubNotifyingPullSubscriber subscriber = subscriberOf(client);
        IllegalStateException boom = new IllegalStateException("the streaming pull gave up");
        client.fail(boom);

        // What Guava's AbstractService.checkCurrentState(TERMINATED) raises on a FAILED service:
        // an IllegalStateException carrying failureCause(), which is the throwable above.
        IllegalStateException report =
                new IllegalStateException(
                        "Expected the service to be TERMINATED, but it FAILED", boom);
        client.failTerminationWith(report);

        try (LogCapture capture = LogCapture.of(PubSubNotifyingPullSubscriber.class)) {
            assertThatCode(subscriber::close).doesNotThrowAnyException();

            // The log is the whole of the report, so it is what has to carry the subscription and
            // the cause — nothing else observes an absorbed failure.
            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.getMessage()).contains(SUBSCRIPTION.toString());
                                assertThat(event.getThrowable()).isSameAs(report);
                            });
        }
    }

    @Test
    void absorbsAClientThatDoesNotTerminateWithinTheTimeout() throws Exception {
        ScriptedClient client = new ScriptedClient();
        PubSubNotifyingPullSubscriber subscriber = subscriberOf(client);
        TimeoutException expired = new TimeoutException("still shutting down");
        client.failTerminationWith(expired);

        try (LogCapture capture = LogCapture.of(PubSubNotifyingPullSubscriber.class)) {
            assertThatCode(subscriber::close).doesNotThrowAnyException();

            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(event -> assertThat(event.getThrowable()).isSameAs(expired));
        }
    }

    @Test
    void waitsOutTheShutdownForTheConfiguredTimeout() throws Exception {
        ScriptedClient client = new ScriptedClient();

        subscriberOf(client).close();

        // The budget is a knob (PubSubSubscriberOptions.shutdownTimeout), so a hand-off that
        // dropped it would leave the client's own default in force and look identical.
        assertThat(client.awaitedMillis).containsExactly(SHUTDOWN_TIMEOUT.toMillis());
        assertThat(client.awaitedUnits).containsExactly(TimeUnit.MILLISECONDS);
    }

    @Test
    void closeNacksAndStopsBeforeItWaits() throws Exception {
        ScriptedClient client = new ScriptedClient();

        subscriberOf(client).close();

        // Order, not just occurrence: waiting before asking the client to stop would spend the
        // whole budget on a client that had not been told to shut down, and nacking after the wait
        // would hold the split's messages for its duration.
        assertThat(calls).containsExactly("nackSplit", "stopAsync", "awaitTerminated");
    }

    @Test
    void aSecondShutdownNeitherNacksNorStopsAgain() throws Exception {
        ScriptedClient client = new ScriptedClient();
        PubSubNotifyingPullSubscriber subscriber = subscriberOf(client);

        subscriber.shutdown();
        subscriber.shutdown();
        // The reader shuts every split down and then closes every one, so close() meets an
        // already-shut-down subscriber on the ordinary path and must only wait.
        subscriber.close();

        assertThat(calls).containsExactly("nackSplit", "stopAsync", "awaitTerminated");
    }

    @Test
    void stopsTheClientWhenItFailsToStart() {
        ScriptedClient client = new ScriptedClient();
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
        ScriptedClient client = new ScriptedClient();
        NoClassDefFoundError missing = new NoClassDefFoundError("io/grpc/netty/shaded/Absent");
        client.failStartWith(missing);

        // Rethrown unchanged, not wrapped: an Error is not an IOException, and it is the type
        // Flink's escalation reads.
        assertThatThrownBy(() -> subscriberOf(client)).isSameAs(missing);

        // The Error takes the same release as the exception above — #324's reason for the sibling
        // guard in DefaultMutationBatcherFactory.create. Same caveat as that test's.
        assertThat(calls).containsExactly("stopAsync", "awaitTerminated");
    }

    private PubSubNotifyingPullSubscriber subscriberOf(ScriptedClient client) throws IOException {
        return new PubSubNotifyingPullSubscriber(
                SPLIT_ID,
                SUBSCRIPTION,
                ackTracker,
                () -> calls.add("dataAvailable"),
                SHUTDOWN_TIMEOUT,
                client::start,
                client::stopAsync,
                client::awaitTerminated);
    }

    /**
     * A client whose three lifecycle operations can be made to misbehave, standing in for the SDK
     * {@code Subscriber} — which cannot be subclassed, its only constructor being private.
     */
    private final class ScriptedClient {

        private final List<Long> awaitedMillis = new ArrayList<>();
        private final List<TimeUnit> awaitedUnits = new ArrayList<>();

        @Nullable private Consumer<Throwable> onPermanentFailure;
        @Nullable private Throwable startFailure;
        @Nullable private Throwable terminationFailure;

        /** Takes a {@link Throwable} so a test can script an {@link Error} as well. */
        void failStartWith(Throwable failure) {
            this.startFailure = failure;
        }

        void failTerminationWith(Throwable failure) {
            this.terminationFailure = failure;
        }

        /** Delivers a permanent failure the way the client's own service listener would. */
        void fail(Throwable failure) {
            requireStarted().accept(failure);
        }

        void start(Consumer<Throwable> onPermanentFailure) {
            this.onPermanentFailure = onPermanentFailure;
            if (startFailure != null) {
                ExceptionUtils.rethrow(startFailure);
            }
        }

        void stopAsync() {
            calls.add("stopAsync");
        }

        void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {
            calls.add("awaitTerminated");
            awaitedMillis.add(unit.toMillis(timeout));
            awaitedUnits.add(unit);
            if (terminationFailure instanceof TimeoutException) {
                throw (TimeoutException) terminationFailure;
            }
            if (terminationFailure instanceof RuntimeException) {
                throw (RuntimeException) terminationFailure;
            }
        }

        private Consumer<Throwable> requireStarted() {
            if (onPermanentFailure == null) {
                throw new IllegalStateException("the subscriber was never started");
            }
            return onPermanentFailure;
        }
    }

    /** Records the one call this class makes, and answers the rest with nothing. */
    private final class RecordingAckTracker implements AckTracker {

        @Override
        public void addPendingAck(String splitId, String messageId, AckHandle ackHandle) {}

        @Override
        public void stagePendingAck(String splitId, String messageId) {}

        @Override
        public void ackPendingImmediately(String splitId, String messageId) {}

        @Override
        public void nackPendingImmediately(String splitId, String messageId) {}

        @Override
        public void addCheckpoint(long checkpointId) {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void nackSplit(String splitId) {
            calls.add("nackSplit");
        }
    }
}
