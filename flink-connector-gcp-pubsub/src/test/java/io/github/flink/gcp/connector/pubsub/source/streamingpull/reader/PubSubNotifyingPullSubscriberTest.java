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

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.PubSubShutdownResidue;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

    /**
     * The fragments the docs WARN table quotes (as {@code … fragment …}) from the four teardown
     * messages, pinned in full so a reword cannot keep these tests green while silently breaking
     * the table (#359). Test-local copies, not the production strings: a production constant would
     * inline here and compare against itself. The {@code doesNotContain} assertions below stay on
     * shorter literals on purpose — for a negative assertion the shorter substring is the stronger
     * check.
     */
    private static final String QUOTED_ABANDONED_WAIT = "did not finish shutting down within";

    private static final String QUOTED_RE_REPORT =
            "reported at shutdown the failure it had already reported to the reader";

    private static final String QUOTED_ONLY_REPORT =
            "failed while shutting down, and this is the only report of it";

    private static final String QUOTED_FAILED_START =
            "did not shut down cleanly after failing to start";

    private final RecordingAckTracker ackTracker = new RecordingAckTracker();
    private final List<String> calls = new ArrayList<>();

    /**
     * Before and after, so this class can assert absolute counts and still leave the fork as it
     * found it — the residues are static, and every other class that asserts on one asserts
     * absolutely too.
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
        // Delivering it is what makes the one below a *re*-report, and this class is where that
        // distinction is decided — so the test has to do what the reader does rather than assume
        // it. Recorded-and-never-read is a different case, covered by
        // aFailureTheStopItselfProducesIsNotCalledARepeat.
        assertThatThrownBy(() -> subscriber.pullMessages(10)).isInstanceOf(IOException.class);

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
                                // The wording, not only the fact of a warning: #351 splits this
                                // catch three ways, and only this branch may say the reader
                                // already has the failure. Saying that of a first report would
                                // send an operator looking for a job failure that is not coming.
                                assertThat(event.getMessage()).contains(QUOTED_RE_REPORT);
                                assertThat(event.getThrowable()).isSameAs(report);
                            });
        }

        // And nothing is counted (#358): the reader has this failure and the job is going down over
        // it, so a series here would be a second report of an incident already reported the loudest
        // way there is. An increment that escaped the else branch below would land on this case.
        assertThat(abandonedShutdowns()).isZero();
        assertThat(unreportedFailures()).isZero();
    }

    @Test
    void reportsAFailureRaisedDuringTheShutdownAsOneNothingElseReports() throws Exception {
        // #351: Subscriber.doStop() runs runShutdown() on a thread of its own under
        // catch (Exception e) { notifyFailed(e); }, so a client that was healthy when shutdown()
        // ran can fail *during* the teardown. That failure reaches the listener with nothing left
        // to read it — pullMessages will not be called again — and arrives here as the same
        // IllegalStateException the re-report above does. Absorbing it is right; reporting it as
        // a repeat of something already handled was not.
        ScriptedClient client = new ScriptedClient();
        PubSubNotifyingPullSubscriber subscriber = subscriberOf(client);
        IllegalStateException raisedAtShutdown =
                new IllegalStateException("the connections would not close");
        client.failTerminationWith(
                new IllegalStateException(
                        "Expected the service to be TERMINATED, but it FAILED", raisedAtShutdown));

        try (LogCapture capture = LogCapture.of(PubSubNotifyingPullSubscriber.class)) {
            assertThatCode(subscriber::close).doesNotThrowAnyException();

            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.getMessage()).contains(SUBSCRIPTION.toString());
                                assertThat(event.getMessage())
                                        .contains(QUOTED_ONLY_REPORT)
                                        .doesNotContain("had already reported to the reader");
                                // The log is the whole of the report on this branch, so it has to
                                // carry the throwable as well as say which case it is.
                                assertThat(event.getThrowable()).hasCause(raisedAtShutdown);
                            });
        }

        // This is the branch a metric exists for (#358): nothing else reports this failure, so
        // without the counter a dashboard cannot show it at all. Counted here and not as an expired
        // wait — the two mean different things to act on.
        assertThat(unreportedFailures()).isEqualTo(1);
        assertThat(abandonedShutdowns()).isZero();
    }

    @Test
    void aFailureTheStopItselfProducesIsNotCalledARepeat() throws Exception {
        // The shape the production path actually produces, and the one a snapshot of
        // permanentError taken in close() would get wrong: the reader's own close runs every
        // subscriber's shutdown() before any close(), so stopAsync() has already run — and the
        // thread doStop() spawns can record its failure through the listener in that window. The
        // failure is then in the field but has been read by nobody.
        ScriptedClient client = new ScriptedClient();
        PubSubNotifyingPullSubscriber subscriber = subscriberOf(client);
        IllegalStateException raisedByTheStop =
                new IllegalStateException("the connections would not close");
        client.failWhenStopped(raisedByTheStop);
        client.failTerminationWith(
                new IllegalStateException(
                        "Expected the service to be TERMINATED, but it FAILED", raisedByTheStop));

        try (LogCapture capture = LogCapture.of(PubSubNotifyingPullSubscriber.class)) {
            subscriber.shutdown();
            assertThatCode(subscriber::close).doesNotThrowAnyException();

            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event ->
                                    assertThat(event.getMessage())
                                            .contains(QUOTED_ONLY_REPORT)
                                            .doesNotContain("had already reported to the reader"));
        }

        assertThat(unreportedFailures()).isEqualTo(1);
        assertThat(abandonedShutdowns()).isZero();
    }

    @Test
    void aDifferentFailureAtShutdownIsNotCalledARepeatOfTheReportedOne() throws Exception {
        // Pins the identity half of the check, which the other tests leave free: here a failure
        // *has* been reported, so a comparison reduced to "something was reported" would call this
        // second, unrelated one a repeat. The SDK is not expected to produce this — Guava keeps the
        // first cause, which the production javadoc argues from — so this defends the code against
        // that argument turning out to be wrong rather than against a case seen in the wild.
        ScriptedClient client = new ScriptedClient();
        PubSubNotifyingPullSubscriber subscriber = subscriberOf(client);
        IllegalStateException reported = new IllegalStateException("the streaming pull gave up");
        client.fail(reported);
        assertThatThrownBy(() -> subscriber.pullMessages(10)).isInstanceOf(IOException.class);

        IllegalStateException unrelated = new IllegalStateException("and the stub would not close");
        client.failTerminationWith(
                new IllegalStateException(
                        "Expected the service to be TERMINATED, but it FAILED", unrelated));

        try (LogCapture capture = LogCapture.of(PubSubNotifyingPullSubscriber.class)) {
            assertThatCode(subscriber::close).doesNotThrowAnyException();

            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event ->
                                    assertThat(event.getMessage())
                                            .contains(QUOTED_ONLY_REPORT)
                                            .doesNotContain("had already reported to the reader"));
        }

        // The counter takes the identity half with it: a check reduced to "something was reported"
        // would call this unrelated failure a repeat, and the metric would be silent about the one
        // case it exists for.
        assertThat(unreportedFailures()).isEqualTo(1);
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
                    .satisfies(
                            event -> {
                                assertThat(event.getThrowable()).isSameAs(expired);
                                // A timeout is neither of the other two, and the message must not
                                // read as either: it reports the budget, which is the knob whose
                                // value an operator meeting this line has to weigh.
                                assertThat(event.getMessage())
                                        .contains(QUOTED_ABANDONED_WAIT)
                                        .contains(SHUTDOWN_TIMEOUT.toString());
                            });
        }

        // The tuning signal, counted apart from the incident above (#358): a rate here says
        // shutdownTimeout is too low for this deployment, which is a different action entirely.
        assertThat(abandonedShutdowns()).isEqualTo(1);
        assertThat(unreportedFailures()).isZero();
    }

    @Test
    void aCleanTeardownCountsNothing() throws Exception {
        ScriptedClient client = new ScriptedClient();

        subscriberOf(client).close();

        // The counters report outcomes, so an increment that escaped its catch block would make
        // every healthy close read on a dashboard exactly like a deployment whose subscribers are
        // failing to shut down.
        assertThat(abandonedShutdowns()).isZero();
        assertThat(unreportedFailures()).isZero();
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
        ScriptedClient client = new ScriptedClient();
        PubSubNotifyingPullSubscriber subscriber = subscriberOf(client);
        RuntimeException refused = new RuntimeException("the consumer was already settled");
        ackTracker.failNackWith(refused);

        assertThatThrownBy(subscriber::close).isSameAs(refused);

        // Both halves, and neither held before: the stop ran although the nack failed, and the
        // wait ran although the shutdown failed.
        assertThat(calls).containsExactly("nackSplit", "stopAsync", "awaitTerminated");
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
    void aFailedStartsTeardownIsReportedAsTheReleaseOfAClientThatNeverRan() throws Exception {
        // The release path has its own message because both of awaitTerminated()'s would be false
        // here. There is no repeat to identify — the start failure is on its way to the caller —
        // and asking would be a race besides: Guava dispatches the failure listener from the SDK's
        // own thread after releasing the monitor awaitRunning() blocks on, so permanentError can
        // still be unset, as it is here. Nor has anything been nacked, shutdown() never having run.
        ScriptedClient client = new ScriptedClient();
        IllegalStateException refused =
                new IllegalStateException("could not reach the subscription");
        client.failStartWith(refused);
        client.failTerminationWith(
                new IllegalStateException(
                        "Expected the service to be TERMINATED, but it FAILED", refused));

        try (LogCapture capture = LogCapture.of(PubSubNotifyingPullSubscriber.class)) {
            assertThatThrownBy(() -> subscriberOf(client))
                    .isInstanceOf(IOException.class)
                    .hasCause(refused);

            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event ->
                                    assertThat(event.getMessage())
                                            .contains(QUOTED_FAILED_START)
                                            .doesNotContain("were nacked before the wait")
                                            .doesNotContain("had already reported to the reader"));
        }

        // Counted by nothing, deliberately (#358): the start failure asserted above is an
        // IOException on its way to failing the job, so this release is a footnote to something
        // already reported rather than a signal of its own. Pinned so that a later change which
        // reuses awaitTerminated()'s absorb here — the shape stopQuietly() argues against — cannot
        // start counting it silently.
        assertThat(abandonedShutdowns()).isZero();
        assertThat(unreportedFailures()).isZero();
    }

    @Test
    void stopsTheClientWhenItsFirstClassloadFails() {
        ScriptedClient client = new ScriptedClient();
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
        PubSubNotifyingPullSubscriber subscriber = subscriberOf(new ScriptedClient());
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
        PubSubNotifyingPullSubscriber subscriber = subscriberOf(new ScriptedClient());
        PubsubMessage first = message("1", "x");
        PubsubMessage second = message("2", "xxxxxxxxxx");
        subscriber.receiveMessage(first, new RecordingAckHandle("1"));
        subscriber.receiveMessage(second, new RecordingAckHandle("2"));

        assertThat(subscriber.pullMessages(1)).containsExactly(first);

        assertThat(subscriber.bufferUsage().messages()).isEqualTo(1);
        assertThat(subscriber.bufferUsage().bytes()).isEqualTo(second.getSerializedSize());
    }

    @Test
    void shutdownEmptiesTheUsageItReports() throws Exception {
        PubSubNotifyingPullSubscriber subscriber = subscriberOf(new ScriptedClient());
        subscriber.receiveMessage(message("1", "xxxxx"), new RecordingAckHandle("1"));

        subscriber.shutdown();

        // The buffer is discarded and nacked by the shutdown, so what a parked subscriber reports
        // holding is nothing.
        assertThat(subscriber.bufferUsage().messages()).isZero();
        assertThat(subscriber.bufferUsage().bytes()).isZero();
    }

    @Test
    void aMessageArrivingAfterShutdownIsNackedRatherThanBuffered() throws Exception {
        PubSubNotifyingPullSubscriber subscriber = subscriberOf(new ScriptedClient());
        subscriber.shutdown();
        RecordingAckHandle late = new RecordingAckHandle("late");

        subscriber.receiveMessage(message("1", "xxxxx"), late);

        assertThat(subscriber.bufferUsage().messages()).isZero();
        assertThat(subscriber.bufferUsage().bytes()).isZero();
        assertThat(late.isNacked()).isTrue();
    }

    private static long abandonedShutdowns() {
        return PubSubShutdownResidue.SUBSCRIBER_SHUTDOWNS_ABANDONED.sum();
    }

    private static long unreportedFailures() {
        return PubSubShutdownResidue.SUBSCRIBER_FAILURES_UNREPORTED.sum();
    }

    private static PubsubMessage message(String messageId, String payload) {
        return PubsubMessage.newBuilder()
                .setMessageId(messageId)
                .setData(ByteString.copyFromUtf8(payload))
                .build();
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
        @Nullable private Throwable stopFailure;

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

        /**
         * Makes the stop deliver a permanent failure the way {@code Subscriber.doStop()}'s own
         * thread does, which is the only way the field can be written after the shutdown began.
         */
        void failWhenStopped(Throwable failure) {
            this.stopFailure = failure;
        }

        void stopAsync() {
            calls.add("stopAsync");
            if (stopFailure != null) {
                requireStarted().accept(stopFailure);
            }
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

        @Nullable private RuntimeException nackFailure;

        /**
         * Makes {@link #nackSplit} throw, which is the step {@code shutdown()} runs before it asks
         * the client to stop.
         */
        void failNackWith(RuntimeException nackFailure) {
            this.nackFailure = nackFailure;
        }

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
            if (nackFailure != null) {
                // After the record, as the real tracker's own nacks happen before anything can
                // fail: this is a nack that ran and then threw, not one that never started.
                throw nackFailure;
            }
        }
    }
}
