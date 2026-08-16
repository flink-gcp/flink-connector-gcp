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

import io.github.flink.gcp.connector.pubsub.PubSubShutdownResidue;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link SubscriberTeardown}'s three-exit classification — the re-reported failure, the
 * failure nothing else reports, and the shutdown wait that expired — and the doc-quoted WARN
 * fragments those exits log (#325/#351/#358/#359). Driven through {@link StreamingPullSubscriber},
 * because the teardown is reached only through it.
 */
@Timeout(30)
class SubscriberTeardownTest {

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

    private final List<String> calls = new ArrayList<>();
    private final RecordingAckTracker ackTracker = new RecordingAckTracker(calls);

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
    void absorbsTheClientsReportOfTheFailureItAlreadyDelivered() throws Exception {
        ScriptedClient client = new ScriptedClient(calls);
        StreamingPullSubscriber subscriber = subscriberOf(client);
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

        try (LogCapture capture = LogCapture.of(SubscriberTeardown.class)) {
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
        ScriptedClient client = new ScriptedClient(calls);
        StreamingPullSubscriber subscriber = subscriberOf(client);
        IllegalStateException raisedAtShutdown =
                new IllegalStateException("the connections would not close");
        client.failTerminationWith(
                new IllegalStateException(
                        "Expected the service to be TERMINATED, but it FAILED", raisedAtShutdown));

        try (LogCapture capture = LogCapture.of(SubscriberTeardown.class)) {
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
        ScriptedClient client = new ScriptedClient(calls);
        StreamingPullSubscriber subscriber = subscriberOf(client);
        IllegalStateException raisedByTheStop =
                new IllegalStateException("the connections would not close");
        client.failWhenStopped(raisedByTheStop);
        client.failTerminationWith(
                new IllegalStateException(
                        "Expected the service to be TERMINATED, but it FAILED", raisedByTheStop));

        try (LogCapture capture = LogCapture.of(SubscriberTeardown.class)) {
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
        ScriptedClient client = new ScriptedClient(calls);
        StreamingPullSubscriber subscriber = subscriberOf(client);
        IllegalStateException reported = new IllegalStateException("the streaming pull gave up");
        client.fail(reported);
        assertThatThrownBy(() -> subscriber.pullMessages(10)).isInstanceOf(IOException.class);

        IllegalStateException unrelated = new IllegalStateException("and the stub would not close");
        client.failTerminationWith(
                new IllegalStateException(
                        "Expected the service to be TERMINATED, but it FAILED", unrelated));

        try (LogCapture capture = LogCapture.of(SubscriberTeardown.class)) {
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
        ScriptedClient client = new ScriptedClient(calls);
        StreamingPullSubscriber subscriber = subscriberOf(client);
        TimeoutException expired = new TimeoutException("still shutting down");
        client.failTerminationWith(expired);

        try (LogCapture capture = LogCapture.of(SubscriberTeardown.class)) {
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
        ScriptedClient client = new ScriptedClient(calls);

        subscriberOf(client).close();

        // The counters report outcomes, so an increment that escaped its catch block would make
        // every healthy close read on a dashboard exactly like a deployment whose subscribers are
        // failing to shut down.
        assertThat(abandonedShutdowns()).isZero();
        assertThat(unreportedFailures()).isZero();
    }

    @Test
    void aFailedStartsTeardownIsReportedAsTheReleaseOfAClientThatNeverRan() throws Exception {
        // The release path has its own message because both of awaitTerminated()'s would be false
        // here. There is no repeat to identify — the start failure is on its way to the caller —
        // and asking would be a race besides: Guava dispatches the failure listener from the SDK's
        // own thread after releasing the monitor awaitRunning() blocks on, so permanentError can
        // still be unset, as it is here. Nor has anything been nacked, shutdown() never having run.
        ScriptedClient client = new ScriptedClient(calls);
        IllegalStateException refused =
                new IllegalStateException("could not reach the subscription");
        client.failStartWith(refused);
        client.failTerminationWith(
                new IllegalStateException(
                        "Expected the service to be TERMINATED, but it FAILED", refused));

        try (LogCapture capture = LogCapture.of(SubscriberTeardown.class)) {
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

    private static long abandonedShutdowns() {
        return PubSubShutdownResidue.SUBSCRIBER_SHUTDOWNS_ABANDONED.sum();
    }

    private static long unreportedFailures() {
        return PubSubShutdownResidue.SUBSCRIBER_FAILURES_UNREPORTED.sum();
    }

    private StreamingPullSubscriber subscriberOf(ScriptedClient client) throws IOException {
        return new StreamingPullSubscriber(
                SPLIT_ID,
                SUBSCRIPTION,
                ackTracker,
                () -> calls.add("dataAvailable"),
                SHUTDOWN_TIMEOUT,
                client::start,
                client::stopAsync,
                client::awaitTerminated);
    }
}
