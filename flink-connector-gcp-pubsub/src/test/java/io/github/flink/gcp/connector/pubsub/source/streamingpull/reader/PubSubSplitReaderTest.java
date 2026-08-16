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

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;
import org.apache.flink.util.clock.ManualClock;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubSplitReader}. */
@Timeout(30)
class PubSubSplitReaderTest {

    private static final SubscriptionSplit SPLIT_A =
            new SubscriptionSplit(SubscriptionDestination.of("project", "sub-a"), "0");
    private static final SubscriptionSplit SPLIT_B =
            new SubscriptionSplit(SubscriptionDestination.of("project", "sub-b"), "1");
    private static final Duration BUDGET = Duration.ofMinutes(10);

    /**
     * A bound no test reaches by accident, so a park only ever happens where one was asked for. The
     * park tests build their own reader with a bound they cross deliberately.
     */
    private static final PausedSplitBufferLimits NO_PARK =
            PausedSplitBufferLimits.of(
                    PubSubSubscriberOptions.builder()
                            .pausedSplitBufferMaxMessages(Long.MAX_VALUE)
                            .pausedSplitBufferMaxBytes(Long.MAX_VALUE)
                            .build());

    private final Map<String, FakePullSubscriber> subscribers = new HashMap<>();
    private final TestReaderMetrics readerMetrics = new TestReaderMetrics();

    /** Set by the ordering test so the fakes record their release/close calls in one list. */
    private List<String> calls;

    /** Set by the reopen test to make the next {@code PullSubscriberOpener} call fail. */
    private IOException failNextOpen;

    private static MissingCheckpointDetector noCheckpointDetector() {
        return new MissingCheckpointDetector(Duration.ZERO, () -> 0);
    }

    /** The bound a park test crosses deliberately. */
    private static PausedSplitBufferLimits boundOf(long maxMessages, long maxBytes) {
        return PausedSplitBufferLimits.of(
                PubSubSubscriberOptions.builder()
                        .pausedSplitBufferMaxMessages(maxMessages)
                        .pausedSplitBufferMaxBytes(maxBytes)
                        .build());
    }

    @Test
    void drainsEveryAssignedSplitInOneFetch() throws Exception {
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        subscriberOf(SPLIT_A).deliver(message("a1"), message("a2"));
        subscriberOf(SPLIT_B).deliver(message("b1"));

        RecordsWithSplitIds<PubsubMessage> records = reader.fetch();

        assertThat(payloadsBySplit(records))
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                                SPLIT_A.splitId(), List.of("a1", "a2"),
                                SPLIT_B.splitId(), List.of("b1")));
        reader.close();
    }

    @Test
    void drainsInDeliveryOrderWhichIsWhatPreservesOrderingKeyOrder() throws Exception {
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        subscriberOf(SPLIT_A).deliver(message("1"), message("2"), message("3"));

        assertThat(payloadsBySplit(reader.fetch()).get(SPLIT_A.splitId()))
                .containsExactly("1", "2", "3");
        reader.close();
    }

    @Test
    void capsEachSplitsDrainAtTheConfiguredMaximum() throws Exception {
        PubSubSplitReader reader = reader(2);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        subscriberOf(SPLIT_A).deliver(message("1"), message("2"), message("3"));

        assertThat(payloadsBySplit(reader.fetch()).get(SPLIT_A.splitId()))
                .containsExactly("1", "2");
        assertThat(payloadsBySplit(reader.fetch()).get(SPLIT_A.splitId())).containsExactly("3");
        reader.close();
    }

    @Test
    void assigningTheSameSplitTwiceOpensOneSubscriber() throws Exception {
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        FakePullSubscriber first = subscriberOf(SPLIT_A);

        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));

        assertThat(subscriberOf(SPLIT_A)).isSameAs(first);
        reader.close();
    }

    @Test
    void removingASplitClosesItsSubscriberAndStopsDrainingIt() throws Exception {
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        FakePullSubscriber removed = subscriberOf(SPLIT_A);
        removed.deliver(message("dropped"));

        reader.handleSplitsChanges(new SplitsRemoval<>(List.of(SPLIT_A)));
        subscriberOf(SPLIT_B).deliver(message("kept"));

        assertThat(removed.isClosed()).isTrue();
        assertThat(payloadsBySplit(reader.fetch())).containsOnlyKeys(SPLIT_B.splitId());
        reader.close();
    }

    @Test
    void pausedSplitsAreNotDrained() throws Exception {
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        subscriberOf(SPLIT_A).deliver(message("a"));
        subscriberOf(SPLIT_B).deliver(message("b"));

        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());
        assertThat(payloadsBySplit(reader.fetch())).containsOnlyKeys(SPLIT_B.splitId());

        reader.pauseOrResumeSplits(Collections.emptyList(), List.of(SPLIT_A));
        assertThat(payloadsBySplit(reader.fetch())).containsOnlyKeys(SPLIT_A.splitId());
        reader.close();
    }

    @Test
    void aPausedSplitWithNothingBufferedIsNotMistakenForAFailedOne() throws Exception {
        // The negative half of the check below, and the state a real aligned job spends its time
        // in: paused, healthy, and producing nothing — which is what a paused split is *supposed*
        // to do. A check that read silence rather than the recorded failure would fail every
        // aligned job here. Deliberately not covered by pausedSplitsAreNotDrained, whose paused
        // split has a message buffered, so a silence-based check would not fire there either.
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());
        subscriberOf(SPLIT_B).deliver(message("b"));

        assertThat(payloadsBySplit(reader.fetch())).containsOnlyKeys(SPLIT_B.splitId());
        reader.close();
    }

    @Test
    void aPausedSplitsFailureSurfacesFromFetchEvenThoughNothingDrainsIt() throws Exception {
        // #348: a paused split is skipped by the drain, so pullMessages — the only thing that used
        // to report a permanent failure — is never reached, and the job would run on green with
        // this subscription dead.
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        subscriberOf(SPLIT_B).deliver(message("b"));
        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());

        subscriberOf(SPLIT_A).failWith(new IOException("stream broke"));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessage("stream broke");
        reader.close();
    }

    @Test
    void aPausedSplitOutgrowingItsMessageBoundHasItsSubscriberStopped() throws Exception {
        // #357: what was supposed to bound a paused split's buffer is the client library's flow
        // control, and it stops doing so after maxAckExtensionPeriod — the client releases a
        // message's permit while this reader still holds the message. Past the bound the reader
        // stops the client itself.
        PubSubSplitReader reader = reader(10, noCheckpointDetector(), boundOf(2, Long.MAX_VALUE));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        FakePullSubscriber parked = subscriberOf(SPLIT_A);
        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());
        parked.deliver(message("1"), message("2"), message("3"));
        subscriberOf(SPLIT_B).deliver(message("b"));

        try (LogCapture capture = LogCapture.of(PubSubSplitReader.class)) {
            reader.fetch();

            // The counter says how many parks happened; only the log says which split, at what
            // size, against what bound — which is the whole of what sizing the knob needs.
            assertThat(capture.getMessages())
                    .singleElement()
                    .satisfies(
                            logged -> {
                                assertThat(logged).contains(SPLIT_A.splitId());
                                assertThat(logged).contains("3 messages");
                                assertThat(logged).contains("2 messages");
                                assertThat(logged).contains("pausedSplitBufferMaxMessages");
                            });
        }

        assertThat(parked.isShutdownRequested()).isTrue();
        assertThat(parked.isClosed()).isTrue();
        assertThat(readerMetrics.counter("splitsParked")).isEqualTo(1);
        assertThat(readerMetrics.gauge("parkedSplits")).isEqualTo(1);
        reader.close();
    }

    @Test
    void aPausedSplitOutgrowingItsByteBoundHasItsSubscriberStopped() throws Exception {
        // The dimension that fires here is bytes, with the message count deliberately under its
        // own bound: whichever limit binds depends on message size, so a reader that required both
        // would never park a workload of few large messages — the one whose buffer is a memory
        // problem soonest.
        PubSubSplitReader reader = reader(10, noCheckpointDetector(), boundOf(100, 200));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        FakePullSubscriber parked = subscriberOf(SPLIT_A);
        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());
        long buffered = parked.deliverSized(3, 100);

        assertThat(buffered).isGreaterThan(200);
        reader.fetch();

        assertThat(parked.isClosed()).isTrue();
        assertThat(readerMetrics.counter("splitsParked")).isEqualTo(1);
        reader.close();
    }

    @Test
    void aPausedSplitExactlyAtItsBoundIsNotParked() throws Exception {
        // Strictly greater, so the bound is a size the buffer may hold rather than one it may not
        // reach — and a paused split at exactly the flow-control limit is the healthy state, not
        // the lapsed one.
        PubSubSplitReader reader = reader(10, noCheckpointDetector(), boundOf(2, Long.MAX_VALUE));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        FakePullSubscriber paused = subscriberOf(SPLIT_A);
        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());
        paused.deliver(message("1"), message("2"));

        reader.fetch();

        assertThat(paused.isClosed()).isFalse();
        assertThat(readerMetrics.counter("splitsParked")).isZero();
        reader.close();
    }

    @Test
    void aSplitThatIsNotPausedIsNeverParkedHoweverMuchItBuffers() throws Exception {
        // A split being drained cannot be over its bound for long, and stopping its client would
        // be a connector deciding to stop consuming a split Flink never asked it to pause. The
        // bound applies to a paused split alone.
        PubSubSplitReader reader = reader(1, noCheckpointDetector(), boundOf(2, Long.MAX_VALUE));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        FakePullSubscriber busy = subscriberOf(SPLIT_A);
        busy.deliver(message("1"), message("2"), message("3"), message("4"));

        try (LogCapture capture = LogCapture.of(PubSubSplitReader.class)) {
            reader.fetch();

            assertThat(capture.getMessages()).isEmpty();
        }

        assertThat(busy.isClosed()).isFalse();
        assertThat(readerMetrics.counter("splitsParked")).isZero();
        assertThat(readerMetrics.gauge("parkedSplits")).isZero();
        reader.close();
    }

    @Test
    void theBufferGaugesSumWhatEverySplitsSubscriberIsHolding() throws Exception {
        // What a split holds and has not handed over is otherwise reported by nothing: pendingAcks
        // covers messages received or emitted alike, and the paused-split bound reads a paused
        // split alone. Under backpressure the fetch loop stops entering fetch(), so a gauge the
        // metric reporter reads on a thread of its own is the only thing that sees it (#377).
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        long bytesA = subscriberOf(SPLIT_A).deliverSized(2, 50);
        long bytesB = subscriberOf(SPLIT_B).deliverSized(3, 10);

        assertThat(readerMetrics.gauge("bufferedMessages")).isEqualTo(5);
        assertThat(readerMetrics.gauge("bufferedBytes")).isEqualTo(bytesA + bytesB);

        reader.fetch();

        assertThat(readerMetrics.gauge("bufferedMessages")).isZero();
        assertThat(readerMetrics.gauge("bufferedBytes")).isZero();
        reader.close();
    }

    @Test
    void parkingASplitTakesItOutOfTheBufferGaugesAndAResumePutsItBack() throws Exception {
        PubSubSplitReader reader = reader(10, noCheckpointDetector(), boundOf(1, Long.MAX_VALUE));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());
        subscriberOf(SPLIT_A).deliver(message("1"), message("2"));

        assertThat(readerMetrics.gauge("bufferedMessages")).isEqualTo(2);

        reader.fetch();

        // The park handed every message back, so a parked split holds nothing. What this pins is
        // the registry rather than the value: the production subscriber empties its own buffer in
        // shutdown(), so a missing subscriberClosed(...) would leak a dead client rather than
        // misreport a live one. The fake deliberately keeps its deque, which is what makes the
        // leak visible here at all.
        assertThat(readerMetrics.gauge("parkedSplits")).isEqualTo(1);
        assertThat(readerMetrics.gauge("bufferedMessages"))
                .as("the parked split's client is no longer one the gauges sum")
                .isZero();

        reader.pauseOrResumeSplits(Collections.emptyList(), List.of(SPLIT_A));
        subscriberOf(SPLIT_A).deliver(message("3"));

        assertThat(readerMetrics.gauge("bufferedMessages")).isEqualTo(1);
        reader.close();
    }

    @Test
    void aRemovedSplitAndAClosedReaderLeaveNothingInTheBufferGauges() throws Exception {
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        subscriberOf(SPLIT_A).deliver(message("1"));
        subscriberOf(SPLIT_B).deliver(message("2"));

        reader.handleSplitsChanges(new SplitsRemoval<>(List.of(SPLIT_A)));

        assertThat(readerMetrics.gauge("bufferedMessages")).isEqualTo(1);

        reader.close();

        // The metrics outlive the split reader — a fetcher may be rebuilt over a reader's life —
        // so a reader that closed still registered would leak every client it owned into a gauge
        // read for the rest of the job. As above, the fake keeps its deque so the leak shows as a
        // value; in production shutdown() would have emptied it.
        assertThat(readerMetrics.gauge("bufferedMessages"))
                .as("a closed reader leaves no client registered")
                .isZero();
    }

    @Test
    void aPausedSplitWithNothingBufferedIsNotParked() throws Exception {
        // The state an aligned job spends its time in. A bound that fired on the pause rather than
        // on the buffer would stop every paused split's client and make alignment cost a
        // reconnection each time.
        PubSubSplitReader reader = reader(10, noCheckpointDetector(), boundOf(2, Long.MAX_VALUE));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());
        subscriberOf(SPLIT_B).deliver(message("b"));

        reader.fetch();

        assertThat(subscriberOf(SPLIT_A).isClosed()).isFalse();
        assertThat(readerMetrics.counter("splitsParked")).isZero();
        reader.close();
    }

    @Test
    void resumingAParkedSplitOpensAFreshSubscriberThatDrainsAgain() throws Exception {
        PubSubSplitReader reader = reader(10, noCheckpointDetector(), boundOf(1, Long.MAX_VALUE));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        FakePullSubscriber parked = subscriberOf(SPLIT_A);
        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());
        parked.deliver(message("1"), message("2"));
        reader.fetch();
        assertThat(parked.isClosed()).isTrue();

        reader.pauseOrResumeSplits(Collections.emptyList(), List.of(SPLIT_A));

        // A different instance, because the stopped client cannot be restarted — and one that
        // consumes, because Pub/Sub redelivers what the park nacked.
        FakePullSubscriber reopened = subscriberOf(SPLIT_A);
        assertThat(reopened).isNotSameAs(parked);
        assertThat(readerMetrics.gauge("parkedSplits")).isZero();
        assertThat(readerMetrics.counter("splitsParked")).isEqualTo(1);

        reopened.deliver(message("1"), message("2"));
        assertThat(payloadsBySplit(reader.fetch()).get(SPLIT_A.splitId()))
                .containsExactly("1", "2");
        reader.close();
    }

    @Test
    void aSplitPausedWhileAlreadyOverItsBoundIsStillParked() throws Exception {
        // The case pausing itself has to signal for. This split's buffer went over the bound while
        // it was being drained, so its arrival signal is already spent; the pause then stops the
        // client delivering, and nothing is left to end the next fetch's wait. Without a signal
        // from the pause, that fetch parks before reaching the check and this test hangs to its
        // class timeout rather than failing an assertion.
        PubSubSplitReader reader = reader(1, noCheckpointDetector(), boundOf(1, Long.MAX_VALUE));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        FakePullSubscriber subscriber = subscriberOf(SPLIT_A);
        subscriber.deliver(message("1"), message("2"), message("3"));
        // Drains one of the three and consumes the signal, leaving two buffered and none pending.
        reader.fetch();

        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());
        reader.fetch();

        assertThat(subscriber.isClosed()).isTrue();
        assertThat(readerMetrics.counter("splitsParked")).isEqualTo(1);
        reader.close();
    }

    @Test
    void resumingASplitThatWasNeverParkedKeepsItsSubscriber() throws Exception {
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        FakePullSubscriber original = subscriberOf(SPLIT_A);
        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());

        reader.pauseOrResumeSplits(Collections.emptyList(), List.of(SPLIT_A));

        assertThat(subscriberOf(SPLIT_A)).isSameAs(original);
        assertThat(readerMetrics.gauge("parkedSplits")).isZero();
        reader.close();
    }

    @Test
    void aPausedSplitThatIsBothFailedAndOverfullFailsTheJobRatherThanBeingParked()
            throws Exception {
        // The reason the failure check runs before the park: parking closes the subscriber, and
        // close() absorbs the client's own report of a failure it has already delivered (#325). A
        // park that ran first would swallow the failure the reader exists to raise.
        PubSubSplitReader reader = reader(10, noCheckpointDetector(), boundOf(1, Long.MAX_VALUE));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        FakePullSubscriber failing = subscriberOf(SPLIT_A);
        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());
        failing.deliver(message("1"), message("2"));
        failing.failWith(new IOException("stream broke"));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessage("stream broke");

        assertThat(readerMetrics.counter("splitsParked")).isZero();
        reader.close();
    }

    @Test
    void aFailureToReopenAResumedSplitFailsTheJob() throws Exception {
        PubSubSplitReader reader = reader(10, noCheckpointDetector(), boundOf(1, Long.MAX_VALUE));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        subscriberOf(SPLIT_A).deliver(message("1"), message("2"));
        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());
        reader.fetch();
        failNextOpen = new IOException("no such subscription");

        // The same failure an assignment that cannot open its subscriber raises — a subscription
        // deleted during the pause is noticed here, since a parked split has no client to notice
        // with.
        assertThatThrownBy(
                        () -> reader.pauseOrResumeSplits(Collections.emptyList(), List.of(SPLIT_A)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(SPLIT_A.splitId())
                .hasRootCauseMessage("no such subscription");
        reader.close();
    }

    @Test
    void removingAParkedSplitReleasesNothingAndStopsCountingIt() throws Exception {
        PubSubSplitReader reader = reader(10, noCheckpointDetector(), boundOf(1, Long.MAX_VALUE));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        subscriberOf(SPLIT_A).deliver(message("1"), message("2"));
        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());
        reader.fetch();

        reader.handleSplitsChanges(new SplitsRemoval<>(List.of(SPLIT_A)));

        // Parking already nacked and closed; what removal must not do is leave the gauge counting
        // a split that no longer exists.
        assertThat(readerMetrics.gauge("parkedSplits")).isZero();
        reader.close();
    }

    @Test
    void closingTheReaderWithAParkedSplitIsClean() throws Exception {
        PubSubSplitReader reader = reader(10, noCheckpointDetector(), boundOf(1, Long.MAX_VALUE));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        FakePullSubscriber parked = subscriberOf(SPLIT_A);
        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());
        parked.deliver(message("1"), message("2"));
        reader.fetch();

        reader.close();

        // The parked subscriber is not shut down twice, and the split still holding a client is.
        assertThat(subscriberOf(SPLIT_B).isClosed()).isTrue();
        assertThat(parked.isClosed()).isTrue();
        // And the count goes back. The gauge lives in the reader's metrics precisely because it
        // outlives this object, so a close that dropped a parked split without giving its count
        // back would leave the gauge reporting a split that no longer exists.
        assertThat(readerMetrics.gauge("parkedSplits")).isZero();
    }

    @Test
    void parkingSeveralSplitsAtOnceShutsThemAllDownBeforeWaitingOnAny() throws Exception {
        // Alignment pauses a subtask's splits as a group and they cross the bound in the same wave,
        // so this is the ordinary case rather than an exotic one. shutdown() nacks and returns at
        // once while close() waits up to shutdownTimeout, so parking one split at a time would
        // spend splits × timeout on the fetcher thread — the cost close() is written to avoid.
        calls = new ArrayList<>();
        PubSubSplitReader reader = reader(10, noCheckpointDetector(), boundOf(1, Long.MAX_VALUE));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        reader.pauseOrResumeSplits(List.of(SPLIT_A, SPLIT_B), Collections.emptyList());
        subscriberOf(SPLIT_A).deliver(message("a1"), message("a2"));
        subscriberOf(SPLIT_B).deliver(message("b1"), message("b2"));

        reader.fetch();

        assertThat(calls)
                .containsExactly(
                        "shutdown:" + SPLIT_A.splitId(),
                        "shutdown:" + SPLIT_B.splitId(),
                        "close:" + SPLIT_A.splitId(),
                        "close:" + SPLIT_B.splitId());
        assertThat(readerMetrics.gauge("parkedSplits")).isEqualTo(2);
        reader.close();
    }

    @Test
    void aSplitWhoseReopenFailedIsStillTreatedAsPaused() throws Exception {
        // The reopen runs before the pause is lifted, so a split that could not be reopened keeps
        // both halves of "parked implies paused". Lifting the pause first would leave a split with
        // no subscriber that the drain no longer skips, and the drain dereferences it.
        // SPLIT_B stays active so the fetch below has something to return: a reader holding only a
        // paused split parks, and the point here is what the drain does once it runs.
        PubSubSplitReader reader = reader(10, noCheckpointDetector(), boundOf(1, Long.MAX_VALUE));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        subscriberOf(SPLIT_A).deliver(message("1"), message("2"));
        reader.pauseOrResumeSplits(List.of(SPLIT_A), Collections.emptyList());
        reader.fetch();
        failNextOpen = new IOException("no such subscription");
        assertThatThrownBy(
                        () -> reader.pauseOrResumeSplits(Collections.emptyList(), List.of(SPLIT_A)))
                .isInstanceOf(RuntimeException.class);

        // Flink fails the job on that throw, so this fetch does not happen in production — it is
        // here because the invariant must not rest on that. Lift the pause before the reopen and
        // the drain stops skipping a split whose subscriber is gone, then dereferences it.
        subscriberOf(SPLIT_B).deliver(message("b"));
        assertThat(payloadsBySplit(reader.fetch())).containsOnlyKeys(SPLIT_B.splitId());
        reader.close();
    }

    @Test
    void subscriberFailureSurfacesFromFetch() throws Exception {
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        subscriberOf(SPLIT_A).failWith(new IOException("stream broke"));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessage("stream broke");
        reader.close();
    }

    @Test
    void fetchBlocksUntilAMessageArrives() throws Exception {
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));

        CompletableFuture<RecordsWithSplitIds<PubsubMessage>> fetch =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return reader.fetch();
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        });
        assertThat(fetch).isNotDone();

        subscriberOf(SPLIT_A).deliver(message("late"));

        assertThat(payloadsBySplit(fetch.get()).get(SPLIT_A.splitId())).containsExactly("late");
        reader.close();
    }

    @Test
    void wakeUpUnblocksAParkedFetch() throws Exception {
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));

        CompletableFuture<RecordsWithSplitIds<PubsubMessage>> fetch =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return reader.fetch();
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        });

        reader.wakeUp();

        assertThat(payloadsBySplit(fetch.get())).isEmpty();
        reader.close();
    }

    @Test
    void aWakeUpArrivingBeforeTheFetchIsNotLost() throws Exception {
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));

        // The fetcher checks its own wake-up flag *before* entering fetch(), so this is the window
        // a wake-up genuinely lands in, and it is delivered exactly once. Dropping it would park
        // the fetch forever: on the shutdown path nothing else ever wakes the fetcher, so the
        // reader would never be closed and its messages never nacked. Without a level-triggered
        // signal this test hangs until the class timeout.
        reader.wakeUp();

        assertThat(payloadsBySplit(reader.fetch())).isEmpty();
        reader.close();
    }

    @Test
    void closeShutsDownEverySubscriberEvenWhenOneFails() throws Exception {
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        subscriberOf(SPLIT_A).failOnClose();

        assertThatThrownBy(reader::close).isInstanceOf(IOException.class);

        assertThat(subscriberOf(SPLIT_A).isClosed()).isTrue();
        assertThat(subscriberOf(SPLIT_B).isClosed()).isTrue();
    }

    @Test
    void closeShutsDownEverySubscriberEvenWhenOneCloseThrowsAnError() throws Exception {
        // #276: Flink's IOUtils.closeAll rethrows an Error from inside its loop, so the second
        // subscriber was left open — holding messages Pub/Sub only redelivers once their
        // acknowledgement deadline expires. That the Error reaches the caller as an Error is the
        // other half: Flink halts the JVM on a fatal one, and only if it arrives unwrapped.
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        subscriberOf(SPLIT_A).failOnClose(new NoClassDefFoundError("close blew up"));

        assertThatThrownBy(reader::close)
                .isInstanceOf(NoClassDefFoundError.class)
                .hasMessage("close blew up");

        assertThat(subscriberOf(SPLIT_A).isClosed()).isTrue();
        assertThat(subscriberOf(SPLIT_B).isClosed()).isTrue();
    }

    @Test
    void closeShutsDownAndClosesEverySubscriberEvenWhenOneShutdownThrows() throws Exception {
        // #297: shutdown() declares no checked exception, and the loop that called it was a bare
        // for loop — so one unchecked failure skipped every later nack *and* skipped the closeAll
        // entirely, leaving even the already-shut-down subscribers open. Both halves are asserted
        // here: B is shut down (nacked) and both are closed.
        List<String> calls = new ArrayList<>();
        this.calls = calls;
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        subscriberOf(SPLIT_A).failOnShutdown(new IllegalStateException("shutdown blew up"));

        assertThatThrownBy(reader::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("shutdown blew up");

        assertThat(subscriberOf(SPLIT_B).isShutdownRequested()).isTrue();
        assertThat(subscriberOf(SPLIT_A).isClosed()).isTrue();
        assertThat(subscriberOf(SPLIT_B).isClosed()).isTrue();
        // The failing shutdown does not cost the ordering either: still every shutdown before any
        // close, which is what keeps the waits overlapping. Same shape as the test below, which
        // owns that property on the success path.
        assertThat(calls).hasSize(4);
        assertThat(calls.subList(0, 2))
                .containsExactlyInAnyOrder(
                        "shutdown:" + SPLIT_A.splitId(), "shutdown:" + SPLIT_B.splitId());
        assertThat(calls.subList(2, 4))
                .containsExactlyInAnyOrder(
                        "close:" + SPLIT_A.splitId(), "close:" + SPLIT_B.splitId());
    }

    @Test
    void closeStartsEverySubscriberShutdownBeforeWaitingOnAny() throws Exception {
        // shutdown() nacks the split.s messages and returns at once; close() then waits up to the
        // shutdown timeout. Interleaving the two costs splits × timeout, and past roughly six
        // splits on one reader that exceeds Flink's source.reader.close.timeout — so the splits
        // whose turn never came would never be nacked, and their messages would sit until their
        // acknowledgement deadline expired.
        List<String> calls = new ArrayList<>();
        this.calls = calls;
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));

        reader.close();

        // Order between the two subscribers is not specified; what matters is that no subscriber is
        // waited on until every one of them has been asked to shut down.
        assertThat(calls).hasSize(4);
        assertThat(calls.subList(0, 2))
                .containsExactlyInAnyOrder(
                        "shutdown:" + SPLIT_A.splitId(), "shutdown:" + SPLIT_B.splitId());
        assertThat(calls.subList(2, 4))
                .containsExactlyInAnyOrder(
                        "close:" + SPLIT_A.splitId(), "close:" + SPLIT_B.splitId());
    }

    @Test
    void theMissingCheckpointBudgetOnlyStartsOnceTheReaderHasASplit() throws Exception {
        // A reader is created before the enumerator assigns it anything, so a budget started at
        // construction can be spent before there is anything to checkpoint. Failing then reports
        // "enable checkpointing" against a job that checkpoints perfectly well.
        ManualClock clock = new ManualClock();
        PubSubSplitReader reader = reader(10, detector(clock));
        clock.advanceTime(BUDGET.multipliedBy(10));

        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        subscriberOf(SPLIT_A).deliver(message("a"));

        assertThat(payloadsBySplit(reader.fetch()).get(SPLIT_A.splitId())).containsExactly("a");
        reader.close();
    }

    @Test
    void aSplitAssignmentArmsTheMissingCheckpointDetector() throws Exception {
        ManualClock clock = new ManualClock();
        PubSubSplitReader reader = reader(10, detector(clock));

        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        clock.advanceTime(BUDGET);
        subscriberOf(SPLIT_A).deliver(message("a"));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No checkpoint has been taken");
        reader.close();
    }

    @Test
    void anEmptyAdditionDoesNotStartTheMissingCheckpointBudget() throws Exception {
        // Nothing was assigned, so there is still nothing to checkpoint.
        ManualClock clock = new ManualClock();
        PubSubSplitReader reader = reader(10, detector(clock));

        reader.handleSplitsChanges(new SplitsAddition<>(List.of()));
        clock.advanceTime(BUDGET.multipliedBy(10));

        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));
        subscriberOf(SPLIT_A).deliver(message("a"));

        assertThat(payloadsBySplit(reader.fetch()).get(SPLIT_A.splitId())).containsExactly("a");
        reader.close();
    }

    private static MissingCheckpointDetector detector(ManualClock clock) {
        return new MissingCheckpointDetector(BUDGET, () -> 1, clock::relativeTimeNanos);
    }

    private PubSubSplitReader reader(int maxRecordsPerFetch) {
        return reader(maxRecordsPerFetch, new MissingCheckpointDetector(Duration.ZERO, () -> 0));
    }

    private PubSubSplitReader reader(
            int maxRecordsPerFetch, MissingCheckpointDetector checkpointDetector) {
        return reader(maxRecordsPerFetch, checkpointDetector, NO_PARK);
    }

    private PubSubSplitReader reader(
            int maxRecordsPerFetch,
            MissingCheckpointDetector checkpointDetector,
            PausedSplitBufferLimits pausedSplitBufferLimits) {
        return new PubSubSplitReader(
                (split, signal) -> {
                    if (failNextOpen != null) {
                        IOException failure = failNextOpen;
                        failNextOpen = null;
                        throw failure;
                    }
                    FakePullSubscriber subscriber =
                            new FakePullSubscriber(signal).named(split.splitId());
                    if (calls != null) {
                        subscriber.recordCallsInto(calls);
                    }
                    subscribers.put(split.splitId(), subscriber);
                    return subscriber;
                },
                maxRecordsPerFetch,
                checkpointDetector,
                pausedSplitBufferLimits,
                readerMetrics.metrics());
    }

    private FakePullSubscriber subscriberOf(SubscriptionSplit split) {
        return subscribers.get(split.splitId());
    }

    private static PubsubMessage message(String payload) {
        return PubsubMessage.newBuilder()
                .setMessageId(payload)
                .setData(ByteString.copyFromUtf8(payload))
                .build();
    }

    private static Map<String, List<String>> payloadsBySplit(
            RecordsWithSplitIds<PubsubMessage> records) {
        Map<String, List<String>> bySplit = new HashMap<>();
        String splitId;
        while ((splitId = records.nextSplit()) != null) {
            List<String> payloads = new ArrayList<>();
            PubsubMessage message;
            while ((message = records.nextRecordFromSplit()) != null) {
                payloads.add(message.getData().toString(StandardCharsets.UTF_8));
            }
            bySplit.put(splitId, payloads);
        }
        return bySplit;
    }
}
