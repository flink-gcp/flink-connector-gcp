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

import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.source.AbstractPubSubSourceEmulatorITCase;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import io.github.flink.gcp.connector.testutils.Awaits;
import io.github.flink.gcp.connector.testutils.pubsub.PubSubSplitReaders;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures what bounds a paused split's buffer, against the emulator and the production {@link
 * DefaultSubscriberFactory}.
 *
 * <p><b>This class exists to measure a premise rather than to cover a feature.</b> A split paused
 * by watermark alignment is never drained, and the claim the connector rested on is that the client
 * library's flow control bounds what accumulates. It does — for one {@code maxAckExtensionPeriod}.
 * In {@code google-cloud-pubsub} 1.152.0 {@code MessageDispatcher} stamps a message's {@code
 * totalExpiration} at <em>receipt</em>, and once it is reached {@code AckHandler.forget()} calls
 * {@code flowController.release(...)} while this connector is still holding the message. Permits
 * therefore free up, pulling resumes, and nothing caps the buffer after that (<a
 * href="https://github.com/laughingman7743/flink-connector-gcp/issues/357">#357</a>).
 *
 * <p>The two methods are each other's control: the first holds the reader's own bound out of the
 * way and measures the growth, so it goes on measuring the SDK premise this design rests on — the
 * thing that would silently stop being true on a client-library bump — and the second sets the
 * bound and measures the park stopping exactly that growth.
 *
 * <p>The harness drives the split reader directly rather than a job under watermark alignment,
 * because <b>a paused split emits nothing</b>: a MiniCluster test has no observable for the state
 * under measurement, while {@code pauseOrResumeSplits} reaches it in one call — that alignment
 * pauses splits at all is already measured, by {@code PubSubSourceWatermarkAlignmentITCase}. The
 * growth method calls no {@code fetch()} at all, since the buffer fills from the client library's
 * own threads; the park method must, because that is where the reader's bound is evaluated, and it
 * nudges the reader because a paused split delivers no records to end a fetch's wait.
 *
 * <p>The reader's own counters are the instrument, so nothing here reads a private field. Every
 * buffered message is one {@code addPendingAck}, hence exactly one {@code messagesReceived}; {@code
 * pendingAcks} counts <em>distinct</em> message ids, since a redelivery supersedes its predecessor
 * there; and that supersede is the only thing incrementing {@code messagesNacked} while a split is
 * paused. So {@code messagesReceived} is the buffer, and {@code messagesReceived - pendingAcks} is
 * how much of it is redelivered copies of what the connector already held.
 *
 * <p>Tagged {@code slow} and so run weekly rather than per pull request, for the same reason as its
 * backpressured sibling: the expiry waves it samples arrive one ack deadline apart. See the tag in
 * the root pom.
 */
@Tag("slow")
class PubSubPausedSplitBufferITCase extends AbstractPubSubSourceEmulatorITCase {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubPausedSplitBufferITCase.class);

    /**
     * The lowest deadline the emulator is asked for anywhere in this repository. It is not what
     * governs the lease under streaming pull — that comes from the client's own {@code
     * streamAckDeadlineSeconds}, floored at {@code MIN_STREAM_ACK_DEADLINE} (10 s) — so it only
     * decides how long a delivery survives before the client's first modack.
     */
    private static final int ACK_DEADLINE_SECONDS = 10;

    /**
     * The bound under test. The client library holds this many messages outstanding and then stops
     * pulling, so with nothing ever acknowledged the buffer may not pass it — until the
     * ack-extension budget lapses.
     *
     * <p>Fifty rather than something smaller, and the reason is an emulator artifact worth
     * recording: at a limit of 10 the growth stops dead after a single wave (measured 10 → 21, then
     * flat for 110 s over two runs), while at 50 it repeats on schedule. Real Pub/Sub is the
     * authority on the rate either way; what this class establishes is that the bound lapses at
     * all, and at 50 it establishes it more than once.
     */
    private static final long FLOW_CONTROL_MESSAGES = 50;

    /**
     * Short enough to observe within a test, and deliberately not the smallest possible value. The
     * permit is released one deadline period <em>before</em> {@code totalExpiration} — {@code
     * extendDeadlines} forgets a message it can no longer extend past the next deadline — so at 20
     * s against a 10 s deadline the first wave lands about 10 s in, leaving a readable plateau
     * before it. At 10 s there would be no plateau at all, and the test could not tell flow control
     * working from flow control never having applied.
     */
    private static final Duration MAX_ACK_EXTENSION_PERIOD = Duration.ofSeconds(20);

    /**
     * Eight times the flow-control window, so the measurement is never limited by the backlog: a
     * buffer that stops growing must have stopped because something bounded it.
     */
    private static final int BACKLOG = 400;

    /** When the plateau is sampled — after the window has filled, before the first wave. */
    private static final Duration PLATEAU_SAMPLE_AT = Duration.ofSeconds(5);

    /**
     * How far the buffer may be above the limit and still be called bounded. The measured plateau
     * is the limit exactly; the margin is against a delivery batch straddling the sample, and stays
     * far below the first wave's {@code 2 × limit + 1}.
     */
    private static final double PLATEAU_TOLERANCE = 1.2;

    /**
     * How far past the flow-control limit the buffer must go before the claim is established. Three
     * times the limit is two full waves, so it cannot be explained by one batch overshooting.
     */
    private static final long GROWTH_FACTOR = 3;

    /**
     * The park bound the second method sets, just above the flow-control window: nothing crosses it
     * while flow control holds, and the first wave past the lapse does.
     */
    private static final long PARK_BOUND = FLOW_CONTROL_MESSAGES + 10;

    /** Bound on waiting for the resumed subscriber's first delivery. */
    private static final Duration RESUME_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Bound on waiting for that growth. Two waves land at about 10 s and 20 s — one per {@code
     * maxAckExtensionPeriod} minus one deadline period — so this is more than twice what a healthy
     * run needs.
     */
    private static final Duration GROWTH_TIMEOUT = Duration.ofSeconds(45);

    @Test
    void aPausedSplitsBufferOutgrowsFlowControlOnceTheAckExtensionBudgetLapses() throws Exception {
        SubscriptionDestination subscription =
                createTopicAndSubscription("paused-buffer", ACK_DEADLINE_SECONDS);
        publish("paused-buffer", payloads(BACKLOG));

        SubscriptionSplit split = new SubscriptionSplit(subscription, "0");
        TestReaderMetrics readerMetrics = new TestReaderMetrics();
        PubSubAckTracker ackTracker = new PubSubAckTracker(readerMetrics.metrics(), null);
        // As the source does: the gauges read the tracker, so they exist only once it is bound.
        readerMetrics.metrics().bindAckTracker(ackTracker);
        try (PubSubSplitReader reader = reader(ackTracker, readerMetrics.metrics())) {
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
            reader.pauseOrResumeSplits(Collections.singletonList(split), Collections.emptyList());
            // Only the steps, not every poll: the buffer sits flat between waves, so a full series
            // is a hundred repetitions of the two numbers that matter.
            StepSeries buffered = new StepSeries(readerMetrics);

            Thread.sleep(PLATEAU_SAMPLE_AT.toMillis());
            buffered.sample();
            assertThat(readerMetrics.counter("messagesReceived"))
                    .as("flow control bounds the buffer at first: %s", buffered)
                    .isLessThanOrEqualTo((long) (PLATEAU_TOLERANCE * FLOW_CONTROL_MESSAGES));

            Awaits.await(
                    "the paused split's buffer to outgrow the flow-control limit",
                    GROWTH_TIMEOUT,
                    () -> buffered.sample() >= GROWTH_FACTOR * FLOW_CONTROL_MESSAGES,
                    buffered::toString);

            // The measurement this class exists to take: where the buffer stepped, and whether any
            // of the growth was the emulator redelivering what the connector still held.
            LOG.info("Paused-split buffer growth (#357): {}", buffered);
        }
    }

    /** The buffer's size over time, recorded only where it changed. */
    private static final class StepSeries {

        private final TestReaderMetrics readerMetrics;
        private final long startNanos = System.nanoTime();
        private final List<String> steps = new ArrayList<>();
        private long last = -1;

        StepSeries(TestReaderMetrics readerMetrics) {
            this.readerMetrics = readerMetrics;
        }

        /** Records the buffer's size if it has changed, and returns it either way. */
        long sample() {
            long received = readerMetrics.counter("messagesReceived");
            if (received != last) {
                last = received;
                steps.add(
                        String.format(
                                "%.1fs:received=%d,pending=%d,nacked=%d",
                                (System.nanoTime() - startNanos) / 1e9,
                                received,
                                readerMetrics.gauge("pendingAcks"),
                                readerMetrics.counter("messagesNacked")));
            }
            return received;
        }

        @Override
        public String toString() {
            return String.join(" ", steps);
        }
    }

    @Test
    void aPausedSplitsSubscriberIsStoppedOnceItsBufferPassesItsBoundAndReopensOnResume()
            throws Exception {
        SubscriptionDestination subscription =
                createTopicAndSubscription("paused-buffer-park", ACK_DEADLINE_SECONDS);
        publish("paused-buffer-park", payloads(BACKLOG));

        SubscriptionSplit split = new SubscriptionSplit(subscription, "0");
        TestReaderMetrics readerMetrics = new TestReaderMetrics();
        PubSubAckTracker ackTracker = new PubSubAckTracker(readerMetrics.metrics(), null);
        readerMetrics.metrics().bindAckTracker(ackTracker);

        try (PubSubSplitReader reader = reader(ackTracker, readerMetrics.metrics(), PARK_BOUND)) {
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
            reader.pauseOrResumeSplits(Collections.singletonList(split), Collections.emptyList());
            StepSeries buffered = new StepSeries(readerMetrics);

            fetchUntil(
                    reader,
                    "the paused split's subscriber to be stopped",
                    () -> {
                        buffered.sample();
                        return readerMetrics.gauge("parkedSplits") == 1;
                    },
                    buffered::toString);

            long bufferedAtPark = readerMetrics.counter("messagesReceived");
            LOG.info(
                    "Paused-split park (#357): parked at {} buffered, bound {}, series {}",
                    bufferedAtPark,
                    PARK_BOUND,
                    buffered);

            // The park is a bound, so what it stopped at has to be near the bound rather than
            // merely finite: it fires on the wave that crosses it, so the overshoot is one
            // delivery batch and no more.
            assertThat(bufferedAtPark).isGreaterThan(PARK_BOUND);
            assertThat(readerMetrics.counter("splitsParked")).isEqualTo(1);
            // Every message the buffer held went back to Pub/Sub rather than being dropped: the
            // park nacks, which is what makes stopping the client safe.
            assertThat(readerMetrics.counter("messagesNacked")).isEqualTo(bufferedAtPark);
            assertThat(readerMetrics.gauge("pendingAcks")).isZero();

            reader.pauseOrResumeSplits(Collections.emptyList(), Collections.singletonList(split));

            // A fresh subscriber consumes the backlog, so nothing the park discarded is lost.
            assertThat(readerMetrics.gauge("parkedSplits")).isZero();
            assertThat(PubSubSplitReaders.fetchUntil(reader, 1, RESUME_TIMEOUT)).isNotEmpty();
        }
    }

    /**
     * Fetches until the condition holds, nudging the reader so a fetch with nothing to drain cannot
     * outlive the deadline — a paused split delivers no records, so the fetch parks on a signal
     * that only the client library's next delivery completes.
     */
    private static void fetchUntil(
            PubSubSplitReader reader, String what, BooleanSupplier done, Supplier<String> diagnosis)
            throws Exception {
        ScheduledExecutorService waker = Executors.newSingleThreadScheduledExecutor();
        try {
            waker.scheduleAtFixedRate(reader::wakeUp, 200, 200, TimeUnit.MILLISECONDS);
            long deadline = System.nanoTime() + GROWTH_TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                reader.fetch();
                if (done.getAsBoolean()) {
                    return;
                }
            }
        } finally {
            waker.shutdownNow();
        }
        throw new AssertionError(
                "Timed out waiting for "
                        + what
                        + " (waited "
                        + GROWTH_TIMEOUT
                        + "). "
                        + diagnosis.get());
    }

    private static PubSubSplitReader reader(
            PubSubAckTracker ackTracker, PubSubSourceReaderMetrics metrics) {
        return reader(ackTracker, metrics, Long.MAX_VALUE);
    }

    private static PubSubSplitReader reader(
            PubSubAckTracker ackTracker, PubSubSourceReaderMetrics metrics, long parkBound) {
        PubSubSubscriberOptions options =
                PubSubSubscriberOptions.builder()
                        .flowControlMaxOutstandingElementCount(FLOW_CONTROL_MESSAGES)
                        .maxAckExtensionPeriod(MAX_ACK_EXTENSION_PERIOD)
                        // Held out of the way for the growth measurement, and set for the park
                        // one: the first method needs the buffer to keep growing, the second needs
                        // the reader to stop it.
                        .pausedSplitBufferMaxMessages(parkBound)
                        .pausedSplitBufferMaxBytes(Long.MAX_VALUE)
                        .build();
        return new PubSubSplitReader(
                new DefaultSubscriberFactory(
                        options,
                        OrderingMode.NONE,
                        EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint")),
                ackTracker,
                options,
                new MissingCheckpointDetector(Duration.ZERO, ackTracker::outstandingAckCount),
                metrics);
    }

    private static String[] payloads(int count) {
        return IntStream.range(0, count)
                .mapToObj(index -> "m" + index)
                .collect(Collectors.toList())
                .toArray(new String[0]);
    }
}
