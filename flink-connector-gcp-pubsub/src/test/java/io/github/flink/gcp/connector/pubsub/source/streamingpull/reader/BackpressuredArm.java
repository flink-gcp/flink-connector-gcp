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

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * One subscription drained at a fixed rate, with its subscriber's buffer sampled over time — the
 * arm of the #377 measurement, shared by the emulator run ({@link
 * PubSubBackpressuredSplitBufferITCase}) and the real-service one ({@link
 * PubSubBackpressuredSplitBufferRealGcpITCase}).
 *
 * <p>Draining at a rate is what makes this a <em>backpressured</em> split rather than a paused one:
 * a paused split is never drained at all, and #357's bound already covers that. Here the fetch loop
 * runs, just not as fast as the client library delivers.
 *
 * <p>The constants live here rather than in either rig because the two are only comparable if they
 * are the same constants — a promise the real-GCP class would otherwise be making in prose. They
 * are in turn {@link PubSubPausedSplitBufferITCase}'s, so all three series can be read together;
 * that class's javadoc is where each value is argued.
 *
 * <p>The buffer is read through {@link NotifyingPullSubscriber#bufferUsage()} on a handle the
 * opener records, because once anything is drained the {@code messagesReceived} counter stops
 * equalling the deque. Sampling it from another thread is what the SPI requires of it.
 *
 * <p>{@link #start()} is what opens the subscriber, deliberately not the constructor: arms are
 * built one after another, each publishing its own backlog, so a subscriber opened in the
 * constructor would have been delivering for seconds before the last arm existed — a skew a
 * cross-arm comparison cannot absorb.
 */
final class BackpressuredArm {

    /** The lowest the service allows, so a lease lapses as soon as the client stops extending. */
    static final int ACK_DEADLINE_SECONDS = 10;

    /** The flow-control window under test; 10 leaves the emulator's growth flat after one wave. */
    static final long FLOW_CONTROL_MESSAGES = 50;

    /** Short enough to observe, and two deadlines wide so a plateau precedes the first wave. */
    static final Duration MAX_ACK_EXTENSION_PERIOD = Duration.ofSeconds(20);

    /**
     * How long the client holds a permit: the extension budget less one lease extension, because
     * {@code extendDeadlines} forgets a message it can no longer extend past the <em>next</em> one.
     * ADR-0066's series shows it directly — waves at 10.3 s and 20.4 s under a 20 s budget.
     *
     * <p>The term subtracted is {@code MessageDispatcher.messageDeadlineSeconds}, which starts at
     * {@code Subscriber.MIN_STREAM_ACK_DEADLINE} (10 s) and is then recomputed from a percentile of
     * observed acknowledgement latency — <em>not</em> the subscription's ack deadline, which it
     * coincides with here only because both are 10 s. Because it adapts, this is an approximation:
     * the arms sit a factor of five and three either side of the rate it implies rather than beside
     * it.
     */
    static final Duration PERMIT_HOLD = MAX_ACK_EXTENSION_PERIOD.minusSeconds(ACK_DEADLINE_SECONDS);

    /**
     * Messages a second the expiry wave returns to the client, {@code W/H}, and so roughly the
     * drain rate either side of which a buffer accumulates. Written as a formula rather than a
     * number so a change to either constant above moves every arm with it.
     */
    static final double REFILL_RATE = FLOW_CONTROL_MESSAGES / (double) PERMIT_HOLD.toSeconds();

    /** Never exhausted: at most a few {@link #REFILL_RATE} seconds' worth is delivered. */
    static final int BACKLOG = 400;

    /** One message per fetch, so a drain is a rate rather than a sawtooth of whole batches. */
    private static final int MAX_RECORDS_PER_FETCH = 1;

    private static final Duration SAMPLE_EVERY = Duration.ofMillis(250);
    private static final Duration WAKE_EVERY = Duration.ofMillis(200);
    private static final Duration DRAIN_IDLE_SLEEP = Duration.ofMillis(20);
    private static final Duration DRAIN_JOIN_TIMEOUT = Duration.ofSeconds(10);

    private final String name;
    private final double ratePerSecond;
    private final TestReaderMetrics metrics = new TestReaderMetrics();
    private final PubSubAckTracker ackTracker;
    private final AtomicReference<NotifyingPullSubscriber> subscriber = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    /** What killed the drain, so a run that lost its drain cannot be read as a measurement. */
    private final AtomicReference<Throwable> drainFailure = new AtomicReference<>();

    private final PubSubSplitReader reader;
    private final SubscriptionSplit split;

    /** Sampled buffer sizes, recorded only where the buffer or the delivery count changed. */
    private final List<String> steps = new ArrayList<>();

    /** Distinct message ids the drain saw, so a redelivered copy shows up as a repeat. */
    private final Set<String> distinctDrained = new HashSet<>();

    private volatile long drained;
    private volatile long buffered;
    private volatile long peakBuffered;
    private volatile long received;
    private volatile long nacked;
    private volatile long samples;
    private long lastBuffered = -1;
    private long lastReceived = -1;
    private long startNanos;
    @Nullable private Thread drainThread;

    /**
     * @param name the arm's name, used in the log line and the thread name
     * @param ratePerSecond messages a second to drain, or zero to never fetch at all
     * @param subscription the subscription to consume, already carrying its backlog
     * @param emulatorEndpoint the emulator, or {@code null} for production Pub/Sub
     */
    BackpressuredArm(
            String name,
            double ratePerSecond,
            SubscriptionDestination subscription,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.name = name;
        this.ratePerSecond = ratePerSecond;
        this.ackTracker = new PubSubAckTracker(metrics.metrics(), null);
        // As the source does: the gauges read the tracker, so they exist only once it is bound.
        metrics.metrics().bindAckTracker(ackTracker);

        PubSubSubscriberOptions options = options();
        DefaultSubscriberFactory factory =
                new DefaultSubscriberFactory(options, OrderingMode.NONE, emulatorEndpoint);
        this.reader =
                new PubSubSplitReader(
                        (assigned, signal) -> {
                            PubSubNotifyingPullSubscriber opened =
                                    new PubSubNotifyingPullSubscriber(
                                            assigned.splitId(),
                                            assigned.getSubscription(),
                                            factory,
                                            ackTracker,
                                            signal,
                                            options.getShutdownTimeout());
                            subscriber.set(opened);
                            return opened;
                        },
                        MAX_RECORDS_PER_FETCH,
                        new MissingCheckpointDetector(
                                Duration.ZERO, ackTracker::outstandingAckCount),
                        PausedSplitBufferLimits.of(options),
                        metrics.metrics());
        this.split = new SubscriptionSplit(subscription, "0");
    }

    /** The options every arm runs under; the paused-split bound is held out of the way. */
    static PubSubSubscriberOptions options() {
        return PubSubSubscriberOptions.builder()
                .flowControlMaxOutstandingElementCount(FLOW_CONTROL_MESSAGES)
                .maxAckExtensionPeriod(MAX_ACK_EXTENSION_PERIOD)
                // Moot as well as held out of the way: no split here is ever paused, and the bound
                // reads a paused split alone.
                .pausedSplitBufferMaxMessages(Long.MAX_VALUE)
                .pausedSplitBufferMaxBytes(Long.MAX_VALUE)
                .build();
    }

    /** A backlog of distinct payloads for one arm's subscription. */
    static String[] payloads(int count) {
        return IntStream.range(0, count)
                .mapToObj(index -> "m" + index)
                .collect(Collectors.toList())
                .toArray(new String[0]);
    }

    /**
     * Runs every arm for the window, sampling all of them on one clock, and tears them down.
     *
     * <p>The teardown is three passes rather than one per arm, and the order is load-bearing: a
     * close nacks and waits on its client, so folding it into the loop would stamp each arm's final
     * sample seconds after the one before it, and a cross-arm comparison would then be reading
     * different windows. Each pass is one {@link Closers#closeAll} list, so an arm that throws
     * cannot leave the others' clients running for the rest of the surefire fork.
     */
    static void runFor(Duration window, List<BackpressuredArm> arms) throws Exception {
        ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService waker = Executors.newSingleThreadScheduledExecutor();
        try {
            for (BackpressuredArm arm : arms) {
                arm.start();
            }
            sampler.scheduleAtFixedRate(
                    () -> arms.forEach(BackpressuredArm::sample),
                    0,
                    SAMPLE_EVERY.toMillis(),
                    TimeUnit.MILLISECONDS);
            // A fetch with nothing to drain parks on a signal only the next delivery completes, and
            // an arm that has caught up would otherwise sit in one past the end of the window.
            waker.scheduleAtFixedRate(
                    () -> arms.forEach(BackpressuredArm::wakeUp),
                    WAKE_EVERY.toMillis(),
                    WAKE_EVERY.toMillis(),
                    TimeUnit.MILLISECONDS);

            Thread.sleep(window.toMillis());
        } finally {
            waker.shutdownNow();
            sampler.shutdownNow();
            boolean samplerStopped =
                    sampler.awaitTermination(SAMPLE_EVERY.toMillis() * 4, TimeUnit.MILLISECONDS);
            List<AutoCloseable> teardown = new ArrayList<>(arms.size() * 2);
            // Asserted rather than assumed: the last sample below runs on this thread, and only a
            // terminated sampler makes the series and the counters safe to read across.
            teardown.add(
                    () -> {
                        if (!samplerStopped) {
                            throw new AssertionError(
                                    "The sampler did not stop, so its series cannot be read.");
                        }
                    });
            // Before the drains stop, not after: a delivery arrives in whole flow-control waves, so
            // one landing between a stopped drain and the last sample leaves an arm holding a wave
            // it was never given the chance to drain — a reading about the teardown rather than
            // about the drain rate. Live-drain sampling is what the 160 samples before it do
            // anyway.
            arms.forEach(arm -> teardown.add(arm::sample));
            arms.forEach(arm -> teardown.add(arm::stopDraining));
            arms.forEach(arm -> teardown.add(arm::close));
            Closers.closeAll(teardown);
        }
    }

    /** Opens the subscriber, which is when delivery starts, and starts the clock. */
    private void start() {
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
        startNanos = System.nanoTime();
        if (ratePerSecond == 0) {
            return;
        }
        drainThread = new Thread(this::drain, "drain-" + name);
        drainThread.start();
    }

    /** Wakes a fetch parked waiting for a delivery that the window may not contain. */
    private void wakeUp() {
        if (drainThread != null) {
            reader.wakeUp();
        }
    }

    /**
     * Records the buffer's size if it, or the delivery count, has changed since the last sample.
     */
    private void sample() {
        NotifyingPullSubscriber opened = subscriber.get();
        if (opened == null) {
            return;
        }
        long current = opened.bufferUsage().messages();
        buffered = current;
        received = metrics.counter("messagesReceived");
        // Sampled rather than read at the end, because the close nacks everything the arm still
        // holds: read afterwards this would always equal `received` and could say nothing about
        // whether a redelivery superseded a message mid-run.
        nacked = metrics.counter("messagesNacked");
        peakBuffered = Math.max(peakBuffered, current);
        samples++;
        // Delivery as well as the buffer, because an arm that keeps up holds nothing: its buffer
        // reads zero from the moment it catches up, and a series keyed on the buffer alone ends
        // there while the arm goes on being delivered hundreds of messages. #440 was filed against
        // an emulator read as starving an arm it had in fact been generous to, off exactly that
        // gap. `drained` is left out because it moves on nearly every sample of a live arm, and a
        // step per sample is the series this condition exists to avoid.
        if (current != lastBuffered || received != lastReceived) {
            lastBuffered = current;
            lastReceived = received;
            steps.add(
                    String.format(
                            "%.1fs:buffered=%d,received=%d,drained=%d",
                            (System.nanoTime() - startNanos) / 1e9, current, received, drained));
        }
    }

    private void stopDraining() throws Exception {
        running.set(false);
        if (drainThread != null) {
            reader.wakeUp();
            drainThread.join(DRAIN_JOIN_TIMEOUT.toMillis());
            if (drainThread.isAlive()) {
                // Closing the reader under a live drain would race close() against fetch() on the
                // split reader, and the failure would read as a connector defect rather than as
                // this harness giving up.
                throw new AssertionError(
                        "The drain of " + name + " did not stop within " + DRAIN_JOIN_TIMEOUT);
            }
        }
        Throwable failure = drainFailure.get();
        if (failure != null) {
            // A drain that died mid-run leaves every number below looking like a measurement of a
            // slower arm, and every assertion in this class passes more easily for it.
            throw new AssertionError("The drain of " + name + " failed", failure);
        }
    }

    private void close() throws Exception {
        reader.close();
    }

    /** Messages the subscriber is holding as of the last {@link #sample()}. */
    long buffered() {
        return buffered;
    }

    /** The largest {@link #buffered()} any sample saw. */
    long peakBuffered() {
        return peakBuffered;
    }

    /** Messages the client library delivered to the connector, redelivered copies included. */
    long received() {
        return received;
    }

    /** Messages the drain took out of the subscriber. */
    long drained() {
        return drained;
    }

    /** How many samples were taken, so a series cut short by a dead sampler cannot read as flat. */
    long samples() {
        return samples;
    }

    /** Distinct message ids among {@link #drained()}; fewer means copies were redelivered. */
    long distinct() {
        synchronized (distinctDrained) {
            return distinctDrained.size();
        }
    }

    /**
     * Messages nacked as of the last {@link #sample()}, which is taken before the close.
     *
     * <p>This is the direct observation of a redelivery. While an arm runs, the only thing that
     * nacks is {@code PubSubAckTracker.addPendingAck} superseding a message the connector still
     * holds with the copy the service has just redelivered — a park nacks too, but no split here is
     * paused, and the teardown's nack lands after the last sample.
     */
    long nacked() {
        return nacked;
    }

    /** Fetches no faster than the arm's rate, one message at a time. */
    private void drain() {
        while (running.get()) {
            double elapsedSeconds = (System.nanoTime() - startNanos) / 1e9;
            if (drained >= ratePerSecond * elapsedSeconds) {
                try {
                    Thread.sleep(DRAIN_IDLE_SLEEP.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                continue;
            }
            try {
                RecordsWithSplitIds<PubsubMessage> records = reader.fetch();
                while (records.nextSplit() != null) {
                    PubsubMessage message;
                    while ((message = records.nextRecordFromSplit()) != null) {
                        synchronized (distinctDrained) {
                            distinctDrained.add(message.getMessageId());
                        }
                        drained++;
                    }
                }
            } catch (Exception e) {
                if (running.get()) {
                    // Recorded rather than thrown: this thread has no handler, so a throw here
                    // would be swallowed and the run would be reported as a measurement.
                    drainFailure.compareAndSet(null, e);
                }
                return;
            }
        }
    }

    @Override
    public String toString() {
        return String.format(
                "%s(rate=%.2f/s buffered=%d peak=%d received=%d drained=%d distinct=%d nacked=%d"
                        + " samples=%d) %s",
                name,
                ratePerSecond,
                buffered,
                peakBuffered,
                received,
                drained,
                distinct(),
                nacked,
                samples,
                String.join(" ", steps));
    }
}
