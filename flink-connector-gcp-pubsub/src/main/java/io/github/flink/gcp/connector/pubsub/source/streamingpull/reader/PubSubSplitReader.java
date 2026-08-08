/*
 * Copyright 2023 Google LLC
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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;
import org.apache.flink.util.ExceptionUtils;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Multiplexes the streaming-pull subscribers of one reader subtask's splits.
 *
 * <p>Every method except {@link #wakeUp()} runs on the fetcher thread, so the subscriber map needs
 * no synchronization; only the data-available signal crosses threads.
 *
 * <p>The data-available signal is armed <em>before</em> draining, so a message arriving mid-drain
 * completes the armed future rather than being missed, and it is level-triggered, so a signal that
 * arrives while no wait is armed is remembered instead of dropped. Both matter: a fetch that parks
 * on a lost signal is never woken again on the shutdown path, which would leave the subscribers
 * open and their messages unnacked.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0).
 * Deviations: one shared signal instead of a per-subscriber notification future composed on every
 * fetch (which accumulated callbacks on idle subscribers), draining many messages per fetch instead
 * of exactly one per split, and support for split removal and for pausing splits (watermark
 * alignment), which upstream rejects and omits respectively.
 */
@Internal
public class PubSubSplitReader implements SplitReader<PubsubMessage, SubscriptionSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSplitReader.class);

    private final SubscriberOpener subscriberOpener;
    private final int maxRecordsPerFetch;
    private final MissingCheckpointDetector checkpointDetector;
    private final PausedSplitBufferLimits pausedSplitBufferLimits;
    private final PubSubSourceReaderMetrics metrics;

    /** Assigned splits by id, in assignment order so drains visit splits fairly. */
    private final Map<String, AssignedSplit> splits = new LinkedHashMap<>();

    private final Set<String> pausedSplits = new HashSet<>();

    private final Object signalLock = new Object();

    @GuardedBy("signalLock")
    private CompletableFuture<Void> dataAvailable = new CompletableFuture<>();

    /** Remembers a signal that arrived while no wait was armed, so it cannot be lost. */
    @GuardedBy("signalLock")
    private boolean signalled;

    /**
     * Creates the split reader.
     *
     * @param subscriberFactory creates the client backing each split
     * @param ackTracker tracks the acknowledgement lifecycle of received messages
     * @param options the subscriber tuning options, which carry the per-fetch drain size and the
     *     per-subscriber shutdown budget
     * @param checkpointDetector fails the reader if checkpoints never arrive; armed by the first
     *     split assignment and evaluated from {@link #fetch()}, because the state it detects is the
     *     state with no records
     * @param metrics the reader's metrics, which own the parked-split count because it outlives
     *     this object
     */
    public PubSubSplitReader(
            SubscriberFactory subscriberFactory,
            AckTracker ackTracker,
            PubSubSubscriberOptions options,
            MissingCheckpointDetector checkpointDetector,
            PubSubSourceReaderMetrics metrics) {
        this(
                (split, signal) ->
                        new PubSubNotifyingPullSubscriber(
                                split.splitId(),
                                split.getSubscription(),
                                subscriberFactory,
                                ackTracker,
                                signal,
                                options.getShutdownTimeout()),
                options.getMaxRecordsPerFetch(),
                checkpointDetector,
                PausedSplitBufferLimits.of(options),
                metrics);
    }

    @VisibleForTesting
    PubSubSplitReader(
            SubscriberOpener subscriberOpener,
            int maxRecordsPerFetch,
            MissingCheckpointDetector checkpointDetector,
            PausedSplitBufferLimits pausedSplitBufferLimits,
            PubSubSourceReaderMetrics metrics) {
        this.subscriberOpener = subscriberOpener;
        this.maxRecordsPerFetch = maxRecordsPerFetch;
        this.checkpointDetector = checkpointDetector;
        this.pausedSplitBufferLimits = pausedSplitBufferLimits;
        this.metrics = metrics;
    }

    /**
     * One assigned split and the subscriber serving it.
     *
     * <p>The split itself is retained because reopening a parked one needs it, and a {@code null}
     * subscriber <em>is</em> the parked state: {@link #addSplit} either opens one or throws, so
     * nothing else can produce one, and a second flag could disagree with it.
     */
    private static final class AssignedSplit {

        private final SubscriptionSplit split;
        @Nullable private NotifyingPullSubscriber subscriber;

        private AssignedSplit(SubscriptionSplit split, NotifyingPullSubscriber subscriber) {
            this.split = split;
            this.subscriber = subscriber;
        }

        private boolean isParked() {
            return subscriber == null;
        }
    }

    /** Opens the subscriber backing one split; the seam that lets tests supply a fake client. */
    @FunctionalInterface
    @VisibleForTesting
    interface SubscriberOpener {

        /**
         * Opens a subscriber for the given split.
         *
         * @param split the split to consume
         * @param dataAvailableSignal invoked when the subscriber has messages or has failed
         * @return the opened subscriber
         * @throws IOException if the subscriber cannot be opened
         */
        NotifyingPullSubscriber open(SubscriptionSplit split, Runnable dataAvailableSignal)
                throws IOException;
    }

    @Override
    public RecordsWithSplitIds<PubsubMessage> fetch() throws IOException {
        CompletableFuture<Void> signal = armSignal();
        RecordsBySplits.Builder<PubsubMessage> builder = new RecordsBySplits.Builder<>();
        if (drainInto(builder) == 0) {
            // Nothing buffered: park until a message arrives, a subscriber fails, or the fetcher is
            // woken up to run a queued task (which is how new splits reach this reader).
            await(signal);
            drainInto(builder);
        }
        checkPausedSplitsForFailure();
        parkOverfullPausedSplits();
        checkpointDetector.check();
        return builder.build();
    }

    /**
     * Reports a permanent failure of a split the drain above skipped (#348).
     *
     * <p>{@link #drainInto} reports one for every split it visits, because {@link
     * NotifyingPullSubscriber#pullMessages} throws — but a paused split is not visited, and nothing
     * else ever reads that subscriber's failure. So a subscriber that dies while watermark
     * alignment holds its split leaves the job running and green with one subscription silently
     * dead, and the reader closing while it is still paused loses the failure for good, since the
     * client's re-report at teardown is absorbed (#325).
     *
     * <p>Evaluated from {@link #fetch()}, and beside {@link MissingCheckpointDetector#check()}
     * rather than inside the drain, because the two are the same kind of guard: both exist for a
     * state whose whole symptom is the <em>absence</em> of records, which no record-driven check
     * can see. What separates "paused and healthy" from "paused and dead" here is the subscriber's
     * recorded failure and nothing about its message count, so a pause the job is meant to have
     * costs nothing.
     */
    private void checkPausedSplitsForFailure() throws IOException {
        // Driven from the subscriber map rather than from pausedSplits. The reason is
        // reproducibility: pausedSplits is a HashSet while subscribers is a LinkedHashMap in
        // assignment order, so with two paused splits failing at once the iteration order decides
        // which failure is reported, and only one of the two orders is stable. It also removes a
        // deref — pauseOrResumeSplits adds an id unconditionally — though that one is belt and
        // braces: SplitFetcherManager.pauseOrResumeSplits filters requested ids through the
        // fetcher's assignedSplits before enqueuing the task (checked against flink-connector-base
        // 2.1.2 sources and 1.20.0 bytecode), and AddSplitsTask populates that map and calls
        // handleSplitsChanges in the same task on this thread.
        for (Map.Entry<String, AssignedSplit> entry : splits.entrySet()) {
            AssignedSplit assigned = entry.getValue();
            // A parked split has no client, so there is no failure for it to have. What that costs
            // is stated where the park happens.
            if (pausedSplits.contains(entry.getKey()) && !assigned.isParked()) {
                assigned.subscriber.checkFailure();
            }
        }
    }

    /**
     * Stops the subscriber of any paused split whose buffer has outgrown its bound, to be reopened
     * when the split resumes (#357).
     *
     * <p><b>The bound exists because the client library's does not hold.</b> A paused split is
     * never drained, and what was supposed to cap it is flow control — but the client stops
     * extending a message's deadline once {@code maxAckExtensionPeriod} has passed since it was
     * received, and releases that message's flow-control permit when it does, while this reader is
     * still holding the message. Permits therefore free up at the rate messages expire, pulling
     * resumes, and the buffer grows for as long as the pause lasts (measured by {@code
     * PubSubPausedSplitBufferITCase}: one flow-control window per period, indefinitely). An
     * indefinite pause is ordinary — an aligned group holds its slowest member's watermark, so one
     * quiet subscription pauses every other split until the strategy's idleness lets it go — which
     * makes this a memory hazard rather than a stall.
     *
     * <p>Stopping the client is the response because the alternatives are worse where it counts.
     * Refusing messages in the receiver callback means either blocking a client-library thread,
     * which stalls an ordering key's dispatch chain, or nacking a message already leased, which
     * loops it back through delivery and spends a dead-letter policy's attempts on a split nobody
     * is consuming. Failing the job trades an eventual heap exhaustion for a restart into the same
     * state. Stopping the client returns every lease at once ({@code NACK_IMMEDIATELY}), frees the
     * buffer, and leaves a split that is doing nothing costing nothing — which is what a pause
     * asked for.
     *
     * <p>Evaluated from {@link #fetch()} after {@link #checkPausedSplitsForFailure()}, and the
     * order is load-bearing: parking runs {@code close()}, which absorbs what the client raises
     * (#325), so a split that is both dead and overfull has to fail the job rather than be quietly
     * stopped. Failing first covers every split; the one that could fail in between is covered by
     * the {@code checkFailure} at the head of the list below.
     *
     * <p>The release goes through <b>one {@link Closers#closeAll} list holding every parked split's
     * steps, every shutdown before any close</b> — the shape and the reason of {@link #close()}
     * (#297). Alignment pauses a subtask's splits as a group and they cross the bound in the same
     * wave, so parking them one at a time would spend {@code shutdownTimeout} per split on the
     * fetcher thread, serially, exactly the {@code splits × timeout} cost that method exists to
     * avoid. Starting every shutdown first overlaps the waits into one, and the single list keeps
     * every nack running when an earlier step throws.
     */
    private void parkOverfullPausedSplits() throws IOException {
        if (pausedSplits.isEmpty()) {
            // The ordinary path, and the one that must cost nothing: a job that never aligns
            // watermarks pauses no split, so it neither walks the map a second time nor allocates.
            return;
        }
        // Two lists so the closes can be appended after every shutdown, as close() orders them.
        List<AutoCloseable> steps = new ArrayList<>();
        List<AutoCloseable> closes = new ArrayList<>();
        for (Map.Entry<String, AssignedSplit> entry : splits.entrySet()) {
            AssignedSplit assigned = entry.getValue();
            if (assigned.isParked() || !pausedSplits.contains(entry.getKey())) {
                continue;
            }
            BufferUsage usage = assigned.subscriber.bufferUsage();
            if (!pausedSplitBufferLimits.exceededBy(usage)) {
                continue;
            }
            LOG.warn(
                    "Paused split {} has buffered {}, past the {} a paused split may hold, so its"
                            + " Pub/Sub subscriber is being stopped and its messages returned for"
                            + " redelivery; a fresh subscriber opens when the split resumes. The split"
                            + " is paused by watermark alignment, and a pause this long usually"
                            + " means an aligned source has gone idle without withIdleness(...) on"
                            + " its watermark strategy: add it, or bring the aligned group's slow"
                            + " member forward. Raising pausedSplitBufferMaxMessages or"
                            + " pausedSplitBufferMaxBytes only holds more memory for a split"
                            + " nothing is consuming, so prefer it only for a pause you expect to"
                            + " end.",
                    entry.getKey(),
                    usage,
                    pausedSplitBufferLimits);
            NotifyingPullSubscriber subscriber = assigned.subscriber;
            // Marked parked before the release, so a release that throws cannot leave the reader
            // holding a half-closed client it would go on to drain or close a second time.
            assigned.subscriber = null;
            metrics.splitParked();
            // The failure check first, and this is its last chance: after the park there is no
            // client for checkPausedSplitsForFailure to watch, and close() absorbs the client's own
            // report of it (#325).
            steps.add(subscriber::checkFailure);
            steps.add(subscriber::shutdown);
            closes.add(subscriber);
        }
        if (closes.isEmpty()) {
            return;
        }
        steps.addAll(closes);
        try {
            Closers.closeAll(steps);
        } catch (Exception e) {
            // Rethrown as-is when it is the IOException checkFailure raises, so a failure reported
            // here is the same failure, of the same type, the drain would have reported.
            ExceptionUtils.rethrowIOException(e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<SubscriptionSplit> splitsChange) {
        if (splitsChange instanceof SplitsAddition) {
            for (SubscriptionSplit split : splitsChange.splits()) {
                addSplit(split);
            }
            if (!splitsChange.splits().isEmpty()) {
                // Not on an empty addition: the budget must not start before there is work.
                checkpointDetector.startBudget();
            }
        } else if (splitsChange instanceof SplitsRemoval) {
            for (SubscriptionSplit split : splitsChange.splits()) {
                removeSplit(split);
            }
        } else {
            throw new IllegalArgumentException("Unsupported split change: " + splitsChange);
        }
    }

    private void addSplit(SubscriptionSplit split) {
        if (splits.containsKey(split.splitId())) {
            // The enumerator recomputes assignments deterministically, so a re-registered reader
            // may be handed a split it already consumes.
            return;
        }
        LOG.info("Opening a Pub/Sub subscriber for split {}.", split.splitId());
        splits.put(split.splitId(), new AssignedSplit(split, openSubscriber(split)));
    }

    private NotifyingPullSubscriber openSubscriber(SubscriptionSplit split) {
        try {
            return subscriberOpener.open(split, this::signalDataAvailable);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to open the Pub/Sub subscriber for split " + split.splitId(), e);
        }
    }

    private void removeSplit(SubscriptionSplit split) {
        AssignedSplit assigned = splits.remove(split.splitId());
        pausedSplits.remove(split.splitId());
        if (assigned == null) {
            return;
        }
        if (assigned.isParked()) {
            // Nothing to close and nothing to nack: parking already did both. The count still has
            // to be given back, or the gauge keeps reporting a split that no longer exists.
            metrics.splitUnparked();
            return;
        }
        NotifyingPullSubscriber subscriber = assigned.subscriber;
        LOG.info("Closing the Pub/Sub subscriber for removed split {}.", split.splitId());
        try {
            // The failure check comes before the close, and is the last chance to report one: a
            // split removed while paused carries a failure nothing has read — the check in fetch()
            // is its only reader and has not run since — and close() absorbs the client's own
            // report of it (#325). Unlike the close path, removal happens while the job carries on,
            // so this is where a failure would be lost rather than merely raced by a teardown.
            //
            // Unreachable today, and kept anyway: SplitsRemoval reaches a reader only through
            // SourceReaderBase's eofRecordEvaluator branch, and PubSubSourceReader supplies none,
            // while the enumerator never removes a split either. One list, so the subscriber is
            // released whichever step throws.
            Closers.closeAll(subscriber::checkFailure, subscriber);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to close the Pub/Sub subscriber for split " + split.splitId(), e);
        }
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<SubscriptionSplit> splitsToPause,
            Collection<SubscriptionSplit> splitsToResume) {
        // A streaming-pull client cannot be paused, so a paused split is simply not drained. The
        // client library's flow control then stops pulling once its outstanding limit fills — for
        // one maxAckExtensionPeriod, after which parkOverfullPausedSplits() is the bound (#357).
        splitsToPause.forEach(split -> pausedSplits.add(split.splitId()));
        for (SubscriptionSplit split : splitsToResume) {
            // Reopened before the pause is lifted, so "parked implies paused" holds at every
            // point: a reopen that throws would otherwise leave a split with no subscriber that
            // the drain no longer skips, and the drain dereferences it.
            reopenIfParked(split.splitId());
            pausedSplits.remove(split.splitId());
        }
        // Pausing is itself an event the guards in fetch() have to see, because after it there may
        // be nothing left to wake them. A split paused while it still holds messages an earlier
        // fetch did not drain is already over its bound with its signal spent, and the client has
        // stopped delivering — so the next fetch drains nothing, waits on a signal that never
        // comes, and never reaches the checks that sit after that wait. One signal here costs an
        // empty fetch and closes it.
        //
        // Growth and failure each carry their own signal (receiveMessage and fail both raise it),
        // so this is the pause's own case and not a second belt for theirs.
        signalDataAvailable();
    }

    /**
     * Opens a fresh subscriber for a split that was parked while paused.
     *
     * <p>Here rather than lazily in the drain, mirroring {@link #addSplit}: the reopen belongs
     * where its cause is, on the same thread and through the same seam, and a drain should not
     * throw for a reason that is not about draining. A reopen that fails fails the job, exactly as
     * an assignment that cannot open its subscriber does — the same client failure, reported
     * whenever it happens.
     */
    private void reopenIfParked(String splitId) {
        AssignedSplit assigned = splits.get(splitId);
        if (assigned == null || !assigned.isParked()) {
            return;
        }
        LOG.info(
                "Reopening the Pub/Sub subscriber for split {}, parked while it was paused.",
                splitId);
        assigned.subscriber = openSubscriber(assigned.split);
        metrics.splitUnparked();
    }

    @Override
    public void wakeUp() {
        signalDataAvailable();
    }

    @Override
    public void close() throws Exception {
        try {
            // Start every shutdown before waiting on any. shutdown() nacks the split's messages and
            // returns at once, while the wait inside close() costs up to shutdownTimeout each.
            // Waiting one subscriber at a time costs splits × timeout — past roughly six splits on
            // one reader that exceeds Flink's source.reader.close.timeout (30 s by default), and
            // the splits whose turn never came would not have been nacked at all, leaving their
            // messages to expire instead. Starting them all first overlaps the waits, so the total
            // is one timeout however many splits the reader owns.
            //
            // One list rather than a loop and then a call (#297): closeAll runs every entry before
            // reporting anything, so the later nacks and every close still run when a shutdown
            // throws — a bare loop would skip the closes wholesale, leaving every subscriber open
            // holding messages Pub/Sub only redelivers once their acknowledgement deadline
            // expires. The order within the list is the property the paragraph above argues for.
            //
            // A parked split contributes neither step: its shutdown already ran, and with it the
            // nack, so there is nothing left to release.
            List<NotifyingPullSubscriber> open = openSubscribers();
            List<AutoCloseable> steps = new ArrayList<>(open.size() * 2);
            for (NotifyingPullSubscriber subscriber : open) {
                steps.add(subscriber::shutdown);
            }
            steps.addAll(open);
            Closers.closeAll(steps);
        } finally {
            // Give the parked count back before dropping the splits, as removeSplit does: the
            // gauge lives in the reader's metrics because it outlives this object, so a reader
            // closing while it holds parked splits would leave it reporting splits that are gone.
            for (AssignedSplit assigned : splits.values()) {
                if (assigned.isParked()) {
                    metrics.splitUnparked();
                }
            }
            splits.clear();
            pausedSplits.clear();
        }
    }

    /** Returns the subscribers of every split that is not parked, in assignment order. */
    private List<NotifyingPullSubscriber> openSubscribers() {
        List<NotifyingPullSubscriber> open = new ArrayList<>(splits.size());
        for (AssignedSplit assigned : splits.values()) {
            if (!assigned.isParked()) {
                open.add(assigned.subscriber);
            }
        }
        return open;
    }

    private int drainInto(RecordsBySplits.Builder<PubsubMessage> builder) throws IOException {
        int total = 0;
        for (Map.Entry<String, AssignedSplit> entry : splits.entrySet()) {
            AssignedSplit assigned = entry.getValue();
            if (pausedSplits.contains(entry.getKey())) {
                // Skipped here and watched by checkPausedSplitsForFailure instead, which is the
                // only thing that reports a paused subscriber's failure. A parked split is a paused
                // one, so it never reaches the pull below.
                continue;
            }
            List<PubsubMessage> messages = assigned.subscriber.pullMessages(maxRecordsPerFetch);
            if (!messages.isEmpty()) {
                builder.addAll(entry.getKey(), messages);
                total += messages.size();
            }
        }
        return total;
    }

    /**
     * Returns the future the next wait parks on, which is already complete when a signal arrived
     * while no wait was in progress.
     *
     * <p>The remembered flag is what makes the signal level-triggered, and it is load-bearing:
     * {@code SplitFetcher} checks its own wake-up flag <em>before</em> entering {@link #fetch()}
     * and calls {@link #wakeUp()} exactly once per event, so an edge-triggered signal delivered in
     * the window between that check and the arming below would be lost. On the shutdown path that
     * is unrecoverable — nothing else would ever wake the fetcher, so the reader would never be
     * closed and its messages never nacked.
     */
    private CompletableFuture<Void> armSignal() {
        synchronized (signalLock) {
            if (signalled) {
                signalled = false;
                return CompletableFuture.completedFuture(null);
            }
            dataAvailable = new CompletableFuture<>();
            return dataAvailable;
        }
    }

    private void signalDataAvailable() {
        CompletableFuture<Void> current;
        synchronized (signalLock) {
            signalled = true;
            current = dataAvailable;
        }
        current.complete(null);
    }

    private void await(CompletableFuture<Void> signal) throws IOException {
        try {
            long parkTimeoutMillis = checkpointDetector.parkTimeoutMillis();
            if (parkTimeoutMillis > 0) {
                signal.get(parkTimeoutMillis, TimeUnit.MILLISECONDS);
            } else {
                signal.get();
            }
        } catch (TimeoutException e) {
            // Woke only to re-evaluate the checkpoint detector; the caller drains nothing and
            // returns an empty batch. Arming is level-triggered, so a signal that arrived while
            // this wait was running is remembered by the next arm rather than lost.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            // Unreachable: the signal is only ever completed normally. Subscriber failures surface
            // from pullMessages on the following drain.
            throw new IOException("Failed while waiting for Pub/Sub messages.", e);
        }
    }
}
