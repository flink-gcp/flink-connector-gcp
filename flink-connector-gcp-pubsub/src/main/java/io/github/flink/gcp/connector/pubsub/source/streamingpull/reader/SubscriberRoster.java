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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.util.ExceptionUtils;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The reader's assigned splits and the subscriber slot serving each one, with every transition a
 * slot makes: opened on assignment, paused and resumed, parked past the paused-split buffer bound,
 * reopened on resume, and closed together at teardown.
 *
 * <p>Every method runs on the fetcher thread, so the slot map needs no synchronization; the only
 * thing that crosses threads is the data-available signal, raised through the {@code Runnable}
 * handed to each subscriber it opens.
 *
 * <p>Logs through the {@link PubSubSplitReader} logger it is constructed with, so the park warning
 * and the lifecycle lines keep the category operators already watch.
 */
@Internal
final class SubscriberRoster {

    private final PullSubscriberOpener subscriberOpener;
    private final int maxRecordsPerFetch;
    private final PausedSplitBufferLimits pausedSplitBufferLimits;
    private final SubscriberBufferBudget bufferBudget;
    private final PubSubSourceReaderMetrics metrics;
    private final Logger log;
    private final Runnable dataAvailableSignal;

    /** Assigned splits by id, in assignment order so drains visit splits fairly. */
    private final Map<String, SubscriberSlot> slots = new LinkedHashMap<>();

    /**
     * @param subscriberOpener opens the subscriber backing each split
     * @param maxRecordsPerFetch how many messages one drain takes from each split
     * @param pausedSplitBufferLimits the bound past which a paused split's subscriber is parked
     * @param metrics the reader's metrics, which own the parked-split count because it outlives
     *     this object
     * @param log the reader's logger, so what this class reports stays under the reader's category
     * @param dataAvailableSignal raised by every subscriber this roster opens
     */
    SubscriberRoster(
            PullSubscriberOpener subscriberOpener,
            int maxRecordsPerFetch,
            PausedSplitBufferLimits pausedSplitBufferLimits,
            SubscriberBufferBudget bufferBudget,
            PubSubSourceReaderMetrics metrics,
            Logger log,
            Runnable dataAvailableSignal) {
        this.subscriberOpener = subscriberOpener;
        this.maxRecordsPerFetch = maxRecordsPerFetch;
        this.pausedSplitBufferLimits = pausedSplitBufferLimits;
        this.bufferBudget = bufferBudget;
        this.metrics = metrics;
        this.log = log;
        this.dataAvailableSignal = dataAvailableSignal;
    }

    /** Opens a subscriber for the split and adds its slot, unless the split is already held. */
    void addSplit(SubscriptionSplit split) {
        if (slots.containsKey(split.splitId())) {
            // The enumerator recomputes assignments deterministically, so a re-registered reader
            // may be handed a split it already consumes.
            return;
        }
        log.info("Opening a Pub/Sub subscriber for split {}.", split.splitId());
        PullSubscriber subscriber = openSubscriber(split);
        slots.put(split.splitId(), new SubscriberSlot(split, subscriber));
        registerSubscriber(split, subscriber);
    }

    private PullSubscriber openSubscriber(SubscriptionSplit split) {
        try {
            return subscriberOpener.open(split, dataAvailableSignal);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to open the Pub/Sub subscriber for split " + split.splitId(), e);
        }
    }

    private void registerSubscriber(SubscriptionSplit split, PullSubscriber subscriber) {
        SubscriberBufferBudget.Admission registration =
                bufferBudget.register(split.splitId(), subscriber::requestStop);
        // The one place a subscriber is registered, so the one place the buffer gauges have to
        // hear about it; a reopen after a park re-registers under the same split id.
        metrics.subscriberOpened(split.splitId(), subscriber);
        // After the slot and metric are installed: a registration racing a pending reader-wide
        // park promotes it to failure, and cleanup must still own this subscriber if responding
        // raises an exception.
        registration.respond();
    }

    /** Closes the split's subscriber and drops its slot, if this roster holds one. */
    void removeSplit(SubscriptionSplit split) {
        SubscriberSlot slot = slots.remove(split.splitId());
        if (slot == null) {
            return;
        }
        // Below the guard, not above it: the registry is shared with every split reader this
        // reader's supplier makes, so evicting on a removal this one does not own would drop
        // another's live subscriber from the gauges with nothing to say so.
        bufferBudget.unregister(split.splitId());
        metrics.subscriberClosed(split.splitId());
        if (slot.isParked()) {
            // Nothing to close and nothing to nack: parking already did both. The count still has
            // to be given back, or the gauge keeps reporting a split that no longer exists.
            metrics.splitUnparked();
            return;
        }
        PullSubscriber subscriber = slot.subscriber();
        log.info("Closing the Pub/Sub subscriber for removed split {}.", split.splitId());
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

    /**
     * Drains up to {@code maxRecordsPerFetch} messages from every unpaused split into the builder,
     * and returns how many it drained in total.
     */
    BufferUsage drainInto(RecordsBySplits.Builder<PubsubMessage> builder) throws IOException {
        int total = 0;
        long totalBytes = 0;
        for (Map.Entry<String, SubscriberSlot> entry : slots.entrySet()) {
            SubscriberSlot slot = entry.getValue();
            if (slot.isPaused()) {
                // Skipped here and watched by checkPausedSplitFailures instead, which is the
                // only thing that reports a paused subscriber's failure. A parked split is a paused
                // one, so it never reaches the pull below.
                continue;
            }
            List<PubsubMessage> messages = slot.subscriber().pullMessages(maxRecordsPerFetch);
            if (!messages.isEmpty()) {
                builder.addAll(entry.getKey(), messages);
                total += messages.size();
                totalBytes += messages.stream().mapToLong(PubsubMessage::getSerializedSize).sum();
            }
        }
        return BufferUsage.of(total, totalBytes);
    }

    /**
     * Reports a permanent failure of a split the drain above skipped (#348).
     *
     * <p>{@link #drainInto} reports one for every split it visits, because {@link
     * PullSubscriber#pullMessages} throws — but a paused split is not visited, and nothing else
     * ever reads that subscriber's failure. So a subscriber that dies while watermark alignment
     * holds its split leaves the job running and green with one subscription silently dead, and the
     * reader closing while it is still paused loses the failure for good, since the client's
     * re-report at teardown is absorbed (#325).
     *
     * <p>Evaluated from {@link PubSubSplitReader#fetch()}, and beside {@link
     * MissingCheckpointDetector#check()} rather than inside the drain, because the two are the same
     * kind of guard: both exist for a state whose whole symptom is the <em>absence</em> of records,
     * which no record-driven check can see. What separates "paused and healthy" from "paused and
     * dead" here is the subscriber's recorded failure and nothing about its message count, so a
     * pause the job is meant to have costs nothing.
     */
    void checkPausedSplitFailures() throws IOException {
        // Driven from the slot map, which is a LinkedHashMap in assignment order. The reason is
        // reproducibility: with two paused splits failing at once the iteration order decides
        // which failure is reported, and only one of the two orders is stable — which is why the
        // paused bit lives on the slot inside that map rather than in an unordered set of ids
        // beside it. A pause for an id the map does not hold is dropped where it is requested,
        // though that guard is belt and braces: SplitFetcherManager.pauseOrResumeSplits filters
        // requested ids through the fetcher's assignedSplits before enqueuing the task (checked
        // against flink-connector-base 2.1.2 sources and 1.20.0 bytecode), and AddSplitsTask
        // populates that map and calls handleSplitsChanges in the same task on this thread.
        for (SubscriberSlot slot : slots.values()) {
            // A parked split has no client, so there is no failure for it to have. What that costs
            // is stated where the park happens.
            if (slot.isPaused() && !slot.isParked()) {
                slot.subscriber().checkFailure();
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
     * <p>Evaluated from {@link PubSubSplitReader#fetch()} after {@link
     * #checkPausedSplitFailures()}, and the order is load-bearing: parking runs {@code close()},
     * which absorbs what the client raises (#325), so a split that is both dead and overfull has to
     * fail the job rather than be quietly stopped. Failing first covers every split; the one that
     * could fail in between is covered by the {@code checkFailure} at the head of the list below.
     *
     * <p>The release goes through <b>one {@link Closers#closeAll(Iterable)} list holding every
     * parked split's steps, every shutdown before any close</b> — the shape and the reason of
     * {@link #closeAll()} (#297). Alignment pauses a subtask's splits as a group and they cross the
     * bound in the same wave, so parking them one at a time would spend {@code shutdownTimeout} per
     * split on the fetcher thread, serially, exactly the {@code splits × timeout} cost that method
     * exists to avoid. Starting every shutdown first overlaps the waits into one, and the single
     * list keeps every nack running when an earlier step throws.
     */
    void parkOverfullPausedSplits() throws IOException {
        boolean readerWidePark = bufferBudget.parkingRequested();
        // Two lists so the closes can be appended after every shutdown, as closeAll() orders them.
        // Allocated only once an overfull paused split is found, so the ordinary path — the one
        // that must cost nothing, because a job that never aligns watermarks pauses no split —
        // allocates nothing. It does walk the slot map once, which the base file's set-emptiness
        // fast path did not: with pause state in the slots there is no set to test, and the walk
        // is O(assigned splits) beside drainInto's identical walk in the same fetch.
        List<AutoCloseable> steps = null;
        List<AutoCloseable> closes = null;
        for (Map.Entry<String, SubscriberSlot> entry : slots.entrySet()) {
            SubscriberSlot slot = entry.getValue();
            if (slot.isParked() || !slot.isPaused()) {
                continue;
            }
            PullSubscriber subscriber = slot.subscriber();
            BufferUsage usage = subscriber.bufferUsage();
            if (!readerWidePark && !pausedSplitBufferLimits.exceededBy(usage)) {
                continue;
            }
            if (readerWidePark) {
                log.warn(
                        "The source reader's assigned splits are all paused and their subscribers"
                                + " have filled the aggregate subscriber buffer budget at {}, so"
                                + " split {} is being parked and its messages returned for"
                                + " redelivery; a fresh subscriber opens when the split resumes.",
                        bufferBudget.usage(),
                        entry.getKey());
            } else {
                log.warn(
                        "Paused split {} has buffered {}, past the {} a paused split may hold, so"
                                + " its Pub/Sub subscriber is being stopped and its messages"
                                + " returned for redelivery; a fresh subscriber opens when the"
                                + " split resumes. The split is paused by watermark alignment, and"
                                + " a pause this long usually means an aligned source has gone idle"
                                + " without withIdleness(...) on its watermark strategy: add it, or"
                                + " bring the aligned group's slow member forward. Raising"
                                + " pausedSplitBufferMaxMessages or pausedSplitBufferMaxBytes only"
                                + " holds more memory for a split nothing is consuming, so prefer"
                                + " it only for a pause you expect to end.",
                        entry.getKey(),
                        usage,
                        pausedSplitBufferLimits);
            }
            // Marked parked before the release, so a release that throws cannot leave the reader
            // holding a half-closed client it would go on to drain or close a second time.
            slot.park();
            metrics.splitParked();
            metrics.subscriberClosed(entry.getKey());
            if (steps == null) {
                steps = new ArrayList<>();
                closes = new ArrayList<>();
            }
            // The failure check first, and this is its last chance: after the park there is no
            // client for checkPausedSplitFailures to watch, and close() absorbs the client's own
            // report of it (#325).
            steps.add(subscriber::checkFailure);
            steps.add(subscriber::shutdown);
            String splitId = entry.getKey();
            // Behind shutdown in the same failure-tolerant list. Until shutdown marks the client
            // closed, an SDK callback can still race in; keeping its paused registration visible
            // prevents that callback from turning a paused-split park into an active-reader FAIL.
            steps.add(() -> bufferBudget.unregister(splitId));
            closes.add(subscriber);
        }
        if (closes == null) {
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

    /**
     * Pauses and resumes the given splits' slots, reopening a parked one before its pause lifts.
     */
    void pauseOrResume(
            Collection<SubscriptionSplit> splitsToPause,
            Collection<SubscriptionSplit> splitsToResume) {
        // A streaming-pull client cannot be paused, so a paused split is simply not drained. The
        // client library's flow control then stops pulling once its outstanding limit fills — for
        // one maxAckExtensionPeriod, after which parkOverfullPausedSplits() is the bound (#357).
        List<String> assignedPauses = new ArrayList<>();
        for (SubscriptionSplit split : splitsToPause) {
            if (slots.containsKey(split.splitId())) {
                assignedPauses.add(split.splitId());
            }
        }
        // One budget transition for the whole Flink request. A crossing callback may run before or
        // after it, but cannot observe only part of an alignment group as paused.
        bufferBudget.setPaused(assignedPauses, true);
        for (SubscriptionSplit split : splitsToPause) {
            SubscriberSlot slot = slots.get(split.splitId());
            if (slot != null) {
                slot.pause();
            }
        }
        List<String> assignedResumes = new ArrayList<>();
        for (SubscriptionSplit split : splitsToResume) {
            if (slots.containsKey(split.splitId())) {
                assignedResumes.add(split.splitId());
            }
        }
        bufferBudget.setPaused(assignedResumes, false);
        for (SubscriptionSplit split : splitsToResume) {
            SubscriberSlot slot = slots.get(split.splitId());
            if (slot == null) {
                continue;
            }
            // Reopened before the pause is lifted, so "parked implies paused" holds at every
            // point: a reopen that throws would otherwise leave a split with no subscriber that
            // the drain no longer skips, and the drain dereferences it.
            reopenIfParked(slot);
            slot.resume();
        }
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
    private void reopenIfParked(SubscriberSlot slot) {
        if (!slot.isParked()) {
            return;
        }
        log.info(
                "Reopening the Pub/Sub subscriber for split {}, parked while it was paused.",
                slot.split().splitId());
        PullSubscriber subscriber = openSubscriber(slot.split());
        slot.reopen(subscriber);
        registerSubscriber(slot.split(), subscriber);
        metrics.splitUnparked();
    }

    /** Closes every open subscriber and drops every slot; the reader's {@code close()}. */
    void closeAll() throws Exception {
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
            List<PullSubscriber> open = openSubscribers();
            List<AutoCloseable> steps = new ArrayList<>(open.size() * 2);
            for (PullSubscriber subscriber : open) {
                steps.add(subscriber::shutdown);
            }
            steps.addAll(open);
            Closers.closeAll(steps);
        } finally {
            // Give the parked count back before dropping the splits, as removeSplit does: the
            // gauge lives in the reader's metrics because it outlives this object, so a reader
            // closing while it holds parked splits would leave it reporting splits that are gone.
            for (Map.Entry<String, SubscriberSlot> entry : slots.entrySet()) {
                bufferBudget.unregister(entry.getKey());
                if (entry.getValue().isParked()) {
                    metrics.splitUnparked();
                }
                // The buffer gauges would report zero for these anyway — shutdown() empties a
                // subscriber's buffer — but the metrics outlive this reader, so leaving them
                // registered would leave it holding clients a closed reader no longer owns.
                metrics.subscriberClosed(entry.getKey());
            }
            slots.clear();
        }
    }

    /** Returns the subscribers of every split that is not parked, in assignment order. */
    private List<PullSubscriber> openSubscribers() {
        List<PullSubscriber> open = new ArrayList<>(slots.size());
        for (SubscriberSlot slot : slots.values()) {
            if (!slot.isParked()) {
                open.add(slot.subscriber());
            }
        }
        return open;
    }
}
