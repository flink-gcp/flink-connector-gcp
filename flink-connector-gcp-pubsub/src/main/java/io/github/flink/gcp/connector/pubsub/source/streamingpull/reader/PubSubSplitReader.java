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

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /** Subscribers by split id, in assignment order so drains visit splits fairly. */
    private final Map<String, NotifyingPullSubscriber> subscribers = new LinkedHashMap<>();

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
     */
    public PubSubSplitReader(
            SubscriberFactory subscriberFactory,
            AckTracker ackTracker,
            PubSubSubscriberOptions options,
            MissingCheckpointDetector checkpointDetector) {
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
                checkpointDetector);
    }

    @VisibleForTesting
    PubSubSplitReader(
            SubscriberOpener subscriberOpener,
            int maxRecordsPerFetch,
            MissingCheckpointDetector checkpointDetector) {
        this.subscriberOpener = subscriberOpener;
        this.maxRecordsPerFetch = maxRecordsPerFetch;
        this.checkpointDetector = checkpointDetector;
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
        checkpointDetector.check();
        return builder.build();
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
        if (subscribers.containsKey(split.splitId())) {
            // The enumerator recomputes assignments deterministically, so a re-registered reader
            // may be handed a split it already consumes.
            return;
        }
        LOG.info("Opening a Pub/Sub subscriber for split {}.", split.splitId());
        try {
            subscribers.put(
                    split.splitId(), subscriberOpener.open(split, this::signalDataAvailable));
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to open the Pub/Sub subscriber for split " + split.splitId(), e);
        }
    }

    private void removeSplit(SubscriptionSplit split) {
        NotifyingPullSubscriber subscriber = subscribers.remove(split.splitId());
        pausedSplits.remove(split.splitId());
        if (subscriber == null) {
            return;
        }
        LOG.info("Closing the Pub/Sub subscriber for removed split {}.", split.splitId());
        try {
            subscriber.close();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to close the Pub/Sub subscriber for split " + split.splitId(), e);
        }
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<SubscriptionSplit> splitsToPause,
            Collection<SubscriptionSplit> splitsToResume) {
        // A streaming-pull client cannot be paused, so a paused split is simply not drained; the
        // client library's flow control then stops pulling once its buffer fills.
        splitsToPause.forEach(split -> pausedSplits.add(split.splitId()));
        splitsToResume.forEach(split -> pausedSplits.remove(split.splitId()));
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
            // reporting anything, so a shutdown that throws no longer skips the later nacks — nor
            // the closes, which a bare loop skipped wholesale, leaving every subscriber open
            // holding messages Pub/Sub would only redeliver once their acknowledgement deadline
            // expired. The order within the list is the property the paragraph above argues for.
            List<AutoCloseable> steps = new ArrayList<>(subscribers.size() * 2);
            for (NotifyingPullSubscriber subscriber : subscribers.values()) {
                steps.add(subscriber::shutdown);
            }
            steps.addAll(subscribers.values());
            Closers.closeAll(steps);
        } finally {
            subscribers.clear();
            pausedSplits.clear();
        }
    }

    private int drainInto(RecordsBySplits.Builder<PubsubMessage> builder) throws IOException {
        int total = 0;
        for (Map.Entry<String, NotifyingPullSubscriber> entry : subscribers.entrySet()) {
            if (pausedSplits.contains(entry.getKey())) {
                continue;
            }
            List<PubsubMessage> messages = entry.getValue().pullMessages(maxRecordsPerFetch);
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
