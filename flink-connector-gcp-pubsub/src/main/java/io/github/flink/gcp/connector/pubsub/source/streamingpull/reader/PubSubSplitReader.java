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
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Multiplexes the streaming-pull subscribers of one reader subtask's splits.
 *
 * <p>Every method except {@link #wakeUp()} runs on the fetcher thread, so the subscriber map needs
 * no synchronization; only the data-available signal crosses threads.
 *
 * <p>The signal is armed <em>before</em> draining, so a message that arrives while the drain is in
 * progress completes the armed future and the following wait returns at once instead of missing the
 * notification.
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

    /** Subscribers by split id, in assignment order so drains visit splits fairly. */
    private final Map<String, NotifyingPullSubscriber> subscribers = new LinkedHashMap<>();

    private final Set<String> pausedSplits = new HashSet<>();

    private final Object signalLock = new Object();

    @GuardedBy("signalLock")
    private CompletableFuture<Void> dataAvailable = new CompletableFuture<>();

    /**
     * Creates the split reader.
     *
     * @param subscriberFactory creates the client backing each split
     * @param ackTracker tracks the acknowledgement lifecycle of received messages
     * @param maxRecordsPerFetch the maximum number of messages drained per split per fetch
     */
    public PubSubSplitReader(
            SubscriberFactory subscriberFactory, AckTracker ackTracker, int maxRecordsPerFetch) {
        this(
                (split, signal) ->
                        new PubSubNotifyingPullSubscriber(
                                split.splitId(),
                                split.getSubscription(),
                                subscriberFactory,
                                ackTracker,
                                signal),
                maxRecordsPerFetch);
    }

    @VisibleForTesting
    PubSubSplitReader(SubscriberOpener subscriberOpener, int maxRecordsPerFetch) {
        this.subscriberOpener = subscriberOpener;
        this.maxRecordsPerFetch = maxRecordsPerFetch;
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
        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<SubscriptionSplit> splitsChange) {
        if (splitsChange instanceof SplitsAddition) {
            for (SubscriptionSplit split : splitsChange.splits()) {
                addSplit(split);
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
        Exception failure = null;
        for (Map.Entry<String, NotifyingPullSubscriber> entry : subscribers.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                // Keep closing the rest: every subscriber left open holds messages that Pub/Sub
                // would only redeliver after their acknowledgement deadline expires.
                LOG.warn("Failed to close the Pub/Sub subscriber for split {}.", entry.getKey(), e);
                failure = e;
            }
        }
        subscribers.clear();
        pausedSplits.clear();
        if (failure != null) {
            throw failure;
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

    private CompletableFuture<Void> armSignal() {
        synchronized (signalLock) {
            dataAvailable = new CompletableFuture<>();
            return dataAvailable;
        }
    }

    private void signalDataAvailable() {
        CompletableFuture<Void> current;
        synchronized (signalLock) {
            current = dataAvailable;
        }
        current.complete(null);
    }

    private void await(CompletableFuture<Void> signal) throws IOException {
        try {
            signal.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            // Unreachable: the signal is only ever completed normally. Subscriber failures surface
            // from pullMessages on the following drain.
            throw new IOException("Failed while waiting for Pub/Sub messages.", e);
        }
    }
}
