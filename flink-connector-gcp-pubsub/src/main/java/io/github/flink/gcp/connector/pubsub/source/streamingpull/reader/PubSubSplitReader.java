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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriberBufferLimitExceededEvent;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Multiplexes the streaming-pull subscribers of one reader subtask's splits.
 *
 * <p>Every method except {@link #wakeUp()} runs on the fetcher thread, so the {@link
 * SubscriberRoster} needs no synchronization; only the {@link DataAvailabilitySignal} crosses
 * threads.
 */
@Internal
public class PubSubSplitReader implements SplitReader<PubsubMessage, SubscriptionSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSplitReader.class);

    private final SubscriberRoster roster;
    private final DataAvailabilitySignal signal;
    private final MissingCheckpointDetector checkpointDetector;
    private final PubSubSourceReaderMetrics metrics;

    /**
     * Creates the split reader and reports a callback-side subscriber-buffer overflow through the
     * source coordinator.
     *
     * @param subscriberFactory creates the SDK subscriber for each assigned split
     * @param ackTracker holds acknowledgement handles until checkpoint completion
     * @param options source-reader and subscriber options
     * @param checkpointDetector detects a source that receives messages without checkpoints
     * @param metrics source-reader metrics
     * @param failureReporter sends a hard-buffer-limit event to the source coordinator
     */
    public PubSubSplitReader(
            SubscriberFactory subscriberFactory,
            AckTracker ackTracker,
            PubSubSubscriberOptions options,
            MissingCheckpointDetector checkpointDetector,
            PubSubSourceReaderMetrics metrics,
            Consumer<SubscriberBufferLimitExceededEvent> failureReporter) {
        this(
                subscriberFactory,
                ackTracker,
                options,
                checkpointDetector,
                metrics,
                new SubscriberBufferBudget(
                        options.getSubscriberBufferMaxMessages(),
                        options.getSubscriberBufferMaxBytes(),
                        failureReporter));
    }

    PubSubSplitReader(
            SubscriberFactory subscriberFactory,
            AckTracker ackTracker,
            PubSubSubscriberOptions options,
            MissingCheckpointDetector checkpointDetector,
            PubSubSourceReaderMetrics metrics,
            SubscriberBufferBudget bufferBudget) {
        this(
                new DefaultPullSubscriberOpener(
                        subscriberFactory, ackTracker, options.getShutdownTimeout(), bufferBudget),
                options.getMaxRecordsPerFetch(),
                checkpointDetector,
                PausedSplitBufferLimits.of(options),
                metrics,
                bufferBudget);
    }

    @VisibleForTesting
    PubSubSplitReader(
            PullSubscriberOpener subscriberOpener,
            int maxRecordsPerFetch,
            MissingCheckpointDetector checkpointDetector,
            PausedSplitBufferLimits pausedSplitBufferLimits,
            PubSubSourceReaderMetrics metrics) {
        this(
                subscriberOpener,
                maxRecordsPerFetch,
                checkpointDetector,
                pausedSplitBufferLimits,
                metrics,
                SubscriberBufferBudget.unbounded());
    }

    @VisibleForTesting
    PubSubSplitReader(
            PullSubscriberOpener subscriberOpener,
            int maxRecordsPerFetch,
            MissingCheckpointDetector checkpointDetector,
            PausedSplitBufferLimits pausedSplitBufferLimits,
            PubSubSourceReaderMetrics metrics,
            SubscriberBufferBudget bufferBudget) {
        this.checkpointDetector = checkpointDetector;
        this.metrics = metrics;
        this.signal = new DataAvailabilitySignal();
        this.roster =
                new SubscriberRoster(
                        subscriberOpener,
                        maxRecordsPerFetch,
                        pausedSplitBufferLimits,
                        bufferBudget,
                        metrics,
                        LOG,
                        signal::raise);
    }

    /**
     * Drains every unpaused split once and evaluates the reader's three guards.
     *
     * <p><b>How often this runs is Flink's to decide, and under backpressure it is the drain
     * rate</b> (#377, measured by {@code PubSubBackpressuredReaderGuardTest}). {@code FetchTask}
     * keeps the batch it could not hand over and skips this method while it holds one, so a
     * downstream that is merely slow delays each guard by one element-queue slot, while one that
     * has stopped outright stops them entirely. Placing those fetch-thread guards on a thread of
     * their own would not help the second case: the same stall means {@code pollNext} is not being
     * called, and it is the only path a fetcher's recorded failure has to the job. The reader-wide
     * subscriber-buffer guard is deliberately different: it runs on the SDK callback thread and
     * reports an active reader's crossing through the coordinator. The {@code bufferedMessages} and
     * {@code bufferedBytes} gauges remain available to a metric reporter whatever this loop is
     * doing.
     */
    @Override
    public RecordsWithSplitIds<PubsubMessage> fetch() throws IOException {
        CompletableFuture<Void> armed = signal.arm();
        RecordsBySplits.Builder<PubsubMessage> builder = new RecordsBySplits.Builder<>();
        BufferUsage fetched = roster.drainInto(builder);
        if (fetched.messages() == 0) {
            // Nothing buffered: park until a message arrives, a subscriber fails, or the fetcher is
            // woken up to run a queued task (which is how new splits reach this reader).
            signal.await(armed, checkpointDetector.parkTimeoutMillis());
            fetched = roster.drainInto(builder);
        }
        MeteredRecordsWithSplitIds records =
                new MeteredRecordsWithSplitIds(
                        builder.build(), fetched.messages(), fetched.bytes(), metrics);
        try {
            // Reports a permanent failure of a split the drain above skipped because it is paused
            // (#348).
            roster.checkPausedSplitFailures();
            // Bounds a paused split's buffer by stopping its subscriber (#357, ADR-0066).
            roster.parkOverfullPausedSplits();
            // Fails the reader if checkpoints never arrive while messages are outstanding
            // (ADR-0011).
            checkpointDetector.check();
            return records;
        } catch (IOException | RuntimeException | Error failure) {
            // The batch left the subscriber deques before the guards ran, so its fetcher metrics
            // start before them too. A failing guard hands no batch to Flink; recycle it so the
            // failed fetch does not leave retention attributed to the fetcher forever.
            try {
                records.recycle();
            } catch (RuntimeException | Error recycleFailure) {
                failure.addSuppressed(recycleFailure);
            }
            throw failure;
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<SubscriptionSplit> splitsChange) {
        if (splitsChange instanceof SplitsAddition) {
            for (SubscriptionSplit split : splitsChange.splits()) {
                roster.addSplit(split);
            }
            if (!splitsChange.splits().isEmpty()) {
                // Not on an empty addition: the budget must not start before there is work.
                checkpointDetector.startBudget();
            }
        } else if (splitsChange instanceof SplitsRemoval) {
            for (SubscriptionSplit split : splitsChange.splits()) {
                roster.removeSplit(split);
            }
        } else {
            throw new IllegalArgumentException("Unsupported split change: " + splitsChange);
        }
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<SubscriptionSplit> splitsToPause,
            Collection<SubscriptionSplit> splitsToResume) {
        roster.pauseOrResume(splitsToPause, splitsToResume);
        // Pausing is itself an event the guards in fetch() have to see, because after it there may
        // be nothing left to wake them. A split paused while it still holds messages an earlier
        // fetch did not drain is already over its bound with its signal spent, and the client has
        // stopped delivering — so the next fetch drains nothing, waits on a signal that never
        // comes, and never reaches the checks that sit after that wait. One signal here costs an
        // empty fetch and closes it.
        //
        // Growth and failure each carry their own signal (receiveMessage and fail both raise it),
        // so this is the pause's own case and not a second belt for theirs.
        signal.raise();
    }

    @Override
    public void wakeUp() {
        signal.raise();
    }

    @Override
    public void close() throws Exception {
        roster.closeAll();
    }
}
