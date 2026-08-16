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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.core.io.InputStatus;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.DeserializationFailurePolicy;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import io.github.flink.gcp.connector.testutils.CollectingReaderOutput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Measures what sustained downstream backpressure does to the guards {@link
 * PubSubSplitReader#fetch()} carries ([#377]).
 *
 * <p><b>This class measures a framework premise rather than covering a feature</b>, the way {@link
 * PubSubPausedSplitBufferITCase} does for a paused split. The premise, read out of {@code
 * flink-connector-base} and pinned here: {@code FetchTask.run()} keeps its {@code lastRecords}
 * until {@code elementsQueue.put(...)} succeeds and skips {@code splitReader.fetch()} while it
 * holds one, and {@code FutureCompletingBlockingQueue.put} blocks while the queue is at capacity
 * ({@code source.reader.element.queue.capacity}, 2 by default). A subtask whose downstream has
 * stopped consuming therefore stops entering {@code fetch()} altogether — and every guard the
 * reader evaluates there stops running with it: the paused-split failure check (#348), the
 * paused-split buffer bound (#357) and {@link MissingCheckpointDetector}.
 *
 * <p>One counter over {@code fetch()} entries covers all three, because all three sit inside that
 * one method. Counting entries is therefore the instrument, not an assertion per guard — a guard
 * added there later is covered by the same measurement, and a guard moved out of it is the change
 * this class is asking about.
 *
 * <p>{@link MissingCheckpointDetector} is the guard singled out because backpressure produces
 * exactly the state it exists to catch: a reader holding unacknowledged messages while no
 * checkpoint completes. Its clock is injected here, so "the budget is spent" is a fact the test
 * sets rather than a duration it waits out; what is left to wait for is only the fetcher thread
 * reaching its block.
 *
 * <p>This runs without an emulator: the messages come from {@link FakePullSubscriber}, and what is
 * under measurement is Flink's fetcher loop, not Pub/Sub. Whether the *buffer* grows in this state
 * is a different question, measured against the emulator by {@link
 * PubSubBackpressuredSplitBufferITCase}.
 */
@Timeout(60)
class PubSubBackpressuredReaderGuardTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(PubSubBackpressuredReaderGuardTest.class);

    /** Flink's own default, set explicitly because the arithmetic below quotes it. */
    private static final int ELEMENT_QUEUE_CAPACITY = 2;

    /**
     * Messages the fake subscriber holds. Large enough that a frozen fetch loop is unmistakable
     * against {@link #MAX_RECORDS_PER_FETCH} of 1: the loop can only have entered {@code fetch()} a
     * handful of times, with dozens of messages still waiting.
     */
    private static final int BUFFERED = 64;

    /** One message per fetch, so the freeze is counted in fetches rather than in batches. */
    private static final int MAX_RECORDS_PER_FETCH = 1;

    /** Nominal, since the clock is injected; only its ratio to the advanced clock matters. */
    private static final Duration DETECTOR_BUDGET = Duration.ofSeconds(10);

    /**
     * How long the frozen fetch count is watched after the detector's budget is spent. A blocked
     * fetcher stays blocked forever, so this only has to outlast the scheduling noise of a loop
     * whose iterations are microseconds when it is running at all.
     */
    private static final Duration BLIND_WINDOW = Duration.ofMillis(500);

    /** No change for this long means the fetcher has reached its block rather than being slow. */
    private static final Duration QUIESCENT_FOR = Duration.ofMillis(300);

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(20);

    private static final SubscriptionSplit SPLIT =
            new SubscriptionSplit(SubscriptionDestination.of("it-project", "backpressured"), "0");

    private final TestReaderMetrics testMetrics = new TestReaderMetrics();
    private final PubSubAckTracker ackTracker = new PubSubAckTracker(testMetrics.metrics(), null);
    private final CollectingReaderOutput<String> output = new CollectingReaderOutput<>();

    /** The detector's clock: zero until a test declares the budget spent. */
    private final AtomicLong clockNanos = new AtomicLong();

    /**
     * One instance, shared by the reader and the split reader as production shares it: the reader
     * retires it at a barrier, the split reader evaluates it in {@code fetch()}.
     */
    private final MissingCheckpointDetector checkpointDetector =
            new MissingCheckpointDetector(
                    DETECTOR_BUDGET, ackTracker::outstandingAckCount, clockNanos::get);

    /** Entries into {@code PubSubSplitReader.fetch()}, incremented on the fetcher thread. */
    private final AtomicInteger fetches = new AtomicInteger();

    /** Messages handed out of the subscriber, incremented on the fetcher thread. */
    private final AtomicInteger pulled = new AtomicInteger();

    @Test
    void aStalledFetchLoopStopsEnteringFetchAndOneFreedSlotRunsItAgain() throws Exception {
        try (PubSubSourceReader<String> reader = reader(MAX_RECORDS_PER_FETCH, BUFFERED)) {
            reader.addSplits(List.of(SPLIT));
            // One record out is all it takes: the fetcher refills the slot that freed and then has
            // nowhere to put the next batch.
            pollUntilEmitted(reader, 1);

            int frozen = awaitFetchesFrozen();
            LOG.info(
                    "Stalled fetch loop (#377): fetch() entered {} times and then stopped, with {}"
                            + " of {} messages still held by the subscriber, at"
                            + " maxRecordsPerFetch={} and elementQueueCapacity={}",
                    frozen,
                    BUFFERED - pulled.get(),
                    BUFFERED,
                    MAX_RECORDS_PER_FETCH,
                    ELEMENT_QUEUE_CAPACITY);
            // An equality, not a bound, because the arithmetic is exact: the queue's capacity, the
            // fetch the reader is working through, and the batch the fetcher cannot put. A
            // different number means Flink's fetcher holds a different amount, which is the
            // premise this class measures rather than a guard that kept running.
            assertThat(frozen)
                    .as(
                            "the loop stops once the queue, the current fetch and one held batch are"
                                    + " full; a different count means the fetcher's shape changed")
                    .isEqualTo(ELEMENT_QUEUE_CAPACITY + 2);
            assertThat(output.records())
                    .as("almost everything the subscriber holds was never fetched")
                    .hasSizeLessThan(BUFFERED / 4);

            // Everything the detector fires on is now true: budget spent, messages outstanding.
            clockNanos.set(DETECTOR_BUDGET.multipliedBy(2).toNanos());
            assertThat(ackTracker.outstandingAckCount()).isPositive();

            Thread.sleep(BLIND_WINDOW.toMillis());

            assertThat(fetches.get())
                    .as("the guard-bearing loop did not run once while its condition held")
                    .isEqualTo(frozen);

            // The firing control, in the same trial, and the distinction the issue does not draw.
            // A downstream that is merely *slow* frees an element-queue slot for every batch it
            // takes, and each freed slot lets the blocked put through and one more fetch() run — so
            // ordinary backpressure delays a guard by one drain interval rather than skipping it.
            // Only the stall above blinds it, and there nothing on another thread would help
            // either: the same stall means pollNext is not called to surface what was recorded.
            assertThatThrownBy(() -> pollUntilThrows(reader))
                    .rootCause()
                    .hasMessageContaining("No checkpoint has been taken within");
            LOG.info(
                    "Slowly drained fetch loop (#377): the guard ran {} fetch(es) after the freeze"
                            + " at {}, on the slot the first poll freed",
                    fetches.get() - frozen,
                    frozen);
            assertThat(fetches.get() - frozen)
                    .as("one freed slot ran the guard, not a backlog of fetches")
                    .isBetween(1, ELEMENT_QUEUE_CAPACITY);
        }
    }

    @Test
    void aDrainedFetchLoopKeepsEnteringFetch() throws Exception {
        // The control for the counter itself: the freeze above is caused by the backpressure, not
        // by a fetcher that died, a subscriber that ran dry or a decorator that stopped counting.
        try (PubSubSourceReader<String> reader = reader(MAX_RECORDS_PER_FETCH, BUFFERED)) {
            reader.addSplits(List.of(SPLIT));

            pollUntilEmitted(reader, BUFFERED);

            assertThat(fetches.get()).isGreaterThan(ELEMENT_QUEUE_CAPACITY + 2);
            assertThat(pulled.get()).isEqualTo(BUFFERED);
        }
    }

    @Test
    void aFrozenLoopPinsWholeBatchesWhereTheSubscribersBufferCannotSeeThem() throws Exception {
        // What `bufferUsage()` reports is the subscriber's deque alone, and the bound built on it
        // (#357) is evaluated against that. Everything the fetcher has already pulled is held in
        // the element queue, in the fetch the reader is working through and in the batch the
        // fetcher cannot put — invisible to the deque and to every metric.
        int batch = 1000;
        int buffered = 5 * batch;
        try (PubSubSourceReader<String> reader = reader(batch, buffered)) {
            reader.addSplits(List.of(SPLIT));
            pollUntilEmitted(reader, 1);
            awaitFetchesFrozen();

            int pinned = pulled.get() - output.records().size();

            LOG.info(
                    "Backpressured reader footprint (#377): {} messages pulled, {} emitted,"
                            + " {} pinned outside the subscriber's buffer, at"
                            + " maxRecordsPerFetch={} and elementQueueCapacity={}",
                    pulled.get(),
                    output.records().size(),
                    pinned,
                    batch,
                    ELEMENT_QUEUE_CAPACITY);

            // The queue plus the current fetch plus the held batch, less the one record
            // emitted to free the slot that let the third put through.
            assertThat(pinned).isEqualTo((ELEMENT_QUEUE_CAPACITY + 2) * batch - 1);
        }
    }

    /** Polls until {@code records} have been emitted, or fails. */
    private void pollUntilEmitted(PubSubSourceReader<String> reader, int records) throws Exception {
        long deadline = System.nanoTime() + POLL_TIMEOUT.toNanos();
        while (output.records().size() < records) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError(
                        "Only "
                                + output.records().size()
                                + " of "
                                + records
                                + " records were emitted within "
                                + POLL_TIMEOUT);
            }
            if (reader.pollNext(output) == InputStatus.NOTHING_AVAILABLE) {
                Thread.sleep(5);
            }
        }
    }

    /** Polls until something is thrown, or fails. */
    private void pollUntilThrows(PubSubSourceReader<String> reader) throws Exception {
        long deadline = System.nanoTime() + POLL_TIMEOUT.toNanos();
        while (System.nanoTime() - deadline < 0) {
            if (reader.pollNext(output) == InputStatus.NOTHING_AVAILABLE) {
                Thread.sleep(5);
            }
        }
        throw new AssertionError("Nothing was thrown within " + POLL_TIMEOUT);
    }

    /**
     * Waits for the fetch loop to stop and returns how many times it ran.
     *
     * <p>Two conditions, and the floor is the one that is easy to leave out: the loop has to have
     * reached the state where it <em>can</em> block — the queue full, a fetch in hand, a batch it
     * cannot put — before a stretch of no change means it is blocked. Without it, a fetcher thread
     * descheduled for {@link #QUIESCENT_FOR} anywhere in that ramp returns a partial count, and
     * every caller then measures against a state the reader was still climbing towards.
     */
    private int awaitFetchesFrozen() throws InterruptedException {
        long deadline = System.nanoTime() + POLL_TIMEOUT.toNanos();
        int last = -1;
        long stableSince = System.nanoTime();
        while (System.nanoTime() - deadline < 0) {
            int current = fetches.get();
            if (current != last) {
                last = current;
                stableSince = System.nanoTime();
            } else if (current >= ELEMENT_QUEUE_CAPACITY + 2
                    && System.nanoTime() - stableSince >= QUIESCENT_FOR.toNanos()) {
                return current;
            }
            Thread.sleep(10);
        }
        throw new AssertionError(
                "The fetch loop never settled at "
                        + (ELEMENT_QUEUE_CAPACITY + 2)
                        + " entries within "
                        + POLL_TIMEOUT
                        + "; last "
                        + last);
    }

    private PubSubSourceReader<String> reader(int maxRecordsPerFetch, int buffered) {
        Configuration configuration = new Configuration();
        configuration.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, ELEMENT_QUEUE_CAPACITY);
        return new PubSubSourceReader<>(
                () -> countingSplitReader(maxRecordsPerFetch, buffered),
                new PubSubRecordEmitter<>(
                        PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()),
                        ackTracker,
                        DeserializationFailurePolicy.FAIL,
                        testMetrics.metrics()),
                configuration,
                new FakeSourceReaderContext(testMetrics.metricGroup(), configuration),
                ackTracker,
                checkpointDetector);
    }

    private SplitReader<PubsubMessage, SubscriptionSplit> countingSplitReader(
            int maxRecordsPerFetch, int buffered) {
        PubSubSplitReader delegate =
                new PubSubSplitReader(
                        (split, signal) -> preloaded(split, signal, buffered),
                        maxRecordsPerFetch,
                        checkpointDetector,
                        PausedSplitBufferLimits.of(
                                PubSubSubscriberOptions.builder()
                                        .pausedSplitBufferMaxMessages(Long.MAX_VALUE)
                                        .pausedSplitBufferMaxBytes(Long.MAX_VALUE)
                                        .build()),
                        testMetrics.metrics());
        return new FetchCountingSplitReader(delegate);
    }

    /**
     * Opens a subscriber already holding {@code count} messages, each registered with the ack
     * tracker as the real subscriber registers one on arrival — which is what makes {@code
     * outstandingAckCount} nonzero, and so the detector's condition true.
     *
     * <p>Runs on the fetcher thread, inside {@code handleSplitsChanges}, which is the same thread
     * that later drains the fake — so the fake needs no synchronization of its own.
     */
    private PullSubscriber preloaded(
            SubscriptionSplit split, Runnable dataAvailableSignal, int count) {
        FakePullSubscriber subscriber = new FakePullSubscriber(dataAvailableSignal);
        List<PubsubMessage> messages = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            String messageId = "m" + index;
            ackTracker.addPendingAck(split.splitId(), messageId, new RecordingAckHandle(messageId));
            messages.add(
                    PubsubMessage.newBuilder()
                            .setMessageId(messageId)
                            .setData(ByteString.copyFromUtf8(messageId))
                            .build());
        }
        subscriber.deliver(messages.toArray(new PubsubMessage[0]));
        return new PullCountingSubscriber(subscriber, pulled);
    }

    /** Counts entries into {@link PubSubSplitReader#fetch()}, whatever the fetch then does. */
    private final class FetchCountingSplitReader
            implements SplitReader<PubsubMessage, SubscriptionSplit> {

        private final PubSubSplitReader delegate;

        private FetchCountingSplitReader(PubSubSplitReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public RecordsWithSplitIds<PubsubMessage> fetch() throws IOException {
            // Counted before delegating, so a fetch that throws is still an entry.
            fetches.incrementAndGet();
            return delegate.fetch();
        }

        @Override
        public void handleSplitsChanges(SplitsChange<SubscriptionSplit> splitsChange) {
            delegate.handleSplitsChanges(splitsChange);
        }

        @Override
        public void wakeUp() {
            delegate.wakeUp();
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }

    /** Counts messages handed out of the subscriber, which the deque no longer holds. */
    private static final class PullCountingSubscriber implements PullSubscriber {

        private final PullSubscriber delegate;
        private final AtomicInteger pulled;

        private PullCountingSubscriber(PullSubscriber delegate, AtomicInteger pulled) {
            this.delegate = delegate;
            this.pulled = pulled;
        }

        @Override
        public List<PubsubMessage> pullMessages(int maxMessages) throws IOException {
            List<PubsubMessage> messages = delegate.pullMessages(maxMessages);
            pulled.addAndGet(messages.size());
            return messages;
        }

        @Override
        public void checkFailure() throws IOException {
            delegate.checkFailure();
        }

        @Override
        public BufferUsage bufferUsage() {
            return delegate.bufferUsage();
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }
}
