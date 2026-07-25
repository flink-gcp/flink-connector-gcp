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
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
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

    private final Map<String, FakeNotifyingPullSubscriber> subscribers = new HashMap<>();

    /** Set by the ordering test so the fakes record their release/close calls in one list. */
    private List<String> calls;

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
        FakeNotifyingPullSubscriber first = subscriberOf(SPLIT_A);

        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A)));

        assertThat(subscriberOf(SPLIT_A)).isSameAs(first);
        reader.close();
    }

    @Test
    void removingASplitClosesItsSubscriberAndStopsDrainingIt() throws Exception {
        PubSubSplitReader reader = reader(10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(SPLIT_A, SPLIT_B)));
        FakeNotifyingPullSubscriber removed = subscriberOf(SPLIT_A);
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
    void closeReleasesEverySubscriberBeforeWaitingOnAny() throws Exception {
        // Releasing nacks the split's messages and returns at once; close() then waits up to the
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
        // waited on until every one of them has been released.
        assertThat(calls).hasSize(4);
        assertThat(calls.subList(0, 2))
                .containsExactlyInAnyOrder(
                        "release:" + SPLIT_A.splitId(), "release:" + SPLIT_B.splitId());
        assertThat(calls.subList(2, 4))
                .containsExactlyInAnyOrder(
                        "close:" + SPLIT_A.splitId(), "close:" + SPLIT_B.splitId());
    }

    private PubSubSplitReader reader(int maxRecordsPerFetch) {
        return new PubSubSplitReader(
                (split, signal) -> {
                    FakeNotifyingPullSubscriber subscriber =
                            new FakeNotifyingPullSubscriber(signal).named(split.splitId());
                    if (calls != null) {
                        subscriber.recordCallsInto(calls);
                    }
                    subscribers.put(split.splitId(), subscriber);
                    return subscriber;
                },
                maxRecordsPerFetch,
                new MissingCheckpointDetector(Duration.ZERO, () -> 0));
    }

    private FakeNotifyingPullSubscriber subscriberOf(SubscriptionSplit split) {
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
