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

import org.apache.flink.connector.base.source.reader.RecordsBySplits;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the roster properties that are its own rather than inherited from {@link
 * PubSubSplitReader}, whose test exercises the same code through the reader's SPI surface.
 */
@Timeout(30)
class SubscriberRosterTest {

    private static final SubscriptionSplit SPLIT =
            new SubscriptionSplit(SubscriptionDestination.of("project", "sub-a"), "0");

    private final Map<String, FakePullSubscriber> subscribers = new HashMap<>();
    private final TestReaderMetrics readerMetrics = new TestReaderMetrics();

    /** Set to make the next opener call fail. */
    private IOException failNextOpen;

    private SubscriberRoster roster(long maxMessages) {
        return new SubscriberRoster(
                (split, signal) -> {
                    if (failNextOpen != null) {
                        IOException failure = failNextOpen;
                        failNextOpen = null;
                        throw failure;
                    }
                    FakePullSubscriber subscriber =
                            new FakePullSubscriber(signal).named(split.splitId());
                    subscribers.put(split.splitId(), subscriber);
                    return subscriber;
                },
                10,
                PausedSplitBufferLimits.of(
                        PubSubSubscriberOptions.builder()
                                .pausedSplitBufferMaxMessages(maxMessages)
                                .pausedSplitBufferMaxBytes(Long.MAX_VALUE)
                                .build()),
                readerMetrics.metrics(),
                LoggerFactory.getLogger(PubSubSplitReader.class),
                () -> {});
    }

    @Test
    void aFailedReopenLeavesTheSlotParkedAndPaused() throws Exception {
        // The half of "parked implies paused" that only a throwing opener can reach: the reopen
        // runs before the pause is lifted, so a resume whose reopen fails must leave the slot
        // both parked and paused — or the next drain dereferences a subscriber that is gone.
        SubscriberRoster roster = roster(1);
        roster.addSplit(SPLIT);
        roster.pauseOrResume(List.of(SPLIT), List.of());
        subscribers.get(SPLIT.splitId()).deliver(message("1"), message("2"));
        roster.parkOverfullPausedSplits();
        failNextOpen = new IOException("no such subscription");

        assertThatThrownBy(() -> roster.pauseOrResume(List.of(), List.of(SPLIT)))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("no such subscription");

        RecordsBySplits.Builder<PubsubMessage> builder = new RecordsBySplits.Builder<>();
        assertThat(roster.drainInto(builder)).isZero();
        assertThat(readerMetrics.gauge("parkedSplits")).isEqualTo(1);
        roster.closeAll();
    }

    @Test
    void theParkReportsAFailureItMeetsAndStillReleasesTheClient() throws Exception {
        // The in-list checkFailure is a failed paused split's last chance between the fetch
        // guard and the park — after the park there is no client to watch, and close() absorbs
        // the client's own report (#325). Three properties in one scripted incident: the failure
        // surfaces as the same IOException instance (the rethrow preserves type and identity),
        // the slot is already parked when it does (marked before the release), and the one-list
        // release keeps running past the throw so the client is still shut down and closed.
        SubscriberRoster roster = roster(1);
        roster.addSplit(SPLIT);
        roster.pauseOrResume(List.of(SPLIT), List.of());
        FakePullSubscriber subscriber = subscribers.get(SPLIT.splitId());
        subscriber.deliver(message("1"), message("2"));
        IOException cause = new IOException("stream died while paused");
        subscriber.failWith(cause);

        assertThatThrownBy(roster::parkOverfullPausedSplits).isSameAs(cause);

        assertThat(readerMetrics.gauge("parkedSplits")).isEqualTo(1);
        assertThat(subscriber.isShutdownRequested()).isTrue();
        assertThat(subscriber.isClosed()).isTrue();
        roster.closeAll();
    }

    @Test
    void aPauseForAnUnassignedSplitIsDroppedNotStored() throws Exception {
        // SplitFetcherManager filters requested ids through the fetcher's assignedSplits, so this
        // does not happen in production. The base file's set recorded the id anyway, which would
        // have started a later-assigned split with that id silently paused; the slot shape drops
        // the request, and the drain after assignment is what tells the two apart.
        SubscriberRoster roster = roster(Long.MAX_VALUE);
        roster.pauseOrResume(List.of(SPLIT), List.of(SPLIT));

        roster.addSplit(SPLIT);
        subscribers.get(SPLIT.splitId()).deliver(message("1"));
        RecordsBySplits.Builder<PubsubMessage> builder = new RecordsBySplits.Builder<>();
        assertThat(roster.drainInto(builder)).isEqualTo(1);
        roster.closeAll();
    }

    private static PubsubMessage message(String payload) {
        return PubsubMessage.newBuilder()
                .setMessageId(payload)
                .setData(ByteString.copyFromUtf8(payload))
                .build();
    }
}
