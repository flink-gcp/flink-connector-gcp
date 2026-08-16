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

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.pubsub.v1.Subscriber;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PausedSplitBufferLimits}, whose three-rung fallback is the thing at risk. */
class PausedSplitBufferLimitsTest {

    private static final FlowControlSettings SDK_DEFAULTS =
            Subscriber.Builder.getDefaultFlowControlSettings();

    @Test
    void unsetKnobsFallBackToTwiceTheClientLibrarysOwnFlowControlDefaults() {
        PausedSplitBufferLimits limits =
                PausedSplitBufferLimits.of(PubSubSubscriberOptions.builder().build());

        // Read from the SDK rather than restated, so a version that changed them cannot leave this
        // bound pointing at a number the client no longer enforces — and doubled, because a bound
        // at the limit itself has one message of headroom and a healthy split can reach it.
        assertThat(limits.maxMessages())
                .isEqualTo(2 * SDK_DEFAULTS.getMaxOutstandingElementCount());
        assertThat(limits.maxBytes()).isEqualTo(2 * SDK_DEFAULTS.getMaxOutstandingRequestBytes());
    }

    @Test
    void aHealthyBufferAtTheFlowControlLimitIsNotOverTheDefaultBound() {
        // The reason for the headroom, as a case rather than a comment. Each of these puts the
        // buffer above the flow-control limit without any lease having lapsed: one oversized
        // message (gax clamps the byte reservation and lets its permits go negative), a dead-letter
        // subscription's delivery-attempt attribute (added after the reservation), and a redelivery
        // buffered beside the copy whose permit was just released. A bound at the limit would park
        // a healthy split for any of them.
        PausedSplitBufferLimits limits =
                PausedSplitBufferLimits.of(
                        PubSubSubscriberOptions.builder()
                                .flowControlMaxOutstandingElementCount(1_000)
                                .flowControlMaxOutstandingRequestBytes(1_000_000)
                                .build());

        assertThat(limits.exceededBy(BufferUsage.of(1_001, 1_000_033))).isFalse();
        assertThat(limits.exceededBy(BufferUsage.of(1_000, 2_000_000))).isFalse();
        // And the first lease-expiry wave, which adds a whole window, still crosses it.
        assertThat(limits.exceededBy(BufferUsage.of(2_001, 0))).isTrue();
        assertThat(limits.exceededBy(BufferUsage.of(0, 2_000_001))).isTrue();
    }

    @Test
    void aFlowControlLimitNearTheMaximumDoesNotWrap() {
        PausedSplitBufferLimits limits =
                PausedSplitBufferLimits.of(
                        PubSubSubscriberOptions.builder()
                                .flowControlMaxOutstandingElementCount(Long.MAX_VALUE)
                                .flowControlMaxOutstandingRequestBytes(Long.MAX_VALUE)
                                .build());

        assertThat(limits.maxMessages()).isEqualTo(Long.MAX_VALUE);
        assertThat(limits.maxBytes()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void aPausedSplitDefaultsToTwiceTheConfiguredFlowControlLimit() {
        // The rung that matters most: a deployment that tuned flow control has moved the point at
        // which a buffer proves the client's bound has lapsed, and this bound has to move with it.
        PausedSplitBufferLimits limits =
                PausedSplitBufferLimits.of(
                        PubSubSubscriberOptions.builder()
                                .flowControlMaxOutstandingElementCount(50_000)
                                .flowControlMaxOutstandingRequestBytes(4_000_000)
                                .build());

        assertThat(limits.maxMessages()).isEqualTo(2 * 50_000);
        assertThat(limits.maxBytes()).isEqualTo(2 * 4_000_000);
    }

    @Test
    void anExplicitPausedSplitBoundWinsOverTheFlowControlLimitItShadows() {
        PausedSplitBufferLimits limits =
                PausedSplitBufferLimits.of(
                        PubSubSubscriberOptions.builder()
                                .flowControlMaxOutstandingElementCount(50_000)
                                .flowControlMaxOutstandingRequestBytes(4_000_000)
                                .pausedSplitBufferMaxMessages(7)
                                .pausedSplitBufferMaxBytes(11)
                                .build());

        assertThat(limits.maxMessages()).isEqualTo(7);
        assertThat(limits.maxBytes()).isEqualTo(11);
    }

    @Test
    void eachDimensionFallsBackOnItsOwn() {
        // Crossed: the message dimension is set explicitly, the byte dimension is not — and the
        // byte bound must come from flow control's byte limit, never from the message number.
        PausedSplitBufferLimits limits =
                PausedSplitBufferLimits.of(
                        PubSubSubscriberOptions.builder()
                                .flowControlMaxOutstandingRequestBytes(4_000_000)
                                .pausedSplitBufferMaxMessages(7)
                                .build());

        // The explicit bound is taken as given; only the shadowed limit is doubled.
        assertThat(limits.maxMessages()).isEqualTo(7);
        assertThat(limits.maxBytes()).isEqualTo(2 * 4_000_000);
    }

    @Test
    void eitherDimensionAloneIsEnoughToBeOverTheBound() {
        // Whichever binds depends on message size, so the bound is crossed by either — an `and`
        // here would never park a workload of few large messages, the one that fills a heap first.
        PausedSplitBufferLimits limits =
                PausedSplitBufferLimits.of(
                        PubSubSubscriberOptions.builder()
                                .pausedSplitBufferMaxMessages(10)
                                .pausedSplitBufferMaxBytes(1_000)
                                .build());

        assertThat(limits.exceededBy(BufferUsage.of(11, 1))).isTrue();
        assertThat(limits.exceededBy(BufferUsage.of(1, 1_001))).isTrue();
        assertThat(limits.exceededBy(BufferUsage.of(11, 1_001))).isTrue();
    }

    @Test
    void aUsageExactlyAtEitherLimitIsNotOverIt() {
        PausedSplitBufferLimits limits =
                PausedSplitBufferLimits.of(
                        PubSubSubscriberOptions.builder()
                                .pausedSplitBufferMaxMessages(10)
                                .pausedSplitBufferMaxBytes(1_000)
                                .build());

        assertThat(limits.exceededBy(BufferUsage.of(10, 1_000))).isFalse();
        assertThat(limits.exceededBy(BufferUsage.of(0, 0))).isFalse();
    }
}
